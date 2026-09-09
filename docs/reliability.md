# Retries, redelivery and shutdown

## The attempt counter, and why it rides on the message

A handler that returns `retry()` gets another delivery if the policy has one
left. The message that comes back has `x-acemq-attempt` one higher than the one
that failed, and `message.envelope.attempt` reads it.

That count lives **on the message**, and getting it there is why a retry is
*republished* rather than requeued. A requeue returns the bytes the broker was
given: the attempt header would still say what the publisher wrote however many
times the message had come round, and the count would live only in the memory of
the process that has been failing — which is the one place it is lost when that
process restarts.

The cost is that a retried message goes to the **back** of its queue rather than
the front, so it is no longer in order with its neighbours. For a message that
has already failed once, that is the better trade.

`redelivered` is a different thing and is worth not confusing with it. It is the
broker's own flag, set when the broker hands the same delivery back — after a
consumer died without settling it. A message can be on attempt 1 and
redelivered.

## Where a message goes when it is given up on

Two queues, and they are different on purpose:

| | |
|---|---|
| `{queue}.dlq` | ran out of attempts, was rejected, or was marked fatal. Usually the world |
| `{queue}.parked` | nothing could read it: the body would not decode, or a handler returned `park(...)`. Usually a producer |

The parked queue is reached two ways and they mean the same thing. The engine
parks a body its codec refuses, before any handler runs; a handler that gets
one layer further in and finds a schema it was never taught returns
[`park(error)`](consuming.md#reject-or-park) and lands in the same place. Both
count on `acemq.messages.dead.lettered.total{outcome="parked"}`, and neither is
filed with the dead letters themselves —
which is the entire reason there are two queues.

`Topology().queue(name, dead_letter=True)` declares both, and **so does the
consumer, when it starts**. A library that parks messages into a queue nobody
declared has only moved the disappearance somewhere else: the republish reaches
no queue, the broker drops it, and nothing anywhere says so. Declaring both ends
of that at start-up costs a few round trips once per consumer and closes the
path. See [who declares what](topology.md#who-declares-what) for the whole split
and for the `declare=False` a consumer with no `configure` permission needs.

The message arrives with `x-acemq-error` saying which limit it hit and what the
last failure was:

```
exhausted 5 attempts: RuntimeError: the warehouse is not answering
exceeded the maximum message age of 6:00:00: TimeoutError: no answer in 30s
the handler rejected it: ValueError: no such customer
could not be decoded: FatalError: acemq: this message is not JSON: ...
```

The reason travels as an envelope field, so a consumer of the dead-letter queue
reads it back through the API — `message.envelope.error` — rather than having to
know the wire header name.

The original delivery is then **acknowledged**. Acknowledging a message that
failed looks wrong and is what makes this reliable: the message has already been
safely republished somewhere else, so acknowledging the original is removing the
copy that has been dealt with. Rejecting it instead would either requeue it into
a hot loop or, with a dead-letter exchange configured on the queue, send it
somewhere this consumer did not choose and without the reason.

If the dead-letter queue is **not** there, the message is rejected to the broker
instead — the broker's own dead-lettering is the last thing left between it and
nothing — and `acemq.messages.set.aside.failed` is counted.

## A policy

```python
from datetime import timedelta
from acemq_amqp import exponential_retry, fixed_retry, no_retry

no_retry()                                              # one delivery, no second chance
exponential_retry(5, timedelta(seconds=1), timedelta(minutes=1))
fixed_retry(4, timedelta(minutes=1))
```

| | |
|---|---|
| `no_retry()` | `max_attempts=1`. **The default** on a connection with no policy |
| `exponential_retry(n, initial, max_delay=0)` | doubling delays with 20% jitter, which is the sane default |
| `fixed_retry(n, delay)` | the same wait every time, no jitter |

`max_attempts` counts **total deliveries including the first**, so
`exponential_retry(5, ...)` means one attempt and four retries.

The default being one delivery is deliberate. A `retry()` on a connection with
no policy dead-letters the message and says so in the reason, which is louder
than the alternative default — an immediate requeue, which is a hot loop nobody
asked for.

A policy goes on the connection, or on one consumer:

```python
mq = await connect(url, retry=exponential_retry(5, timedelta(seconds=1)))
await mq.consume("slow.things", handle, retry=fixed_retry(3, timedelta(minutes=5)))
```

`RetryPolicy` is a frozen dataclass, so building one by hand is fine and the
adjusting methods return copies:

```python
policy = (
    exponential_retry(6, timedelta(seconds=10), timedelta(minutes=10))
    .give_up_after(timedelta(hours=6))
    .with_jitter(0.1)
)
```

### Seeing what a policy would do

```python
policy = exponential_retry(6, timedelta(seconds=10))
policy.schedule()      # [10s, 20s, 40s, 80s, 160s], without jitter
policy.broker_rungs()  # [40s, 80s, 160s]
```

`schedule()` reports the delays **without** jitter, which is what to read when
deciding whether a policy is the one you meant: five numbers are easier to argue
with than four parameters. It has one entry fewer than `max_attempts`, because
the first delivery is not waited for.

`next_wait(attempt, age)` is the whole answer for one message — how long, and
where — and returns `None` when there should be no further attempt.

## Jitter

`exponential_retry` carries 20% jitter. It moves a delay **both ways**:
one-sided jitter only ever delays, which turns a thundering herd into a slower
thundering herd rather than spreading it.

It applies **below the broker threshold only**. A rung queue's time-to-live is
fixed when the queue is declared, so a moved delay would name a queue that does
not exist. Above the threshold the spread comes free anyway: each message's TTL
starts when it enters the rung, so a fleet that failed over ten seconds is
released over ten seconds.

## Giving up on age

```python
policy = exponential_retry(5, timedelta(seconds=1)).give_up_after(timedelta(hours=6))
```

Attempts say nothing about how long a message has been waiting. A queue that was
paused overnight hands back messages that are on attempt one and four days old,
and delivering them now is usually worse than not. `max_message_age` is checked
against `envelope.age` before every retry decision.

There is **no age limit unless you ask for one**. `max_message_age` defaults to
zero, and zero means never — the attempt count is the only thing stopping a
message that nobody bounded by age. Go, .NET and Ruby read zero the same way.
Java does not: its default is 365 days and it is compared against
unconditionally, so a message exactly a year old is abandoned there and retried
here. That is a real divergence, and it is
[recorded as one](testing.md#where-the-libraries-do-not-agree-yet) rather than
hidden, but it is not one a service will meet by accident.

## Errors that will not improve

```python
from acemq_amqp import FatalError

raise FatalError("this producer is on a schema this consumer cannot read")
return retry(FatalError("no such customer, and there never will be"))
```

Either form skips the attempts that are left. The mark on the error is honoured
over the request in the acknowledgement, which is the point of having it: a
handler deep in a call stack can say "do not bother" without having to reach the
`Ack` the top of the handler returns.

`JsonCodec` raises `FatalError` for a body that is not JSON, for the same
reason — though a decode failure never reaches the retry path at all; it is
parked before the handler runs.

## Where the waiting happens

A short wait is spent in the consumer, holding the delivery and one prefetch
slot. A long one is spent in the broker, on a `{queue}.retry.{delay}` queue
whose `x-message-ttl` is the wait and whose dead-letter target is the source
queue. **The line between them is 30 seconds** —
`DEFAULT_BROKER_WAIT_THRESHOLD` — unless a policy says otherwise:

```python
policy.wait_in_broker_from(timedelta(minutes=1))   # move the line
policy.wait_in_broker_from(timedelta(0))           # never; every wait is held here
```

Zero is the way out for a service that may not declare queues on its broker: no
rungs exist, so nothing tries to declare any and nothing publishes to one. Pair
it with `declare=False` on `consume()`, which stops the consumer declaring its
dead-letter queues too. It is the wrong setting for a policy with delays measured
in minutes.

Splitting at a threshold rather than picking one of the two takes the durability
where it is worth its complexity and leaves the simplicity where it is not:

- **A consumer sleeping on a five-minute backoff loses the whole wait when it
  restarts.** The broker redelivers the unacknowledged message at once, so a
  five-minute policy delivers in none.
- **A queue's TTL is fixed at declaration and cannot express a jittered delay.**
  Below the threshold, jitter is worth more than durability; above it, the
  reverse.

### The rungs

One queue per **distinct** delay at or above the threshold — a fixed policy that
waits a minute three times needs one queue, not three:

```python
from acemq_amqp import retry_queue, rung_args

retry_queue("shipping.orders", timedelta(seconds=40))
# 'shipping.orders.retry.40s'

rung_args("shipping.orders", timedelta(seconds=40))
# {'x-message-ttl': 40000,
#  'x-dead-letter-exchange': 'acemq.retry',
#  'x-dead-letter-routing-key': 'shipping.orders'}
```

Exactly three keys, and the same three in every AceMQ library. Two services
consuming one queue declare the same rung by name, so a rung declared with
anything else answers the second service `PRECONDITION_FAILED` and leaves it
unable to consume at all. That is why the table is pinned by a test and why
`rung_args` is the only place it is written.

Nothing consumes a rung. The time-to-live is the only thing that ever takes a
message out of one, and the binding made by `Topology.queue` is what brings it
home.

**Never use a per-message TTL for this.** The obvious simplification is one rung
queue with `expiration` set on each message, and it does not work: RabbitMQ
expires messages only from the **head** of a queue, so a ten-minute wait at the
front holds back every thirty-second wait behind it. The delays that come out
would bear no relation to the ones that went in, and the bug would only appear
under the load that puts two different waits on one queue at once.

### The two exchanges this library declares

| Constant | Value | What it carries |
| --- | --- | --- |
| `RETRY_EXCHANGE` | `acemq.retry` | An expired rung message, back to the queue it came from |
| `DEAD_LETTER_EXCHANGE` | `acemq.dlx` | The `{queue}.dlq` and `{queue}.parked` queues, each bound on its own name |

Both are **direct** and **durable**, and both are declared by
`Topology().queue(...)` when it is asked for retries or for dead-lettering — as
is the one binding that brings an expired message home, `{queue}` to
`acemq.retry` on `{queue}`. A consumer declares the same exchanges, the same
rungs and the same binding again when it starts, with the same arguments, so the
second declaration is a no-op whichever of the two arrives first.

That binding is not optional and is not lazy. A direct exchange drops what it
cannot route and says nothing, so a topology missing it loses every expired
retry while the queue looks quiet.

Python used to dead-letter a rung through the default exchange, which needs no
exchange and no binding and works perfectly well on its own. Java has always
used the named exchange, most of the released code follows Java, and two
libraries cannot both be right about one queue. This is the settled answer, and
a broker already carrying a Java service has this arrangement on it.

### When a rung is missing

A consumer declares its own rungs at start-up, so this is rarer than it was —
but it has not gone away. A consumer started with `declare=False` declares
nothing, and a rung deleted under a running consumer is gone whoever made it.

If the rung queue is not on the broker, the message is **still retried** — the
consumer holds it and waits here instead — and:

- `acemq.retry.rung.missing` is counted, labelled with the rung's name
- an error is logged naming the queue, the `Topology().queue(..., retry=...)`
  call that would declare it, and the `declare=True` that would have

That metric is the one to alert on, because nothing else shows it. The message
is still retried and the wait still happens, so a dashboard reads as normal
while the reason the rung exists is quietly gone — and a consumer restart
mid-wait shortens a five-minute backoff to nothing.

## When the connection drops

The transport underneath is an `aio_pika` robust connection, so a broker restart
is a pause rather than an outage: the connection is reopened, the channels and
the consumers with it, and the messages that were unacknowledged when it went
are redelivered.

Redelivered, not lost — and redelivered means **duplicated** from the
application's point of view, because a handler that finished its work and had
not yet acknowledged will run again. See [duplicates](#duplicates).

Topology is **not** redeclared on reconnection. Declare at start-up; the queues
are durable and outlive the connection.

## When the broker refuses something

A refused declaration — `PRECONDITION_FAILED`, from a queue that already exists
with different arguments — kills its channel. This library hands out channels
rather than sharing one for exactly that reason: on a shared channel, one
service's disagreement about one queue would also stop every publisher and
consumer that happened to be using it.

`Topology.apply` stops at the first failure and passes the refusal on rather
than swallowing it. It means this service and the broker disagree about what a
queue is, and carrying on would leave the service using a queue that is not the
one it asked for.

## Publisher confirms

They are on and cannot be turned off. `send` does not return until the broker
has acknowledged the message, and `result.confirmed` says so.

Without them a publish succeeds as soon as the bytes reach the socket, which is
not the broker promising anything, and a service that treats it as one loses
messages it believes it sent. The cost is a round trip per message; a service
that publishes in bulk should publish **concurrently** — `asyncio.gather` over
several `send` calls — rather than looking for a way to switch confirms off.

## Messages that reach no queue

An unroutable message is the quietest failure AMQP has: the publish succeeds,
the message is discarded, the consumer waits, and nothing says why.

`mq.publisher(..., mandatory=True)` makes the broker return it and this library
raise `PublishError`. Without it, `result.routed` still reports the fact — it is
only the raising that is opted into.

Every hop this library makes itself is mandatory: a retry, a dead letter, a
parked message and a replay all republish by queue name and check that the queue
was there.

## Duplicates

At-least-once delivery is not a flaw to be worked around; it is the only
guarantee a broker can give cheaply, and every AceMQ retry is a redelivery on
purpose. A handler that changes anything needs to be able to tell it has seen a
message before.

`x-acemq-id` is stable across every redelivery of the same message, which makes
it the natural key:

```python
from acemq_amqp.patterns import InMemoryIdempotencyStore, idempotent

await mq.consume("shipping.orders", idempotent(InMemoryIdempotencyStore(), ship))
```

See [idempotency](patterns.md#idempotency), and note what the in-memory store's
own docstring says about not being the one to use in production.

## Putting dead letters back

```python
from acemq_amqp import dead_letter_queue
from acemq_amqp.patterns import replay

result = await replay(
    mq,
    dead_letter_queue("shipping.orders"),
    limit=500,
    deadline=timedelta(minutes=5),
    only=lambda envelope, body: "timeout" in envelope.error,
)
print(result)   # moved 137, skipped 363 (drained)
```

A replay **resets the attempt counter** by default: the envelope goes back with
`attempt=1` and `error=""`. That is what a replay is for. A message that arrives
back on attempt five of a five-attempt policy is dead-lettered again before a
handler sees it, and the operator who has just fixed the bug has moved two
thousand messages from one queue to the same queue.

`restart=False` puts back exactly what was there — for an audit, or for a queue
read by something that counts attempts itself.

The count comes back with the reason it stopped, because "moved 500" means
something quite different when the limit was 500. See
[replay](patterns.md#replay).

## Shutdown

```python
await consumer.close()   # this consumer
await mq.close()         # every consumer on the connection, then the transport
```

Or let the context managers do it:

```python
async with await connect(url) as mq:
    async with await mq.consume("shipping.orders", ship):
        await stop_signal.wait()
```

`close` on a consumer unsubscribes first, then waits for the handlers already
running to finish **and settle**. A message that had been delivered but not
started is given back to the broker rather than held: it is the broker's to hand
to another consumer, and working through a retry delay for it would make closing
take as long as the schedule.

The subscription is released last, after everything has been settled, because a
settlement travels on the channel its delivery arrived on.

What this means in practice: a rolling deploy finishes the messages it started
and gives back the ones it had not, so the new instance picks them up. It does
not mean nothing is duplicated — a handler that completed its work and was
killed before its acknowledgement left is a duplicate whatever the shutdown
does, which is [why idempotency is a pattern](patterns.md#idempotency) rather
than a setting.
