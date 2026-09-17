# Tutorial 2 — Surviving failure

**20 minutes.** Continues from
[tutorial 1](tutorial-first-message.md). Same broker.

Your handler fails. This tutorial is about what should happen next, and about
the answer most services reach for first — which is wrong in a way that takes a
production incident to notice.

## Step 1 — Watch it fail

```python
async def ship(message: Message) -> Ack:
    print("attempt", message.envelope.attempt)
    raise RuntimeError("the warehouse is not answering")


consumer = await mq.consume("shipping.orders", ship)
```

```
attempt 1
```

One attempt, and the message is in `shipping.orders.dlq` with a reason on it.

That is the default because **the default retry policy is `no_retry()`** — one
delivery, no second chance. A handler that raises is asking for a retry, the
policy says there is none, and the message is dead-lettered with
*exhausted 1 attempt: RuntimeError: the warehouse is not answering* written into
its envelope.

This is a deliberate choice and worth a sentence. The obvious alternative default
is *requeue*, which is what most AMQP clients do: a message that fails, requeues,
is redelivered instantly, fails again and requeues is a **poison message loop** —
one bad message saturating a consumer at thousands of attempts a second with
nothing else getting through. A hot loop against somebody's broker is not a
default this library is willing to have. Dead-lettering is loud, recoverable, and
says why.

## Step 2 — The answer that looks right

Almost every service reaches for this first:

```python
async def ship(message: Message) -> Ack:
    for attempt in range(3):
        try:
            await warehouse.reserve(message.payload)
            return accept()
        except Exception:
            await asyncio.sleep(1)      # ← the problem
    return reject(RuntimeError("gave up"))
```

**Do not do this.** The sleep is inside the consumer, which means:

- the delivery is held for the whole three seconds, holding one of this
  consumer's prefetch slots;
- with `concurrency=1` — the default — everything behind the failure waits for
  it. One unavailable downstream service becomes a stalled queue;
- the broker cannot tell a slow consumer from a stuck one. Your consumer looks
  healthy while doing nothing;
- **the retry count is a local variable**, so if the process dies mid-sleep the
  count dies with it, and the message comes back on attempt one.

The last one is the real problem, and it is the one a policy fixes.

## Step 3 — A policy

```python
from datetime import timedelta

from acemq_amqp import exponential_retry

policy = exponential_retry(
    4,                          # attempts, including the first
    timedelta(seconds=20),      # the first delay
    timedelta(minutes=5),       # the ceiling
)

consumer = await mq.consume("shipping.orders", ship, retry=policy)
```

Or on the connection, for every consumer on it:

```python
async with await connect(url, retry=policy) as mq:
```

`exponential_retry` doubles, so this waits 20s, 40s and 80s. You can look at the
schedule before running anything:

```python
policy.schedule()        # [0:00:20, 0:00:40, 0:01:20]
policy.broker_rungs()    # [0:00:40, 0:01:20]
```

`schedule()` reports the delays **without** jitter, which is why it is the thing
to assert on in a test: a jittered schedule is not reproducible and a test
against one fails on a Tuesday.

### The attempt counter rides on the message

A retry is **republished**, not requeued. A requeue hands back the bytes the
broker already had, so the attempt header would still read what the publisher
wrote however many times the message had come round, and the count would live
only in this process's memory — the one place it is lost when the process that
has been failing restarts.

The cost is that a retried message goes to the back of the queue rather than the
front. For a message that has already failed once, that is the better trade.

### Where the waiting happens

This is the part `broker_rungs()` was hinting at. **Thirty seconds is the line.**

A wait below it is held here: the delivery is kept, one prefetch slot is spent,
and for a few seconds that is cheaper than asking somebody's broker for a queue.
A wait above it is not. The message is published to a **rung queue** whose
`x-message-ttl` is the delay and whose dead-letter target is the queue it came
from, and the consumer is free immediately.

```
shipping.orders                  your queue
shipping.orders.retry.40s        a rung: TTL 40s, dead-letters back
shipping.orders.retry.80s        a rung
shipping.orders.dlq              attempts exhausted
shipping.orders.parked           the body could not be decoded
acemq.dlx, acemq.retry           the two exchanges that route those
```

There is no `shipping.orders.retry.20s`, and that is the threshold doing its job.
Twenty seconds lost to a restart is twenty seconds; eighty is where that stops
being acceptable, because a consumer that dies mid-wait **does not resume
mid-wait** — the broker redelivers the unacknowledged message at once, and a
policy that said eighty seconds delivers in none.

**One rung per distinct delay, not one queue per message.** The delay is a
property of the queue, so a thousand waiting messages cost one queue.

The obvious simplification — one rung and a per-message `expiration` — does not
work, and it is worth knowing why before someone suggests it. RabbitMQ only ever
expires the message at the **head** of a queue, so a ten-minute message at the
front holds back every thirty-second one behind it, and the delays that come out
bear no resemblance to the ones that went in.

Move the line, or remove it:

```python
policy = policy.wait_in_broker_from(timedelta(minutes=1))   # rungs past a minute
policy = policy.wait_in_broker_from(timedelta(0))           # no rungs at all
```

Zero is the way out, not the way in: with no threshold, nothing is long enough
to reach the broker, so every wait is spent in the consumer and no rung queue is
needed. That is the right setting for a service whose broker it may not declare
queues on, and the wrong one for a policy with delays measured in minutes.

Note the reassignment. `RetryPolicy` is frozen, like everything else on the wire
here, so these return a **new policy** and calling one for its effect does
nothing.

### Jitter

Twenty percent, in both directions, already applied. Consumers that fail together
do not then all retry at the same instant and hit the recovering service
simultaneously. `with_jitter(factor)` changes the amount, and `fixed_retry`
starts with none because a fixed delay is an exact request.

Jitter is **never** applied to a wait the broker holds: a rung's TTL is fixed
when the queue is declared, so a moved delay would name a queue that does not
exist.

## Step 4 — Say which failures are worth retrying

Retrying is only correct for failures that might not happen next time. A
malformed message will be malformed on every attempt, and four attempts only
delay the inevitable by two minutes.

```python
from acemq_amqp import FatalError, accept, park, reject, retry


async def ship(message: Message) -> Ack:
    order = message.payload
    if order["totalCents"] <= 0:
        return reject(ValueError(f"total must be positive, was {order['totalCents']}"))
    try:
        await warehouse.reserve(order)
    except Unreachable as down:
        return retry(down)
    except NoSuchItem as gone:
        return retry(FatalError(f"no such item: {gone}"))
    return accept()
```

| | |
|---|---|
| `retry(e)` | ask the policy. Another attempt if there is one, dead-letter if not |
| `retry(FatalError(...))` | the handler asked for a retry and marked the reason as one that will not change. The mark wins: straight to the dead-letter queue |
| `reject(e)` | this message is wrong. Dead-letter it now |
| `park(e)` | nothing could **read** it |
| raising | the same as `retry(e)` — an unclassified failure is assumed transient, because the cost of retrying a permanent failure is a delay and the cost of not retrying a transient one is a lost message |

### Reject or park?

`reject` is for a message that was read and refused; `park` for one that could
not be read at all. They go to different queues on purpose. A message that failed
five times and a message nothing can decode are different problems with different
answers — one is usually the world, the other is usually a producer that deployed
a format change ahead of its consumers — and whoever drains the dead letters
should not have to sort them by hand.

A body the codec itself refuses is parked without the handler ever running, for
the same reason.

## Step 5 — Read the dead letters

The reason and the attempt count are attached when the message is set aside, not
reconstructed later from logs:

```python
async def look(message: Message) -> Ack:
    print(f"gave up on {message.payload['orderId']}")
    print(f"  attempts: {message.envelope.attempt}")
    print(f"  reason:   {message.envelope.error}")
    return accept()


from acemq_amqp import dead_letter_queue

watcher = await mq.consume(dead_letter_queue("shipping.orders"), look, declare=False)
```

```
gave up on o-1
  attempts: 4
  reason:   exhausted 4 attempts: RuntimeError: the warehouse is not answering
```

Note `declare=False`. A one-off reader of `shipping.orders.dlq` has no business
creating `shipping.orders.dlq.dlq`, and that is what leaving it on would do.

`dead_letter_queue(name)` and `parked_queue(name)` build the names, so nothing in
your code has to know the suffix is `.dlq`.

## Step 6 — Put them back

The warehouse is fixed. The messages are still in the dead-letter queue.

```python
from acemq_amqp.patterns import replay

result = await replay(mq, dead_letter_queue("shipping.orders"), limit=500)
print(result)      # moved 137, skipped 0 (drained)
```

`replay` reads a queue and republishes what it finds, keeping each message's own
routing key so a message goes back where it came from rather than everywhere.

**Every message gets a fresh set of attempts**, because that is what a replay is
for: a message arriving back on attempt four of a four-attempt policy is
dead-lettered again before a handler sees it, and the operator who has just fixed
the bug has moved two thousand messages from one queue to the same queue. Pass
`restart=False` to put back exactly what was there.

Replay some of them when only some should go back:

```python
result = await replay(
    mq,
    dead_letter_queue("shipping.orders"),
    limit=100,
    only=lambda envelope, body: "warehouse" in envelope.error,
)
```

What the filter declines goes **back**, not away — and it is held unsettled until
the pass is over rather than returned one at a time, because a message returned
immediately goes to the *head* of the queue and the next read hands over the same
one for ever.

Replayed messages are marked with `acemq-replayed-from`, `acemq-replayed-at` and
`acemq-replay-count`, so a message going round for the third time is visible as
such rather than looking fresh. Those names carry no `x-acemq-` prefix on purpose:
that namespace is stripped before a handler sees it, and these are meant to reach
one.

## Step 7 — All together

```python
import asyncio
from datetime import timedelta

from acemq_amqp import (
    Ack, Message, Topology, accept, connect, dead_letter_queue, fixed_retry,
)
from acemq_amqp.patterns import replay


async def main() -> None:
    policy = fixed_retry(3, timedelta(seconds=1))

    async with await connect("amqp://guest:guest@localhost:5672/", retry=policy) as mq:
        await mq.declare(Topology().queue("shipping.orders", dead_letter=True, retry=policy))

        attempts = []
        gave_up = asyncio.Event()

        async def ship(message: Message) -> Ack:
            attempts.append(message.envelope.attempt)
            print(f"attempt {message.envelope.attempt} for {message.payload['orderId']}")
            raise RuntimeError("the warehouse is not answering")

        async def look(message: Message) -> Ack:
            print(f"dead: {message.payload['orderId']}"
                  f" after {message.envelope.attempt} attempts"
                  f" — {message.envelope.error}")
            gave_up.set()
            return accept()

        consumer = await mq.consume("shipping.orders", ship)
        watcher = await mq.consume(
            dead_letter_queue("shipping.orders"), look, declare=False
        )

        await mq.publisher(routing_key="shipping.orders").send(
            {"orderId": "o-1", "totalCents": 4250}
        )

        await asyncio.wait_for(gave_up.wait(), 30)
        await consumer.close()

        moved = await replay(mq, dead_letter_queue("shipping.orders"), limit=10)
        print(moved)
        await watcher.close()


asyncio.run(main())
```

```
attempt 1 for o-1
attempt 2 for o-1
attempt 3 for o-1
dead: o-1 after 3 attempts — exhausted 3 attempts: RuntimeError: the warehouse is not answering
moved 0, skipped 0 (drained)
```

Note the gaps between the attempts, and note that nothing slept in a handler to
produce them. `fixed_retry(3, 1s)` is below the threshold, so the library holds
the delivery rather than your handler — a wait the retry accounting knows about
either way. The replay moved nothing because the watcher had already drained the
dead-letter queue; leave it out and it moves the one message.

## What to watch in production

| | |
|---|---|
| Depth of `*.dlq` | rising means something is failing permanently. This is the alert |
| Depth of `*.retry.*` | rising means something is failing transiently, right now |
| Depth of `*.parked` | anything above zero is a message nothing can decode |
| `acemq.retry.rung.missing` | **the one nothing else shows.** A long backoff waited in the consumer because its rung queue is not on the broker: throughput and dead letters all read as normal while the reason the rung exists is quietly gone |

Depth alone is not an alert. A queue is a buffer and it is *supposed* to have
things in it — alert on depth that is not draining, or on the age of the oldest
message, never on a threshold.

## Next

**[Tutorial 3 — Never processing twice](tutorial-exactly-once.md).** Retries
create duplicates by construction. What that means when the handler takes money.
