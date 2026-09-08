# Consuming

```python
async def ship(message: Message) -> Ack:
    place(message.payload)
    return accept()


consumer = await mq.consume("shipping.orders", ship)
```

`consume` subscribes and returns a running `Consumer`. It is a coroutine because
subscribing is a round trip to the broker.

## The decision is the return value

```python
from acemq_amqp import accept, reject, retry

accept()                 # done. Acknowledged and gone
retry(error)             # try again, if the policy has an attempt left
reject(error)            # do not try again. Straight to {queue}.dlq
```

Three functions returning an `Ack`, not three exceptions and not a mutable
parameter. The value carries the error with it, and that error is what ends up
in `x-acemq-error` on the dead letter, so an operator draining
`shipping.orders.dlq` reads *why* rather than only *that*:

```python
return retry(RuntimeError("the warehouse is not answering"))
```

The reason is worth writing properly. `exhausted 5 attempts: RuntimeError: the
warehouse is not answering` is a message somebody can act on; `exhausted 5
attempts: Exception` is not.

### A handler that raises

An exception escaping the handler is treated as `retry` with that exception.
That is deliberate: in Python an exception is how a thing says it failed, and
most of the failures it carries are transient, so the useful default is another
attempt. It is also what the Java library does with the same situation, and what
a handler written without reading this page will expect.

To say the opposite, raise `FatalError`:

```python
from acemq_amqp import FatalError

if message.payload.get("version") != 2:
    raise FatalError("this producer is on a schema this consumer cannot read")
```

`FatalError` skips the attempts that are left, because they would all fail the
same way. Returning `retry(FatalError(...))` means the same thing — the mark on
the error is honoured over the request in the acknowledgement, which is the
point of having it.

### Retry or reject?

`retry` for anything that might work next time: a timeout, a dependency that is
down, a lock somebody else holds. `reject` for anything that will not: a payload
that fails validation, an entity that does not exist and will not appear, a
message this consumer is not the right version to read.

A rejected message goes to `{queue}.dlq` immediately, with no attempts spent.

## What the handler receives

`Message` is a frozen dataclass:

| | |
|---|---|
| `payload` | the decoded body, from the connection's codec |
| `envelope` | the [envelope](envelope.md): id, type, correlation, attempt, origin, first seen, your headers |
| `routing_key` | what it was published under |
| `content_type` | what the producer said the body was, or `None` |
| `redelivered` | the broker's flag: this delivery has been attempted before |
| `body` | the raw bytes, before the codec |

`redelivered` is the broker's own flag and is not the same as
`envelope.attempt`. The attempt count is on the message and advances when this
library republishes it; `redelivered` is set when the *broker* hands the same
delivery back, which happens after a consumer dies without settling. A message
can be on attempt 1 and redelivered, and that is a useful thing to notice — see
[idempotency](patterns.md#idempotency).

`body` is there for the handler that wants the bytes as they were: writing them
to a log, forwarding them unchanged, or comparing them against a signature.

## Concurrency

```python
consumer = await mq.consume("shipping.orders", ship, concurrency=8)
```

One by default, which keeps a queue's messages in order. Raising it trades that
order for throughput, which is the right trade for handlers that spend their
time waiting on something else — a database, an HTTP call — and the wrong one
where a later message about the same entity must not overtake an earlier one.

It is not the loop's concurrency by accident: the consumer runs exactly
`concurrency` worker tasks and feeds them from a queue of its own. Handing every
delivery straight to a task would make prefetch the real concurrency limit,
which is not what anyone asked for.

Where ordering matters for *some* messages and not others, keep the concurrency
and order by key:

```python
from acemq_amqp.patterns import by_header, ordered

await mq.consume("shipping.orders", ordered(by_header("orderId"), ship), concurrency=8)
```

See [ordering](patterns.md#ordering).

**A group is the other axis.** `concurrency` runs several handlers on one
consumer sharing one channel and one prefetch; a `ConsumerGroup` runs several
consumers, each with its own. Reach for a group when one channel's prefetch is
the limit, or when a fair share across processes matters. See
[consumer groups](patterns.md#consumer-groups).

## Prefetch

```python
consumer = await mq.consume("shipping.orders", ship, prefetch=100)
```

How many unacknowledged messages the broker will send before waiting. The
connection's `prefetch` — 20 by default — applies unless the consumer overrides
it.

Too low and the consumer waits on the network between messages. Too high and one
instance takes a share of the queue it cannot work through, so a second instance
starves and a restart redelivers a pile. Twenty is a reasonable default for
handlers that take tens of milliseconds; raise it for handlers that take one, and
drop it towards `concurrency` for handlers that take seconds.

A consumer waiting out a short retry delay holds its delivery and therefore one
prefetch slot. That is one of the two reasons long waits go to the broker
instead; see [where the waiting happens](reliability.md#where-the-waiting-happens).

## Naming the consumer

```python
consumer = await mq.consume("shipping.orders", ship, tag="shipping@pod-7")
```

The tag is what the consumer is called in `rabbitmqctl list_consumers` and in
the management UI. Left out, the broker generates one, and an operator looking at
four identical generated tags cannot tell which pod is the slow one.

`args` passes broker-specific consumer arguments straight through — a stream
offset, a single-active-consumer flag, a priority.

## A different codec

```python
from acemq_amqp import BytesCodec

await mq.consume("shipping.orders.dlq", inspect, codec=BytesCodec())
```

The connection's codec applies unless a consumer overrides it. `BytesCodec` is
what to read a dead-letter queue with: the message that went there may be
exactly the one nothing could decode, and a codec that fails on it would park it
a second time. See [codecs](serialization.md).

## When a message cannot be decoded

It goes to `{queue}.parked`, not to `{queue}.dlq`, with
`x-acemq-error` saying `could not be decoded: ...`.

That is a different queue on purpose. A message that failed five times and a
message nothing could read are different problems with different answers — one
is usually the world, the other is usually a producer — and whoever drains the
dead letters should not have to sort them by hand. A body that will not decode
decodes no better next time, so it does not go round the retry schedule until it
ages out either.

`Topology().queue(name, dead_letter=True)` declares `{name}.parked` alongside
`{name}.dlq` for this reason. A library that parks messages into a queue nobody
declared has only moved the disappearance somewhere else.

## A handler that returns the wrong thing

A handler returning something that is not an `Ack` — most often because a branch
forgot to return at all, and returned `None` — has its message dead-lettered
with `the handler returned NoneType instead of an Ack`. Not acknowledged, and
not retried into a loop: the message is set aside and the reason says what the
bug is.

## Stopping

```python
await consumer.close()
```

Or as a context manager, which closes it however the block is left:

```python
async with await mq.consume("shipping.orders", ship):
    await asyncio.Event().wait()
```

`close` unsubscribes, then waits for the handlers already running to finish and
settle. A message that had been delivered but not started is **given back** to
the broker rather than held: it is the broker's to hand to another consumer, and
working through a retry delay for it would make closing take as long as the
schedule.

Closing the connection closes every consumer on it first, so a service that
exits its `async with await connect(...)` block has already drained.

## What the consumer will tell you

```python
consumer.queue        # the queue it reads
consumer.running      # subscribed, with workers left to hand a delivery to
consumer.in_flight    # how many messages it is working on right now
consumer.closed       # whether it has been stopped
```

`running` is the interesting one and is what `Connection.health()` reads. A
consumer whose workers have all finished without it being closed is one the
broker is still sending messages to and nothing is reading — indistinguishable
from a quiet queue from outside. See [metrics and health](observability.md).
