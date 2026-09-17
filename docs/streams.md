# Streams

A queue forgets a message when it is acknowledged, so exactly one consumer ever
sees it and nobody can look at it again. A **stream** keeps everything until its
retention policy discards it, so several consumers read the same stream
independently and a new one can start from the beginning.

```python
from datetime import timedelta

from acemq_amqp.patterns import (
    StreamRetention, declare_stream, from_first, read_stream,
)

await declare_stream(mq, "events", StreamRetention(max_age=timedelta(days=7)))

consumer = await read_stream(mq, "events", project, offset=from_first())
```

That sounds like a queue with better retention. It is not, and the differences
are quiet rather than loud. This page is mostly about the quiet ones.

## Declaring

```python
await declare_stream(mq, "events", StreamRetention(max_age=timedelta(days=7)))
```

A stream is an ordinary queue declared with `x-queue-type: stream`, so it goes
through the same [topology](topology.md) machinery as everything else.
`declare_stream` is the short way; `stream(name, retention)` returns a
`Topology` instead, so a stream can be described as a value and applied
alongside the rest of a service's shape rather than through a call of its own:

```python
from acemq_amqp.patterns import stream

shape = stream("orders.log", StreamRetention(max_bytes=10_000_000_000))
shape.validate()              # before anything is declared
await mq.declare(shape)
```

There is no operator for joining two topologies — apply each in turn, or add the
stream's arguments to a topology you are already building:

```python
from acemq_amqp import QUEUE_TYPE_ARG

await mq.declare(
    Topology()
    .exchange("orders-events", "topic")
    .queue("orders.log", args={QUEUE_TYPE_ARG: "stream"})
    .binding("orders.log", "orders-events", "order.#")
)
```

**Durable, never exclusive, never auto-deleting.** Those three are set here
rather than left to the caller, because getting one of them wrong is answered by
the broker with a message that never mentions streams.

### Retention

```python
StreamRetention(
    max_age=timedelta(days=7),          # x-max-age
    max_bytes=10_000_000_000,           # x-max-length-bytes
    segment_bytes=500_000_000,          # x-stream-max-segment-size-bytes
)
```

`StreamRetention()` is **unbounded**, which for a stream means until the disk is
full — and a full disk is a broker-wide alarm that blocks every publisher on the
node, not a problem confined to this stream. Set at least one of these on
anything that will run for longer than an afternoon.

`max_age` is written the way RabbitMQ wants it, which is a number and a unit
suffix rather than a count of seconds, and the units are the broker's own: `D`
for days but lowercase for the rest.

| `timedelta(...)` | on the wire |
|---|---|
| `days=2` | `2D` |
| `hours=3` | `3h` |
| `minutes=90` | `90m` |
| `seconds=45` | `45s` |

`segment_bytes` is how large each of the stream's files on disk gets. Retention
happens **a whole segment at a time** — nothing is discarded until an entire
segment can be — so a very large segment makes every other retention setting
coarse: a stream told to keep an hour, in segments large enough to hold a day,
keeps a day.

**There is no default segment size, on purpose.** The broker has one and it is
tuned for the broker's storage. More than tidiness is at stake: a queue
redeclared with an argument the first declaration did not carry is a
`PRECONDITION_FAILED` rather than an adjustment, so a segment size invented here
would make a stream declared from Python impossible to redeclare from Java, Go,
.NET or Ruby. All five leave it out the same way.

## Where to start reading

```python
from acemq_amqp.patterns import (
    from_first, from_last, from_next, from_offset, from_timestamp,
)
```

| | |
|---|---|
| `from_next()` | the next message published, ignoring everything already there. **The default** |
| `from_first()` | the oldest message the stream **still holds** — a retention policy means that is not necessarily the first one ever written |
| `from_last()` | the last chunk. Roughly "the recent past" rather than an exact number of messages: a stream is stored in segments and this starts at the beginning of the last one |
| `from_offset(n)` | an exact position, inclusive |
| `from_timestamp(when)` | the first message published at or after a time |

The offset is a **consumer** setting, not a queue setting. Two readers of the
same stream sit at different places, so it cannot belong to the queue — which is
also why these are functions returning a `StreamOffset` rather than an enum: the
broker takes a different kind of value for each, and a constructor that accepted
any of them would be a constructor nobody could read.

`from_next()` is the default because it is the only one that does not replay
history the first time a reader starts. That is right for a consumer joining a
live system and **wrong for a projection**: one built without `from_first()`
silently skips its own history and looks perfectly healthy while being wrong.
State the position rather than inheriting it.

## Reading

```python
consumer = await read_stream(
    mq,
    "events",
    project,
    offset=from_first(),
    prefetch=100,
    consumer_name="projection-a",
    concurrency=1,
)
```

An ordinary [`Consumer`](consuming.md) comes back — the same object
`mq.consume` returns, closed the same way, usable as an `async with`. The
handler takes a `Message` and returns an `Ack`, exactly as anywhere else.

`prefetch` **cannot be zero**: RabbitMQ refuses a stream consumer without one,
because it is the only backpressure a stream has, and the message it gives back
does not mention streams. Ten by default — enough to keep a handler busy, small
enough that a slow one is not holding a batch nobody else can have. A projection
catching up on history wants rather more than ten.

`consumer_name` is the consumer tag the broker is told, and it is what makes
server-side offset tracking possible.

`concurrency` is 1 by default, because a stream's order is usually why it is a
stream. Raising it trades that order for throughput.

## What an acknowledgement means here

This is the part to understand before using one.

**Acknowledging does not remove the message.** It advances *this* consumer's
position. The message stays on the stream for its retention, and another
consumer reading from `from_first()` tomorrow sees everything you consumed.

Everything else follows from that, and not always in the direction another
library's documentation would suggest. Java's streams page says a stream has no
dead-letter queue and that a failed message stays where it is. **In this library
that is only half true**, because a stream consumer is the same `Consumer` the
rest of the library uses and it does what it always does: it **republishes a
copy** and then acknowledges the original.

| the handler returns | what happens |
|---|---|
| `accept()` | acknowledged. The position advances; the message is still on the stream |
| `reject(e)` | a copy is published to `{stream}.dlq`, then the delivery is acknowledged. The original is still on the stream |
| `park(e)`, or a body the codec refuses | a copy goes to `{stream}.parked`, same shape |
| `retry(e)` with attempts left | the message is **published back onto the stream**, appending a second copy, and the delivery is acknowledged |
| `retry(e)` with no attempts left | a copy goes to `{stream}.dlq` |

The fourth row is the one that bites. A retry on a queue puts a message back on
that queue; a retry on a stream **appends** to it, so every other consumer of
that stream reads the retry as a new message, and a handler that fails three
times leaves three copies on the log for ever. With
`fixed_retry(3, timedelta(milliseconds=200))` on the connection, one published
message and a handler that always raises produces exactly that: attempts 1, 2
and 3 all sitting on the stream, and one copy in `{stream}.dlq`.

This is why `read_stream` has **no `retry` parameter**. It is not an oversight
and it is not a way of saying nothing retries — the connection's default policy
still reaches the consumer, because it is an ordinary consumer. It is that
naming the policy on `read_stream` would advertise a knob whose effect is to
corrupt the log, and hiding the surprise behind a parameter is worse than
leaving it where it can be read.

**So a failing handler's failure belongs to the handler.** Log it, copy it
somewhere, count it, and let the stream move on. The default connection policy
is [`no_retry()`](reliability.md#a-policy), which for a stream is the right
default and the reason the trap is not usually sprung.

### The queues a stream consumer declares

`read_stream` declares `{stream}.dlq` and `{stream}.parked` before subscribing,
the same as [any other consumer](consuming.md#what-starting-a-consumer-declares),
because the table above means it really can publish to both. Those are ordinary
quorum queues, and a message in one is a message a person has to look at.

Pass `declare=False` to stop it — for a login without `configure` permission, or
because you would rather a stream's failures went somewhere you chose. A consumer
started that way still publishes to those names if a handler rejects, so
somebody else has to have declared them.

### Depth does not mean anything on a stream

```python
await mq.message_count("events")     # 0, whatever is on the stream
```

The broker reports zero, because a message on a stream is not waiting for
anybody: depth is the count of messages nobody has taken yet, and on a stream
nobody ever takes one. It is not a bug and there is nothing to work around — it
is the number being meaningless rather than wrong. To know how far behind a
reader is, compare its offset with the newest one, which means recording the
offset yourself.

## Resuming where you stopped

The broker does not remember your position between runs. That is what makes
streams cheap, and it is what makes checkpointing your job. A reader restarted
with `from_first()` reprocesses everything; one restarted with `from_next()`
misses whatever arrived while it was down.

The offset of each delivery reaches the handler as an ordinary header:

```python
async def project(message: Message) -> Ack:
    offset = message.envelope.headers["x-stream-offset"]
    await apply(message.payload)
    await checkpoints.save("projection-a", offset)
    return accept()


consumer = await read_stream(
    mq, "events", project,
    offset=from_offset(await checkpoints.load("projection-a") + 1),
    consumer_name="projection-a",
)
```

It survives into `Envelope.headers` because the engine only strips the reserved
`x-acemq-` namespace, and `x-stream-offset` is the broker's rather than this
library's. Resume from **one more than** the last offset handled;
`from_offset(n)` is inclusive.

Saving the checkpoint after handling means a crash between the two replays the
last message. Saving it before means a crash loses one. Neither is avoidable
without a transaction spanning the broker and your store, so write the checkpoint
in the same transaction as the projection's own writes and the pair is exactly
once — anywhere else is at-least-once, which is fine if the handler is
[idempotent](patterns.md#idempotency).

There is no `last_handled_offset` on the consumer, and no `handled`, `failed` or
`skipped` counters. .NET reports all four and Java reports the first two; here
the offset is on the message and the counts are on
[`acemq.consume.total`](observability.md#what-is-reported), tagged with the queue
and the outcome.

## Testing

The [fake transport](testing.md) declares a stream and accepts a subscription,
so the declaration arguments, the offset argument and the prefetch are all
assertable without a broker:

```python
consumer = await read_stream(mq, "events", handler, offset=from_first())
assert transport.specs["events"].args == {"x-stream-offset": "first"}
```

What it does not have is stream *semantics*: nothing is retained, no second
reader sees what the first one read, and no offset comes back on a delivery.
Anything that turns on replay needs a real broker — which is the same position
Java takes, for the same reason.

## When to use one

Use a stream when more than one consumer needs the same messages, when history
has to be re-readable, or when a projection must be rebuildable from scratch:
event sourcing, audit logs, analytics fan-out.

Stay with a queue for work that is done once and finished. Consuming removes
nothing, so two workers reading one stream both do the same job — a stream is the
wrong shape for distributing work. And as the table above shows, the retry
ladder, the dead-letter queue and the parking lot do not mean on a stream what
they mean on a queue. Rebuilding those on top of one is how a simple job becomes
a distributed systems project.

## Requirements

Streams need RabbitMQ 3.9 or later. There is nothing to install: the
`x-queue-type` argument is all it takes, and this library sends it through the
ordinary AMQP connection rather than over the dedicated stream protocol — so the
per-message performance of the stream plugin's own client is not what you get,
and the replay, the retention and the independent positions are.

## Related

- [Patterns](patterns.md#streams) — the same thing in the pattern tour
- [Exchanges, queues and bindings](topology.md)
- [Consuming](consuming.md) — the `Consumer` `read_stream` returns
- [Retries, redelivery and shutdown](reliability.md) — what the table above is
  describing a stream's version of
