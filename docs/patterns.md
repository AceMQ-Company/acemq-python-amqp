# Patterns

`acemq_amqp.patterns` holds the things every service that consumes a queue ends
up writing for itself. Nothing here is new. Every service eventually grows a way
to ignore a message it has already handled, a way to publish and commit without
losing one, a way to ask a question and wait for the answer. They get written
again in each service, slightly differently, and the differences are where the
bugs live.

```python
from acemq_amqp.patterns import (
    ClaimCheckCodec, ConsumerGroup, InMemoryIdempotencyStore, Requester,
    RoutingSlip, Saga, Scheduler, chain, idempotent, ordered, replay, serve,
    with_timeout,
)
```

These mean the same as the Go library's, because a routing slip written by a Go
service has to be readable by a Python one. The API shape is Python's: Go takes
a `ctx` and returns an `error`, and this takes keyword arguments and raises.

Everything here is built out of the public library — handlers, envelopes,
publishers — so nothing is possible with a pattern that would not be possible
without it.

## Request and reply

```python
from acemq_amqp.patterns import Requester, serve

async with await Requester.open(mq, "pricing", "price.request") as ask:
    price = await ask.ask({"sku": "A-1"})
```

```python
async def price(message: Message) -> dict[str, Any]:
    return {"pence": look_up(message.payload["sku"])}


async with await serve(mq, "price-requests", price):
    ...
```

Messaging is asynchronous and request-reply is a synchronous shape drawn on top
of it, which is a real cost rather than a free convenience: a caller waiting on
a reply is holding a task, a connection and a deadline, and a responder that
slows down turns into a caller that stops responding. Reach for it where the
caller genuinely cannot continue without the answer, and publish an event
otherwise.

The pairing is the envelope's **correlation identifier**, and the return address
is an ordinary application header. Neither uses AMQP's own `reply-to` and
`correlation-id` properties: those are lost the moment a message passes through
a service that rebuilds it, and the envelope is the thing this library promises
to carry end to end.

`Requester.open` generates the reply queue — transient, exclusive,
auto-deleting and classic, belonging to this process. Name one only when replies
must survive a restart; a named reply queue that outlives its requester collects
answers nobody is waiting for.

`ask` raises `RequestTimeoutError` when nothing came back in time, and
`ResponderError` when the responder said it could not do it. That distinction is
the reason a responder's failure is sent back rather than swallowed: a caller
blocked on a reply should learn that it failed in milliseconds rather than wait
out its whole timeout to learn nothing. The default timeout is 30 seconds, and
`ask(..., timeout=...)` overrides it for one call.

`serve` returns an ordinary `Consumer`, which already knows how to be closed and
how to be an `async with`.

## Idempotency

```python
from acemq_amqp.patterns import InMemoryIdempotencyStore, idempotent

store = InMemoryIdempotencyStore()
consumer = await mq.consume("orders", idempotent(store, place))
```

At-least-once delivery is not a flaw to be worked around; it is the only
guarantee a broker can give cheaply, and every AceMQ retry is a redelivery on
purpose. So a handler that changes anything needs a way to tell that it has seen
a message before. `x-acemq-id` is stable across every redelivery of the same
message, which makes it the natural key.

A duplicate is **accepted**, not rejected. The work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right.

When the handler does *not* accept, the key is forgotten so the retry can
actually run. That ordering is what makes this a guard against duplicates rather
than a promise of exactly-once: between the handler finishing and the
acknowledgement reaching the broker there is still a gap where a crash leaves a
message that will be delivered again. **Only a store written in the same
transaction as the work closes it**, which is why `IdempotencyStore` is a
protocol and not a class:

```python
class IdempotencyStore(Protocol):
    async def first_time(self, key: str) -> bool: ...
    async def forget(self, key: str) -> None: ...
```

Where the natural key is in the payload rather than the envelope — an order
identifier that two different messages both carry, where handling either twice
is the thing to prevent — pass one:

```python
idempotent(store, place, key=lambda message: message.payload["orderId"])
```

## The outbox

A service that commits a row and then publishes has two things that fail
independently. Commit and then crash, and the message never goes: the order
exists and nothing downstream knows. Publish and then fail to commit, and the
message is about an order that does not exist. No amount of care with the
ordering removes the gap, because there are two systems and no transaction
spanning them.

The outbox removes it by having only one system:

```python
from acemq_amqp.patterns import OutboxRelay, record

async with database.transaction() as tx:
    await place_order(tx, order)
    await store.add(record(mq, "orders-events", "order.placed", event))
```

```python
relay = OutboxRelay(mq, store, interval=timedelta(seconds=1), batch=100)
relay.start()
...
await relay.close()
```

The message is written into the same transaction as the work, so the record and
the row commit together or neither does, and the relay publishes what was
committed.

`record` encodes there and then rather than in the relay. The record has to
survive a deployment that changes the class the payload was written from, so
what is stored is bytes and a content type; the relay is then a thing that moves
bytes and needs to know nothing about what they mean.

The relay is **at-least-once by construction**: a record is removed only after
the broker has confirmed it, so a crash in between sends it twice. Removing it
first would lose it instead, and a repeated message is a problem a consumer can
solve while a lost one is not — which is what [idempotency](#idempotency) is
for.

`start` twice is a no-op rather than a second sweeper. Two relays on one store
publish everything twice, and the shape of that mistake is a service that starts
one relay per worker. `await relay.sweep()` publishes one batch on demand, for
flushing at the end of a request rather than up to a second later, and for
driving the relay in a test without waiting for a tick. It returns how many went
out, and raises whatever the store or the broker raised — with the records still
in the outbox, which is the entire point.

`OutboxRelay` is an async context manager too.

## Ordering

A queue delivers in order and a consumer with `concurrency` above one stops
honouring that. Usually the right trade; wrong where a later message about the
same thing must not overtake an earlier one — an "order cancelled" handled
before the "order placed" it cancels leaves an order that exists and should not.

The answer is not to give up concurrency, because the messages that must not
overtake each other are only the ones about the same entity:

```python
from acemq_amqp.patterns import by_correlation, by_header, ordered

consumer = await mq.consume(
    "orders", ordered(by_header("order-id"), handle), concurrency=16
)
```

`by_header(name)` orders by an application header — a tenant, a customer, an
aggregate. `by_correlation()` orders by correlation identifier, which keeps one
business action's messages in sequence.

**What it does not do.** It orders the handling of messages that have already
been delivered. It cannot reorder ones the broker delivered out of order, and
with several consumers on one queue it orders only *within each process*.
Ordering across processes needs the messages to reach the same process in the
first place, which is a routing decision rather than a handler one:

```python
from acemq_amqp.patterns import partition, partitioned_routing_key

partition("o-1", 8)                          # 0
partitioned_routing_key("orders", "o-1", 8)  # 'orders.0'
```

FNV-1a rather than `hash`, because Python's string hash is randomised per
process by default: two workers in the same deployment would disagree about
where a key belongs, which is the one thing a partition function must not do.
The same arithmetic in every AceMQ library, so a Go publisher and a Python
consumer agree about which partition an entity is in.

A message whose key is empty is handled with no ordering at all. That is
deliberate over treating "no key" as one shared key, which would serialise every
message missing the header — turning a producer's bug into a consumer's outage.

## Pipelines

Two ideas that belong together.

**A chain** puts the cross-cutting concerns around one handler in an order you
can read:

```python
from acemq_amqp.patterns import chain, with_idempotency, with_logging, with_timeout

handler = chain(
    handle,
    with_logging(),
    with_timeout(timedelta(seconds=10)),
    with_idempotency(store),
)
```

Outermost first, so logging records what the deadline and the idempotency guard
decided rather than only what the handler did. `with_ordering(key)` is there
too.

`with_timeout` reports a handler that runs past its deadline as a **retry**
whatever it would have said, which is a deliberate choice between two imperfect
answers: retrying work that may have succeeded risks doing it twice, and
accepting work that may have failed loses it. An `async` handler is really
cancelled at the deadline. A blocking handler running on a worker thread — which
is what [the sync API](getting-started.md#not-running-an-event-loop) gives it —
cannot be: the message is settled as a retry on time, but the thread carries on.
Worth knowing before putting a short deadline in front of one.

`with_logging` logs the **decision**, not the payload. A payload is the one
thing on a message that may not be safe to write down, and a log line that is
unsafe to keep is one somebody eventually turns off.

**`then`** makes a pipeline out of several services: each consumes, does its
part, and publishes what comes out, carrying the correlation forward so the
whole run can be followed afterwards.

```python
from acemq_amqp.patterns import NOTHING, then

handler = then(
    mq.publisher("shipping-events", "shipment.requested"),
    lambda message: NOTHING if message.payload["digital"] else ship(message),
)
```

Returning `NOTHING` publishes nothing and accepts the message, which is how a
step says "this one does not continue" without inventing an empty message for
the next service to handle.

The message is accepted only once the next one is out, so a publish that fails
retries the step and the work runs again — which is why a step that changes
anything should be idempotent.

There is deliberately **no** middleware for turning an exception into a
decision. An exception escaping a handler is already a retry — it is how Python
says a thing failed, and most failures it carries are transient — so a wrapper
that caught them could only make that worse.

## Replay

The pattern somebody actually needs at three in the morning: a dead-letter queue
has two thousand messages in it, the bug is fixed, and they have to go back
through — but not all of them, not silently, and not in a way that cannot be
stopped half way.

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

The reason comes back with the count, because "moved 500" means something quite
different when the limit was 500. `drained`, `limit`, `deadline` or `failed`.

Each message is stamped with `acemq-replayed-from`, `acemq-replayed-at` and
`acemq-replay-count`, so a consumer that needs to treat a replayed message
differently can, and one that does not is unaffected.

`restart=True`, the default, **resets the attempt counter** and clears the error
that dead-lettered it. A message that arrives back on attempt five of a
five-attempt policy is dead-lettered again before a handler sees it, and the
operator who has just fixed the bug has moved two thousand messages from one
queue to the same queue. `restart=False` puts back exactly what was there.

`routing_key=None`, the default, keeps each message's own, so a message goes
back where it came from rather than everywhere.

Messages the filter declines are held unsettled until the pass is over rather
than returned one at a time. A rejected message goes back to the *head* of the
queue, which is what RabbitMQ does, so returning one immediately means reading
the same message for ever and never seeing what is behind it.

A failure raises `ReplayError` carrying the partial result: whatever had already
been moved has been moved, and the rest is still on the queue. Each message is
republished *before* the original is acknowledged, so a failure in the gap
replays one twice rather than losing it — the right way round for a queue
somebody is putting back by hand.

## Routing slips

Each service does its part and sends the message to the next stop written on the
slip, so the route is decided once — by whoever started the work — and travels
with the message rather than living in a component every service has to reach.

```python
from acemq_amqp.patterns import RoutingSlip, follow_slip, start

slip = (
    RoutingSlip()
    .then("orders-events", "order.validate", name="validate")
    .then("orders-events", "order.charge", name="charge")
    .then("orders-events", "order.ship", name="ship")
)
await start(mq, slip, order)
```

```python
consumer = await mq.consume("charge-queue", follow_slip(mq, charge))
```

What it costs is that no single place says what the whole route is while it is
running, so a route that is wrong is discovered one hop at a time. Worth it when
the steps vary per message: an order over a threshold visits an approver, a
document goes to whichever reviewer owns it. Not worth it when every message
goes the same way, where a fixed chain of consumers is simpler.

The slip travels as JSON in an application header rather than in the payload,
because a step that rewrites the payload must not be able to lose the itinerary
by accident. `slip_from(envelope)` reads it, returning `None` when there is
none and raising `FatalError` when there is one that cannot be parsed — fatal,
because a slip that will not parse will not parse on the next attempt either.

`RoutingSlip` is immutable and `advance()` returns a new one: a slip is on a
message that has already been published by the time anybody reads it, so
changing one in place would describe a journey that did not happen. `done`
carries what has already happened, oldest first, so a slip that fails halfway
says how far it got.

The message is accepted only once the next one is out, which is again why a step
that changes anything should be idempotent.

## Sagas

Several things that must all happen, across systems that share no transaction.

```python
from acemq_amqp.patterns import Saga

saga = (
    Saga("place order")
    .step("reserve stock", reserve, release)
    .step("charge card", charge, refund)
    .step("notify", notify)
)

result = await saga.run(order)
if result.has_unresolved:
    alert(result)
```

Each step has a name, an action and — usually — something that undoes it. The
steps run in order; when one fails, the ones that already completed are
compensated in **reverse**, because that is the order the world was changed in
and a compensation often depends on state a later step has not yet altered. The
refund has to happen before the stock goes back, not after it.

A step may be a coroutine, because a step that publishes a message will be, and
a saga is where "reserve the stock" and "publish the reservation" sit next to
each other.

`run` **returns rather than raises**. A failed saga is not an exceptional
condition to a caller that has to decide what happens next, and the interesting
part is not the exception:

| | |
|---|---|
| `complete` | every step ran |
| `compensated` | one failed and the earlier ones were undone |
| `failed_at` | which step |
| `failure` | what it raised |
| `completed` | the steps that ran, in order |
| `unresolved` | the steps whose compensation *also* failed |
| `has_unresolved` | whether there are any |

**`has_unresolved` is the flag to alert on.** Everything else a saga reports is
recoverable by construction. Those are real-world effects that happened, were
meant to be undone, and were not: nothing else in the system knows about them,
no retry resolves them, and a person has to.

Two decisions are worth knowing about because they are not obvious.

**A compensation that fails does not stop the others.** It is logged at `error`,
its step name goes into `unresolved`, and compensation carries on down the list.
Stopping at the first failure would leave more undone than continuing — the
label would stay printed and the stock stay reserved because the refund could
not be reached.

**A completed step with no compensation is skipped, not an error.** A step that
only read something needs no undo. The cost of that leniency is that a step
which *should* have had one looks identical, which is the argument for writing
the compensation first and the action second.

Nothing here touches a broker. A saga is arithmetic over functions, and it is a
pattern for a single process that orchestrates several systems — not a
distributed coordinator. If the process dies halfway, nothing resumes it; the
half-finished work is what an outbox and idempotency at the far end are for.

## Delivering a message later

```python
from acemq_amqp.patterns import Scheduler

async with await Scheduler.open(mq) as scheduler:
    await scheduler.after(timedelta(hours=4), "billing", "invoice.due", invoice)
    await scheduler.at(renewal_date, "policies", "policy.renew", policy)
```

`at` takes a moment and refuses a naive `datetime`: one would be read as the
local time of whichever machine scheduled it, and a scheduler is the last place
to discover that two of them disagree.

### Why not a per-message time to live

The obvious implementation is to set an expiration on the message, drop it in a
queue nobody consumes and let it dead-letter to its destination. It is what most
articles suggest and it is wrong for anything but a single fixed delay, because
**a classic queue expires messages only at its head**.

Put a four-hour message in, then a one-minute message behind it, and the
one-minute message is delivered in four hours. Nothing reports this: the queue
looks healthy, the message is not lost, it is simply late by a factor nobody
predicted. It fails in production under mixed load rather than in testing under
uniform load.

### The ladder

Five queues, each with a *uniform* time to live, and a message hops through them
until it is due:

```
acemq.schedule.1h  acemq.schedule.10m  acemq.schedule.1m
acemq.schedule.10s  acemq.schedule.1s
```

Every message in a rung has the same delay, so the head is always the one due
soonest and head-of-line expiry is harmless. Each expiry dead-letters the
message into `acemq.schedule.due`, where the scheduler works out what is left
and either drops it into the largest rung that does not overshoot or delivers
it. A four-hour delay is four one-hour hops; a ninety-second delay is one
minute, then three tens; a one-day delay is twenty-four hops and a one-minute
delay is one, which is the right way round — short delays are common and want
to be cheap.

The cost is honest and worth stating: a long delay is several broker round trips
rather than one, and delivery is accurate to about the smallest rung rather than
to the second. A scheduler that must fire at 09:00:00.000 exactly is a
scheduler, not a message broker. The alternative is RabbitMQ's
delayed-message-exchange plugin, which does this properly and is a plugin — so a
library that silently required it would be a library that works on your laptop.

### What it puts on the broker

`schedule_topology()` is the whole of it, and every name and argument is shared
with the Java library. Two services scheduling on one broker declare the same
queues, so a difference would not be a difference in behaviour — it would be a
`PRECONDITION_FAILED` on whichever of them started second.

```
exchange  acemq.schedule            direct, durable
queue     acemq.schedule.1h         classic, x-message-ttl=3600000
queue     acemq.schedule.10m        classic, x-message-ttl=600000
queue     acemq.schedule.1m         classic, x-message-ttl=60000
queue     acemq.schedule.10s        classic, x-message-ttl=10000
queue     acemq.schedule.1s         classic, x-message-ttl=1000
queue     acemq.schedule.due        classic, no arguments
```

Every rung carries exactly three arguments — the TTL,
`x-dead-letter-exchange: acemq.schedule` and
`x-dead-letter-routing-key: acemq.schedule.due` — and every queue is bound to
`acemq.schedule` under its own name. Classic is spelled by leaving
`x-queue-type` off entirely, which is what the other libraries send and
therefore the only spelling a broker finds equivalent to theirs.

Declare it yourself from a migration if your services run with a login that has
no `configure` permission; `Scheduler.open` applies it otherwise.

### What travels with the message

Four application headers, and deliberately **not** under the `x-acemq-` prefix:
that namespace belongs to the engine, `Envelope` refuses it outright, and a
scheduler header using it would be written on publish and gone on consume.

| | |
|---|---|
| `x-schedule-exchange` | where it is going |
| `x-schedule-routing-key` | what it will be published under |
| `x-schedule-due-at` | when it is due, as epoch milliseconds |
| `x-schedule-content-type` | what the payload was encoded as |

The content type is carried because the scheduler republishes **bytes**. The
payload is encoded once, when it is scheduled, and moved unchanged from then on
— the control consumer reads raw bytes with `BytesCodec` and never decodes a
payload, because a scheduler that decodes acquires opinions about message
formats it has no business having. Publishing pre-encoded bytes under
`application/octet-stream` would produce a message the intended consumer cannot
decode: it arrives, it is the right bytes, and nothing can read it.

None of the four is passed on to the destination. They are bookkeeping, and a
consumer that started depending on them would be depending on how a message got
to it. What the consumer sees is an ordinary message of type `ScheduledMessage`.

### The control consumer declares nothing

A consumer normally declares its dead-letter queues when it starts. This one is
opened with `declare=False`, because `acemq.schedule.due` is a shared name:
without it, every service running a scheduler would leave `acemq.schedule.due.dlq`
and `acemq.schedule.due.parked` on the broker — two durable queues that nothing
publishes to and nobody ever reads. Everything it needs is in
`schedule_topology()`, which `Scheduler.open` has already applied.

A message that reaches the control queue without the headers a scheduled message
carries is rejected rather than retried: no number of attempts adds a header,
and something else publishing into the scheduler's queues is a bug rather than a
delivery to guess a destination for.

`scheduled`, `delivered` and `hops` say what it has done. `hops` divided by
`delivered` is the average number of hops, which is the number to look at when a
scheduler is busier than expected — long delays cost hops.

## Consumer groups

```python
from acemq_amqp.patterns import ConsumerGroup

async with await ConsumerGroup.start(mq, "orders", 4, handle):
    ...
```

Two things it saves. Starting consumers by hand means remembering to close every
one, and a partial shutdown leaves messages held by a consumer nobody is waiting
for. And a group can be sized from configuration, which is the number most often
changed after a service is already running.

If one fails to start, the ones already running are closed before the failure is
raised. A half-started group is worse than none: it holds messages nothing is
going to finish handling, and the caller that saw the exception has no handle to
close it with.

**Concurrency, or a group?** `concurrency` runs several handlers on one consumer
and one channel, sharing its prefetch. A group runs several consumers, each with
its own channel and its own prefetch. Reach for a group when the handlers are
slow enough that one channel's prefetch is the limit, or when a fair share
across processes matters: the broker round-robins between consumers, so four
here compete evenly with four in another instance rather than one process taking
half the queue.

Each consumer gets its own tag, numbered from 1, so the management interface
shows which one is holding a message rather than four identical rows.

## Schema registry

A producer and a consumer have to agree about what a message means. They do not
have to agree about which *version* of it they are on, and insisting that they
do is what turns adding a field into a deployment everyone has to attend.

```python
from acemq_amqp.patterns import InMemorySchemaRegistry, fingerprint

registry = InMemorySchemaRegistry()
schema = await registry.register("order.placed", "json-schema", definition)

schema.id            # what a message carries
schema.version       # 1, 2, 3 ... per subject
schema.fingerprint   # SHA-256 of the exact bytes

await registry.by_id(schema.id)
await registry.latest("order.placed")
await registry.versions("order.placed")
```

The shape is stored once and the message carries a small identifier; a consumer
that meets a version it does not know looks it up rather than failing. That is
what lets a producer add a field on Tuesday and the last consumer catch up in a
fortnight.

The registry is deliberately **incurious about what a schema says**. It stores
the text, hashes it, and hands it back. Interpreting Avro, Protobuf or JSON
Schema is a job for a library that knows one of them, and a registry that
half-understood all three would be wrong in three ways.

`fingerprint` is SHA-256 of the exact bytes, the same in every language. Two
definitions differing only in whitespace hash differently and are therefore two
schemas — strict, and the safer half of the trade: normalising first would need
a parser per format, and a registry that treated two definitions as one because
it mis-parsed them would be worse than one that is merely fussy.

`by_id` and `latest` raise `SchemaNotFoundError`.

## Streams

A queue forgets a message when it is acknowledged, so exactly one consumer ever
sees it and nobody can look at it again. A stream keeps everything until its
retention policy discards it, so several consumers read the same stream
independently and a new one can start from the beginning.

```python
from acemq_amqp.patterns import (
    StreamRetention, declare_stream, from_first, read_stream, stream,
)

await declare_stream(mq, "events", StreamRetention(max_age=timedelta(days=7)))
consumer = await read_stream(mq, "events", project, offset=from_first())
```

`stream(name, retention)` returns a `Topology` instead, so a stream can be
declared alongside everything else rather than through a call of its own.

**What changes is what an acknowledgement means**, and it is the thing to
understand before using one. Acknowledging does not remove the message; it
advances *this* consumer's position. And rejecting does not dead-letter, because
there is nothing to remove the message from. A message a handler cannot deal
with has to be dealt with by the handler — logged, copied elsewhere, counted —
and the stream moves on regardless. Nothing is lost, and nothing is retried for
you.

The retry policy is therefore not a parameter on `read_stream`. Retrying on a
stream means republishing to it, which appends a second copy for every other
consumer to read as well.

Where to start:

| | |
|---|---|
| `from_next()` | the next message published. The default |
| `from_first()` | the oldest message the stream **still holds** — a retention policy means that is not necessarily the first ever written |
| `from_last()` | the last chunk |
| `from_offset(n)` | a specific offset |
| `from_timestamp(when)` | the first message published at or after a time |

`StreamRetention` is unbounded by default, which for a stream means until the
disk is full. Set at least one of `max_age`, `max_bytes` or `segment_bytes` on
anything that will run for longer than an afternoon. Retention happens a whole
segment at a time, so a very large segment makes retention coarse.

`prefetch` cannot be zero — RabbitMQ refuses a stream consumer without one — and
defaults to 10. `consumer_name` is what makes server-side offset tracking
possible. `concurrency` is 1 by default, because a stream's order is usually why
it is a stream.

`Connection.message_count` on a stream says how many messages are **retained**,
not how many are outstanding, for the same reason: there is no such thing as
outstanding on a stream.

## The claim check

A forty-megabyte message is possible and is a mistake: it fills the broker's
memory, it is copied to every bound queue, it makes a dead-letter queue
impossible to inspect, and it turns a broker into a filesystem with worse tools.
What travels instead is a **claim check** — the payload goes to a store and the
message carries the key.

```python
from acemq_amqp.codec import JsonCodec
from acemq_amqp.patterns import ClaimCheckCodec, FilesystemClaimCheckStore

store = FilesystemClaimCheckStore("/mnt/payloads")
mq = await connect(url, codec=ClaimCheckCodec(JsonCodec(), store))
```

That is the whole of it. `ClaimCheckCodec` is a `Codec`, so it goes wherever a
codec goes: on the connection, on one publisher, on one consumer.

**Only when it is worth it.** Below the threshold the payload travels inline,
exactly as it would without this codec. That matters more than it sounds:
offloading a two-hundred-byte message turns one broker round trip into a store
round trip *and* a broker round trip, so an unconditional claim check makes the
common case slower to fix the rare one. The default threshold is
`DEFAULT_THRESHOLD`, 64 KiB — comfortably above an ordinary event and comfortably
below the size at which a broker starts to care. Pass `threshold=` to change it;
`threshold=0` offloads everything, which is occasionally what a store-backed
audit trail wants.

### What is on the wire

Three bytes, then either the payload or the key:

```
0xAC  0x01  0x00  payload      inline, and identical to what the delegate wrote
0xAC  0x01  0x01  key          a claim check
```

Byte for byte what the Java library writes, so a document a Java service put
aside is one a Python service can read. Two things about that are worth being
explicit about, because getting either wrong means the two languages cannot
exchange a large message even though both "have claim check":

- **The key is bare.** No scheme, no `acemq://`, no wrapper — whatever the
  store's `put` returned, in UTF-8. A key is meaningful only to the store that
  issued it, so dressing it up as a URI would be inventing an authority nobody
  reads.
- **A consumer decides from the body, never from a header.** The reserved
  `x-acemq-claim` header is for an application that wants to say where a payload
  went in a form an operator can read; it is not what this codec dispatches on.
  Headers get dropped by shovels, federation links and plugins, and a message
  whose body is a key but whose header went missing would be handed to a JSON
  parser as if it were a document.

A body this codec did not write — no magic, wrong version, an unknown third byte
— goes to the delegate untouched. That is what makes it safe to introduce on a
queue that already has messages in it, and safe to change the threshold
afterwards.

```python
from acemq_amqp.patterns import claim_key_of, is_claim_check

is_claim_check(message_body)   # is the payload somewhere else?
claim_key_of(message_body)     # which object does it need? None when inline
```

For the operator looking at a dead-letter queue, that one line is the difference
between a five-minute check and restoring a backup.

### Retention is the part that goes wrong

The store and the queue have different lifetimes and nothing enforces a
relationship between them. A message replayed a month later carries a key, and
if the store expired that key the replay produces a message nobody can read —
**worse than a lost message, because it looks like a message** and fails deep
inside a consumer rather than visibly. A redeemed claim that is not there raises
`FatalError`, which dead-letters the message rather than retrying it forever.

So the store's retention must exceed every retention that could bring a message
back: queue TTLs, dead-letter queues, and however long somebody might sit on a
message before replaying it by hand. When in doubt, longer. The codec never
calls `delete` for the same reason: deleting on read breaks the second consumer,
deleting on acknowledgement breaks a replay, and when a payload may be removed
is a decision that belongs to whoever owns the data.

### The stores

`ClaimCheckStore` is three synchronous methods — `put`, `get`, `delete` — so a
store over S3 or Azure Blob Storage is a small class. It is synchronous because
`Codec` is: a codec is called from inside both the async and the blocking API,
and a store that had to be awaited could not serve the second.

| | |
|---|---|
| `InMemoryClaimCheckStore` | the payloads are held in the publisher's own memory, which is where they were going to be anyway — so this takes them off the broker and does nothing else. A consumer in another process gets "the claim check is not in the store". For tests |
| `FilesystemClaimCheckStore` | a shared, durable mount: NFS, a persistent volume. Writes go to a temporary file and are moved into place, so a consumer fast enough to read the key before the writer finished sees the whole payload or none of it. On a container's *local* disk it is the in-memory store with extra steps |

`FilesystemClaimCheckStore` checks a key rather than trusting it — a key becomes
a path segment, and `../../etc/passwd` is a key too. It accepts exactly what
Java's accepts, so the two can share one mount.

## Stores that survive a restart

Three of these patterns have a storage seam with an in-memory implementation:
`IdempotencyStore`, `OutboxStore` and `SchemaRegistry`. Each of the in-memory
versions says in its own docstring why it is not the one to use in production,
and it is worth repeating here:

| | |
|---|---|
| `InMemoryIdempotencyStore` | right behind one consumer, wrong the moment there are two — each has its own memory, so both believe they are first. Lost on restart, which turns every message in flight into a duplicate |
| `InMemoryOutboxStore` | has none of the property the pattern exists for. Nothing here shares a transaction with a database, so a crash between the work committing and the record being written loses the message exactly as publishing directly would |
| `InMemorySchemaRegistry` | not shared between processes, so a consumer cannot look up a schema a producer registered somewhere else — which is the entire point of having one |

They are for tests, and for seeing the shape of the thing before writing the
version that matters. Each seam is a `Protocol` with two to four methods, so the
real one is a small class over whatever database the service already has — and
for the outbox and the idempotency store, it must be *that* database, in *that*
transaction, or the gap the pattern exists to close is still open.

That class ships, in `acemq_amqp.patterns.sql`:

```python
import sqlite3

from acemq_amqp.patterns import (
    SqlIdempotencyStore, SqlOutboxStore, SqlSchemaRegistry, create_schema,
)

connections = lambda: sqlite3.connect("acemq.db")   # or a pool's checkout

create_schema(connections)                          # development; see below
outbox = SqlOutboxStore(connections)
seen = SqlIdempotencyStore(connections)
registry = SqlSchemaRegistry(connections)
```

**No driver dependency.** Nothing in the module imports a database driver. It is
written against the DB-API 2.0 connection and cursor protocols, which every
Python driver already implements, so one class serves `sqlite3`, psycopg and
anything else following PEP 249. The one thing that differs is the placeholder,
and that is a constructor argument: `paramstyle="qmark"` for `sqlite3` — the
default — and `paramstyle="format"` for psycopg.

**What has been run.** The automated suite exercises `sqlite3`, which is in the
standard library, so the tests need nothing installed. The same checks —
`create_schema`, the rollback, the round trip through `bytea`, the claim race,
the lease take-over, four registries registering one schema at once — have been
run **by hand against PostgreSQL 17 through psycopg 3**, and they pass. They are
not in the suite, because a suite that needs a database server is a suite that
gets skipped, and a skipped test proves nothing. So: sqlite3 on every commit,
PostgreSQL when somebody runs it. `pip install "acemq-amqp[postgres]"` installs
psycopg for a project that has not chosen a driver, and nothing more. Anything
else — MySQL, SQL Server, Oracle — needs at least a different upsert, and is not
claimed.

### The outbox, and the transaction it joins

This is the one that matters, because the in-memory store admits it does not
have the property the pattern exists for.

```python
async with database.transaction() as tx:
    await place_order(tx, order)
    await outbox.add(
        record(mq, "orders-events", "order.placed", event),
        connection=tx.connection,
    )
# one commit; the order and the message are the same decision
```

`add` writes on the connection **you** hand it, and does not commit it, does not
roll it back and does not close it. That is not an oversight — it is the
guarantee. The insert becomes durable exactly when your transaction does, and if
you roll that transaction back the message is not in the outbox, because it never
was: it was one insert among yours. There is a test that does exactly that and
then looks.

The connection can come from either end. Pass `connection=` at the call site,
which is clearest where the transaction is a local variable; or give the
constructor a `transaction=` callable and let it fetch the connection bound to
the current transaction, which is what a framework with a thread-local or
context-local session wants. What it must **never** be is "open a new
connection". A fresh connection with autocommit on inserts the row immediately
and independently, so a later rollback of the business work leaves a message
queued for something that never happened — the exact fault the pattern was
adopted to prevent, now harder to notice because the code looks right. With no
transaction to join, `add` raises rather than opening one.

The relay's own work — `pending` and `mark_published` — is not your transaction
and has no business joining it, so it uses the `connections` factory and commits
for itself.

**One relay per outbox.** `pending` takes no lease, because the `OutboxStore`
protocol has nowhere to put one, so two processes sweeping the same table publish
everything twice. Java's JDBC store leases rows for this reason; this one does
not, and the shape of the mistake is a service that starts a relay per worker.

### A claim is a lease, not a fact

`SqlIdempotencyStore` writes a row that starts `CLAIMED`, with a deadline
`claim_timeout` away; `confirm` turns it into `CONFIRMED` with a deadline
`retention` away, and `idempotent(...)` calls that for you when a handler
accepts. The insert *is* the claim, and the primary key is what makes it atomic
across every process using the table — two consumers racing on one message
cannot both be told they are first, without a lock anybody has to remember to
take.

The lease is what a fact cannot do. A consumer that dies holding a message would
otherwise have recorded it as handled without handling it, and the redelivery —
the thing at-least-once delivery is *for* — would be skipped. So a hold that has
run out can be taken over, and the message gets handled by somebody.

Nothing on the message path deletes anything: schedule `purge_expired`, hourly
is ample. A store that tidies up on the hot path makes every message pay for it.

`confirm` is duck-typed rather than a third method on `IdempotencyStore`, so a
store that does not hand out leases — `InMemoryIdempotencyStore`, or your own —
is unaffected and nobody has to implement a method meaning "nothing".

### Tables, and who owns them

`create_schema(connections)` creates whichever of the three tables you ask for.
It is for development and for tests. In production they belong in whatever
migration tool already owns the schema — the outbox table especially, because it
has to live in the same database as the business tables it commits with, and
that database's shape is not a messaging library's to change at start-up. So the
DDL is printable:

```python
from acemq_amqp.patterns import schema_ddl

for statement in schema_ddl(dialect="postgres"):
    print(statement + ";")
```

Table names are constructor arguments, and are checked rather than trusted: a
table name reaches SQL by concatenation because no database binds one as a
parameter, so anything that is not a plain identifier is refused.

Times are stored as **epoch milliseconds** in a `BIGINT` — the same units as
`x-acemq-first-seen` on the envelope, and the one representation of an instant
that every database and every driver agrees about without a per-driver
conversion. It reads worse in `psql` than a timestamp column would, and it is
the same number everywhere, which is worth more.

The tables are not a cross-language contract. Java's `JdbcOutboxStore` stores a
message type and a string payload where this stores a body, a content type and
rendered headers, because the two libraries' `OutboxRecord` are not the same
record. What crosses languages is the message on the broker, which is identical;
a Java relay draining a Python outbox table is not something either side
supports.
