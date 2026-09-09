# Interceptors

Every organisation has something every message needs and no library can guess: a
tenant identifier, a trace context, an authorisation token, a schema version, a
correlation identifier in the logging context, a timer, a size limit.

Without a seam these end up copied into every call site, where one of them is
eventually forgotten and nobody finds out until the message that needed it is
the one that went without it.

## One function, handed the message and the rest of the work

```python
import time

from acemq_amqp import Ack, ConsumeContext, ConsumeNext, PublishContext, PublishNext


async def stamped(context: PublishContext, send: PublishNext):
    context.set_header("tenant", tenant.current())
    return await send(context)


async def timed(context: ConsumeContext, handle: ConsumeNext) -> Ack:
    started = time.monotonic()
    try:
        return await handle(context)
    finally:
        log.info("%s took %.3fs", context.queue, time.monotonic() - started)
```

Register them on the connection:

```python
mq = await connect(url, on_publish=[stamped], on_consume=[timed])
```

Or afterwards, which still reaches publishers that already exist:

```python
mq.intercept_publish(stamped).intercept_consume(timed)
```

Both return the connection, so they chain. Reading the interceptors per message
rather than at construction is what makes the second form work: a publisher
built during start-up picks up an interceptor registered after it.

## Why this shape

Java has a pair of before-and-after hooks. Python already has the construct:
`try`/`finally` runs the way out in the reverse of the way in, nests correctly
without anybody having to reverse a list, and makes an interceptor that opens
something and closes it **one** function instead of two halves that have to
agree about a key in a map.

It composes the way ASGI middleware does, which is the arrangement a Python
programmer already knows.

## Order

Interceptors run in the order they were registered. **First registered is
outermost**: it sees the message first on the way in and last on the way out.

```
stamped ──► authorised ──► sized ──► the publish
        ◄──           ◄──        ◄──
```

That matters as soon as one of them reads what another wrote. A tenant
interceptor that must run before an authorisation interceptor is registered
first.

## What a publish interceptor can touch

`PublishContext` is a mutable dataclass — mutable rather than
rebuilt-and-returned, because an interceptor that only wants to add one header
should not have to reconstruct the rest, and one that reads what an earlier
interceptor wrote needs to be looking at the same object rather than a copy of
an earlier version.

| | |
|---|---|
| `exchange` | where it is going. Changing it redirects the message |
| `routing_key` | what it is published under |
| `envelope` | the metadata. Reserved names are still refused when it is rendered, whoever wrote them |
| `payload` | what is about to be encoded, **before the codec sees it** |
| `persistent` | whether the broker is asked to write it to disk |
| `mandatory` | whether reaching no queue is an error rather than a silence |
| `set_header(name, value)` | add one application header, without rebuilding the envelope |

`payload` being visible before the codec runs is the point of putting the chain
where it is. An interceptor can change the message and not only its metadata — a
field redacted, a body encrypted, a claim check substituted for a payload too
big to send — where a codec that had already run would leave it holding bytes
and nothing it could do with them.

## What a consume interceptor can touch

`ConsumeContext`, likewise mutable:

| | |
|---|---|
| `queue` | what it was read from |
| `envelope` | the metadata **as it will reach the handler**. Changing it changes what the handler is given |
| `payload` | the decoded body, likewise |
| `body` | the undecoded bytes, for an interceptor that wants to see what actually arrived |
| `content_type` | what the sender said the body was, or `None` |
| `routing_key` | the key it arrived under |
| `redelivered` | the broker saying it has handed this one over before |
| `state` | somewhere to leave something for a later interceptor, or for your own way out. This library never reads it |

Whatever the interceptors left is what gets used. An envelope rewritten on the
way in is the one the handler is given **and** the one that gets dead-lettered,
so an operator reading `{queue}.dlq` is not missing the very field the
interceptor exists to add.

## Knowing how the delivery really ended

The `Ack` an interceptor sees on the way out is what the handler *asked for*,
which is not always what happened. A handler asking for another attempt when
there are none left is dead-lettered, and an interceptor that recorded the `Ack`
would report a retry that never happened — which is exactly how a trace backend
ends up with no dead letters in it and a dead-letter queue that is full.

`when_settled` closes that gap:

```python
async def audited(context: ConsumeContext, handle: ConsumeNext) -> Ack:
    def settled(settlement: Settlement) -> None:
        if settlement.dead_lettered:
            audit.record(context.envelope.id, settlement.reason)

    context.when_settled(settled)
    return await handle(context)
```

A `Settlement` carries the `outcome` — `acked`, `retried`, `rejected`,
`dead_lettered` or `parked` — the `reason` it was set aside for, and the `delay`
before the next attempt. Its `dead_lettered` property is true for a rejection as
well: both end up in the same queue, and the difference between the two words is
who decided. It is **false** for `parked`, which went to a different queue on
purpose; `settlement.parked` is the property for that one.

The listener is called **once**, on the consumer's task, after the decision and
before it is carried out. Before, so that a consumer-side backoff is not
something you are made to wait through. `when_settled` returns whether anything
will ever call it — `False` when the chain is being run by something other than
a consumer, so nothing should be waiting on an answer that is not coming. A
listener that raises is logged and otherwise ignored; the delivery still has to
be settled.

This is what the tracing adapter uses, and the reason a message that ran out of
attempts has a span saying `dead_lettered` — see
[observability](observability.md#what-the-consumer-did-rather-than-what-the-handler-said).

## Refusing is raising

A **publish** interceptor that raises stops the publish, and the caller sees the
exception. That is the whole point of being able to intercept rather than only
observe: a message that must not go out can be stopped in one place rather than
in every publisher.

```python
async def within_limits(context: PublishContext, send: PublishNext):
    if len(str(context.payload)) > 128 * 1024:
        raise ValueError(f"{context.routing_key} is too big to publish")
    return await send(context)
```

A **consume** interceptor that raises is treated exactly as a handler that
raised: retried, and then dead-lettered with the reason it gave. The alternative
is acknowledging a message nothing processed.

Not calling `send` or `handle` at all, and returning something instead, is the
quieter form. A consume interceptor can return `accept()` to swallow a message
without running the handler, which is how a filter or a deduplicator is written
— see [`idempotent`](patterns.md#idempotency), which does the same job as a
handler wrapper.

## Timing

`acemq.consume.duration` is measured around the interceptors **as well as** the
handler, because what an operator wants to know is how long a message takes to
deal with, and an interceptor that opens a transaction is part of dealing with
it.

## They are public API all the way down

Everything an interceptor touches is public. An interceptor sees the envelope,
the payload, the destination and the body, and may change any of them — and that
is the same rule the [patterns](patterns.md) follow.

It is what keeps this a seam rather than a privileged back door: anything an
interceptor can do, application code could have done, and nothing here depends
on reaching inside the library.

## Composing them by hand

`publish_chain` and `consume_chain` are exported, so a test can run a chain
without a broker:

```python
from acemq_amqp import ConsumeContext, Envelope, accept, consume_chain


async def handler(context: ConsumeContext):
    return accept()


chain = consume_chain([timed], handler)
ack = await chain(
    ConsumeContext(
        queue="shipping.orders",
        envelope=Envelope(type="order.placed"),
        payload={"id": "7"},
        body=b'{"id": "7"}',
        content_type="application/json",
        routing_key="order.placed",
        redelivered=False,
    )
)
```

The chain is folded backwards — the last interceptor wraps the work, the one
before wraps that — which is what puts the first on the outside.

## On the blocking API

`sync.connect(...).connection` is the asynchronous connection underneath, so
registering there applies to everything above it:

```python
with sync.connect(url) as mq:
    mq.connection.intercept_consume(timed)
```

The interceptor still runs on the loop thread; only the handler is moved to a
worker. An interceptor that blocks blocks the loop, exactly as it would on the
asynchronous API.
