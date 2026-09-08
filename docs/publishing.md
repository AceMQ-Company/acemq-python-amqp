# Publishing

## A publisher per destination

```python
orders = mq.publisher("orders-events", "order.placed")
await orders.send({"orderId": "o-1", "totalCents": 4250})
```

`Connection.publisher` takes an exchange and a routing key, and both default to
the empty string. Publishing to the default exchange — no exchange, routing key
equal to a queue name — is how a message goes straight to one queue:

```python
direct = mq.publisher(routing_key="shipping.orders")
```

Build them at start-up and keep them. A `Publisher` holds a destination, a codec
and two flags; it opens nothing, registers nothing and is safe to share across
tasks, so one per message type costs nothing and puts the decisions in one
place. Building one per message is not wrong, only pointless.

### It is not a coroutine

`mq.publisher(...)` is an ordinary method call and only `send` is awaited. That
is deliberate: nothing about constructing a publisher touches the broker, and a
coroutine there would suggest it did.

## Sending

```python
result = await orders.send({"orderId": "o-1", "totalCents": 4250})
result.message_id   # the envelope's id, which is also the AMQP message id
result.confirmed    # the broker acknowledged it
result.routed       # it reached at least one queue
```

The payload is whatever the codec can encode. With the default `JsonCodec` that
is a dict, a list, a scalar — or a dataclass, which goes on the wire as an
object with its field names:

```python
@dataclass
class OrderPlaced:
    order_id: str
    total_cents: int


await orders.send(OrderPlaced("o-1", 4250))
```

A publisher bound to an exchange rather than to one key can name the key per
message:

```python
events = mq.publisher("orders-events")
await events.send(payload, routing_key="order.cancelled")
```

## What the envelope gets

`send` builds an [envelope](envelope.md) when none is given: a fresh
identifier, a correlation identifier defaulting to it, this connection's
`origin`, attempt 1, version 1, and the time now. The **type** falls back to the
routing key, which is why a publisher bound to `order.placed` needs no further
configuration to say what it sends.

## Carrying context forward

A message published because of another one should say so, or a trace through
four services has four unrelated identifiers in it:

```python
async def handle(message: Message) -> Ack:
    await shipped.send(
        {"orderId": message.payload["orderId"]},
        envelope=message.envelope.with_(
            causation_id=message.envelope.id,
            type="order.shipped",
        ),
    )
    return accept()
```

`with_` returns a copy, because `Envelope` is frozen: an envelope describes a
message that has already been published or received, and changing one in place
would change what a log line said after it was written.

Carrying the envelope forward keeps `correlation_id` — the field the whole chain
shares — and `causation_id` names the immediate parent. Note what is *not*
carried: `id` should be new for a new message, so pass `id=str(uuid.uuid4())` or
build a fresh `Envelope` and copy the correlation across:

```python
Envelope(
    type="order.shipped",
    correlation_id=message.envelope.correlation_id,
    causation_id=message.envelope.id,
)
```

`origin` is filled in for you either way: an envelope arriving without one gets
this connection's, because a reply is published by *this* process and an origin
naming the service before it would be a lie in a log line somebody is going to
trust.

## Your own headers

Application headers live on the envelope, apart from the reserved ones:

```python
await orders.send(
    payload,
    envelope=Envelope(type="order.placed", headers={"tenant": "acme"}),
)
```

A reserved name — anything in the `x-acemq-` set — is **refused** rather than
dropped, with a `ValueError` naming the offenders. Silently discarding a header
somebody set is worse than saying no. See
[the reserved namespace](envelope.md#the-reserved-namespace).

For a header every message needs, do not put it at every call site. That is what
[interceptors](interceptors.md) are for:

```python
async def stamped(context: PublishContext, send: PublishNext):
    context.set_header("tenant", tenant.current())
    return await send(context)


mq.intercept_publish(stamped)
```

## Durability

```python
transient = mq.publisher("metrics", "tick", persistent=False)
```

Messages are persistent by default: the broker writes them to disk and they
survive a restart. `persistent=False` is for the ones where the next message
makes the last one irrelevant — a gauge, a heartbeat, a cache invalidation on a
short timer. It is faster, and it means a broker restart loses whatever was in
the queue.

Persistence is not durability on its own. A persistent message on a
non-durable queue still goes when the broker does; see
[exchanges, queues and bindings](topology.md).

## Messages that reach no queue

```python
required = mq.publisher("orders-events", "order.placed", mandatory=True)
await required.send(payload)   # raises PublishError if nothing is bound
```

An unroutable message is the quietest failure AMQP has: the publish succeeds,
the message is discarded, the consumer waits, and nothing anywhere says why.
`mandatory=True` makes the broker return it, and this library **raises**
`PublishError` rather than leaving the fact in the result, because a caller who
does not read the result would otherwise carry on believing the message went
somewhere:

```python
from acemq_amqp import PublishError

try:
    await required.send(payload)
except PublishError as failure:
    log.error("%s went nowhere: %s", failure.message_id, failure)
```

Without `mandatory`, `result.routed` still says what happened — it is only the
raising that is opted into.

## A different codec for one publisher

```python
from acemq_amqp import BytesCodec

raw = mq.publisher("files", "uploaded", codec=BytesCodec())
await raw.send(b"\x89PNG\r\n\x1a\n...")
```

The publisher's codec decides the content type on the wire, and a consumer picks
its codec by reading that. See [codecs](serialization.md).

## What can go wrong

| | |
|---|---|
| The payload will not encode | `TypeError` from the codec, naming the type. Nothing is published |
| An interceptor refused | whatever it raised, reaching the caller. Nothing is published — that is the point of intercepting rather than observing |
| The broker rejected the publish | the transport's exception, unwrapped. `acemq.messages.publish.failed` is counted |
| Nothing was bound to receive it | `PublishError` when `mandatory=True`; `result.routed` is `False` either way |

Most of what goes wrong with a broker is the broker's own exception, and this
library does not wrap every one of them in a class of its own: that would hide
the detail somebody actually needs while adding a name they then have to learn.
The exceptions it does define are `AceMQError`, `PublishError` and
`SecurityError`, plus `FatalError`, which is a statement about a message rather
than about the library.

## Publisher confirms

Publisher confirms are on. `send` does not return until the broker has
acknowledged the message, and `result.confirmed` says so. That is a round trip
per message, which is the cost of knowing; a service that publishes in bulk
should publish concurrently — `asyncio.gather` over several `send` calls —
rather than turning confirms off, because there is no way to turn them off here.

For the stronger guarantee — that a message and the database row it describes
either both happen or neither does — see
[the outbox](patterns.md#the-outbox).
