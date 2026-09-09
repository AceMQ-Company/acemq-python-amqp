# Getting started

The [overview](index.md) shows a whole program in one block. This takes it
apart.

## Install it

```bash
pip install "acemq-amqp[rabbitmq]"
```

Python 3.10 or newer. The extra is the broker client — `aio-pika` — and it is an
extra rather than a dependency because the contract layer needs nothing at all:

```bash
pip install acemq-amqp                # envelopes, codecs, retry arithmetic
pip install "acemq-amqp[rabbitmq]"    # and a way to talk to a broker
pip install "acemq-amqp[prometheus]"  # and a registry to report into
```

A service that reads an envelope off a message somebody else delivered — a log
shipper, a replay tool, a test — installs the first line and never links an AMQP
client in. `connect` reaches for `aio-pika` at the moment it is called rather
than at import, so the split is real rather than nominal.

## One import

```python
from acemq_amqp import Ack, Message, Topology, accept, connect, retry
```

Everything worth naming is re-exported from the package root. There is no
separate module to remember for the acknowledgements, the envelope or the
topology.

Two names are worth reading twice. `retry` at the package root is the
*acknowledgement* — what a handler returns to ask for another attempt — and it
is what the Java, Go and .NET libraries call it too. The retry *policy* lives in
`acemq_amqp.retry`, and `from acemq_amqp.retry import RetryPolicy` still reaches
it; `exponential_retry`, `fixed_retry` and `no_retry` are re-exported from the
root alongside `RetryPolicy` itself.

## Connect

```python
mq = await connect("amqp://guest:guest@localhost:5672/")
```

`connect` is a coroutine, so it is `await connect(...)`, and the connection is
an async context manager:

```python
async with await connect("amqp://guest:guest@localhost:5672/") as mq:
    ...
```

`async with await` reads oddly the first time. It is two things: `await
connect(...)` produces the connection, and `async with` closes it — every
consumer stopped and the transport shut down — however the block is left.

Three keyword arguments are worth setting from the start:

```python
mq = await connect(
    "amqp://guest:guest@localhost:5672/",
    origin="checkout@pod-7",
    retry=exponential_retry(5, timedelta(seconds=1), timedelta(minutes=1)),
    prefetch=20,
)
```

`origin` is stamped on every message this connection publishes and is what an
operator reads off a dead letter to find out who sent it. Left out, it is
derived from the process and the host. `retry` is the default policy for every
consumer on this connection; without one the default is **one delivery and no
second chance**. `prefetch` is how many unacknowledged messages a consumer will
hold, and defaults to 20.

For a broker that is not on this laptop, see [security](security.md): an
`amqps://` URL and, usually, a `Security` naming the authority that issued the
broker's certificate.

## Declare what you use

```python
await mq.declare(
    Topology()
    .exchange("orders-events", "topic")
    .queue("shipping.orders", dead_letter=True)
    .binding("shipping.orders", "orders-events", "order.placed")
)
```

A `Topology` is a description before it is an action. It can be printed and
checked before anything reaches the broker:

```python
topology = Topology().queue("shipping.orders", dead_letter=True)
print(topology)
for action in topology.plan():
    print(action)
```

`dead_letter=True` declares `shipping.orders.dlq` and `shipping.orders.parked`
beside the queue and points the broker at them, so a message that runs out of
attempts or cannot be decoded lands somewhere an operator can find by name. See
[exchanges, queues and bindings](topology.md).

## Publish

```python
publisher = mq.publisher("orders-events", "order.placed")
await publisher.send({"orderId": "o-1", "totalCents": 4250})
```

`publisher(...)` is not a coroutine — it builds a small object and does not
touch the broker — so it is `mq.publisher(...)` and only `send` is awaited. Keep
one per message type rather than building one per call: it is where the codec,
the durability flag and the destination are settled.

The payload is anything the codec can encode, which for the default `JsonCodec`
means anything `json.dumps` accepts — a dict, a list, or a dataclass by way of
`dataclasses.asdict`. `send` returns a `PublishResult` saying whether the broker
confirmed it and whether it reached a queue. See [publishing](publishing.md).

## Consume

```python
async def ship(message: Message) -> Ack:
    if not warehouse_is_up():
        return retry(RuntimeError("the warehouse is not answering"))
    place(message.payload)
    return accept()


consumer = await mq.consume("shipping.orders", ship)
```

The handler's **return value** is the decision, not an exception and not a side
effect on a parameter:

| | |
|---|---|
| `accept()` | done. The message is acknowledged and gone |
| `retry(error)` | try again, if the policy has an attempt left |
| `reject(error)` | do not try again. Straight to `{queue}.dlq` |
| `park(error)` | nothing could read it. Straight to `{queue}.parked` |

A handler that raises is treated as `retry` with that exception, because in
Python an exception is how a thing says it failed and most failures it carries
are transient. Raising `FatalError` — or returning `retry(FatalError(...))` —
skips the attempts that are left, because they would all fail the same way.

A synchronous handler works too: the library accepts both
`Callable[[Message], Ack]` and `Callable[[Message], Awaitable[Ack]]` and awaits
what needs awaiting. A blocking handler on the asyncio API still blocks the
loop, though — if the handler is blocking, use
[the sync API](#not-running-an-event-loop), which runs handlers on worker
threads for exactly that reason.

The consumer runs until it is closed, and is an async context manager as well:

```python
async with await mq.consume("shipping.orders", ship):
    await asyncio.Event().wait()
```

See [consuming](consuming.md).

## Not running an event loop

Plenty of Python is not asyncio and has no reason to become asyncio because it
sends a message:

```python
from acemq_amqp import Topology, accept, sync

with sync.connect("amqp://guest:guest@localhost:5672/") as mq:
    mq.declare(Topology().queue("shipping.orders", dead_letter=True))
    mq.publisher(routing_key="shipping.orders").send({"id": "7"})

    with mq.consume("shipping.orders", lambda message: accept()):
        ...
```

It is a facade, not a second implementation. A loop runs on a thread of its own
and every call here is handed to it, so the envelope rules, the codec
negotiation and the retry engine are the ones in `acemq_amqp.connection` — there
is one set of retry arithmetic in this library and it is not the sync module's.
Handlers run on a worker thread, so a blocking handler blocks nothing but
itself.

`sync.connect` takes the same keyword arguments, `security=` included. Reach for
the asynchronous API when the process is already asyncio, and this one
otherwise.

## A whole program

```python
import asyncio
from dataclasses import dataclass
from datetime import timedelta

from acemq_amqp import (
    Ack, Message, Topology, accept, connect, exponential_retry, retry,
)


@dataclass
class OrderPlaced:
    order_id: str
    total_cents: int


async def main() -> None:
    async with await connect(
        "amqp://guest:guest@localhost:5672/",
        origin="checkout@pod-7",
        retry=exponential_retry(5, timedelta(seconds=1), timedelta(minutes=1)),
    ) as mq:
        await mq.declare(
            Topology()
            .exchange("orders-events", "topic")
            .queue("shipping.orders", dead_letter=True)
            .binding("shipping.orders", "orders-events", "order.placed")
        )

        async def ship(message: Message) -> Ack:
            print("shipping", message.payload, "attempt", message.envelope.attempt)
            return accept()

        async with await mq.consume("shipping.orders", ship):
            await mq.publisher("orders-events", "order.placed").send(
                OrderPlaced(order_id="o-1", total_cents=4250)
            )
            await asyncio.sleep(1)


asyncio.run(main())
```

Against a local RabbitMQ, that prints the payload and the attempt number. Where
to go from here: [publishing](publishing.md) and [consuming](consuming.md) for
what each half can do, [testing](testing.md) for running the same handler
without a broker at all.
