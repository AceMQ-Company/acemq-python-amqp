# Tutorial 1 — Your first message

**10 minutes.** By the end you will have published a message and consumed it,
and you will know what every line does.

## Step 1 — Install it

```bash
pip install "acemq-amqp[rabbitmq]"
```

The core package has **no dependencies at all** — not even a UUID library — so
the AMQP client is an extra. A program that only reads an AceMQ envelope
somebody else delivered installs `acemq-amqp` and nothing more.

## Step 2 — A broker

```bash
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

**This tutorial needs one, and the Java and .NET versions of it do not.** Those
two ship an in-process broker behind a `memory://` URL; this library does not,
and the reason is in [testing without a broker](testing.md): `Transport` is a
six-method `Protocol`, so the fake a test needs is a small class rather than a
shipped subsystem. That trade is right for tests and it costs you a `docker run`
here. The management UI is at <http://localhost:15672>, guest/guest.

## Step 3 — Connect

```python
import asyncio

from acemq_amqp import connect


async def main() -> None:
    async with await connect("amqp://guest:guest@localhost:5672/") as mq:
        print("connected")


asyncio.run(main())
```

`await connect(...)` and then `async with` — two steps rather than one, because
`connect` has to dial the broker before there is a connection to enter. The
`async with` closes it, and closing stops every consumer on it first so that a
message being handled is finished rather than abandoned.

The library is asyncio because a broker client is: every publish is a round trip
and every delivery arrives on its own. If your program is not running a loop, use
[`acemq_amqp.sync`](getting-started.md#not-running-an-event-loop) — it is the
same connection behind a thread, not a second implementation.

## Step 4 — Say what the broker should have

```python
from acemq_amqp import Topology

shape = (
    Topology()
    .exchange("orders-events", "topic")
    .queue("shipping.orders")
    .binding("shipping.orders", "orders-events", "order.*")
)

await mq.declare(shape)
```

Three concepts, and they are the whole of AMQP routing:

| | |
|---|---|
| **Exchange** | where you publish. It holds nothing; it decides where things go |
| **Queue** | where messages wait. This is the thing that holds messages |
| **Binding** | the rule joining the two — "send anything matching `order.*` to `shipping.orders`" |

You never publish to a queue. You publish to an exchange and the bindings decide.
That indirection is the point: adding a second consumer later is a binding, not
a change to the publisher.

`topic` means the routing key is matched as a pattern. `order.*` matches
`order.placed` and `order.cancelled` but not `order.line.added` — `*` is one
word, `#` is any number.

**A `Topology` is a value, not three calls.** Java's tutorial teaches three
imperative calls first and the builder second; here there is only the value,
because it is the same amount of typing and you can hold it:

```python
shape.validate()     # is this description even coherent? No broker involved
shape.queues         # ['shipping.orders', ...]
shape.plan()         # what would be declared, as objects you can print
await mq.declare(shape)
```

`validate()` catches a binding to a queue the topology never declares before the
broker sees anything. See
[reading it before applying it](topology.md#reading-it-before-applying-it).

**`queue(...)` gives you a durable quorum queue**, because that is right for
anything holding real messages, and because it is what all five libraries
declare — the queue type is an argument the broker compares, so a Java service
and a Python service that disagree about `orders` cannot both consume it.

Declaring is idempotent. Running this twice is fine; running it against a queue
that exists **with different settings** is not, and the broker refuses rather
than adjusting. That is [topology drift](topology.md#redeclaring), and it is
tutorial 2's problem rather than yours yet.

## Step 5 — Consume

```python
from acemq_amqp import Ack, Message, accept


async def ship(message: Message) -> Ack:
    print("got", message.payload["orderId"])
    return accept()


consumer = await mq.consume("shipping.orders", ship)
```

Start the consumer **before** publishing. A message published to an exchange with
nothing bound behind it is discarded by the broker — that is AMQP rather than
this library, and it surprises everybody once.

**The decision is the return value.** This is the biggest difference from the
Java tutorial, where a handler that returns normally is acknowledged and one that
throws is rejected. Here you say which:

| | |
|---|---|
| `accept()` | done. The broker may forget it |
| `retry(e)` | it failed, and another attempt might work |
| `reject(e)` | it failed and will keep failing. Dead-letter it |
| `park(e)` | nothing could read it. A different queue, because it is a different problem |

Returning a value rather than falling off the end means the decision is in the
code and visible in a diff. A handler that *raises* is treated as `retry(e)` —
an exception is how Python says a thing failed, and a handler written without
reading this page will expect that — but the four functions are how you say what
you mean.

The handler takes a `Message`, not the payload, because you will want the things
around it: `message.envelope`, `message.envelope.attempt`, `message.redelivered`,
`message.body` for the undecoded bytes. `message.payload` is the object.

Both a coroutine function and a plain one are accepted. A handler that talks to a
database over asyncio is a coroutine; one that only does arithmetic should not
have to pretend to be.

## Step 6 — Publish

```python
orders = mq.publisher("orders-events", "order.placed")
result = await orders.send({"orderId": "o-1", "totalCents": 4250})

print(result.message_id, result.confirmed, result.routed)
```

`mq.publisher(...)` is an ordinary method call and only `send` is awaited:
nothing about building a publisher touches the broker, and a coroutine there
would suggest it did. Build them at start-up and keep them.

**`send` does not return until the broker has confirmed it has the message.** Not
until the bytes reach a socket: until the broker says it took responsibility.
Publisher confirms are opt-in in AMQP and off by default in most clients, which
is why "we published it" and "it exists" are so often different facts. Here there
is no way to turn them off.

That is a round trip per message, which is the price of knowing. For several
messages, pay it once:

```python
results = await orders.send_all([first, second, third])
```

Every message goes out before any confirm is awaited, and then all of them are
checked together. The results come back in the order the payloads did, whatever
order the broker answered in. It is **not** atomic — AMQP has no such thing — and
nothing caps how wide a batch may be, so hand it thousands rather than a
million-row cursor. See [publishing](publishing.md#several-at-once).

## Step 7 — All together

```python
import asyncio

from acemq_amqp import Ack, Message, Topology, accept, connect


async def main() -> None:
    async with await connect("amqp://guest:guest@localhost:5672/") as mq:
        await mq.declare(
            Topology()
            .exchange("orders-events", "topic")
            .queue("shipping.orders")
            .binding("shipping.orders", "orders-events", "order.*")
        )

        arrived = asyncio.Event()

        async def ship(message: Message) -> Ack:
            print(f"got {message.payload['orderId']} for {message.payload['totalCents']}")
            arrived.set()
            return accept()

        consumer = await mq.consume("shipping.orders", ship)

        orders = mq.publisher("orders-events", "order.placed")
        results = await orders.send_all(
            [
                {"orderId": "o-1", "totalCents": 4250},
                {"orderId": "o-2", "totalCents": 990},
            ]
        )
        print(f"published {len(results)}, all confirmed: {all(r.confirmed for r in results)}")

        # Only because main() would otherwise return before the handler ran.
        # A real service stays up.
        await asyncio.wait_for(arrived.wait(), 5)
        await consumer.close()


asyncio.run(main())
```

```
published 2, all confirmed: True
got o-1 for 4250
got o-2 for 990
```

Open <http://localhost:15672/#/queues> and you will see `shipping.orders` —
along with `shipping.orders.dlq` and `shipping.orders.parked`, which you did not
declare. Starting a consumer declares the queues it will need when a message
fails, before it subscribes, so the first message it gives up on already has
somewhere to go. Tutorial 2 is about those two.

## What you did not have to do

No serializer configuration: JSON is the default and the content type travels
with the message, so a consumer reads whatever arrived. No acknowledgement
bookkeeping, no confirm handling, no prefetch tuning — it is bounded at twenty
rather than unlimited, which is what stops one consumer pulling a whole queue
into memory.

Each of those is the safe default rather than the convenient one, and every one
of them has a name you can change.

## You did not need the broker for all of it

A handler is a function from a `Message` to an `Ack`, and both are ordinary
values, so the part of your service most worth testing needs nothing running:

```python
from acemq_amqp import Action, Envelope, Message, accept


async def test_it_ships_an_order() -> None:
    message = Message(
        payload={"orderId": "o-1"},
        envelope=Envelope(type="order.placed"),
        routing_key="order.placed",
        content_type="application/json",
        redelivered=False,
        body=b'{"orderId": "o-1"}',
    )

    assert await ship(message) == accept()
```

See [testing without a broker](testing.md). Write most of your tests this way.

## Next

**[Tutorial 2 — Surviving failure](tutorial-surviving-failure.md).** Your handler
fails. What should happen, what actually happens, and why sleeping in a consumer
is the wrong answer.
