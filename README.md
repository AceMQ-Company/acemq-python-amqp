# AceMQ for Python

[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB)](#requirements)
[![brokers](https://img.shields.io/badge/broker-RabbitMQ-lightgrey)](#requirements)

A Python client for AceMQ messaging over AMQP, speaking the same wire contract
as the [Java](https://github.com/AceMQ-Company/acemq-java-amqp),
[Go](https://github.com/AceMQ-Company/acemq-go-amqp) and
[.NET](https://github.com/AceMQ-Company/acemq-dotnet-amqp) libraries: the same
reserved headers, the same defaults, the same retry arithmetic. A Python
consumer reads what a Java producer writes, and the fixtures generated from the
Java implementation pin that rather than leaving it to be discovered in
production.

> **Status: in build.** The contract layer, the transport and the pattern
> library are implemented and tested, against a real broker as well as a fake
> one. Nothing is published to PyPI yet.

## Sending and receiving

```python
import asyncio
from datetime import timedelta

from acemq_amqp import (
    Ack, Message, Topology, accept, connect, exponential_retry, retry,
)


async def main() -> None:
    async with await connect(
        "amqp://guest:guest@localhost:5672/",
        origin="checkout@pod-7",
        retry=exponential_retry(5, timedelta(seconds=1), timedelta(minutes=1)),
    ) as mq:
        # Everything this service needs the broker to have, in one description
        # somebody can read before it is applied.
        await mq.declare(
            Topology()
            .exchange("orders-events", "topic")
            .queue("shipping.orders", dead_letter=True)
            .binding("shipping.orders", "orders-events", "order.placed")
        )

        async def ship(message: Message) -> Ack:
            if not warehouse_is_up():
                return retry(RuntimeError("the warehouse is not answering"))
            place(message.payload)
            return accept()

        consumer = await mq.consume("shipping.orders", ship)

        await mq.publisher("orders-events", "order.placed").send({"id": "7"})
        await asyncio.sleep(5)
        await consumer.close()


asyncio.run(main())
```

The transport is an extra, because reading an AceMQ envelope should not require
installing a broker client:

```bash
pip install acemq-amqp              # the contract, no dependencies at all
pip install "acemq-amqp[rabbitmq]"  # and a way to talk to a broker
```

### Not running an event loop?

```python
from acemq_amqp import Topology, accept, sync

with sync.connect("amqp://guest:guest@localhost:5672/") as mq:
    mq.declare(Topology().queue("shipping.orders", dead_letter=True))
    mq.publisher(routing_key="shipping.orders").send({"id": "7"})

    with mq.consume("shipping.orders", lambda message: accept()):
        ...
```

It is a facade, not a second implementation: a loop runs on a thread of its own
and every call is handed to it, so the envelope rules and the retry engine are
the ones above rather than a copy that can drift. Handlers run on a worker
thread, so a blocking handler blocks nothing but itself.

## What is identical, and what is not

**Identical**, because a message crosses languages: the reserved header names
and their types, the defaults applied when they are absent, the retry schedule
arithmetic, the `{queue}.dlq` / `{queue}.parked` / `{queue}.retry.{delay}`
naming, and the rules for giving up.

**Not identical**, deliberately: the API shape. Go gets `ctx`, .NET gets
`IAsyncEnumerable`, and Python gets dataclasses, `async with` and type hints.
Forcing a Java shape onto Python produces a library nobody enjoys using. The
contract is portable; the ergonomics are native.

### The envelope

| Header | |
|---|---|
| `x-acemq-id` | The message identifier, and the default idempotency key |
| `x-acemq-type` | The logical type, falling back to the routing key |
| `x-acemq-version` | Schema version, from 1 |
| `x-acemq-correlation` | Defaults to the id, so a chain has something to copy |
| `x-acemq-causation` | The message that caused this one |
| `x-acemq-attempt` | Delivery attempt, from 1 |
| `x-acemq-first-seen` | Epoch **milliseconds** of the first publish |
| `x-acemq-origin` | `service@host` |
| `x-acemq-error` | Why it was dead-lettered |
| `x-acemq-claim` | Where the payload is, when it is stored outside the message |

Application headers are kept apart from these. A reserved name in your own
headers is refused rather than dropped — silently discarding a header somebody
set is worse than saying no — and unknown `x-acemq-` names from a newer version
of another language's library are not handed back as yours.

### Retry, and where a message goes when it runs out

```python
policy = exponential_retry(5, timedelta(seconds=1), timedelta(minutes=1))
policy = policy.give_up_after(timedelta(hours=6))
```

`schedule()` shows the delays without jitter, which is what to read when
deciding whether a policy is the one you meant. Jitter moves a delay **both
ways**: one-sided jitter only ever delays, which turns a thundering herd into a
slower thundering herd.

Giving up on **age** as well as attempts is the honest limit when a queue has
been paused — a message can be on attempt one and four days old.

A handler that returns `retry()` gets another delivery if the policy has one
left. When it does not, the message is republished to `{queue}.dlq` with
`x-acemq-error` saying which limit it hit and what the last failure was —
`exhausted 5 attempts: RuntimeError: the warehouse is not answering` — and the
original is acknowledged, because it has already been safely put somewhere else.
Raising `FatalError`, or returning `retry(FatalError(...))`, skips the attempts
that are left: they would all fail the same way.

A retry is republished rather than requeued, so `x-acemq-attempt` really
advances and the count lives on the message rather than in the memory of the
process that has been failing. The trade is that a retried message goes to the
back of its queue rather than the front.

### Where the waiting happens

A short wait is spent in the consumer, which holds the delivery and one prefetch
slot. A long one is spent in the broker, on a `{queue}.retry.{delay}` queue whose
`x-message-ttl` is the wait and whose dead-letter target is the source queue.
The line between them is 30 seconds by default:

```python
policy = exponential_retry(6, timedelta(seconds=10))
policy.schedule()      # [10s, 20s, 40s, 80s, 160s]
policy.broker_rungs()  # [40s, 80s, 160s] — one queue each

await mq.declare(Topology().queue("shipping.orders", dead_letter=True, retry=policy))
```

`queue()` takes the **policy**, not a list of delays, because the rungs a
consumer publishes to are derived from the policy it runs: a second copy of the
list is free to drift from the first, and the way that drift shows up is a retry
addressed to a queue nobody declared, at the moment the service is already
failing.

The threshold is there because neither answer is right at both scales. A
consumer sleeping on a five-minute backoff loses the whole wait when it
restarts — the broker redelivers the unacknowledged message at once, so a
five-minute policy delivers in none. But a queue's TTL is fixed at declaration
and cannot express a jittered delay, so **jitter applies below the threshold
only**; above it the spread is free, because each message's TTL starts when it
enters the rung. Move the line with `wait_in_broker_from(...)`, and pass zero to
keep every wait in the consumer.

Per-message TTL is never used, and is the trap worth naming: RabbitMQ expires
messages only from the **head** of a queue, so one queue of per-message TTLs
lets a ten-minute wait at the front hold back every thirty-second wait behind
it. That is why a policy needs one queue per delay rather than one queue.

**The default policy is one delivery and no second chance.** A `retry()` on a
connection with no policy dead-letters the message and says so in the reason,
which is louder than the alternative default — an immediate requeue, which is a
hot loop nobody asked for.

### Codecs

JSON by default. A codec declares the content type it writes and says which ones
it will read, and a `CompositeCodec` tries them in order — which is what a queue
carrying two formats during a migration needs:

```python
from acemq_amqp import BytesCodec, CompositeCodec, JsonCodec, TextCodec

codec = CompositeCodec(JsonCodec(), TextCodec())
```

A message with **no** content type rules nothing out, so every codec is a
candidate and the first that can actually read the body wins. `BytesCodec`
answers for everything and hands back the bytes unchanged, which is what to read
a dead-letter queue with: the message that went there may be exactly the one
nothing could decode.

## Patterns

`acemq_amqp.patterns` holds the things every service that consumes a queue ends
up writing for itself. They mean the same as the Go library's, because a routing
slip written by a Go service has to be readable by a Python one; the API shape is
Python's.

```python
from acemq_amqp.patterns import InMemoryIdempotencyStore, chain, idempotent, with_timeout
```

| | |
|---|---|
| `idempotent(store, handler)` | Handle a message once however many times it arrives. A duplicate is **accepted**, not rejected: the work was done |
| `record(...)` / `OutboxRelay` | Write the message into the same transaction as the work, and let a relay publish what was committed |
| `Requester` / `serve(...)` | Ask a question and wait for the answer. A responder's failure comes back as a failure, not as a timeout |
| `replay(...)` | Put dead letters back, with a filter, a limit and a deadline, and a report of what it did and why it stopped |
| `ordered(key, handler)` | Keep one entity's messages in sequence while everything else runs at once |
| `ConsumerGroup` | Several consumers over one queue, started and stopped as one thing |
| `RoutingSlip` / `follow_slip(...)` | An itinerary the message carries, instead of an orchestrator that knows it |
| `chain(...)` / `then(...)` | Wrap a handler in a deadline, logging and the guards; publish what a step produced onwards |
| `SchemaRegistry` | Remember what a message used to look like, so a producer can add a field without a synchronised deployment |
| `read_stream(...)` | Read a queue that keeps what it has already handed out |

Every one is built out of the public library — handlers, envelopes, publishers —
so nothing is possible with a pattern that would not be possible without it. The
storage seams (`IdempotencyStore`, `OutboxStore`, `SchemaRegistry`) are
interfaces with in-memory implementations that say in their own docstrings why
they are not the ones to use in production: an outbox store that does not share a
transaction with your database has the gap the pattern exists to close.

## Requirements

Python 3.10 or newer, and RabbitMQ for the transport.

## Development

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest -m "not integration"     # the contract and the engine, no broker needed
ruff check . && mypy

# And against a real one:
ACEMQ_TEST_BROKER=amqp://guest:guest@localhost:5672/ pytest -m integration
```

The fixtures under `tests/fixtures/` are generated by the Java implementation
and shared with Go and .NET. They are the definition of "the same wire
contract", and they are checked here rather than assumed.

The integration tests name everything they create `pyit.*` and delete it
afterwards, so they can be pointed at a broker that is not theirs alone.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not
affiliated with it.
