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

## Reaching a real broker: TLS and credentials

**The URL scheme decides whether the connection is encrypted.** `amqps://` is,
`amqp://` is not, because that is the part somebody reads in a configuration
file and believes. An `amqps://` URL needs nothing else: the broker is verified
against the machine's trust store and nothing older than TLS 1.2 is spoken.

```python
mq = await connect("amqps://broker.internal:5671/")
```

Most brokers are not on a public certificate authority, because no public
authority issues certificates for `broker.internal`. Name the authority instead,
and keep the password out of the URL while you are there:

```python
from acemq_amqp import Security, connect, credentials_from_environment

mq = await connect(
    "amqps://broker.internal:5671/",
    security=Security(
        certificate_authority="/etc/acemq/ca.crt",
        credentials=credentials_from_environment("MQ_USER", "MQ_PASSWORD"),
    ),
)
```

Naming an authority **replaces** the system trust store rather than adding to
it. That is the point: a broker holding a certificate from a public authority is
not your broker, and the several hundred authorities a machine trusts by default
are several hundred ways to be wrong. A self-signed broker certificate works
here too — put the broker's own certificate in the file and it becomes the one
authority you trust.

| | |
|---|---|
| `certificate_authority` | The PEM authority to verify the broker against, and no other |
| `client_certificate` / `client_key` | What to present, for a broker that authenticates clients by certificate |
| `client_key_password` | The passphrase on that key. Kept out of `repr` |
| `server_name` | The name to check the certificate against, when it is not the one in the URL — an IP address, a tunnel, a container's internal name |
| `credentials` | The login, or a source asked for it at connection time |

Everything above works the same on the blocking API — `sync.connect(url,
security=...)` — because a program that is not running an event loop has exactly
the same broker to reach.

### Credentials that do not end up in a log

A password in a connection string reaches every log line, crash report and `ps`
listing that URL ever appears in. Supply it separately and the URL carries a host
and nothing else:

```python
from acemq_amqp import Credentials, credentials_from_environment, credentials_from_file

Credentials("app", password)                        # from wherever you already had it
credentials_from_environment("MQ_USER", "MQ_PASSWORD")
credentials_from_file("/run/secrets/broker", username="app")
```

`Credentials` never renders its secret — `repr`, `str` and an f-string all give
back `Credentials(username='app', secret=<secret>)` — and a test in the suite
asserts it, because the whole value of the type is that it holds under a
`print()` somebody added at three in the morning.

The two file-reading sources are read **each time a connection is made**, not
once at import. That is what makes a password rotated by a sidecar or a
remounted Kubernetes secret take effect without a restart. A source is just a
callable, so `lambda: Credentials("app", vault.read())` is a complete
implementation of one.

### The way out, and what it costs

```python
from acemq_amqp import without_verifying_the_broker

mq = await connect("amqps://localhost:5671/", security=without_verifying_the_broker())
```

This encrypts the traffic and checks nothing at all about who is on the other
end. Any certificate is accepted, from any issuer, for any name, so somebody who
can answer on the address in your URL receives every message you publish and
every password you log in with — over a connection that looks encrypted in every
log and every metric.

It is a function with a long name rather than a `verify=False` on purpose: there
is no keyword argument anywhere in this library that turns verification off, so
it should be impossible to end up here by pasting one, and hard to leave in a
file nobody rereads. **It is wrong in production, always.** The alternative is
one line — `Security(certificate_authority="ca.crt")` — and gives back
everything this gives up.

Settings that describe TLS are **refused against an `amqp://` URL** rather than
ignored, and that is deliberate too: a service that was handed a certificate
authority, connected in plaintext and reported success is the failure this whole
module exists to prevent. Credentials alone are welcome on either scheme.

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

### The two exchanges this library declares

A rung is declared with exactly three arguments, and the same three in Java, Go
and .NET. Two services consuming one queue declare the same rung by name, so a
rung declared with anything else answers the second service
`PRECONDITION_FAILED` and leaves it unable to consume at all — which is why the
table is pinned by a test and why `rung_args(...)` is the only place it is
written:

```python
rung_args("shipping.orders", timedelta(seconds=40))
# {'x-message-ttl': 40000,
#  'x-dead-letter-exchange': 'acemq.retry',
#  'x-dead-letter-routing-key': 'shipping.orders'}
```

| Constant | Value | What it carries |
| --- | --- | --- |
| `RETRY_EXCHANGE` | `acemq.retry` | An expired rung message, back to the queue it came from |
| `DEAD_LETTER_EXCHANGE` | `acemq.dlx` | The `{queue}.dlq` and `{queue}.parked` queues, each bound on its own name |

Both are **direct** and **durable**, and both are declared by
`Topology().queue(...)` when it is asked for retries or for dead-lettering — as
is the one binding that brings an expired message home, `{queue}` to
`acemq.retry` on `{queue}`. That binding is not optional and is not lazy: a
direct exchange drops what it cannot route and says nothing, so a topology
missing it loses every expired retry while the queue looks quiet.

Python used to dead-letter a rung through the default exchange, which needs no
exchange and no binding and works perfectly well on its own. Java has always
used the named exchange, most of the released code follows Java, and two
libraries cannot both be right about one queue. This is the settled answer, and
a broker already carrying a Java service has the arrangement above on it.

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

## Interceptors

Every organisation has something every message needs and no library can guess: a
tenant, a trace context, an authorisation token, a correlation identifier in the
logging context, a timer, a size limit. Without a seam these get copied into
every call site, where one of them is eventually forgotten and nobody finds out
until the message that needed it is the one that went without.

An interceptor is **one function handed the message and the rest of the work**:

```python
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

mq = await connect(url, on_publish=[stamped], on_consume=[timed])
# or later, which still reaches publishers that already exist:
mq.intercept_publish(stamped).intercept_consume(timed)
```

That shape rather than the pair of before-and-after hooks the Java library uses,
because Python already has the construct. `try`/`finally` runs the way out in
the reverse of the way in, nests correctly without anybody reversing a list, and
makes an interceptor that opens something and closes it **one** function instead
of two halves that have to agree. It composes the way ASGI middleware does.

Interceptors run in the order they were registered — **first registered is
outermost**, so it sees the message first on the way in and last on the way out.
That matters as soon as one reads what another wrote.

| | |
|---|---|
| `PublishContext` | `exchange`, `routing_key`, `envelope`, `payload`, `persistent`, `mandatory`, and `set_header(...)`. Seen **before the codec runs**, so the payload can be changed and not only its metadata |
| `ConsumeContext` | `queue`, `envelope`, `payload`, `body`, `content_type`, `routing_key`, `redelivered`, and a `state` dict this library never reads |

**Refusing is raising.** A publish interceptor that raises stops the publish and
the caller sees the exception — the whole point of intercepting rather than
observing. A consume interceptor that raises is treated exactly as a handler
that raised: retried, then dead-lettered with the reason it gave. The
alternative is acknowledging a message nothing processed.

Whatever the interceptors left is what gets used: an envelope rewritten on the
way in is the one the handler is given **and** the one that gets dead-lettered,
so an operator reading `{queue}.dlq` is not missing the very field the
interceptor exists to add.

Everything an interceptor touches is public API. That is the same rule the
patterns follow, and it is what keeps this a seam rather than a privileged back
door: anything an interceptor can do, application code could have done.

The blocking API inherits them — `sync.connect(...).connection` is the
asynchronous connection underneath, and registering there applies to everything
above it.

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

The TLS tests need a broker with a TLS listener, and skip when they are not told
where one is. Two are wanted, because one broker cannot both accept a client
that has no certificate and refuse it:

```bash
export ACEMQ_TEST_TLS_CERTIFICATES=/path/to/certs   # ca.crt, client.crt, client.key,
                                                    # other-ca.crt, stranger.crt, stranger.key
export ACEMQ_TEST_TLS_BROKER=amqps://localhost:25891/          # ssl_options.verify = verify_none
export ACEMQ_TEST_TLS_MUTUAL_BROKER=amqps://localhost:25893/   # verify_peer, fail_if_no_peer_cert
pytest -m integration
```

They prove the handshake rather than the configuration: the certificate the
broker presented, the version and cipher that were agreed, that a broker the
system trust store does not vouch for is **refused**, and that a client
certificate signed by the wrong authority gets no further than one signed by
none.

The fixtures under `tests/fixtures/` are generated by the Java implementation
and shared with Go and .NET. They are the definition of "the same wire
contract", and they are checked here rather than assumed.

The integration tests name everything they create `pyit.*` and delete it
afterwards, so they can be pointed at a broker that is not theirs alone.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not
affiliated with it.
