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
pip install acemq-amqp                # the contract, no dependencies at all
pip install "acemq-amqp[rabbitmq]"    # and a way to talk to a broker
pip install "acemq-amqp[prometheus]"  # and a registry to report into
pip install "acemq-amqp[yaml]"        # and one of the five optional formats
```

The formats go the same way, one extra each — `[yaml]`, `[toml]`, `[protobuf]`,
`[avro]` — because a service that speaks YAML has no reason to install a
protobuf runtime. XML has no extra: it is written against the standard library.

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

### Certificates for a broker on a laptop

```bash
pip install "acemq-amqp[crypto]"
python -m acemq_amqp.devcerts --directory certs --broker localhost
```

An authority, a broker certificate, a client certificate and a `rabbitmq.conf`
pointing the broker at them — the same file names Go's `acemq-certs` writes, so
it is a drop-in replacement for it.

Everything it writes carries `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` in its
subject organisation, and **this library refuses any certificate carrying it,
however trust is configured — `without_verifying_the_broker()` included**. That
is not a warning in a docstring; it is the mechanism. A generated authority's
private key sits next to its certificate and usually ends up in a repository, so
a development certificate that could reach production would be an authority
anybody who can read that repository can issue against, and the connection would
succeed. Java, Go and .NET stamp the same string and enforce it the same way.

The way through, for the one place it belongs:

```python
security=Security(certificate_authority="certs/ca.crt", allow_development_certificates=True)
```

A named argument a reviewer will see, and one more thing to `grep` for in a
deployed configuration. See [Security](docs/security.md#development-certificates).

## What is identical, and what is not

**Identical**, because a message crosses languages: the reserved header names
and their types, the defaults applied when they are absent, the retry schedule
arithmetic, the `{queue}.dlq` / `{queue}.parked` / `{queue}.retry.{delay}`
naming, the queue type and arguments each of those is declared with, and the
rules for giving up.

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

### Quorum by default, and what stays classic

A durable queue asked for through `Topology().queue(...)` is a **quorum** queue,
declared with `x-queue-type: quorum`:

```python
print(Topology().queue("shipping.orders", dead_letter=True))
# Topology: 1 exchanges, 3 queues, 2 bindings
#   declare exchange acemq.dlx (direct, durable)
#   declare queue shipping.orders (durable, x-dead-letter-exchange='acemq.dlx',
#                                  x-dead-letter-routing-key='shipping.orders.dlq',
#                                  x-queue-type='quorum')
#   declare queue shipping.orders.dlq (durable)
#   declare queue shipping.orders.parked (durable)
#   declare binding shipping.orders.dlq (from acemq.dlx on shipping.orders.dlq)
#   declare binding shipping.orders.parked (from acemq.dlx on shipping.orders.parked)
```

It is the same contract as the rung arguments and it exists for the same reason.
A queue type is one more thing the broker compares, so a Java service and a
Python service consuming `shipping.orders` that disagree about it cannot both
consume it — the second one to declare is answered `PRECONDITION_FAILED`. Java
has declared quorum since it had deployments, so quorum is the answer here.

**Three kinds of queue stay classic**, deliberately, and Java declares them
classic too:

| | |
| --- | --- |
| `{queue}.retry.{delay}` | Nothing consumes a rung; replicating it buys nothing and would cost a Java service its declaration of the same name |
| `{queue}.dlq`, `{queue}.parked` | The same, for the same reason |
| Anything exclusive, auto-deleting or transient | RabbitMQ **refuses** a quorum queue that is any of those |

That last row is not a preference. A generated reply queue is exclusive and
auto-deleting so that it goes when its requester does, and asking for it as
quorum is a declaration the broker rejects — so a queue that is any of those
three is declared classic without being asked. Asking for both at once is
refused here, where the contradiction is written down, rather than at the broker,
which answers it with a message that never mentions the word quorum:

```python
Topology().queue("replies", exclusive=True, quorum=True)
# ValueError: acemq: queue 'replies' asks for quorum=True and also exclusive; ...
```

A caller who wants a classic queue says so, and gets one with no `x-queue-type`
at all — the spelling every AceMQ library uses, and therefore the only one a
broker finds equivalent to theirs:

```python
Topology().queue("shipping.orders", quorum=False)
```

Streams are unaffected: `stream(...)` writes its own `x-queue-type`, and a queue
whose arguments already name a kind keeps it.

**A queue that already exists cannot change kind.** AMQP has no way to alter a
queue's type in place, so a queue declared classic by 0.1.0 has to be drained
and deleted before this version can declare it.

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

Five more formats ship behind an extra each — the same five Java and Go ship, so
a message from either is readable here:

```python
from acemq_amqp.codecs.avro import AvroCodec          # avro/binary
from acemq_amqp.codecs.protobuf import ProtobufCodec  # application/x-protobuf
from acemq_amqp.codecs.toml import TomlCodec          # application/toml
from acemq_amqp.codecs.xml import XmlCodec            # application/xml
from acemq_amqp.codecs.yaml import YamlCodec          # application/yaml

mq = await connect(url, codec=CompositeCodec(JsonCodec(), YamlCodec()))
```

Each reads a wider set than it writes, because a producer in another stack uses
whichever spelling its own library picked — `text/yaml` and `application/x-yaml`
both predate the registered `application/yaml`, and refusing them would park a
message that was perfectly readable. That is the gap these close: not
capability, interoperability. See [Codecs](docs/serialization.md), and note what
the XML codec does about document type declarations.

### Encrypted bodies

`EncryptedCodec` wraps any of the above and encrypts what it produced, so the
broker, its disk, its backups and its management interface hold ciphertext:

```python
# pip install "acemq-amqp[crypto]"
from acemq_amqp.codecs.encrypted import EncryptedCodec, Keyring, generate_key

keyring = Keyring.of("2026-01", generate_key())
mq = await connect(url, codec=EncryptedCodec(JsonCodec(), keyring))
```

AES-GCM, a fresh nonce per message, and the key identifier in the clear in front
of the ciphertext — which is what makes rotation possible, because a consumer
reads which key a message needs instead of assuming the current one. The header
is bound in as associated data, so an identifier altered in flight makes the
message fail to open rather than open as something else. `key_id_of(body)`
answers "which key does this need?" from the bytes alone, without holding any.

No failure message, log line or exception ever contains the plaintext or the key,
and a wrong key and a tampered body fail identically — GCM authenticates before
it returns anything, and nothing here adds a check that would tell them apart.

**It interoperates with the Java library and with nothing else.** Java, Go and
.NET currently write three different framings under one content type; a body from
Go or .NET is refused here, visibly, rather than misread. The table and the test
vector to converge on are in
[Codecs → Encryption](docs/serialization.md#encryption).

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
| `ConsumeContext` | `queue`, `envelope`, `payload`, `body`, `content_type`, `routing_key`, `redelivered`, a `state` dict this library never reads, and `when_settled(...)` |

**`when_settled` is how an interceptor learns what really happened.** The `Ack`
it sees on the way out is what the handler *asked for*: a handler asking for
another attempt when there are none left is dead-lettered, and an interceptor
recording the `Ack` would report a retry that never happened. A listener
registered with `when_settled` is told the `Settlement` — `acked`, `retried`,
`rejected` or `dead_lettered`, with the delay chosen or the reason given — once,
after the consumer decides and before it acts. It returns `False` when nothing
is driving the chain and no answer is coming.

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

## Telemetry and health

How much is going through, how much is failing and how long a handler takes are
numbers nobody outside the process can see. Whether the connection is really up
— as opposed to a socket that is open and wedged — is a question only something
holding the connection can ask. So the library answers both.

**No hard dependency.** The library calls a three-method `Observer` and nothing
else; a metrics client is an extra, exactly as the broker client is. Taking one
would put every user of this package on whichever was picked.

```python
from acemq_amqp import Metrics, prometheus_text

metrics = Metrics()                       # keeps the numbers in memory
mq = await connect(url, observer=metrics)
...
print(prometheus_text(metrics))           # a scrape body, nothing installed
```

```python
# Or into a registry the rest of the application already writes to:
#   pip install "acemq-amqp[prometheus]"
from acemq_amqp.prometheus import PrometheusObserver

mq = await connect(url, observer=PrometheusObserver())
```

The names are the same in Java, Go, .NET and here, so a dashboard built against
one reads against another:

| Metric | |
|---|---|
| `acemq.messages.published` / `.publish.failed` | Handed to the broker, and not. Labelled by exchange and key |
| `acemq.messages.consumed` | Delivered to a handler. Labelled by queue |
| `acemq.messages.accepted` / `.retried` / `.rejected` | What handlers decided. A retry says `where`: `consumer` or `broker` |
| `acemq.messages.dead.lettered` | Ran out of attempts and went to `{queue}.dlq` |
| `acemq.messages.parked` | Never reached the handler and went to `{queue}.parked` |
| `acemq.handler.duration` | Seconds, timed around the interceptors as well as the handler |
| `acemq.messages.in.flight` | Being handled right now |
| `acemq.retry.rung.missing` | **Worth an alert.** A long retry that had to wait in the consumer because its rung queue is not on the broker |
| `acemq.messages.set.aside.failed` | Could not be moved to a dead-letter or parking queue, so was rejected to the broker instead |

`acemq.retry.rung.missing` is the one to alert on, because nothing else shows
it. The message is still retried and the wait still happens, so a dashboard
reads as normal — while the reason the rung exists is gone, and a consumer
restart mid-wait shortens a five-minute backoff to nothing.

### Health

```python
from acemq_amqp import BrokerHealth, aggregate_health

report = await mq.health()
report.status     # HealthStatus.UP / DOWN / DEGRADED
report.healthy    # what a readiness probe should return; degraded passes

# Combined with the application's own checks, worst wins:
report = await aggregate_health(BrokerHealth(mq), my_database_check)
```

Two halves. The broker half asks the broker a question — a socket that is open
but wedged answers a socket-level check exactly as a healthy one does, right up
until something is asked of it. It asks one that **creates nothing**: an
exclusive queue is released only when the channel that declared it closes, so a
probe that declared one would leave a queue behind on every connection.

The consumer half is the one nothing outside can see. A consumer whose workers
have died without it being closed is one the broker is still sending messages to
and nothing is reading — indistinguishable from a quiet queue, and reported as
**degraded**: the connection works, and a replacement instance would almost
certainly stall the same way, so it is worth an alert and not worth taking out
of rotation.

`aggregate_health` runs checks at once rather than in turn, under a deadline, so
one that hangs cannot hang the probe with it — and a probe that hangs is a pod
that never comes back.

### Tracing

```python
# pip install "acemq-amqp[opentelemetry]"
from acemq_amqp.tracing import OpenTelemetryTracing

OpenTelemetryTracing().install(mq)
```

Metrics answer *how much*. A trace answers *what happened to this message* — this
one was published by checkout, retried twice over four minutes and given up on —
and a counter cannot.

**A consumer's span is a child of the publish that caused it**, taken from the
message's own headers rather than from whatever context happened to be current
when the delivery arrived. Those are different processes and often minutes apart,
and joining them is the one thing a messaging system needs from tracing that an
HTTP client does not.

The context travels in `traceparent` and `tracestate` — deliberately **not**
`x-acemq-` prefixed, unlike every other header here, because they are the W3C
names every other piece of tracing tooling already reads. Java, Go and .NET write
the same two.

| Span | Kind |
|---|---|
| `<destination> publish` | `PRODUCER` |
| `<queue> process` | `CONSUMER` |
| `<destination> request` | `CLIENT` — because that one *waits*, so its duration measures a responder rather than a broker |

`unroutable`, `failed` and `dead_lettered` set the span status to `ERROR`; the
others, `retried` included, do not — a retry is the system working, and a wall of
red traces that turned out fine is how people learn to ignore the colour.

**The outcome is the consumer's decision, not the handler's answer.** A
`process` span stays open past the handler and takes its outcome from what the
consumer actually did, so a message that ran out of attempts reads
`dead_lettered` and carries a `message.dead_lettered` event with the reason —
rather than `retried`, which is what somebody querying for dead letters finds
nothing under. A retry carries `message.retried` with the delay the policy
chose, which is a jittered number that exists nowhere else. The backoff itself
is not inside the span.

Spans are recorded under the instrumentation scope `org.acemq.amqp`, which is
what Java, Ruby, Go and .NET register too, so one query reads across all five.

The dependency is `opentelemetry-api`, not the SDK, so without an application
configuring one this exports nothing at all. See
[Tracing](docs/observability.md#tracing).

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
| `Saga` | Steps that must all happen across systems sharing no transaction, compensated in reverse when one fails. It reports what could **not** be undone rather than raising |
| `Scheduler` | Deliver a message later, through a ladder of time-to-live queues rather than a per-message expiration that a queue only honours at its head |
| `chain(...)` / `then(...)` | Wrap a handler in a deadline, logging and the guards; publish what a step produced onwards |
| `SchemaRegistry` | Remember what a message used to look like, so a producer can add a field without a synchronised deployment |
| `ClaimCheckCodec` | Put a payload too large for a broker into a store and send the key, above a threshold and inline below it |
| `read_stream(...)` | Read a queue that keeps what it has already handed out |

Every one is built out of the public library — handlers, envelopes, publishers —
so nothing is possible with a pattern that would not be possible without it.

### Stores that survive a restart

Three of these have a storage seam — `IdempotencyStore`, `OutboxStore`,
`SchemaRegistry` — and each in-memory implementation says in its own docstring
why it is not the one to use in production. `acemq_amqp.patterns.sql` is the one
to use:

```python
import sqlite3

from acemq_amqp.patterns import SqlOutboxStore, create_schema

connections = lambda: sqlite3.connect("acemq.db")   # or a pool's checkout
create_schema(connections)                          # development only
outbox = SqlOutboxStore(connections)

async with database.transaction() as tx:
    await place_order(tx, order)
    await outbox.add(record(mq, "orders-events", "order.placed", event),
                     connection=tx.connection)
# one commit; the order and the message are the same decision
```

`add` writes on the connection **you** hand it and does not commit it, does not
roll it back and does not close it. That is the guarantee rather than an
oversight: roll your transaction back and the message is not in the outbox,
because it never was. With no transaction to join it raises rather than opening
one — a fresh connection with autocommit on would leave a message queued for
work that never happened, which is the exact fault the pattern was adopted to
prevent.

Nothing in the module imports a database driver. It is written against the
DB-API 2.0 protocols, so `sqlite3` from the standard library works with nothing
installed and psycopg works if you have it — `paramstyle="format"` for the
latter. **The automated suite exercises `sqlite3`**; the same checks have been
run by hand against PostgreSQL 17 through psycopg 3 and pass, but they are not
in the suite, because a suite that needs a database server is a suite that gets
skipped. Anything else — MySQL, SQL Server, Oracle — needs at least a different
upsert and is not claimed.

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
and committed byte for byte by all five libraries.
`envelope-fixtures.json` is the wire contract — the headers that travel with a
message. `contract-fixtures.json` is the behaviour contract: the retry
schedules, the jitter bounds, where a wait is spent, the queue names, the rung
arguments, the declared topology and the queue types. Both are checked here
rather than assumed, and `../scripts/check-fixtures.sh` compares every library's
copy so that a file edited in one repository cannot quietly become that
repository's private opinion.

The integration tests name everything they create `pyit.*` and delete it
afterwards, so they can be pointed at a broker that is not theirs alone.

### The documentation site

```bash
pip install -e ".[dev]"
bash .github/scripts/build-docs-site.sh    # needs pandoc as well
open site/index.html
```

The prose plus an API reference under `/apidocs/`, generated by pdoc from the
docstrings in `src/`. Go links out to pkg.go.dev instead, because Go has one
built from the source of every published version; Python has nothing equivalent
for a package, so nothing else will host this. Generating it from the same
commit as the prose is what stops the two drifting apart.

`docs.yml` publishes it to GitHub Pages on a push to `main`, and fails the build
on a link to a page that does not exist or an API reference missing a module —
pdoc reports a module it cannot import as a page rather than as an error, so a
missing dependency otherwise produces a reference that is present, linked and
empty.

### Releasing

```bash
git tag v0.1.1 && git push origin v0.1.1
```

`release.yml` runs the whole suite including the integration tests, builds the
sdist and the wheel, checks that the sdist alone installs and imports, uploads
to PyPI, then installs the published version from PyPI into an empty
interpreter to confirm consumers get what was built.

It publishes through **PyPI Trusted Publishing** — the runner mints a
short-lived OIDC token for this workflow and this environment, and PyPI checks
it. No upload token is stored. That matters more here than for a feed we
control: a leaked token can publish any version to an index every Python
installation in the world resolves from, and a published version is permanent.

Two guards, both copied from the Go, Java and .NET release workflows. The
version must be a version — an allow-list, because it reaches a file name and a
package version — and it must be `0.1.x`, so a mistyped tag cannot ship a `1.0.0`
that nobody can withdraw.

Before the first tag, PyPI needs a pending publisher for `acemq-amqp` naming
this repository, `release.yml` and the `pypi` environment, and this repository
needs that environment. Until then a tag reaches the upload and PyPI refuses it,
which is the right way round: nothing is published by accident and the failure
says what is missing.

## Licence

Apache-2.0. RabbitMQ is a trademark of Broadcom Inc.; this project is not
affiliated with it.
