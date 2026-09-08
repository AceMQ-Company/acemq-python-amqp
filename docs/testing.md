# Testing without a broker

Most of what is worth testing about a service that consumes a queue does not
need a broker. What a handler decides, what an interceptor adds, whether a
policy waits as long as it was meant to, what a topology would declare — all of
it is decidable in milliseconds, and none of it involves Docker.

There is no `memory://` here. The Go library ships an in-memory transport and
this one does not: `Transport` is a `Protocol` with six methods, so the fake
that a test needs is a small class rather than a shipped subsystem, and every
level above it works against the protocol rather than against a client library.

## Testing a handler on its own

A handler takes a `Message` and returns an `Ack`. Both are ordinary values, so
the smallest useful test involves nothing at all:

```python
from acemq_amqp import Action, Envelope, Message, accept


async def test_it_ships_an_order():
    message = Message(
        payload={"orderId": "o-1"},
        envelope=Envelope(type="order.placed"),
        routing_key="order.placed",
        content_type="application/json",
        redelivered=False,
        body=b'{"orderId": "o-1"}',
    )

    assert (await ship(message)).action is Action.ACCEPT
```

`Ack` carries the action and the error, and prints itself, so an assertion that
fails says what the handler decided. `accept()`, `retry(e)` and `reject(e)`
compare equal to each other by value, being a frozen dataclass, so
`assert await ship(message) == accept()` works too.

This is the test to write most of. Everything else on this page is about the
paths that only exist once a broker is involved.

## Testing what a topology would do

```python
from acemq_amqp import Topology, exponential_retry


def test_the_rungs_are_declared():
    policy = exponential_retry(6, timedelta(seconds=10))
    topology = Topology().queue("shipping.orders", dead_letter=True, retry=policy)

    assert "shipping.orders.dlq" in topology.queues
    assert "shipping.orders.retry.40s" in topology.queues
    assert "acemq.retry" in topology.exchanges
```

`plan()` gives the same thing as `PlanAction` objects, and `validate()` raises
on a description that cannot be right. Nothing here touches a broker.

## Testing a retry policy

```python
def test_the_policy_is_the_one_we_meant():
    policy = exponential_retry(6, timedelta(seconds=10))

    assert policy.schedule() == [
        timedelta(seconds=10), timedelta(seconds=20), timedelta(seconds=40),
        timedelta(seconds=80), timedelta(seconds=160),
    ]
    assert policy.broker_rungs() == [
        timedelta(seconds=40), timedelta(seconds=80), timedelta(seconds=160),
    ]
```

`schedule()` reports the delays **without** jitter, which is exactly why it is
the thing to assert on: a jittered schedule is not reproducible and a test
against one is a test that fails on a Tuesday.

`next_wait(attempt, age)` is the one-message version, and it *does* jitter below
the threshold — assert on `wait.in_broker` and on a range rather than on an
exact delay.

## Testing an interceptor

`consume_chain` and `publish_chain` are exported, so a chain runs without a
connection:

```python
from acemq_amqp import ConsumeContext, Envelope, accept, consume_chain


async def test_the_tenant_reaches_the_handler():
    seen = {}

    async def handler(context: ConsumeContext):
        seen.update(context.envelope.headers)
        return accept()

    context = ConsumeContext(
        queue="shipping.orders",
        envelope=Envelope(type="order.placed", headers={"tenant": "acme"}),
        payload={"id": "7"},
        body=b'{"id": "7"}',
        content_type="application/json",
        routing_key="order.placed",
        redelivered=False,
    )
    await consume_chain([my_interceptor], handler)(context)

    assert seen["tenant"] == "acme"
```

## Testing what the library decides

The consumer's decisions — what is acknowledged, what is republished, what ends
up in the dead-letter queue and with which reason — are the part of this library
most worth testing and the part a broker tells you least about.

A fake transport makes them observable without one. `tests/fake_transport.py` in
this repository is the whole thing, in about two hundred lines; it is not
shipped, because a fake belongs to the tests that use it and every project wants
a slightly different one. Copy it, or write the six methods:

```python
class Transport(Protocol):
    async def declare_queue(self, name: str, spec: QueueSpec) -> None: ...
    async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None: ...
    async def bind(self, queue: str, exchange: str, routing_key: str) -> None: ...
    async def publish(self, exchange: str, routing_key: str, message: Outbound) -> PublishResult: ...
    async def consume(self, queue: str, spec: ConsumeSpec, deliver) -> Subscription: ...
    async def close(self) -> None: ...
```

Build a `Connection` on it directly rather than through `connect`:

```python
from acemq_amqp import Connection, accept, retry

transport = FakeTransport()
mq = Connection(transport, retry=exponential_retry(3, timedelta(0)))

await mq.consume("orders", lambda m: retry(RuntimeError("nope")))
await transport.deliver("orders", b'{"id": "7"}')

assert transport.sent_to("orders")          # republished, one attempt further on
```

Everything above `Transport` is then the real thing: the envelope rules, the
codec negotiation, the retry arithmetic, the dead-letter reasons, the metrics.
That is the point of the protocol being where it is — a fake exercises the same
code path RabbitMQ does, rather than a second implementation of it that can
drift.

Two optional protocols sit beside it and are worth implementing when a test
needs them: `MessageSource` (`pull`, which replay reads through) and
`QueueAdmin` (`queue_exists`, `message_count`, `delete_queue`, which the health
check and the queue questions go through).

## It is not kinder than a broker

A fake worth having is one that refuses what RabbitMQ refuses. Two details from
this repository's own, both of which caught real bugs:

- **A publish to a queue that was never declared is unroutable.** That is how
  the "the dead-letter queue is not there" path gets tested at all.
- **A rejected message goes back to the *head* of its queue**, which is what
  RabbitMQ does and is the detail everything about replay turns on: returning a
  message one at a time means reading the same one for ever and never seeing
  what is behind it.

A fake that quietly accepts everything tests that your code compiles.

## Testing the numbers

```python
from acemq_amqp import METRIC_DEAD_LETTERED, Metrics
from acemq_amqp.telemetry import metric_key

metrics = Metrics()
mq = Connection(transport, observer=metrics)
...
assert metrics.counts[metric_key(METRIC_DEAD_LETTERED, {"queue": "orders"})] == 1
```

`metric_key` builds the same key the library does, with the labels sorted, so a
test does not have to know the rendering.

## Against a real broker

```bash
pytest -m "not integration"     # everything above. No broker, no Docker

ACEMQ_TEST_BROKER=amqp://guest:guest@localhost:5672/ pytest -m integration
```

Integration tests are marked, so `pytest` on a laptop with no Docker runs
everything that does not need one, and the integration suite skips rather than
fails when it is not told where a broker is.

What it is for is the questions a fake cannot answer: whether RabbitMQ agrees
about the rung arguments, whether a quorum queue really is refused when it is
declared exclusive, whether an expired rung message really comes back through
`acemq.retry`, whether a `PRECONDITION_FAILED` really kills only its own
channel.

The suite names everything it creates `pyit.*` and deletes it afterwards, so it
can be pointed at a broker that is not its alone. Write yours the same way.

### TLS

Two brokers, because one broker cannot both accept a client that has no
certificate and refuse it:

```bash
export ACEMQ_TEST_TLS_CERTIFICATES=/path/to/certs   # ca.crt, client.crt, client.key,
                                                    # other-ca.crt, stranger.crt, stranger.key
export ACEMQ_TEST_TLS_BROKER=amqps://localhost:25891/          # ssl_options.verify = verify_none
export ACEMQ_TEST_TLS_MUTUAL_BROKER=amqps://localhost:25893/   # verify_peer, fail_if_no_peer_cert
pytest -m integration
```

These prove the handshake rather than the configuration: the certificate the
broker presented, the version and cipher that were agreed, that a broker the
system trust store does not vouch for is **refused**, and that a client
certificate signed by the wrong authority gets no further than one signed by
none.

## How we know the five libraries agree

Two fixture files, both generated by the Java library and committed **byte for
byte** by all five: Java, Go, .NET, Python and Ruby. Neither is written by hand
and neither may be edited in one repository alone — a copy that has drifted is
worse than no fixture at all, because the library carrying it still passes its
own suite. It is simply agreeing with the wrong file, in private.

### The wire contract

`tests/fixtures/envelope-fixtures.json` is what travels with a message. A test
reads each fixture, builds the envelope and writes the headers back: no header
gained, none lost, none renamed. See
[the envelope](envelope.md#how-this-is-kept-honest).

### The behaviour contract

`tests/fixtures/contract-fixtures.json` is what the libraries *do*, and
`tests/test_contract.py` holds this one to it:

- **`retrySchedules`** — five named policies with their unjittered delays, the
  rungs each needs, and a table of `{attempt, age}` retry decisions covering both
  limits.
- **`jitter`** — the factor, both directions, the bounds, the floor, and that it
  applies to a wait spent here and never to one spent on a rung.
- **`brokerWaitThreshold`** — thirty rows of "where is this wait spent".
- **`naming`** — `.dlq`, `.parked`, and the rung names.
- **`rungArguments`** — each rung queue with **exactly three** arguments. The
  count matters as much as the values: a fourth is a `PRECONDITION_FAILED` for
  the second service to declare that queue.
- **`topology`** — the whole declared plan, exchange by exchange and binding by
  binding, with each entry labelled `declaredBy` — the topology, the consumer, or
  both. Three tests read that label: one that a topology produces all three
  groups, one that a topology without a policy produces everything but the rungs,
  and one that a **consumer** starting against a bare broker produces exactly the
  `consumer` and `both` groups and nothing from `topology`.
- **`queueTypeDefaults`** — quorum source queue, classic rungs and dead letters,
  everything durable.

The expectations are derived rather than read back wherever there is a second way
to reach them. A test that reads a number out of the fixture and compares it to
the same number read out of the library proves only that both can be read, so the
policies are built from the fixture's own `how` string, the doubling is checked
as arithmetic, the threshold table is recomputed from the rule written beside it,
and the rung names are rendered a second time from the convention.

This exists because of what happened without it. Java shipped `exponential` with
a multiplier of 5.0 and 10% jitter through ten releases while Go, .NET, Python
and Ruby doubled with 20%: `exponential(5, 1s, 1m)` produced `1s, 5s, 25s, 60s`
there and `1s, 2s, 4s, 8s` here, and every suite passed, because every library
was testing its own arithmetic against its own expectations. Three more — the
rung's dead-letter exchange, the source queue's dead-letter arguments, and the
queue type — turned up the same way on the same afternoon, all four found by a
person reading five codebases side by side.

### A disagreement the suite settled

**Who declares the dead-letter queues.** The first `declaredBy` reading found
that Java's consumer declared `acemq.dlx`, `{queue}.dlq`, `{queue}.parked`, their
bindings and the whole retry half when it started, and that this library's
consumer declared **nothing at all** — it published to a rung, counted
`acemq.retry.rung.missing` when the broker could not route it, and left every
queue to `Topology`. Go, .NET and Ruby sat between the two.

The union is identical once a topology has been applied, so nothing was ever
missing in a correctly deployed system. The problem was the system where it had
not been: a consumer that gave up republished to `{queue}.dlq`, the broker could
not route it, and an unroutable message is discarded without a trace. Declaring
at start-up costs a few idempotent declares once per consumer and removes a
silent-loss path, so all five libraries moved to Java's side — the reverse of the
last disagreement, which Java lost. The rule is that the safer behaviour wins,
not that the majority does.

A test asserts the new split rather than the old one, and would fail if this
library's consumer ever stopped declaring its half again. An integration test
proves the point end to end: a queue on a broker nothing was applied to, a
handler that gives up, and the message on `{queue}.dlq` where it used to vanish.

### Where the libraries do not agree yet

Two of them, asserted as disagreements rather than smoothed over. A suite that
loosened an assertion to go green would be back to proving that a library agrees
with itself.

**Sub-second rung names.** Java renders a delay under a second in milliseconds —
`orders.new.retry.500ms` — where Go, Python and Ruby round to whole seconds and
reach `orders.new.retry.0s`. The fixture records this rather than settling it.
Out of reach through the default thirty-second threshold: a wait that short is
spent in the consumer and never names a queue.

**The default maximum message age.** Java's `RetryPolicy` has no "no limit"
value. Its default `maxMessageAge` is 365 days and it is compared against
unconditionally, so a message exactly a year old is abandoned by a policy nobody
asked to give up on age. Go, .NET, Ruby and Python all read zero as "never" and
carry on. Four libraries to one, and the fixture records Java's answer; six of
its decision rows therefore disagree with what this library does. The test
collects every disagreement and fails if the set is anything other than those
six, so a *new* divergence cannot hide behind a known one.

### Checking the copies

```bash
../scripts/check-fixtures.sh
```

Compares every library's copy of both files and fails on a single changed
character. `tests/test_contract.py` also pins the digest itself, so an edit made
in this repository fails here rather than waiting for someone to run the
workspace script.

## Running it all

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"

pytest -m "not integration"
ruff check .
mypy
```

`mypy` runs in strict mode over `src` and `tests`, and the package ships
`py.typed`, so a service that type-checks its own code gets the library checked
with it.
