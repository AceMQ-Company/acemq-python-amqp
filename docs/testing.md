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

## The wire contract

`tests/fixtures/envelope-fixtures.json` is generated by the Java implementation
and shared with the Go and .NET libraries. A test reads each fixture, builds the
envelope and writes the headers back: no header gained, none lost, none renamed.

That is the definition of "the same wire contract", and it is checked here
rather than assumed. See [the envelope](envelope.md#how-this-is-kept-honest).

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
