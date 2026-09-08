# Metrics and health

Two questions an operator asks about a service that consumes a queue, and the
library is the only thing that can answer either.

How much is going through, how much is failing and how long a handler takes are
numbers nobody outside the process can see. Whether the connection is really up
— as opposed to a socket that is open and wedged — is a question only something
holding the connection can ask.

## No hard dependency

The library calls a three-method `Observer` and nothing else. Python's standard
library has no metrics interface at all, which is why this module defines a
small one; taking a dependency on a metrics client instead would put every user
of this package on whichever one was picked. That is the same argument the
transport already makes and wins.

```python
from acemq_amqp import Observer


class Observer(Protocol):
    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None: ...
    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None: ...
    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None: ...
```

Implement it against OpenTelemetry, statsd, a log line, or anything else. It is
a `Protocol`, so there is nothing to inherit from. `NullObserver` is the default
and does nothing.

## If you only want the numbers

```python
from acemq_amqp import Metrics, prometheus_text

metrics = Metrics()
mq = await connect(url, observer=metrics)
...
print(prometheus_text(metrics))
```

`Metrics` keeps the numbers in memory and is enough for a health endpoint, a
test, or a signal handler that prints them. `prometheus_text` renders it as a
scrape body using only the standard library:

```
# TYPE acemq_messages_consumed counter
acemq_messages_consumed{queue="shipping.orders"} 3
# TYPE acemq_messages_published counter
acemq_messages_published{exchange="orders-events",key="order.placed"} 1
# TYPE acemq_messages_in_flight gauge
acemq_messages_in_flight{queue="shipping.orders"} 2
# TYPE acemq_handler_duration summary
acemq_handler_duration_count{queue="shipping.orders"} 2
acemq_handler_duration_sum{queue="shipping.orders"} 0.06
```

Dots become underscores because Prometheus does not allow them in a metric name.

`Metrics` also prints itself, which is what a signal handler or a failing test
wants:

```python
print(metrics)
# acemq.messages.consumed{queue="shipping.orders"} 3
# acemq.handler.duration{queue="shipping.orders"} count=2 mean=0.0300s
```

and exposes the three maps for assertions:

```python
metrics.counts       # {'acemq.messages.consumed{queue="shipping.orders"}': 3}
metrics.gauges
metrics.durations    # DurationSummary(count, total, fastest, slowest), and .mean
```

`acemq_amqp.telemetry.metric_key(metric, labels)` builds the same key, with the
labels sorted — so
the same labels always give the same key however the mapping was built. Unsorted,
one counter quietly becomes several that each hold part of the answer.

## Into a registry the application already has

```bash
pip install "acemq-amqp[prometheus]"
```

```python
from acemq_amqp.prometheus import PrometheusObserver

mq = await connect(url, observer=PrometheusObserver())
```

For the case where the numbers have to join a registry the rest of the
application is already writing to. It takes a registry, a metric-name namespace
and its own histogram buckets:

```python
PrometheusObserver(registry, namespace="checkout", buckets=(0.01, 0.1, 1.0, 10.0))
```

The default buckets run from 5ms to 60s, which covers a handler that calls a
database and a handler that calls something slow.

## What is reported

The names are the same in Java, Go, .NET and here, so a dashboard built against
one library reads against another. Java publishes them through Micrometer and
.NET through `System.Diagnostics.Metrics`.

| Metric | |
|---|---|
| `acemq.messages.published` / `.publish.failed` | Handed to the broker, and not. Labelled by exchange and key |
| `acemq.messages.consumed` | Delivered to a handler. Labelled by queue |
| `acemq.messages.accepted` / `.retried` / `.rejected` | What handlers decided. A retry says `where`: `consumer` or `broker` |
| `acemq.messages.dead.lettered` | Ran out of attempts and went to `{queue}.dlq` |
| `acemq.messages.parked` | Never reached the handler and went to `{queue}.parked` |
| `acemq.handler.duration` | Seconds, timed around the interceptors as well as the handler |
| `acemq.messages.in.flight` | A gauge: how many are being handled right now |
| `acemq.retry.rung.missing` | **Worth an alert.** A long retry that had to wait in the consumer because its rung queue is not on the broker |
| `acemq.messages.set.aside.failed` | Could not be moved to a dead-letter or parking queue, so was rejected to the broker instead |

Every name is a constant — `METRIC_CONSUMED`, `METRIC_RUNG_MISSING` and so on —
so an alert rule and a test can name the same string the library does.

### The one to alert on

`acemq.retry.rung.missing`, because nothing else shows it.

The message is still retried and the wait still happens, so throughput,
failures and dead letters all read as normal — while the reason the rung queue
exists is gone, and a consumer restart mid-wait shortens a five-minute backoff
to nothing. It means a queue was declared without its rungs; the log line beside
the metric names the missing queue and the `Topology().queue(..., retry=policy)`
call that would declare it. See
[when a rung is missing](reliability.md#when-a-rung-is-missing).

`acemq.messages.set.aside.failed` is the other one worth a rule: it means a
message that should have gone to `{queue}.dlq` could not, and was handed back to
the broker's own dead-lettering instead.

## Health

```python
report = await mq.health()

report.status     # HealthStatus.UP / DOWN / DEGRADED
report.healthy    # what a readiness probe should return; degraded passes
report.detail     # why, when it is not simply up
report.parts      # {'consumers': 2, 'in-flight': 0, 'round-trip': 0.0021}
print(report)     # 'degraded: these consumers have stopped reading: shipping.orders'
```

Two halves.

**The broker half** asks the broker a real question — whether a queue with a
random name exists — and times the round trip. A TCP connection that is open but
wedged — the broker paused, the network black-holing — answers a socket-level
check exactly as a healthy one does, right up until something is asked of it.

It asks a question that **creates nothing**. An exclusive queue is released only
when the channel that declared it closes, so a probe that declared one would
leave a queue on the broker for the life of the connection and a new one behind
every restart.

**The consumer half** is the one a probe usually wants and nothing outside can
see. A consumer whose workers have died without it being closed is one the
broker is still sending messages to and nothing is reading, and from outside
that is indistinguishable from a quiet queue. It is reported as **degraded**:
the connection works, and a replacement instance would almost certainly stall
the same way, so it is worth an alert and not worth taking out of rotation.

It costs a round trip, so it is not something to call per request. Wire it to a
readiness probe and let the probe's interval decide how often.

### Three states, not a boolean

`degraded` exists because "working, but not as well as it should" is worth an
alert and is not worth taking an instance out of rotation — the replacement will
almost certainly be degraded too. `report.healthy` is `True` for `up` and
`degraded`, and `False` for `down`, which is what a readiness probe should
return.

### Combining checks

```python
from acemq_amqp import BrokerHealth, aggregate_health

report = await aggregate_health(BrokerHealth(mq), database_check, timeout=5.0)
report.parts["broker"]      # the connection's own report
report.parts["database"]
```

`HealthCheck` is a `Protocol`: a `name` property and an `async check()`
returning a `HealthReport`. `BrokerHealth(mq)` wraps the connection as one, and
takes a `label` if `"broker"` is not the right name for it.

The combined status is the **worst** of them: one thing being down makes the
whole report down, because a service that cannot reach its broker is not ready
however healthy the rest of it is.

Checks run **at once** rather than in turn, so a slow one does not add its
latency to the others, and under a deadline, so one that hangs cannot hang the
probe with it — and a probe that hangs is a pod that never comes back. A check
that times out is reported as down with the reason, which is the honest reading:
something that will not answer in five seconds is not something a request can
depend on. A check that *raises* is caught and reported as down rather than
being allowed to take the probe down.

## Wiring it to an endpoint

There is no HTTP server here, because a Python service already has one and it is
not this library's business which. Two handlers, in whichever framework:

```python
from acemq_amqp import Metrics, prometheus_text

metrics = Metrics()
mq = await connect(url, observer=metrics)


async def scrape():
    return Response(prometheus_text(metrics), media_type="text/plain; version=0.0.4")


async def ready():
    report = await mq.health()
    return JSONResponse(
        {"status": report.status.value, "detail": report.detail},
        status_code=200 if report.healthy else 503,
    )
```

Neither is authenticated by anything here. A metrics endpoint says which queues
exist, how much traffic each carries and when a service started failing, and a
health endpoint says which dependencies it has. Put both behind whatever the
rest of the service is behind, or on a port that is not published.
