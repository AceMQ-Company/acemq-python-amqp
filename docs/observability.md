# Metrics, health and tracing

Three questions an operator asks about a service that consumes a queue, and the
library is the only thing that can answer any of them.

How much is going through, how much is failing and how long a handler takes are
numbers nobody outside the process can see. Whether the connection is really up
— as opposed to a socket that is open and wedged — is a question only something
holding the connection can ask. And what happened to *one particular message*,
across the two services and the several minutes it took, is a question nothing
outside the library can even join up: see [Tracing](#tracing).

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

## Tracing

Metrics answer *how much*. A trace answers *what happened to this message* — and
the two are not substitutes. A counter says a thousand messages were
dead-lettered; a trace says this one was published by checkout, retried twice
over four minutes and given up on, which is the question somebody is actually
holding when they open a dashboard.

```bash
pip install "acemq-amqp[opentelemetry]"
```

```python
from acemq_amqp import connect
from acemq_amqp.tracing import OpenTelemetryTracing

mq = await connect(url)
OpenTelemetryTracing().install(mq)
```

`install` registers a publish interceptor and a consume interceptor. Nothing else
changes.

The dependency is `opentelemetry-api`, not the SDK — the package a library is
supposed to depend on. Without an SDK installed and configured by the
application, the API's no-op implementation runs and this exports nothing at all,
so importing it can never start sending data nobody asked for. The application
picks the SDK, the exporter and the sampler.

### The join is the point

A consumer's span is a child of the **publish that caused it**, taken from the
message's own headers rather than from whatever context happened to be current
when the delivery arrived. Those are different processes, different machines and
often minutes apart, and joining them is the one thing a messaging system needs
from tracing that an HTTP client does not.

Reading the ambient context instead would produce a trace that looks joined up
and joins the wrong things — a delivery attached to the connection's context, or
to the *previous* message's.

### The headers

`traceparent` and `tracestate`. Deliberately **not** `x-acemq-` prefixed, unlike
every other header this library writes: they are the W3C names every piece of
tracing tooling already reads, so a message published here joins up in a consumer
that has never heard of AceMQ, and one published by such a consumer joins up
here. Java, Go and .NET write the same two names for the same reason.

### The spans

| Name | Kind | When |
|---|---|---|
| `<destination> publish` | `PRODUCER` | a message goes out |
| `<queue> process` | `CONSUMER` | a handler runs |
| `<destination> request` | `CLIENT` | a request waits for a reply |

`request` is `CLIENT` rather than `PRODUCER` because that span *waits*. Its
duration means something different as a result — a slow publish is a slow broker,
a slow request is a slow responder — and the kind is what makes a backend show
them apart rather than averaging one into the other.

```python
with tracing.request_span("pricing", envelope):
    answer = await requester.ask(...)
```

### The attributes

`messaging.system`, `messaging.destination.name`, `messaging.operation`,
`messaging.message.id`, `messaging.message.conversation_id`,
`messaging.rabbitmq.destination.routing_key`, and three of AceMQ's own where the
conventions have no name: `messaging.acemq.message_type`,
`messaging.acemq.attempt` and `messaging.acemq.outcome`. The same names in all
four libraries, so one dashboard reads across them.

`unroutable`, `failed` and `dead_lettered` set the span status to `ERROR`. The
others — including `retried` — do not. A retry is the system working and usually
succeeds; colouring a trace red for it produces a wall of red traces that turned
out fine, which is how people learn to ignore the colour.

### Events, not spans

`outbox.publish_failed`, `pipeline.run_finished`, `message.retried` and
`message.dead_lettered` are recorded as events on whatever span is current:

```python
tracing.message_retried(queue, envelope, delay_ms=5000)
tracing.message_dead_lettered(queue, envelope, "out of attempts")
```

A zero-length span at the end of a trace adds a row to the waterfall and no
information. An event lands on the span that was doing the work, which is where
whoever is reading the trace is already looking.

### Propagating by hand

`propagation_headers()` injects the current context into a fresh carrier, for a
message this library does not publish — one going out through a different client,
or into a database row an outbox relay will publish later:

```python
row["headers"] = tracing.propagation_headers()   # {'traceparent': '00-...'}
```

A fresh carrier every time, so nothing already on the message is overwritten and
nothing from a previous one is left behind.
