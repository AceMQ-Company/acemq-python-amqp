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
# TYPE acemq_consume_total counter
acemq_consume_total{outcome="acked",queue="shipping.orders"} 3
# TYPE acemq_publish_total counter
acemq_publish_total{exchange="orders-events",outcome="confirmed",routing_key="order.placed"} 1
# TYPE acemq_consume_in_flight gauge
acemq_consume_in_flight{queue="shipping.orders"} 2
# TYPE acemq_consume_duration summary
acemq_consume_duration_count{outcome="acked",queue="shipping.orders"} 2
acemq_consume_duration_sum{outcome="acked",queue="shipping.orders"} 0.06
```

Dots become underscores because Prometheus does not allow them in a metric name
— **and neither in a label name**, so the `routing.key` tag is exported as
`routing_key`. The label *values* keep their dots: a routing key is where the
dots mean something.

`Metrics` also prints itself, which is what a signal handler or a failing test
wants:

```python
print(metrics)
# acemq.consume.total{outcome="acked",queue="shipping.orders"} 3
# acemq.consume.duration{outcome="acked",queue="shipping.orders"} count=2 mean=0.0300s
```

and exposes the three maps for assertions:

```python
metrics.counts       # {'acemq.consume.total{outcome="acked",queue="shipping.orders"}': 3}
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

Every name below is Java's, spelled out in `MetricNames` in `acemq-amqp-api`,
character for character — so a dashboard built against one library reads against
another. Java publishes them through Micrometer and .NET through
`System.Diagnostics.Metrics`.

| Metric | |
|---|---|
| `acemq.publish.total` | Publishes. Labelled `exchange`, `routing.key` and `outcome`: `confirmed`, `unroutable`, `failed` |
| `acemq.consume.total` | Deliveries settled. Labelled `queue` and `outcome`: `acked`, `retried`, `rejected`, `dead_lettered`, `parked` |
| `acemq.consume.duration` | Seconds, timed around the interceptors as well as the handler, and carrying the same `outcome` |
| `acemq.consume.in.flight` | A gauge: how many are being handled right now |
| `acemq.messages.retried.total` | Messages given another attempt. Says `where`: `consumer`, `broker` or `requeued` |
| `acemq.messages.dead.lettered.total` | Messages set aside, tagged `outcome`: `dead_lettered` went to `{queue}.dlq`, `parked` went to `{queue}.parked` because the body would not decode or a handler returned `park(...)` |
| `acemq.retry.rung.missing` | **Worth an alert.** A long retry that had to wait in the consumer because its rung queue is not on the broker |
| `acemq.messages.set.aside.failed` | Could not be moved to a dead-letter or parking queue, so was rejected to the broker instead |
| `acemq.outbox.total` | Outbox records the relay handled. Labelled `exchange`, `routing.key` and `outcome`: `published` or `failed` |
| `acemq.outbox.lag` | **Worth an alert.** How long a record waited between being committed and being published, in seconds. See [the outbox](#the-outbox-lag-and-the-half-of-it-this-library-can-write) |

### And the names this library does not write

Worth stating, because a dashboard panel that is empty looks the same as a
service that has stopped. `MetricNames` also names `acemq.publish.duration`,
`acemq.consume.attempts`, `acemq.request.duration`, `acemq.request.total`,
`acemq.pipeline.run.duration` and `acemq.pipeline.run.total`. Nothing here emits
any of them.

`Observer` has counters, gauges and durations and no general distribution, so
`acemq.consume.attempts` has nowhere to go — and the number is on every message
as `Envelope.attempt`, which a handler that wants it records in one line.
`acemq.publish.duration` is the timing beside `acemq.publish.total`, and only
the total is written here.

The request and routing-slip names have a different reason. `Requester` and
`follow_slip` are built over a connection rather than being something the
connection knows it is doing, so nothing on that path is holding an observer.
Both are answered on the trace instead — a `request` span that ends `answered`
or `timed_out`, and a `pipeline.run_finished` event — which is where the shape
of one particular call belongs anyway.

Parking is not a metric of its own. It is `acemq.messages.dead.lettered.total`
with `outcome="parked"`, which is what Java settled on: both are a message
this queue gave up on, and an operator asking how much a queue is giving up on
wants one number that can then be split. The split still matters — a message
that failed five times and a message nothing could read are different problems
with different answers — which is exactly what the tag is for.

There is no counter for "a message arrived". Every delivery is counted once when
it is settled, and the sum across the outcomes of `acemq.consume.total` is how
many arrived — one counter that leads the others by however many messages are in
flight is a counter that makes an operator wonder which one is lying.

Every name is a constant — `METRIC_CONSUME_TOTAL`, `METRIC_RUNG_MISSING` and so
on — so an alert rule and a test can name the same string the library does. The
tag names and the publish outcomes are constants too, in `acemq_amqp.telemetry`;
the delivery outcomes are the `OUTCOME_*` names in `acemq_amqp.ack`, which the
settlement and the span already use.

### One counter with an outcome, not one counter per outcome

Python used to publish `acemq.messages.published`, `.accepted`, `.rejected` and
their neighbours — a name per outcome, and none of them a name Java knew. A
dashboard could read Python or it could read Java, and the claim in this file
that it read both was simply wrong. The mapping from the old names to these is
in the changelog, under the release that made the change.

### The counters say what the consumer decided, not what the handler asked for

The outcome on `acemq.consume.total` is chosen from the settlement, which is the
same thing [the delivery's span](#what-the-consumer-did-rather-than-what-the-handler-said)
takes its outcome from — the same word, in both places. That is worth stating
because the two are written in different files and a dashboard reads them
together:

| span outcome | counters |
|---|---|
| `acked` | `acemq.consume.total{outcome="acked"}` |
| `retried` | `acemq.consume.total{outcome="retried"}`, and `acemq.messages.retried.total` |
| `rejected` | `acemq.consume.total{outcome="rejected"}`, and `acemq.messages.dead.lettered.total{outcome="dead_lettered"}` because that is where it went |
| `dead_lettered` | `acemq.consume.total{outcome="dead_lettered"}`, and `acemq.messages.dead.lettered.total{outcome="dead_lettered"}` |
| `parked` | `acemq.consume.total{outcome="parked"}`, and `acemq.messages.dead.lettered.total{outcome="parked"}` — the same counter as a dead letter, told apart by the outcome and still a different queue |

The standalone `retried` and `dead.lettered` counters are not a redundancy, and
Java keeps them for the same reason: the outcome says what was decided about a
delivery, and the counter says how many messages are going round again or have
been set aside, which is the number an alert is written against.

A handler that asks for another attempt when there are none left is
dead-lettered, so both the counter and the span say `dead_lettered` — not
`retried`. Counting the request rather than the answer would leave the dead
letters short by exactly the messages an operator goes looking for.

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

`acemq.outbox.lag` is the third, for a service that has one, and it is the only
number that shows a relay falling behind: a committed and unpublished row
appears in no queue depth anywhere, so every other series reads as a service
with nothing to send.

Read it with `acemq.outbox.total`, because the two failures look different. A
relay that is running and behind publishes records with a climbing lag, which
the histogram shows directly. A relay that is *stopped* — its task gone, or the
broker refusing — publishes nothing, so it records no lag at all and the series
goes quiet instead of rising; what shows that is
`acemq.outbox.total{outcome="published"}` at a rate of zero, with
`outcome="failed"` climbing when the broker is the reason and nothing at all
when the sweeper is.

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

It ends with an outcome either way: `answered` when the reply came back,
`timed_out` when the deadline did, `failed` for anything else. The first is the
one worth having — a span that carried an outcome only when something went wrong
left every round trip that worked with nothing to count.

A timeout is told apart by its type, not its message. `RequestTimeoutError` is a
`TimeoutError`, so is anything `asyncio.wait_for` raises, and a caller who waits
some other way is understood without the tracing module having to know how.

### The attributes

`messaging.system`, `messaging.destination.name`, `messaging.operation`,
`messaging.message.id`, `messaging.message.conversation_id`, and three of
AceMQ's own where the conventions have no name: `messaging.acemq.message_type`,
`messaging.acemq.attempt` and `messaging.acemq.outcome`. The same names in all
five libraries, so one dashboard reads across them.

`messaging.rabbitmq.destination.routing_key` is on the **publish** span only. A
delivery's span does not carry one, because no other library's does — Java's
`consumeStarted` is not even handed a routing key — and an attribute that exists
on one library's process spans and not on the rest is worse than one nobody
writes: a query built around it comes back with the Python services and looks
like a complete answer.

`unroutable`, `failed` and `dead_lettered` set the span status to `ERROR`. The
others — including `retried` and `parked` — do not. A retry is the system
working and usually succeeds; colouring a trace red for it produces a wall of
red traces that turned out fine, which is how people learn to ignore the colour.
A parked message is a decision a handler made on purpose, and what an operator
watches for those is `acemq.messages.dead.lettered.total{outcome="parked"}` and
the queue itself. A `timed_out`
request
is red too, but by its exception rather than by its outcome: the outcome list is
Java's, character for character, and a round trip that never got its answer is
still a failure from where the caller is standing.

Spans are recorded under the instrumentation scope `org.acemq.amqp` — the
reverse-domain name, not a Python module path, and the same one Java, Ruby, Go
and .NET register. That name is what groups spans in a backend, so five
libraries tracing the same system have to answer to one of them or a query that
finds one finds only one.

### Events, not spans

`outbox.publish_failed`, `pipeline.run_finished`, `message.retried` and
`message.dead_lettered` are recorded as events on whatever span is current.
A zero-length span at the end of a trace adds a row to the waterfall and no
information. An event lands on the span that was doing the work, which is where
whoever is reading the trace is already looking.

`pipeline.run_finished` carries `pipeline`, `step` and `outcome` — bare, without
a namespace, because they are the metric tag names and all five libraries write
them that way on this event. The span *attribute* for an outcome is still
`messaging.acemq.outcome`; different thing, different place.

### What the consumer did, rather than what the handler said

The last two events are emitted by the consumer itself, on the delivery's own
`process` span. That matters more than it sounds, because the two are not the
same thing:

```python
async def handler(message: Message) -> Ack:
    return retry(TimeoutError("the payment gateway did not answer"))
```

A handler asking for another attempt when there are none left is **not**
retried; it is dead-lettered. So the `process` span stays open past the handler,
waits for the consumer's decision, and takes its outcome from that:

| what happened | outcome | event | status |
|---|---|---|---|
| the handler accepted it | `acked` | — | |
| there is another attempt | `retried` | `message.retried`, with the delay | |
| the handler rejected it | `rejected` | `message.dead_lettered` | |
| it ran out of attempts, aged out, or was fatal | `dead_lettered` | `message.dead_lettered`, with the reason | `ERROR` |
| the handler parked it | `parked` | — | |

The retry delay is the one thing about a retry nobody can reconstruct
afterwards — it comes from the policy, the attempt and, where there is jitter, a
random number — so it is recorded where it was chosen. The backoff itself is
*not* in the span: the consumer announces its decision before acting on it, so a
message waiting five minutes does not produce a five-minute handler.

The two methods remain callable for a retry or a dead letter something else
arranged:

```python
tracing.message_retried(queue, envelope, delay_ms=5000)
tracing.message_dead_lettered(queue, envelope, "out of attempts")
```

### Watching a settlement yourself

The same seam is public. An interceptor — or anything else composing one — can
ask to be told how a delivery ended:

```python
async def audited(context: ConsumeContext, handle: ConsumeNext) -> Ack:
    def settled(settlement: Settlement) -> None:
        if settlement.dead_lettered:
            audit.record(context.envelope.id, settlement.reason)

    context.when_settled(settled)
    return await handle(context)
```

The listener is called once, on the consumer's task, after the decision and
before it is carried out. `when_settled` returns whether anything will ever call
it: `False` means the chain is being run by something other than a consumer — a
test, a bridge — and no answer is coming, so do not wait for one. A listener
that raises is logged and ignored; the delivery still has to be settled.

### Propagating by hand

`propagation_headers()` injects the current context into a fresh carrier, for a
message this library does not publish — one going out through a different client,
or into a database row an outbox relay will publish later:

```python
row["headers"] = tracing.propagation_headers()   # {'traceparent': '00-...'}
```

A fresh carrier every time, so nothing already on the message is overwritten and
nothing from a previous one is left behind.

### The outbox lag, and the half of it this library can write

The lag is the one number nothing else can see. A row that has been committed
and not yet published is a message that exists, is owed to somebody, and appears
in no queue depth anywhere; a relay that has stopped looks exactly like a system
with nothing to send until this is measured.

`OutboxRelay` measures it, and reports it as a **metric**:

| | |
|---|---|
| `acemq.outbox.total` | one per record the sweep handled, tagged `outcome="published"` or `outcome="failed"` |
| `acemq.outbox.lag` | seconds, for a record that went out |

Both go through the connection's observer, labelled with the record's `exchange`
and `routing.key`, so a relay falling behind on one destination is visible as
that rather than as a single average. Nothing has to be wired up: a relay left
running under `start()` reports without anybody calling `sweep()`.

**The lag is measured from the record's own commit**, not from the sweep that
picked it up. What a lag answers is how long somebody has been owed this
message, so the wait for a sweep is part of the answer rather than the start of
it — timed from the sweep, a relay that has been down for an hour reports the
same handful of milliseconds as one that is keeping up, which is precisely the
case the number exists to show. A commit clock ahead of the sweeping one reads
as zero rather than as a negative.

Java, Go and .NET write the same two names for the same records.

#### The span attribute is the half only you can write

`messaging.acemq.outbox_lag_ms` is the same measurement on the trace, beside the
work that caused it, and `tracing.outbox_published(destination, lag_ms=...)`
writes it — **from your code, never from `OutboxRelay`**. Java's relay calls its
equivalent; this one cannot, for two reasons:

- the relay publishes with `Connection.publish_raw`, which is beneath the
  interceptor chain and so beneath the `publish` span. There is no span for the
  record it just sent.
- `start()` sweeps on a task of its own, where nothing else is current either.

An attribute has to land on a span, and a hook wired into the relay would
compute a lag on every record and hand it to nothing. A metric needs no span,
which is why the numbers above are written and this one is not. So it stays
where it works — inside a span you are holding, which is what an on-demand
`sweep()` at the end of a request already is:

```python
with tracer.start_as_current_span("checkout"):
    ...
    await relay.sweep()
    tracing.outbox_published("orders", lag_ms=lag)
```

`outbox_publish_failed(destination, reason)` has no such problem: it is an event
and is dropped when no span is current, which is the ordinary behaviour of every
other event here.

#### A relayed message has no publish span and no publish count

The same `publish_raw` that costs the relay its span costs it the ordinary
publish telemetry too. A record the relay sends produces no `<destination>
publish` span and no `acemq.publish.total`, so a service whose events all go out
through the outbox reads as a service that publishes nothing.

That is deliberate and not fixable from inside the relay. The record's bytes and
its headers were produced inside the caller's transaction, by `record(...)`, and
have to reach the broker exactly as they were committed — the class the payload
came from may not exist by the time the relay runs, and re-encoding would put
different bytes on the wire from the ones that were promised. `publish_raw`
hands them to the transport unchanged, and the publish span and the publish
counter both live on the encoding path above it. Go's and Ruby's relays publish
below their chains for the same reason.

`acemq.outbox.total` is the count to use instead. It is one per record the relay
put on the wire, which is the same population `acemq.publish.total` would have
counted, under a name that says where those messages came from.
