# Tutorial 4 — Seeing what happens

**20 minutes.** Continues from
[tutorial 3](tutorial-exactly-once.md).

A message goes in and does not come out. This tutorial is about answering *where
did it stop* in under a minute, rather than by grepping four services' logs for
an order id.

## Step 1 — Turn it on

Nothing is on by default. That is the one real difference from the Java and .NET
versions of this tutorial, where instrumentation auto-detects what is on the
classpath: here you pass an `Observer`, and a service that wants no numbers pays
for none.

```python
from acemq_amqp import Metrics

metrics = Metrics()

async with await connect(url, observer=metrics) as mq:
    ...

print(metrics)
```

```
acemq.consume.total{outcome="acked",queue="shipping.orders"} 3
acemq.publish.total{exchange="orders-events",outcome="confirmed",routing.key="order.placed"} 3
acemq.consume.in.flight{queue="shipping.orders"} 0
acemq.consume.duration{outcome="acked",queue="shipping.orders"} count=3 mean=0.0021s
```

Counters first, then gauges, then the timings — and note `routing.key` with a
dot. Java and .NET both tag a publish with the fully-qualified name, so a
dashboard that groups by it reads the same in all three.

`Metrics` holds them in memory. It is the whole of what you need to *see* the
instrumentation, and it is genuinely useful in a test — assert on
`metrics.counts` and you are asserting on what a dashboard will show.
`counts`, `gauges` and `durations` are properties rather than methods, and each
hands back a copy keyed by the metric name and its labels.

`Observer` is three methods — `count`, `observe`, `gauge` — so the real one is a
small class over whatever your service already reports to. Two ship:

```python
from acemq_amqp import NullObserver          # the default: nothing, and cheaply
from acemq_amqp.prometheus import PrometheusObserver
```

```bash
pip install "acemq-amqp[prometheus]"
```

```python
from prometheus_client import start_http_server

start_http_server(9464)

async with await connect(url, observer=PrometheusObserver()) as mq:
    ...
```

```bash
curl -s localhost:9464/metrics | grep -E '^acemq_(publish|consume)_total'
```

```
acemq_publish_total{exchange="orders-events",outcome="confirmed",routing_key="order.placed"} 20.0
acemq_consume_total{outcome="acked",queue="shipping.orders"} 16.0
acemq_consume_total{outcome="dead_lettered",queue="shipping.orders"} 4.0
```

Twenty published, sixteen handled, four given up on. The `outcome` label is what
makes that readable.

If you would rather not add a dependency, `prometheus_text(metrics)` renders a
`Metrics` in the exposition format from the core package, which has no
dependencies at all:

```python
from acemq_amqp import prometheus_text

body = prometheus_text(metrics)      # serve this from your own handler
```

The names are dotted in code and underscored when scraped:
`acemq.consume.total` becomes `acemq_consume_total`.

## Step 2 — What is actually reported

Every name is Java's, character for character, so a dashboard built against one
library reads against another.

| | |
|---|---|
| `acemq.publish.total` | publishes, labelled `exchange`, `routing.key` and `outcome`: `confirmed`, `unroutable`, `failed` |
| `acemq.consume.total` | deliveries settled, labelled `queue` and `outcome`: `acked`, `retried`, `rejected`, `dead_lettered`, `parked` |
| `acemq.consume.duration` | seconds, timed around the interceptors as well as the handler, carrying the same `outcome` |
| `acemq.consume.in.flight` | a gauge: how many are being handled right now |
| `acemq.messages.retried.total` | labelled `where`: `consumer`, `broker` or `requeued` |
| `acemq.messages.dead.lettered.total` | labelled `outcome`: `dead_lettered` went to `{queue}.dlq`, `parked` went to `{queue}.parked` |
| `acemq.retry.rung.missing` | a long retry that had to wait in the consumer because its rung queue is not there |
| `acemq.messages.set.aside.failed` | could not be moved to a dead-letter or parking queue, so was rejected to the broker instead |
| `acemq.outbox.total` | outbox records the relay handled, labelled `outcome`: `published` or `failed` |
| `acemq.outbox.lag` | how long a record waited between commit and publish, in seconds |

**Build your dashboard from that list and nothing else.** Java's `MetricNames`
also spells `acemq.publish.duration`, `acemq.consume.attempts`,
`acemq.request.duration`, `acemq.request.total`, `acemq.pipeline.run.duration`
and `acemq.pipeline.run.total`. **Nothing here emits any of them**, and a panel
that is empty looks exactly like a service that has stopped — which is the worst
possible thing for a panel to look like at 3am.

Two reasons, and neither is an oversight waiting to be fixed. `Observer` has
counters, gauges and durations and no general distribution, so
`acemq.consume.attempts` has nowhere to go — and the number is on every message
as `envelope.attempt`, which a handler that wants it records in one line. And
`Requester` and the pipeline runner are built *over* a connection rather than
being something the connection knows it is doing, so nothing on those paths is
holding an observer; both are answered on the trace instead. See
[the names this library does not write](observability.md#and-the-names-this-library-does-not-write).

There is also no counter for "a message arrived". Every delivery is counted once
when it is **settled**, and the sum across the outcomes of `acemq.consume.total`
is how many arrived. One counter that leads the others by however many messages
are in flight is a counter that makes an operator wonder which one is lying.

### The counters say what the consumer decided

Not what the handler asked for. A handler that returns `retry()` with no attempts
left is dead-lettered, and both the counter and the span say `dead_lettered`.
Counting the request rather than the answer would leave the dead letters short by
exactly the messages an operator goes looking for.

### The three to alert on

**`acemq.retry.rung.missing` above zero**, because nothing else shows it. The
message is still retried and the wait still happens, so throughput, failures and
dead letters all read as normal — while a five-minute backoff has quietly become
no backoff at all across a restart. The log line beside the metric names the
missing queue.

**`acemq.messages.set.aside.failed` above zero.** A message that should have gone
to `{queue}.dlq` could not, and was handed back to the broker's own
dead-lettering instead.

**`acemq.outbox.lag` climbing**, for a service that has an outbox. It is the only
number that sees a relay falling behind: a committed and unpublished row appears
in no queue depth anywhere.

And what *not* to alert on: queue depth alone. A queue is a buffer and it is
supposed to have things in it. Depth that is not draining matters; depth does
not.

## Step 3 — Health, for a probe

```python
report = await mq.health()

report.status        # HealthStatus.UP | DEGRADED | DOWN
report.healthy       # UP or DEGRADED
report.detail        # why, when it is not UP
report.parts         # {'consumers': 2, 'in-flight': 0, 'round-trip': 0.0013}
```

The broker half asks the broker a real question — whether a queue nobody has ever
declared exists — rather than looking at a socket. A TCP connection that is open
but wedged (the broker paused, the network black-holing) answers a socket-level
check exactly as a healthy one does, right up until something is asked of it.
It creates nothing, which matters: declaring a temporary queue instead would
leave one on the broker for the life of every connection.

The consumer half is the one a probe usually wants and **nothing outside can
see**. A consumer whose workers have died without it being closed is one the
broker is still sending messages to and nothing is reading — and from outside
that is indistinguishable from a quiet queue.

### Three states, not a boolean

`DEGRADED` is why. A stalled consumer means the connection works and a
replacement instance would almost certainly stall the same way — so it is worth
waking somebody and not worth taking this pod out of rotation. Map `UP` and
`DEGRADED` to 200 and `DOWN` to 503, and a Kubernetes probe reads the status code
without parsing anything.

It costs a round trip, so it is not something to call per request. Wire it to a
readiness probe and let the probe's interval decide.

```python
from acemq_amqp import BrokerHealth, aggregate_health

report = await aggregate_health(BrokerHealth(mq), DatabaseHealth(pool))
```

`BrokerHealth` makes the connection a `HealthCheck` like any other, so the broker
and your own dependencies go into one report — which is what a readiness endpoint
actually needs.

## Step 4 — The trace crosses the broker

This is the part that matters and the part most setups do not have.

```bash
pip install "acemq-amqp[opentelemetry]"
```

```python
from acemq_amqp.tracing import OpenTelemetryTracing

tracing = OpenTelemetryTracing()
tracing.install(mq)
```

`install` registers a publish interceptor and a consume interceptor, which is all
it is — there is no separate machinery, and you could write the same thing
yourself against [the interceptor seam](interceptors.md).

```python
# service A
await mq.publisher("orders-events", "order.placed").send(order)

# service B, a different process, possibly minutes later
await mq.consume("shipping.orders", ship)
```

The publish span writes a **W3C `traceparent`** into the message headers. The
consumer reads it back and starts its span as a child of the publish. One trace
spanning both services and the queue between them, with the queue wait visible as
the gap.

`traceparent` is the W3C standard header rather than an `x-acemq-` invention, so
a service that has never heard of this library — a Java consumer, an
OpenTelemetry-instrumented HTTP hop — joins the same trace.

**Spans are named after the destination**: `orders-events publish` and
`shipping.orders process`, rather than after a function.

| attribute | |
|---|---|
| `messaging.system` | `rabbitmq` |
| `messaging.destination.name` | the exchange when publishing, the queue when consuming |
| `messaging.message.id` | the envelope id — the same one idempotency keys on |
| `messaging.message.conversation_id` | the correlation id, constant across a whole flow |
| `messaging.acemq.attempt` | which attempt this delivery is |
| `messaging.acemq.outcome` | what the consumer decided |

That fourth one is what turns a trace into a story: everything caused by one
order shares it.

### The round trip a metric cannot see

A request that waits for a reply gets its own span, and you open it yourself
because [`Requester`](request-reply.md) is not something the connection knows it
is doing:

```python
with tracing.request_span("pricing", envelope):
    quote = await ask.ask(payload, envelope=envelope)
```

CLIENT rather than PRODUCER, because this one waits: its duration is a round trip
and not a handover. It ends with an outcome **either way** — `answered`,
`timed_out` or `failed` — and the first of those is the one worth having: a span
that carried an outcome only when it went wrong leaves every successful round
trip with no outcome at all, which is not a thing a dashboard can divide by.

## Step 5 — Reading it when something is wrong

An order was placed and never shipped. In order:

| what you see | what it usually means |
|---|---|
| `publish.total{outcome="unroutable"}` above zero | the broker took the message and nothing was bound to receive it. A routing key typo, or a binding missing after a deploy — and silent by default, because the broker *did* accept it |
| a publish span with no matching process span | same thing, from the trace side |
| two process spans for one message | it was retried. Look at the outcome and reason on the first |
| a process span with a long gap before it | it queued. Not an error — check `consume.in.flight` and scale |
| `consume.in.flight` pinned at your concurrency | every handler is busy and the queue is growing behind them. This is the number that tells you to scale before the depth does |
| `dead.lettered.total{outcome="parked"}` climbing | messages nothing can read. Usually a producer that deployed a format change ahead of its consumers; look in `{queue}.parked` |
| `retry.rung.missing` above zero | a rung queue was never declared. The log line names it |
| no spans at all | it was never published. Check the outbox table from tutorial 3 — the row will be there, uncommitted to the broker, and no broker metric anywhere will show it |

That last row is the one that catches people. If the outbox has it and the relay
does not, no amount of broker monitoring will show you anything, because as far
as the broker is concerned the message does not exist. `acemq.outbox.lag` is what
sees it.

## Step 6 — What is not instrumented

Worth knowing, because each is a question this library cannot answer for you:

- **No publish latency.** `acemq.publish.total` counts publishes and nothing
  times them, so the confirm round trip is invisible from here.
- **No request metrics.** `Requester` reports no counters of its own —
  see [request and reply](request-reply.md#numbers-and-the-ones-this-library-does-not-report).
  The span is the answer.
- **Consumer spans start after decoding**, so a message that cannot be decoded is
  parked with no span of its own and its trace ends at the publish. It is still
  counted, on `acemq.consume.total{outcome="parked"}`.
- **Pipeline steps have no span each.** They are legible as
  `{queue} process` spans, and the run is reported as a
  `pipeline.run_finished` event rather than a span of its own.

## What you have

Metrics a dashboard can read, health a probe can read, and traces that cross the
broker and cross languages. That is the set —
[metrics, health and tracing](observability.md) has the detail on each.

## Where to go next

You have finished the tutorials.

- [Retries, redelivery and shutdown](reliability.md) — the full policy,
  dead-letter and replay guide
- [Patterns](patterns.md) — the thirteen things every service ends up writing
- [Request and reply](request-reply.md) and [streams](streams.md)
- [Codecs](serialization.md) — including Avro schema resolution, which is how a
  producer adds a field and deploys before its consumers
- [Testing without a broker](testing.md)
