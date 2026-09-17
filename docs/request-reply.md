# Request and reply

```python
from acemq_amqp.patterns import Requester, serve


async def price(message: Message) -> dict[str, Any]:
    return {"pence": look_up(message.payload["sku"])}


async with await serve(mq, "price-requests", price):
    async with await Requester.open(mq, "", "price-requests") as ask:
        quote = await ask.ask({"sku": "A-1"})
```

Messaging is one-way. This draws a round trip on top of it: the caller names a
queue to answer on, the responder publishes the answer there, and the two are
paired by a correlation identifier.

## Read this before using it

Request/reply over a broker is a **synchronous shape wearing asynchronous
clothes**, and it inherits the costs of both. A caller waiting on a reply is
holding a task, a connection and a deadline; a responder that slows down turns
into a caller that stops responding; and the failure modes are a broker's rather
than an HTTP client's.

If both sides can speak HTTP or gRPC, they should. Those tools have timeouts,
retries, load balancing, circuit breakers and health checks that a messaging
library will not match, and everybody already knows how to operate them.

What this is genuinely for:

- the responder is reachable **only** on the broker — no HTTP endpoint, behind a
  firewall, on a network you do not control;
- one worker among many, where the broker is already doing the load balancing;
- a request that should **queue** rather than fail while the responder is
  redeploying.

Those cases are real, and doing them by hand means a reply queue, a correlation
identifier, a consumer to read the replies and a timeout somebody always forgets.

## The two halves are shaped differently, on purpose

```python
requester = await Requester.open(mq, exchange, routing_key)   # an object
responder = await serve(mq, queue, respond)                   # also an object
```

A `Requester` is a class because it owns state: a reply queue, a consumer of its
own, and a dictionary of futures for the calls still outstanding. It is built by
`Requester.open` rather than by its constructor because **both the queue and the
consumer have to exist before it is any use**, and neither can be created
without awaiting something. A half-built requester that looked finished is a
thing somebody would hold on to.

`serve` returns a `ResponderHandle`, which is the consumer plus the two counters
Java and .NET have always reported. It is closed the same way, is the same
`async with`, and reports the same `queue`, `running` and `in_flight`;
`responder.consumer` reaches the [`Consumer`](consuming.md) underneath for
anything not forwarded. It used to return the bare consumer, on the argument
that a responder owns nothing — which stopped being true the moment it had
counters to own.

The name is `ResponderHandle` rather than Java's `Responder` because that name
is taken here, by the **type alias** for what a responder *does*:

```python
Responder = Callable[[Message], Any]
```

A message in, an answer out, an exception for a failure. Both a coroutine
function and a plain one are accepted, and `serve` awaits the result only if it
is awaitable — a responder that does arithmetic should not have to pretend to be
async. Renaming that alias to free the word up would break every signature
written against it for a cosmetic gain, so the running object takes the longer
name and the callable keeps the short one.

## Where the reply address lives

A request carries the address **twice**: on AMQP's own `reply-to` property, and
on an `acemq-reply-to` application header. They always hold the same value, and
a responder reads **the header first and the property second**.

| | |
|---|---|
| requester | writes both, to the same value |
| responder | reads the header, falls back to the property |

The duplication is the fix for a real split. Python, Go and Ruby used to write
only the header while Java and .NET read only the property, so a Java caller and
a Python responder could not talk at all: the request arrived, the responder
found no header, and dead-lettered it for having nowhere to reply. Writing both
and reading either makes all twenty-five caller/responder pairs work.

The header is read first because it is the half that **survives a hop which
rebuilds the message** — a retry rung, a dead-letter, a shovel — where the native
property does not. The property is kept because it is what the other four
libraries read, and because a service that never heard of this library can
answer a caller that uses it.

Neither name is in the reserved `x-acemq-` namespace, deliberately: that
namespace belongs to the engine and is stripped before a handler sees one, so a
responder could never read a reply address hidden in it.

```python
from acemq_amqp.patterns import HEADER_REPLY_TO   # "acemq-reply-to"
```

None of this is visible in the API. It matters when the requester or the
responder on the other side of the queue is not this library.

`Message.reply_to` exposes the native property, for a handler writing its own
responder rather than using `serve`.

## The reply queue

One queue serves every request from a `Requester`. Build one per connection at
start-up — one per call declares and deletes a queue for every question you ask,
which is the difference between request/reply being usable at rate and being a
curiosity.

A generated queue is **transient, exclusive, auto-deleting and classic**:

```python
async with await Requester.open(mq, "pricing", "price.request") as ask:
    ask.reply_queue      # "acemq-reply-3f2a…"
```

Each of those is load-bearing. Exclusive and auto-deleting mean the broker
removes the queue when the connection that declared it goes, so a process that
crashes leaves nothing behind — this library does **not** set `x-expires` on it,
because exclusivity already covers the same failure without a timer to tune.
Classic because RabbitMQ refuses a quorum queue that is exclusive or
auto-deleting, so a reply queue could not be quorum even if replication were
worth it for answers nobody will read once this process is gone.

Name one only when replies must survive a restart:

```python
await Requester.open(mq, "pricing", "price.request", reply_queue="pricing-replies")
```

A named queue is an ordinary durable quorum queue, and it is a decision rather
than a convenience: a named reply queue that outlives its requester collects
answers nobody is waiting for.

### The one consumer in this library that declares nothing

`Requester.open` starts its consumer with `declare=False`. Every other consumer
declares `{queue}.dlq`, `{queue}.parked` and the retry rungs before it subscribes
— see [what starting a consumer declares](consuming.md#what-starting-a-consumer-declares)
— and here that would be wrong twice over. A generated reply queue has a
different name every restart, so the dead-letter queues would be two durable
queues per process that nothing ever reads and nothing ever deletes. And there
is nothing for them to catch: every reply is accepted, including one nobody is
waiting for, so no message on a reply queue is ever dead-lettered or parked.

## Asking

```python
answer = await ask.ask({"sku": "A-1"})
answer = await ask.ask(payload, timeout=timedelta(seconds=5))
answer = await ask.ask(payload, envelope=Envelope(type="price.request"))
```

`ask` returns the reply's **payload** — decoded, through the codec, like any
other message. The envelope you pass is used as given except for its correlation
identifier, which is replaced: that field is what pairs a reply with its request,
so it belongs to the call rather than to the caller.

The waiter is registered **before** the request goes out, because a fast
responder can reply before `send` has returned.

Two failures, and they are different facts:

| | |
|---|---|
| `RequestTimeoutError` | nothing came back before the deadline |
| `ResponderError` | the responder answered, and the answer was that it could not do it |

`ResponderError` is the better of the two in every way: the caller learns in
milliseconds rather than at the deadline, and learns why. That is the whole
reason `serve` sends a failure back rather than swallowing it.

`RequestTimeoutError` is **also a `TimeoutError`**, so the clause a Python caller
actually writes catches it:

```python
try:
    quote = await ask.ask(request, timeout=timedelta(seconds=5))
except TimeoutError:
    ...
```

`except AceMQError` still catches it too; the extra base only adds a way to be
caught. It is also how
[`request_span`](observability.md#tracing) tells a deadline apart from a
responder that failed, without importing the pattern module.

**A timeout says no answer arrived. It does not say nothing happened.** The
request may still be queued, still being handled, or already done with the reply
lost on the way back. Retrying is therefore a decision about whether the
responder is idempotent, not a reflex — and where the work takes money or sends
email, a timeout is a question for [idempotency](patterns.md#idempotency) or for
a human, not for a loop.

The default deadline is thirty seconds. `Requester.open(..., timeout=...)` moves
it for every call; `ask(..., timeout=...)` for one. Zero or negative is refused
at `open` rather than producing a requester that can never succeed.

### Closing fails what is still waiting

```python
await ask.close()      # or leave the async with
```

Anything still outstanding is failed with `RequestTimeoutError` saying *the
requester was closed while … was outstanding* — rather than left to wait out its
whole timeout for an answer that can no longer arrive. The reason is the sentence
in the exception, not the type, which is why the type is the same one a deadline
raises.

## Answering

```python
responder = await serve(mq, "price-requests", price, concurrency=4, prefetch=10)
```

`serve` passes anything else through to
[`Connection.consume`](consuming.md), so `retry`, `codec`, `prefetch`,
`concurrency`, `tag` and `declare` all mean what they mean there.

**A responder handles one request at a time by default, and every caller behind
a slow one is blocked.** `concurrency` is the first setting to reach for when
request/reply feels slow — and a responder is the best possible case for raising
it, because a responder that has to be fast usually spends its time waiting on
something else.

What the responder does with a failure is worth knowing, because three different
things happen:

| | |
|---|---|
| The responder raised, and the failure reached the caller | the request is **rejected** — the caller has its answer, and a retry would answer the same question twice |
| The responder raised, and the failure could not be delivered | **retried** — the caller has not been told and is still waiting, so another attempt is the only path that can still reach it |
| The responder answered, and the answer could not be delivered | **retried**, which repeats the work — this is why a responder should be idempotent |
| The request named nowhere to reply | **rejected** as a `FatalError`, so it is dead-lettered rather than looped: redelivery cannot make a return address appear |

A reply that reaches no queue is **not** an error. A generated reply queue is
auto-deleting and belongs to a caller that may have given up, so a reply landing
nowhere is the ordinary aftermath of a timeout rather than something to retry.

### The reply's envelope

`serve` publishes the answer to the reply queue through the default exchange,
with a fresh envelope carrying:

- the request's `correlation_id`, which is what the requester matches on;
- `causation_id` set to the request's `id`, so a trace reads as an answer to a
  specific question rather than to a conversation;
- this connection's `origin`;
- `acemq-error` when the responder failed, carrying `TypeName: message`.

## The numbers

Four counters and two metrics, and all of them mean what they mean in Java and
.NET.

```python
async with await serve(mq, "price-requests", price) as responder:
    async with await Requester.open(mq, "", "price-requests") as ask:
        quote = await ask.ask({"sku": "A-1"})

        responder.answered        # requests answered, counted before the reply left
        responder.unanswerable    # requests that named nowhere to reply
        ask.timed_out             # callers that gave up
        ask.unmatched             # replies that arrived with nobody waiting
```

| | |
|---|---|
| `responder.answered` | Requests answered, counted **before** the reply left |
| `responder.unanswerable` | Requests that arrived with no reply address. Anything above zero means a caller is using `send` where it means to `ask` |
| `ask.timed_out` | Callers that gave up |
| `ask.unmatched` | Replies that arrived with nobody waiting — almost always the timeout being too short |

`unmatched` rising while `timed_out` rises is the signature of a responder that
is slower than callers expect. Nothing is broken; the timeout is wrong.

### What the counters promise

`answered` is incremented **before** the reply is published, so a caller holding
its answer can rely on the count already including it. The other order looks
more natural and is wrong: it leaves a window where the reply is in the caller's
hands and the responder still says nothing has been answered, which is a
monitoring dashboard reporting an idle service that is demonstrably working. **A
publish that fails takes its increment back**, so this counts replies that were
sent rather than replies that were attempted.

A responder that *failed* counts no answer. The caller is still told — in
milliseconds rather than at its deadline — but the reply is the thing carrying
the failure, and counting it would let a service that fails every request report
a perfectly healthy answered rate.

**The counters exist before the responder subscribes**, so a request the broker
hands over during start-up — what a queue with a backlog looks like from in here
— is counted like any other. Neither number needs a wait before it can be
trusted, and code that sleeps before reading one is working around a defect that
is not here. Java says the same thing about field initialisation order and .NET
had to lift its counters out of the responder to get it; here the
`ResponderHandle` is built before `connection.consume` is called, and the
handler closes over it.

### The two metrics

| Metric | |
|---|---|
| `acemq.request.total` | Round trips. Labelled `routing.key` and `outcome`: `answered`, `timed_out` or `failed` |
| `acemq.request.duration` | Seconds, the same labels |

Both are written by the **caller**, and only by the caller. A responder's side is
an ordinary queue and is counted on
[`acemq.consume.total`](observability.md#what-is-reported) like any other; what
nothing else can see is how long *asking* took, because the publish and the
reply's delivery are two unrelated hops and neither of them is the round trip.

The timer starts before the request is published, so the publish is inside the
number — the caller's wait begins there. A call that times out is recorded too,
at its deadline, because a p99 that quietly drops the slowest calls says a
service is fast right up to the point where nothing answers at all. `answered`
covers a reply that came back whatever it said: a responder answering "I could
not do it" answered, and that failure belongs in the reply rather than in the
round trip. `failed` is for the publish itself not getting out, which is the one
case where there was never a question to be slow about.

They go wherever the connection's [`Observer`](observability.md) goes, so a
connection with no observer pays for none of this.

Java tags the same two with `message.type` and `transport` as well. Python tags
no metric with either — `acemq.publish.total` has never carried them — and a
duration labelled differently from the total beside it cannot be divided into
it, so the narrower set is kept and stated rather than widened here alone.

### The round trip on the trace, as well

`OpenTelemetryTracing.request_span` is a CLIENT span around a call that waits,
and it ends with an outcome either way — `answered`, `timed_out`, or `failed`:

```python
with tracing.request_span("pricing", envelope):
    quote = await ask.ask(payload, envelope=envelope)
```

CLIENT rather than PRODUCER because this one waits: its duration is a round trip
and not a handover, and a backend that knows the difference shows it against the
responder's latency rather than the broker's. The span is the shape of one
particular call; the metrics are the shape of all of them.


## From a program with no event loop

There is none. [`acemq_amqp.sync`](getting-started.md#not-running-an-event-loop)
has `SyncConnection`, `SyncPublisher` and `SyncConsumer`, and no blocking
requester — `Requester` creates an `asyncio.Future` per outstanding call and
waits on it, which is exactly the thing a blocking facade cannot hand back to a
thread that is not on the loop.

A blocking **responder** can be written by hand: `SyncConnection.consume` gives
the handler a `Message`, `message.reply_to` and
`message.envelope.headers["acemq-reply-to"]` give the address, and a
`SyncPublisher` bound to it sends the answer. A blocking **requester** cannot,
and a program that needs one should run the async API on a loop of its own
rather than have this library pretend otherwise.

## Related

- [Patterns](patterns.md#request-and-reply) — the same thing in the pattern tour
- [Publishing](publishing.md) and [consuming](consuming.md)
- [Retries, redelivery and shutdown](reliability.md) — and
  [idempotency](patterns.md#idempotency), which decides whether a timeout can be
  retried
- [Metrics, health and tracing](observability.md)
