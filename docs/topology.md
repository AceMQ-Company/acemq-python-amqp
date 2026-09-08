# Exchanges, queues and bindings

A `Topology` is a description of what a service needs a broker to have. It can
be built, printed, checked and then applied, which is the difference between a
deployment somebody approves and one they find out about.

```python
from acemq_amqp import Topology

topology = (
    Topology()
    .exchange("orders-events", "topic")
    .queue("shipping.orders", dead_letter=True)
    .binding("shipping.orders", "orders-events", "order.placed")
)

await mq.declare(topology)
```

Every method returns the topology, so a description reads as one expression, and
the order things are added in does not matter: `apply` declares exchanges, then
queues, then bindings, whatever order they were written in.

## Queues

```python
Topology().queue("shipping.orders")
```

A durable queue is a **quorum** queue unless it is asked to be something else.
That is what Java has always declared and what the other libraries declare now,
and it has to be the same everywhere: the queue type is one more argument the
broker compares, so a Java service and a Python service that disagree about
`orders` cannot both consume it. The second one to declare is answered
`PRECONDITION_FAILED` and gets no further.

```python
Topology().queue("shipping.orders", quorum=False)   # classic
```

A classic queue is declared by leaving `x-queue-type` off entirely rather than
by writing `x-queue-type='classic'` — the spelling every AceMQ library uses, and
therefore the only one a broker finds equivalent to theirs.

| | |
|---|---|
| `durable` | survives a broker restart. On by default |
| `quorum` | replicated rather than classic. `None`, the default, means quorum for a durable queue and classic for one belonging to a single connection |
| `auto_delete` | goes away when its last consumer does |
| `exclusive` | usable only by the connection that declared it |
| `dead_letter` | also declare and wire `{name}.dlq`, and declare `{name}.parked` |
| `retry` | also declare the rung queues this policy's long waits use |
| `args` | broker-specific arguments |

`exclusive`, `auto_delete` and `durable=False` all mean a queue that belongs to
one connection, and RabbitMQ refuses a quorum queue that is any of those. Such a
queue is declared classic **without being asked**, because the alternative is a
declaration the broker rejects with a message that never mentions the word
quorum. Asking for both — `quorum=True` *and* `exclusive=True` — is refused here
instead, where the contradiction is written down rather than discovered from a
broker error.

A queue whose `args` already name a kind keeps it. That is how
[streams](patterns.md#streams) are declared, and a caller who wrote
`x-queue-type` down meant it. Writing it down *and* passing `quorum` is two
answers to one question, and is refused.

### Redeclaring

Declaring a queue that already exists is fine and does nothing, **provided every
argument matches**. Where they do not, the broker answers `PRECONDITION_FAILED`,
kills the channel, and `apply` passes the refusal on rather than swallowing it:
it means this service and the broker disagree about what the queue is, and
carrying on would leave the service using a queue that is not the one it asked
for.

The channel dying is why this library hands out channels rather than sharing
one. A refused declaration takes its channel down; on a shared channel that
would also stop every publisher and consumer that happened to be using it, for a
failure that has nothing to do with them.

## Exchanges

```python
Topology().exchange("orders-events", "topic")
Topology().exchange("audit", "fanout")
```

`direct`, `topic`, `fanout` or `headers`, durable by default.

- **direct** routes on an exact routing key. Precise, and the thing to know
  about it is that it *drops* what it cannot route and says nothing.
- **topic** routes on a pattern: `*` matches one word, `#` matches zero or more.
  `order.*` matches `order.placed` but not `order.line.added`; `order.#`
  matches both.
- **fanout** copies to every bound queue and ignores the routing key.
- **headers** routes on header values instead of the key. Rare, and slower.

Topic is the default kind and is the one to reach for: a consumer that later
needs `order.cancelled` as well binds a second key, and nothing that publishes
has to change.

## Bindings

```python
Topology().binding("shipping.orders", "orders-events", "order.placed")
```

Both the queue and the exchange must be declared by the same topology. A binding
naming a queue nothing declares is [caught before the broker sees
it](#it-is-checked-before-the-broker-sees-it).

## The default exchange

Publishing with an empty exchange name and a routing key equal to a queue name
puts the message straight on that queue:

```python
await mq.declare(Topology().queue("shipping.orders"))
await mq.publisher(routing_key="shipping.orders").send(payload)
```

No exchange, no binding. Fine for a work queue with exactly one consumer, and a
dead end the moment a second thing wants the same messages — at which point the
publisher has to change, which is the thing an exchange exists to avoid.

This library uses it internally for every hop it makes itself: a retry, a dead
letter, a parked message, and a replay all republish by queue name.

## A shape that usually works

One topic exchange per bounded context, one queue per consuming service, and a
binding per event that service cares about:

```python
topology = (
    Topology()
    .exchange("orders-events", "topic")
    .queue("shipping.orders", dead_letter=True, retry=policy)
    .binding("shipping.orders", "orders-events", "order.placed")
    .binding("shipping.orders", "orders-events", "order.cancelled")
    .queue("billing.orders", dead_letter=True, retry=policy)
    .binding("billing.orders", "orders-events", "order.placed")
)
```

The publisher names the event, not the audience. Adding a third consumer is a
queue and a binding in *that* service's own topology, and nothing that publishes
finds out.

## The queues a consumer needs to fail into

`dead_letter=True` and `retry=policy` are the two arguments worth setting on
every queue a consumer reads.

```python
from datetime import timedelta
from acemq_amqp import Topology, exponential_retry

policy = exponential_retry(6, timedelta(seconds=10))
policy.schedule()      # [timedelta(seconds=10), 20s, 40s, 80s, 160s]
policy.broker_rungs()  # [40s, 80s, 160s] — one queue each

topology = (
    Topology()
    .exchange("orders-events", "topic")
    .queue("shipping.orders", dead_letter=True, retry=policy)
    .binding("shipping.orders", "orders-events", "order.placed")
)
```

`queue()` takes the **policy**, not a list of delays, because the rungs a
consumer publishes to are derived from the policy it runs. A second copy of the
list is free to drift from the first, and the way that drift shows up is a retry
addressed to a queue nobody declared, at the moment the service is already
failing.

Both exchanges the library needs — `acemq.dlx` and `acemq.retry` — and the one
binding that brings an expired retry home are declared here rather than left to
a caller to remember. See [retries and
redelivery](reliability.md#the-two-exchanges-this-library-declares).

## Reading it before applying it

```python
print(topology)
```

```
Topology: 3 exchanges, 6 queues, 4 bindings
  declare exchange orders-events (topic, durable)
  declare exchange acemq.dlx (direct, durable)
  declare exchange acemq.retry (direct, durable)
  declare queue shipping.orders (durable, x-dead-letter-exchange='acemq.dlx', x-dead-letter-routing-key='shipping.orders.dlq', x-queue-type='quorum')
  declare queue shipping.orders.dlq (durable)
  declare queue shipping.orders.parked (durable)
  declare queue shipping.orders.retry.40s (durable, x-dead-letter-exchange='acemq.retry', x-dead-letter-routing-key='shipping.orders', x-message-ttl=40000)
  declare queue shipping.orders.retry.80s (durable, x-dead-letter-exchange='acemq.retry', x-dead-letter-routing-key='shipping.orders', x-message-ttl=80000)
  declare queue shipping.orders.retry.160s (durable, x-dead-letter-exchange='acemq.retry', x-dead-letter-routing-key='shipping.orders', x-message-ttl=160000)
  declare binding shipping.orders.dlq (from acemq.dlx on shipping.orders.dlq)
  declare binding shipping.orders.parked (from acemq.dlx on shipping.orders.parked)
  declare binding shipping.orders (from acemq.retry on shipping.orders)
  declare binding shipping.orders (from orders-events on order.placed)
```

Three lines of description become thirteen declarations, and this is what makes
that reviewable. `plan()` returns the same thing as `PlanAction` objects, for a
service that wants to log it or assert on it:

```python
for action in topology.plan():
    print(action.kind, action.name, action.detail)
```

It is deliberately **not** a difference against the live broker. AMQP offers no
way to enumerate what is there without the management API, and a plan that
quietly guessed would be worse than one that is honest about being a statement
of intent.

`queues`, `exchanges` and `bindings` are properties, for the same purpose in a
test:

```python
assert "shipping.orders.retry.40s" in topology.queues
```

### It is checked before the broker sees it

```python
topology.validate()
```

`plan` and `apply` both call it. The mistake worth catching is a binding naming
a queue the topology does not declare: the broker would accept it if the queue
happened to exist already, and the service would then depend on something
nothing declares — which works until the day it is deployed somewhere new.

Mistakes are raised where they are made rather than collected for later. Go
accumulates them on the builder because a Go builder has nowhere else to put
them; Python has an exception and a traceback pointing at the line that is
wrong, which is more useful than a message saying a topology is bad.

## Asking about a queue, and removing one

```python
await mq.queue_exists("shipping.orders")     # True or False
await mq.message_count("shipping.orders")    # how many are waiting
await mq.delete_queue("pyit.temporary")      # gone, and everything in it
```

`message_count` is what a replay tool reads to know how much work there is, and
what a test reads to know the message arrived. It counts messages *ready*, so a
message a consumer holds unacknowledged is not in it — and on a
[stream](patterns.md#streams) it says how many are retained rather than how many
are outstanding, because a stream keeps what it has already handed out.

`delete_queue` is destructive and is here for tests and for tooling. The
integration suite names everything it creates `pyit.*` and deletes it afterwards,
so it can be pointed at a broker that is not its alone.

## Where to declare

At start-up, once, before the first consumer. Declaring is idempotent, so a
service that declares its own topology on every boot is doing the right thing:
it works on an empty broker, works on a full one, and the description in the
repository is the truth rather than something a person did once with the
management UI.

What it does not do is remove. A queue nothing declares any more stays on the
broker, holding messages, until somebody deletes it. Drift accumulates in one
direction, and the topology is the record of what *should* be there rather than
what is.
