# Tutorial 3 — Never processing twice

**25 minutes.** Continues from
[tutorial 2](tutorial-surviving-failure.md). Needs the broker, and uses
`sqlite3` from the standard library as the database.

Tutorial 2 gave you retries. **Retries create duplicates — that is not a bug in
the design, it is the design.** This tutorial is about what that means when the
handler takes money.

## The thing to understand first

There is no exactly-once delivery over a network. Not here, not in Kafka, not
anywhere. The broker sends a message and waits for an acknowledgement, and if
the acknowledgement does not arrive it cannot tell these apart:

- the message never arrived;
- it arrived, was handled, and the acknowledgement was lost.

It has to choose. Deliver again and risk handling twice, or do not and risk
losing it. Every broker worth using chooses **at least once**, because a
duplicate is a problem you can solve and a lost message is not.

So "exactly once" is not a delivery guarantee you switch on. It is
**at-least-once delivery plus an idempotent consumer**, and the second half is
yours. This tutorial builds it, and then builds the other half of the problem
that nobody expects.

## Step 1 — See the duplicate

```python
charged = []


async def take_payment(message: Message) -> Ack:
    charged.append(message.payload["paymentId"])   # the money moves
    print(f"charged, total {len(charged)}")
    raise RuntimeError("the receipt email service is down")


consumer = await mq.consume(
    "payments.new", take_payment, retry=fixed_retry(3, timedelta(milliseconds=200))
)
```

```
charged, total 1
charged, total 2
charged, total 3
```

Three charges for one payment, and the customer is right to be annoyed. The
failure had nothing to do with the charge.

## Step 2 — Remember what you have done

```python
from acemq_amqp.patterns import InMemoryIdempotencyStore, idempotent

seen = InMemoryIdempotencyStore()

consumer = await mq.consume(
    "payments.new",
    idempotent(seen, take_payment),
    retry=fixed_retry(3, timedelta(milliseconds=200)),
)
```

```
charged, total 1
```

The key is the **envelope's id**, which this library sets on publish and which
survives the retry ladder — the message is republished for each attempt, but the
identity goes with it. Every attempt after the first finds the id already
recorded and skips the handler.

A duplicate is **accepted**, not rejected. The work was done, so the message has
been handled, and dead-lettering it would raise an alarm about something that
went right.

### Two operations, not three

```python
class IdempotencyStore(Protocol):
    async def first_time(self, key: str) -> bool: ...
    async def forget(self, key: str) -> None: ...
```

Java and .NET spell this `claim` / `confirm` / `release`. Python's protocol is
two methods, and the difference is real rather than cosmetic: **`first_time` is
the claim**, named for what a caller needs to know rather than for what it does
internally, and `forget` is the release. When the handler does not accept, the
key is forgotten so the retry can actually run.

The third operation is optional and is asked for by duck typing: when the
handler accepts and the store **has** a `confirm` method, `idempotent` calls it.
A store that hands out a *lease* rather than a fact needs telling when the lease
becomes a fact; one that does not — `InMemoryIdempotencyStore` — is unaffected,
and is two methods rather than three because it has no third thing to say.

### The claim happens before the work

That ordering is the whole design. Marking afterwards leaves a window — the
process dies between the charge and the mark — where the charge happened and
nothing recorded it, and the retry charges again.

The cost is the opposite failure: the process dies *after* claiming and *before*
charging, and the claim blocks a retry that should have happened. That is why a
real claim is a **lease** — if it is not confirmed within the claim timeout, it
expires and the message can be tried again.

Which trade you want is a real question, and this one is chosen deliberately: a
payment not taken is a support ticket, a payment taken twice is a chargeback.

And what it buys is a guard, not a guarantee. Between the handler finishing and
the acknowledgement reaching the broker there is still a gap where a crash leaves
a message that will be delivered again. Only a store written **in the same
transaction as the work** closes it — which is why `IdempotencyStore` is a
`Protocol` rather than a class.

## Step 3 — Make it survive a restart

`InMemoryIdempotencyStore` is a dictionary. Restart the process and it has
forgotten everything, so every in-flight message is a duplicate waiting to
happen — and two replicas of the same service do not share one, so each handles
the message once. "Once each" is not once.

```python
import sqlite3

from acemq_amqp.patterns import SqlIdempotencyStore, create_schema


def connections() -> sqlite3.Connection:
    return sqlite3.connect("payments.db")


create_schema(connections, idempotency="acemq_idempotency", outbox=None, registry=None)

seen = SqlIdempotencyStore(connections, claim_timeout=timedelta(minutes=5))
```

Now the record is a row, `first_time` is an insert, and the uniqueness is the
database's problem — which databases are extremely good at. Two replicas racing
to claim the same id: one insert wins, the other is told `False`.

**No driver dependency.** Nothing in `acemq_amqp.patterns.sql` imports a
database driver. It is written against the DB-API 2.0 connection and cursor
protocols, so one class serves `sqlite3`, psycopg and anything else following
PEP 249. The one thing that differs is the placeholder, and that is a
constructor argument: `paramstyle="qmark"` for `sqlite3` — the default — and
`paramstyle="format"` for psycopg.

`create_schema` is for development. In production the tables belong in whatever
migration tool already owns your schema, and `schema_ddl()` prints exactly the
statements it would run so you can paste them into one:

```python
from acemq_amqp.patterns import schema_ddl

for statement in schema_ddl(dialect="postgres"):
    print(statement)
```

A library that creates tables at start-up is a library deciding when your
database changes.

Old rows do not live forever:

```python
removed = await seen.purge_expired()     # run this on a schedule
```

Retention is how far back a duplicate can arrive. A day is the default and is
usually plenty; it should comfortably exceed your longest retry ladder.

## Step 4 — The other duplicate, and the harder one

Idempotency fixes duplicates on the *consuming* side. There is a matching
problem on the publishing side, and it is worse because it produces **missing**
messages rather than extra ones:

```python
# Do not do this.
async def place_order(order):
    await orders.save(order)                                    # database
    await mq.publisher("orders-events", "order.placed").send(order)   # broker
```

Two systems, one of which your transaction covers. Three outcomes:

1. Both succeed. Fine.
2. The save fails. Nothing published. Fine.
3. **The save succeeds and the publish fails** — or succeeds and the transaction
   then rolls back. The order exists and nobody was told.

Number three is the one that pages you at 3am, because the order is genuinely in
the database and the warehouse genuinely never heard about it, and no log
anywhere says so. Swapping the two lines does not help: then you announce an
order that might still roll back.

## Step 5 — The outbox

Write the message to the same database, in the same transaction, as the thing it
describes. Then one commit decides both.

```python
from acemq_amqp.patterns import OutboxRelay, SqlOutboxStore, record

create_schema(connections, idempotency=None, outbox="acemq_outbox", registry=None)
outbox = SqlOutboxStore(connections)
```

In your transaction:

```python
tx = sqlite3.connect("payments.db")
try:
    await save_order(tx, order)
    await outbox.add(
        record(mq, "orders-events", "order.placed", order),
        connection=tx,               # ← the transaction's own connection
    )
    tx.commit()                      # one decision, both rows
except BaseException:
    tx.rollback()
    raise
finally:
    tx.close()
```

**The two writes must use the same connection.** A record written on a different
connection is a record in a different transaction, which is the bug this pattern
exists to prevent, reproduced faithfully. `add` refuses rather than working
around it: a store with no transactional connection to write into raises instead
of opening one, because opening one here would silently reintroduce the gap.

`record(...)` **encodes** the payload right there, inside the transaction, and
that two-step is the point. What is stored is bytes and a content type, so the
record survives a deployment that changes the class the payload was written from
— and the relay becomes a thing that moves bytes and needs to know nothing about
what they mean.

Then something publishes what was committed:

```python
async with OutboxRelay(mq, outbox, interval=timedelta(seconds=1)) as relay:
    relay.start()
    ...
```

It polls, reads a batch, publishes, and marks each record published.
`await relay.sweep()` runs one batch on demand — at the end of a request, say,
rather than up to a second later — and is what a test uses to drive it without
waiting for a tick.

`start()` twice is a no-op rather than a second sweeper. Two relays on one store
publish everything twice, and the shape of that mistake is a service that starts
one relay per worker.

### What the outbox actually gives you

**Not** exactly-once publishing. The relay can publish a record and die before
marking it, and the next relay publishes it again. What it gives you is
**at-least-once publishing, atomic with the database write** — the message is
never lost, and never sent for work that rolled back.

The duplicate that remains is handled by step 2, on the consumer. That is the
whole architecture: the outbox stops messages going missing, idempotency stops
them counting twice, and together they are what people mean by "exactly once".

## Step 6 — Order of operations

For a handler that takes money, in order:

1. `first_time` the message id. Stop if it is already claimed.
2. Do the work, in a database transaction.
3. `record(...)` any resulting messages into the outbox **on that same
   connection**.
4. Commit.
5. Let `idempotent` confirm the claim and acknowledge.

Each step exists to close a window opened by the one before it. Steps 1 and 3
can both join your transaction — `first_time` and `add` each take a
`connection=` — and when they do, the claim becomes durable exactly when the work
does, and the gap in step 2's docstring closes with it.

## What to watch in production

| | |
|---|---|
| `acemq.outbox.lag` | **the one to graph.** How long a record waited between being committed and being published. A committed and unpublished row appears in no queue depth anywhere, so every broker metric reads as a service with nothing to send |
| `acemq.outbox.total{outcome="published"}` at a rate of zero | a relay that has **stopped**, which records no lag at all — the series goes quiet rather than rising, so the lag histogram alone will not show it |
| `acemq.outbox.total{outcome="failed"}` climbing | the relay is running and the broker is refusing |
| Idempotency table size | should plateau. Growing forever means nothing is calling `purge_expired` |
| `first_time` returning `False` | your actual duplicate rate. Zero forever means either nothing retries or the store is not wired in |

## Next

**[Tutorial 4 — Seeing what happens](tutorial-observability.md).** All of the
numbers above, and where they come from.
