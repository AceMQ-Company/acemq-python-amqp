# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added

- **Sagas.** `Saga` runs named steps in order and, when one fails, compensates
  the completed ones in reverse — the order the world was changed in, because a
  compensation often depends on state a later step has not yet altered. A step
  may be a coroutine, because a step that publishes a message will be. Two
  behaviours are the pattern rather than an accident of it and are pinned by
  tests: a compensation that itself fails is logged, collected into
  `SagaResult.unresolved` and does **not** stop the others, and a completed step
  with no compensation is skipped rather than treated as an error. `run` returns
  a `SagaResult` instead of raising: `complete`, `compensated`, `failed_at`,
  `failure`, `completed`, `unresolved`, and `has_unresolved` — which is the flag
  to alert on, because everything else a saga reports is recoverable by
  construction and those are effects that happened and were not undone.

- **Scheduled delivery.** `Scheduler` delivers a message later through a ladder
  of five time-to-live queues — an hour, ten minutes, a minute, ten seconds, a
  second — that dead-letter into `acemq.schedule.due`, where the scheduler either
  hops the message down a rung or delivers it. A per-message expiration would be
  simpler and wrong: a classic queue expires messages only at its head, so a
  one-minute message queued behind a four-hour one is delivered in four hours and
  nothing reports it. The whole topology is byte for byte what the Java library
  declares — the `acemq.schedule` direct exchange, `acemq.schedule.{1h,10m,1m,10s,1s}`
  and `acemq.schedule.due`, all classic, every rung carrying exactly
  `x-message-ttl`, `x-dead-letter-exchange` and `x-dead-letter-routing-key`, and
  every queue bound on its own name — so a Python service and a Java service
  scheduling on one broker declare the same queues rather than meeting a
  `PRECONDITION_FAILED`. An integration test declares them from Python and then
  again from a second connection with Java's literal argument table, and proves
  the broker accepts that and refuses a different one.

- The scheduler carries four headers, deliberately without the reserved
  `x-acemq-` prefix — `x-schedule-exchange`, `x-schedule-routing-key`,
  `x-schedule-due-at` (epoch milliseconds, as Java's `Instant.toEpochMilli()`
  writes it) and `x-schedule-content-type`. The payload is encoded once, when it
  is scheduled, and moved as bytes from then on: the control consumer reads with
  `BytesCodec` and never decodes a payload, and the content type travels with it
  so the eventual consumer can still choose a codec. None of the four is passed
  on to the destination.

- The scheduler's control consumer is opened with `declare=False`. A consumer
  declares its dead-letter queues at start-up, which is right for a service queue
  and wrong for a shared one: without it every service running a scheduler would
  leave `acemq.schedule.due.dlq` and `acemq.schedule.due.parked` behind on the
  broker. `schedule_topology()` is public, so a deployment can declare the ladder
  from a migration and run its services with no `configure` permission at all.

## [0.3.0] - 2026-09-08

### Added

- `declare_where_failures_go(transport, queue, retry)` declares the dead-letter
  and retry half of a queue's topology without declaring the queue itself, which
  is the one arrangement `Topology` cannot describe: it refuses a binding on a
  queue it does not declare, and a consumer must not guess at the source queue.
  It is what `consume()` calls at start-up.

- **The claim check.** `ClaimCheckCodec` wraps any codec, sends a payload of at
  least `DEFAULT_THRESHOLD` (64 KiB) to a `ClaimCheckStore` and puts the key on
  the wire in its place; anything smaller travels inline, unchanged. The framing
  is byte for byte what the Java library writes — `0xAC 0x01 0x00` before an
  inline payload and `0xAC 0x01 0x01` before a bare UTF-8 key — so a document a
  Java service put aside is one a Python service can redeem, and a body neither
  wrote goes to the wrapped codec untouched. `InMemoryClaimCheckStore` and
  `FilesystemClaimCheckStore` implement the seam; `claim_key_of` and
  `is_claim_check` read a body without fetching anything, which is what an
  operator looking at a dead-letter queue wants.
- **Database-backed stores**, in `acemq_amqp.patterns.sql`.
  `SqlIdempotencyStore`, `SqlOutboxStore` and `SqlSchemaRegistry` are the three
  storage seams over a real database instead of a dictionary, written against
  the DB-API 2.0 protocols so the module imports no driver: `sqlite3` works with
  nothing installed and psycopg works with `paramstyle="format"`. The suite
  exercises `sqlite3`; the same checks have been run by hand against PostgreSQL
  17 through psycopg 3 and pass, but are not in the suite because a test needing
  a database server is a test that gets skipped. `create_schema` and
  `schema_ddl` make the tables, or print them for a migration tool to own.
- `SqlOutboxStore.add` writes on a connection the **caller** supplies, and
  neither commits nor closes it, so the outbox insert and the business write
  commit together or not at all — the property `InMemoryOutboxStore` says in its
  own docstring that it does not have. With no transaction to join it raises
  rather than opening a connection of its own.

### Changed

- **A consumer declares the queues it will need when a message fails, before it
  subscribes**: `acemq.dlx`, `{queue}.dlq`, `{queue}.parked` and their two
  bindings, plus `acemq.retry`, the rung queues and the binding home when its
  retry policy has waits the broker holds. `mq.consume(...)` used to declare
  nothing at all and leave every one of those to `Topology`. The union is the
  same once a topology has been applied, so a correctly deployed service sees no
  change beyond a few extra declares at start-up; what it fixes is the service
  deployed *without* one, where a consumer that gave up republished to
  `{queue}.dlq`, the broker could not route it, and the message was discarded
  without a trace. Every declaration carries the arguments `Topology` writes, so
  either order is accepted and neither is a `PRECONDITION_FAILED`. The source
  queue is **not** declared — it belongs to whoever set the service up, and
  `x-dead-letter-exchange` on it can still only be asked for through
  `Topology().queue(name, dead_letter=True)`.
- `mq.consume(..., declare=False)` turns that off, for a login with no
  `configure` permission on the vhost and for a tool draining a queue it does not
  own. `sync.SyncConnection.consume` takes it too. A `Requester` passes it
  itself: a reply queue is a mailbox for one process that never dead-letters
  anything, and declaring for it would leave two durable queues behind per
  restart.
- `acemq.retry.rung.missing` and the fallback to waiting in the consumer stay,
  because a rung can still be absent — a consumer started with `declare=False`
  declares none, and a rung deleted under a running consumer is gone whoever
  made it.
- `idempotent(...)` calls `store.confirm(key)` when a handler accepts, if the
  store has one. A store that hands out a *lease* rather than a fact — so a
  consumer that dies holding a message does not block its redelivery — needs to
  be told when the lease becomes a fact. Duck-typed, so a store without one,
  including `InMemoryIdempotencyStore`, behaves exactly as before.

## [0.2.0] — 2026-09-07

> ### ⚠ Migrating: a retry rung now returns through `acemq.retry`
>
> A rung queue is declared with `x-dead-letter-exchange` set to `acemq.retry`,
> where 0.1.0 used `""` (the default exchange). **A rung that already exists
> with the old argument cannot be redeclared with the new one** — AMQP forbids
> changing a queue's arguments in place, so the declare is refused with
> `PRECONDITION_FAILED`.
>
> This only affects a broker that has already run 0.1.0 with a retry policy
> whose delays reach 30 seconds. Delete the `{queue}.retry.*` queues and let
> them be declared again; they hold nothing but messages waiting to be retried,
> and anything in them at the time is lost, so drain first if that matters.
>
> The change exists because the other four libraries all use `acemq.retry`, and
> two services on one queue that disagree about a rung's arguments cannot both
> consume it.

> ### ⚠ Migrating: a durable queue is now a quorum queue
>
> `Topology().queue(...)` declares a durable queue with
> `x-queue-type: quorum`, where 0.1.0 sent no `x-queue-type` at all and
> therefore got a classic queue. **A queue that already exists as classic
> cannot be redeclared as quorum.** AMQP offers no way to change a queue's type
> in place, so the declare is refused with `PRECONDITION_FAILED` — the broker
> answers `inequivalent arg 'x-queue-type' ... received 'quorum' but current is
> 'classic'` — and the service cannot consume the queue at all.
>
> Draining and recreating the queue is the only way through it. Stop the
> consumers, let the queue empty or move what is on it somewhere else, delete
> it, and let this version declare it again; anything still on the queue when it
> is deleted is lost. A service that cannot do that yet can keep the queue
> classic on purpose with `Topology().queue(name, quorum=False)`, which declares
> exactly what 0.1.0 declared.
>
> `{queue}.retry.{delay}`, `{queue}.dlq` and `{queue}.parked` are **not**
> affected: they stay classic, as they are in Java, so an existing one is
> redeclared without complaint. Nor is any queue that is exclusive,
> auto-deleting or transient — a generated reply queue, a temporary queue —
> because RabbitMQ refuses a quorum queue that is any of those, and one is now
> declared classic without being asked rather than being refused by the broker.
>
> The change exists because Java has declared quorum since it had deployments,
> a queue type is compared as strictly as any other argument, and a Java service
> and a Python service consuming one queue cannot disagree about it.

### Added

- **TLS and credentials.** `amqps://` with the system trust store by default, a
  custom CA, client certificates for mutual TLS, and credentials supplied apart
  from the URL so a password need never be embedded in a connection string.
  `Credentials` does not render its secret in `repr`. Turning verification off
  is reachable only through `without_verifying_the_broker(...)`, which says in
  its own docstring what it surrenders.
- **Interceptors.** One function taking `(context, next)` wraps publishing and
  handling, so logging, tracing and tenancy are written once rather than in
  every handler. A publish interceptor sees the payload before the codec runs;
  a consume interceptor sees the envelope after decoding, and what it leaves is
  what the handler gets.
- **Telemetry and health.** Metric names identical to the Go, Java and .NET
  libraries; an `Observer` protocol with a null default; an in-memory `Metrics`
  and a `prometheus_text()` renderer with no dependencies at all.
  `PrometheusObserver` lives behind a `[prometheus]` extra, as the transport
  lives behind `[rabbitmq]`. `Connection.health()` answers up, down or degraded,
  and its broker check creates nothing.
- **The pattern library**: idempotency, outbox, request/reply, replay, ordered
  handling, consumer groups, routing slips, pipelines, a schema registry and
  stream reading. Each is built out of the public API, so nothing is possible
  with a pattern that is not possible without one.
- **A way to install it.** A release workflow publishing to PyPI through Trusted
  Publishing, and a docs workflow publishing the reference to GitHub Pages.
  Until 0.2.0 the `v0.1.0` tag existed and nobody could install it.

### Changed

- **A rung returns through the named `acemq.retry` exchange** rather than the
  default exchange, with one binding `{queue} -> acemq.retry -> {queue}`, and
  dead letters route through `acemq.dlx`. See the migration note above.
- **A durable queue is declared quorum**, as it is in Java, Go, .NET and Ruby.
  `Topology().queue(..., quorum=False)` still declares a classic one, and a
  queue that is exclusive, auto-deleting or transient stays classic on its own
  because a quorum queue cannot be any of those. The rungs, `{queue}.dlq` and
  `{queue}.parked` stay classic too. `QUEUE_TYPE_ARG` and `QUORUM_QUEUE_TYPE`
  are exported for a caller who needs to write the argument themselves. See the
  migration note above.
- **A replay resets `x-acemq-attempt` to 1** unless told otherwise. A message
  dead-lettered on its last attempt would otherwise be dead-lettered again
  before a handler ever saw it.

### Fixed

- `from acemq_amqp import retry` returned the module rather than the function,
  so calling it raised `TypeError: 'module' object is not callable`. This was
  the shipped behaviour of 0.1.0.
- The sync facade's tests deleted two of the three queues they declared, leaving
  one `{queue}.parked` behind on every run.
