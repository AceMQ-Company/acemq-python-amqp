# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

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
- **A replay resets `x-acemq-attempt` to 1** unless told otherwise. A message
  dead-lettered on its last attempt would otherwise be dead-lettered again
  before a handler ever saw it.

### Fixed

- `from acemq_amqp import retry` returned the module rather than the function,
  so calling it raised `TypeError: 'module' object is not callable`. This was
  the shipped behaviour of 0.1.0.
- The sync facade's tests deleted two of the three queues they declared, leaving
  one `{queue}.parked` behind on every run.
