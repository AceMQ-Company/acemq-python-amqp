# Changelog

All notable changes to this project are documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the version is `0.x` the public API may change in any release.

## [Unreleased]

### Added

- **`OutboxRelay` reports what it is doing, so a stopped relay is visible.** It
  counts every record it handles on `acemq.outbox.total`, tagged
  `outcome="published"` or `outcome="failed"`, and times every record it
  publishes on `acemq.outbox.lag` — both through the connection's observer,
  labelled with the record's `exchange` and `routing.key`. Java, Go and .NET
  write the same two names.

  A committed and unpublished row is a message that exists, is owed to somebody,
  and appears in no queue depth anywhere, so until now a relay that had stopped
  read from outside exactly like a service with nothing to send.

  **The lag is measured from the record's own commit**, not from the sweep that
  picked it up. What a lag answers is how long somebody has been owed this
  message, so the wait for a sweep is part of the answer rather than the start of
  it; timed from the sweep, a relay an hour behind reports the same handful of
  milliseconds as one that is keeping up. A commit timestamp built without a
  zone is read as UTC, and a commit clock ahead of the sweeping one reads as zero
  rather than as a negative duration no histogram can hold.

  Nothing has to be wired up: a relay left running under `start()` reports
  without anybody calling `sweep()`. That is the whole point — the span-attribute
  path documented below needs a caller holding a span, and a relay sweeping on
  its own task has none.

- **`METRIC_OUTBOX_TOTAL` and `METRIC_OUTBOX_LAG`** in `acemq_amqp.telemetry`,
  re-exported from the package root, and both carrying a `HELP` line in
  `PrometheusObserver`.

### Changed

- **`tracing.outbox_published(...)` is still application-only, and now says why
  that is a smaller limitation than it read as.** The span attribute
  `messaging.acemq.outbox_lag_ms` still cannot be written by the relay —
  `publish_raw` is beneath the interceptor chain and so beneath the `publish`
  span, and `start()` sweeps where nothing is current — but the measurement is no
  longer lost with it, because a metric needs no span to land on. The method
  remains what an application calls from inside a span of its own.

### Fixed

Documentation, all of it a claim the code stopped supporting.

- **The metric-name compatibility claim was wider than the truth.** The README
  and `docs/observability.md` both said the names here are Java's `MetricNames`
  and that a dashboard built against one library reads against another, without
  saying that six of Java's names are not written here at all:
  `acemq.publish.duration`, `acemq.consume.attempts`, `acemq.request.duration`,
  `acemq.request.total`, `acemq.pipeline.run.duration` and
  `acemq.pipeline.run.total`. Every name that *is* written is Java's character
  for character; the six that are not are now listed, with the reason for each,
  because a dashboard panel that is empty looks the same as a service that has
  stopped.

- **The encrypted-body divergence table was missing Ruby**, which writes Java's
  framing byte for byte. `docs/serialization.md` and
  `acemq_amqp.codecs.encrypted` said "all four AceMQ libraries write four
  different things"; it is five libraries writing three, and Python interoperates
  with Java *and Ruby* rather than with Java alone.

- **A relayed message produces no publish span and no `acemq.publish.total`**,
  which nothing said. `publish_raw` puts the committed bytes on the wire
  unchanged — the reason `record(...)` encodes inside the transaction — and both
  the span and the counter live on the encoding path above it. Recorded in
  `docs/observability.md` and `docs/patterns.md`, with `acemq.outbox.total` named
  as the count to read instead.

- **Ruby was missing from a dozen family claims** written when there were four
  libraries: the development-certificate marker and the certificates it stamps,
  the envelope fields, the rung arguments, the tracing system value, the
  acknowledgement vocabulary and the retry acknowledgement's name. All verified
  against `acemq-ruby-amqp` rather than assumed.

- **`park(...)` and `reject(...)` were absent from the README's handler
  vocabulary**, which showed only `accept()` and `retry()`. `park` was added in
  0.5.0 and the README had never named it.

- **The claim check was missing from the storage seams.** The README and
  `docs/patterns.md` both said three patterns have a storage seam; there are
  four, and `ClaimCheckStore` is the one that is synchronous and not in
  `acemq_amqp.patterns.sql`.

## [0.5.0] - 2026-09-09

### Added

- **A routing slip is read in either of the family's two wire forms, and can be
  written in either.** Python writes the JSON slip, `acemq-routing-slip`, as it
  always has; Java's `Pipeline` writes `x-acemq-route` — the step names,
  comma-separated, with `x-acemq-route-position` and `x-acemq-route-id` beside
  them — resolved against a pipeline declared in advance. Neither could read the
  other, so a Java step and a Python step could not be on the same route.

  `slip_from` now reads whichever the message carries, and `follow_slip` writes
  the same one back, which is what lets a Python step sit in the middle of a
  Java-declared pipeline without either side being told about the other.
  Answering a declared route with a JSON slip would hand the next Java step a
  message with no route on it at all, and the run would stop halfway with
  nothing anywhere saying why — so echoing the form is the default rather than
  an option.

  ```python
  consumer = await mq.consume(
      "fulfilment.enrich", follow_slip(mq, enrich, pipeline="fulfilment")
  )
  ```

  `pipeline=` is the exchange the step names are bound to — the pipeline's own
  name, whose queues are `{pipeline}.{step}` — and it is the one thing the
  declared form does not put on the wire. Writing that form deliberately is
  `route_of("fulfilment", "validate", "enrich", "dispatch")`, or `form=` on
  `start` and `follow_slip`, from the new `SlipForm`.

  **JSON stays the default**, because three of the five libraries write it and
  it is the self-describing one: each step carries its own exchange and routing
  key, so a message can be followed by a service that knows nothing about the
  route, and a route can be built per message. The declared form buys a slip
  readable in a management console and gives up sending a message anywhere the
  declaration does not already know about.

  A slip that cannot be said in the declared form is refused where it is built
  rather than sent somewhere unexpected: steps spanning two exchanges, a step
  with no name, and a step whose name and routing key disagree are each a
  `ValueError`, and a step that raises one on the way out rejects the message
  rather than retrying it — it will not become writable on another attempt.

- **`Envelope.route`, `.route_position` and `.route_id`**, the three reserved
  headers that form carries. Fields rather than application headers for the same
  reason `claim` is one: the names are reserved, so the application map refuses
  them, and a pattern reading one off a delivery had nowhere else to look. They
  are carried by `with_`, so a retry, a dead-letter and a replay all keep them —
  which is what makes replaying a dead-lettered message *resume* its route
  instead of starting it again. Java's `Envelope` carries the same three.

- **`park(error)` joins `accept`, `retry` and `reject` in the handler's
  vocabulary.** It settles the message onto `{queue}.parked`, counts on
  `acemq.messages.dead.lettered.total` with `outcome="parked"`, and puts
  `parked` on the delivery's `process` span.
  The engine could already park — a body its codec refuses goes there before
  any handler runs — but a handler could not ask for it, so one that got a layer
  further in and found a schema it was never taught had to `reject` the message
  into the dead letters and lose the distinction the parked queue exists to
  make. `reject` is *understood, and refused*; `park` is *never understood at
  all*, and mixing the two means whoever drains `{queue}.dlq` has to sort the
  producer emitting rubbish out of the thousands that merely ran out of
  attempts. `Settlement.parked` is the property for it, and
  `Settlement.dead_lettered` is deliberately **false** for a parked message: it
  went to a different queue. Go and Ruby are adding the same word.

- A parked span is **not** an `ERROR`, for the same reason `rejected` is not: it
  is a decision a handler made on purpose. The `parked` outcome on
  `acemq.messages.dead.lettered.total` and the queue itself are what an operator
  watches for those.

- **`Message.reply_to`**, and `reply_to` on `Outbound`, `Delivery`,
  `PublishContext`, `ConsumeContext` and `Publisher.send(...)` — AMQP's own
  `reply-to` property, which this library previously neither wrote nor read.
  See the request-and-reply entry under **Changed**.

- **Payload encryption: `acemq_amqp.codecs.encrypted`, behind the `[crypto]`
  extra.** `EncryptedCodec` wraps any other codec and encrypts what it produced,
  so the broker, its disk, its backups and its management interface hold
  ciphertext. AES-GCM through `cryptography`, a fresh 12-byte nonce per message
  from `os.urandom`, and a 128-bit tag. The framing is
  `0xAE | 0x01 | keyIdLen | keyId | nonce | ciphertext+tag`, and the whole header
  is bound in as associated data, so a key identifier altered in flight makes the
  message fail to open rather than open as something else.

- The key identifier travels **in the clear**, which is what makes rotation
  possible: a consumer reads which key a message needs instead of assuming the
  current one, so a new key can be introduced while messages written with the old
  one are still queued. `key_id_of(body)` answers that question from the bytes
  alone, without holding any key — which is what an operator looking at a
  dead-letter queue they can no longer read actually needs. `Keyring` holds
  several keys with one current; `add` then `use` is the order to rotate in.

- No failure message, log line or `repr` in that module ever contains the
  plaintext, the key, or any part of either, and a wrong key and a tampered body
  produce the identical `FatalError` — GCM authenticates before it returns
  anything, and nothing was added that would tell the two apart. Both properties
  are pinned by tests that sweep every failure path rather than by one assertion
  each.

- **A cross-language divergence, recorded rather than smoothed over.** All four
  libraries write `application/vnd.acemq.encrypted` and four different things
  underneath it. Java uses a `0xAE` magic byte, a one-byte identifier length and
  AES-GCM; Go omits the magic byte and writes a two-byte big-endian length; .NET
  omits the magic byte and uses AES-256-CBC with an HMAC-SHA-256 tag over a
  16-byte IV. **This library implements Java's, byte for byte, and interoperates
  with Java alone** — a Go or .NET body is refused visibly rather than misread.
  Java's is the one to converge on, being the only framing whose first byte
  identifies the format at all. `tests/test_encrypted.py` carries a complete test
  vector — known key, known nonce, known plaintext, exact bytes — to converge
  against, and a body produced by the compiled Java codec that is decrypted here.

- **Development certificates: `acemq_amqp.devcerts`, behind the `[crypto]`
  extra.** `python -m acemq_amqp.devcerts` writes a certificate authority, a
  broker certificate, a client certificate and a `rabbitmq.conf` pointing the
  broker at them — the same file names Go's `acemq-certs` writes, so it is a
  drop-in replacement for it in a script such as `scripts/tls-broker.sh`. ECDSA
  on P-256, thirty days by default, an hour of slack at the front for a
  container's clock, private keys written `0600`.

- **Every certificate carries `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` in its
  subject organisation, and `acemq_amqp.security` now refuses any certificate
  carrying it — however trust is configured, `without_verifying_the_broker()`
  included.** That is what stops a development certificate reaching production: a
  generated authority's private key sits next to its certificate and usually ends
  up in a repository, so one that could reach production would be an authority
  anybody who can read that repository can issue against, and the connection
  would succeed. Java, Go and .NET stamp the same string and enforce it the same
  way.

- The refusal happens twice, because a development certificate arrives from two
  directions. Files this configuration *names* are checked when the TLS context
  is built, where the error can point at the setting to change; what the broker
  *presents* is checked at the handshake through `SSLContext.sslobject_class`,
  which is the only seam `ssl` offers — there is no verify callback of the sort
  Go's `VerifyPeerCertificate` and Java's `X509TrustManager` give. The handshake
  check reads the encoded certificate rather than a verified chain, because under
  `CERT_NONE` there is no verified chain and that is precisely the configuration
  in which a development certificate is most likely to be reached for.

- `Security(allow_development_certificates=True)` is the way through, named the
  same as in the other three libraries. It counts as a TLS setting, so it is
  refused against an `amqp://` URL alongside the rest.

- `DEVELOPMENT_MARKER` and `is_development_certificate` are exported from
  `acemq_amqp`. The latter reads DER, and reads PEM by decoding it first — the
  marker is plain ASCII inside a DER certificate and nowhere to be seen in the
  Base64 that wraps it, so a PEM file searched as it stands comes back clean
  every time.

- **OpenTelemetry tracing: `acemq_amqp.tracing`, behind the `[opentelemetry]`
  extra.** `OpenTelemetryTracing().install(mq)` registers a publish interceptor
  and a consume interceptor. Written against `opentelemetry-api` and not the SDK,
  which is the package a library is supposed to depend on: without an SDK
  installed and configured by the application, the API's no-op implementation
  runs and nothing is exported.

- **A consumer's span is a child of the publish that caused it**, extracted from
  the message's own headers rather than from ambient context. That join — across
  processes and minutes — is the entire reason to trace a message system, and
  reading the ambient context instead would produce traces that look joined up
  and join the wrong things.

- Trace context travels in `traceparent` and `tracestate`, now in
  `acemq_amqp.headers` and **deliberately not `x-acemq-` prefixed** unlike every
  other name in that module: they are the W3C names other tooling already
  recognises, and prefixing them would have made the context private to AceMQ.
  Java, Go and .NET write the same two.

- Spans are `<destination> publish` (PRODUCER), `<queue> process` (CONSUMER) and
  `<destination> request` (CLIENT). CLIENT for the request because that span
  waits for an answer, so its duration measures a responder rather than a broker.
  Attributes are `messaging.system`, `messaging.destination.name`,
  `messaging.operation`, `messaging.message.id`,
  `messaging.message.conversation_id`,
  `messaging.rabbitmq.destination.routing_key`, `messaging.acemq.message_type`,
  `messaging.acemq.attempt` and `messaging.acemq.outcome` — the same names in all
  four libraries. `unroutable`, `failed` and `dead_lettered` set the span status
  to ERROR; the others, `retried` included, do not.

- `outbox.publish_failed`, `pipeline.run_finished`, `message.retried` and
  `message.dead_lettered` are **events on the current span rather than spans of
  their own**, because a zero-length span at the end of a trace adds a row and no
  information. `propagation_headers()` injects the current context into a fresh
  carrier for a message this library does not publish.

- The tracing tests use the SDK's `InMemorySpanExporter` and assert on spans that
  were really emitted, not on mock calls — a mocked tracer is satisfied by an
  adapter whose spans never end, are never parented and never reach an exporter.
  An integration test proves the same join survives a real broker round trip.

- **The five codecs Java and Go ship: YAML, TOML, XML, Protobuf and Avro.** One
  module each under `acemq_amqp.codecs`, one extra each, and nothing added to the
  core's dependencies. The gap they close is not capability — Python could always
  parse YAML — but interoperability: a Java or Go service publishing any of these
  formats produced a message this library refused, because nothing here claimed
  the content type. The write types are the contract and are the ones the other
  libraries write: `application/yaml`, `application/toml`, `application/xml`,
  `application/x-protobuf`, and `avro/binary` or `application/vnd.acemq.avro`.

- Each codec **reads a wider set than it writes**, because a producer in another
  stack uses whichever spelling its own library picked. YAML also accepts
  `application/x-yaml`, `text/yaml` and `text/x-yaml` — all three predate the
  RFC 9512 registration and are what most tooling still emits — TOML also accepts
  `text/toml`, XML also accepts `text/xml`, Protobuf also accepts
  `application/protobuf`, and every one of them accepts its `+suffix` form. None
  of the five answers for a message with **no** content type; `JsonCodec` remains
  the only codec that does, which is right, because a YAML codec that volunteered
  would return the correct value while recording that a YAML message had arrived.

- The suite proves this against **another language's bytes rather than its own**.
  `tests/fixtures/codec-interop-fixtures.json` holds eleven message bodies
  produced by a Go program calling the same functions `acemq-go-amqp/codec/*`
  calls at the versions its `go.mod` files pin, and by a Java program whose
  Jackson mappers are the bodies of the Java codecs' `defaultMapper()` methods at
  the versions `acemq-java-amqp/pom.xml` declares. A round trip would have proved
  nothing about either. Java and Go turned out to write byte-identical XML and
  byte-identical Avro in both framings; their YAML differs only in list
  indentation and their TOML only in quote style, and both of each pair decode to
  the same value here.

- **`ProtobufCodec` accepts `application/vnd.google.protobuf`, which Java does
  not.** Go's codec accepts it and Java's does not, so a message written with the
  type Google's own tooling emits is read by a Go consumer and refused by a Java
  one standing beside it. The union is taken here: accepting it costs nothing and
  closes the hole in this direction. Java's `ProtobufCodec.canDecode` is the side
  that should change.

- **Avro's two modes are not interchangeable, and each claims only its own
  content type — Java's rule, not Go's.** A registered message carries five bytes
  of Confluent framing that a fixed-schema codec reads as the first field; that
  does not throw, so a codec accepting the other framing hands back a record full
  of silent nonsense. Java's `canDecode` is mode-aware and Go's is not, so a Go
  fixed-schema codec claims `application/vnd.acemq.avro` and mis-decodes it. This
  follows Java. Java and Go do agree on the constants themselves and on the
  framing — one zero byte, four bytes of identifier big-endian, then the body —
  and the fixture proves the bytes match.

- **Where the Avro codec improves on Java rather than copying it**: Java's
  fixed-schema `decode` refuses any body of five bytes or more beginning with a
  zero byte, on the grounds that it might be framed — but a legitimate Avro body
  begins with a zero byte whenever its first field encodes to one, which an empty
  string, a `0`, a `false` and the first branch of a union all do. That rule
  refuses real messages. Here the content type is used first, because it is the
  actual contract and it is on the message; the byte heuristic is kept only for
  the case where the sender said nothing at all, where it is the only signal
  there is.

- **`XmlCodec` needs no extra and refuses every DTD, not configurably.** It is
  written against `xml.etree.ElementTree` and `xml.parsers.expat`, so there would
  be nothing in an `[xml]` extra. External entities are already inert in Python —
  `xml.etree` does not resolve them — but **internal** entity expansion is not:
  the billion-laughs attack needs no network and no readable file, and
  `ElementTree.fromstring` expands it, which was checked against the interpreter
  this library is tested on rather than taken from a table. So rather than
  disabling the individual hazards, expat's `StartDoctypeDeclHandler`,
  `EntityDeclHandler`, `UnparsedEntityDeclHandler` and `ExternalEntityRefHandler`
  each raise, and a body carrying `<!DOCTYPE` is a `FatalError` whatever the DTD
  would have said. `defusedxml` was considered and not used: what it does is turn
  these handlers off, and this turns the same handlers off directly.

- `YamlCodec` loads with `safe_load` and there is no way to ask for anything
  else. PyYAML's default loader builds arbitrary Python objects out of tags like
  `!!python/object/apply`, and a message body is untrusted input.

- `TomlCodec` refuses a payload that is not a mapping, in `encode`, the way Java
  and Go both do — TOML is a table format, and a bare list published as TOML is a
  message nothing can read, discovered by the consumer rather than the publisher.
  Reading uses `tomllib` on 3.11 and later and `tomli` on 3.10; writing uses
  `tomli-w`, because the standard library has no writer.

- Importing `acemq_amqp.codecs.yaml`, `.toml` or `.xml` registers it under that
  name, the way Go's `init()` does, so `codec_by_name("yaml")` works from
  configuration. `protobuf` and `avro` are deliberately not registrable: neither
  format's bytes describe themselves, so a codec needs a message type or a schema
  and a no-argument factory has nothing to hand back.

- `AvroCodec.from_registry` and `learn_from` lift schema resolution out of the
  message path. Java's schema registry is a synchronous interface and Go's takes
  a context, so both can look a schema up from inside `encode`; `SchemaRegistry`
  here is a set of coroutines and `encode` cannot await one. A message carrying
  an identifier the codec has not been taught raises `FatalError` naming the
  identifier rather than guessing at it.

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

- **`ConsumeContext.when_settled(listener)`, and `Settlement` in
  `acemq_amqp.ack`.** What a handler answered is not what happened to the
  message, and until now nothing could tell the difference: an interceptor saw
  the `Ack` and never the consumer's decision. A listener registered here is
  told what the consumer settled on — `acked`, `retried`, `rejected` or
  `dead_lettered`, with the delay it chose or the reason it gave — once, on the
  consumer's task, after the decision and before it is carried out. It returns
  whether anything will ever call it, which is `False` for a chain composed by
  hand with no consumer driving it.

### Changed

- **Every metric is renamed onto Java's vocabulary. This breaks every existing
  Python dashboard.** Java's `MetricNames` in `acemq-amqp-api` is the family's
  vocabulary and Go, Python and Ruby are moving onto it. Python's names were an
  entirely disjoint set: no dashboard could read Python and Java together,
  though `docs/observability.md` said one could — that claim is corrected here
  as well as the names.

  | Old | New |
  |---|---|
  | `acemq.messages.published` | `acemq.publish.total{outcome="confirmed"}` |
  | `acemq.messages.publish.failed` | `acemq.publish.total{outcome="unroutable"}` for a mandatory publish that reached no queue, `{outcome="failed"}` for one that did not reach the broker |
  | `acemq.messages.consumed` | *gone* — the sum of `acemq.consume.total` across its outcomes is how many arrived |
  | `acemq.messages.accepted` | `acemq.consume.total{outcome="acked"}` |
  | `acemq.messages.retried` | `acemq.consume.total{outcome="retried"}`, and `acemq.messages.retried.total`, which keeps its `where` label |
  | `acemq.messages.rejected` | `acemq.consume.total{outcome="rejected"}` |
  | `acemq.messages.dead.lettered` | `acemq.messages.dead.lettered.total{outcome="dead_lettered"}`, and `acemq.consume.total{outcome="dead_lettered"}` |
  | `acemq.messages.parked` | `acemq.messages.dead.lettered.total{outcome="parked"}`, and `acemq.consume.total{outcome="parked"}` |
  | `acemq.handler.duration` | `acemq.consume.duration`, now tagged with the outcome |
  | `acemq.messages.in.flight` | `acemq.consume.in.flight` |
  | `acemq.retry.rung.missing` | unchanged |
  | `acemq.messages.set.aside.failed` | unchanged |

  The shape changes as well as the spelling: **where Python had one counter per
  outcome, there is now one counter with an `outcome` tag**, which is what a
  dashboard wants — a failure rate is a ratio between two series of one metric
  and not a division between two differently-named ones. `acemq.messages.retried.total`
  and `acemq.messages.dead.lettered.total` stay as standalone counters beside
  the tagged one, exactly as Java keeps them: the outcome says what was decided
  about a delivery, the counter says how many messages are going round again or
  have been set aside, and that second number is the one an alert is written
  against.

  `acemq.messages.consumed` has no replacement on purpose. It counted arrivals
  and the others counted settlements, so it always led them by however many
  messages were in flight — two counters for one thing that never agreed. Every
  delivery is now counted exactly once, when it is settled, including a body
  that would not decode.

  **Parking is no longer a metric of its own.** Java had no name for it when
  this began and has one now — it is `acemq.messages.dead.lettered.total` with
  `outcome="parked"`, the same counter a dead-lettering moves — so
  `acemq.messages.parked` and `METRIC_PARKED` are gone. Both are a message this
  queue gave up on, and an operator asking how much a queue is giving up on
  wants one number that can then be split; the split still matters, and the tag
  is what keeps it. `acemq.messages.set.aside.failed` is unchanged and is now
  Java's name too, `target` tag included.

  The constants are renamed with the metrics — `METRIC_PUBLISH_TOTAL`,
  `METRIC_CONSUME_TOTAL`, `METRIC_CONSUME_DURATION`, `METRIC_CONSUME_IN_FLIGHT`,
  `METRIC_RETRIED_TOTAL`, `METRIC_DEAD_LETTERED_TOTAL` — and the tag names and
  publish outcomes are constants too, in `acemq_amqp.telemetry`. The delivery
  outcomes are the `OUTCOME_*` names in `acemq_amqp.ack` that the settlement and
  the span already used, so the counter and the span now carry the same word for
  the same delivery by construction.

- **Dotted tag names are rewritten for Prometheus rather than emitted as they
  are.** `routing.key` and `message.type` are legal tag names in Micrometer and
  OpenTelemetry and illegal label names in Prometheus, which allows
  `[a-zA-Z_][a-zA-Z0-9_]*` and nothing else. `prometheus_text` was rendering
  `acemq_messages_published{routing.key="…"}`, which a scraper rejects *in
  full* — the whole scrape, not the one sample it could not parse — so the
  publish counters were reaching nobody. It now writes `routing_key`.
  `PrometheusObserver` sanitises the names it registers too: a
  prometheus-client old enough to validate label names refuses the collector
  outright, which takes the publisher down at the first message rather than
  under-reporting. Label **values** are untouched: a routing key is where the
  dots mean something.

- **A request now carries its return address twice, and a responder reads
  either.** `Requester.ask` sets AMQP's own `reply-to` property *and* the
  `acemq-reply-to` header to the same queue; `serve` reads the header first and
  falls back to the property. All five libraries do exactly this, in that order.

  Before this, request and reply **did not work across the family at all**.
  Python, Go and Ruby wrote only the header; Java and .NET read only the native
  property. A Java or .NET caller reaching a Python responder had its request
  dead-lettered for carrying no return address, and a Python caller reaching a
  Java responder was never answered. No fixture covered the pair, which is how
  it survived. Writing both and reading either makes all twenty-five
  caller/responder combinations work.

  The header is kept rather than replaced because it is the half that survives a
  service which rebuilds the message; the property is what the other four read.
  Header first is the agreed order, so a request republished under a new reply
  queue by an intermediary goes where the intermediary said. `Message.reply_to`
  exposes the property to a handler writing its own responder, and the transport
  protocol carries it in both directions — `Outbound.reply_to` out,
  `Delivery.reply_to` in. A transport that cannot report it leaves it empty and
  the header path is unaffected.

- **The publish counters are tagged `routing.key`, not `key`.**
  `acemq.messages.published` and `acemq.messages.publish.failed` are labelled
  `exchange` and `routing.key`; the tag used to be `key`. Java and .NET both
  spell the fully-qualified name (`MetricNames.TAG_ROUTING_KEY`), Java is the
  reference, and Go is moving in parallel — so this is Python and Go aligning on
  the name three of the five already used rather than a new invention.

  **This changes a label an existing dashboard may group by.** In Prometheus the
  rendered label goes from `key="order.placed"` to `routing_key="order.placed"`,
  so a query reading `acemq_messages_published{key=...}` or grouping `by (key)`
  needs the name changed. The metric names moved in the same release — see the
  rename onto Java's vocabulary above — so a dashboard is being rewritten
  anyway; the tag is `routing.key` on whichever counter it lands on.

- **The tracer is registered as `org.acemq.amqp` rather than `acemq_amqp`**, and
  the `pipeline.run_finished` event's keys are `pipeline`, `step` and `outcome`
  rather than `messaging.acemq.`-prefixed. Java, Ruby, Go and .NET all write the
  reverse-domain scope name and the bare event keys; Python was the odd one out,
  three to one, and the cost was exactly the thing the naming exists for — spans
  from five libraries did not group under one instrumentation scope, and a query
  that found a pipeline event in one library found it in one library. The span
  *attribute* for an outcome is still `messaging.acemq.outcome`; only the
  event's own keys are bare, which is what the others do.

- **A delivery's `process` span now ends when the consumer decides, not when
  the handler returns**, and takes its outcome from that decision. The backoff
  is deliberately outside it: the consumer announces what it is going to do
  before it does it, so a message waiting five minutes in the consumer no longer
  produces a five-minute handler span.

- **A `request` span now ends with an outcome even when nothing went wrong**:
  `answered` when the reply arrived, `timed_out` when the deadline did, `failed`
  for anything else. Those are the two words Java's `MetricNames` spells and its
  requester writes on the same span. Previously an outcome appeared only on a
  failure, so every round trip that worked carried none at all and "how many
  requests were answered" had nothing to divide by. A timeout is recognised by
  type rather than by message — `RequestTimeoutError` is now a `TimeoutError` as
  well as an `AceMQError`, so `except TimeoutError` catches it and so does
  anything already catching `AceMQError` — and it keeps the `ERROR` status,
  because a caller that never got its answer is not a green span whatever the
  outcome attribute calls it.

- **`messaging.rabbitmq.destination.routing_key` is no longer set on a
  delivery's `process` span.** It is a publish-side attribute in every other
  library — Java's `consumeStarted` is not even handed a routing key, and Ruby's
  process span does not carry one — and Python was the only one writing it. An
  attribute present on one library's spans and absent from the other four's is
  worse than one nobody writes, because a query built around it returns the
  Python services and looks like a complete answer. It is unchanged on the
  `publish` span.

- **`outbox_published` is documented as an application call, and why it cannot
  be a library one.** Java's relay calls its equivalent, so
  `messaging.acemq.outbox_lag_ms` is written by the library there. This one
  cannot: `OutboxRelay` publishes with `Connection.publish_raw`, which is
  beneath the interceptor chain and so beneath the `publish` span, and `start()`
  sweeps on a task of its own where nothing else is current either. An attribute
  has to land on a span, and there is no span for a relayed record to land on. A
  hook wired into the relay would compute a lag on every record and hand it to
  nothing, which is worse than no hook — a number silently dropped looks exactly
  like a number that is zero. The method stays where it works, inside a span the
  caller is holding, which is what an on-demand `sweep()` at the end of a request
  already is. Ruby reached the same conclusion for a related reason.

### Fixed

- **A message that exhausted its attempts carried `outcome='retried'` on its
  span, and never `dead_lettered`.** `message_retried` and
  `message_dead_lettered` existed on `OpenTelemetryTracing` and nothing in the
  library called them; they were hooks an application could reach for and
  nobody knew to. So every message the system ever gave up on was recorded as
  one that would be tried again, and a trace backend queried for dead letters
  came back empty while the dead-letter queue filled up. The consumer now
  records both events itself, on the delivery's own span: `message.retried` with
  the delay the policy actually chose — which is a jittered number that exists
  nowhere else — and `message.dead_lettered` with the reason, alongside an
  outcome of `dead_lettered` and an `ERROR` status. A handler that raised and
  was then given up on carries the exception *and* the outcome, in that order,
  as Java does. An outright rejection still reads `rejected`: it goes to the
  same queue, and the word records that somebody meant it. The same gap was
  open in Go, .NET and Ruby and is being closed in all of them.

- **A retry the broker had to hand back was recorded on the span and counted
  nowhere.** When a consumer's own queue has gone, the message cannot be
  republished and is returned to the broker unacknowledged instead. That is
  still the retry the settlement announced and the `process` span records, and
  it moved no counter at all — the one remaining place where the numbers and the
  trace disagreed about a single delivery. It now increments
  `acemq.messages.retried` with `where='requeued'`, a third value beside
  `consumer` and `broker`; the label *names* are unchanged, so no collector is
  affected. The agreement itself is now a test, run over every way a delivery
  can end: the counter that moves and the outcome on the span are asserted to be
  the same verdict.

  The classification bug that Ruby and .NET carried — counters read from the
  handler's `Ack` rather than from the consumer's `Settlement`, so a message
  that exhausted its attempts incremented *retried* — was never present here.
  `_carry_out` has always switched on the settlement. **These numbers do not
  move**, unlike .NET's, where a dashboard will show retries falling and
  dead-letters rising with no change in service behaviour.

- **`AvroCodec` in fixed-schema mode applied its leading-zero-byte heuristic
  only when the content type was absent, and not when it was present and said
  nothing useful.** A message labelled `application/octet-stream` whose body
  began with `0x00` was decoded as a fixed-schema body, which is how five bytes
  of somebody else's schema identifier get read as the first field and every
  value after it comes back silently wrong. The rule is now exactly the one all
  five libraries follow: a content type naming Avro — `avro/binary`,
  `application/avro`, anything ending `+avro` — is believed whatever the first
  byte is; `application/vnd.acemq.avro` is refused by a fixed-schema codec; and
  only a content type that is absent or names something other than Avro lets the
  zero byte have a vote, where it refuses rather than guesses.

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
