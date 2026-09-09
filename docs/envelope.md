# The envelope

Every message carries an `Envelope`: what it is, what caused it, which attempt
this is, who sent it and when it was first published. It is what makes a message
traceable across four services and three languages, and it is written and read
by the library rather than by the application.

```python
@dataclass(frozen=True, slots=True)
class Envelope:
    id: str                    # a UUID by default
    type: str = ""             # falls back to the routing key
    version: int = 1
    correlation_id: str = ""   # defaults to id
    causation_id: str = ""
    attempt: int = 1
    first_seen: datetime       # now, by default
    origin: str = ""
    error: str = ""
    claim: str = ""
    headers: Mapping[str, Any] = {}
```

Frozen, because an envelope describes a message that has already been published
or received. Changing one in place would change what a log line said after it
was written.

## On the wire

| Header | Type | |
|---|---|---|
| `x-acemq-id` | string | The message identifier, and the default idempotency key |
| `x-acemq-type` | string | The logical type, falling back to the routing key |
| `x-acemq-version` | int | Schema version, from 1 |
| `x-acemq-correlation` | string | Defaults to the id, so a chain has something to copy |
| `x-acemq-causation` | string | The message that caused this one |
| `x-acemq-attempt` | int | Delivery attempt, from 1 |
| `x-acemq-first-seen` | int | Epoch **milliseconds** of the first publish |
| `x-acemq-origin` | string | `service@host` |
| `x-acemq-error` | string | Why it was dead-lettered or parked |
| `x-acemq-claim` | string | Where the payload is, when it is stored outside the message |
| `x-acemq-route` | string | The steps of a declared route, by name, comma-separated |
| `x-acemq-route-position` | int | Which of them this message is for, from 0 |
| `x-acemq-route-id` | string | One run through that route, across every hop |

`x-acemq-claim` is for saying, in a form an operator reading a dead-letter queue
can use, where a payload went. It is **not** how a consumer decides that a
message is a claim check: [`ClaimCheckCodec`](patterns.md#the-claim-check) puts
three bytes at the front of the body and dispatches on those, because a header
can be dropped by a shovel, a federation link or a plugin and a body cannot.

These names are the contract between languages, not an implementation detail of
this one. Nothing here is renamed for Python's benefit —
`x-acemq-first-seen` stays hyphenated and stays epoch milliseconds, however
un-Pythonic that looks — because a Python consumer reads what a Java producer
writes only if both agree on the strings and on the types of their values.

The last three are Java's `Pipeline` form of a
[routing slip](patterns.md#two-wire-forms-and-both-are-read), read here as
`envelope.route`, `.route_position` and `.route_id`. They are fields rather than
application headers because the names are reserved, and a pattern that needs
them off a delivery would otherwise have nowhere to read them from. `with_`
carries them, which is what makes replaying a dead-lettered message *resume* its
route rather than start it again.

Empty is **absent**. `causation_id`, `origin`, `error`, `claim` and the route
are written only when they have a value, because a header carrying `""` is a
header somebody has to write a special case for at the other end. The one
exception is `x-acemq-route-position`, which is written whenever there is a
route, zero included: a first hop with no position is the only hop whose slip is
incomplete.

## The reserved namespace

Everything beginning `x-acemq-` belongs to the library. Setting one of them by
hand in your own headers raises:

```python
Envelope(headers={"x-acemq-id": "mine"})
# ValueError: these header names belong to AceMQ and cannot be set by hand: x-acemq-id
```

Refused rather than dropped: silently discarding a header somebody set is worse
than saying no. `acemq_amqp.headers.is_reserved(name)` is the check, if you want
to make it yourself before building the envelope.

Reading works the same way round. An `x-acemq-` name this version does not know
— from a newer release of another language's library — is **not** handed back in
`envelope.headers`, because an application that treated it as its own would
start writing it back and would then be lying about a field it does not
understand.

## Reading it in a handler

```python
async def ship(message: Message) -> Ack:
    envelope = message.envelope
    log.info(
        "%s type=%s attempt=%d correlation=%s from=%s age=%s",
        envelope.id,
        envelope.type,
        envelope.attempt,
        envelope.correlation_id,
        envelope.origin,
        envelope.age,
    )
    tenant = envelope.headers.get("tenant")
    return accept()
```

`age` is computed rather than stored: `datetime.now(timezone.utc) -
first_seen`. It is the basis for
[giving up on age](reliability.md#giving-up-on-age) rather than on attempts,
which is the honest limit when a queue has been paused — a message can be on
attempt two and four days old.

## Building one

```python
from acemq_amqp import Envelope

envelope = Envelope(
    type="order.placed",
    version=2,
    correlation_id=incoming.correlation_id,
    causation_id=incoming.id,
    headers={"tenant": "acme"},
)

await publisher.send(payload, envelope=envelope)
```

Defaults are applied when the envelope is **built**, not when it is read, so two
libraries reading the same message agree without having to agree on a second set
of rules:

- `id` is a fresh UUID
- `correlation_id` falls back to `id`
- `attempt` and `version` are at least 1 — a lower value is corrected, not
  refused
- `first_seen` is now
- `type` falls back to the routing key, at the moment the headers are written

`with_` gives a modified copy:

```python
envelope.with_(causation_id=envelope.id, type="order.shipped")
```

## Reading values off the wire

`Envelope.from_headers(raw, routing_key)` builds one from a delivery's headers,
and it is deliberately forgiving. Anything missing takes its default, and
anything *unreadable* takes its default too: a producer that wrote
`x-acemq-attempt` as the string `"2"` still sent a message, and refusing to
deliver it would hand the application an outage rather than a message.

Strings are decoded rather than assumed. RabbitMQ's Java client sends strings as
`LongString` and some clients send them as bytes, so a header arriving as
`b"order.placed"` reads back as `"order.placed"`.

You rarely call this: the consumer does it for every delivery. It is public
because reading an envelope off a message somebody else delivered — in a replay
tool, a log shipper, a test — is a real use, and it needs no broker client
installed to do it.

## How this is kept honest

`tests/fixtures/envelope-fixtures.json` was generated by the Java implementation
and is shared with the Go and .NET libraries. A test reads each fixture, builds
the envelope, writes the headers back and compares: no header gained, none lost,
none renamed, no type changed.

That is the difference between a port and a claim. A wire contract hand-copied
out of documentation acquires a difference nobody notices until two languages
disagree in production, at which point the message that proves it is the one
already in the dead-letter queue.
