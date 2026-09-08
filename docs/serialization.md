# Codecs

A codec turns a payload into bytes and bytes back into a payload, and declares
the content type it writes. That content type is what makes this work across
languages: a Java producer says `application/json`, a Python consumer picks the
codec that answers for it, and neither end has been told about the other.

```python
mq = await connect(url, codec=JsonCodec())              # the connection's default
mq.publisher("files", "uploaded", codec=BytesCodec())   # one publisher
await mq.consume("orders.dlq", inspect, codec=BytesCodec())  # one consumer
```

JSON is the default. Nothing has to be configured to get it.

## The three that need nothing installed

| | Writes | Reads |
|---|---|---|
| `JsonCodec` | `application/json` | JSON, `text/json`, any `+json` type, and a message with **no** content type |
| `TextCodec` | `text/plain; charset=utf-8` | `text/*` and nothing else |
| `BytesCodec` | `application/octet-stream` | everything |

## The five behind an extra

| | Writes | Reads | Install |
|---|---|---|---|
| `YamlCodec` | `application/yaml` | that, `application/x-yaml`, `text/yaml`, `text/x-yaml`, any `+yaml` type | `[yaml]` |
| `TomlCodec` | `application/toml` | that, `text/toml`, any `+toml` type | `[toml]` |
| `XmlCodec` | `application/xml` | that, `text/xml`, any `+xml` type | nothing |
| `ProtobufCodec` | `application/x-protobuf` | that, `application/protobuf`, `application/vnd.google.protobuf`, any `+protobuf` type | `[protobuf]` |
| `AvroCodec` | `avro/binary`, or `application/vnd.acemq.avro` with a registry | see [Avro](#avro) | `[avro]` |

`EncryptedCodec` is a sixth, and a different shape: it wraps one of the others
rather than replacing it. See [Encryption](#encryption).

**None of the five answers for a message with no content type.** Only
`JsonCodec` does that, and it is the only one that should: JSON is the default
format, so an untyped message is far more likely to be JSON than anything else.
A YAML codec that volunteered would be worse than useless — YAML parses JSON, so
it would return the right value while recording that a YAML message had arrived.

They live one module each, and none of them is imported by `acemq_amqp`, so the
core keeps its promise of depending on nothing:

```bash
pip install "acemq-amqp[yaml]"
```

```python
from acemq_amqp import CompositeCodec, JsonCodec
from acemq_amqp.codecs.yaml import YamlCodec

mq = await connect(url, codec=CompositeCodec(JsonCodec(), YamlCodec()))
```

### Why they exist

Not because Python could not parse YAML. It always could. The gap was that a
**Java or Go service publishing YAML produced a message this library refused** —
the bytes were readable and nothing here claimed the content type, so a
perfectly good message was parked. Java and Go ship these five as separate
optional modules; these are the same five, writing the same content types and
accepting the same wider sets, so a message crosses either way.

What each codec *reads* is deliberately wider than what it writes, because a
producer in another stack uses whichever spelling its own library picked.
`application/yaml` was only registered by RFC 9512; `application/x-yaml`,
`text/yaml` and `text/x-yaml` all predate it and are what a lot of tooling still
emits. Getting that set wrong is the failure these codecs exist to prevent.

The tests are not round trips. `tests/fixtures/codec-interop-fixtures.json`
holds message bodies produced by Java and by Go — by programs calling the same
functions those codecs call, at the versions their build files pin — and the
suite decodes those. Java and Go turned out to write byte-identical XML and
byte-identical Avro; their YAML differs only in list indentation and their TOML
only in quote style, which is exactly the difference a decoder has to absorb and
a round trip would never have produced.

### YAML

Block style, not flow style, which is the whole reason to pick YAML — flow style
produces something very close to JSON and leaves nothing to justify the cost. No
leading `---`: a message body is a single document, so the marker separates
nothing from nothing. Keys keep the order the payload was built in.

```python
YamlCodec().encode({"orderId": "o-1", "items": ["widget", "gasket"]})
# b'orderId: o-1\nitems:\n- widget\n- gasket\n'
```

**Loading is always safe loading, and that is not configurable.** PyYAML's
default loader constructs arbitrary Python objects out of tags like
`!!python/object/apply`, and a message body is untrusted input.

YAML costs more to parse than JSON and is a poor choice for high volume. It
earns its place where somebody will actually read the message — a configuration
change broadcast to a fleet, a command replayed by hand from a dead-letter
queue.

### TOML

**The top level has to be a mapping.** TOML is a table format; a bare list or a
bare number is not a TOML document, and `encode` says so rather than emitting
something nothing can read:

```python
TomlCodec().encode(["a", "b"])
# TypeError: ... the top level has to be a mapping ...
```

Refused at the publisher rather than discovered by the consumer, which is what
Java and Go both do. Reading uses `tomllib` from the standard library on 3.11
and later and `tomli` — the same parser under its original name — on 3.10;
writing has no standard-library answer at all, so the extra brings `tomli-w`.

### XML

**No extra**, because there would be nothing in it: the codec is written against
`xml.etree.ElementTree` and `xml.parsers.expat`.

**Every document with a DTD is refused, and that is not configurable.** A
message body arrives from a queue, which is exactly the sort of place a message
from somewhere unexpected turns up. Python's position here is better than it is
often given credit for and worse than it needs to be:

- External entities are already inert. `<!ENTITY x SYSTEM "file:///etc/passwd">`
  is not resolved by `xml.etree`; the reference fails as an undefined entity. So
  plain XXE and DTD retrieval are not live hazards.
- **Internal** entity expansion is not inert. The billion-laughs attack needs no
  network and no readable file — a few nested internal entities expand to
  gigabytes inside the parser — and `ElementTree.fromstring` expands them
  happily. That was checked against the interpreter this library is tested on
  rather than taken from a table.

So rather than disabling the individual hazards, the codec refuses the construct
they all need: expat's `StartDoctypeDeclHandler`, `EntityDeclHandler`,
`UnparsedEntityDeclHandler` and `ExternalEntityRefHandler` each raise, and a
body carrying `<!DOCTYPE` is a `FatalError` whatever the DTD would have said.
There is no constructor argument to relax it. `defusedxml` was considered and
not used: what it does is turn these handlers off, and this turns the same
handlers off directly, in a way nobody can turn back on.

**XML has no types, so everything decodes to a string**, a nested dict, or a
list where a tag repeats — the same thing Jackson gives when an XML message is
read into a `Map`. A consumer that wants an `int` converts it, where it can
decide what an unparseable field means.

A dataclass is written under its class name, the way Jackson and Go's
`encoding/xml` write theirs; a plain mapping has no name to take, so
`XmlCodec(root="OrderPlaced")` supplies one. The default is `message`.

### Protobuf

**A codec is built for one message type**, the way it is in Java:

```python
from acemq_amqp.codecs.protobuf import ProtobufCodec
from myapp.orders_pb2 import OrderPlaced

codec = ProtobufCodec(OrderPlaced)
```

Protobuf bytes carry no name — they are field numbers and wire types — so a
reader must already know which message it is holding, or the bytes are not
interpretable at all. A destination carrying several message types has to say
which is which, and the protobuf answer to that is a wrapper message with a
`oneof`: a decision about the schema, not about the transport.

`decode` hands back the generated message itself, with its fields typed, rather
than a dict.

### Avro

**There is no schema-free Avro and there cannot be one.** Avro's bytes describe
nothing about themselves: a reader must already hold the schema the writer used.
Two ways to say where it comes from, and the choice matters more than it looks.

```python
from acemq_amqp.codecs.avro import AvroCodec

codec = AvroCodec(schema)                                        # avro/binary
codec = await AvroCodec.from_registry(registry, "order.placed", schema)
```

`AvroCodec(schema)` fixes one schema for the codec's whole life. Small, fast,
nothing extra to run — and the writer's schema is whatever the reader happens to
have. The moment a producer adds a field, every consumer still holding the old
schema reads the new bytes wrongly and Avro will not always notice. Sound only
where producer and consumer are released together.

`AvroCodec.from_registry(...)` writes the schema's identifier into the front of
every message, so a reader can look up exactly what the writer used and let Avro
resolve it against its own. That is what makes a field addition safe, and it is
the mode to use unless there is a reason not to. The framing is **one zero byte,
four bytes of identifier big-endian, then the Avro body** — Confluent's layout,
and byte-for-byte the one Java and Go write.

| Mode | Writes | Reads |
|---|---|---|
| fixed schema | `avro/binary` | `avro/binary`, any `avro/*`, `application/avro`, any type containing `avro` that is not the registered one |
| registered | `application/vnd.acemq.avro` | that, `application/avro`, any type containing `avro` that is not `avro/…` |

**Each mode claims only its own framing type.** The two are not interchangeable
and the difference is invisible in the bytes: a framed message begins with five
bytes a fixed-schema codec would read as the first field. That does not throw —
Avro decodes the shifted bytes into whatever they happen to mean — so a codec
that accepted the other framing would hand back a record full of silent
nonsense.

**The registry here is async and a codec is not.** Java's registry is a
synchronous interface and Go's takes a context, so both can look a schema up
from inside `encode`. `SchemaRegistry` in this library is a set of coroutines,
and `encode` cannot await one — it runs on the publisher's hot path and, in the
`sync` facade, on a worker thread with no loop. So resolution is lifted out of
the message path and done once, explicitly:

```python
codec = await AvroCodec.from_registry(registry, "order.placed", schema)
await codec.learn_from(registry, 7)   # a writer version this consumer will meet
```

A message carrying an identifier the codec has not been taught raises
`FatalError` naming the identifier rather than guessing.

### JSON

Dataclasses are encoded through `dataclasses.asdict`, so a payload written as a
dataclass goes on the wire as an object with its field names:

```python
@dataclass
class OrderPlaced:
    order_id: str
    total_cents: int


await orders.send(OrderPlaced("o-1", 4250))
# {"order_id": "o-1", "total_cents": 4250}
```

Everything else is handed to `json` unchanged, which covers dicts, lists and the
scalars. A payload `json` cannot carry raises `TypeError` naming the type, and
nothing is published.

**Decoding produces dicts and lists, not your class.** The wire says nothing
about which class was meant, and a library that guessed would be guessing on
every message. An application that wants its own type back constructs it from
what it got, where it can decide what a missing field means:

```python
async def ship(message: Message) -> Ack:
    order = OrderPlaced(**message.payload)
```

**It answers for a message with no content type.** JSON is the default format,
so an untyped message is far more likely to be JSON than anything else, and
something has to read it. That is the rule the .NET library uses too.

**A malformed body is fatal, not retryable.** A body that is not JSON is not
JSON the next time either, so `JsonCodec.decode` raises `FatalError` — and a
decode failure never reaches the retry path anyway: the consumer
[parks](consuming.md#when-a-message-cannot-be-decoded) it in `{queue}.parked`
before the handler runs.

### Text

For messages that really are text — a line of a log, a command somebody typed —
rather than a structure that happens to be readable. Prefer JSON for anything
with fields.

It deliberately does **not** answer for a message with no content type. There is
no reason to think an untyped message is text, and a codec that decodes anything
into a string never fails, so it would quietly hand a handler the printable
rendering of a body it should have refused.

### Bytes

Passes bodies through untouched. For a payload that is already encoded — an
image, something another system's serializer produced — and for reading a
message this process has no type for.

It is what to read a **dead-letter queue** with. The message that went there may
be exactly the one nothing could decode, and a codec that fails on it would park
it a second time. Replaying uses it for the same reason: the bytes that were
committed are the bytes that should go back, and re-encoding through a class
that has since gained a field would produce something else.

It answers for every content type, which is why it has to be asked for rather
than found. In a `CompositeCodec` it must go **last**, or nothing after it is
ever reached.

## Several formats on one queue

```python
from acemq_amqp import CompositeCodec, JsonCodec, TextCodec

mq = await connect(url, codec=CompositeCodec(JsonCodec(), TextCodec()))
```

The **first** codec is what it writes. All of them are offered a message to
read, in the order given, and the first that claims the content type *and*
succeeds wins.

With no content type nothing can be ruled out, so every codec is a candidate
rather than the first being assumed right. A message that claims one format and
is written in another is still read, because a candidate that refuses says
nothing about the next one. Only when every candidate has refused is anything
raised — and it carries **all** the reasons rather than whichever came last:

```
acemq: no codec could read this message. Tried JsonCodec: acemq: this message is
not JSON: ...; TextCodec: acemq: this message is not UTF-8 text: ...
```

That is the shape a queue needs during a migration, and the shape it needs where
several producers were written at different times.

## Writing one

`Codec` is a runtime-checkable `Protocol`, so there is nothing to inherit from.
Four members:

```python
from typing import Any

import msgpack   # for the sake of the example


class MsgPackCodec:
    @property
    def content_type(self) -> str:
        return "application/msgpack"

    def encode(self, payload: Any) -> bytes:
        return msgpack.packb(payload)

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        try:
            return msgpack.unpackb(body)
        except Exception as failure:
            raise FatalError(f"not msgpack: {failure}") from failure

    def can_decode(self, content_type: str | None) -> bool:
        return content_type is not None and "msgpack" in content_type.lower()
```

Two rules worth following, because the rest of the library relies on them:

- **Raise `FatalError` for a body this codec will never read.** Anything else is
  treated as a transient failure by the code that catches it, and a body that is
  not msgpack is not msgpack the next time either.
- **Be honest in `can_decode`.** A codec that answers `True` for everything
  makes a `CompositeCodec` stop at it.

`decode` takes the content type as a defaulted argument rather than needing a
second interface for it. That is where this departs from Go: Go's `Decode` takes
a pointer, because that is how every Go decoder works, and it then needs
something else to get the content type to a composite codec that has to choose.

## The registry

```python
from acemq_amqp import codec_by_name, codec_names, register_codec

register_codec("msgpack", MsgPackCodec)

codec_names()                    # ['bytes', 'json', 'msgpack', 'text']
codec = codec_by_name(config.codec)
```

So that configuration can name a format without the code that reads the
configuration importing every format it might name. `json`, `bytes` and `text`
are registered by the library; importing `acemq_amqp.codecs.yaml`,
`.toml` or `.xml` adds `yaml`, `toml` and `xml`, which is how Go's `init()`
does it.

`protobuf` and `avro` are deliberately **not** registrable. Neither format's
bytes describe themselves, so a codec needs a message type or a schema before it
can read anything, and a no-argument factory has nothing to hand back.

`register_codec` takes a **factory**, not an instance, so `codec_by_name`
returns a new one each time. Registering the same name twice replaces the first,
which is what lets a test override a default. `codec_by_name` raises `KeyError`
listing the names it does know.

## Encryption

`EncryptedCodec` wraps another codec and encrypts what it produced, so the
broker, its disk, its backups and everybody who can read its management
interface hold ciphertext.

```bash
pip install "acemq-amqp[crypto]"
```

```python
from acemq_amqp import JsonCodec, connect
from acemq_amqp.codecs.encrypted import EncryptedCodec, Keyring, generate_key

keyring = Keyring.of("2026-01", generate_key())
mq = await connect(url, codec=EncryptedCodec(JsonCodec(), keyring))
```

It wraps a delegate rather than serialising anything itself, so choosing a format
and choosing to encrypt stay independent: JSON in, AES-GCM out, and Avro just as
well. The content type on the wire is `application/vnd.acemq.encrypted` —
deliberately not `...+json`, whatever the plaintext underneath is, because a
`+json` suffix would make every JSON-aware consumer volunteer to parse
ciphertext.

### What is on the wire

```
0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
```

**The key identifier travels in the clear.** That is the point: a consumer reads
which key a message needs rather than assuming the current one, so a key can be
rotated while messages written with the old one are still queued. `key_id_of()`
reads it back from the bytes alone, which is what an operator staring at a
dead-letter queue they can no longer read actually wants — and it needs no key
to do it.

The header is authenticated but not encrypted: GCM binds all ten-or-so bytes of
it as associated data, so a key identifier altered in flight makes the message
fail to open rather than quietly opening as something else.

### Rotation

```python
keyring.add(EncryptionKey("2026-02", generate_key()))   # every consumer first
keyring.use("2026-02")                                  # then one publisher
```

That order, always. A keyring holding one key cannot rotate without an outage.

### Interoperability, and a divergence worth knowing about

All four AceMQ libraries write `application/vnd.acemq.encrypted` and **four
different things underneath it**. This is a real bug in the family, recorded here
rather than smoothed over, because a consumer cannot tell which it is about to be
handed:

| | magic | version | id length | iv | cipher | tag |
|---|---|---|---|---|---|---|
| Java | `0xAE` | `0x01` | 1 byte | 12-byte nonce | AES-GCM | 16 bytes |
| **Python** | `0xAE` | `0x01` | 1 byte | 12-byte nonce | AES-GCM | 16 bytes |
| Go | none | `0x01` | 2 bytes, big-endian | 12-byte nonce | AES-GCM | 16 bytes |
| .NET | none | `0x01` | 1 byte | 16-byte IV | AES-256-CBC | HMAC-SHA-256, 32 bytes |

**Python interoperates with Java, and with nothing else.** A body written by Go
or .NET is refused here — visibly, saying it was not written by this codec —
rather than being decrypted into something wrong.

Java's is the framing to converge on. It is the only one whose first byte
identifies the format at all, which is what lets a body that was never encrypted
be refused rather than misparsed; Go needs the magic byte and a one-byte length,
and .NET needs both of those plus AES-GCM in place of encrypt-then-MAC.
`tests/test_encrypted.py` holds a complete test vector — a known key, a known
nonce and a known plaintext, with the exact bytes written out — for whoever does
that work.

### What it does not do

The broker can no longer read the message, and neither can the people who operate
it. **Decide what they do instead before turning this on**: a dead-letter queue
full of ciphertext is a queue nobody can triage.

Encryption is not authorisation — every service holding the keyring reads every
message, so separate audiences mean separate keys — and it is not a signature:
anybody holding a key can write a message this codec will decrypt without
complaint. It does not hide the routing either. Exchange, routing key, headers
and message size stay in the clear, and for many systems the routing key is the
sensitive part.

Nothing in the module ever puts a plaintext, a key, or any part of either into an
exception, a log line or a `repr`, and a failure to decrypt says the same thing
whether the key was wrong or the bytes were altered. Both are pinned by tests.

## What is not here

Compression. A codec that compresses is a codec; it wraps another one and needs
nothing from this library that is not on this page.

A payload too large for a broker is also a codec's problem, and it does ship:
`ClaimCheckCodec` wraps another codec, sends anything over a threshold to a
store, and puts the key on the wire instead. See
[the claim check](patterns.md#the-claim-check).

Schema *evolution* is a different problem from serialization and has its own
answer: see [the schema registry](patterns.md#schema-registry).
