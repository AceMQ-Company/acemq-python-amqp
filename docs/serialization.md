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

## The three that ship

| | Writes | Reads |
|---|---|---|
| `JsonCodec` | `application/json` | JSON, `text/json`, any `+json` type, and a message with **no** content type |
| `TextCodec` | `text/plain; charset=utf-8` | `text/*` and nothing else |
| `BytesCodec` | `application/octet-stream` | everything |

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
are registered by the library.

`register_codec` takes a **factory**, not an instance, so `codec_by_name`
returns a new one each time. Registering the same name twice replaces the first,
which is what lets a test override a default. `codec_by_name` raises `KeyError`
listing the names it does know.

## What is not here

No XML, YAML, TOML, Protobuf or Avro. The Go library has a module each for those
and Python could have the same, but a codec is thirty lines and a dependency —
and the dependency is the part that matters, because taking one here would put
every user of this package on it.

Encryption is the same shape. A codec that encrypts is a codec, and so is a
codec that compresses; both wrap another one and neither needs anything from
this library that is not on this page. An [interceptor](interceptors.md) is the
alternative for encryption specifically, because it sees the payload before the
codec runs and applies to every publisher without being remembered at each call
site.

Schema *evolution* is a different problem from serialization and has its own
answer: see [the schema registry](patterns.md#schema-registry).
