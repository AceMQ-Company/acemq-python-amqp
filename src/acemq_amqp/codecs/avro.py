# Copyright 2026 AceMQ.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Reads and writes Avro::

    pip install "acemq-amqp[avro]"

    from acemq_amqp.codecs.avro import AvroCodec

    codec = AvroCodec(schema)

There is no ``codec_by_name("avro")`` and there cannot be one. Avro's bytes
describe nothing about themselves: a reader must already hold the schema the
writer used, or the message is unreadable. A no-argument factory would have
nothing to build a codec out of. That asymmetry with JSON is not an oversight;
it is the difference between the formats, made visible.

**Two ways to say where the schema comes from, and the choice matters more than
it looks.**

:class:`AvroCodec` with a schema alone fixes one schema for the codec's whole
life. Small, fast, and nothing extra to run — and the writer's schema is
whatever the reader happens to have. The moment a producer adds a field, every
consumer still holding the old schema reads the new bytes wrongly, and Avro will
not always notice. Sound only where producer and consumer are released together.
It writes ``avro/binary``.

:meth:`AvroCodec.from_registry` writes the schema's identifier into the front of
every message, so a reader can look up exactly what the writer used and let Avro
resolve it against its own. This is what makes a field addition safe, and it is
the mode to use unless there is a reason not to. It writes
``application/vnd.acemq.avro``.

**A consumer says which schema it was written against, and Avro resolves the
writer's onto it.** That is what schema evolution actually needs, and it is the
second half of the registered mode: with the writer's schema alone a consumer
receives whatever the producer sent, so a field it has never heard of arrives and
a field it expects is simply missing until the producer starts sending it. Given
both schemas, Avro resolves them — a field the reader does not know is skipped,
and one the writer never wrote is filled in from the reader's default — so the
consumer always sees the shape it was compiled against, whichever version of the
producer wrote the message:

.. code-block:: python

    codec = AvroCodec.reading(my_schema)     # consume only; nothing to register
    await codec.learn_from(registry, 1)      # the version still in production
    await codec.learn_from(registry, 2)      # and the one being rolled out

A change Avro does not call compatible — a field whose type changed, a field
added without a default — raises :class:`~acemq_amqp.ack.FatalError` naming both
schemas, rather than a record of silent nonsense. This is Java's
``registered(registry, readerSchema)`` and .NET's ``ReaderSchema``.

The framing is **one zero byte, then four bytes of identifier, big-endian, then
the Avro body** — the layout Confluent's clients use, and byte-for-byte the one
the Java and Go libraries write, so messages written here can be read by any of
them and the other way round.

**The registry in this library is async and a codec is not.** Java's registry is
a synchronous interface and Go's takes a context, so both can look a schema up
from inside ``encode``. :class:`~acemq_amqp.patterns.schema.SchemaRegistry` here
is a set of coroutines, and ``encode`` cannot await one — a codec is called from
the publisher's hot path and, in the sync facade, from a worker thread with no
loop of its own. So resolution is lifted out of the message path and done once,
explicitly:

.. code-block:: python

    codec = await AvroCodec.from_registry(registry, "order.placed", schema)
    await codec.learn_from(registry, 7)     # a writer version this consumer will meet

A message carrying an identifier the codec has not been taught raises
:class:`~acemq_amqp.ack.FatalError` naming the identifier, rather than guessing.

**This codec never volunteers for a message whose sender set no content type.**
Avro bytes are not recognisable, so answering for an untyped message would mean
decoding whatever arrived and reporting nonsense as success.

**The content type decides the framing, and the leading zero byte is only ever a
last resort.** A message that says ``avro/binary`` — or ``application/avro``, or
anything ending ``+avro`` — is read as a fixed-schema body, whatever its first
byte happens to be, because a body whose first field encodes to zero is a real
message and refusing it would be refusing the sender's own word. A message that
says ``application/vnd.acemq.avro`` is refused by a fixed-schema codec, because
reading five bytes of schema identifier as the first field produces a record
full of silent nonsense. Only when the content type is absent, or names
something that is not Avro at all, does the leading zero byte get a vote — and
then it refuses rather than guesses. All five libraries follow this rule.
"""

from __future__ import annotations

import dataclasses
import io
import json
import threading
from typing import TYPE_CHECKING, Any

from ..ack import FatalError
from ..errors import AceMQError

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from ..patterns.schema import SchemaRegistry

#: What a codec with a fixed schema writes. The same constant Java calls
#: ``FIXED_CONTENT_TYPE`` and Go calls ``FixedContentType``.
AVRO_CONTENT_TYPE = "avro/binary"

#: What a codec framing a schema identifier writes. The same constant Java calls
#: ``REGISTERED_CONTENT_TYPE`` and Go calls ``RegisteredContentType``.
AVRO_REGISTERED_CONTENT_TYPE = "application/vnd.acemq.avro"

#: Confluent's wire framing, which Java and Go both write: one zero byte, then
#: four bytes of schema identifier, big-endian, then the body.
_MAGIC = 0
_FRAME_BYTES = 5


class _NeverRaisedError(Exception):
    """Stands in for an exception type a future fastavro stopped exposing.

    Catching it catches nothing, so a resolution failure falls through to the
    general refusal below rather than the module failing to import.
    """


def _resolution_failure() -> type[BaseException]:
    """The exception fastavro raises when two schemas cannot be resolved.

    ``SchemaResolutionError`` lives in a private module and is re-exported from
    ``fastavro.read``; it is the one thing this codec needs from fastavro beyond
    reading and writing, and it is reached by name so that nothing new is
    installed and an older or newer fastavro cannot stop the import.
    """
    try:
        from fastavro import read as reading
    except ImportError:  # pragma: no cover - fastavro without its own read module
        return _NeverRaisedError
    failure = getattr(reading, "SchemaResolutionError", None)
    if isinstance(failure, type) and issubclass(failure, BaseException):
        return failure
    return _NeverRaisedError  # pragma: no cover - depends on the install


def _parse(avro: Any, schema: str | dict[str, Any], text: str) -> Any:
    """A schema fastavro can use, or a refusal saying it is not one."""
    try:
        return avro.parse_schema(json.loads(text) if isinstance(schema, str) else schema)
    except Exception as failure:
        raise AceMQError(f"acemq: this is not a usable Avro schema: {failure}") from failure


def _name_of(schema: Any) -> str:
    """What to call a schema in a failure message, which is its full name."""
    if isinstance(schema, dict):
        for key in ("name", "type"):
            value = schema.get(key)
            if isinstance(value, str):
                return value
    return str(schema)


class AvroCodec:
    """Reads and writes Avro.

    :param schema: the schema, as JSON text or as the parsed dict a schema file
        holds. This is the schema messages are written with, and — unless
        ``reader_schema`` says otherwise — the one every message is resolved onto
    :param schema_id: the registry identifier to frame into each message. Given
        this, the codec is in registered mode and writes
        ``application/vnd.acemq.avro``; without it the codec has a fixed schema
        and writes ``avro/binary``. Prefer :meth:`from_registry`, which fetches
        the identifier for you
    :param reader_schema: the schema this consumer was written against. Every
        message is resolved onto it, so a field the writer added is skipped and
        one the writer never wrote is filled in from this schema's default. It
        belongs with the registered mode, where the writer's schema varies from
        message to message; in fixed mode both ends are pinned and giving one
        only means reading a known writer onto a different reader. Prefer
        :meth:`reading`, which is the consumer's whole story in one call
    :raises AceMQError: when fastavro is not installed, or either schema is not
        usable Avro
    """

    def __init__(
        self,
        schema: str | dict[str, Any],
        *,
        schema_id: int | None = None,
        reader_schema: str | dict[str, Any] | None = None,
    ) -> None:
        try:
            import fastavro
        except ImportError as missing:  # pragma: no cover - depends on the install
            raise AceMQError(
                "acemq: AvroCodec needs fastavro, which is an optional extra. "
                'Install it with pip install "acemq-amqp[avro]"'
            ) from missing

        if schema_id is not None and schema_id <= 0:
            raise ValueError("acemq: a schema identifier is a positive integer")

        self._avro: Any = fastavro
        self._clash = _resolution_failure()
        self._text = schema if isinstance(schema, str) else json.dumps(schema)
        self._schema: Any = _parse(fastavro, schema, self._text)

        if reader_schema is None:
            self._reader_text = self._text
            self._reader: Any = self._schema
        else:
            self._reader_text = (
                reader_schema if isinstance(reader_schema, str) else json.dumps(reader_schema)
            )
            self._reader = _parse(fastavro, reader_schema, self._reader_text)

        self._schema_id = schema_id
        # Registered mode is normally "there is an identifier to frame", but a
        # codec built by reading() has no identifier and is in it all the same:
        # it reads framed messages and writes none.
        self._registered = schema_id is not None
        self._lock = threading.Lock()
        # Writer schemas by identifier, so a definition is parsed once rather
        # than on every message. An identifier stands for one schema forever.
        self._known: dict[int, Any] = {}
        if schema_id is not None:
            self._known[schema_id] = self._schema

    @classmethod
    def reading(cls, reader_schema: str | dict[str, Any]) -> AvroCodec:
        """Returns a codec that only reads, resolving every message onto a schema.

        This is the consumer's half of the registered mode, and the one place
        schema evolution is actually paid for. The codec reads framed messages —
        it claims ``application/vnd.acemq.avro`` like any registered codec — looks
        the writer's schema up among the ones it has been taught, and hands Avro
        both, so a field the producer added is skipped and a field the producer
        has not started sending yet arrives as this schema's default.

        Nothing is registered, because nothing is written: a consumer that never
        publishes has no schema to put in a registry and no identifier to frame,
        and :meth:`encode` says so rather than inventing one. Teach it the writer
        versions it will meet with :meth:`learn_from` or :meth:`learn`.

        Java spells this ``AvroCodec.registered(registry, readerSchema)`` and
        .NET reads it off ``ReaderSchema``; both can look an identifier up from
        inside ``encode`` and so keep one codec for both directions. The registry
        here is async and a codec is not, which is why the two directions are two
        objects.

        :param reader_schema: the schema this consumer was written against
        :returns: a read-only codec in registered mode
        :raises AceMQError: when fastavro is not installed, or the schema is not
            usable Avro
        """
        codec = cls(reader_schema)
        codec._registered = True
        return codec

    @classmethod
    async def from_registry(
        cls,
        registry: SchemaRegistry,
        subject: str,
        schema: str | dict[str, Any],
        *,
        reader_schema: str | dict[str, Any] | None = None,
    ) -> AvroCodec:
        """Registers a schema and returns a codec that frames its identifier.

        :param registry: where schema identifiers are resolved
        :param subject: groups the versions of one message type, conventionally
            the message type itself — ``order.placed``
        :param schema: the schema to write with, and — unless ``reader_schema``
            says otherwise — to resolve messages onto
        :param reader_schema: the schema to resolve every message onto, for a
            service that publishes one version and consumes another. Only
            ``schema`` is registered; this one is never written
        :returns: a codec in registered mode
        """
        codec = cls(schema)
        definition = await registry.register(subject, "avro", codec.schema_text)
        return cls(schema, schema_id=definition.id, reader_schema=reader_schema)

    @property
    def content_type(self) -> str:
        """``application/vnd.acemq.avro`` in registered mode, else ``avro/binary``."""
        return AVRO_REGISTERED_CONTENT_TYPE if self.is_registered else AVRO_CONTENT_TYPE

    @property
    def is_registered(self) -> bool:
        """Whether messages carry a schema identifier on the front."""
        return self._registered

    @property
    def schema_text(self) -> str:
        """The schema as text, which is what a registry stores."""
        return self._text

    @property
    def reader_schema_text(self) -> str:
        """The schema every message is resolved onto, as text.

        The same as :attr:`schema_text` unless a reader schema was given, which
        is the only case where the two differ.
        """
        return self._reader_text

    @property
    def schema_id(self) -> int | None:
        """The identifier framed into each message, or ``None`` when fixed."""
        return self._schema_id

    def learn(self, schema_id: int, schema: str | dict[str, Any]) -> None:
        """Teaches this codec a writer schema it will meet.

        :param schema_id: the identifier that arrives on the wire
        :param schema: the schema registered under it
        """
        parsed = self._avro.parse_schema(
            json.loads(schema) if isinstance(schema, str) else schema
        )
        with self._lock:
            self._known[schema_id] = parsed

    async def learn_from(self, registry: SchemaRegistry, schema_id: int) -> None:
        """Looks a writer schema up and remembers it.

        :param registry: where schema identifiers are resolved
        :param schema_id: the identifier that arrives on the wire
        :raises AceMQError: when the registry holds that identifier under some
            other format
        """
        definition = await registry.by_id(schema_id)
        if definition.format != "avro":
            raise AceMQError(
                f"acemq: schema id {schema_id} is registered as {definition.format}, and "
                "this codec reads avro. The message was written by something else."
            )
        self.learn(schema_id, definition.definition)

    def encode(self, payload: Any) -> bytes:
        if self._registered and self._schema_id is None:
            raise AceMQError(
                "acemq: this codec was built with AvroCodec.reading(...) to consume, and has "
                "no schema identifier to frame into a message. Publish with await "
                "AvroCodec.from_registry(registry, subject, schema), which registers the "
                "schema you write and gives back the identifier."
            )

        if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
            payload = dataclasses.asdict(payload)

        out = io.BytesIO()
        if self._schema_id is not None:
            out.write(bytes([_MAGIC]))
            out.write(self._schema_id.to_bytes(4, "big"))
        try:
            self._avro.schemaless_writer(out, self._schema, payload)
        except Exception as failure:
            raise TypeError(
                f"acemq: cannot write a {type(payload).__name__} as Avro against this "
                f"schema: {failure}"
            ) from failure
        return out.getvalue()

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        writer_schema = self._writer_schema_for(body, content_type)
        offset = _FRAME_BYTES if self._registered else 0
        try:
            # Writer schema and reader schema both given to Avro, which is the
            # whole point of the registered mode: it resolves the difference, so
            # a field the writer added and this reader does not know is skipped
            # rather than shifting every field after it, and a field this reader
            # expects and the writer never sent arrives as the reader's default.
            return self._avro.schemaless_reader(
                io.BytesIO(body[offset:]), writer_schema, self._reader
            )
        except FatalError:
            raise
        except self._clash as clash:
            raise FatalError(self._cannot_resolve(body, writer_schema, clash)) from clash
        except Exception as failure:
            raise FatalError(
                f"acemq: this message is not Avro this codec reads: {failure}"
            ) from failure

    def _cannot_resolve(self, body: bytes, writer_schema: Any, clash: BaseException) -> str:
        """Why two schemas would not go together, naming both of them.

        Avro's own message says what diverged — ``long is not string``, ``no
        default value for field x`` — and says nothing about whose schemas they
        were. On a queue carrying several producer versions at once that is the
        first thing anybody needs, so the identifier the message arrived with and
        the name of the schema this codec reads onto are put in front of it.
        """
        writer = _name_of(writer_schema)
        if self._registered and len(body) >= _FRAME_BYTES:
            writer = f"schema {int.from_bytes(body[1:_FRAME_BYTES], 'big')} ({writer})"
        return (
            f"acemq: this message was written with {writer} and this codec reads onto "
            f"{_name_of(self._reader)}, and Avro will not resolve the one onto the other: "
            f"{clash}. The two have diverged by something Avro does not call a compatible "
            "change — a field whose type changed, or a field added without a default — so no "
            "reader can make these bytes mean that record. Give the new field a default, or "
            "read this version with a codec built against a schema that resolves against it."
        )

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts the Avro content types, and never an absent one.

        The two framings are not interchangeable and the difference is invisible
        in the bytes: a registered message begins with five bytes of framing
        that a fixed-schema codec would read as the first field. That does not
        throw — Avro decodes the shifted bytes into whatever they happen to
        mean — so a codec that accepted the other framing would hand back a
        record full of silent nonsense. **Each mode accepts only its own
        content type**, which is Java's rule; Go's codec accepts both in either
        mode, which is the laxer of the two and the reason this follows Java.

        The remaining Avro types say nothing about framing, so both modes claim
        them and :meth:`decode` sorts it out.
        """
        if not content_type:
            return False
        lowered = content_type.lower()
        if lowered.startswith(AVRO_REGISTERED_CONTENT_TYPE):
            return self.is_registered
        if lowered.startswith(AVRO_CONTENT_TYPE) or lowered.startswith("avro/"):
            return not self.is_registered
        return "avro" in lowered and "acemq.avro" not in lowered

    def _writer_schema_for(self, body: bytes, content_type: str | None) -> Any:
        """The schema a message was written with, or a refusal saying why not."""
        if self._registered:
            if len(body) < _FRAME_BYTES or body[0] != _MAGIC:
                raise FatalError(
                    "acemq: this message has no schema identifier on the front of it. This "
                    "codec was built from a registry and reads messages written by one; "
                    "these bytes were written by a codec with a fixed schema, or by "
                    "something else entirely."
                )
            identifier = int.from_bytes(body[1:_FRAME_BYTES], "big")
            with self._lock:
                known = self._known.get(identifier)
            if known is None:
                raise FatalError(
                    f"acemq: this message was written with schema {identifier}, which this "
                    "codec has not been taught. Call await codec.learn_from(registry, "
                    f"{identifier}) before consuming, or codec.learn({identifier}, schema)."
                )
            return known

        # A fixed-schema codec handed framed bytes would decode the five bytes
        # of identifier as the beginning of the first field: no exception, and a
        # record whose every value is wrong. Java guards that by refusing any
        # body starting with a zero byte, which also refuses a legitimate
        # message whose first field encodes to zero — an int 0, a false, an
        # empty string, the first branch of a union. That is a real message this
        # library would then be unable to read.
        #
        # The content type is the better signal and it is right here, so it is
        # used first: a sender that said avro/binary is believed, and the
        # heuristic is kept only for the case where nothing useful was said,
        # where it is the only signal there is. "Nothing useful" is no content
        # type at all or one that does not name Avro — application/octet-stream
        # is as uninformative as silence, and believing it would be believing
        # nobody.
        lowered = (content_type or "").lower()
        if lowered.startswith(AVRO_REGISTERED_CONTENT_TYPE):
            raise FatalError(
                "acemq: this message says it carries a schema identifier and this codec has "
                "a fixed schema, so reading it would silently produce the wrong values. "
                "Build the codec with AvroCodec.from_registry(...) to read it."
            )
        if "avro" not in lowered and len(body) >= _FRAME_BYTES and body[0] == _MAGIC:
            raise FatalError(
                "acemq: these bytes look like they carry a schema identifier and nothing "
                "said they were Avro, so a fixed-schema codec will not guess. Build the "
                "codec with AvroCodec.from_registry(...), or set the message's content "
                "type to avro/binary if it really has a fixed schema."
            )
        return self._schema

    def __repr__(self) -> str:
        if not self._registered:
            where = "fixed"
        elif self._schema_id is None:
            where = "reading"
        else:
            where = f"schema_id={self._schema_id}"
        if self._reader_text != self._text:
            where += f", reader={_name_of(self._reader)}"
        return f"AvroCodec({where})"
