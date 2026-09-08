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


class AvroCodec:
    """Reads and writes Avro.

    :param schema: the schema, as JSON text or as the parsed dict a schema file
        holds. This is the schema messages are written with, and — in registered
        mode — the reader schema every message is resolved onto
    :param schema_id: the registry identifier to frame into each message. Given
        this, the codec is in registered mode and writes
        ``application/vnd.acemq.avro``; without it the codec has a fixed schema
        and writes ``avro/binary``. Prefer :meth:`from_registry`, which fetches
        the identifier for you
    :raises AceMQError: when fastavro is not installed, or the schema is not
        usable Avro
    """

    def __init__(self, schema: str | dict[str, Any], *, schema_id: int | None = None) -> None:
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
        self._text = schema if isinstance(schema, str) else json.dumps(schema)
        try:
            self._schema: Any = fastavro.parse_schema(
                json.loads(self._text) if isinstance(schema, str) else schema
            )
        except Exception as failure:
            raise AceMQError(f"acemq: this is not a usable Avro schema: {failure}") from failure

        self._schema_id = schema_id
        self._lock = threading.Lock()
        # Writer schemas by identifier, so a definition is parsed once rather
        # than on every message. An identifier stands for one schema forever.
        self._known: dict[int, Any] = {}
        if schema_id is not None:
            self._known[schema_id] = self._schema

    @classmethod
    async def from_registry(
        cls,
        registry: SchemaRegistry,
        subject: str,
        schema: str | dict[str, Any],
    ) -> AvroCodec:
        """Registers a schema and returns a codec that frames its identifier.

        :param registry: where schema identifiers are resolved
        :param subject: groups the versions of one message type, conventionally
            the message type itself — ``order.placed``
        :param schema: the schema to write with, and to resolve messages onto
        :returns: a codec in registered mode
        """
        codec = cls(schema)
        definition = await registry.register(subject, "avro", codec.schema_text)
        return cls(schema, schema_id=definition.id)

    @property
    def content_type(self) -> str:
        """``application/vnd.acemq.avro`` in registered mode, else ``avro/binary``."""
        return AVRO_REGISTERED_CONTENT_TYPE if self.is_registered else AVRO_CONTENT_TYPE

    @property
    def is_registered(self) -> bool:
        """Whether messages carry a schema identifier on the front."""
        return self._schema_id is not None

    @property
    def schema_text(self) -> str:
        """The schema as text, which is what a registry stores."""
        return self._text

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
        offset = _FRAME_BYTES if self._schema_id is not None else 0
        try:
            # Writer schema and reader schema both given to Avro, which is the
            # whole point of the registered mode: it resolves the difference, so
            # a field the writer added and this reader does not know is skipped
            # rather than shifting every field after it.
            return self._avro.schemaless_reader(
                io.BytesIO(body[offset:]), writer_schema, self._schema
            )
        except FatalError:
            raise
        except Exception as failure:
            raise FatalError(
                f"acemq: this message is not Avro this codec reads: {failure}"
            ) from failure

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
        if self._schema_id is not None:
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
        # heuristic is kept only for the case where nothing was said at all,
        # where it is the only signal there is.
        if content_type and content_type.lower().startswith(AVRO_REGISTERED_CONTENT_TYPE):
            raise FatalError(
                "acemq: this message says it carries a schema identifier and this codec has "
                "a fixed schema, so reading it would silently produce the wrong values. "
                "Build the codec with AvroCodec.from_registry(...) to read it."
            )
        if not content_type and len(body) >= _FRAME_BYTES and body[0] == _MAGIC:
            raise FatalError(
                "acemq: these bytes look like they carry a schema identifier and nothing "
                "said what they are, so a fixed-schema codec will not guess. Build the "
                "codec with AvroCodec.from_registry(...), or set the message's content "
                "type to avro/binary if it really has a fixed schema."
            )
        return self._schema

    def __repr__(self) -> str:
        where = f"schema_id={self._schema_id}" if self.is_registered else "fixed"
        return f"AvroCodec({where})"
