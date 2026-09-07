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

"""Turning a payload into bytes, and bytes back into a payload.

The content type is what makes this work across languages: a Java producer says
``application/json`` and a Python consumer picks the codec that answers for it,
without either end having been told about the other. A message that arrives with
no content type at all is the interesting case, and the rule is the same one the
.NET library uses — nothing has been ruled out, so every codec is a candidate
and the first that can actually read the body wins.

Decoding returns the value rather than filling a destination that was passed in,
which is where this departs from Go. Go's ``Decode`` takes a pointer because
that is how every Go decoder works, and it then needs a second interface to get
the content type to a composite codec that has to choose. Python can put the
content type on ``decode`` as a defaulted argument, so one method serves both the
codecs that need it and the codecs that do not.
"""

from __future__ import annotations

import dataclasses
import json
import threading
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

from .ack import FatalError

#: What :class:`JsonCodec` writes, and what Java, Go and .NET write.
JSON_CONTENT_TYPE = "application/json"

#: What :class:`BytesCodec` writes when nothing better is known.
BYTES_CONTENT_TYPE = "application/octet-stream"

#: What :class:`TextCodec` writes.
TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"


@runtime_checkable
class Codec(Protocol):
    """Reads and writes message bodies in one format."""

    @property
    def content_type(self) -> str:
        """What this codec writes onto a message it encodes."""

    def encode(self, payload: Any) -> bytes:
        """Turns a payload into bytes.

        :param payload: what the application wants to send
        :returns: the body
        :raises TypeError: when the payload is not something this codec writes
        """

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        """Turns bytes back into a payload.

        :param body: the message body
        :param content_type: what the sender said it was, or ``None`` when the
            sender said nothing
        :returns: the payload
        :raises FatalError: when the body cannot be read, which is fatal because
            the same bytes will not read any better on a second attempt
        """

    def can_decode(self, content_type: str | None) -> bool:
        """Whether this codec should be offered a message.

        :param content_type: the sender's content type, or ``None`` when there
            was none
        :returns: whether this codec claims it
        """


class JsonCodec:
    """Reads and writes JSON. The default, and the format every AceMQ library
    has without an extra dependency.

    Dataclasses are encoded through :func:`dataclasses.asdict`, so a payload
    written as a dataclass goes on the wire as an object with its field names.
    Everything else is handed to :mod:`json` unchanged, which covers dicts,
    lists and the scalars.

    Decoding produces dicts and lists rather than a class, because the wire says
    nothing about which class was meant. An application that wants its own type
    back constructs it from what it gets, where it can decide what a missing
    field means.
    """

    @property
    def content_type(self) -> str:
        return JSON_CONTENT_TYPE

    def encode(self, payload: Any) -> bytes:
        if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
            payload = dataclasses.asdict(payload)
        try:
            return json.dumps(payload).encode("utf-8")
        except TypeError as failure:
            raise TypeError(
                f"acemq: a {type(payload).__name__} is not something JSON can carry: {failure}"
            ) from failure

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        try:
            return json.loads(body)
        except (ValueError, UnicodeDecodeError) as failure:
            # Fatal rather than retryable: a body that is not JSON is not JSON
            # the next time either, and retrying it only holds a queue open
            # until the message ages out.
            raise FatalError(f"acemq: this message is not JSON: {failure}") from failure

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts JSON, any ``+json`` media type, and a message with none.

        Answering for a message whose sender set no content type is deliberate
        and is what the other libraries do: JSON is the default format, so an
        untyped message is far more likely to be JSON than anything else, and
        something has to read it.
        """
        if content_type is None or content_type == "":
            return True
        lowered = content_type.lower()
        return (
            lowered.startswith(JSON_CONTENT_TYPE)
            or lowered.startswith("text/json")
            or "+json" in lowered
        )


class BytesCodec:
    """Passes bodies through without touching them.

    For a payload that is already encoded — an image, something another system's
    serializer produced — and for reading a message this process has no type
    for. Replaying a dead-lettered message uses it: the bytes that were
    committed are the bytes that should go back, and re-encoding through a class
    that has since gained a field would produce something else.

    It answers for every content type, which is why it has to be asked for
    rather than found: put it last in a :class:`CompositeCodec`, or nothing
    after it is ever reached.
    """

    @property
    def content_type(self) -> str:
        return BYTES_CONTENT_TYPE

    def encode(self, payload: Any) -> bytes:
        if payload is None:
            return b""
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, bytearray | memoryview):
            return bytes(payload)
        if isinstance(payload, str):
            return payload.encode("utf-8")
        raise TypeError(
            f"acemq: BytesCodec cannot encode a {type(payload).__name__}; "
            "it takes bytes or a string"
        )

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        return bytes(body)

    def can_decode(self, content_type: str | None) -> bool:
        return True


class TextCodec:
    """Reads and writes text.

    For messages that really are text — a line of a log, a command somebody
    typed — rather than a structure that happens to be readable. Prefer
    :class:`JsonCodec` for anything with fields.
    """

    @property
    def content_type(self) -> str:
        return TEXT_CONTENT_TYPE

    def encode(self, payload: Any) -> bytes:
        if isinstance(payload, str):
            return payload.encode("utf-8")
        if isinstance(payload, bytes):
            return payload
        raise TypeError(
            f"acemq: TextCodec cannot encode a {type(payload).__name__}; "
            "it takes a string or bytes"
        )

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError as failure:
            raise FatalError(f"acemq: this message is not UTF-8 text: {failure}") from failure

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts ``text/*`` and nothing else.

        Unlike :class:`JsonCodec` it does not answer for a message with no
        content type, because there is no reason to think an untyped message is
        text — and a codec that decodes anything into a string never fails, so
        it would quietly hand a handler the printable rendering of a body it
        should have refused.
        """
        return content_type is not None and content_type.lower().startswith("text/")


class CompositeCodec:
    """Picks a codec by the message's content type.

    For a queue carrying more than one format — during a migration, or where
    several producers were written at different times::

        codec = CompositeCodec(JsonCodec(), TextCodec())

    The first codec is what it writes. All of them are offered a message to
    read, in the order they were given, and the first that claims the content
    type *and* succeeds wins. Order matters when two overlap: :class:`BytesCodec`
    answers for everything, so anything after it would never be reached.
    """

    def __init__(self, *codecs: Codec) -> None:
        if not codecs:
            raise ValueError("acemq: a CompositeCodec needs at least one codec in it")
        self._codecs: tuple[Codec, ...] = codecs

    @property
    def codecs(self) -> tuple[Codec, ...]:
        """The codecs, in the order they are tried."""
        return self._codecs

    @property
    def content_type(self) -> str:
        """The first codec's, since that is the one that encodes."""
        return self._codecs[0].content_type

    def encode(self, payload: Any) -> bytes:
        return self._codecs[0].encode(payload)

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        """Reads a body with whichever codec recognises the content type.

        With no content type nothing can be ruled out, so every codec is a
        candidate rather than the first being assumed right. Candidates are
        tried in turn and the first that reads the body wins — so a message that
        claims one format and is written in another is still read — and only
        when every one has refused is anything raised, carrying all the reasons
        rather than just the last.
        """
        if content_type is None:
            candidates = list(self._codecs)
        else:
            candidates = [codec for codec in self._codecs if codec.can_decode(content_type)]

        if not candidates:
            raise FatalError(
                f"acemq: no codec here reads {content_type!r}; "
                f"this one holds {self._describe()}"
            )

        refusals: list[str] = []
        for codec in candidates:
            try:
                return codec.decode(body, content_type)
            except Exception as failure:
                # Collected rather than raised: a codec refusing says nothing
                # about the next one, and the message that matters at the end is
                # every reason together rather than whichever came last.
                refusals.append(f"{type(codec).__name__}: {failure}")

        raise FatalError(
            "acemq: no codec could read this message. Tried " + "; ".join(refusals)
        )

    def can_decode(self, content_type: str | None) -> bool:
        return any(codec.can_decode(content_type) for codec in self._codecs)

    def _describe(self) -> str:
        return ", ".join(codec.content_type for codec in self._codecs)

    def __repr__(self) -> str:
        return f"CompositeCodec({self._describe()})"


_registry_lock = threading.Lock()
_registry: dict[str, Callable[[], Codec]] = {}


def register_codec(name: str, build: Callable[[], Codec]) -> None:
    """Makes a codec available by name.

    So that configuration can name a format without the code that reads the
    configuration importing every format it might name. Registering the same
    name twice replaces the first, which is what lets a test override a default.

    :param name: what configuration will call it, such as ``"json"``
    :param build: makes an instance
    """
    with _registry_lock:
        _registry[name] = build


def codec_by_name(name: str) -> Codec:
    """Builds the codec registered under a name.

    :param name: the registered name
    :returns: a new instance
    :raises KeyError: when nothing is registered under that name
    """
    with _registry_lock:
        build = _registry.get(name)
    if build is None:
        raise KeyError(
            f"acemq: no codec named {name!r} is registered; known: {', '.join(codec_names())}"
        )
    return build()


def codec_names() -> list[str]:
    """The registered codec names, sorted."""
    with _registry_lock:
        return sorted(_registry)


register_codec("json", JsonCodec)
register_codec("bytes", BytesCodec)
register_codec("text", TextCodec)
