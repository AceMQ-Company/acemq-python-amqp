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

"""Reads and writes TOML::

    pip install "acemq-amqp[toml]"

    from acemq_amqp.codecs.toml import TomlCodec

Reach for it for configuration broadcast to a fleet, feature flags, deployment
instructions and anything replayed by hand from a dead-letter queue. It is a
poor choice for high volume: it is text, it is larger than JSON, and it parses
more slowly.

**The shape of the data has to suit it.** TOML is a table format, so a message
body must be a mapping at the top level — a bare list or a bare number is not a
TOML document, and this codec says so rather than inventing a wrapper. Deep
nesting reads poorly too; where the payload is a tree rather than a table, JSON
is the honest answer. Java and Go both refuse the same shapes, and both refuse
them in ``encode`` rather than leaving the consumer to discover that nothing can
read the message.

Like the YAML codec, this one **never volunteers for a message whose sender set
no content type**. Guessing wrong here would record a TOML message arriving
where a JSON one did.

Reading uses :mod:`tomllib` from the standard library on Python 3.11 and later
and ``tomli`` — the same parser, under its original name — on 3.10. Writing has
no standard-library answer at all, so it uses ``tomli-w``. Both arrive with the
``toml`` extra; on 3.11 and later only the writer is actually installed.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from importlib import import_module
from typing import Any

from ..ack import FatalError
from ..codec import register_codec
from ..errors import AceMQError

#: What this codec writes, and what Java, Go and .NET write. The registered
#: type, as of the TOML specification's IANA entry.
TOML_CONTENT_TYPE = "application/toml"

#: Every content type this codec answers for. ``text/toml`` predates the
#: registration and is still what a lot of tooling writes. Java and Go accept
#: exactly these two and any ``+toml`` suffix.
TOML_ACCEPTS = (TOML_CONTENT_TYPE, "text/toml")


class TomlCodec:
    """Reads and writes TOML.

    :raises AceMQError: when the TOML reader or writer is not installed
    """

    def __init__(self) -> None:
        # Imported by name rather than with an import statement, because the two
        # readers are the same module under two names and which one exists
        # depends on the interpreter. A static import of both would be a lie on
        # every version.
        try:
            reader = import_module("tomllib")
        except ImportError:  # pragma: no cover - Python 3.10 only
            try:
                reader = import_module("tomli")
            except ImportError as missing:
                raise AceMQError(
                    "acemq: on Python 3.10 TomlCodec needs tomli, which the toml extra "
                    'installs. Install it with pip install "acemq-amqp[toml]"'
                ) from missing

        try:
            writer = import_module("tomli_w")
        except ImportError as missing:  # pragma: no cover - depends on the install
            raise AceMQError(
                "acemq: TomlCodec needs tomli-w to write, which is an optional extra. "
                'Install it with pip install "acemq-amqp[toml]"'
            ) from missing

        self._reader: Any = reader
        self._writer: Any = writer

    @property
    def content_type(self) -> str:
        return TOML_CONTENT_TYPE

    def encode(self, payload: Any) -> bytes:
        if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
            payload = dataclasses.asdict(payload)
        # Checked rather than left to the writer, because a message that is not
        # a TOML document has to fail at the publisher. The alternative is a
        # body nothing can read, discovered by the consumer, which is the wrong
        # end of the wire to find out.
        if not isinstance(payload, Mapping):
            raise TypeError(
                f"acemq: cannot encode a {type(payload).__name__} as TOML: a TOML document "
                "is a table, so the top level has to be a mapping. A list, a string or a "
                "number has no TOML representation — wrap it in a mapping with a named "
                "key, or use JsonCodec."
            )
        try:
            written: str = self._writer.dumps(payload)
            return written.encode("utf-8")
        except (TypeError, ValueError) as failure:
            raise TypeError(
                f"acemq: this payload is not something TOML can carry: {failure}"
            ) from failure

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        try:
            return self._reader.loads(body.decode("utf-8"))
        except (self._reader.TOMLDecodeError, UnicodeDecodeError) as failure:
            # Fatal rather than retryable: the same bytes will not parse any
            # better on a second attempt.
            raise FatalError(f"acemq: this message is not TOML: {failure}") from failure

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts ``application/toml``, ``text/toml`` and any ``+toml`` type.

        Never a message with no content type. The reasoning is YAML's: a sender
        that said nothing is almost always sending JSON, and answering for it
        would be right about the value and wrong about the format.
        """
        if not content_type:
            return False
        lowered = content_type.lower()
        return lowered.startswith(TOML_ACCEPTS) or "+toml" in lowered

    def __repr__(self) -> str:
        return "TomlCodec()"


register_codec("toml", TomlCodec)
