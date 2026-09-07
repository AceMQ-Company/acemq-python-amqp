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

"""What travels with a message, besides the message."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from . import headers


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _millis(when: datetime) -> int:
    return int(when.timestamp() * 1000)


def _from_millis(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


@dataclass(frozen=True, slots=True)
class Envelope:
    """The metadata AceMQ carries with every message.

    Frozen, because an envelope describes a message that has already been
    published or received: changing one in place would change what a log line
    said after it was written. :meth:`with_` returns a modified copy for the
    cases where a new message derives from an old one.

    Defaults are applied when the envelope is built rather than when it is
    read, so two libraries reading the same message agree without having to
    agree on a second set of rules. ``correlation_id`` defaults to ``id``, an
    attempt starts at 1, a version starts at 1, and ``type`` falls back to the
    routing key the message was published under.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: str = ""
    version: int = 1
    correlation_id: str = ""
    causation_id: str = ""
    attempt: int = 1
    first_seen: datetime = field(default_factory=_now)
    origin: str = ""
    error: str = ""
    claim: str = ""

    #: The application's own headers. Never contains a reserved name.
    headers: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.correlation_id:
            object.__setattr__(self, "correlation_id", self.id)
        if self.attempt < 1:
            object.__setattr__(self, "attempt", 1)
        if self.version < 1:
            object.__setattr__(self, "version", 1)
        # Reserved names in the application's map would be written twice and
        # read back inconsistently, so they are refused rather than dropped:
        # silently discarding a header somebody set is worse than saying no.
        offending = sorted(name for name in self.headers if headers.is_reserved(name))
        if offending:
            raise ValueError(
                "these header names belong to AceMQ and cannot be set by hand: "
                + ", ".join(offending)
            )

    @property
    def age(self) -> timedelta:
        """How long since the message was first published.

        The basis for giving up on age rather than on attempts, which is the
        honest limit when a queue has been paused: a message can be on attempt
        two and four days old.
        """
        return _now() - self.first_seen

    def with_(self, **changes: Any) -> Envelope:
        """A copy with fields changed.

        :param changes: the fields to replace
        :returns: a new envelope
        """
        return replace(self, **changes)

    def to_headers(self, routing_key: str = "") -> dict[str, Any]:
        """The AMQP headers for this envelope.

        :param routing_key: what the message is published under, used when no
            type was given
        :returns: the reserved headers, plus the application's own
        """
        written: dict[str, Any] = {
            headers.ID: self.id,
            headers.TYPE: self.type or routing_key,
            headers.VERSION: self.version,
            headers.CORRELATION: self.correlation_id or self.id,
            headers.ATTEMPT: self.attempt,
            headers.FIRST_SEEN: _millis(self.first_seen),
        }
        # Absent rather than empty. A header carrying "" is a header somebody
        # has to write a special case for at the other end.
        for name, value in (
            (headers.CAUSATION, self.causation_id),
            (headers.ORIGIN, self.origin),
            (headers.ERROR, self.error),
            (headers.CLAIM, self.claim),
        ):
            if value:
                written[name] = value

        written.update(self.headers)
        return written

    @classmethod
    def from_headers(cls, raw: Mapping[str, Any] | None, routing_key: str = "") -> Envelope:
        """Reads an envelope off a delivery.

        Anything missing takes its default, and anything unreadable takes its
        default too: a message from a producer that wrote ``x-acemq-attempt``
        as a string is still a message, and refusing to deliver it would hand
        the application an outage rather than a message.

        :param raw: the delivery's headers
        :param routing_key: what it arrived on, used when no type was set
        :returns: the envelope
        """
        raw = raw or {}
        application = {
            name: value for name, value in raw.items() if not headers.is_reserved(name)
        }

        identifier = _text(raw.get(headers.ID)) or str(uuid.uuid4())
        first_seen = raw.get(headers.FIRST_SEEN)

        return cls(
            id=identifier,
            type=_text(raw.get(headers.TYPE)) or routing_key,
            version=_number(raw.get(headers.VERSION), 1),
            correlation_id=_text(raw.get(headers.CORRELATION)) or identifier,
            causation_id=_text(raw.get(headers.CAUSATION)),
            attempt=_number(raw.get(headers.ATTEMPT), 1),
            first_seen=_from_millis(_number(first_seen, _millis(_now()))),
            origin=_text(raw.get(headers.ORIGIN)),
            error=_text(raw.get(headers.ERROR)),
            claim=_text(raw.get(headers.CLAIM)),
            headers=application,
        )


def _text(value: Any) -> str:
    """A header as a string.

    RabbitMQ's Java client sends strings as ``LongString`` and some clients
    send them as bytes, so this decodes rather than assuming.
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _number(value: Any, fallback: int) -> int:
    """A header as an integer, or the default when it is not one."""
    if isinstance(value, bool):
        return fallback
    if isinstance(value, int):
        return value
    try:
        return int(_text(value))
    except (TypeError, ValueError):
        return fallback
