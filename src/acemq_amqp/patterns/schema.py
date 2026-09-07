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

"""Remembering what a message used to look like.

A producer and a consumer have to agree about what a message means. They do not
have to agree about which version of it they are on, and insisting that they do
is what turns adding a field into a deployment everyone has to attend.

A registry breaks that. The shape is stored once and the message carries a small
identifier; a consumer that meets a version it does not know looks it up rather
than failing. That is what lets a producer add a field on Tuesday and the last
consumer catch up in a fortnight.

The registry is deliberately incurious about what a schema *says*. It stores the
text, hashes it, and hands it back. Interpreting Avro, Protobuf or JSON Schema is
a job for a library that knows one of them, and a registry that half-understood
all three would be wrong in three ways.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from ..errors import AceMQError


class SchemaNotFoundError(AceMQError):
    """A lookup found nothing.

    Raised rather than answered with an empty definition, because a consumer
    reading a message whose schema it cannot find has a real problem — usually a
    producer registered against a different registry — and carrying on with a
    blank shape turns that into silent nonsense downstream.
    """


@dataclass(frozen=True, slots=True)
class SchemaDefinition:
    """One version of a message's shape.

    :param id: assigned by the registry, and what goes on the wire
    :param subject: groups the versions of one message type, conventionally the
        type itself — ``order.placed``
    :param version: counts from 1 within a subject
    :param format: ``avro``, ``protobuf``, ``json-schema`` — whatever it is
        written in. The registry does not interpret it
    :param definition: the schema itself
    :param fingerprint: a hash of the definition, so the same schema registered
        twice gets the same identifier rather than a second version
    :param registered_at: when it was first seen
    """

    id: int
    subject: str
    version: int
    format: str
    definition: str
    fingerprint: str
    registered_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __str__(self) -> str:
        return f"{self.subject} v{self.version} ({self.format}, id {self.id})"


@runtime_checkable
class SchemaRegistry(Protocol):
    """Where message shapes live."""

    async def register(
        self, subject: str, format: str, definition: str
    ) -> SchemaDefinition:
        """Records a schema and returns it with an identifier.

        Registering the same definition twice must return the same identifier
        rather than making a second version. Without that, a service that
        registers its schemas on every start adds a version per restart, and the
        version number stops meaning anything.
        """

    async def by_id(self, identifier: int) -> SchemaDefinition:
        """Looks a schema up by the identifier a message carries.

        :raises SchemaNotFoundError: when there is no such schema
        """

    async def latest(self, subject: str) -> SchemaDefinition:
        """The newest version of a subject.

        :raises SchemaNotFoundError: when the subject has no versions
        """

    async def versions(self, subject: str) -> Sequence[SchemaDefinition]:
        """Every version of a subject, oldest first."""


class InMemorySchemaRegistry:
    """Schemas kept in this process.

    It is not a registry in the sense that matters. Nothing is shared between
    processes, so a consumer cannot look up a schema a producer registered
    somewhere else — which is the entire point of having one. It is here for
    tests, and for seeing the shape of the thing before choosing a real one.
    """

    def __init__(self) -> None:
        self._by_id: dict[int, SchemaDefinition] = {}
        self._by_subject: dict[str, list[SchemaDefinition]] = {}
        self._by_fingerprint: dict[tuple[str, str], SchemaDefinition] = {}
        self._next_id = 1
        self._lock = threading.Lock()

    async def register(
        self, subject: str, format: str, definition: str
    ) -> SchemaDefinition:
        if not subject or not definition:
            raise ValueError("acemq: a schema needs a subject and a definition")

        print_of = fingerprint(definition)
        with self._lock:
            # The same definition registered twice is the same schema.
            already = self._by_fingerprint.get((subject, print_of))
            if already is not None:
                return already

            versions = self._by_subject.setdefault(subject, [])
            schema = SchemaDefinition(
                id=self._next_id,
                subject=subject,
                version=len(versions) + 1,
                format=format,
                definition=definition,
                fingerprint=print_of,
            )
            self._next_id += 1
            self._by_id[schema.id] = schema
            versions.append(schema)
            self._by_fingerprint[(subject, print_of)] = schema
            return schema

    async def by_id(self, identifier: int) -> SchemaDefinition:
        with self._lock:
            found = self._by_id.get(identifier)
        if found is None:
            raise SchemaNotFoundError(f"acemq: no schema with id {identifier}")
        return found

    async def latest(self, subject: str) -> SchemaDefinition:
        with self._lock:
            versions = self._by_subject.get(subject) or []
            if not versions:
                raise SchemaNotFoundError(f"acemq: no schema for subject {subject!r}")
            return versions[-1]

    async def versions(self, subject: str) -> Sequence[SchemaDefinition]:
        with self._lock:
            return list(self._by_subject.get(subject) or [])


def fingerprint(definition: str) -> str:
    """Hashes a schema definition, the same way in every language.

    SHA-256 of the exact bytes. Two definitions differing only in whitespace
    hash differently and are therefore two schemas — which looks strict and is
    the safer half of the trade. Normalising first would need a parser per
    format, and a registry that treated two definitions as one because it
    mis-parsed them would be worse than one that is merely fussy.

    :param definition: the schema text
    :returns: the hash, as lowercase hexadecimal
    """
    return hashlib.sha256(definition.encode("utf-8")).hexdigest()
