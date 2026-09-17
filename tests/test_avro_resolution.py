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

"""Where this library lands on Avro schema resolution, pinned to shared bytes.

The rule is the same in all five AceMQ libraries: **resolution happens when the
library has a reader schema to resolve onto, and not otherwise.** What differs is
where a reader schema comes from, and so how often there is one. A Go struct
carries no schema, so Go resolves only when asked. Java is handed one by a
generated class or by ``registered(registry, readerSchema)``. .NET, Python and
Ruby construct their codec *with* a schema, so there is always one.

That puts this library in the ``resolved`` column of
``tests/fixtures/avro-resolution-fixtures.json``, which
``acemq-java-amqp`` generates and every library commits a copy of. The bodies
asserted against here were not produced by this library; they are Java's bytes,
and the expected decodes are Java's expectations. ``scripts/check-fixtures.sh``
in the workspace asserts the copies are identical, because a copy that has
drifted still passes its own suite — it is simply agreeing with the wrong file.

This file goes one step further than the column the fixture assigns. Python can
reach *both* columns, because the reader schema is an argument: hand the codec
the reader's schema and it resolves onto it, hand it the writer's own schema and
resolution has nothing to do, which is the ``writerShape`` column. Asserting only
the first would pin half the rule and leave the other half a claim in prose.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from acemq_amqp.codecs.avro import AVRO_REGISTERED_CONTENT_TYPE, AvroCodec
from acemq_amqp.patterns.schema import (
    SchemaDefinition,
    SchemaNotFoundError,
    SchemaRegistry,
    fingerprint,
)

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "avro-resolution-fixtures.json").read_text("utf-8")
)
CASES: list[dict[str, Any]] = FIXTURES["cases"]

#: One subject for both fixture schemas, which is what they are: two versions of
#: the same message type, one of which removed a field and one of which added
#: one. The registry cares about the identifier, not the name.
SUBJECT = "resolution.order"


class FixtureRegistry:
    """The fixture's writer schemas, behind the ``SchemaRegistry`` protocol.

    ``InMemorySchemaRegistry`` cannot stand in here. It assigns identifiers from
    1 upwards, and the whole point of these bodies is the identifier framed into
    them: 101 and 102, chosen by the library that wrote the bytes. A registry
    that answered 101 with something else would be testing this suite's own
    arithmetic rather than Java's message.

    So the fixture's cases are pre-loaded under the identifiers the wire carries,
    and anything else registered — a consumer's own reader schema, which a
    service that publishes registers on start — gets an identifier well clear of
    them. ``register`` returns a definition already held rather than making a
    second version of it, which is the one behaviour the protocol insists on.
    """

    def __init__(self) -> None:
        self._by_id: dict[int, SchemaDefinition] = {}
        self._by_subject: dict[str, list[SchemaDefinition]] = {}
        self._next_id = 1000
        for version, entry in enumerate(CASES, start=1):
            self._remember(
                SchemaDefinition(
                    id=entry["schemaId"],
                    subject=SUBJECT,
                    version=version,
                    format="avro",
                    definition=entry["writerSchema"],
                    fingerprint=fingerprint(entry["writerSchema"]),
                )
            )

    def _remember(self, schema: SchemaDefinition) -> None:
        self._by_id[schema.id] = schema
        self._by_subject.setdefault(schema.subject, []).append(schema)

    async def register(
        self, subject: str, format: str, definition: str
    ) -> SchemaDefinition:
        for known in self._by_id.values():
            if known.subject == subject and known.definition == definition:
                return known
        versions = self._by_subject.setdefault(subject, [])
        schema = SchemaDefinition(
            id=self._next_id,
            subject=subject,
            version=len(versions) + 1,
            format=format,
            definition=definition,
            fingerprint=fingerprint(definition),
        )
        self._next_id += 1
        self._remember(schema)
        return schema

    async def by_id(self, identifier: int) -> SchemaDefinition:
        found = self._by_id.get(identifier)
        if found is None:
            raise SchemaNotFoundError(f"acemq: no schema with id {identifier}")
        return found

    async def latest(self, subject: str) -> SchemaDefinition:
        versions = self._by_subject.get(subject) or []
        if not versions:
            raise SchemaNotFoundError(f"acemq: no schema for subject {subject!r}")
        return versions[-1]

    async def versions(self, subject: str) -> Sequence[SchemaDefinition]:
        return list(self._by_subject.get(subject) or [])


def case_id(entry: dict[str, Any]) -> str:
    return str(entry["case"])


def body_of(entry: dict[str, Any]) -> bytes:
    return base64.b64decode(entry["bodyBase64"])


def removed_case() -> dict[str, Any]:
    """The case that matters: a field the writer left out and the reader declares.

    It is the one where the two columns differ in a *value* rather than in a
    spare key, and the reader schema defaults ``currency`` to ``"GBP"`` rather
    than to the empty string precisely so that an unresolved decode cannot pass
    by accident.
    """
    return next(c for c in CASES if c["case"] == "field-removed-by-the-writer")


# --------------------------------------------------------------------------
# The column this library lands on.
# --------------------------------------------------------------------------


def test_the_fixture_puts_this_library_in_the_resolved_column() -> None:
    """Which column is asserted below is the fixture's decision, not this file's."""
    mine = next(
        entry for entry in FIXTURES["libraries"] if entry["library"] == "acemq-python-amqp"
    )
    assert mine["column"] == "resolved"
    assert FIXTURES["columns"].keys() == {"resolved", "writerShape"}


async def test_a_registry_codec_resolves_onto_the_schema_it_was_built_with() -> None:
    """The default, and the reason Python is in the ``resolved`` column.

    Nobody asked for resolution here. The codec was constructed with a schema,
    which is the only way to construct one, so it always has a reader schema and
    always resolves. That is the whole of the rule as it applies to this library,
    shown on the case where the two columns disagree about a value.
    """
    entry = removed_case()
    registry = FixtureRegistry()
    codec = await AvroCodec.from_registry(registry, "consumer.schema", entry["readerSchema"])
    await codec.learn_from(registry, entry["schemaId"])

    assert codec.decode(body_of(entry), AVRO_REGISTERED_CONTENT_TYPE) == entry["resolved"]


@pytest.mark.parametrize("entry", CASES, ids=case_id)
async def test_every_case_decodes_to_the_resolved_column(entry: dict[str, Any]) -> None:
    registry = FixtureRegistry()
    codec = await AvroCodec.from_registry(registry, "consumer.schema", entry["readerSchema"])
    await codec.learn_from(registry, entry["schemaId"])

    assert codec.decode(body_of(entry), AVRO_REGISTERED_CONTENT_TYPE) == entry["resolved"]


@pytest.mark.parametrize("entry", CASES, ids=case_id)
async def test_reading_is_the_consumer_only_spelling_of_the_same_column(
    entry: dict[str, Any],
) -> None:
    """A service that only consumes reaches the same column by the shorter road."""
    registry = FixtureRegistry()
    codec = AvroCodec.reading(entry["readerSchema"])
    await codec.learn_from(registry, entry["schemaId"])

    assert codec.decode(body_of(entry), AVRO_REGISTERED_CONTENT_TYPE) == entry["resolved"]


@pytest.mark.parametrize("entry", CASES, ids=case_id)
async def test_an_explicit_reader_schema_reaches_the_same_column(
    entry: dict[str, Any],
) -> None:
    """The publish-and-consume arrangement: write one version, read onto another.

    ``reader_schema=`` is the explicit half of the rule. The schema registered
    and written is the writer's — so the identifier this codec frames is the
    fixture's own, without anything being taught — and every message is resolved
    onto the reader schema all the same.
    """
    registry = FixtureRegistry()
    codec = await AvroCodec.from_registry(
        registry, SUBJECT, entry["writerSchema"], reader_schema=entry["readerSchema"]
    )

    assert codec.schema_id == entry["schemaId"]
    assert codec.reader_schema_text == entry["readerSchema"]
    assert codec.decode(body_of(entry), AVRO_REGISTERED_CONTENT_TYPE) == entry["resolved"]


# --------------------------------------------------------------------------
# The other column, which Python reaches by being handed the writer's schema.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("entry", CASES, ids=case_id)
async def test_the_writers_own_schema_as_the_reader_schema_gives_the_writer_shape(
    entry: dict[str, Any],
) -> None:
    """The ``writerShape`` column, from a library that always resolves.

    There is no mode here that skips resolution; there is a reader schema that
    happens to be the writer's, and resolving a schema onto itself leaves the
    record as it was written. Nothing is filled in, because the reader declares
    nothing the writer did not, and nothing is skipped, because it declares
    nothing less.
    """
    registry = FixtureRegistry()
    codec = await AvroCodec.from_registry(registry, SUBJECT, entry["writerSchema"])

    assert codec.schema_id == entry["schemaId"]
    assert codec.decode(body_of(entry), AVRO_REGISTERED_CONTENT_TYPE) == entry["writerShape"]


@pytest.mark.parametrize("entry", CASES, ids=case_id)
async def test_reading_the_writers_schema_gives_the_writer_shape_too(
    entry: dict[str, Any],
) -> None:
    registry = FixtureRegistry()
    codec = AvroCodec.reading(entry["writerSchema"])
    await codec.learn_from(registry, entry["schemaId"])

    assert codec.decode(body_of(entry), AVRO_REGISTERED_CONTENT_TYPE) == entry["writerShape"]


# --------------------------------------------------------------------------
# Why the two columns are worth pinning separately.
# --------------------------------------------------------------------------


def test_the_two_columns_differ_in_a_value_and_not_only_in_a_spare_key() -> None:
    """The case somebody actually gets bitten by, stated as an assertion.

    A field the writer removed and the reader declares with a default arrives
    carrying that default under resolution and is simply absent without it. The
    default is ``"GBP"`` and not the empty string on purpose: a default that is
    also the type's zero value passes whether resolution happened or not.
    """
    entry = removed_case()

    assert entry["resolved"]["currency"] == "GBP"
    assert "currency" not in entry["writerShape"]
    assert b"GBP" not in body_of(entry), (
        "the default is supposed to be absent from the wire; if it is in the bytes, "
        "an unresolved decode would produce it too and this fixture proves nothing"
    )


def test_the_added_field_shifts_nothing_in_either_column() -> None:
    """The direction everybody expects to be dangerous, and is not."""
    entry = next(c for c in CASES if c["case"] == "field-added-by-the-writer")

    for declared in ("orderId", "total", "currency"):
        assert entry["resolved"][declared] == entry["writerShape"][declared]
    assert "channel" in entry["writerShape"]
    assert "channel" not in entry["resolved"]


@pytest.mark.parametrize("entry", CASES, ids=case_id)
def test_resolution_changes_nothing_about_the_wire(entry: dict[str, Any]) -> None:
    """Both bodies are what a plain registered codec here writes, byte for byte.

    Resolution is a reader-side decision. A library that resolves reads exactly
    the bytes one that does not reads, and this library writes exactly the bytes
    Java wrote — the framing is Confluent's in both.
    """
    body = body_of(entry)

    assert body[0] == 0
    assert int.from_bytes(body[1:5], "big") == entry["schemaId"]

    writer = AvroCodec(entry["writerSchema"], schema_id=entry["schemaId"])
    assert writer.encode(entry["writerShape"]) == body
    assert writer.content_type == AVRO_REGISTERED_CONTENT_TYPE


async def test_the_registry_this_file_stubs_is_the_one_the_library_asks_for() -> None:
    """A stub that answered a different protocol would be testing nothing."""
    registry = FixtureRegistry()
    assert isinstance(registry, SchemaRegistry)

    for entry in CASES:
        found = await registry.by_id(entry["schemaId"])
        assert found.definition == entry["writerSchema"]
        assert found.format == "avro"

    with pytest.raises(SchemaNotFoundError):
        await registry.by_id(999)
