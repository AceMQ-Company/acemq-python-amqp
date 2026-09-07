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

"""Remembering what a message used to look like."""

from __future__ import annotations

import pytest

from acemq_amqp.patterns import (
    InMemorySchemaRegistry,
    SchemaNotFoundError,
    fingerprint,
)

V1 = '{"type": "record", "name": "OrderPlaced", "fields": [{"name": "id"}]}'
V2 = '{"type": "record", "name": "OrderPlaced", "fields": [{"name": "id"}, {"name": "sku"}]}'


async def test_a_schema_comes_back_with_an_identifier_and_a_version() -> None:
    registry = InMemorySchemaRegistry()

    schema = await registry.register("order.placed", "avro", V1)

    assert schema.id == 1
    assert schema.version == 1
    assert schema.subject == "order.placed"
    assert schema.format == "avro"
    assert schema.fingerprint == fingerprint(V1)
    assert str(schema) == "order.placed v1 (avro, id 1)"


async def test_the_same_definition_registered_twice_is_the_same_schema() -> None:
    # Without this a service that registers its schemas on every start adds a
    # version per restart, and the version number stops meaning anything.
    registry = InMemorySchemaRegistry()

    first = await registry.register("order.placed", "avro", V1)
    again = await registry.register("order.placed", "avro", V1)

    assert again == first
    assert len(await registry.versions("order.placed")) == 1


async def test_a_changed_definition_is_the_next_version() -> None:
    registry = InMemorySchemaRegistry()

    await registry.register("order.placed", "avro", V1)
    second = await registry.register("order.placed", "avro", V2)

    assert (second.id, second.version) == (2, 2)
    assert [schema.version for schema in await registry.versions("order.placed")] == [1, 2]
    assert (await registry.latest("order.placed")).version == 2


async def test_subjects_are_versioned_separately() -> None:
    registry = InMemorySchemaRegistry()

    await registry.register("order.placed", "avro", V1)
    other = await registry.register("order.shipped", "avro", V1)

    # The same definition under a different subject is a different schema, and
    # its version counts from one again.
    assert other.version == 1
    assert other.id == 2


async def test_a_message_can_find_the_shape_it_names() -> None:
    registry = InMemorySchemaRegistry()
    written = await registry.register("order.placed", "avro", V1)

    assert await registry.by_id(written.id) == written


async def test_a_schema_that_is_not_there_is_said_rather_than_shrugged_at() -> None:
    # A consumer reading a message whose schema it cannot find has a real
    # problem, and carrying on with a blank shape turns that into silent
    # nonsense downstream.
    registry = InMemorySchemaRegistry()

    with pytest.raises(SchemaNotFoundError, match="no schema with id 9"):
        await registry.by_id(9)
    with pytest.raises(SchemaNotFoundError, match=r"order\.placed"):
        await registry.latest("order.placed")
    assert await registry.versions("order.placed") == []


async def test_a_schema_needs_a_subject_and_a_definition() -> None:
    registry = InMemorySchemaRegistry()

    with pytest.raises(ValueError, match="subject and a definition"):
        await registry.register("", "avro", V1)
    with pytest.raises(ValueError, match="subject and a definition"):
        await registry.register("order.placed", "avro", "")


def test_the_fingerprint_is_of_the_exact_bytes() -> None:
    # Strict on purpose: normalising first would need a parser per format, and a
    # registry that treated two definitions as one because it mis-parsed them
    # would be worse than one that is merely fussy.
    assert fingerprint(V1) != fingerprint(V1 + " ")
    assert fingerprint(V1) == fingerprint(V1)
    # SHA-256 of the exact bytes, the same in every language.
    assert fingerprint("") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
