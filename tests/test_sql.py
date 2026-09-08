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

"""The database-backed stores, against sqlite3 from the standard library.

sqlite3 is not the database anybody runs an outbox on, but it is a real DB-API
2.0 driver with real transactions — which is what these are about. The one thing
a test on a dictionary cannot show is that a rolled-back transaction takes the
message with it, and that is the first test here.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from acemq_amqp.errors import AceMQError
from acemq_amqp.patterns import (
    IdempotencyStore,
    OutboxRecord,
    OutboxStore,
    SchemaNotFoundError,
    SchemaRegistry,
    SqlIdempotencyStore,
    SqlOutboxStore,
    SqlSchemaRegistry,
    create_schema,
    fingerprint,
    schema_ddl,
)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[Path]:
    """A file rather than ``:memory:``, because the point of these stores is
    that several connections see the same rows."""
    path = tmp_path / "acemq.db"
    opened = sqlite3.connect(path)
    try:
        create_schema(opened)
    finally:
        opened.close()
    yield path


@pytest.fixture
def connections(database: Path) -> Callable[[], sqlite3.Connection]:
    """A fresh connection per call, which is what a pool checkout behaves like."""
    return lambda: sqlite3.connect(database)


def business_table(database: Path) -> None:
    opened = sqlite3.connect(database)
    try:
        opened.execute("CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY)")
        opened.commit()
    finally:
        opened.close()


def an_order(identifier: str = "m-1") -> OutboxRecord:
    return OutboxRecord(
        id=identifier,
        exchange="orders-events",
        routing_key="order.placed",
        body=b'{"id": "o-1"}',
        content_type="application/json",
        headers={"x-acemq-id": identifier, "x-acemq-attempt": 1},
        created_at=datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc),
    )


# --------------------------------------------------------------------------
# The outbox, and the guarantee it exists for
# --------------------------------------------------------------------------


async def test_a_rolled_back_transaction_takes_the_message_with_it(
    database: Path, connections: Callable[[], sqlite3.Connection]
) -> None:
    """The whole reason this module exists.

    The business write and the outbox insert are one transaction. Abandon it and
    there is no order and no message — not an order without a message, and not a
    message about an order that does not exist.
    """
    business_table(database)
    store = SqlOutboxStore(connections)

    work = sqlite3.connect(database)
    try:
        work.execute("INSERT INTO orders (id) VALUES ('o-1')")
        await store.add(an_order(), connection=work)
        # Both writes are visible inside the transaction...
        assert await store.count(connection=work) == 1
        work.rollback()
        # ...and neither survives it.
        assert await store.count(connection=work) == 0
        assert work.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0
    finally:
        work.close()

    # And from a connection that was never part of it, which is the check that
    # matters: nothing was committed by anybody.
    assert await store.count() == 0
    assert list(await store.pending(0)) == []


async def test_a_committed_transaction_leaves_the_message_behind(
    database: Path, connections: Callable[[], sqlite3.Connection]
) -> None:
    business_table(database)
    store = SqlOutboxStore(connections)

    work = sqlite3.connect(database)
    try:
        work.execute("INSERT INTO orders (id) VALUES ('o-1')")
        await store.add(an_order(), connection=work)
        work.commit()
    finally:
        work.close()

    waiting = list(await store.pending(0))
    assert len(waiting) == 1
    assert waiting[0].id == "m-1"
    assert waiting[0].exchange == "orders-events"
    assert waiting[0].routing_key == "order.placed"
    assert waiting[0].body == b'{"id": "o-1"}'
    assert waiting[0].content_type == "application/json"
    assert waiting[0].headers == {"x-acemq-id": "m-1", "x-acemq-attempt": 1}
    assert waiting[0].created_at == datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)


async def test_the_message_is_not_visible_to_anybody_else_before_the_commit(
    database: Path, connections: Callable[[], sqlite3.Connection]
) -> None:
    """A relay sweeping mid-transaction must not find a half-decided message."""
    store = SqlOutboxStore(connections)

    work = sqlite3.connect(database)
    try:
        await store.add(an_order(), connection=work)
        assert await store.count() == 0  # its own connection, outside the transaction
        work.commit()
        assert await store.count() == 1
    finally:
        work.close()


async def test_a_transaction_supplier_serves_the_same_purpose(
    database: Path, connections: Callable[[], sqlite3.Connection]
) -> None:
    """Where the transaction is a framework's rather than a local variable."""
    work = sqlite3.connect(database)
    try:
        store = SqlOutboxStore(connections, transaction=lambda: work)
        await store.add(an_order())
        work.rollback()
        assert await store.count() == 0
    finally:
        work.close()


async def test_an_outbox_without_a_transaction_refuses_rather_than_opening_one(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlOutboxStore(connections)

    with pytest.raises(AceMQError, match="no transactional connection"):
        await store.add(an_order())


async def test_a_supplier_that_forgets_to_return_is_told_so(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlOutboxStore(connections, transaction=lambda: None)  # type: ignore[arg-type,return-value]

    with pytest.raises(AceMQError, match="returned None"):
        await store.add(an_order())


async def test_the_same_record_twice_is_one_message(
    database: Path, connections: Callable[[], sqlite3.Connection]
) -> None:
    """A caller retrying its own transaction is the ordinary case."""
    store = SqlOutboxStore(connections)

    work = sqlite3.connect(database)
    try:
        await store.add(an_order(), connection=work)
        await store.add(an_order(), connection=work)
        work.commit()
    finally:
        work.close()

    assert await store.count() == 1


async def test_pending_is_oldest_first_and_bounded(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlOutboxStore(connections)
    base = datetime(2026, 9, 8, 10, 0, tzinfo=timezone.utc)

    work = connections()
    try:
        for offset in (2, 0, 1):
            entry = an_order(f"m-{offset}")
            await store.add(
                OutboxRecord(
                    id=entry.id,
                    exchange=entry.exchange,
                    routing_key=entry.routing_key,
                    body=entry.body,
                    content_type=entry.content_type,
                    headers=entry.headers,
                    created_at=base + timedelta(seconds=offset),
                ),
                connection=work,
            )
        work.commit()
    finally:
        work.close()

    assert [entry.id for entry in await store.pending(0)] == ["m-0", "m-1", "m-2"]
    assert [entry.id for entry in await store.pending(2)] == ["m-0", "m-1"]


async def test_marking_published_removes_the_record(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlOutboxStore(connections)

    work = connections()
    try:
        await store.add(an_order(), connection=work)
        work.commit()
    finally:
        work.close()

    await store.mark_published("m-1")
    assert await store.count() == 0
    await store.mark_published("m-1")  # twice is not an error


def test_the_outbox_store_is_an_outbox_store(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    assert isinstance(SqlOutboxStore(connections), OutboxStore)


# --------------------------------------------------------------------------
# Idempotency
# --------------------------------------------------------------------------


async def test_only_one_of_two_stores_is_told_it_is_first(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """Two consumers, two processes, one table. The insert is the claim."""
    one = SqlIdempotencyStore(connections)
    two = SqlIdempotencyStore(connections)

    firsts = await asyncio.gather(one.first_time("order-1"), two.first_time("order-1"))

    assert sorted(firsts) == [False, True]
    assert await one.size() == 1


async def test_a_confirmed_message_is_a_duplicate_afterwards(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlIdempotencyStore(connections)

    assert await store.first_time("order-1") is True
    await store.confirm("order-1")

    assert await store.is_confirmed("order-1") is True
    assert await store.first_time("order-1") is False


async def test_forgetting_lets_the_retry_run(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlIdempotencyStore(connections)

    assert await store.first_time("order-1") is True
    await store.forget("order-1")

    assert await store.first_time("order-1") is True


async def test_a_store_will_not_release_somebody_elses_claim(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """Deleting another consumer's claim would put two of them on one message."""
    mine = SqlIdempotencyStore(connections)
    theirs = SqlIdempotencyStore(connections)

    assert await mine.first_time("order-1") is True
    await theirs.forget("order-1")

    assert await theirs.first_time("order-1") is False
    assert await mine.size() == 1


async def test_a_confirmation_is_not_undone_by_a_release(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    store = SqlIdempotencyStore(connections)

    assert await store.first_time("order-1") is True
    await store.confirm("order-1")
    await store.forget("order-1")

    assert await store.is_confirmed("order-1") is True


async def test_an_expired_hold_can_be_taken_over(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """A consumer that died holding a message must not block its redelivery."""
    died = SqlIdempotencyStore(connections, claim_timeout=timedelta(milliseconds=1))
    alive = SqlIdempotencyStore(connections)

    assert await died.first_time("order-1") is True
    await asyncio.sleep(0.01)

    assert await alive.first_time("order-1") is True
    assert await alive.size() == 1


async def test_a_confirmation_outlives_a_lease(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """A confirmed message is remembered for the retention, not for the lease."""
    store = SqlIdempotencyStore(
        connections, claim_timeout=timedelta(milliseconds=1), retention=timedelta(hours=1)
    )

    assert await store.first_time("order-1") is True
    await store.confirm("order-1")
    await asyncio.sleep(0.01)

    assert await store.first_time("order-1") is False


async def test_purging_removes_what_nobody_is_bound_by(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    short = SqlIdempotencyStore(
        connections, claim_timeout=timedelta(milliseconds=1), retention=timedelta(hours=1)
    )
    kept = SqlIdempotencyStore(connections)

    await short.first_time("expired")
    await kept.first_time("held")
    await asyncio.sleep(0.01)

    assert await kept.purge_expired() == 1
    assert await kept.size() == 1
    assert await kept.is_confirmed("expired") is False


async def test_an_empty_key_is_refused(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        await SqlIdempotencyStore(connections).first_time("")


async def test_confirming_a_message_whose_row_went_records_it_anyway(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """The work happened. An unrecorded confirmation is how the same charge gets
    made twice."""
    store = SqlIdempotencyStore(connections)

    await store.confirm("order-1")  # no claim was ever taken

    assert await store.is_confirmed("order-1") is True


def test_the_idempotency_store_is_an_idempotency_store(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    assert isinstance(SqlIdempotencyStore(connections), IdempotencyStore)


# --------------------------------------------------------------------------
# The schema registry
# --------------------------------------------------------------------------


async def test_the_same_definition_twice_is_one_schema(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    registry = SqlSchemaRegistry(connections)

    first = await registry.register("order.placed", "json-schema", '{"a": 1}')
    again = await registry.register("order.placed", "json-schema", '{"a": 1}')

    assert first.id == again.id
    assert first.version == again.version == 1
    assert first.fingerprint == fingerprint('{"a": 1}')


async def test_versions_count_from_one_within_a_subject(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    registry = SqlSchemaRegistry(connections)

    one = await registry.register("order.placed", "json-schema", '{"a": 1}')
    two = await registry.register("order.placed", "json-schema", '{"a": 2}')
    other = await registry.register("order.shipped", "json-schema", '{"b": 1}')

    assert (one.version, two.version, other.version) == (1, 2, 1)
    # Identifiers are handed out by the counter and are unique across subjects.
    assert len({one.id, two.id, other.id}) == 3

    assert (await registry.latest("order.placed")).id == two.id
    assert [s.version for s in await registry.versions("order.placed")] == [1, 2]
    assert (await registry.by_id(one.id)).definition == '{"a": 1}'


async def test_registering_the_same_schema_from_several_instances_at_once(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """The unique index is what decides, so the losers re-read rather than
    writing a second identifier for the same bytes."""
    registries = [SqlSchemaRegistry(connections) for _ in range(4)]

    written = await asyncio.gather(
        *(r.register("order.placed", "json-schema", '{"a": 1}') for r in registries)
    )

    assert len({schema.id for schema in written}) == 1
    assert len(await registries[0].versions("order.placed")) == 1


async def test_a_registry_survives_a_restart(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    """Which is the whole difference from the in-memory one."""
    written = await SqlSchemaRegistry(connections).register(
        "order.placed", "json-schema", '{"a": 1}'
    )

    # A different instance, as a different process would be.
    read = await SqlSchemaRegistry(connections).by_id(written.id)

    assert read.definition == '{"a": 1}'
    assert read.registered_at.tzinfo is timezone.utc


async def test_a_missing_schema_raises(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    registry = SqlSchemaRegistry(connections)

    with pytest.raises(SchemaNotFoundError):
        await registry.by_id(404)
    with pytest.raises(SchemaNotFoundError):
        await registry.latest("nothing.here")
    assert list(await registry.versions("nothing.here")) == []


async def test_a_schema_needs_a_subject_and_a_definition(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    with pytest.raises(ValueError, match="subject and a definition"):
        await SqlSchemaRegistry(connections).register("", "json-schema", "{}")


def test_the_registry_is_a_registry(
    connections: Callable[[], sqlite3.Connection],
) -> None:
    assert isinstance(SqlSchemaRegistry(connections), SchemaRegistry)


# --------------------------------------------------------------------------
# The plumbing
# --------------------------------------------------------------------------


def test_a_table_name_that_is_not_an_identifier_is_refused() -> None:
    """It reaches SQL by concatenation, because no database binds it."""
    for hostile in ("orders; DROP TABLE users", "", "1outbox", "a" * 100):
        with pytest.raises(ValueError, match="plain SQL identifier"):
            SqlOutboxStore(table=hostile)


def test_an_unknown_paramstyle_is_refused() -> None:
    with pytest.raises(ValueError, match="paramstyle"):
        SqlOutboxStore(paramstyle="oracle")


def test_the_placeholders_follow_the_driver() -> None:
    for statement in schema_ddl(dialect="postgres", idempotency=None, registry=None):
        assert "BYTEA" in statement or "INDEX" in statement
    for statement in schema_ddl(dialect="sqlite", idempotency=None, registry=None):
        assert "BLOB" in statement or "INDEX" in statement


def test_an_unknown_dialect_is_refused() -> None:
    with pytest.raises(ValueError, match="dialect"):
        schema_ddl(dialect="mysql")


def test_the_ddl_is_printable_for_a_migration_to_own() -> None:
    statements = schema_ddl()

    assert len(statements) == 8
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS acemq_outbox") for s in statements)
    assert any(
        s.startswith("CREATE TABLE IF NOT EXISTS acemq_schema_registry_seq")
        for s in statements
    )


def test_a_store_with_no_connections_says_so() -> None:
    store = SqlIdempotencyStore()

    with pytest.raises(AceMQError, match="without a connection factory"):
        asyncio.run(store.first_time("order-1"))


async def test_tables_can_be_named(tmp_path: Path) -> None:
    """A service already using the names, or several services in one database."""
    path = tmp_path / "named.db"
    opened = sqlite3.connect(path)
    try:
        create_schema(opened, idempotency=None, registry=None, outbox="shipping_outbox")
    finally:
        opened.close()

    store = SqlOutboxStore(lambda: sqlite3.connect(path), table="shipping_outbox")
    work = sqlite3.connect(path)
    try:
        await store.add(an_order(), connection=work)
        work.commit()
    finally:
        work.close()

    assert store.table == "shipping_outbox"
    assert await store.count() == 1
