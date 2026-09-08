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

"""The three storage seams, backed by a database instead of a dictionary.

:class:`~acemq_amqp.patterns.IdempotencyStore`,
:class:`~acemq_amqp.patterns.OutboxStore` and
:class:`~acemq_amqp.patterns.SchemaRegistry` each shipped with an in-memory
implementation that says in its own docstring why it is not the one to use. This
module is the one to use: the same three seams over whatever database the
service already has.

The outbox is the reason this exists. Its in-memory store admits that nothing
there shares a transaction with a database, so a crash between the work
committing and the record being written loses the message exactly as publishing
directly would — which is the entire gap the pattern exists to close. So
:meth:`SqlOutboxStore.add` writes on a connection **you** hand it, inside the
transaction that is already doing your work, and it neither commits nor closes
it. Roll that transaction back and the message is not in the outbox, because it
never was: it is one insert among yours.

**No driver dependency.** Nothing here imports a database driver. The code is
written against the DB-API 2.0 connection and cursor protocols, which is a
contract every Python database driver already implements, so the same class
serves :mod:`sqlite3`, psycopg and anything else that follows PEP 249. What
differs between them is the placeholder — ``?`` or ``%s`` — and that is a
constructor argument.

**What has actually been run.** The automated suite exercises :mod:`sqlite3`,
which is in the standard library, so the tests need nothing installed. The same
checks have been run by hand against PostgreSQL 17 through psycopg 3 —
``create_schema``, the rollback, the round trip through ``bytea``, the claim
race, the lease take-over and four registries registering one schema at once —
and they pass; they are not in the suite, because a suite that needs a database
server is a suite that gets skipped. Anything else — MySQL, SQL Server, Oracle —
will need at least a different upsert and is not claimed.

**Times are epoch milliseconds**, in a ``BIGINT``. The same units as
``x-acemq-first-seen`` on the envelope, and the one representation of an instant
that every database and every driver agrees about without a per-driver
conversion. A timestamp column would read better in ``psql`` and would have to
be adapted, read back and compared differently on each of them.

**Creating tables is for development.** ``create_schema`` is here so a test and a
first afternoon are one line. In production the tables belong in whatever
migration tool already owns the schema, alongside the business tables the outbox
commits with: a library that creates tables at start-up has taken a decision
about when your database changes that is not its to take.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

from ..errors import AceMQError
from .outbox import OutboxRecord
from .schema import SchemaDefinition, SchemaNotFoundError, fingerprint

#: Where an idempotency store keeps its rows unless it is told otherwise.
DEFAULT_IDEMPOTENCY_TABLE = "acemq_idempotency"

#: Where an outbox keeps its rows unless it is told otherwise.
DEFAULT_OUTBOX_TABLE = "acemq_outbox"

#: Where a schema registry keeps its rows unless it is told otherwise. The
#: counter lives in the same name with ``_seq`` on the end.
DEFAULT_REGISTRY_TABLE = "acemq_schema_registry"

#: How long a claim is held before another consumer may take it over.
#:
#: The window in which a consumer that died holding a message blocks its
#: redelivery. Long enough that an ordinary handler finishes inside it, short
#: enough that a crash does not park the message for an afternoon.
DEFAULT_CLAIM_TIMEOUT = timedelta(minutes=5)

#: How long a handled message is remembered.
#:
#: It should comfortably exceed the longest a message can go on being retried,
#: because a key forgotten while a redelivery is still possible is a duplicate.
DEFAULT_RETENTION = timedelta(hours=24)

_CLAIMED = "CLAIMED"
_CONFIRMED = "CONFIRMED"

# A table name reaches SQL by concatenation, because no database lets it be
# bound as a parameter. Refusing anything but a plain identifier is what keeps
# that safe.
_SAFE_TABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")

#: The placeholder styles this module writes. ``qmark`` is :mod:`sqlite3`;
#: ``format`` is psycopg, psycopg2 and mysqlclient.
PARAMSTYLES = {"qmark": "?", "format": "%s"}


class DbCursor(Protocol):
    """As much of a DB-API 2.0 cursor as this module uses."""

    @property
    def rowcount(self) -> int:
        """How many rows the last statement affected."""

    def execute(self, operation: str, parameters: Any = ..., /) -> Any:
        """Runs one statement."""

    def fetchone(self) -> Any:
        """The next row, or ``None``."""

    def fetchall(self) -> Any:
        """Every remaining row."""

    def close(self) -> None:
        """Releases the cursor."""


@runtime_checkable
class DbConnection(Protocol):
    """As much of a DB-API 2.0 connection as this module uses.

    Deliberately small. Anything following PEP 249 has these four, so nothing
    here is tied to a driver — and a caller's connection, whatever framework it
    came out of, is one of these.
    """

    def cursor(self) -> DbCursor:
        """A cursor on this connection."""

    def commit(self) -> None:
        """Commits the open transaction."""

    def rollback(self) -> None:
        """Abandons the open transaction."""

    def close(self) -> None:
        """Releases the connection."""


#: Where a store gets a connection it owns: a callable returning one it may
#: commit, roll back and close. A pool's checkout is exactly this shape, and so
#: is ``lambda: sqlite3.connect(path)``.
Connections = Callable[[], DbConnection]


class _Sql:
    """A table name and a placeholder style, checked once."""

    def __init__(self, table: str, paramstyle: str) -> None:
        if not _SAFE_TABLE.fullmatch(table):
            raise ValueError(
                f"acemq: a table name must be a plain SQL identifier, got {table!r}"
            )
        if paramstyle not in PARAMSTYLES:
            raise ValueError(
                f"acemq: paramstyle must be one of {sorted(PARAMSTYLES)}, got {paramstyle!r}"
            )
        self.table = table
        self.paramstyle = paramstyle
        self._mark = PARAMSTYLES[paramstyle]

    def __call__(self, statement: str) -> str:
        """Fills in the table name and the placeholders.

        ``{table}`` becomes the table, and every ``?`` becomes whatever this
        driver wants. Written with ``?`` in the source because that is the one
        that reads as a placeholder rather than as a format string.
        """
        return statement.format(table=self.table).replace("?", self._mark)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _as_ms(when: datetime) -> int:
    return int(when.timestamp() * 1000)


def _from_ms(stamp: int) -> datetime:
    return datetime.fromtimestamp(stamp / 1000, timezone.utc)


class _Work:
    """Runs statements on either the caller's connection or one of our own.

    The difference is the whole of the transaction story, so it lives in one
    place rather than being repeated with a comment at every call site:

    * A connection the **caller** supplied is borrowed. It is not committed and
      not closed, because the transaction that owns it is not finished and
      whether it commits is the caller's decision — which is exactly what makes
      an outbox insert and a business write one atomic thing.
    * A connection the **store** opened is owned. It is committed on success,
      rolled back on failure, and closed either way.
    """

    def __init__(self, connections: Connections | None) -> None:
        self._connections = connections

    def open(self) -> DbConnection:
        if self._connections is None:
            raise AceMQError(
                "acemq: this store was built without a connection factory, so it can only "
                "work on a connection passed to the call. Pass connections= to the "
                "constructor, or connection= to this method."
            )
        return self._connections()


def _run(
    work: _Work,
    connection: DbConnection | None,
    statements: Callable[[DbCursor], Any],
) -> Any:
    """Runs a unit of work, committing only what we opened ourselves."""
    if connection is not None:
        cursor = connection.cursor()
        try:
            return statements(cursor)
        finally:
            cursor.close()

    owned = work.open()
    try:
        cursor = owned.cursor()
        try:
            answer = statements(cursor)
        finally:
            cursor.close()
        owned.commit()
        return answer
    except BaseException:
        owned.rollback()
        raise
    finally:
        owned.close()


async def _off_loop(
    work: _Work,
    connection: DbConnection | None,
    statements: Callable[[DbCursor], Any],
) -> Any:
    """Runs a unit of work where it belongs.

    A DB-API driver is blocking, so work on a connection the store owns goes to
    a worker thread and the event loop keeps running. Work on the *caller's*
    connection does not: their transaction belongs to their thread — sqlite3
    refuses a connection used from another one, and a framework's
    transaction-bound connection is a thread local — so moving it would be
    wrong in a way that only shows up under load.
    """
    if connection is not None:
        return _run(work, connection, statements)
    return await asyncio.to_thread(_run, work, None, statements)


#: The one column type the two databases disagree about. Everything else in the
#: DDL — ``BIGINT``, ``VARCHAR``, ``TEXT``, ``INTEGER`` — both accept as written.
BLOB_TYPES = {"sqlite": "BLOB", "postgres": "BYTEA"}


def create_schema(
    connections: Connections | DbConnection,
    *,
    dialect: str = "sqlite",
    idempotency: str | None = DEFAULT_IDEMPOTENCY_TABLE,
    outbox: str | None = DEFAULT_OUTBOX_TABLE,
    registry: str | None = DEFAULT_REGISTRY_TABLE,
) -> None:
    """Creates whichever of the three tables are asked for, if they are absent.

    For development and for tests. In production these belong in the migration
    tool that already owns the schema — the outbox table especially, because it
    has to live in the same database as the business tables it commits with, and
    that database's shape is not a messaging library's to change at start-up.
    The DDL is printable with :func:`schema_ddl` for exactly that reason: copy it
    into a migration and stop calling this.

    :param connections: a connection, or a callable returning one
    :param dialect: ``"sqlite"`` or ``"postgres"``, which decides one column type
    :param idempotency: the idempotency table, or ``None`` to skip it
    :param outbox: the outbox table, or ``None`` to skip it
    :param registry: the schema registry table, or ``None`` to skip it. Its
        counter table is created alongside, named with ``_seq`` on the end
    """
    # Asked "is it a connection?" rather than "is it callable?", because a
    # sqlite3 connection is itself callable — it compiles a statement — and a
    # test for callability quietly picks the wrong branch for the one driver
    # everybody starts with.
    given = isinstance(connections, DbConnection)
    opened = connections if isinstance(connections, DbConnection) else connections()
    cursor = opened.cursor()
    try:
        for statement in schema_ddl(
            dialect=dialect, idempotency=idempotency, outbox=outbox, registry=registry
        ):
            cursor.execute(statement)
    finally:
        cursor.close()
    opened.commit()
    if not given:
        # Ours to close. A connection the caller handed over is not.
        opened.close()


def schema_ddl(
    *,
    dialect: str = "sqlite",
    idempotency: str | None = DEFAULT_IDEMPOTENCY_TABLE,
    outbox: str | None = DEFAULT_OUTBOX_TABLE,
    registry: str | None = DEFAULT_REGISTRY_TABLE,
) -> list[str]:
    """The statements :func:`create_schema` would run, for a migration to own.

    :param dialect: ``"sqlite"`` or ``"postgres"``
    :param idempotency: the idempotency table, or ``None`` to skip it
    :param outbox: the outbox table, or ``None`` to skip it
    :param registry: the schema registry table, or ``None`` to skip it
    :returns: the statements, in the order they must run
    """
    if dialect not in BLOB_TYPES:
        raise ValueError(
            f"acemq: dialect must be one of {sorted(BLOB_TYPES)}, got {dialect!r}"
        )
    blob = BLOB_TYPES[dialect]

    statements: list[str] = []
    for table, ddl in (
        (idempotency, _IDEMPOTENCY_DDL),
        (outbox, _OUTBOX_DDL),
        (registry, _REGISTRY_DDL),
    ):
        if table is None:
            continue
        written = _Sql(table, "qmark")
        statements += [written(one).replace("%BLOB%", blob).strip() for one in ddl]
    return statements


_IDEMPOTENCY_DDL = (
    """
    CREATE TABLE IF NOT EXISTS {table} (
        message_id  VARCHAR(255) NOT NULL PRIMARY KEY,
        state       VARCHAR(16)  NOT NULL,
        claimed_by  VARCHAR(64),
        recorded_at BIGINT       NOT NULL,
        expires_at  BIGINT       NOT NULL
    )
    """,
    # One column carries both deadlines because they are never both meaningful:
    # while a row is CLAIMED it holds the lease expiry, and once CONFIRMED it
    # holds the retention expiry. Both answer the same question -- is this row
    # still binding? -- so one index serves the claim path and the purge.
    "CREATE INDEX IF NOT EXISTS {table}_expiry ON {table} (expires_at)",
)

_OUTBOX_DDL = (
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id            VARCHAR(64)  NOT NULL PRIMARY KEY,
        exchange_name VARCHAR(255) NOT NULL,
        routing_key   VARCHAR(255) NOT NULL,
        body          %BLOB%       NOT NULL,
        content_type  VARCHAR(255) NOT NULL,
        headers       TEXT         NOT NULL,
        created_at    BIGINT       NOT NULL
    )
    """,
    # The relay's only query is "oldest first", so that is what is indexed.
    "CREATE INDEX IF NOT EXISTS {table}_pending ON {table} (created_at)",
)

_REGISTRY_DDL = (
    """
    CREATE TABLE IF NOT EXISTS {table} (
        id            INTEGER      NOT NULL PRIMARY KEY,
        subject       VARCHAR(255) NOT NULL,
        version       INTEGER      NOT NULL,
        format        VARCHAR(32)  NOT NULL,
        definition    TEXT         NOT NULL,
        fingerprint   VARCHAR(64)  NOT NULL,
        registered_at BIGINT       NOT NULL
    )
    """,
    # The fingerprint is what makes registration idempotent: the same schema
    # offered twice must come back with the same id, from any instance, forever.
    # Unique rather than merely indexed, so two instances racing to register the
    # same schema end with one row and one loser that re-reads, instead of two
    # ids for the same bytes.
    "CREATE UNIQUE INDEX IF NOT EXISTS {table}_fingerprint ON {table} (subject, fingerprint)",
    "CREATE UNIQUE INDEX IF NOT EXISTS {table}_version ON {table} (subject, version)",
    # One row, holding the last identifier handed out.
    #
    # A sequence would be the obvious tool and is spelled differently on every
    # database this has to run on. Taking the highest id and adding one needs no
    # sequence and is wrong under load: two writers registering two different
    # schemas at the same moment compute the same next id, and one of them loses
    # a race it cannot win by retrying, because the writer it lost to is doing
    # the same arithmetic. Updating this row takes a row lock, so writers queue
    # for an instant and every one of them gets a number. Registration happens
    # once per schema in the lifetime of a system, so serialising it costs
    # nothing worth measuring.
    """
    CREATE TABLE IF NOT EXISTS {table}_seq (
        only_row INTEGER NOT NULL PRIMARY KEY,
        last_id  INTEGER NOT NULL
    )
    """,
)


class SqlIdempotencyStore:
    """Remembers handled messages in a table every consumer can see.

    This is what :class:`~acemq_amqp.patterns.InMemoryIdempotencyStore` points
    at when it says the store has to be one the workers share. Two consumers
    racing on the same message cannot both be told they are first, because the
    insert *is* the claim and a primary key makes it atomic across every process
    using the table — whatever else they are doing, and without a lock anybody
    has to remember to take.

    **A claim is a lease, not a fact.** A row starts ``CLAIMED`` with a deadline
    ``claim_timeout`` away. :meth:`confirm` turns it into ``CONFIRMED`` with a
    deadline ``retention`` away, and :func:`~acemq_amqp.patterns.idempotent`
    calls that for you when a handler accepts. The lease is what a fact cannot
    do: a consumer that dies holding a message would otherwise have recorded it
    as handled without handling it, and the redelivery — the thing at-least-once
    delivery is *for* — would be skipped. So a hold that has run out can be
    taken over, and the message gets handled by somebody.

    Nothing on the message path deletes anything. Schedule :meth:`purge_expired`
    instead; hourly is ample. A store that tidies up on the hot path makes every
    message pay for it.

    :param connections: a callable returning a connection this store may commit
        and close — a pool checkout, or ``lambda: sqlite3.connect(path)``
    :param table: where the rows go
    :param paramstyle: ``"qmark"`` for :mod:`sqlite3`, ``"format"`` for psycopg
    :param claim_timeout: how long a claim is held before it can be taken over
    :param retention: how long a handled message is remembered
    """

    def __init__(
        self,
        connections: Connections | None = None,
        *,
        table: str = DEFAULT_IDEMPOTENCY_TABLE,
        paramstyle: str = "qmark",
        claim_timeout: timedelta = DEFAULT_CLAIM_TIMEOUT,
        retention: timedelta = DEFAULT_RETENTION,
    ) -> None:
        if claim_timeout <= timedelta(0):
            raise ValueError(f"acemq: a claim timeout must be positive, got {claim_timeout}")
        if retention <= timedelta(0):
            raise ValueError(f"acemq: a retention window must be positive, got {retention}")
        self._sql = _Sql(table, paramstyle)
        self._work = _Work(connections)
        self._claim_timeout = claim_timeout
        self._retention = retention
        # Which process holds a claim. Only this instance may release its own,
        # because releasing somebody else's would put two consumers on one
        # message.
        self._node = uuid.uuid4().hex

    @property
    def table(self) -> str:
        """Where the rows are."""
        return self._sql.table

    @property
    def node(self) -> str:
        """What this instance writes into ``claimed_by``."""
        return self._node

    async def first_time(
        self, key: str, *, connection: DbConnection | None = None
    ) -> bool:
        """Claims a message, and says whether the claim was ours to take.

        :param key: what identifies the message
        :param connection: a connection to work on instead of one of our own.
            Pass the one your business transaction is using and the claim
            becomes durable exactly when your work does
        :returns: whether this is the first time
        """
        if not key:
            raise ValueError("acemq: an idempotency key cannot be empty")
        now = _now_ms()

        def statements(cursor: DbCursor) -> bool:
            # The insert is the claim, and the primary key is what makes it
            # atomic. ON CONFLICT DO NOTHING rather than catching the driver's
            # integrity error, because every driver spells that differently and
            # a failed statement poisons an open PostgreSQL transaction.
            cursor.execute(
                self._sql(
                    "INSERT INTO {table} "
                    "(message_id, state, claimed_by, recorded_at, expires_at) "
                    "VALUES (?, ?, ?, ?, ?) ON CONFLICT (message_id) DO NOTHING"
                ),
                (key, _CLAIMED, self._node, now, now + _ms(self._claim_timeout)),
            )
            if cursor.rowcount == 1:
                return True

            # Somebody already has a row. Taking it over is allowed only if
            # their hold has run out, and the guard lives in the WHERE clause so
            # the check and the take-over are one statement: a select followed
            # by an update would let two consumers both pass the check and both
            # conclude they had won.
            cursor.execute(
                self._sql(
                    "UPDATE {table} SET state = ?, claimed_by = ?, recorded_at = ?, "
                    "expires_at = ? WHERE message_id = ? AND expires_at <= ?"
                ),
                (
                    _CLAIMED,
                    self._node,
                    now,
                    now + _ms(self._claim_timeout),
                    key,
                    now,
                ),
            )
            return cursor.rowcount == 1

        answer = await _off_loop(self._work, connection, statements)
        return bool(answer)

    async def confirm(self, key: str, *, connection: DbConnection | None = None) -> None:
        """Records that the message really was handled.

        Called by :func:`~acemq_amqp.patterns.idempotent` when a handler
        accepts. Until it happens the row is a lease that will expire; after it,
        the message is remembered for ``retention`` and every redelivery inside
        that window is a duplicate.

        :param key: what identifies the message
        :param connection: a connection to work on instead of one of our own
        """
        now = _now_ms()

        def statements(cursor: DbCursor) -> None:
            cursor.execute(
                self._sql(
                    "UPDATE {table} SET state = ?, claimed_by = NULL, recorded_at = ?, "
                    "expires_at = ? WHERE message_id = ?"
                ),
                (_CONFIRMED, now, now + _ms(self._retention), key),
            )
            if cursor.rowcount:
                return
            # The row was purged, or the lease expired and somebody else took
            # it. The work still happened, so it is recorded either way: an
            # unrecorded confirmation is how the same charge gets made twice.
            cursor.execute(
                self._sql(
                    "INSERT INTO {table} "
                    "(message_id, state, claimed_by, recorded_at, expires_at) "
                    "VALUES (?, ?, NULL, ?, ?) ON CONFLICT (message_id) DO NOTHING"
                ),
                (key, _CONFIRMED, now, now + _ms(self._retention)),
            )

        await _off_loop(self._work, connection, statements)

    async def forget(self, key: str, *, connection: DbConnection | None = None) -> None:
        """Releases our own claim, so the message can be tried again.

        Only our own, and never a confirmation: deleting somebody else's claim
        would put two consumers on one message, and deleting a confirmation
        would undo it.

        :param key: what identifies the message
        :param connection: a connection to work on instead of one of our own
        """

        def statements(cursor: DbCursor) -> None:
            cursor.execute(
                self._sql(
                    "DELETE FROM {table} "
                    "WHERE message_id = ? AND state = ? AND claimed_by = ?"
                ),
                (key, _CLAIMED, self._node),
            )

        await _off_loop(self._work, connection, statements)

    async def is_confirmed(
        self, key: str, *, connection: DbConnection | None = None
    ) -> bool:
        """Whether a message is recorded as handled and still inside retention.

        For an operator asking why a message did nothing. Expired rows answer
        ``False`` rather than being deleted: a read that writes turns every
        duplicate check into a write on a shared table.
        """

        def statements(cursor: DbCursor) -> bool:
            cursor.execute(
                self._sql("SELECT expires_at FROM {table} WHERE message_id = ? AND state = ?"),
                (key, _CONFIRMED),
            )
            row = cursor.fetchone()
            return row is not None and int(row[0]) > _now_ms()

        return bool(await _off_loop(self._work, connection, statements))

    async def purge_expired(self, *, connection: DbConnection | None = None) -> int:
        """Removes rows nobody is bound by any more.

        Confirmations past their retention, and claims whose lease has run out.
        Schedule it; nothing on the message path calls it.

        :returns: how many rows went
        """

        def statements(cursor: DbCursor) -> int:
            cursor.execute(
                self._sql("DELETE FROM {table} WHERE expires_at <= ?"), (_now_ms(),)
            )
            return max(cursor.rowcount, 0)

        return int(await _off_loop(self._work, connection, statements))

    async def size(self, *, connection: DbConnection | None = None) -> int:
        """How many rows the table holds, expired ones included."""

        def statements(cursor: DbCursor) -> int:
            cursor.execute(self._sql("SELECT COUNT(*) FROM {table}"))
            row = cursor.fetchone()
            return int(row[0])

        return int(await _off_loop(self._work, connection, statements))

    def __repr__(self) -> str:
        return f"SqlIdempotencyStore({self._sql.table})"


class SqlOutboxStore:
    """An outbox in the database the work is written to.

    The point of the whole pattern, and the thing
    :class:`~acemq_amqp.patterns.InMemoryOutboxStore` cannot do::

        async with database.transaction() as tx:
            await place_order(tx, order)
            await store.add(
                record(mq, "orders-events", "order.placed", event),
                connection=tx.connection,
            )
        # one commit; the order and the message are the same decision

    :meth:`add` writes on the connection you hand it and does not commit, does
    not roll back and does not close it. That is not an oversight — it is the
    guarantee. The insert becomes durable exactly when your transaction does,
    and if your transaction rolls back the message was never in the outbox to
    begin with. A store that opened a connection of its own would have the gap
    straight back, and would have bought nothing but a second place for messages
    to go missing.

    The connection can come from either end. Pass ``connection=`` at the call
    site, which is clearest where the transaction is a local variable; or give
    the constructor a ``transaction`` callable and let it fetch the connection
    bound to the current transaction, which is what a framework with a
    thread-local or context-local session wants. What it must never be is
    "open a new connection": a fresh connection with autocommit on inserts the
    outbox row immediately and independently, so a later rollback of the
    business work leaves a message queued for something that never happened —
    the exact fault the pattern was adopted to prevent, now harder to notice
    because the code looks right.

    The relay's own work — :meth:`pending` and :meth:`mark_published` — is not
    the caller's transaction and has no business joining it, so it uses
    ``connections`` and commits for itself.

    **One relay per outbox.** :meth:`pending` takes no lease, because the
    :class:`~acemq_amqp.patterns.OutboxStore` protocol has nowhere to put one,
    so two processes sweeping the same table publish everything twice. Java's
    JDBC store leases rows for this reason; this one does not, and the shape of
    the mistake is a service that starts a relay per worker.

    :param connections: a callable returning a connection for the relay's own
        work, which this store may commit and close
    :param transaction: a callable returning the connection belonging to the
        caller's current transaction, for :meth:`add`
    :param table: where the records go
    :param paramstyle: ``"qmark"`` for :mod:`sqlite3`, ``"format"`` for psycopg
    """

    def __init__(
        self,
        connections: Connections | None = None,
        *,
        transaction: Connections | None = None,
        table: str = DEFAULT_OUTBOX_TABLE,
        paramstyle: str = "qmark",
    ) -> None:
        self._sql = _Sql(table, paramstyle)
        self._relay = _Work(connections)
        self._transaction = transaction

    @property
    def table(self) -> str:
        """Where the records are."""
        return self._sql.table

    async def add(
        self, entry: OutboxRecord, *, connection: DbConnection | None = None
    ) -> None:
        """Writes a record into the caller's transaction.

        :param entry: what to publish later, already encoded
        :param connection: the connection your transaction is using. Omit it and
            the ``transaction`` callable given to the constructor is asked
        :raises AceMQError: when there is no transactional connection to write
            into, which is refused rather than worked around: opening one here
            would silently reintroduce the gap
        """
        if not entry.id:
            raise ValueError("acemq: an outbox record needs an id")

        joining = connection if connection is not None else self._caller_connection()

        def statements(cursor: DbCursor) -> None:
            # Kept rather than replaced. A caller retrying its own transaction
            # is the ordinary case, and the second write is the same message.
            cursor.execute(
                self._sql(
                    "INSERT INTO {table} (id, exchange_name, routing_key, body, "
                    "content_type, headers, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (id) DO NOTHING"
                ),
                (
                    entry.id,
                    entry.exchange,
                    entry.routing_key,
                    entry.body,
                    entry.content_type,
                    json.dumps(dict(entry.headers)),
                    _as_ms(entry.created_at),
                ),
            )

        # Deliberately inline and deliberately not committed: it is the caller's
        # connection, on the caller's thread, inside the caller's transaction.
        cursor = joining.cursor()
        try:
            statements(cursor)
        finally:
            cursor.close()

    def _caller_connection(self) -> DbConnection:
        if self._transaction is None:
            raise AceMQError(
                "acemq: the outbox has no transactional connection to write into. Pass "
                "connection= to add(), or transaction= to the constructor. Opening one "
                "here would put the message outside your transaction, which is the gap "
                "the outbox exists to close."
            )
        joining = self._transaction()
        if joining is None:
            # Typed as never happening, and it happens: a supplier that forgets
            # to return is a one-character mistake, and the message it would
            # otherwise produce is an AttributeError inside a cursor() call.
            raise AceMQError(
                "acemq: the transaction callable returned None; the outbox needs the "
                "caller's transaction to write into."
            )
        return joining

    async def pending(
        self, limit: int, *, connection: DbConnection | None = None
    ) -> Sequence[OutboxRecord]:
        """Records waiting to be published, oldest first.

        :param limit: at most this many, or zero for all of them
        :param connection: a connection to read on instead of one of our own
        """

        def statements(cursor: DbCursor) -> list[OutboxRecord]:
            statement = (
                "SELECT id, exchange_name, routing_key, body, content_type, headers, "
                "created_at FROM {table} ORDER BY created_at, id"
            )
            if limit > 0:
                cursor.execute(self._sql(statement + " LIMIT ?"), (limit,))
            else:
                cursor.execute(self._sql(statement))
            return [_record_from(row) for row in cursor.fetchall()]

        waiting = await _off_loop(self._relay, connection, statements)
        return list(waiting)

    async def mark_published(
        self, entry_id: str, *, connection: DbConnection | None = None
    ) -> None:
        """Removes a record, once the broker has confirmed the message."""

        def statements(cursor: DbCursor) -> None:
            cursor.execute(self._sql("DELETE FROM {table} WHERE id = ?"), (entry_id,))

        await _off_loop(self._relay, connection, statements)

    async def count(self, *, connection: DbConnection | None = None) -> int:
        """How many records are waiting."""

        def statements(cursor: DbCursor) -> int:
            cursor.execute(self._sql("SELECT COUNT(*) FROM {table}"))
            row = cursor.fetchone()
            return int(row[0])

        return int(await _off_loop(self._relay, connection, statements))

    def __repr__(self) -> str:
        return f"SqlOutboxStore({self._sql.table})"


def _record_from(row: Sequence[Any]) -> OutboxRecord:
    headers: Mapping[str, Any] = json.loads(row[5])
    return OutboxRecord(
        id=str(row[0]),
        exchange=str(row[1]),
        routing_key=str(row[2]),
        body=bytes(row[3]),
        content_type=str(row[4]),
        headers=headers,
        created_at=_from_ms(int(row[6])),
    )


class SqlSchemaRegistry:
    """Message shapes in a table every service can read.

    What :class:`~acemq_amqp.patterns.InMemorySchemaRegistry` is not: a consumer
    that meets a version it has never seen looks it up and finds what the
    producer registered, in another process, on another host, a fortnight ago.

    Registration is idempotent on the fingerprint, enforced by a unique index
    rather than by a check-then-insert. A service that registers its schemas on
    every start would otherwise add a version per restart and the version number
    would stop meaning anything; and two instances starting together would race.
    The loser of that race re-reads instead of writing a second id for the same
    bytes.

    Identifiers come from a one-row counter table rather than from ``MAX(id) +
    1``. A sequence is spelled differently on every database this has to run on;
    the arithmetic is simply wrong under load, because two writers registering
    two different schemas compute the same next id and the loser cannot win by
    retrying. Updating the counter row takes a row lock, so writers queue for an
    instant and every one of them gets a number. Registration happens once per
    schema in the lifetime of a system, so serialising it costs nothing.

    :param connections: a callable returning a connection this registry may
        commit and close
    :param table: where the schemas go; the counter is the same name with
        ``_seq`` on the end
    :param paramstyle: ``"qmark"`` for :mod:`sqlite3`, ``"format"`` for psycopg
    """

    #: How many times registration re-reads after losing a race before giving
    #: up. Losing twice is already unusual; losing five times means something
    #: other than contention is wrong, and looping forever would hide it.
    ATTEMPTS = 5

    def __init__(
        self,
        connections: Connections | None = None,
        *,
        table: str = DEFAULT_REGISTRY_TABLE,
        paramstyle: str = "qmark",
    ) -> None:
        self._sql = _Sql(table, paramstyle)
        self._work = _Work(connections)

    @property
    def table(self) -> str:
        """Where the schemas are."""
        return self._sql.table

    async def register(
        self,
        subject: str,
        format: str,
        definition: str,
        *,
        connection: DbConnection | None = None,
    ) -> SchemaDefinition:
        """Records a schema and returns it with an identifier.

        The same definition under the same subject comes back with the same
        identifier however many times it is offered, and from however many
        instances at once.
        """
        if not subject or not definition:
            raise ValueError("acemq: a schema needs a subject and a definition")
        print_of = fingerprint(definition)

        def statements(cursor: DbCursor) -> SchemaDefinition:
            already = self._find(cursor, subject, print_of)
            if already is not None:
                return already
            # Not there a moment ago. Take a number, write the row, and let the
            # unique index decide whether somebody else got there first — the
            # read and the insert are not one statement and cannot be, so the
            # index is what makes the answer correct rather than merely usual.
            return self._insert(cursor, subject, format, definition, print_of)

        for attempt in range(self.ATTEMPTS):
            try:
                written = await _off_loop(self._work, connection, statements)
            except Exception:
                # Somebody registered the same schema between the read and the
                # write and the unique index refused ours. Their row is the
                # answer, so go round and read it rather than reporting a
                # failure for something that succeeded.
                if attempt == self.ATTEMPTS - 1:
                    raise
                continue
            schema: SchemaDefinition = written
            return schema

        raise AceMQError(  # pragma: no cover — the loop returns or raises
            f"acemq: could not register {subject!r} in {self._sql.table!r} after "
            f"{self.ATTEMPTS} attempts."
        )

    async def by_id(
        self, identifier: int, *, connection: DbConnection | None = None
    ) -> SchemaDefinition:
        """Looks a schema up by the identifier a message carries."""

        def statements(cursor: DbCursor) -> SchemaDefinition | None:
            cursor.execute(
                self._sql(f"SELECT {_REGISTRY_COLUMNS} FROM {{table}} WHERE id = ?"),
                (identifier,),
            )
            row = cursor.fetchone()
            return None if row is None else _schema_from(row)

        found = await _off_loop(self._work, connection, statements)
        if found is None:
            raise SchemaNotFoundError(f"acemq: no schema with id {identifier}")
        schema: SchemaDefinition = found
        return schema

    async def latest(
        self, subject: str, *, connection: DbConnection | None = None
    ) -> SchemaDefinition:
        """The newest version of a subject."""

        def statements(cursor: DbCursor) -> SchemaDefinition | None:
            cursor.execute(
                self._sql(
                    f"SELECT {_REGISTRY_COLUMNS} FROM {{table}} WHERE subject = ? "
                    "ORDER BY version DESC LIMIT 1"
                ),
                (subject,),
            )
            row = cursor.fetchone()
            return None if row is None else _schema_from(row)

        found = await _off_loop(self._work, connection, statements)
        if found is None:
            raise SchemaNotFoundError(f"acemq: no schema for subject {subject!r}")
        schema: SchemaDefinition = found
        return schema

    async def versions(
        self, subject: str, *, connection: DbConnection | None = None
    ) -> Sequence[SchemaDefinition]:
        """Every version of a subject, oldest first."""

        def statements(cursor: DbCursor) -> list[SchemaDefinition]:
            cursor.execute(
                self._sql(
                    f"SELECT {_REGISTRY_COLUMNS} FROM {{table}} WHERE subject = ? "
                    "ORDER BY version"
                ),
                (subject,),
            )
            return [_schema_from(row) for row in cursor.fetchall()]

        return list(await _off_loop(self._work, connection, statements))

    def _find(self, cursor: DbCursor, subject: str, print_of: str) -> SchemaDefinition | None:
        cursor.execute(
            self._sql(
                f"SELECT {_REGISTRY_COLUMNS} FROM {{table}} "
                "WHERE subject = ? AND fingerprint = ?"
            ),
            (subject, print_of),
        )
        row = cursor.fetchone()
        return None if row is None else _schema_from(row)

    def _insert(
        self,
        cursor: DbCursor,
        subject: str,
        format: str,
        definition: str,
        print_of: str,
    ) -> SchemaDefinition:
        cursor.execute(self._sql("UPDATE {table}_seq SET last_id = last_id + 1"))
        if cursor.rowcount < 1:
            # Nobody seeded the counter. Doing it here rather than at
            # construction keeps the constructor from touching the database at
            # all, which is what lets a store be built before its schema exists.
            cursor.execute(
                self._sql("INSERT INTO {table}_seq (only_row, last_id) VALUES (1, 1)")
            )
            identifier = 1
        else:
            cursor.execute(self._sql("SELECT last_id FROM {table}_seq WHERE only_row = 1"))
            identifier = int(cursor.fetchone()[0])

        cursor.execute(
            self._sql("SELECT COUNT(*) FROM {table} WHERE subject = ?"), (subject,)
        )
        version = int(cursor.fetchone()[0]) + 1
        registered = _now_ms()

        cursor.execute(
            self._sql(
                "INSERT INTO {table} "
                "(id, subject, version, format, definition, fingerprint, registered_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            (identifier, subject, version, format, definition, print_of, registered),
        )
        return SchemaDefinition(
            id=identifier,
            subject=subject,
            version=version,
            format=format,
            definition=definition,
            fingerprint=print_of,
            registered_at=_from_ms(registered),
        )

    def __repr__(self) -> str:
        return f"SqlSchemaRegistry({self._sql.table})"


_REGISTRY_COLUMNS = "id, subject, version, format, definition, fingerprint, registered_at"


def _schema_from(row: Sequence[Any]) -> SchemaDefinition:
    return SchemaDefinition(
        id=int(row[0]),
        subject=str(row[1]),
        version=int(row[2]),
        format=str(row[3]),
        definition=str(row[4]),
        fingerprint=str(row[5]),
        registered_at=_from_ms(int(row[6])),
    )


def _ms(span: timedelta) -> int:
    return int(span.total_seconds() * 1000)
