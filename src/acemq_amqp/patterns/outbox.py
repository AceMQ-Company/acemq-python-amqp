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

"""Writing to a database and publishing a message, without a gap between them.

A service that commits a row and then publishes has two things that fail
independently. Commit and then crash, and the message never goes: the order
exists and nothing downstream knows. Publish and then fail to commit, and the
message is about an order that does not exist. No amount of care with the
ordering removes the gap, because there are two systems and no transaction
spanning them.

The outbox removes it by having only one system. The message is written into the
same transaction as the work, so the record and the row commit together or
neither does, and a relay publishes what was committed. The cost is that the
relay is at-least-once by construction — a record is removed only after the
broker has confirmed it, so a crash in between sends it twice — which is what
:mod:`acemq_amqp.patterns.idempotency` is for at the other end.

The thing to watch is ``acemq.outbox.lag``. A committed, unpublished row is a
message that exists and is owed to somebody and appears in no queue depth
anywhere, so a relay that has stopped looks exactly like a service with nothing
to send — until this is measured. :class:`OutboxRelay` reports it through the
connection's observer, beside ``acemq.outbox.total``, and measures it from the
record's commit rather than from the sweep that picked it up.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

from ..codec import Codec
from ..connection import Connection
from ..envelope import Envelope
from ..telemetry import (
    METRIC_OUTBOX_LAG,
    METRIC_OUTBOX_TOTAL,
    OUTCOME_FAILED,
    OUTCOME_PUBLISHED,
    TAG_EXCHANGE,
    TAG_OUTCOME,
    TAG_ROUTING_KEY,
)
from ..transport import Outbound

log = logging.getLogger("acemq")

#: How often the relay sweeps unless it is told otherwise.
DEFAULT_INTERVAL = timedelta(seconds=1)

#: How many records one sweep publishes unless it is told otherwise.
DEFAULT_BATCH = 100


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """A message that has been decided but not yet published.

    :param id: the message identifier, and what stops the relay publishing the
        same record twice
    :param exchange: where it is going, empty for the default exchange
    :param routing_key: what it is published under
    :param body: the already-encoded payload. Bytes rather than a value, because
        the record outlives the process that wrote it and the class it was
        written from may not survive the next deployment
    :param content_type: what the body was encoded as
    :param headers: the envelope, rendered
    :param created_at: when it was written, which is the order the relay
        publishes in
    """

    id: str
    exchange: str
    routing_key: str
    body: bytes
    content_type: str = ""
    headers: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@runtime_checkable
class OutboxStore(Protocol):
    """Holds messages that have been decided but not yet published.

    An implementation is only worth having if :meth:`add` can join the caller's
    transaction. A store that opens a connection of its own has the gap back, and
    has bought nothing but a second place for messages to go missing.
    """

    async def add(self, entry: OutboxRecord) -> None:
        """Records a message to be published.

        Adding the same identifier twice is not an error — a caller may be
        retrying its own transaction — but it must not become two messages.
        """

    async def pending(self, limit: int) -> Sequence[OutboxRecord]:
        """Records waiting to be published, oldest first.

        :param limit: at most this many, or zero for all of them
        """

    async def mark_published(self, entry_id: str) -> None:
        """Removes a record, once the broker has confirmed the message."""


class InMemoryOutboxStore:
    """An outbox in this process.

    It has none of the property the pattern exists for: nothing here shares a
    transaction with a database, so a crash between the work committing and the
    record being written loses the message exactly as publishing directly would.
    It is for tests, and for seeing the shape of the thing before writing the
    version that matters.
    """

    def __init__(self) -> None:
        self._records: dict[str, OutboxRecord] = {}
        # A plain lock, as in the idempotency store: it is held for a dictionary
        # operation and never across an await, so a store written to from the
        # blocking API's worker threads is safe too.
        self._lock = threading.Lock()

    async def add(self, entry: OutboxRecord) -> None:
        if not entry.id:
            raise ValueError("acemq: an outbox record needs an id")
        with self._lock:
            # Kept rather than replaced. A caller retrying its own transaction is
            # the ordinary case, and the second write is the same message.
            self._records.setdefault(entry.id, entry)

    async def pending(self, limit: int) -> Sequence[OutboxRecord]:
        with self._lock:
            waiting = sorted(self._records.values(), key=lambda entry: entry.created_at)
        return waiting[:limit] if limit > 0 else waiting

    async def mark_published(self, entry_id: str) -> None:
        with self._lock:
            self._records.pop(entry_id, None)

    def __len__(self) -> int:
        """How many records are waiting."""
        with self._lock:
            return len(self._records)


def record(
    connection: Connection,
    exchange: str,
    routing_key: str,
    payload: Any,
    *,
    envelope: Envelope | None = None,
    codec: Codec | None = None,
) -> OutboxRecord:
    """Encodes a payload into a record, ready for :meth:`OutboxStore.add`.

    Call it inside the transaction that does the work::

        async with database.transaction() as tx:
            await place_order(tx, order)
            await store.add(record(mq, "orders-events", "order.placed", event))

    Encoding here rather than in the relay is the point of the two steps. The
    record has to survive a deployment that changes the class the payload was
    written from, so what is stored is bytes and a content type; the relay is
    then a thing that moves bytes and needs to know nothing about what they mean.

    :param connection: whose codec and origin the message takes
    :param exchange: where it is going, empty for the default exchange
    :param routing_key: what to publish it under
    :param payload: what to send
    :param envelope: metadata to use instead of a fresh one
    :param codec: a codec other than the connection's
    :returns: the record
    """
    writer = codec or connection.codec
    outgoing = envelope or Envelope(origin=connection.origin)
    return OutboxRecord(
        id=outgoing.id,
        exchange=exchange,
        routing_key=routing_key,
        body=writer.encode(payload),
        content_type=writer.content_type,
        headers=outgoing.to_headers(routing_key=routing_key),
    )


def _lag_seconds(committed: datetime) -> float:
    """How long ago a record was committed, in seconds and never negative.

    A record's ``created_at`` is written by whoever wrote the record, so it can
    be naive — the dataclass default and the SQL store are both UTC-aware, but
    nothing stops a caller passing a bare :class:`~datetime.datetime`. A naive
    one is read as UTC rather than raising, because a relay is not the place to
    discover that a timestamp was built without a zone.

    Clamped at zero. Clock skew between the process that wrote the row and the
    one sweeping it can put the commit in the future, and a negative lag is a
    number no histogram can hold and no dashboard can read.
    """
    if committed.tzinfo is None:
        committed = committed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - committed).total_seconds())


class OutboxRelay:
    """Publishes what the outbox holds, and removes what the broker confirmed.

    Deliberately at-least-once. A record is removed only after the publish has
    returned, so a crash in between sends it a second time; removing it first
    would lose it instead, and a repeated message is a problem a consumer can
    solve while a lost one is not.

    Every record it handles is counted on ``acemq.outbox.total`` and every
    record it publishes is timed on ``acemq.outbox.lag``, both through the
    connection's observer — see :meth:`sweep` for why those are the only two
    signals it can raise, and :meth:`sweep` again for what an application can
    add on top by calling it inside a span of its own.

    :param connection: where to publish
    :param store: what to publish from
    :param interval: how often to sweep
    :param batch: how many records one sweep publishes. A bound rather than a
        tuning knob: without one, a relay starting up against a backlog holds
        every waiting record in memory at once
    """

    def __init__(
        self,
        connection: Connection,
        store: OutboxStore,
        *,
        interval: timedelta = DEFAULT_INTERVAL,
        batch: int = DEFAULT_BATCH,
    ) -> None:
        if interval <= timedelta(0):
            raise ValueError(f"acemq: a relay interval must be positive, got {interval}")
        if batch < 1:
            raise ValueError(f"acemq: a relay batch must be at least 1, got {batch}")
        self._connection = connection
        self._store = store
        self._interval = interval
        self._batch = batch
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Sweeps the outbox until :meth:`close`.

        Starting twice is a no-op rather than a second sweeper: two relays on one
        store publish everything twice, and the shape of that mistake is a
        service that starts one relay per worker.
        """
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="acemq-outbox-relay")

    async def sweep(self) -> int:
        """Publishes one batch and says how many went out.

        Public so an application can flush the outbox on demand — at the end of
        a request, say, rather than up to a second later — and so a test can
        drive the relay without waiting for a tick.

        **What it reports, and what it cannot.** Every record is counted through
        the connection's :class:`~acemq_amqp.telemetry.Observer` on
        ``acemq.outbox.total``, published or failed, and a published one records
        how long it waited on ``acemq.outbox.lag``. That is the whole of the
        relay's telemetry, and deliberately so: the tracing adapter has an
        ``outbox.publish_failed`` event and an ``outbox_lag_ms`` attribute, and
        neither can be written from here. A sweep started by :meth:`start` runs
        on a task of its own with no span current, and
        :meth:`~acemq_amqp.connection.Connection.publish_raw` is beneath the
        interceptor chain and so opens no ``publish`` span either — so an
        attribute written here would land on nothing. Metrics need no span,
        which is why the numbers go out this way and the trace side stays where
        an application can reach it: call ``sweep()`` from inside a span you are
        holding and the tracing adapter's methods work as documented.

        :returns: how many records were published
        :raises Exception: whatever the store or the broker raised. The records
            are still in the outbox, which is the entire point: a relay that
            fails loses nothing
        """
        waiting = await self._store.pending(self._batch)
        observer = self._connection.observer

        published = 0
        for entry in waiting:
            labels = {
                TAG_EXCHANGE: entry.exchange,
                TAG_ROUTING_KEY: entry.routing_key,
            }
            try:
                await self._connection.publish_raw(
                    entry.exchange,
                    entry.routing_key,
                    Outbound(
                        body=entry.body,
                        content_type=entry.content_type,
                        message_id=entry.id,
                        headers=entry.headers,
                        persistent=True,
                    ),
                )
            except Exception:
                # Counted and re-raised. The record is still in the outbox, and
                # stopping here rather than carrying on to the next one keeps
                # the order the records were written in, which is what the
                # writer intended by writing them in that order.
                observer.count(
                    METRIC_OUTBOX_TOTAL, 1, {**labels, TAG_OUTCOME: OUTCOME_FAILED}
                )
                raise

            # Counted here rather than after the record has been removed,
            # because the message is at the broker either way: a store that
            # cannot mark it published sends it a second time, and a counter
            # that skipped it would report fewer messages than went out.
            #
            # The lag is measured from when the record was committed rather than
            # from when this sweep started. A relay that has been down for an
            # hour is publishing hour-old messages, and timing from the sweep
            # would report the same handful of milliseconds as one keeping up.
            observer.count(
                METRIC_OUTBOX_TOTAL, 1, {**labels, TAG_OUTCOME: OUTCOME_PUBLISHED}
            )
            observer.observe(
                METRIC_OUTBOX_LAG,
                _lag_seconds(entry.created_at),
                {**labels, TAG_OUTCOME: OUTCOME_PUBLISHED},
            )

            await self._store.mark_published(entry.id)
            published += 1
        return published

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._stopping.wait(), self._interval.total_seconds())
            except asyncio.TimeoutError:
                # The ordinary case: nobody asked it to stop, so it is time to
                # sweep. Waiting on the event rather than sleeping is what makes
                # close() return in the time it takes to stop rather than in the
                # time left on the tick.
                pass
            else:
                return
            try:
                await self.sweep()
            except Exception:
                # Not fatal, and not even unusual: the broker being down is one
                # of the things an outbox exists to survive. The records are
                # still there and the next tick tries again.
                log.exception(
                    "acemq: a sweep of the outbox failed; every record is still in it"
                )

    async def close(self) -> None:
        """Stops the relay and waits for the sweep in progress.

        Waited for rather than cancelled, because a sweep interrupted between
        publishing a record and marking it published is exactly the case that
        sends a message twice — and it costs nothing to not do it on the way out.
        """
        self._stopping.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def __aenter__(self) -> OutboxRelay:
        self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
