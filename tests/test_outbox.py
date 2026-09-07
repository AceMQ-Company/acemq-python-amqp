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

"""Publishing what was committed, and nothing that was not."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone

import pytest
from fake_transport import FakeTransport

from acemq_amqp import Connection, Envelope, Outbound, PublishResult, headers
from acemq_amqp.patterns import (
    InMemoryOutboxStore,
    OutboxRecord,
    OutboxRelay,
    record,
)


def connection() -> tuple[Connection, FakeTransport]:
    transport = FakeTransport()
    return Connection(transport, origin="checkout@pod-7"), transport


async def test_a_record_carries_the_bytes_and_the_envelope_not_the_object() -> None:
    mq, _ = connection()

    entry = record(mq, "orders-events", "order.placed", {"id": "7"})

    # Bytes, because the record outlives the process that wrote it and the class
    # it came from may not survive the next deployment.
    assert entry.body == b'{"id": "7"}'
    assert entry.content_type == "application/json"
    assert entry.exchange == "orders-events"
    assert entry.routing_key == "order.placed"
    assert entry.headers[headers.TYPE] == "order.placed"
    assert entry.headers[headers.ORIGIN] == "checkout@pod-7"
    assert entry.id == entry.headers[headers.ID]


async def test_a_record_can_carry_an_envelope_the_caller_already_has() -> None:
    mq, _ = connection()
    envelope = Envelope(id="order-1", causation_id="cart-9")

    entry = record(mq, "", "orders", {"id": "7"}, envelope=envelope)

    assert entry.id == "order-1"
    assert entry.headers[headers.CAUSATION] == "cart-9"


async def test_a_sweep_publishes_what_is_waiting_and_forgets_it() -> None:
    mq, transport = connection()
    store = InMemoryOutboxStore()
    await store.add(record(mq, "", "orders", {"id": "7"}))

    published = await OutboxRelay(mq, store).sweep()

    assert published == 1
    assert len(store) == 0
    sent = transport.sent_to("orders")
    assert len(sent) == 1
    assert sent[0].message.body == b'{"id": "7"}'
    assert sent[0].message.persistent is True


async def test_records_go_out_in_the_order_they_were_written() -> None:
    mq, transport = connection()
    store = InMemoryOutboxStore()
    early = datetime.now(timezone.utc) - timedelta(minutes=5)

    await store.add(
        OutboxRecord(id="second", exchange="", routing_key="orders", body=b"2")
    )
    await store.add(
        OutboxRecord(
            id="first", exchange="", routing_key="orders", body=b"1", created_at=early
        )
    )

    await OutboxRelay(mq, store).sweep()

    assert [sent.message.body for sent in transport.sent_to("orders")] == [b"1", b"2"]


async def test_the_same_record_added_twice_is_still_one_message() -> None:
    # A caller retrying its own transaction is the ordinary case, and the second
    # write is the same message rather than a second one.
    mq, transport = connection()
    store = InMemoryOutboxStore()
    entry = record(mq, "", "orders", {"id": "7"})

    await store.add(entry)
    await store.add(entry)

    assert await OutboxRelay(mq, store).sweep() == 1
    assert len(transport.sent_to("orders")) == 1


async def test_a_batch_bounds_what_one_sweep_holds() -> None:
    mq, _ = connection()
    store = InMemoryOutboxStore()
    for n in range(5):
        await store.add(record(mq, "", "orders", {"id": n}))

    relay = OutboxRelay(mq, store, batch=2)

    assert await relay.sweep() == 2
    assert len(store) == 3


async def test_a_record_the_broker_would_not_take_stays_in_the_outbox() -> None:
    # The point of the pattern: a relay that fails loses nothing, because
    # nothing is removed until the publish has returned.
    class Refusing(FakeTransport):
        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            raise ConnectionError("the broker is not answering")

    mq = Connection(Refusing())
    store = InMemoryOutboxStore()
    await store.add(record(mq, "", "orders", {"id": "7"}))

    with pytest.raises(ConnectionError):
        await OutboxRelay(mq, store).sweep()

    assert len(store) == 1


async def test_a_running_relay_sweeps_on_its_own_and_stops_when_closed() -> None:
    mq, transport = connection()
    store = InMemoryOutboxStore()

    async with OutboxRelay(mq, store, interval=timedelta(milliseconds=10)):
        await store.add(record(mq, "", "orders", {"id": "7"}))
        for _ in range(200):
            if len(store) == 0:
                break
            await asyncio.sleep(0.01)

    assert len(transport.sent_to("orders")) == 1

    # And nothing after it was closed.
    await store.add(record(mq, "", "orders", {"id": "8"}))
    await asyncio.sleep(0.05)
    assert len(transport.sent_to("orders")) == 1


async def test_a_failing_sweep_does_not_stop_the_relay() -> None:
    # The broker being down is one of the things an outbox exists to survive.
    mq, transport = connection()
    failures = 0

    class SometimesBroken(InMemoryOutboxStore):
        async def pending(self, limit: int) -> Sequence[OutboxRecord]:
            nonlocal failures
            if failures < 1:
                failures += 1
                raise ConnectionError("the store is not answering")
            return await super().pending(limit)

    store = SometimesBroken()
    await store.add(record(mq, "", "orders", {"id": "7"}))

    async with OutboxRelay(mq, store, interval=timedelta(milliseconds=10)):
        for _ in range(200):
            if len(store) == 0:
                break
            await asyncio.sleep(0.01)

    assert failures == 1
    assert len(transport.sent_to("orders")) == 1


async def test_a_relay_started_twice_is_still_one_relay() -> None:
    # Two relays on one store publish everything twice, and the shape of that
    # mistake is a service that starts one relay per worker.
    mq, transport = connection()
    store = InMemoryOutboxStore()
    relay = OutboxRelay(mq, store, interval=timedelta(milliseconds=10))

    relay.start()
    relay.start()
    await store.add(record(mq, "", "orders", {"id": "7"}))
    await asyncio.sleep(0.08)
    await relay.close()

    assert len(transport.sent_to("orders")) == 1


async def test_a_record_needs_an_identifier() -> None:
    store = InMemoryOutboxStore()
    with pytest.raises(ValueError, match="needs an id"):
        await store.add(OutboxRecord(id="", exchange="", routing_key="orders", body=b"{}"))
