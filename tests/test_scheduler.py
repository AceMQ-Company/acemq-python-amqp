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

"""Delivering a message later, and the queues that is a contract about.

The topology assertions here are deliberately literal. Every name and every
argument is shared with the Java library, and the way a difference shows up on a
broker both of them use is a ``PRECONDITION_FAILED`` on whichever service
started second — so it is worth spelling out rather than deriving.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fake_transport import FakeTransport, Sent

from acemq_amqp import Connection, Envelope, headers
from acemq_amqp.patterns import (
    DEFAULT_SCHEDULE_PREFETCH,
    HEADER_SCHEDULE_CONTENT_TYPE,
    HEADER_SCHEDULE_DUE_AT,
    HEADER_SCHEDULE_EXCHANGE,
    HEADER_SCHEDULE_ROUTING_KEY,
    SCHEDULE_DUE,
    SCHEDULE_EXCHANGE,
    SCHEDULE_RUNGS,
    Scheduler,
    schedule_rung_args,
    schedule_rung_name,
    schedule_topology,
)
from acemq_amqp.topology import QUEUE_TYPE_ARG

#: The rung queues, longest first, exactly as Java names them.
RUNG_QUEUES = [
    "acemq.schedule.1h",
    "acemq.schedule.10m",
    "acemq.schedule.1m",
    "acemq.schedule.10s",
    "acemq.schedule.1s",
]


def now() -> datetime:
    return datetime.now(timezone.utc)


def published_to(transport: FakeTransport, exchange: str, routing_key: str) -> list[Sent]:
    return [
        sent
        for sent in transport.sent
        if sent.exchange == exchange and sent.routing_key == routing_key
    ]


async def running(transport: FakeTransport) -> tuple[Connection, Scheduler]:
    mq = Connection(transport)
    return mq, await Scheduler.open(mq)


def scheduled_headers(
    due_at: Any,
    *,
    exchange: Any = "billing",
    routing_key: Any = "invoice.due",
    content_type: Any = "application/json",
) -> dict[str, Any]:
    """The headers a message coming out of a rung arrives with."""
    application: dict[str, Any] = {
        HEADER_SCHEDULE_EXCHANGE: exchange,
        HEADER_SCHEDULE_ROUTING_KEY: routing_key,
        HEADER_SCHEDULE_DUE_AT: due_at,
    }
    if content_type is not None:
        application[HEADER_SCHEDULE_CONTENT_TYPE] = content_type
    return Envelope(headers=application).to_headers(routing_key=SCHEDULE_DUE)


# --- the topology, which is the contract ------------------------------------


def test_the_rungs_are_five_and_longest_first() -> None:
    assert list(SCHEDULE_RUNGS) == [
        timedelta(hours=1),
        timedelta(minutes=10),
        timedelta(minutes=1),
        timedelta(seconds=10),
        timedelta(seconds=1),
    ]


def test_a_rung_queue_is_named_for_its_duration_the_way_java_spells_it() -> None:
    assert [schedule_rung_name(rung) for rung in SCHEDULE_RUNGS] == RUNG_QUEUES
    # Whole hours before whole minutes before seconds, which is the order Java
    # tries them in and therefore the only order that produces the same names.
    assert schedule_rung_name(timedelta(hours=2)) == "acemq.schedule.2h"
    assert schedule_rung_name(timedelta(minutes=90)) == "acemq.schedule.90m"
    assert schedule_rung_name(timedelta(seconds=90)) == "acemq.schedule.90s"


def test_a_rung_is_declared_with_exactly_three_arguments() -> None:
    assert schedule_rung_args(timedelta(hours=1)) == {
        "x-message-ttl": 3_600_000,
        "x-dead-letter-exchange": "acemq.schedule",
        "x-dead-letter-routing-key": "acemq.schedule.due",
    }
    assert schedule_rung_args(timedelta(seconds=1))["x-message-ttl"] == 1_000


def test_the_topology_is_what_java_declares() -> None:
    topology = schedule_topology()

    assert topology.exchanges == [SCHEDULE_EXCHANGE]
    assert topology.queues == [*RUNG_QUEUES, SCHEDULE_DUE]
    assert [
        (binding.queue, binding.exchange, binding.routing_key)
        for binding in topology.bindings
    ] == [(name, SCHEDULE_EXCHANGE, name) for name in [*RUNG_QUEUES, SCHEDULE_DUE]]
    topology.validate()


async def test_every_scheduler_queue_is_classic_and_the_control_queue_has_no_arguments() -> (
    None
):
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        assert transport.exchanges[SCHEDULE_EXCHANGE].kind == "direct"
        assert transport.exchanges[SCHEDULE_EXCHANGE].durable
        for name, rung in zip(RUNG_QUEUES, SCHEDULE_RUNGS, strict=True):
            spec = transport.queues[name]
            # Classic is spelled by leaving x-queue-type off entirely, which is
            # what Java sends. Sending "classic" would be a different argument
            # table and a broker would refuse one of the two declarations.
            assert QUEUE_TYPE_ARG not in spec.args
            assert dict(spec.args) == schedule_rung_args(rung)
            assert spec.durable
        assert dict(transport.queues[SCHEDULE_DUE].args) == {}
        assert transport.queues[SCHEDULE_DUE].durable
    finally:
        await scheduler.close()
        await mq.close()


async def test_the_control_consumer_declares_no_dead_letter_queues_of_its_own() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        # The whole reason the consumer is opened with declare=False. Without
        # it every service running a scheduler would leak two durable queues
        # that nothing publishes to and nobody reads.
        assert "acemq.schedule.due.dlq" not in transport.queues
        assert "acemq.schedule.due.parked" not in transport.queues
        assert "acemq.dlx" not in transport.exchanges
        assert set(transport.queues) == {*RUNG_QUEUES, SCHEDULE_DUE}
    finally:
        await scheduler.close()
        await mq.close()


async def test_the_control_consumer_holds_fifty_at_a_time_like_java() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        assert DEFAULT_SCHEDULE_PREFETCH == 50
        assert transport.specs[SCHEDULE_DUE].prefetch == 50
    finally:
        await scheduler.close()
        await mq.close()


# --- scheduling -------------------------------------------------------------


async def test_a_delay_goes_into_the_largest_rung_that_does_not_overshoot() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        await scheduler.after(timedelta(hours=4), "billing", "invoice.due", {"n": 1})
        await scheduler.after(timedelta(minutes=90), "billing", "invoice.due", {"n": 2})
        await scheduler.after(timedelta(seconds=45), "billing", "invoice.due", {"n": 3})
        await scheduler.after(timedelta(seconds=3), "billing", "invoice.due", {"n": 4})
    finally:
        await scheduler.close()
        await mq.close()

    landed = [
        (sent.routing_key, json.loads(sent.message.body)["n"])
        for sent in transport.sent
        if sent.exchange == SCHEDULE_EXCHANGE
    ]
    assert landed == [
        ("acemq.schedule.1h", 1),
        ("acemq.schedule.1h", 2),
        ("acemq.schedule.10s", 3),
        ("acemq.schedule.1s", 4),
    ]
    assert scheduler.scheduled == 4
    assert scheduler.hops == 4
    assert scheduler.delivered == 0


async def test_a_message_that_is_already_due_is_delivered_rather_than_queued() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        await scheduler.after(timedelta(0), "billing", "invoice.due", {"n": 1})
        await scheduler.after(timedelta(minutes=-5), "billing", "invoice.due", {"n": 2})
        # Under the smallest rung, so another hop would cost more than the
        # accuracy it buys.
        await scheduler.after(timedelta(milliseconds=200), "billing", "invoice.due", {"n": 3})
    finally:
        await scheduler.close()
        await mq.close()

    delivered = published_to(transport, "billing", "invoice.due")
    assert len(delivered) == 3
    assert scheduler.delivered == 3
    assert scheduler.hops == 0


async def test_the_headers_are_the_four_java_writes_and_due_at_is_epoch_millis() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    when = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
    try:
        await scheduler.at(when + timedelta(0), "billing", "invoice.due", {"sku": "A-1"})
        await scheduler.after(timedelta(hours=2), "billing", "invoice.due", {"sku": "A-1"})
    finally:
        await scheduler.close()
        await mq.close()

    hop = published_to(transport, SCHEDULE_EXCHANGE, "acemq.schedule.1h")[0]
    assert hop.headers[HEADER_SCHEDULE_EXCHANGE] == "billing"
    assert hop.headers[HEADER_SCHEDULE_ROUTING_KEY] == "invoice.due"
    assert hop.headers[HEADER_SCHEDULE_CONTENT_TYPE] == mq.codec.content_type
    # An integer of epoch milliseconds, the same encoding as
    # x-acemq-first-seen, and what Java's Instant.toEpochMilli() produces.
    due_at = hop.headers[HEADER_SCHEDULE_DUE_AT]
    assert isinstance(due_at, int)
    assert not isinstance(due_at, bool)
    # None of them carries the reserved prefix, which the envelope would refuse.
    for name in (
        HEADER_SCHEDULE_EXCHANGE,
        HEADER_SCHEDULE_ROUTING_KEY,
        HEADER_SCHEDULE_DUE_AT,
        HEADER_SCHEDULE_CONTENT_TYPE,
    ):
        assert not headers.is_reserved(name)


async def test_due_at_is_the_moment_that_was_asked_for() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    when = datetime(2027, 3, 1, 9, 0, tzinfo=timezone.utc)
    try:
        await scheduler.at(when, "policies", "policy.renew", {"id": 7})
    finally:
        await scheduler.close()
        await mq.close()

    hop = published_to(transport, SCHEDULE_EXCHANGE, "acemq.schedule.1h")[0]
    assert hop.headers[HEADER_SCHEDULE_DUE_AT] == int(when.timestamp() * 1000)


async def test_a_naive_datetime_is_refused_rather_than_read_as_local_time() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        with pytest.raises(ValueError, match="no time zone"):
            await scheduler.at(datetime(2027, 3, 1, 9, 0), "policies", "renew", {})
    finally:
        await scheduler.close()
        await mq.close()


# --- what comes back out of a rung ------------------------------------------


async def test_a_message_still_short_of_due_goes_down_a_rung_carrying_its_headers() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    due = int((now() + timedelta(minutes=25)).timestamp() * 1000)
    try:
        settlement = await transport.deliver(
            SCHEDULE_DUE,
            b'{"sku": "A-1"}',
            headers=scheduled_headers(due),
            content_type="application/octet-stream",
        )
    finally:
        await scheduler.close()
        await mq.close()

    assert settlement.acked
    hop = published_to(transport, SCHEDULE_EXCHANGE, "acemq.schedule.10m")[0]
    assert hop.message.body == b'{"sku": "A-1"}'
    # The bookkeeping travels on unchanged, including the due time: the next
    # hop works out what is left from it rather than from a fresh clock.
    assert hop.headers[HEADER_SCHEDULE_DUE_AT] == due
    assert hop.headers[HEADER_SCHEDULE_EXCHANGE] == "billing"
    assert hop.headers[HEADER_SCHEDULE_ROUTING_KEY] == "invoice.due"
    assert hop.headers[HEADER_SCHEDULE_CONTENT_TYPE] == "application/json"
    assert scheduler.hops == 1
    assert scheduler.delivered == 0


async def test_a_due_message_reaches_its_target_with_its_bytes_and_content_type() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    body = b"\xac\x01\x00not json at all"
    try:
        await transport.deliver(
            SCHEDULE_DUE,
            body,
            headers=scheduled_headers(
                int(now().timestamp() * 1000) - 5_000,
                exchange="documents",
                routing_key="document.ready",
                content_type="application/vnd.acme+protobuf",
            ),
        )
    finally:
        await scheduler.close()
        await mq.close()

    delivered = published_to(transport, "documents", "document.ready")[0]
    # Byte for byte. Re-encoding here is how a scheduler turns JSON into JSON
    # containing JSON.
    assert delivered.message.body == body
    # And under the content type it was scheduled as, not the octet-stream the
    # scheduler moved it with — the consumer picks its codec from this.
    assert delivered.message.content_type == "application/vnd.acme+protobuf"
    assert scheduler.delivered == 1


async def test_the_schedulers_own_headers_are_not_passed_on_to_the_destination() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        await transport.deliver(
            SCHEDULE_DUE,
            b"{}",
            headers=scheduled_headers(int(now().timestamp() * 1000) - 1),
        )
    finally:
        await scheduler.close()
        await mq.close()

    delivered = published_to(transport, "billing", "invoice.due")[0]
    for name in (
        HEADER_SCHEDULE_EXCHANGE,
        HEADER_SCHEDULE_ROUTING_KEY,
        HEADER_SCHEDULE_DUE_AT,
        HEADER_SCHEDULE_CONTENT_TYPE,
    ):
        assert name not in delivered.headers
    assert delivered.headers[headers.TYPE] == "ScheduledMessage"


async def test_a_message_with_no_content_type_recorded_is_delivered_as_json() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        await transport.deliver(
            SCHEDULE_DUE,
            b"{}",
            headers=scheduled_headers(int(now().timestamp() * 1000) - 1, content_type=None),
        )
    finally:
        await scheduler.close()
        await mq.close()

    assert published_to(transport, "billing", "invoice.due")[0].message.content_type == (
        "application/json"
    )


async def test_headers_written_as_bytes_by_another_client_are_read_the_same() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        await transport.deliver(
            SCHEDULE_DUE,
            b"{}",
            headers=scheduled_headers(
                str(int(now().timestamp() * 1000) - 1).encode(),
                exchange=b"billing",
                routing_key=b"invoice.due",
                content_type=b"text/plain",
            ),
        )
    finally:
        await scheduler.close()
        await mq.close()

    delivered = published_to(transport, "billing", "invoice.due")[0]
    assert delivered.message.content_type == "text/plain"


async def test_a_message_without_the_headers_is_rejected_rather_than_retried() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        settlement = await transport.deliver(SCHEDULE_DUE, b"{}", headers={})
    finally:
        await scheduler.close()
        await mq.close()

    # Nothing went to a rung and nothing went to a destination: a message the
    # scheduler cannot read is not a message it guesses a destination for. What
    # it tried instead was its own dead-letter queue, which nothing declared —
    # so the broker refused it and the message was given back rather than
    # quietly creating a queue nobody asked for.
    assert [sent.routing_key for sent in transport.sent] == ["acemq.schedule.due.dlq"]
    assert "acemq.schedule.due.dlq" not in transport.queues
    assert settlement.nacked
    assert settlement.requeued is False


async def test_an_unreadable_due_time_is_rejected_rather_than_delivered_now() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        settlement = await transport.deliver(
            SCHEDULE_DUE, b"{}", headers=scheduled_headers("soon")
        )
    finally:
        await scheduler.close()
        await mq.close()

    # Guessing early delivers it now and guessing late holds it forever, so it
    # does neither.
    assert published_to(transport, "billing", "invoice.due") == []
    assert published_to(transport, SCHEDULE_EXCHANGE, "acemq.schedule.1s") == []
    assert settlement.nacked


async def test_a_scheduler_reads_as_a_line_in_a_log() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        await scheduler.after(timedelta(hours=1), "billing", "invoice.due", {})
        assert str(scheduler) == (
            "Scheduler(rungs=['1h', '10m', '1m', '10s', '1s'], "
            "scheduled=1, delivered=0, hops=1)"
        )
    finally:
        await scheduler.close()
        await mq.close()


async def test_closing_a_scheduler_stops_the_consumer_and_deletes_nothing() -> None:
    transport = FakeTransport()
    mq, scheduler = await running(transport)
    try:
        async with scheduler:
            assert SCHEDULE_DUE in transport.consumers
        assert SCHEDULE_DUE not in transport.consumers
        # The rungs are shared with every other scheduler on the broker.
        assert set(transport.queues) == {*RUNG_QUEUES, SCHEDULE_DUE}
    finally:
        await mq.close()
