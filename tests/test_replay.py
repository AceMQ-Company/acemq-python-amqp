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

"""Putting dead letters back, in stages, without losing any."""

from __future__ import annotations

from datetime import timedelta

import pytest
from fake_transport import FakeTransport

from acemq_amqp import Connection, Envelope, Outbound, PublishResult, headers
from acemq_amqp.patterns import (
    HEADER_REPLAY_COUNT,
    HEADER_REPLAYED_AT,
    HEADER_REPLAYED_FROM,
    ReplayError,
    replay,
)

DLQ = "orders.new.dlq"
QUEUE = "orders.new"


def stage(transport: FakeTransport, count: int, attempt: int = 1) -> None:
    for n in range(count):
        envelope = Envelope(
            id=f"order-{n}", error="the database timed out", attempt=attempt
        )
        transport.stage(
            DLQ,
            f'{{"id": {n}}}'.encode(),
            headers=envelope.to_headers(routing_key=QUEUE),
            routing_key=QUEUE,
        )


def dead(transport: FakeTransport, body: bytes, error: str) -> None:
    transport.stage(
        DLQ, body, headers=Envelope(error=error).to_headers(), routing_key=QUEUE
    )


async def test_a_replay_moves_everything_and_says_it_drained() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 3)

    result = await replay(mq, DLQ)

    assert (result.moved, result.skipped, result.reason) == (3, 0, "drained")
    assert str(result) == "moved 3, skipped 0 (drained)"
    # Back where they came from, because each message keeps its own routing key
    # unless one is given.
    assert len(transport.sent_to(QUEUE)) == 3
    assert transport.waiting[DLQ] == []


async def test_a_replayed_message_says_it_was_replayed() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 1)

    await replay(mq, DLQ)

    moved = transport.sent_to(QUEUE)[0]
    assert moved.headers[HEADER_REPLAYED_FROM] == DLQ
    assert moved.headers[HEADER_REPLAY_COUNT] == 1
    assert moved.headers[HEADER_REPLAYED_AT]
    assert moved.headers[headers.ID] == "order-0"
    assert moved.message.body == b'{"id": 0}'


async def test_replaying_twice_counts_twice() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    transport.stage(
        DLQ,
        b"{}",
        headers=Envelope(headers={HEADER_REPLAY_COUNT: 4}).to_headers(routing_key=QUEUE),
        routing_key=QUEUE,
    )

    await replay(mq, DLQ)

    assert transport.sent_to(QUEUE)[0].headers[HEADER_REPLAY_COUNT] == 5


async def test_a_replay_gives_a_message_its_attempts_back() -> None:
    # Otherwise a message that arrives back on attempt five of a five-attempt
    # policy is dead-lettered again before a handler sees it, and the replay has
    # moved two thousand messages from one queue to the same queue.
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 1, attempt=5)

    await replay(mq, DLQ)

    moved = transport.sent_to(QUEUE)[0]
    assert moved.headers[headers.ATTEMPT] == 1
    assert headers.ERROR not in moved.headers


async def test_a_replay_can_put_back_exactly_what_was_there() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 1, attempt=5)

    await replay(mq, DLQ, restart=False)

    moved = transport.sent_to(QUEUE)[0]
    assert moved.headers[headers.ATTEMPT] == 5
    assert moved.headers[headers.ERROR] == "the database timed out"


async def test_a_filter_leaves_what_it_declines_where_it_was() -> None:
    # And this is the whole trick: a declined message returned one at a time
    # goes back to the head of the queue, so the next read hands over the same
    # message and nothing behind it is ever looked at.
    transport = FakeTransport()
    mq = Connection(transport)
    dead(transport, b"1", "disk full")
    dead(transport, b"2", "timed out")
    dead(transport, b"3", "disk full")

    result = await replay(mq, DLQ, only=lambda envelope, body: "timed out" in envelope.error)

    assert (result.moved, result.skipped, result.reason) == (1, 2, "drained")
    assert [sent.message.body for sent in transport.sent_to(QUEUE)] == [b"2"]
    # The two it declined are back on the queue, not lost and not republished.
    assert len(transport.waiting[DLQ]) == 2


async def test_a_limit_stops_the_pass_and_says_so() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 10)

    result = await replay(mq, DLQ, limit=4)

    # "Moved 4" means something quite different when the limit was 4.
    assert (result.moved, result.reason) == (4, "limit")
    assert len(transport.waiting[DLQ]) == 6


async def test_a_deadline_stops_the_pass_and_says_so() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 3)

    result = await replay(mq, DLQ, deadline=timedelta(0))

    assert (result.moved, result.reason) == (0, "deadline")
    assert len(transport.waiting[DLQ]) == 3


async def test_a_routing_key_can_send_everything_somewhere_else() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    stage(transport, 2)

    await replay(mq, DLQ, routing_key="orders.quarantine")

    assert len(transport.sent_to("orders.quarantine")) == 2
    assert transport.sent_to(QUEUE) == []


async def test_a_message_that_could_not_be_republished_stays_on_the_queue() -> None:
    # A replay that loses messages is worse than one that stops early.
    class Refusing(FakeTransport):
        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            raise ConnectionError("the broker is not answering")

    transport = Refusing()
    mq = Connection(transport)
    stage(transport, 3)

    with pytest.raises(ReplayError, match="cannot republish") as failure:
        await replay(mq, DLQ)

    # And it says how far it got, because "it failed" and "it failed after
    # moving four hundred" are different things to be told at three in the
    # morning.
    assert failure.value.result.moved == 0
    assert len(transport.waiting[DLQ]) == 3


async def test_a_replay_needs_a_queue_to_read_from() -> None:
    mq = Connection(FakeTransport())
    with pytest.raises(ValueError, match="needs a queue"):
        await replay(mq, "")
