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

"""Closing the blocking API is bounded, as closing the async one is.

A handler on a thread cannot be cancelled, so a handler still running at the
deadline is abandoned: its thread carries on, its result is thrown away, and
its message is left unsettled for the broker to hand out again.
"""

from __future__ import annotations

import inspect
import logging
import threading
import time
from collections.abc import Iterator

import pytest
from fake_transport import FakeTransport
from fake_transport import Settlement as Outcome

from acemq_amqp import DEFAULT_DRAIN_TIMEOUT, Ack, Envelope, Message, accept, sync
from acemq_amqp.connection import Connection
from acemq_amqp.transport import Delivery

QUEUE = "orders.new"


class Blocked:
    """Thread handlers that start, say so, and wait to be let go."""

    def __init__(self) -> None:
        self.started = threading.Semaphore(0)
        self.release = threading.Event()
        self.finished = threading.Event()

    def handler(self, message: Message) -> Ack:
        self.started.release()
        self.release.wait(10)
        self.finished.set()
        return accept()


@pytest.fixture
def wired() -> Iterator[tuple[FakeTransport, sync.SyncConnection]]:
    transport = FakeTransport()
    loop = sync._LoopThread(name="acemq-test-loop")

    async def build() -> Connection:
        return Connection(transport)

    connection = sync.SyncConnection(loop, loop.run(build()))
    try:
        yield transport, connection
    finally:
        connection.close(timeout=0)


def hand_over(
    transport: FakeTransport, connection: sync.SyncConnection, count: int
) -> list[Outcome]:
    """Delivers messages on the loop without waiting for them to be settled."""
    settlements = [Outcome() for _ in range(count)]

    async def deliver() -> None:
        for settlement in settlements:

            async def ack(settlement: Outcome = settlement) -> None:
                settlement.acked = True

            async def nack(requeue: bool, settlement: Outcome = settlement) -> None:
                settlement.nacked = True
                settlement.requeued = requeue

            await transport.consumers[QUEUE](
                Delivery(
                    body=b"{}",
                    content_type="application/json",
                    routing_key=QUEUE,
                    message_id="",
                    headers=Envelope().to_headers(routing_key=QUEUE),
                    redelivered=False,
                    ack=ack,
                    nack=nack,
                )
            )

    connection._loop.run(deliver())
    return settlements


def started(blocked: Blocked, count: int) -> None:
    for _ in range(count):
        assert blocked.started.acquire(timeout=5)


def test_close_takes_the_same_timeout_as_the_async_api() -> None:
    for method in (sync.SyncConsumer.close, sync.SyncConnection.close):
        parameter = inspect.signature(method).parameters["timeout"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default == DEFAULT_DRAIN_TIMEOUT


def test_a_drain_that_finishes_says_so(
    wired: tuple[FakeTransport, sync.SyncConnection],
) -> None:
    transport, connection = wired
    blocked = Blocked()
    consumer = connection.consume(QUEUE, blocked.handler, concurrency=2)
    settlements = hand_over(transport, connection, 2)
    started(blocked, 2)
    threading.Timer(0.05, blocked.release.set).start()

    assert consumer.close(timeout=5.0) is True
    assert [settlement.acked for settlement in settlements] == [True, True]


def test_a_consumer_drain_is_bounded_and_leaves_the_message_unsettled(
    wired: tuple[FakeTransport, sync.SyncConnection],
) -> None:
    transport, connection = wired
    blocked = Blocked()
    consumer = connection.consume(QUEUE, blocked.handler, concurrency=3)
    settlements = hand_over(transport, connection, 3)
    started(blocked, 3)

    began = time.monotonic()
    drained = consumer.close(timeout=0.2)
    elapsed = time.monotonic() - began

    assert drained is False
    assert elapsed < 2.0
    # Abandoned, not stopped: a thread cannot be cancelled.
    assert not blocked.finished.is_set()
    # Cut off is not settled: neither acknowledged nor rejected, so the broker
    # hands each one out again once the channel goes.
    assert settlements == [Outcome()] * 3

    # The thread finishing later must not settle anything after the fact.
    blocked.release.set()
    assert blocked.finished.wait(5)
    time.sleep(0.1)
    assert settlements == [Outcome()] * 3


def test_a_connection_close_is_bounded_and_a_late_thread_settles_nothing(
    wired: tuple[FakeTransport, sync.SyncConnection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport, connection = wired
    blocked = Blocked()
    connection.consume(QUEUE, blocked.handler)
    settlements = hand_over(transport, connection, 1)
    started(blocked, 1)

    began = time.monotonic()
    drained = connection.close(timeout=0.2)
    elapsed = time.monotonic() - began

    assert drained is False
    assert elapsed < 2.0
    assert transport.closed is True
    assert settlements == [Outcome()]

    # The loop is gone by now; the late result goes nowhere and says nothing.
    with caplog.at_level(logging.DEBUG):
        blocked.release.set()
        assert blocked.finished.wait(5)
        time.sleep(0.1)
    assert settlements == [Outcome()]
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    # Closing twice is harmless and has nothing more to drain.
    assert connection.close() is True
