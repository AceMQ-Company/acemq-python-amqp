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

"""The same rules, against a broker that has its own opinions.

The unit tests prove the consumer decides the right thing. These prove a real
RabbitMQ agrees: that a topology declared here is really there, that an envelope
written by this library reads back as the same envelope, that a retry really
comes round again with the attempt counter one higher, and that a message which
runs out of attempts really lands in ``{queue}.dlq`` carrying the reason.

Every name these tests create starts with ``pyit.`` and is deleted afterwards, so
they can be pointed at a broker that is not theirs alone without taking anything
that is not.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import timedelta

import pytest

from acemq_amqp import (
    Ack,
    BytesCodec,
    Codec,
    Connection,
    Envelope,
    FatalError,
    Message,
    Outbound,
    Topology,
    accept,
    connect,
    dead_letter_queue,
    fixed_retry,
    reject,
    retry,
    sync,
)

pytestmark = pytest.mark.integration

#: Where the broker is. The CI job sets it; a laptop points it at whatever is
#: running locally.
BROKER = os.environ.get("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672/")

#: Every queue and exchange these tests create starts with this, so a broker
#: shared with anything else can be cleaned up without guessing.
PREFIX = "pyit."


class Workspace:
    """Names, declares and afterwards removes everything one test needs."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self._run = uuid.uuid4().hex[:8]
        self._queues: list[str] = []

    def name(self, what: str) -> str:
        """A name nothing else on the broker will be using."""
        return f"{PREFIX}{self._run}.{what}"

    def register(self, queue: str) -> None:
        """Remembers a queue this test declared, so it is removed afterwards."""
        self._queues += [queue, dead_letter_queue(queue)]

    async def queue(self, what: str) -> str:
        """Declares a queue and its dead-letter queue, and returns the name."""
        name = self.name(what)
        await self.connection.declare(Topology().queue(name, dead_letter=True))
        self.register(name)
        return name

    async def cleanup(self) -> None:
        for name in self._queues:
            # Suppressed because a test that already failed should report why it
            # failed rather than why the tidying up afterwards did.
            with contextlib.suppress(Exception):
                await self.connection.delete_queue(name)


@pytest.fixture
async def mq() -> AsyncIterator[Connection]:
    connection = await connect(BROKER, origin="acemq-python-tests@ci")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def workspace(mq: Connection) -> AsyncIterator[Workspace]:
    space = Workspace(mq)
    try:
        yield space
    finally:
        await space.cleanup()


async def collect(
    mq: Connection,
    queue: str,
    count: int = 1,
    timeout: float = 20.0,
    codec: Codec | None = None,
) -> list[Message]:
    """Reads messages off a queue until there are enough of them.

    A dead-letter queue is read with BytesCodec, because the message that went
    there may be exactly the one nothing could decode — and a reader that
    dead-letters what it cannot read would send it round again.
    """
    got: list[Message] = []
    enough = asyncio.Event()

    async def handler(message: Message) -> Ack:
        got.append(message)
        if len(got) >= count:
            enough.set()
        return accept()

    consumer = await mq.consume(queue, handler, codec=codec)
    try:
        await asyncio.wait_for(enough.wait(), timeout)
    finally:
        await consumer.close()
    return got


async def test_a_topology_is_really_on_the_broker_afterwards(
    mq: Connection, workspace: Workspace
) -> None:
    queue = workspace.name("orders")
    exchange = workspace.name("events")

    assert await mq.queue_exists(queue) is False

    await mq.declare(
        Topology()
        # Auto-delete so the tidying up afterwards needs only the queues: an
        # exchange with no bindings left goes on its own, and deleting the queue
        # is what takes the last binding away.
        .exchange(exchange, "topic", auto_delete=True)
        .queue(queue, dead_letter=True)
        .binding(queue, exchange, "order.#")
    )
    workspace.register(queue)

    assert await mq.queue_exists(queue) is True
    assert await mq.queue_exists(dead_letter_queue(queue)) is True
    assert await mq.message_count(queue) == 0

    # And the binding is real: a message on the exchange reaches the queue.
    await mq.publisher(exchange, "order.placed", mandatory=True).send({"id": "7"})
    assert (await collect(mq, queue))[0].payload == {"id": "7"}


async def test_the_envelope_survives_a_real_broker(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("orders")
    sent = Envelope(
        id="order-1",
        type="order.placed.v2",
        version=3,
        correlation_id="cart-9",
        causation_id="click-4",
        origin="checkout@pod-7",
        claim="s3://bucket/order-1",
        headers={"tenant": "acme"},
    )

    await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"}, envelope=sent)
    read = (await collect(mq, queue))[0]

    assert read.payload == {"id": "7"}
    assert read.content_type == "application/json"
    assert read.envelope.id == "order-1"
    assert read.envelope.type == "order.placed.v2"
    assert read.envelope.version == 3
    assert read.envelope.correlation_id == "cart-9"
    assert read.envelope.causation_id == "click-4"
    assert read.envelope.origin == "checkout@pod-7"
    assert read.envelope.claim == "s3://bucket/order-1"
    assert read.envelope.attempt == 1
    assert read.envelope.headers == {"tenant": "acme"}
    # Epoch milliseconds survive the round trip to the second, which is all a
    # broker's clock and this one agree on anyway.
    assert abs((read.envelope.first_seen - sent.first_seen).total_seconds()) < 1


async def test_a_retry_really_comes_round_again_and_then_dead_letters(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("flaky")
    policy = fixed_retry(3, timedelta(milliseconds=50))

    attempts: list[int] = []
    spent = asyncio.Event()

    async def handler(message: Message) -> Ack:
        attempts.append(message.envelope.attempt)
        if len(attempts) >= policy.max_attempts:
            spent.set()
        return retry(RuntimeError("the database is down"))

    consumer = await mq.consume(queue, handler, retry=policy)
    try:
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
        await asyncio.wait_for(spent.wait(), 20.0)
    finally:
        await consumer.close()

    # The attempt counter is on the message rather than in this process, so it
    # reads the same to the handler as it does to anything else looking at it.
    assert attempts == [1, 2, 3]

    dead = (await collect(mq, dead_letter_queue(queue)))[0]
    assert dead.payload == {"id": "7"}
    assert dead.envelope.attempt == 3
    assert "exhausted 3 attempts" in dead.envelope.error
    assert "RuntimeError: the database is down" in dead.envelope.error


async def test_a_fatal_error_does_not_use_the_attempts_it_has_left(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("poison")
    seen: list[int] = []

    async def handler(message: Message) -> Ack:
        seen.append(message.envelope.attempt)
        return retry(FatalError("this order has no customer"))

    quick = fixed_retry(5, timedelta(milliseconds=10))
    consumer = await mq.consume(queue, handler, retry=quick)
    try:
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
        dead = (await collect(mq, dead_letter_queue(queue)))[0]
    finally:
        await consumer.close()

    assert seen == [1]
    assert "unprocessable" in dead.envelope.error
    assert "this order has no customer" in dead.envelope.error


async def test_a_body_the_codec_cannot_read_never_reaches_the_handler(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("garbled")
    reached: list[Message] = []

    async def handler(message: Message) -> Ack:
        reached.append(message)
        return accept()

    quick = fixed_retry(5, timedelta(milliseconds=10))
    consumer = await mq.consume(queue, handler, retry=quick)
    try:
        await mq.publish_raw(
            "",
            queue,
            Outbound(body=b"not json at all", content_type="application/json"),
        )
        dead = (await collect(mq, dead_letter_queue(queue), codec=BytesCodec()))[0]
    finally:
        await consumer.close()

    assert reached == []
    assert "could not be decoded" in dead.envelope.error
    # The bytes that could not be read are the bytes that arrive in the
    # dead-letter queue, so somebody can look at what was actually sent.
    assert dead.body == b"not json at all"


async def test_rejecting_dead_letters_without_a_second_attempt(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("wrong")

    async def handler(message: Message) -> Ack:
        return reject(ValueError("no customer on this order"))

    quick = fixed_retry(5, timedelta(milliseconds=10))
    consumer = await mq.consume(queue, handler, retry=quick)
    try:
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
        dead = (await collect(mq, dead_letter_queue(queue)))[0]
    finally:
        await consumer.close()

    assert "the handler rejected it" in dead.envelope.error
    assert "ValueError: no customer on this order" in dead.envelope.error


@pytest.fixture
def blocking() -> Iterator[sync.SyncConnection]:
    connection = sync.connect(BROKER)
    try:
        yield connection
    finally:
        connection.close()


def test_the_blocking_api_publishes_and_consumes(blocking: sync.SyncConnection) -> None:
    queue = f"{PREFIX}{uuid.uuid4().hex[:8]}.blocking"
    blocking.declare(Topology().queue(queue, dead_letter=True))
    try:
        got: list[Message] = []
        arrived = threading.Event()

        def handler(message: Message) -> Ack:
            got.append(message)
            arrived.set()
            return accept()

        with blocking.consume(queue, handler):
            blocking.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
            assert arrived.wait(20.0) is True

        assert got[0].payload == {"id": "7"}
        assert got[0].envelope.origin.startswith("acemq@")
    finally:
        with contextlib.suppress(Exception):
            blocking.delete_queue(queue)
        with contextlib.suppress(Exception):
            blocking.delete_queue(dead_letter_queue(queue))
