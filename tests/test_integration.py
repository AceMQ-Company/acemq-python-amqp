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
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
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
    RetryPolicy,
    Topology,
    accept,
    connect,
    dead_letter_queue,
    fixed_retry,
    parked_queue,
    reject,
    retry,
    retry_queue,
    sync,
)
from acemq_amqp.patterns import (
    InMemoryIdempotencyStore,
    InMemoryOutboxStore,
    OutboxRelay,
    Requester,
    ResponderError,
    idempotent,
    record,
    serve,
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

    def register(self, queue: str, policy: RetryPolicy | None = None) -> None:
        """Remembers a queue this test declared, so it is removed afterwards."""
        self._queues += [queue, dead_letter_queue(queue), parked_queue(queue)]
        if policy is not None:
            self._queues += [retry_queue(queue, rung) for rung in policy.broker_rungs()]

    async def queue(self, what: str, policy: RetryPolicy | None = None) -> str:
        """Declares a queue with its dead-letter, parked and rung queues, and
        returns the name."""
        name = self.name(what)
        await self.connection.declare(
            Topology().queue(name, dead_letter=True, retry=policy)
        )
        self.register(name, policy)
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


async def until(check: Callable[[], Awaitable[bool]], what: str, timeout: float = 15.0) -> None:
    """Waits for something a broker will do in its own time.

    A queue's counters move when the broker gets round to it, so a test that
    reads one immediately after causing it reads the number from before.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"waited {timeout}s and {what} never happened")


async def test_a_long_retry_waits_in_the_broker_and_comes_back(
    mq: Connection, workspace: Workspace
) -> None:
    # A threshold of a second and a two-second wait, so this finishes in seconds
    # rather than in the half hour a policy with a realistic long delay takes.
    # The arithmetic being tested is the same at either scale.
    policy = fixed_retry(2, timedelta(seconds=2)).wait_in_broker_from(timedelta(seconds=1))
    queue = await workspace.queue("slow", policy)
    rung = retry_queue(queue, timedelta(seconds=2))

    assert policy.broker_rungs() == [timedelta(seconds=2)]
    assert await mq.queue_exists(rung) is True

    attempts: list[int] = []

    async def handler(message: Message) -> Ack:
        attempts.append(message.envelope.attempt)
        return retry(RuntimeError("the warehouse is not answering"))

    consumer = await mq.consume(queue, handler, retry=policy)
    try:
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})

        # The wait is the broker's: the message is sitting on the rung, and this
        # consumer is holding nothing at all while it does.
        await until(lambda: _count(mq, rung, 1), "the message reached the rung queue")
        assert await mq.message_count(queue) == 0

        # And the rung gives it back when the TTL expires, one attempt further on.
        dead = (await collect(mq, dead_letter_queue(queue)))[0]
    finally:
        await consumer.close()

    assert attempts == [1, 2]
    assert dead.payload == {"id": "7"}
    assert dead.envelope.attempt == 2
    assert "exhausted 2 attempts" in dead.envelope.error
    await until(lambda: _count(mq, rung, 0), "the rung queue emptied")


async def _count(mq: Connection, queue: str, expected: int) -> bool:
    return await mq.message_count(queue) == expected


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
        parked = (await collect(mq, parked_queue(queue), codec=BytesCodec()))[0]
        dead_letters = await mq.message_count(dead_letter_queue(queue))
    finally:
        await consumer.close()

    assert reached == []
    assert "could not be decoded" in parked.envelope.error
    # Parked, not dead-lettered: a message that failed five times and a message
    # nothing could read are different problems, and whoever drains the dead
    # letters should not have to sort them by hand.
    assert dead_letters == 0
    # The bytes that could not be read are the bytes that arrive, so somebody
    # can look at what was actually sent.
    assert parked.body == b"not json at all"


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


async def test_an_outbox_relay_publishes_what_was_committed(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("outbox")
    store = InMemoryOutboxStore()

    # Written where the work is written, published later by something else. What
    # a real store adds is that this line and the database write commit together.
    await store.add(record(mq, "", queue, {"id": "7"}))

    async with OutboxRelay(mq, store, interval=timedelta(milliseconds=20)):
        got = (await collect(mq, queue))[0]

    assert got.payload == {"id": "7"}
    assert got.envelope.origin == "acemq-python-tests@ci"
    assert len(store) == 0


async def test_a_duplicate_is_accepted_without_running_the_handler_again(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("once")
    ran: list[str] = []
    seen = asyncio.Event()

    async def handler(message: Message) -> Ack:
        ran.append(message.envelope.id)
        seen.set()
        return accept()

    guarded = idempotent(InMemoryIdempotencyStore(), handler)
    consumer = await mq.consume(queue, guarded)
    try:
        envelope = Envelope(id="order-1")
        publisher = mq.publisher(routing_key=queue, mandatory=True)
        # The same message twice, as a redelivery or a relay that swept twice
        # would produce it.
        await publisher.send({"id": "7"}, envelope=envelope)
        await publisher.send({"id": "7"}, envelope=envelope)
        await asyncio.wait_for(seen.wait(), 20.0)
        await until(lambda: _count(mq, queue, 0), "both copies were consumed")
    finally:
        await consumer.close()

    assert ran == ["order-1"]
    # Accepted, not dead-lettered: the work was done, so the message has been
    # handled and nothing should be raising an alarm about it.
    assert await mq.message_count(dead_letter_queue(queue)) == 0


async def test_a_question_over_a_queue_comes_back_answered(
    mq: Connection, workspace: Workspace
) -> None:
    requests = await workspace.queue("price-requests")
    # Named rather than generated, so the name carries this suite's prefix on a
    # broker it is sharing. A service of its own would take the generated one.
    replies = workspace.name("price-replies")
    workspace.register(replies)

    async def price(message: Message) -> dict[str, object]:
        if message.payload["sku"] == "gone":
            raise LookupError("no such sku")
        return {"sku": message.payload["sku"], "pence": 250}

    async with (
        await serve(mq, requests, price),
        await Requester.open(
            mq, "", requests, reply_queue=replies, timeout=timedelta(seconds=15)
        ) as caller,
    ):
        assert await caller.ask({"sku": "A-1"}) == {"sku": "A-1", "pence": 250}

        # A failure comes back as a failure, in milliseconds, rather than as a
        # caller waiting out its whole timeout to learn nothing.
        with pytest.raises(ResponderError, match="no such sku"):
            await caller.ask({"sku": "gone"})


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
