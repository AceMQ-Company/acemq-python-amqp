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
import json
import os
import ssl
import threading
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit, urlunsplit

import aio_pika
import pytest
from aiormq.exceptions import AMQPConnectionError, ChannelPreconditionFailed

from acemq_amqp import (
    DEAD_LETTER_EXCHANGE,
    METRIC_ACCEPTED,
    METRIC_CONSUMED,
    METRIC_HANDLER_DURATION,
    METRIC_PUBLISHED,
    RETRY_EXCHANGE,
    Ack,
    BytesCodec,
    Codec,
    Connection,
    ConsumeContext,
    ConsumeNext,
    Credentials,
    Envelope,
    FatalError,
    HealthStatus,
    Message,
    Metrics,
    Outbound,
    PublishContext,
    PublishNext,
    PublishResult,
    RetryPolicy,
    Security,
    Topology,
    accept,
    connect,
    dead_letter_queue,
    exponential_retry,
    fixed_retry,
    parked_queue,
    reject,
    retry,
    retry_queue,
    rung_args,
    sync,
    without_verifying_the_broker,
)
from acemq_amqp.patterns import (
    HEADER_REPLAY_COUNT,
    HEADER_REPLAYED_FROM,
    HEADER_ROUTING_SLIP,
    NOTHING,
    ConsumerGroup,
    InMemoryIdempotencyStore,
    InMemoryOutboxStore,
    OutboxRelay,
    Requester,
    ResponderError,
    RoutingSlip,
    StreamRetention,
    declare_stream,
    follow_slip,
    from_first,
    idempotent,
    read_stream,
    record,
    replay,
    serve,
    start,
    then,
)
from acemq_amqp.rabbitmq import RabbitMQTransport
from acemq_amqp.telemetry import metric_key

pytestmark = pytest.mark.integration

#: Where the broker is. The CI job sets it; a laptop points it at whatever is
#: running locally.
BROKER = os.environ.get("ACEMQ_TEST_BROKER", "amqp://guest:guest@localhost:5672/")

#: Every queue and exchange these tests create starts with this, so a broker
#: shared with anything else can be cleaned up without guessing.
PREFIX = "pyit."

#: A broker with a TLS listener that verifies nothing about the client, and one
#: that will not talk to a client without a certificate. Both are unset on a
#: laptop with only a plain broker running, and the TLS tests skip rather than
#: fail: a suite that reports red because a machine has no certificates on it
#: teaches everybody to ignore red.
TLS_BROKER = os.environ.get("ACEMQ_TEST_TLS_BROKER", "")
MUTUAL_TLS_BROKER = os.environ.get("ACEMQ_TEST_TLS_MUTUAL_BROKER", "")

#: A directory holding ``ca.crt``, the client's ``client.crt`` and
#: ``client.key``, and a ``stranger.crt``/``stranger.key`` pair signed by an
#: authority the broker has never heard of.
CERTIFICATES = Path(os.environ.get("ACEMQ_TEST_TLS_CERTIFICATES", "/nonexistent"))

#: The login for the TLS brokers, supplied through a :class:`Security` rather
#: than in the URL — which is the thing being tested as much as it is a detail
#: of how these tests connect.
TLS_LOGIN = Credentials(
    os.environ.get("ACEMQ_TEST_TLS_USERNAME", "guest"),
    os.environ.get("ACEMQ_TEST_TLS_PASSWORD", "guest"),
)

needs_a_tls_broker = pytest.mark.skipif(
    not TLS_BROKER, reason="ACEMQ_TEST_TLS_BROKER is not set"
)
needs_a_mutual_tls_broker = pytest.mark.skipif(
    not MUTUAL_TLS_BROKER, reason="ACEMQ_TEST_TLS_MUTUAL_BROKER is not set"
)


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


async def _consumers(mq: Connection, queue: str) -> int:
    """How many consumers the broker thinks are attached to a queue.

    Through the transport rather than through the connection, because this is
    the one question the library does not wrap and the escape hatch is there
    precisely for it. It is what turns "the source queue is empty" from a
    statement about the queue into a statement about the whole system: a queue
    can also be empty because a consumer is holding everything on it.
    """
    transport = mq.transport
    assert isinstance(transport, RabbitMQTransport)
    channel = await transport.connection.channel()
    try:
        found = await channel.get_queue(queue)
        return int(found.declaration_result.consumer_count or 0)
    finally:
        await channel.close()


async def _detached(mq: Connection, queue: str) -> bool:
    return await _consumers(mq, queue) == 0


async def test_a_rung_returns_a_message_through_the_named_retry_exchange(
    mq: Connection, workspace: Workspace
) -> None:
    """The whole broker wait, one step at a time, through ``acemq.retry``.

    The version of this above proves the arithmetic. This one proves the
    routing, which is what changed: a rung no longer dead-letters through the
    default exchange, and the binding that brings an expired message home is now
    something the topology makes rather than something a caller remembers. A
    missing binding does not fail — a direct exchange drops what it cannot route
    and says nothing — so the failure it would cause is every retry disappearing
    while the queue looks quiet.
    """
    delay = timedelta(seconds=5)
    policy = fixed_retry(3, delay).wait_in_broker_from(timedelta(seconds=1))
    queue = await workspace.queue("rung", policy)
    rung = retry_queue(queue, delay)

    # Printed so the declaration can be compared against Go, .NET and Java by
    # eye, which is the only check that catches the three of them agreeing with
    # each other and not with this.
    print(f"\nrung declaration for {queue}")
    print(f"  queue    {rung}")
    for key, value in rung_args(queue, delay).items():
        print(f"  argument {key} = {value!r}")
    print(f"  exchange {RETRY_EXCHANGE} (direct, durable)")
    print(f"  binding  {queue} -> {RETRY_EXCHANGE} -> {queue}")
    print(f"  exchange {DEAD_LETTER_EXCHANGE} (direct, durable)")
    print(
        f"  binding  {dead_letter_queue(queue)} -> {DEAD_LETTER_EXCHANGE}"
        f" -> {dead_letter_queue(queue)}"
    )
    print(
        f"  binding  {parked_queue(queue)} -> {DEAD_LETTER_EXCHANGE}"
        f" -> {parked_queue(queue)}"
    )

    # The binding is live on its own, before any retry needs it: a message put
    # straight on the exchange under the source queue's name reaches the queue.
    # Mandatory, so an exchange that routed it nowhere is an exception here
    # rather than a silence in production.
    probe = await mq.publisher(RETRY_EXCHANGE, queue, mandatory=True).send({"probe": True})
    assert probe.routed is True
    await until(lambda: _count(mq, queue, 1), "the probe reached the queue")
    held = await mq.pull(queue)
    assert held is not None
    await held.ack()

    attempts: list[int] = []

    async def handler(message: Message) -> Ack:
        attempts.append(message.envelope.attempt)
        return retry(RuntimeError("the warehouse is not answering"))

    consumer = await mq.consume(queue, handler, retry=policy)
    await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
    await until(lambda: _count(mq, rung, 1), "the message reached the rung queue")

    # Closed before anything is counted. A zero on the source queue with a
    # consumer still attached proves nothing: the consumer could be holding the
    # message unacknowledged, which is exactly the failure the rung exists to
    # avoid. With it gone, a zero is a zero.
    await consumer.close()
    await until(lambda: _detached(mq, queue), "the consumer detached")

    assert attempts == [1]
    assert await mq.message_count(rung) == 1
    assert await mq.message_count(queue) == 0
    assert await _consumers(mq, queue) == 0
    print(f"  waiting  {rung} holds 1, {queue} holds 0, {queue} has 0 consumers")

    # Nothing consumes a rung. The only thing that takes a message out of one is
    # the time-to-live expiring, and the only thing that brings it back is the
    # exchange and the binding printed above.
    await until(lambda: _count(mq, queue, 1), "the rung returned the message", timeout=30.0)
    assert await mq.message_count(rung) == 0

    returned = await mq.pull(queue)
    assert returned is not None
    await returned.ack()
    envelope = Envelope.from_headers(returned.headers, returned.routing_key)
    assert envelope.attempt == 2
    assert json.loads(returned.body) == {"id": "7"}
    print(f"  returned {queue} holds 1, on attempt {envelope.attempt}, {rung} holds 0")


def java_source_arguments(queue: str) -> dict[str, str]:
    """Exactly what a Java service writes when it declares a source queue.

    Spelled out as literals rather than built from this library's constants on
    purpose. A test that asks this library what this library writes, and then
    checks the answer against itself, proves nothing about the other service;
    these three strings are read off ``Topology.java`` and ``RabbitMqConnection``
    and are the thing being claimed.
    """
    return {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "acemq.dlx",
        "x-dead-letter-routing-key": f"{queue}.dlq",
    }


async def declared_by_another_service(queue: str, arguments: Mapping[str, Any]) -> None:
    """Declares a queue from a second connection, without this library.

    Straight onto ``aio_pika``, on a connection of its own, because the point is
    a service that shares the broker and not the code. Its own channel too: a
    declare the broker refuses takes the channel down with it, and a shared one
    would take every later declare with it.

    :raises ChannelPreconditionFailed: when the broker says this is not the
        queue that is already there
    """
    connection = await aio_pika.connect_robust(BROKER)
    try:
        channel = await connection.channel()
        await channel.declare_queue(queue, durable=True, arguments=dict(arguments))
    finally:
        await connection.close()


async def test_a_queue_python_declares_is_one_a_java_service_can_declare_too(
    mq: Connection, workspace: Workspace
) -> None:
    """The interop claim, proved rather than asserted.

    Two services in different languages consuming one queue both declare it, and
    the broker compares what the second one sends against what the first one
    created. So the claim is not "Python sends ``x-queue-type=quorum``" — it is
    "the declaration Java sends is accepted against the queue Python made", and
    the only thing that can answer that is a broker.

    The refusal underneath is half of the same claim. Without it the test would
    pass just as well against a broker that had stopped comparing arguments at
    all, which is the failure that would let this whole change be wrong and look
    right.
    """
    queue = await workspace.queue("orders")

    print(f"\ninterop for {queue}")
    print(f"  python declared it, then java declares {java_source_arguments(queue)}")

    # Accepted: the second service gets the queue it asked for.
    await declared_by_another_service(queue, java_source_arguments(queue))
    print("  accepted")

    # And a classic declaration of the same queue is refused, which is what a
    # Python service would have got before this change — the same three
    # arguments with x-queue-type left off, which is how classic is spelled.
    classic = {
        key: value
        for key, value in java_source_arguments(queue).items()
        if key != "x-queue-type"
    }
    with pytest.raises(ChannelPreconditionFailed) as refusal:
        await declared_by_another_service(queue, classic)

    said = str(refusal.value)
    print(f"  refused  {classic} -> {said}")
    assert "x-queue-type" in said
    assert "quorum" in said

    # The queue is still there and still usable afterwards: a refused declare is
    # the broker protecting the queue, not damaging it.
    assert await mq.queue_exists(queue) is True
    await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
    assert (await collect(mq, queue))[0].payload == {"id": "7"}


async def test_a_retry_goes_round_a_quorum_source_queue_and_a_classic_rung(
    mq: Connection, workspace: Workspace
) -> None:
    """A whole broker wait, on the queue types this library now declares.

    Not a formality. A quorum queue dead-letters through different machinery
    from a classic one, and the rung it goes to is classic while the queue it
    comes home to is not, so every hop in this cycle crosses between the two.
    """
    # Eight seconds rather than five. The test has to detach the consumer before
    # the rung gives the message back, or a second delivery would arrive while
    # it was still counting the first; on a broker with a cold node the steps in
    # between take long enough to make five a race this test would lose
    # occasionally and confusingly.
    delay = timedelta(seconds=8)
    policy = fixed_retry(2, delay).wait_in_broker_from(timedelta(seconds=1))
    queue = await workspace.queue("mixed", policy)
    rung = retry_queue(queue, delay)

    # The types, confirmed by the broker rather than by this process: an
    # equivalence declare is accepted only when the queue really is what the
    # arguments say. The rung is offered rung_args and nothing else, which is a
    # classic queue — the argument that would make it quorum is not in there.
    await declared_by_another_service(queue, java_source_arguments(queue))
    await declared_by_another_service(rung, rung_args(queue, delay))
    print(f"\nretry cycle for {queue}")
    print(f"  source   {queue} is quorum")
    print(f"  rung     {rung} is classic, {rung_args(queue, delay)}")

    attempts: list[int] = []

    async def handler(message: Message) -> Ack:
        attempts.append(message.envelope.attempt)
        return retry(RuntimeError("the warehouse is not answering"))

    consumer = await mq.consume(queue, handler, retry=policy)
    await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
    await until(lambda: _count(mq, rung, 1), "the message reached the rung queue")

    # Closed before anything is counted, because a zero on the source queue with
    # a consumer still attached could be a consumer holding the message.
    await consumer.close()
    await until(lambda: _detached(mq, queue), "the consumer detached")

    assert attempts == [1]
    assert await mq.message_count(rung) == 1
    assert await mq.message_count(queue) == 0
    assert await _consumers(mq, queue) == 0
    print(f"  waiting  {rung} holds 1, {queue} holds 0 with 0 consumers")

    # And it comes home, one attempt further on, to the quorum queue it left.
    await until(lambda: _count(mq, queue, 1), "the rung returned the message", timeout=30.0)
    assert await mq.message_count(rung) == 0

    returned = await mq.pull(queue)
    assert returned is not None
    await returned.ack()
    envelope = Envelope.from_headers(returned.headers, returned.routing_key)
    assert envelope.attempt == 2
    assert json.loads(returned.body) == {"id": "7"}
    print(f"  returned {queue} holds 1, on attempt {envelope.attempt}, {rung} holds 0")


async def test_the_whole_topology_is_printed_with_every_type_and_argument(
    mq: Connection, workspace: Workspace
) -> None:
    """Everything one topology puts on a broker, in one place, to be read.

    Five libraries have to declare the same thing, and no assertion inside any
    one of them catches the case where four agree with each other and not with
    the fifth. A person comparing five printouts does. So this prints what was
    asked for, and then has the broker confirm each line of it: an equivalence
    declare from a connection that is not this library's is accepted only if the
    queue really carries those arguments and no others.
    """
    policy = exponential_retry(6, timedelta(seconds=10))
    queue = workspace.name("orders")
    exchange = workspace.name("events")
    topology = (
        Topology()
        .exchange(exchange, "topic", auto_delete=True)
        .queue(queue, dead_letter=True, retry=policy)
        .binding(queue, exchange, "order.#")
    )

    await mq.declare(topology)
    workspace.register(queue, policy)

    print(f"\n{topology}")

    confirmed: dict[str, dict[str, Any]] = {
        queue: {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": dead_letter_queue(queue),
        },
        dead_letter_queue(queue): {},
        parked_queue(queue): {},
    }
    for rung in policy.broker_rungs():
        confirmed[retry_queue(queue, rung)] = dict(rung_args(queue, rung))

    print("confirmed against the broker:")
    for name, arguments in confirmed.items():
        await declared_by_another_service(name, arguments)
        kind = arguments.get("x-queue-type", "classic")
        print(f"  {name}: {kind}, {arguments or 'no arguments'}")


async def test_interceptors_wrap_a_real_publish_and_a_real_handler(
    mq: Connection, workspace: Workspace
) -> None:
    # The unit tests prove the chain composes. This proves the header an
    # interceptor added really travels, and that the one on the way in really
    # sees what came off the wire rather than what was handed to the publisher.
    #
    # On a connection of its own rather than the shared one, because an
    # interceptor is registered for the life of a connection and stamping every
    # later test in this file is not what it is here to show.
    queue = await workspace.queue("intercepted")
    order: list[str] = []
    handled: list[Message] = []

    async def stamping(context: PublishContext, send: PublishNext) -> PublishResult:
        context.set_header("tenant", "acme")
        return await send(context)

    async def timing(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        order.append("in")
        try:
            return await handle(context)
        finally:
            order.append("out")

    async def handler(message: Message) -> Ack:
        handled.append(message)
        return accept()

    intercepted = await connect(BROKER, on_publish=[stamping], on_consume=[timing])
    async with intercepted, await intercepted.consume(queue, handler):
        await intercepted.publisher(routing_key=queue, mandatory=True).send({"id": "9"})
        await until(lambda: _handled(handled), "the message was handled")

    assert handled[0].envelope.headers["tenant"] == "acme"
    assert order == ["in", "out"]


async def _handled(seen: list[Message]) -> bool:
    return len(seen) >= 1


async def test_health_asks_the_broker_something_and_leaves_nothing_behind(
    mq: Connection, workspace: Workspace
) -> None:
    # A socket that is open but wedged answers a socket-level check exactly as a
    # healthy one does, so the check asks the broker a question. It has to ask
    # one that creates nothing: a probe that declared even a temporary queue
    # would leave one per connection, because an exclusive queue is released
    # only when the channel that declared it closes, and a readiness probe runs
    # every few seconds for as long as the pod lives.
    queue = await workspace.queue("healthy")

    async def handler(message: Message) -> Ack:
        return accept()

    async with await mq.consume(queue, handler):
        for _ in range(5):
            report = await mq.health()
            assert report.status is HealthStatus.UP

    assert report.healthy is True
    assert report.parts["consumers"] == 1
    assert report.parts["round-trip"] >= 0

    # Five probes and the queue count is what the workspace declared and no
    # more. The whole-run count before and after is checked outside pytest.
    assert await mq.queue_exists(queue) is True


async def test_health_is_down_once_the_connection_has_gone(mq: Connection) -> None:
    connection = await connect(BROKER)
    await connection.close()

    report = await connection.health()

    assert report.status is HealthStatus.DOWN
    assert report.healthy is False


async def test_metrics_count_a_real_round_trip(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("counted")
    metrics = Metrics()

    async def handler(message: Message) -> Ack:
        return accept()

    counted = await connect(BROKER, observer=metrics)
    async with counted, await counted.consume(queue, handler):
        await counted.publisher(routing_key=queue, mandatory=True).send({"id": "1"})
        await until(
            lambda: _counted(metrics, METRIC_ACCEPTED, {"queue": queue}),
            "the message was accepted",
        )

    published = metric_key(METRIC_PUBLISHED, {"exchange": "", "key": queue})
    assert metrics.counts[published] == 1
    assert metrics.counts[metric_key(METRIC_CONSUMED, {"queue": queue})] == 1
    assert metrics.durations[metric_key(METRIC_HANDLER_DURATION, {"queue": queue})].count == 1


async def _counted(metrics: Metrics, metric: str, labels: dict[str, str]) -> bool:
    return metrics.counts.get(metric_key(metric, labels), 0) >= 1


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


async def test_a_group_of_consumers_shares_one_queue_and_closes_as_one(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("shared")
    handled: list[int] = []
    everything = asyncio.Event()

    async def handler(message: Message) -> Ack:
        handled.append(message.payload["id"])
        if len(handled) >= 6:
            everything.set()
        return accept()

    async with await ConsumerGroup.start(mq, queue, 3, handler) as group:
        assert group.size == 3
        publisher = mq.publisher(routing_key=queue, mandatory=True)
        for n in range(6):
            await publisher.send({"id": n})
        await asyncio.wait_for(everything.wait(), 20.0)

    # Each consumer has its own channel and its own prefetch, and the broker
    # round-robins between them, so all six are handled and none is left held by
    # a consumer nobody closed.
    assert sorted(handled) == [0, 1, 2, 3, 4, 5]
    assert await mq.message_count(queue) == 0


async def test_a_replay_moves_dead_letters_back_and_leaves_the_rest(
    mq: Connection, workspace: Workspace
) -> None:
    queue = await workspace.queue("replayed")
    dead = dead_letter_queue(queue)

    publisher = mq.publisher(routing_key=dead, mandatory=True)
    for n, reason in enumerate(["disk full", "the database timed out", "disk full"]):
        await publisher.send(
            {"id": n},
            envelope=Envelope(id=f"order-{n}", attempt=5, error=reason),
        )
    await until(lambda: _count(mq, dead, 3), "the dead letters were all written")

    # The routing key is overridden because these were published to the
    # dead-letter queue: keeping their own would put them straight back on it.
    result = await replay(
        mq,
        dead,
        routing_key=queue,
        only=lambda envelope, body: "timed out" in envelope.error,
    )

    assert (result.moved, result.skipped, result.reason) == (1, 2, "drained")

    back = (await collect(mq, queue))[0]
    assert back.payload == {"id": 1}
    assert back.envelope.id == "order-1"
    # A fresh set of attempts, or the five-attempt policy that killed it would
    # kill it again before a handler saw it.
    assert back.envelope.attempt == 1
    assert back.envelope.error == ""
    assert back.envelope.headers[HEADER_REPLAYED_FROM] == dead
    assert back.envelope.headers[HEADER_REPLAY_COUNT] == 1

    # The two it declined are back on the dead-letter queue rather than lost:
    # holding them unsettled is what stops a pass reading the same message for
    # ever, and the broker returns them when the pass ends.
    await until(lambda: _count(mq, dead, 2), "the declined messages went back")


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


async def test_a_routing_slip_visits_every_stop_on_a_real_broker(
    mq: Connection, workspace: Workspace
) -> None:
    validate = await workspace.queue("validate")
    charge = await workspace.queue("charge")
    ship = await workspace.queue("ship")

    itinerary = (
        RoutingSlip()
        .then("", validate, name="validate")
        .then("", charge, name="charge")
        .then("", ship, name="ship")
    )

    async def stamp(message: Message) -> dict[str, object]:
        """Each stop adds itself to the payload, so the route is visible at the
        end as well as recorded on the slip."""
        visited = list(message.payload.get("visited", []))
        return {**message.payload, "visited": [*visited, message.routing_key]}

    async with (
        await mq.consume(validate, follow_slip(mq, stamp)),
        await mq.consume(charge, follow_slip(mq, stamp)),
    ):
        await start(mq, itinerary, {"order": "order-1"})
        arrived = (await collect(mq, ship))[0]

    # The route was decided once, by whoever started the work, and travelled
    # with the message rather than living in a component in the middle.
    carried = json.loads(arrived.envelope.headers[HEADER_ROUTING_SLIP])
    assert [step["name"] for step in carried["done"]] == ["validate", "charge"]
    assert [step["name"] for step in carried["steps"]] == ["ship"]
    assert all(step["completedAt"] for step in carried["done"])
    assert arrived.payload["visited"] == [validate, charge]
    assert arrived.envelope.causation_id != ""


async def test_a_pipeline_step_publishes_onwards_or_stops(
    mq: Connection, workspace: Workspace
) -> None:
    orders = await workspace.queue("orders")
    shipments = await workspace.queue("shipments")

    async def ship(message: Message) -> object:
        if message.payload["digital"]:
            return NOTHING
        return {"shipment": message.payload["id"]}

    async with await mq.consume(orders, then(mq.publisher("", shipments), ship)):
        publisher = mq.publisher(routing_key=orders, mandatory=True)
        await publisher.send({"id": "order-1", "digital": True})
        await publisher.send({"id": "order-2", "digital": False})
        shipped = (await collect(mq, shipments))[0]
        await until(lambda: _count(mq, orders, 0), "both orders were handled")

    # The digital one stopped here rather than becoming an empty message for the
    # next service to handle.
    assert shipped.payload == {"shipment": "order-2"}
    assert await mq.message_count(shipments) == 0


async def test_a_stream_hands_the_same_history_to_every_reader(
    mq: Connection, workspace: Workspace
) -> None:
    name = workspace.name("events")
    await declare_stream(mq, name, StreamRetention(max_age=timedelta(hours=1)))
    workspace.register(name)

    publisher = mq.publisher(routing_key=name, mandatory=True)
    for n in range(3):
        await publisher.send({"n": n})

    async def reader() -> list[int]:
        got: list[int] = []
        everything = asyncio.Event()

        async def handler(message: Message) -> Ack:
            got.append(message.payload["n"])
            if len(got) >= 3:
                everything.set()
            return accept()

        consumer = await read_stream(mq, name, handler, offset=from_first())
        try:
            await asyncio.wait_for(everything.wait(), 20.0)
        finally:
            await consumer.close()
        return got

    assert await reader() == [0, 1, 2]
    # Acknowledging advanced a position rather than removing anything, so a
    # second reader starting from the beginning sees the same three messages.
    # That is the whole difference between a stream and a queue, and reading it
    # twice is the only way to show it: the broker reports a stream's message
    # count as zero, because nothing on a stream is waiting for anybody.
    assert await reader() == [0, 1, 2]
    assert await mq.message_count(name) == 0


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
        # All three, because declaring with dead_letter=True declares all three.
        # This test names its own queue rather than going through the workspace,
        # so nothing else is going to tidy up after it, and one queue left on
        # somebody's broker per run adds up quietly.
        for name in (queue, dead_letter_queue(queue), parked_queue(queue)):
            with contextlib.suppress(Exception):
                blocking.delete_queue(name)


# --------------------------------------------------------------------------
# Reaching the broker safely.
#
# The unit tests prove the SSL context is built the way it was asked for. These
# prove the handshake really happens: a connection that was configured for TLS
# and then quietly made in plaintext would pass every assertion about the
# context and none of the ones below.
# --------------------------------------------------------------------------


@pytest.fixture
def handshakes(monkeypatch: pytest.MonkeyPatch) -> list[ssl.SSLObject]:
    """Every TLS handshake made while a test runs, so it can be looked at.

    The negotiated version, the cipher and the certificate the broker presented
    are only knowable from the object that did the handshake, and neither
    aio-pika nor aiormq keeps one anywhere reachable — the stream writer lives
    in a closure. Recording them as :mod:`ssl` hands them out is the way to
    assert on what actually crossed the wire rather than on what was configured.
    """
    seen: list[ssl.SSLObject] = []
    original = ssl.SSLContext.wrap_bio

    def recording(self: ssl.SSLContext, *args: object, **kwargs: object) -> ssl.SSLObject:
        handshake = original(self, *args, **kwargs)  # type: ignore[arg-type]
        seen.append(handshake)
        return handshake

    monkeypatch.setattr(ssl.SSLContext, "wrap_bio", recording)
    return seen


def trusting_the_test_authority(**extra: object) -> Security:
    """The settings a service deployed against this broker would really use."""
    return Security(
        certificate_authority=CERTIFICATES / "ca.crt",
        credentials=TLS_LOGIN,
        **extra,  # type: ignore[arg-type]
    )


@needs_a_tls_broker
async def test_an_amqps_connection_really_negotiates_tls_and_carries_a_message(
    handshakes: list[ssl.SSLObject],
) -> None:
    mq = await connect(TLS_BROKER, security=trusting_the_test_authority())
    space = Workspace(mq)
    try:
        queue = await space.queue("encrypted")
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
        assert (await collect(mq, queue))[0].payload == {"id": "7"}

        # The message went over a real TLS session, not over a socket that was
        # configured for one. A modern version, a cipher that was agreed with
        # the other end, and a certificate the broker had to present to get
        # this far.
        assert handshakes, "nothing performed a TLS handshake"
        session = handshakes[0]
        assert session.version() in ("TLSv1.2", "TLSv1.3")
        assert session.cipher() is not None
        presented = session.getpeercert()
        assert presented is not None
        # Issued by the authority we chose to trust, read out of that authority's
        # own certificate rather than written here as a literal. The name a CA
        # happens to carry is a property of whoever generated it — this suite
        # used to hardcode one, which meant the test failed against any other
        # correctly-built authority for a reason that had nothing to do with
        # TLS. What matters is that the broker's issuer is the authority named
        # by ACEMQ_TEST_TLS_CERTIFICATES, whatever it is called.
        authority = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        authority.load_verify_locations(cafile=str(CERTIFICATES / "ca.crt"))
        expected = [
            field
            for cert in authority.get_ca_certs()
            for name in cert["subject"]
            for field in name
        ]
        issuer = [field for name in presented["issuer"] for field in name]
        assert issuer == expected, f"issued by {issuer}, expected {expected}"
    finally:
        await space.cleanup()
        await mq.close()


@needs_a_tls_broker
async def test_a_broker_the_system_trust_store_does_not_vouch_for_is_refused() -> None:
    # No authority named, so the machine's trust store is the one consulted —
    # and it has never heard of the authority that signed this broker. The
    # connection fails rather than falling back to trusting it, which is the
    # single most important thing in this file.
    with pytest.raises(AMQPConnectionError, match="CERTIFICATE_VERIFY_FAILED"):
        await connect(TLS_BROKER, security=Security(credentials=TLS_LOGIN))


@needs_a_tls_broker
async def test_the_wrong_authority_is_refused_as_firmly_as_none_at_all() -> None:
    # An authority that is real, is trusted, and did not sign this broker.
    # Naming one has to mean "this one and no other" or it means nothing.
    settings = Security(
        certificate_authority=CERTIFICATES / "other-ca.crt", credentials=TLS_LOGIN
    )

    with pytest.raises(AMQPConnectionError, match="CERTIFICATE_VERIFY_FAILED"):
        await connect(TLS_BROKER, security=settings)


@needs_a_tls_broker
async def test_the_opt_out_gets_in_where_verification_would_not(
    handshakes: list[ssl.SSLObject],
) -> None:
    # The same broker that was just refused twice, reached by giving up the
    # check. Encrypted, and to whoever answered: which is exactly what
    # without_verifying_the_broker's docstring says it is for and against.
    mq = await connect(
        TLS_BROKER, security=without_verifying_the_broker(credentials=TLS_LOGIN)
    )
    try:
        assert await mq.queue_exists(f"{PREFIX}nothing-of-the-kind") is False
        assert handshakes[0].version() in ("TLSv1.2", "TLSv1.3")
    finally:
        await mq.close()


@needs_a_mutual_tls_broker
async def test_a_client_certificate_gets_in_where_none_at_all_does_not(
    handshakes: list[ssl.SSLObject],
) -> None:
    without = trusting_the_test_authority()
    # This broker will not speak to a client that cannot say who it is, so the
    # handshake ends before there is any AMQP to have an opinion about.
    with pytest.raises(AMQPConnectionError, match="CERTIFICATE"):
        await connect(MUTUAL_TLS_BROKER, security=without)

    handshakes.clear()
    mq = await connect(
        MUTUAL_TLS_BROKER,
        security=trusting_the_test_authority(
            client_certificate=CERTIFICATES / "client.crt",
            client_key=CERTIFICATES / "client.key",
        ),
    )
    space = Workspace(mq)
    try:
        queue = await space.queue("mutual")
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
        assert (await collect(mq, queue))[0].payload == {"id": "7"}
        assert handshakes[0].version() in ("TLSv1.2", "TLSv1.3")
    finally:
        await space.cleanup()
        await mq.close()


@needs_a_mutual_tls_broker
async def test_a_client_certificate_from_an_authority_the_broker_does_not_know_is_refused() -> (
    None
):
    # Presenting *a* certificate is not the same as presenting one the broker
    # accepts, and the difference is the whole of what mutual TLS buys. This
    # one is well-formed, and signed by somebody nobody asked about.
    settings = trusting_the_test_authority(
        client_certificate=CERTIFICATES / "stranger.crt",
        client_key=CERTIFICATES / "stranger.key",
    )

    with pytest.raises(AMQPConnectionError):
        await connect(MUTUAL_TLS_BROKER, security=settings)


async def test_a_password_given_out_of_band_logs_in_and_the_url_never_holds_it() -> None:
    # The point of the whole credentials path: the URL that reaches the logs,
    # the metrics and the process listing carries a host and nothing else.
    url = _without_the_login(BROKER)
    assert "@" not in url

    mq = await connect(url, security=Security(credentials=_login_from(BROKER)))
    space = Workspace(mq)
    try:
        queue = await space.queue("out-of-band")
        await mq.publisher(routing_key=queue, mandatory=True).send({"id": "7"})
        assert (await collect(mq, queue))[0].payload == {"id": "7"}
    finally:
        await space.cleanup()
        await mq.close()


def test_the_blocking_api_takes_the_same_credentials() -> None:
    connection = sync.connect(
        _without_the_login(BROKER), security=Security(credentials=_login_from(BROKER))
    )
    queue = f"{PREFIX}{uuid.uuid4().hex[:8]}.blocking-login"
    try:
        connection.declare(Topology().queue(queue, dead_letter=True))
        assert connection.queue_exists(queue) is True
    finally:
        for name in (queue, dead_letter_queue(queue), parked_queue(queue)):
            with contextlib.suppress(Exception):
                connection.delete_queue(name)
        connection.close()


def _login_from(url: str) -> Credentials:
    """The username and password the test broker's URL carries."""
    parts = urlsplit(url)
    return Credentials(unquote(parts.username or ""), unquote(parts.password or ""))


def _without_the_login(url: str) -> str:
    """The same URL with the userinfo taken off, as a deployment would write it."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))
