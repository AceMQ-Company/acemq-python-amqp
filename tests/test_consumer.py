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

"""What a consumer does with each answer a handler can give.

These are the rules that have to match Go and Java rather than merely work:
which failures are worth another attempt, which are not, what the reason says
when a message is given up on, and what the attempt counter reads after a retry.
A broker cannot be asked any of that quickly, so it is asked of a fake here and
confirmed against a real broker in the integration tests.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    METRIC_DEAD_LETTERED,
    METRIC_PARKED,
    METRIC_REJECTED,
    Ack,
    Envelope,
    FatalError,
    JsonCodec,
    Message,
    Metrics,
    Observer,
    RetryPolicy,
    Settlement,
    accept,
    fixed_retry,
    headers,
    no_retry,
    park,
    reject,
    retry,
)
from acemq_amqp.ack import OUTCOME_PARKED
from acemq_amqp.codec import Codec
from acemq_amqp.connection import Connection, Handler
from acemq_amqp.interceptors import ConsumeContext, ConsumeInterceptor, ConsumeNext
from acemq_amqp.telemetry import metric_key
from acemq_amqp.topology import (
    DEAD_LETTER_EXCHANGE,
    RETRY_EXCHANGE,
    Topology,
    rung_args,
)
from acemq_amqp.transport import QueueSpec

QUEUE = "orders.new"
DLQ = "orders.new.dlq"
PARKED = "orders.new.parked"
RUNG = "orders.new.retry.2m"


@asynccontextmanager
async def running(
    handler: Handler,
    *,
    policy: RetryPolicy | None = None,
    codec: Codec | None = None,
    declare_dead_letter: bool = True,
    declare_rungs: bool = True,
    declare: bool = True,
    observer: Observer | None = None,
    interceptor: ConsumeInterceptor | None = None,
) -> AsyncIterator[FakeTransport]:
    """A consumer on a fake broker, closed again afterwards.

    ``declare`` is the consumer's own declaration, which is on by default and
    would otherwise put back whatever ``declare_dead_letter`` and
    ``declare_rungs`` left out. A test about a queue that is missing has to turn
    both halves off.
    """
    transport = FakeTransport()
    policy = policy or no_retry()
    await (
        Topology()
        .queue(
            QUEUE,
            dead_letter=declare_dead_letter,
            retry=policy if declare_rungs else None,
        )
        .apply(transport)
    )
    connection = Connection(
        transport, retry=policy, codec=codec or JsonCodec(), observer=observer
    )
    if interceptor is not None:
        connection.intercept_consume(interceptor)
    await connection.consume(QUEUE, handler, declare=declare)
    try:
        yield transport
    finally:
        await connection.close()


def wire(envelope: Envelope) -> dict[str, Any]:
    return envelope.to_headers(routing_key=QUEUE)


def always(decision: Ack) -> Handler:
    """A handler that gives the same answer whatever it is sent."""

    async def handler(message: Message) -> Ack:
        return decision

    return handler


async def test_accepting_acknowledges_and_publishes_nothing() -> None:
    async def handler(message: Message) -> Ack:
        assert message.payload == {"id": "7"}
        return accept()

    async with running(handler) as transport:
        settlement = await transport.deliver(QUEUE, b'{"id": "7"}', headers=wire(Envelope()))

    assert settlement.acked is True
    assert transport.sent == []


async def test_a_dead_letter_arrives_on_a_broker_no_topology_was_applied_to() -> None:
    # The hole ADR-032 closes, and the reason a consumer declares at start-up.
    # Nothing has been applied here: the queue exists and nothing else does, which
    # is a service deployed without its topology. Before the change the consumer
    # declared nothing, the republish to orders.new.dlq was unroutable, and the
    # message was rejected to a broker with nowhere to send it — gone, quietly.
    transport = FakeTransport()
    await transport.declare_queue(QUEUE, QueueSpec())
    connection = Connection(transport)
    await connection.consume(QUEUE, always(reject(ValueError("no customer"))))
    try:
        settlement = await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))
    finally:
        await connection.close()

    assert DLQ in transport.queues
    assert PARKED in transport.queues
    dead = transport.sent_to(DLQ)
    assert len(dead) == 1
    assert "no customer" in dead[0].headers[headers.ERROR]
    assert settlement.acked is True


async def test_a_consumer_declares_the_rungs_its_own_policy_will_use() -> None:
    # The rungs come from the policy the consumer is running, so a policy passed
    # to consume() rather than to the connection declares its rungs and not the
    # connection's. A rung nobody declared is a long backoff that quietly becomes
    # a held prefetch slot.
    policy = fixed_retry(3, timedelta(minutes=2))
    transport = FakeTransport()
    await transport.declare_queue(QUEUE, QueueSpec())
    connection = Connection(transport)
    await connection.consume(QUEUE, always(accept()), retry=policy)
    await connection.close()

    assert RUNG in transport.queues
    assert dict(transport.queues[RUNG].args) == rung_args(QUEUE, timedelta(minutes=2))
    assert (QUEUE, RETRY_EXCHANGE, QUEUE) in transport.bindings


async def test_a_consumer_with_no_long_waits_declares_no_retry_exchange() -> None:
    # Every wait this policy has is held in the consumer, so there is no rung to
    # dead-letter home and nothing to bind acemq.retry to. Declaring it anyway
    # would leave an exchange on the broker that routes nothing.
    transport = FakeTransport()
    await transport.declare_queue(QUEUE, QueueSpec())
    connection = Connection(transport, retry=fixed_retry(3, timedelta(seconds=1)))
    await connection.consume(QUEUE, always(accept()))
    await connection.close()

    assert DEAD_LETTER_EXCHANGE in transport.exchanges
    assert RETRY_EXCHANGE not in transport.exchanges
    assert [name for name in transport.queues if ".retry." in name] == []


async def test_rejecting_dead_letters_with_the_reason() -> None:
    async def handler(message: Message) -> Ack:
        return reject(ValueError("no customer on this order"))

    async with running(handler) as transport:
        settlement = await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    dead = transport.sent_to(DLQ)
    assert len(dead) == 1
    reason = dead[0].headers[headers.ERROR]
    assert "the handler rejected it" in reason
    assert "ValueError: no customer on this order" in reason
    # Acknowledged, not rejected: the message has already been safely
    # republished, so the original is a copy that has been dealt with.
    assert settlement.acked is True
    assert settlement.nacked is False


async def test_parking_sets_the_message_aside_rather_than_dead_lettering_it() -> None:
    # The distinction the parked queue exists to make. Before a handler could
    # ask for this, one that knew a message was unreadable had to reject it into
    # the dead letters, where it sat among the messages that had merely run out
    # of luck and could only be told apart by hand.
    async def handler(message: Message) -> Ack:
        return park(ValueError("schema version 9, and this service knows up to 4"))

    async with running(handler) as transport:
        settlement = await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    parked = transport.sent_to(PARKED)
    assert len(parked) == 1
    reason = parked[0].headers[headers.ERROR]
    assert "the handler parked it" in reason
    assert "schema version 9" in reason
    # And nowhere near the dead letters.
    assert transport.sent_to(DLQ) == []
    # Acknowledged for the same reason a dead-lettered message is: the original
    # is a copy of something already safely republished.
    assert settlement.acked is True
    assert settlement.nacked is False


async def test_a_parked_message_is_counted_as_parked_and_not_as_dead_lettered() -> None:
    metrics = Metrics()

    async def handler(message: Message) -> Ack:
        return park(ValueError("nothing here is readable"))

    async with running(handler, observer=metrics) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    labels = {"queue": QUEUE}
    # The same counter the engine already used for a body that would not decode.
    # One parked queue, one parked counter, whether the codec or the handler was
    # the one that could not read it.
    assert metrics.counts[metric_key(METRIC_PARKED, labels)] == 1
    assert metric_key(METRIC_DEAD_LETTERED, labels) not in metrics.counts
    assert metric_key(METRIC_REJECTED, labels) not in metrics.counts


async def test_a_parked_message_says_parked_on_its_settlement() -> None:
    # What the span reads, because the tracing adapter takes its outcome from
    # the settlement and never from the handler's Ack.
    seen: list[Settlement] = []

    async def watching(context: ConsumeContext, call_next: ConsumeNext) -> Ack:
        context.when_settled(seen.append)
        return await call_next(context)

    async def handler(message: Message) -> Ack:
        return park(ValueError("unreadable"))

    async with running(handler, interceptor=watching) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    assert [settlement.outcome for settlement in seen] == [OUTCOME_PARKED]
    # Parked is not dead-lettered, and the property that decides which queue an
    # operator goes looking in has to agree.
    assert seen[0].dead_lettered is False
    assert seen[0].parked is True


async def test_retrying_with_no_policy_dead_letters_and_says_why() -> None:
    # The default is one delivery and no second chance. A handler that asks for
    # a retry on a connection with no policy should find out immediately, in the
    # reason, rather than discover the message was quietly requeued forever.
    async with running(always(retry(RuntimeError("the database is down")))) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    reason = transport.sent_to(DLQ)[0].headers[headers.ERROR]
    assert "exhausted 1 attempt" in reason
    assert "RuntimeError: the database is down" in reason


async def test_a_retry_puts_the_message_back_one_attempt_further_on() -> None:
    policy = fixed_retry(3, timedelta(0))
    async with running(always(retry(RuntimeError("not yet"))), policy=policy) as transport:
        settlement = await transport.deliver(
            QUEUE, b'{"id": "7"}', headers=wire(Envelope(id="abc"))
        )

    again = transport.sent_to(QUEUE)
    assert len(again) == 1
    assert again[0].headers[headers.ATTEMPT] == 2
    assert again[0].headers[headers.ID] == "abc"
    # The bytes go back exactly as they came: re-encoding through a class that
    # has since changed would replace what was committed with something else.
    assert again[0].message.body == b'{"id": "7"}'
    assert settlement.acked is True
    assert transport.sent_to(DLQ) == []


async def test_a_long_wait_is_handed_to_the_broker_rather_than_slept_through() -> None:
    # Two minutes is longer than a consumer should hold an unacknowledged
    # message: a restart in the middle of that wait would lose all of it.
    policy = fixed_retry(3, timedelta(minutes=2))
    async with running(always(retry(RuntimeError("not yet"))), policy=policy) as transport:
        settlement = await transport.deliver(
            QUEUE, b'{"id": "7"}', headers=wire(Envelope(id="abc"))
        )

    rung = transport.sent_to(RUNG)
    assert len(rung) == 1
    # The attempt advances on a rung publish exactly as it does on an immediate
    # retry: the counter belongs to the message, not to the path it took.
    assert rung[0].headers[headers.ATTEMPT] == 2
    assert rung[0].headers[headers.ID] == "abc"
    assert rung[0].message.body == b'{"id": "7"}'
    assert transport.sent_to(QUEUE) == []
    assert settlement.acked is True


async def test_a_short_wait_still_goes_straight_back_to_the_queue() -> None:
    policy = fixed_retry(3, timedelta(milliseconds=1))
    async with running(always(retry(RuntimeError("not yet"))), policy=policy) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    assert transport.sent_to(QUEUE)[0].headers[headers.ATTEMPT] == 2
    assert [sent for sent in transport.sent if ".retry." in sent.routing_key] == []


async def test_a_missing_rung_is_waited_out_here_rather_than_losing_the_message() -> None:
    # A rung that is not there is a mistake, not a reason to drop a message. The
    # wait degrades to what this library did before there were rungs — held here,
    # loudly — and the message still arrives. The delay is a millisecond so the
    # test does not have to wait out a real one.
    #
    # It takes both halves off to arrange, because a consumer declares its own
    # rungs now: this is the consumer that was told not to, reading a queue whose
    # topology has none either.
    policy = fixed_retry(3, timedelta(milliseconds=1)).wait_in_broker_from(
        timedelta(milliseconds=1)
    )
    async with running(
        always(retry(RuntimeError("not yet"))),
        policy=policy,
        declare_rungs=False,
        declare=False,
    ) as transport:
        settlement = await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    assert transport.sent_to(QUEUE)[0].headers[headers.ATTEMPT] == 2
    assert settlement.acked is True


async def test_the_last_attempt_dead_letters_rather_than_retrying_again() -> None:
    policy = fixed_retry(3, timedelta(0))
    async with running(always(retry(RuntimeError("still down"))), policy=policy) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope(attempt=3)))

    assert transport.sent_to(QUEUE) == []
    assert "exhausted 3 attempts" in transport.sent_to(DLQ)[0].headers[headers.ERROR]


async def test_a_fatal_error_skips_the_attempts_that_are_left() -> None:
    # The handler asked for a retry but marked the reason as one that will not
    # change. Honouring the mark rather than the request is the point of it.
    policy = fixed_retry(5, timedelta(0))
    handler = always(retry(FatalError("this order has no id")))
    async with running(handler, policy=policy) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope(attempt=1)))

    assert transport.sent_to(QUEUE) == []
    reason = transport.sent_to(DLQ)[0].headers[headers.ERROR]
    assert "unprocessable" in reason
    assert "this order has no id" in reason


async def test_giving_up_on_age_says_so_rather_than_blaming_the_attempts() -> None:
    policy = RetryPolicy(
        max_attempts=10, initial_delay=timedelta(0), max_message_age=timedelta(seconds=1)
    )
    old = Envelope(first_seen=datetime.now(timezone.utc) - timedelta(hours=4))

    async with running(always(retry(RuntimeError("no"))), policy=policy) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(old))

    reason = transport.sent_to(DLQ)[0].headers[headers.ERROR]
    assert "exceeded the maximum message age" in reason


async def test_an_exception_out_of_a_handler_asks_for_the_retry_policy() -> None:
    # An exception is how Python says a thing failed, so a handler that raises
    # is asking for another attempt rather than confessing a bug.
    async def handler(message: Message) -> Ack:
        raise RuntimeError("the database is down")

    policy = fixed_retry(3, timedelta(0))
    async with running(handler, policy=policy) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    assert transport.sent_to(QUEUE)[0].headers[headers.ATTEMPT] == 2


async def test_a_fatal_error_raised_by_a_handler_stops_there() -> None:
    async def handler(message: Message) -> Ack:
        raise FatalError("this order has no customer")

    policy = fixed_retry(3, timedelta(0))
    async with running(handler, policy=policy) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    assert transport.sent_to(QUEUE) == []
    assert "unprocessable" in transport.sent_to(DLQ)[0].headers[headers.ERROR]


async def test_a_handler_that_forgets_to_decide_is_caught_immediately() -> None:
    async def handler(message: Message) -> Any:
        return None

    async with running(handler) as transport:
        settlement = await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    reason = transport.sent_to(DLQ)[0].headers[headers.ERROR]
    assert "returned NoneType instead of an Ack" in reason
    assert settlement.acked is True


async def test_a_body_that_will_not_decode_never_reaches_the_handler() -> None:
    seen: list[Message] = []

    async def handler(message: Message) -> Ack:
        seen.append(message)
        return accept()

    policy = fixed_retry(5, timedelta(0))
    async with running(handler, policy=policy) as transport:
        await transport.deliver(QUEUE, b"{not json", headers=wire(Envelope()))

    assert seen == []
    # Not retried: a body that will not decode decodes no better next time, and
    # the attempts left would all be spent the same way.
    assert transport.sent_to(QUEUE) == []
    # Parked, not dead-lettered. A message that failed five times and a message
    # nothing could read are different problems with different answers, and
    # whoever drains the dead letters should not have to sort them by hand.
    assert transport.sent_to(DLQ) == []
    assert "could not be decoded" in transport.sent_to(PARKED)[0].headers[headers.ERROR]


async def test_a_handler_sees_the_envelope_that_travelled_with_the_message() -> None:
    seen: list[Message] = []

    async def handler(message: Message) -> Ack:
        seen.append(message)
        return accept()

    envelope = Envelope(
        id="order-1",
        type="order.placed.v2",
        version=3,
        causation_id="cart-9",
        origin="checkout@pod-7",
        attempt=2,
        headers={"tenant": "acme"},
    )
    async with running(handler) as transport:
        await transport.deliver(QUEUE, b"{}", headers=wire(envelope))

    read = seen[0].envelope
    assert read.id == "order-1"
    assert read.type == "order.placed.v2"
    assert read.version == 3
    assert read.correlation_id == "order-1"
    assert read.causation_id == "cart-9"
    assert read.origin == "checkout@pod-7"
    assert read.attempt == 2
    assert read.headers == {"tenant": "acme"}


async def test_a_message_that_cannot_be_dead_lettered_is_left_with_the_broker() -> None:
    # Without a dead-letter queue to put it in, the broker's own dead-lettering
    # is the last thing between this message and nothing. Acknowledging it here
    # would be dropping it.
    #
    # Reaching this at all now takes a consumer that was told not to declare, on
    # a queue whose topology has no dead-letter half either: an ordinary consumer
    # declares the queue this message could not reach, which is the point of
    # declaring at start-up.
    async with running(
        always(reject(RuntimeError("no"))), declare_dead_letter=False, declare=False
    ) as transport:
        settlement = await transport.deliver(QUEUE, b"{}", headers=wire(Envelope()))

    assert settlement.acked is False
    assert settlement.nacked is True
    assert settlement.requeued is False


async def test_the_dead_lettered_message_keeps_its_identity() -> None:
    envelope = Envelope(id="order-1", type="order.placed.v2", causation_id="cart-9")

    async with running(always(reject(RuntimeError("no")))) as transport:
        await transport.deliver(QUEUE, b'{"id":"7"}', headers=wire(envelope))

    dead = transport.sent_to(DLQ)[0]
    assert dead.headers[headers.ID] == "order-1"
    assert dead.headers[headers.TYPE] == "order.placed.v2"
    assert dead.headers[headers.CAUSATION] == "cart-9"
    assert dead.message.body == b'{"id":"7"}'
    assert dead.message.mandatory is True


async def test_a_consumer_needs_at_least_one_worker() -> None:
    transport = FakeTransport()
    connection = Connection(transport)
    with pytest.raises(ValueError, match="at least 1"):
        await connection.consume(QUEUE, always(accept()), concurrency=0)
