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
    Ack,
    Envelope,
    FatalError,
    JsonCodec,
    Message,
    RetryPolicy,
    accept,
    fixed_retry,
    headers,
    no_retry,
    reject,
    retry,
)
from acemq_amqp.codec import Codec
from acemq_amqp.connection import Connection, Handler
from acemq_amqp.topology import Topology

QUEUE = "orders.new"
DLQ = "orders.new.dlq"


@asynccontextmanager
async def running(
    handler: Handler,
    *,
    policy: RetryPolicy | None = None,
    codec: Codec | None = None,
    declare_dead_letter: bool = True,
) -> AsyncIterator[FakeTransport]:
    """A consumer on a fake broker, closed again afterwards."""
    transport = FakeTransport()
    await Topology().queue(QUEUE, dead_letter=declare_dead_letter).apply(transport)
    connection = Connection(transport, retry=policy or no_retry(), codec=codec or JsonCodec())
    await connection.consume(QUEUE, handler)
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
    # Dead-lettered rather than retried: a body that will not decode decodes no
    # better next time, and the attempts left would all be spent the same way.
    assert transport.sent_to(QUEUE) == []
    assert "could not be decoded" in transport.sent_to(DLQ)[0].headers[headers.ERROR]


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
    async with running(
        always(reject(RuntimeError("no"))), declare_dead_letter=False
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
