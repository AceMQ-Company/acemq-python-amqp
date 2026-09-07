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

"""Wrapping a handler in what every handler needs, and joining them up."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    Ack,
    AsyncHandler,
    Connection,
    Envelope,
    Handler,
    Message,
    accept,
    headers,
    reject,
)
from acemq_amqp.patterns import (
    NOTHING,
    InMemoryIdempotencyStore,
    Middleware,
    by_header,
    chain,
    then,
    with_idempotency,
    with_logging,
    with_ordering,
    with_timeout,
)
from acemq_amqp.patterns._support import decide


def message(identifier: str = "order-1", **application: Any) -> Message:
    return Message(
        payload={"order": identifier},
        envelope=Envelope(
            id=identifier, type="order.placed", correlation_id="cart-9", headers=application
        ),
        routing_key="order.placed",
        content_type="application/json",
        redelivered=False,
        body=b"{}",
    )


def noting(what: str, seen: list[str]) -> Middleware:
    """Middleware that only records that it was reached."""

    def wrap(handler: Handler) -> AsyncHandler:
        async def recorded(incoming: Message) -> Ack:
            seen.append(f"{what} in")
            decision = await decide(handler, incoming)
            seen.append(f"{what} out")
            return decision

        return recorded

    return wrap


async def test_the_first_middleware_named_is_the_outermost() -> None:
    # So logging placed first records what the deadline and the guard decided,
    # which is the order somebody reading the list expects.
    seen: list[str] = []

    async def handler(incoming: Message) -> Ack:
        seen.append("handler")
        return accept()

    await chain(handler, noting("outer", seen), noting("inner", seen))(message())

    assert seen == ["outer in", "inner in", "handler", "inner out", "outer out"]


async def test_a_chain_with_no_middleware_is_the_handler() -> None:
    def handler(incoming: Message) -> Ack:
        return accept()

    assert await chain(handler)(message()) == accept()


async def test_a_handler_that_overruns_is_retried_whatever_it_would_have_said() -> None:
    # Between retrying work that may have succeeded and accepting work that may
    # have failed: duplicates are a problem that can be solved, and a lost
    # message is not.
    finished = False

    async def slow(incoming: Message) -> Ack:
        nonlocal finished
        await asyncio.sleep(5)
        finished = True
        return accept()

    decision = await chain(slow, with_timeout(timedelta(milliseconds=20)))(message())

    assert decision.action.value == "retry"
    assert "did not finish within" in str(decision.error)
    # And an async handler really is stopped, rather than left running behind a
    # message that has already been settled.
    assert finished is False


async def test_a_handler_that_finishes_in_time_keeps_its_own_answer() -> None:
    async def quick(incoming: Message) -> Ack:
        return reject(ValueError("no customer"))

    decision = await chain(quick, with_timeout(timedelta(seconds=5)))(message())

    assert decision.action.value == "reject"


def test_a_timeout_has_to_be_a_length_of_time() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        with_timeout(timedelta(0))


async def test_logging_records_the_decision_and_not_the_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(incoming: Message) -> Ack:
        return reject(ValueError("no customer"))

    with caplog.at_level(logging.INFO, logger="acemq"):
        await chain(handler, with_logging())(message())

    written = caplog.text
    assert "order-1" in written
    assert "type=order.placed" in written
    assert "attempt=1" in written
    assert "reject" in written
    # The payload is the one thing on a message that may not be safe to write
    # down, and a log line unsafe to keep is one somebody eventually turns off.
    assert "cart-9" not in written


async def test_logging_does_not_swallow_an_exception_on_its_way_past(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The consumer above turns an exception into the retry policy, and catching
    # it here would quietly take that decision away.
    async def handler(incoming: Message) -> Ack:
        raise ConnectionError("the database is down")

    with caplog.at_level(logging.INFO, logger="acemq"), pytest.raises(ConnectionError):
        await chain(handler, with_logging())(message())

    assert "raised" in caplog.text


async def test_the_guards_compose_the_way_they_read() -> None:
    ran: list[str] = []

    async def handler(incoming: Message) -> Ack:
        ran.append(incoming.envelope.id)
        return accept()

    handling = chain(
        handler,
        with_logging(),
        with_ordering(by_header("order-id")),
        with_idempotency(InMemoryIdempotencyStore()),
    )

    await handling(message("order-1", **{"order-id": "A-1"}))
    await handling(message("order-1", **{"order-id": "A-1"}))

    assert ran == ["order-1"]


async def test_a_step_publishes_what_it_produced_and_says_what_caused_it() -> None:
    transport = FakeTransport()
    mq = Connection(transport, origin="orders@pod-1")

    async def ship(incoming: Message) -> Any:
        return {"shipment": incoming.payload["order"]}

    decision = await then(mq.publisher("shipping-events", "shipment.requested"), ship)(
        message()
    )

    assert decision == accept()
    sent = transport.sent[0]
    assert (sent.exchange, sent.routing_key) == ("shipping-events", "shipment.requested")
    # The correlation runs the length of the pipeline, so afterwards the whole
    # run can be reconstructed from any message in it.
    assert sent.headers[headers.CORRELATION] == "cart-9"
    assert sent.headers[headers.CAUSATION] == "order-1"
    assert sent.headers[headers.ORIGIN] == "orders@pod-1"


async def test_a_step_with_nothing_to_send_on_publishes_nothing() -> None:
    # Rather than inventing an empty message for the next service to handle.
    transport = FakeTransport()
    mq = Connection(transport)

    async def ship(incoming: Message) -> Any:
        return NOTHING

    decision = await then(mq.publisher("shipping-events", "shipment.requested"), ship)(
        message()
    )

    assert decision == accept()
    assert transport.sent == []


async def test_the_next_message_failing_to_go_out_retries_the_step() -> None:
    class Refusing(FakeTransport):
        async def publish(self, *args: Any, **kwargs: Any) -> Any:
            raise ConnectionError("the broker is not answering")

    mq = Connection(Refusing())

    async def ship(incoming: Message) -> Any:
        return {"shipment": "s-1"}

    decision = await then(mq.publisher("shipping-events", "shipment.requested"), ship)(
        message()
    )

    assert decision.action.value == "retry"
    assert "the next message did not go out" in str(decision.error)


def test_nothing_reads_as_nothing() -> None:
    assert repr(NOTHING) == "NOTHING"
