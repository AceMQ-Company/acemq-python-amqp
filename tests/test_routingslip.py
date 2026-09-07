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

"""An itinerary the message carries, instead of an orchestrator that knows it."""

from __future__ import annotations

import json
from typing import Any

import pytest
from fake_transport import FakeTransport

from acemq_amqp import Connection, Envelope, FatalError, Message, headers
from acemq_amqp.patterns import (
    HEADER_ROUTING_SLIP,
    RoutingSlip,
    follow_slip,
    slip_from,
    start,
)

ITINERARY = (
    RoutingSlip()
    .then("orders-events", "order.validate", name="validate")
    .then("orders-events", "order.charge", name="charge")
    .then("orders-events", "order.ship", name="ship")
)


def arriving(slip: RoutingSlip | None, *, identifier: str = "order-1") -> Message:
    application = {} if slip is None else {HEADER_ROUTING_SLIP: slip.to_header()}
    return Message(
        payload={"order": identifier},
        envelope=Envelope(id=identifier, correlation_id="cart-9", headers=application),
        routing_key="order.charge",
        content_type="application/json",
        redelivered=False,
        body=b"{}",
    )


def test_a_slip_reads_as_where_it_has_been_and_where_it_is_going() -> None:
    assert str(ITINERARY) == "RoutingSlip[done:  | next: validate -> charge -> ship]"
    assert ITINERARY.next is not None
    assert ITINERARY.next.name == "validate"
    assert ITINERARY.finished is False


def test_advancing_leaves_the_original_alone() -> None:
    # A slip is on a message that has already been published by the time anybody
    # reads it, so changing one in place would describe a journey that did not
    # happen.
    advanced = ITINERARY.advance()

    assert len(ITINERARY.steps) == 3
    assert len(advanced.steps) == 2
    assert [step.name for step in advanced.done] == ["validate"]
    assert advanced.done[0].completed_at != ""


def test_a_finished_slip_says_so_and_advancing_it_changes_nothing() -> None:
    finished = ITINERARY.advance().advance().advance()

    assert finished.finished is True
    assert finished.next is None
    assert finished.advance() == finished


def test_the_wire_form_is_the_one_four_other_languages_read() -> None:
    written = json.loads(ITINERARY.advance().to_header())

    # camelCase, because the wire is where the languages have to agree and it is
    # not Python's to name.
    assert written["steps"][0] == {
        "exchange": "orders-events",
        "routingKey": "order.charge",
        "name": "charge",
    }
    assert written["done"][0]["name"] == "validate"
    assert "completedAt" in written["done"][0]


def test_a_slip_survives_the_round_trip_through_a_header() -> None:
    read = slip_from(arriving(ITINERARY.advance()).envelope)

    assert read is not None
    assert [step.name for step in read.steps] == ["charge", "ship"]
    assert [step.name for step in read.done] == ["validate"]


def test_a_message_with_no_slip_reads_as_no_slip() -> None:
    assert slip_from(Envelope()) is None


def test_a_slip_that_will_not_parse_is_fatal_rather_than_retried() -> None:
    # It will not parse on the next attempt either, and spending five retries on
    # it only delays the person who has to look at it.
    envelope = Envelope(headers={HEADER_ROUTING_SLIP: "{not json"})
    with pytest.raises(FatalError, match="cannot read the routing slip"):
        slip_from(envelope)


async def test_starting_sends_the_payload_to_the_first_stop() -> None:
    transport = FakeTransport()
    mq = Connection(transport, origin="checkout@pod-7")

    await start(mq, ITINERARY, {"order": "order-1"})

    assert len(transport.sent) == 1
    sent = transport.sent[0]
    assert (sent.exchange, sent.routing_key) == ("orders-events", "order.validate")
    assert sent.headers[headers.ORIGIN] == "checkout@pod-7"
    carried = json.loads(sent.headers[HEADER_ROUTING_SLIP])
    assert [step["name"] for step in carried["steps"]] == ["validate", "charge", "ship"]


async def test_a_slip_with_no_steps_has_nowhere_to_start() -> None:
    mq = Connection(FakeTransport())
    with pytest.raises(ValueError, match="no steps in it"):
        await start(mq, RoutingSlip(), {"order": "order-1"})


async def test_a_step_sends_the_message_on_to_the_next_stop() -> None:
    transport = FakeTransport()
    mq = Connection(transport, origin="billing@pod-2")

    async def charge(message: Message) -> dict[str, Any]:
        return {**message.payload, "charged": True}

    decision = await follow_slip(mq, charge)(arriving(ITINERARY.advance()))

    assert decision.action.value == "accept"
    sent = transport.sent[0]
    assert (sent.exchange, sent.routing_key) == ("orders-events", "order.ship")
    assert json.loads(sent.message.body) == {"order": "order-1", "charged": True}
    # The correlation runs the length of the itinerary, and each hop records
    # what caused it.
    assert sent.headers[headers.CORRELATION] == "cart-9"
    assert sent.headers[headers.CAUSATION] == "order-1"
    assert sent.headers[headers.ORIGIN] == "billing@pod-2"

    carried = json.loads(sent.headers[HEADER_ROUTING_SLIP])
    assert [step["name"] for step in carried["steps"]] == ["ship"]
    assert [step["name"] for step in carried["done"]] == ["validate", "charge"]


async def test_the_last_step_publishes_nothing_and_the_work_is_done() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async def ship(message: Message) -> Any:
        return message.payload

    finished = ITINERARY.advance().advance()
    decision = await follow_slip(mq, ship)(arriving(finished))

    assert decision.action.value == "accept"
    assert transport.sent == []


async def test_a_message_with_no_slip_has_nowhere_to_go_next() -> None:
    mq = Connection(FakeTransport())

    async def charge(message: Message) -> dict[str, Any]:
        raise AssertionError("the step should not have been reached")

    decision = await follow_slip(mq, charge)(arriving(None))

    assert decision.action.value == "reject"
    assert "no routing slip" in str(decision.error)


async def test_a_step_that_fails_asks_for_the_retry_policy() -> None:
    mq = Connection(FakeTransport())

    async def charge(message: Message) -> dict[str, Any]:
        raise ConnectionError("the payment gateway is down")

    decision = await follow_slip(mq, charge)(arriving(ITINERARY))

    assert decision.action.value == "retry"
    assert "payment gateway" in str(decision.error)


async def test_a_step_that_can_never_succeed_says_so() -> None:
    mq = Connection(FakeTransport())

    async def charge(message: Message) -> dict[str, Any]:
        raise FatalError("this order has no payment method")

    decision = await follow_slip(mq, charge)(arriving(ITINERARY))

    assert decision.action.value == "reject"


async def test_the_next_message_failing_to_go_out_retries_this_step() -> None:
    # Which is why a step that changes anything should be idempotent: the work
    # runs again on the next attempt.
    class Refusing(FakeTransport):
        async def publish(self, *args: Any, **kwargs: Any) -> Any:
            raise ConnectionError("the broker is not answering")

    mq = Connection(Refusing())

    async def charge(message: Message) -> Any:
        return message.payload

    decision = await follow_slip(mq, charge)(arriving(ITINERARY))

    assert decision.action.value == "retry"
    assert "the next step did not go out" in str(decision.error)
