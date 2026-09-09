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
    SlipForm,
    follow_slip,
    route_of,
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


# --------------------------------------------------------------------------
# The other wire form: a route declared in advance, which is what Java writes


#: A message as Java's Pipeline publishes one: the step names, the position, the
#: run identifier, and nothing at all about where those steps live.
def java_shaped(
    route: str = "validate,enrich,dispatch",
    position: int = 1,
    *,
    run: str = "run-7",
    identifier: str = "order-1",
) -> Message:
    raw: dict[str, Any] = {
        headers.ID: identifier,
        headers.TYPE: "order.placed",
        headers.CORRELATION: "cart-9",
        headers.ROUTE: route,
        headers.ROUTE_POSITION: position,
        headers.ROUTE_ID: run,
    }
    return Message(
        payload={"order": identifier},
        envelope=Envelope.from_headers(raw, "enrich"),
        routing_key="enrich",
        content_type="application/json",
        redelivered=False,
        body=b"{}",
    )


def test_a_java_route_reads_as_a_slip() -> None:
    # The steps before the position are done and the rest are still to come,
    # which is the shape the JSON slip has — so everything downstream works on
    # either form without knowing which it was handed.
    read = slip_from(java_shaped().envelope, pipeline="fulfilment")

    assert read is not None
    assert read.form is SlipForm.STEP_NAMES
    assert [step.name for step in read.done] == ["validate"]
    assert [step.name for step in read.steps] == ["enrich", "dispatch"]
    assert read.run_id == "run-7"
    # The exchange is the pipeline's own name and the routing key is the step's,
    # which is how Java resolves a step: the queue behind it is
    # ``fulfilment.enrich``.
    assert read.next is not None
    assert (read.next.exchange, read.next.routing_key) == ("fulfilment", "enrich")


def test_a_route_position_past_the_end_is_a_finished_route_rather_than_an_error() -> None:
    # Which is how Java reads it. A step that raised here would turn the end of
    # a route into a dead letter.
    read = slip_from(java_shaped(position=9).envelope, pipeline="fulfilment")

    assert read is not None
    assert read.finished is True


def test_a_route_position_that_will_not_read_starts_the_route_over() -> None:
    # Rather than sending the message to whichever step happened to be first in
    # the list, which is what an unreadable position would otherwise mean.
    raw = {headers.ROUTE: "validate,enrich", headers.ROUTE_POSITION: "not a number"}
    read = slip_from(Envelope.from_headers(raw), pipeline="fulfilment")

    assert read is not None
    assert [step.name for step in read.steps] == ["validate", "enrich"]
    assert read.done == ()


def test_a_message_carrying_both_forms_is_read_as_the_one_that_says_where_it_goes() -> None:
    # The JSON slip carries an exchange and a routing key per step and the
    # declared one has to be resolved against a pipeline the reader may not have
    # been told about, so the self-describing one wins.
    envelope = arriving(ITINERARY).envelope.with_(route="validate,enrich", route_position=0)

    read = slip_from(envelope, pipeline="fulfilment")

    assert read is not None
    assert read.form is SlipForm.JSON
    assert [step.name for step in read.steps] == ["validate", "charge", "ship"]


async def test_a_java_shaped_message_is_followed_to_the_next_step() -> None:
    """The point of reading the other form at all.

    A Python step in the middle of a Java-declared pipeline: it reads the route
    off the headers Java wrote, does its work, and publishes to the pipeline's
    exchange under the next step's name — which is the queue the next Java step
    is consuming.
    """
    transport = FakeTransport()
    mq = Connection(transport, origin="enrichment@pod-3")

    async def enrich(message: Message) -> dict[str, Any]:
        return {**message.payload, "enriched": True}

    decision = await follow_slip(mq, enrich, pipeline="fulfilment")(java_shaped())

    assert decision.action.value == "accept"
    sent = transport.sent[0]
    assert (sent.exchange, sent.routing_key) == ("fulfilment", "dispatch")
    assert json.loads(sent.message.body) == {"order": "order-1", "enriched": True}

    # Written back in the form it arrived in. A JSON slip here would hand the
    # next Java step a message with no route on it at all.
    assert sent.headers[headers.ROUTE] == "validate,enrich,dispatch"
    assert sent.headers[headers.ROUTE_POSITION] == 2
    assert HEADER_ROUTING_SLIP not in sent.headers
    # And the run is the same run, which is what joins the hops up afterwards.
    assert sent.headers[headers.ROUTE_ID] == "run-7"
    assert sent.headers[headers.CORRELATION] == "cart-9"
    assert sent.headers[headers.CAUSATION] == "order-1"


async def test_the_last_step_of_a_java_route_publishes_nothing() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async def dispatch(message: Message) -> Any:
        return message.payload

    decision = await follow_slip(mq, dispatch, pipeline="fulfilment")(
        java_shaped(position=2)
    )

    assert decision.action.value == "accept"
    assert transport.sent == []


async def test_a_java_shaped_message_read_back_is_where_it_was_left() -> None:
    # The round trip that matters: what this library writes, this library reads,
    # and the position it reads is the step the message was published to.
    transport = FakeTransport()
    mq = Connection(transport)

    async def enrich(message: Message) -> Any:
        return message.payload

    await follow_slip(mq, enrich, pipeline="fulfilment")(java_shaped())

    onwards = Envelope.from_headers(transport.sent[0].headers, "dispatch")
    read = slip_from(onwards, pipeline="fulfilment")

    assert read is not None
    assert read.next is not None
    assert read.next.name == "dispatch"
    assert [step.name for step in read.done] == ["validate", "enrich"]


async def test_a_declared_route_can_be_started_from_here() -> None:
    transport = FakeTransport()
    mq = Connection(transport, origin="checkout@pod-7")

    await start(mq, route_of("fulfilment", "validate", "enrich", "dispatch"), {"id": "1"})

    sent = transport.sent[0]
    assert (sent.exchange, sent.routing_key) == ("fulfilment", "validate")
    assert sent.headers[headers.ROUTE] == "validate,enrich,dispatch"
    # Written even though it is zero: a first hop whose position is missing is
    # the only hop whose slip is incomplete, and a reader would have to guess.
    assert sent.headers[headers.ROUTE_POSITION] == 0
    assert sent.headers[headers.ROUTE_ID] != ""


def test_a_declared_route_needs_at_least_one_step() -> None:
    with pytest.raises(ValueError, match="at least one step"):
        route_of("fulfilment")


async def test_a_json_slip_can_be_asked_to_go_out_as_a_declared_route() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    itinerary = (
        RoutingSlip()
        .then("fulfilment", "validate", name="validate")
        .then("fulfilment", "enrich", name="enrich")
    )

    await start(mq, itinerary, {"id": "1"}, form=SlipForm.STEP_NAMES)

    sent = transport.sent[0]
    assert sent.headers[headers.ROUTE] == "validate,enrich"
    assert HEADER_ROUTING_SLIP not in sent.headers


def test_a_route_across_two_exchanges_cannot_be_written_as_step_names() -> None:
    # The declared form has one exchange for the whole route, so this slip means
    # something the wire form cannot say. Refused where it was built rather than
    # silently sending the second step somewhere else.
    both = (
        RoutingSlip()
        .then("orders-events", "validate", name="validate")
        .then("shipping-events", "ship", name="ship")
    )

    with pytest.raises(ValueError, match="span"):
        both.written_as(SlipForm.STEP_NAMES)


def test_a_step_whose_name_and_routing_key_disagree_cannot_be_written_either() -> None:
    # A declared route has one word for both, and picking one silently is how a
    # message ends up on a queue nobody expected.
    named = RoutingSlip().then("fulfilment", "order.charge", name="charge")

    with pytest.raises(ValueError, match="one word for both"):
        named.written_as(SlipForm.STEP_NAMES)


def test_switching_to_the_declared_form_gives_the_run_an_identifier() -> None:
    # That form has a header for one, and a run with no identifier cannot be
    # followed across its hops.
    switched = ITINERARY.written_as(SlipForm.JSON)
    assert switched.run_id == ""

    declared = (
        RoutingSlip().then("fulfilment", "validate", name="validate")
    ).written_as(SlipForm.STEP_NAMES)
    assert declared.run_id != ""


def test_advancing_keeps_the_form_and_the_run() -> None:
    advanced = route_of("fulfilment", "validate", "enrich", run_id="run-7").advance()

    assert advanced.form is SlipForm.STEP_NAMES
    assert advanced.run_id == "run-7"
