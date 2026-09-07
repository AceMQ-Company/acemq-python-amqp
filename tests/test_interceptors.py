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

"""What an interceptor can see, change and refuse.

The seam exists so that the things every message in an organisation needs — a
tenant, a trace context, a timer, a policy — are written once instead of at
every call site. These are the rules that make that safe: what it sees, when it
sees it, what changing it changes, and what happens when it says no.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    Ack,
    Connection,
    ConsumeContext,
    ConsumeInterceptor,
    ConsumeNext,
    Envelope,
    Message,
    PublishContext,
    PublishInterceptor,
    PublishNext,
    PublishResult,
    accept,
    headers,
    reject,
)
from acemq_amqp.connection import Handler
from acemq_amqp.topology import Topology

QUEUE = "orders.new"
DLQ = "orders.new.dlq"


@asynccontextmanager
async def consuming(
    handler: Handler, *interceptors: ConsumeInterceptor
) -> AsyncIterator[FakeTransport]:
    """A consumer whose handler is wrapped in these, closed again afterwards."""
    transport = FakeTransport()
    await Topology().queue(QUEUE, dead_letter=True).apply(transport)
    connection = Connection(transport)
    for interceptor in interceptors:
        connection.intercept_consume(interceptor)
    await connection.consume(QUEUE, handler)
    try:
        yield transport
    finally:
        await connection.close()


def wire() -> dict[str, object]:
    return Envelope(id="m-1", type="order.placed").to_headers(routing_key=QUEUE)


async def test_a_publish_interceptor_sees_the_message_before_it_is_encoded() -> None:
    # Before, and not after: an interceptor handed bytes has nothing useful it
    # can do to them, and changing the message is half of why the seam exists.
    seen: list[PublishContext] = []

    async def watching(context: PublishContext, send: PublishNext) -> PublishResult:
        seen.append(context)
        return await send(context)

    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport).intercept_publish(watching)

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    assert len(seen) == 1
    assert seen[0].exchange == ""
    assert seen[0].routing_key == QUEUE
    assert seen[0].payload == {"id": "1"}
    # The envelope as it stands, not as it will be rendered: the type still
    # falls back to the routing key later, so an interceptor that wants to set
    # one is looking at an empty field rather than at a decision already made.
    assert seen[0].envelope.type == ""


async def test_a_publish_interceptor_can_change_the_payload_and_the_envelope() -> None:
    async def stamping(context: PublishContext, send: PublishNext) -> PublishResult:
        context.set_header("tenant", "acme")
        context.payload = {**context.payload, "stamped": True}
        return await send(context)

    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport).intercept_publish(stamping)

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    sent = transport.sent_to(QUEUE)[0]
    assert sent.headers["tenant"] == "acme"
    assert b'"stamped": true' in sent.message.body


async def test_a_publish_interceptor_can_redirect_a_message() -> None:
    async def elsewhere(context: PublishContext, send: PublishNext) -> PublishResult:
        context.routing_key = "orders.quarantine"
        return await send(context)

    transport = FakeTransport()
    await Topology().queue(QUEUE).queue("orders.quarantine").apply(transport)
    mq = Connection(transport).intercept_publish(elsewhere)

    await mq.publisher(routing_key=QUEUE, mandatory=True).send({"id": "1"})

    assert transport.sent_to(QUEUE) == []
    assert len(transport.sent_to("orders.quarantine")) == 1


async def test_refusing_a_publish_stops_it_and_tells_the_caller() -> None:
    # The whole point of intercepting rather than observing: a message that must
    # not go out is stopped in one place rather than in every publisher, and the
    # caller hears about it rather than believing it was sent.
    async def refusing(context: PublishContext, send: PublishNext) -> PublishResult:
        raise PermissionError("no tenant on this message")

    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport).intercept_publish(refusing)

    with pytest.raises(PermissionError, match="no tenant"):
        await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    assert transport.sent == []


async def test_publish_interceptors_nest_in_the_order_they_were_registered() -> None:
    order: list[str] = []

    def named(name: str) -> PublishInterceptor:
        async def step(context: PublishContext, send: PublishNext) -> PublishResult:
            order.append(f"{name} in")
            try:
                return await send(context)
            finally:
                order.append(f"{name} out")

        return step

    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport)
    mq.intercept_publish(named("first")).intercept_publish(named("second"))

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    # The first registered is outermost, so it is first in and last out. That is
    # what makes a pair which opens something and closes it nest properly, and
    # it is the reason try/finally is the shape rather than two hooks.
    assert order == ["first in", "second in", "second out", "first out"]


async def test_an_interceptor_registered_late_applies_to_a_publisher_that_exists() -> None:
    # Read at publish time rather than copied into each publisher, so a
    # framework wiring one up after the connection is made is not too late.
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport)
    publisher = mq.publisher(routing_key=QUEUE)

    async def stamping(context: PublishContext, send: PublishNext) -> PublishResult:
        context.set_header("tenant", "acme")
        return await send(context)

    mq.intercept_publish(stamping)
    await publisher.send({"id": "1"})

    assert transport.sent_to(QUEUE)[0].headers["tenant"] == "acme"


async def test_a_consume_interceptor_wraps_the_handler() -> None:
    around: list[str] = []
    handled: list[Message] = []

    async def timing(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        around.append(f"before {context.queue}")
        try:
            return await handle(context)
        finally:
            around.append("after")

    async def handler(message: Message) -> Ack:
        handled.append(message)
        return accept()

    async with consuming(handler, timing) as transport:
        settlement = await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert settlement.acked is True
    assert around == [f"before {QUEUE}", "after"]
    assert len(handled) == 1


async def test_a_consume_interceptor_can_change_what_the_handler_is_given() -> None:
    handled: list[Message] = []

    async def enriching(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        context.payload = {**context.payload, "tenant": "acme"}
        context.envelope = context.envelope.with_(origin="rewritten")
        return await handle(context)

    async def handler(message: Message) -> Ack:
        handled.append(message)
        return accept()

    async with consuming(handler, enriching) as transport:
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert handled[0].payload == {"id": "1", "tenant": "acme"}
    assert handled[0].envelope.origin == "rewritten"


async def test_a_consume_interceptor_can_read_the_decision_the_handler_made() -> None:
    # Which is what a metric or a log line about the outcome is made of, and is
    # the half a before-hook cannot reach.
    decisions: list[Ack] = []

    async def watching(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        decision = await handle(context)
        decisions.append(decision)
        return decision

    async def handler(message: Message) -> Ack:
        return reject(ValueError("nothing to do with this"))

    async with consuming(handler, watching) as transport:
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert len(decisions) == 1
    assert str(decisions[0].error) == "nothing to do with this"


async def test_an_interceptor_that_refuses_a_message_fails_the_delivery() -> None:
    # And is not acknowledged as though something had processed it. With no
    # attempts left the message goes to the dead-letter queue carrying the
    # reason the interceptor gave, which is what an operator has to work from.
    reached: list[Message] = []

    async def refusing(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        raise PermissionError("this process does not serve that tenant")

    async def handler(message: Message) -> Ack:
        reached.append(message)
        return accept()

    async with consuming(handler, refusing) as transport:
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert reached == []
    dead = transport.sent_to(DLQ)
    assert len(dead) == 1
    assert "does not serve that tenant" in dead[0].headers[headers.ERROR]


async def test_a_rewritten_envelope_is_the_one_that_gets_dead_lettered() -> None:
    # An interceptor that rewrote the envelope on the way in meant that rewrite
    # for the dead letter as much as for the handler; the alternative is an
    # operator reading a queue of messages missing the very field the
    # interceptor exists to add.
    async def enriching(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        context.envelope = context.envelope.with_(headers={"tenant": "acme"})
        return await handle(context)

    async def handler(message: Message) -> Ack:
        return reject(ValueError("no"))

    async with consuming(handler, enriching) as transport:
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert transport.sent_to(DLQ)[0].headers["tenant"] == "acme"


async def test_consume_interceptors_nest_in_the_order_they_were_registered() -> None:
    order: list[str] = []

    def named(name: str) -> ConsumeInterceptor:
        async def step(context: ConsumeContext, handle: ConsumeNext) -> Ack:
            order.append(f"{name} in")
            try:
                return await handle(context)
            finally:
                order.append(f"{name} out")

        return step

    async def handler(message: Message) -> Ack:
        return accept()

    async with consuming(handler, named("first"), named("second")) as transport:
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert order == ["first in", "second in", "second out", "first out"]


async def test_the_state_bag_carries_one_interceptor_to_the_next() -> None:
    # For the pair that opens something on the way in and reads it later, and
    # for anything an interceptor wants to hand on without putting it on the
    # wire where a consumer in another language would have to understand it.
    seen: list[object] = []

    async def opening(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        context.state["unit-of-work"] = "open"
        return await handle(context)

    async def reading(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        seen.append(context.state.get("unit-of-work"))
        return await handle(context)

    async def handler(message: Message) -> Ack:
        return accept()

    async with consuming(handler, opening, reading) as transport:
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    assert seen == ["open"]


async def test_an_interceptor_sees_the_undecoded_body_as_well() -> None:
    seen: list[bytes] = []

    async def watching(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        seen.append(context.body)
        assert context.content_type == "application/json"
        assert context.redelivered is True
        return await handle(context)

    async def handler(message: Message) -> Ack:
        return accept()

    async with consuming(handler, watching) as transport:
        await transport.deliver(
            QUEUE, b'{"id": "1"}', headers=wire(), redelivered=True
        )

    assert seen == [b'{"id": "1"}']


async def test_interceptors_can_be_given_to_the_connection_up_front() -> None:
    async def stamping(context: PublishContext, send: PublishNext) -> PublishResult:
        context.set_header("tenant", "acme")
        return await send(context)

    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport, on_publish=[stamping])

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    assert transport.sent_to(QUEUE)[0].headers["tenant"] == "acme"


async def test_a_connection_with_no_interceptors_publishes_exactly_as_before() -> None:
    # The seam has to cost nothing when nobody uses it, which is most services.
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport)

    assert mq.publish_interceptors == ()
    assert mq.consume_interceptors == ()

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    assert len(transport.sent_to(QUEUE)) == 1


async def test_the_registered_list_is_a_snapshot_rather_than_the_list_itself() -> None:
    # A publish in progress must not be handed a chain that is being edited
    # underneath it.
    async def nothing(context: PublishContext, send: PublishNext) -> PublishResult:
        return await send(context)

    mq = Connection(FakeTransport()).intercept_publish(nothing)
    registered = mq.publish_interceptors

    mq.intercept_publish(nothing)

    assert len(registered) == 1
    assert len(mq.publish_interceptors) == 2
