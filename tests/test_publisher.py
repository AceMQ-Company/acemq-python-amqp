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

"""What a publisher puts on the wire, and what it refuses to be quiet about."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    AceMQError,
    Ack,
    ConsumeSpec,
    Delivery,
    Envelope,
    ExchangeSpec,
    Message,
    Outbound,
    PublishError,
    PublishResult,
    QueueSpec,
    TextCodec,
    accept,
    headers,
)
from acemq_amqp.connection import Connection, default_origin
from acemq_amqp.topology import Topology
from acemq_amqp.transport import Subscription


async def test_a_publisher_builds_the_envelope_when_it_is_given_none() -> None:
    transport = FakeTransport()
    connection = Connection(transport, origin="checkout@pod-7")

    await connection.publisher(routing_key="orders.new").send({"id": "7"})

    sent = transport.sent[0]
    assert sent.message.body == b'{"id": "7"}'
    assert sent.message.content_type == "application/json"
    # The routing key is the default type, so a publisher bound to one key needs
    # no further configuration to say what it sends.
    assert sent.headers[headers.TYPE] == "orders.new"
    assert sent.headers[headers.ORIGIN] == "checkout@pod-7"
    assert sent.headers[headers.ATTEMPT] == 1
    assert sent.headers[headers.VERSION] == 1
    # Correlation defaults to the id, so a chain has something to copy.
    assert sent.headers[headers.CORRELATION] == sent.headers[headers.ID]
    assert sent.message.message_id == sent.headers[headers.ID]


async def test_an_envelope_given_by_hand_is_the_one_that_goes() -> None:
    transport = FakeTransport()
    connection = Connection(transport)
    envelope = Envelope(
        id="order-1",
        type="order.placed.v2",
        correlation_id="cart-9",
        causation_id="cart-9",
        headers={"tenant": "acme"},
    )

    await connection.publisher(routing_key="orders.new").send({}, envelope=envelope)

    sent = transport.sent[0]
    assert sent.headers[headers.ID] == "order-1"
    assert sent.headers[headers.TYPE] == "order.placed.v2"
    assert sent.headers[headers.CORRELATION] == "cart-9"
    assert sent.headers["tenant"] == "acme"
    # Absent rather than empty: a header carrying "" is one somebody has to
    # write a special case for at the other end.
    assert headers.ERROR not in sent.headers
    assert headers.CLAIM not in sent.headers


async def test_a_publisher_uses_the_codec_it_was_given_rather_than_the_connections() -> None:
    transport = FakeTransport()
    connection = Connection(transport)

    await connection.publisher(routing_key="lines", codec=TextCodec()).send("a line")

    assert transport.sent[0].message.body == b"a line"
    assert transport.sent[0].message.content_type == "text/plain; charset=utf-8"


async def test_a_mandatory_message_that_reaches_no_queue_is_an_error() -> None:
    # Without this the broker drops an unroutable message silently, which is the
    # quietest way to discover a binding was never made: the publish succeeds,
    # the consumer waits, and nothing anywhere says why.
    transport = FakeTransport()
    connection = Connection(transport)

    with pytest.raises(PublishError, match="reached no queue") as failure:
        await connection.publisher(routing_key="nobody.listens", mandatory=True).send({})

    assert failure.value.unroutable is True
    assert failure.value.routing_key == "nobody.listens"


async def test_an_unroutable_message_is_only_an_error_when_it_was_mandatory() -> None:
    transport = FakeTransport()
    connection = Connection(transport)

    result = await connection.publisher(routing_key="nobody.listens").send({})

    assert result.routed is False


async def test_a_routing_key_can_be_chosen_per_message() -> None:
    transport = FakeTransport()
    await Topology().queue("orders.new").apply(transport)
    connection = Connection(transport)

    publisher = connection.publisher("orders-events", "order.placed")
    await publisher.send({}, routing_key="order.cancelled")

    assert transport.sent[0].routing_key == "order.cancelled"
    assert transport.sent[0].headers[headers.TYPE] == "order.cancelled"


def test_the_default_origin_names_the_machine() -> None:
    assert default_origin().startswith("acemq@")


async def test_a_closed_connection_will_not_start_a_consumer() -> None:
    async def handler(message: Message) -> Ack:
        return accept()

    transport = FakeTransport()
    connection = Connection(transport)
    await connection.close()

    assert transport.closed is True
    with pytest.raises(AceMQError, match="closed"):
        await connection.consume("orders.new", handler)


async def test_a_transport_that_cannot_manage_queues_says_so() -> None:
    # Counting messages means nothing to a transport that does not hold them, so
    # asking is a question about the transport in hand and the answer names it
    # rather than failing somewhere further down.
    class Minimal:
        async def declare_queue(self, name: str, spec: QueueSpec) -> None: ...

        async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None: ...

        async def bind(self, queue: str, exchange: str, routing_key: str) -> None: ...

        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            return PublishResult()

        async def consume(
            self,
            queue: str,
            spec: ConsumeSpec,
            deliver: Callable[[Delivery], Awaitable[None]],
        ) -> Subscription:
            raise NotImplementedError

        async def close(self) -> None: ...

    connection = Connection(Minimal())

    with pytest.raises(AceMQError, match="cannot manage queues"):
        await connection.queue_exists("orders.new")
