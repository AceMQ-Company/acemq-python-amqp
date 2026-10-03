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

"""A publish refused for back pressure is told apart from one that failed.

Go, .NET and Java each have a type that means "the broker declined, nothing was
sent, try again later". Without one a caller counting outcomes has to call a
blocked broker a lost message, which is the wrong response to the wrong event.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from fake_transport import FakeTransport

from acemq_amqp import Outbound, PublishError, PublishingPausedError, PublishResult
from acemq_amqp.connection import Connection


async def test_a_blocked_connection_refuses_the_publish_and_sends_nothing() -> None:
    transport = FakeTransport()
    transport.blocked = True
    transport.blocked_reason = "low on disk space"
    connection = Connection(transport)

    with pytest.raises(PublishingPausedError) as refused:
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    # Still a PublishError, so every existing ``except PublishError`` keeps working.
    assert isinstance(refused.value, PublishError)
    assert refused.value.routing_key == "orders.new"
    assert "low on disk space" in str(refused.value)
    # Declined, not lost: nothing reached the wire.
    assert transport.sent == []


async def test_a_blocked_connection_without_a_reason_still_refuses() -> None:
    # The RabbitMQ transport cannot recover the broker's reason, so this is the
    # case production actually sees.
    transport = FakeTransport()
    transport.blocked = True
    connection = Connection(transport)

    with pytest.raises(PublishingPausedError, match="blocked this connection"):
        await connection.publish_raw("", "q", Outbound(body=b"x", message_id="m-1"))

    assert transport.sent == []


async def test_a_connection_that_cannot_say_whether_it_is_blocked_still_publishes() -> None:
    # ``None`` is "could not look", not "blocked": refusing on it would stop every
    # publish on a client that has moved the state out of reach.
    transport = FakeTransport()
    transport.blocked = None
    connection = Connection(transport)

    await connection.publisher(routing_key="orders.new").send({"id": "1"})

    assert len(transport.sent) == 1


async def test_publishing_resumes_once_the_broker_unblocks() -> None:
    transport = FakeTransport()
    transport.blocked = True
    connection = Connection(transport)
    publisher = connection.publisher(routing_key="orders.new")

    with pytest.raises(PublishingPausedError):
        await publisher.send({"id": "1"})
    transport.blocked = False
    await publisher.send({"id": "1"})

    assert len(transport.sent) == 1


class NackingTransport(FakeTransport):
    """A broker that takes the message and declines responsibility for it."""

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        await super().publish(exchange, routing_key, message)
        return PublishResult(
            message_id=message.message_id, confirmed=False, routed=True, return_reason=""
        )


async def test_a_nack_is_a_failure_not_a_refusal() -> None:
    connection = Connection(NackingTransport())

    with pytest.raises(PublishError) as failed:
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    assert not isinstance(failed.value, PublishingPausedError)


async def test_an_unroutable_message_is_a_failure_not_a_refusal() -> None:
    connection = Connection(FakeTransport())

    with pytest.raises(PublishError) as failed:
        await connection.publisher(routing_key="nobody.listens", mandatory=True).send({})

    assert failed.value.unroutable is True
    assert not isinstance(failed.value, PublishingPausedError)


async def test_running_out_of_permits_is_a_failure_not_a_refusal() -> None:
    # The same as Java, .NET and Go, all of which report this as a transport
    # failure: a broker that stopped confirming may be dead rather than busy.
    class Silent(FakeTransport):
        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    connection = Connection(
        Silent(), max_outstanding_publishes=1, confirm_timeout=timedelta(milliseconds=20)
    )
    publisher = connection.publisher(routing_key="orders.new")

    first = asyncio.create_task(publisher.send({"id": "1"}))
    await asyncio.sleep(0)
    with pytest.raises(PublishError) as failed:
        await publisher.send({"id": "2"})
    first.cancel()

    assert not isinstance(failed.value, PublishingPausedError)
