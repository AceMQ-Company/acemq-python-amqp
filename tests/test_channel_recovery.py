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

"""A recovery reuses the channels the transport kept; it does not add one each time.

A robust connection reopens every channel it handed out, and reports each as
closed until it has. 0.7.7 took "closed" for "dead" and opened another, so every
publish made during a recovery left one more channel that the connection went on
reopening for ever: 240 forced recoveries in the soak, 240 extra channels and
twice the resident memory. Counted here across simulated recoveries.
"""

from __future__ import annotations

import asyncio
from typing import Any

from acemq_amqp import Outbound
from acemq_amqp.rabbitmq import RabbitMQTransport


class _Channel:
    """A robust channel: closed while the connection is away, reopened after."""

    def __init__(self) -> None:
        self.is_closed = False
        self._closed: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._reopened = asyncio.Event()
        self._reopened.set()
        self.default_exchange = self

    def connection_lost(self) -> None:
        self.is_closed = True
        self._reopened.clear()

    def reopen(self) -> None:
        self.is_closed = False
        self._reopened.set()

    def closed(self) -> asyncio.Future[bool]:
        return self._closed

    async def ready(self) -> None:
        await self._reopened.wait()

    async def close(self) -> None:
        self.is_closed = True
        if not self._closed.done():
            self._closed.set_result(True)

    async def publish(self, *_: Any, **__: Any) -> object:
        return object()


class _Connection:
    """Hands out channels and, like aio-pika's, keeps every one for recovery."""

    def __init__(self) -> None:
        self.channels: list[_Channel] = []

    async def channel(self, **_: Any) -> _Channel:
        opened = _Channel()
        self.channels.append(opened)
        return opened

    def drop(self) -> None:
        for channel in self.channels:
            channel.connection_lost()

    def recover(self) -> None:
        for channel in self.channels:
            channel.reopen()


def _message() -> Outbound:
    return Outbound(body=b"{}", content_type="application/json", message_id="m", headers={})


async def test_publishing_through_many_recoveries_keeps_one_channel() -> None:
    connection = _Connection()
    transport = RabbitMQTransport(connection)  # type: ignore[arg-type]
    await transport.publish("", "q", _message())

    for _ in range(50):
        connection.drop()
        # A publish made while the connection is away, as a standing load does.
        waiting = asyncio.create_task(transport.publish("", "q", _message()))
        await asyncio.sleep(0)
        connection.recover()
        await waiting

    assert len(connection.channels) == 1


async def test_a_channel_closed_for_good_is_replaced() -> None:
    connection = _Connection()
    transport = RabbitMQTransport(connection)  # type: ignore[arg-type]
    await transport.publish("", "q", _message())

    await connection.channels[0].close()
    await transport.publish("", "q", _message())

    assert len(connection.channels) == 2
