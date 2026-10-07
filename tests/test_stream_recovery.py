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

"""A recovered stream reader carries on where it was, not where it began.

aio-pika's robust queue consumes again after a reconnection with the arguments
it recorded the first time. For a stream those name a starting point, so 0.7.8
asked for it again: a reader that began at "first" was handed the whole stream a
second time, and one that began at "next" skipped everything appended while it
was away. Simulated here the way aio-pika does it -- the recorded arguments,
consumed again -- and run against a real broker in the integration suite.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from acemq_amqp import ConsumeSpec, Delivery
from acemq_amqp.rabbitmq import RabbitMQTransport

OFFSET = "x-stream-offset"


class _Incoming:
    def __init__(self, offset: int | None) -> None:
        self.headers = {} if offset is None else {OFFSET: offset}
        self.body = b"{}"
        self.content_type = "application/json"
        self.routing_key = ""
        self.message_id = ""
        self.redelivered = False
        self.reply_to = ""

    async def ack(self) -> None:
        pass

    async def nack(self, requeue: bool = True) -> None:
        pass


class _Queue:
    """Records a consumer the way aio-pika's RobustQueue does, by reference."""

    def __init__(self) -> None:
        self.callback: Callable[[Any], Awaitable[None]] | None = None
        self.recorded: dict[str, Any] = {}

    async def consume(self, callback: Any, *, arguments: Any = None, **_: Any) -> str:
        self.callback = callback
        self.recorded = {"arguments": arguments}
        return "tag"

    def arguments_on_restore(self) -> Any:
        """What a reconnection would consume with."""
        return self.recorded["arguments"]


class _Channel:
    def __init__(self, queue: _Queue) -> None:
        self._queue = queue

    async def set_qos(self, **_: Any) -> None:
        pass

    async def get_queue(self, _: str) -> _Queue:
        return self._queue


class _Connection:
    def __init__(self) -> None:
        self.queue = _Queue()

    async def channel(self, **_: Any) -> _Channel:
        return _Channel(self.queue)


async def _subscribe(args: dict[str, Any]) -> tuple[_Queue, list[Delivery]]:
    connection = _Connection()
    transport = RabbitMQTransport(connection)  # type: ignore[arg-type]
    held: list[Delivery] = []

    async def deliver(delivery: Delivery) -> None:
        held.append(delivery)

    await transport.consume("s", ConsumeSpec(prefetch=10, args=args), deliver)
    return connection.queue, held


async def _arrive(queue: _Queue, *offsets: int | None) -> None:
    assert queue.callback is not None
    for offset in offsets:
        await queue.callback(_Incoming(offset))


async def test_a_reader_from_first_resumes_at_the_oldest_unsettled_entry() -> None:
    queue, held = await _subscribe({OFFSET: "first"})
    await _arrive(queue, 0, 1, 2, 3, 4)
    for delivery in held[:2]:
        await delivery.ack()
    await held[3].ack()  # settled out of order; 2 is still being worked on

    assert queue.arguments_on_restore() == {OFFSET: 2}


async def test_a_reader_from_next_resumes_after_the_newest_settled_entry() -> None:
    queue, held = await _subscribe({OFFSET: "next"})
    await _arrive(queue, 100, 101, 102)
    for delivery in held:
        await delivery.ack()

    assert queue.arguments_on_restore() == {OFFSET: 103}


async def test_a_nack_settles_a_stream_entry_too() -> None:
    queue, held = await _subscribe({OFFSET: "first"})
    await _arrive(queue, 7)
    await held[0].nack(False)

    assert queue.arguments_on_restore() == {OFFSET: 8}


async def test_a_reader_given_nothing_keeps_its_own_starting_point() -> None:
    queue, _ = await _subscribe({OFFSET: "next"})

    assert queue.arguments_on_restore() == {OFFSET: "next"}


async def test_a_queue_consumer_is_consumed_again_unchanged() -> None:
    queue, held = await _subscribe({})
    await _arrive(queue, None, None)
    for delivery in held:
        await delivery.ack()

    assert queue.arguments_on_restore() is None
