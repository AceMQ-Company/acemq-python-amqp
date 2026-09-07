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

"""A transport that records instead of connecting.

The consumer's decisions — what is acknowledged, what is republished, what ends
up in the dead-letter queue and with which reason — are the part of this library
most worth testing and the part a broker tells you least about. This makes them
observable without one, so the retry rules can be checked in milliseconds and
the integration tests can be about whether a real broker agrees.

It also has to satisfy the transport protocol, so it is a check that the
protocol is implementable by something other than aio-pika.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from acemq_amqp.transport import (
    ConsumeSpec,
    Delivery,
    ExchangeSpec,
    Outbound,
    PublishResult,
    QueueSpec,
)


@dataclass
class Settlement:
    """What a consumer did with one delivery."""

    acked: bool = False
    nacked: bool = False
    requeued: bool | None = None


@dataclass
class Sent:
    """One message this transport was asked to publish."""

    exchange: str
    routing_key: str
    message: Outbound

    @property
    def headers(self) -> Mapping[str, Any]:
        return self.message.headers


@dataclass
class Staged:
    """A message sitting on a fake queue, waiting to be pulled."""

    body: bytes
    content_type: str | None
    routing_key: str
    headers: dict[str, Any]


class FakeSubscription:
    """A subscription that does nothing but stop delivering."""

    def __init__(self, transport: FakeTransport, queue: str) -> None:
        self._transport = transport
        self._queue = queue
        self.released = False

    async def stop(self) -> None:
        self._transport.consumers.pop(self._queue, None)

    async def close(self) -> None:
        self.released = True


@dataclass
class FakeTransport:
    """Records declarations and publishes, and hands out deliveries on demand."""

    queues: dict[str, QueueSpec] = field(default_factory=dict)
    exchanges: dict[str, ExchangeSpec] = field(default_factory=dict)
    bindings: list[tuple[str, str, str]] = field(default_factory=list)
    sent: list[Sent] = field(default_factory=list)
    consumers: dict[str, Callable[[Delivery], Awaitable[None]]] = field(default_factory=dict)
    waiting: dict[str, list[Staged]] = field(default_factory=dict)
    closed: bool = False

    async def declare_queue(self, name: str, spec: QueueSpec) -> None:
        self.queues[name] = spec

    async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None:
        self.exchanges[name] = spec

    async def bind(self, queue: str, exchange: str, routing_key: str) -> None:
        self.bindings.append((queue, exchange, routing_key))

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        self.sent.append(Sent(exchange, routing_key, message))
        # Only the default exchange is modelled, because that is what the retry
        # and dead-letter hops use and the point of this fake is to watch them.
        # A queue that was never declared is unroutable, which is how the "the
        # dead-letter queue is not there" path gets tested.
        routed = bool(exchange) or routing_key in self.queues
        return PublishResult(
            message_id=message.message_id,
            confirmed=True,
            routed=routed,
            return_reason="" if routed else "NO_ROUTE",
        )

    async def consume(
        self,
        queue: str,
        spec: ConsumeSpec,
        deliver: Callable[[Delivery], Awaitable[None]],
    ) -> FakeSubscription:
        self.consumers[queue] = deliver
        return FakeSubscription(self, queue)

    async def close(self) -> None:
        self.closed = True

    def stage(
        self,
        queue: str,
        body: bytes,
        *,
        headers: Mapping[str, Any] | None = None,
        content_type: str | None = "application/json",
        routing_key: str | None = None,
    ) -> None:
        """Puts a message on a queue for :meth:`pull` to find."""
        self.waiting.setdefault(queue, []).append(
            Staged(
                body=body,
                content_type=content_type,
                routing_key=queue if routing_key is None else routing_key,
                headers=dict(headers or {}),
            )
        )

    async def pull(self, queue: str) -> Delivery | None:
        """Hands over the message at the head of a queue, unsettled.

        A rejected message goes back to the *head*, which is what RabbitMQ does
        and is the detail everything about replay turns on: returning a message
        one at a time means reading the same one for ever and never seeing what
        is behind it.
        """
        waiting = self.waiting.setdefault(queue, [])
        if not waiting:
            return None
        entry = waiting.pop(0)

        async def ack() -> None:
            return None

        async def nack(requeue: bool) -> None:
            if requeue:
                waiting.insert(0, entry)

        return Delivery(
            body=entry.body,
            content_type=entry.content_type,
            routing_key=entry.routing_key,
            message_id="",
            headers=entry.headers,
            redelivered=False,
            ack=ack,
            nack=nack,
        )

    async def queue_exists(self, name: str) -> bool:
        return name in self.queues

    async def message_count(self, name: str) -> int:
        return sum(1 for sent in self.sent if not sent.exchange and sent.routing_key == name)

    async def delete_queue(self, name: str) -> None:
        self.queues.pop(name, None)

    def sent_to(self, queue: str) -> list[Sent]:
        """Everything published to a queue through the default exchange."""
        return [sent for sent in self.sent if not sent.exchange and sent.routing_key == queue]

    async def deliver(
        self,
        queue: str,
        body: bytes,
        *,
        headers: Mapping[str, Any] | None = None,
        content_type: str | None = "application/json",
        routing_key: str | None = None,
        redelivered: bool = False,
        timeout: float = 5.0,
    ) -> Settlement:
        """Hands one message to whoever is consuming the queue.

        Waits for the message to be settled rather than returning as soon as it
        has been handed over, because the consumer queues a delivery for a
        worker rather than handling it where it arrives — so without the wait a
        test would read the settlement before anything had happened.

        :returns: what the consumer did with it
        """
        settlement = Settlement()
        settled = asyncio.Event()

        async def ack() -> None:
            settlement.acked = True
            settled.set()

        async def nack(requeue: bool) -> None:
            settlement.nacked = True
            settlement.requeued = requeue
            settled.set()

        await self.consumers[queue](
            Delivery(
                body=body,
                content_type=content_type,
                routing_key=queue if routing_key is None else routing_key,
                message_id="",
                headers=dict(headers or {}),
                redelivered=redelivered,
                ack=ack,
                nack=nack,
            )
        )
        await asyncio.wait_for(settled.wait(), timeout)
        return settlement
