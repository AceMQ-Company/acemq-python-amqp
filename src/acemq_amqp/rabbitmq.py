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

"""Talking to RabbitMQ, through aio-pika.

This is the only module in the library that knows a broker client exists, which
is why it is imported lazily by :func:`acemq_amqp.connect` rather than by the
package: reading an AceMQ envelope should not require installing an AMQP client,
so aio-pika is an extra and everything above :mod:`acemq_amqp.transport` is
written without it.

Channels are handed out rather than shared, because in AMQP a channel is what
dies when the broker refuses something. A queue declared with settings that
disagree with the ones already on the broker answers ``PRECONDITION_FAILED`` and
takes its channel down with it — on a shared channel that would also stop every
publisher and consumer that happened to be using it, for a failure that has
nothing to do with them.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractRobustConnection,
)
from aiormq.exceptions import ChannelNotFoundEntity

from .transport import (
    ConsumeSpec,
    Delivery,
    ExchangeSpec,
    Outbound,
    PublishResult,
    QueueSpec,
)


class RabbitMQTransport:
    """A :class:`~acemq_amqp.transport.Transport` backed by aio-pika.

    Built by :meth:`connect` rather than by hand in the ordinary case. The
    connection underneath is a robust one, so a broker restart is a pause rather
    than an outage: aio-pika reopens the connection, the channels and the
    consumers, and the messages that were unacknowledged when it went are
    redelivered.
    """

    def __init__(self, connection: AbstractRobustConnection) -> None:
        self._connection = connection
        self._admin: AbstractChannel | None = None
        self._publishing: AbstractChannel | None = None
        self._pulling: AbstractChannel | None = None
        self._lock = asyncio.Lock()

    @classmethod
    async def connect(cls, url: str, **kwargs: Any) -> RabbitMQTransport:
        """Opens a connection to a broker.

        :param url: an ``amqp://`` or ``amqps://`` URL
        :param kwargs: passed through to :func:`aio_pika.connect_robust`, which
            is where TLS contexts and client properties are set
        :returns: the transport
        """
        connection = await aio_pika.connect_robust(url, **kwargs)
        return cls(connection)

    @property
    def connection(self) -> AbstractRobustConnection:
        """The aio-pika connection, for anything this library does not wrap."""
        return self._connection

    async def _admin_channel(self) -> AbstractChannel:
        """The channel declarations go on, made again if it has been killed.

        A refused declaration closes its channel, so this checks rather than
        assumes: the next declaration after a ``PRECONDITION_FAILED`` should
        report the broker's answer to *its* question, not the corpse of the
        previous one.
        """
        async with self._lock:
            if self._admin is None or self._admin.is_closed:
                self._admin = await self._connection.channel()
            return self._admin

    async def _publish_channel(self) -> AbstractChannel:
        """The channel publishing goes on, with confirms turned on.

        Confirms cost a round trip and are worth it: without them a publish
        succeeds as soon as the bytes reach the socket, which is not the broker
        promising anything, and a service that treats it as one loses messages
        it believes it sent.
        """
        async with self._lock:
            if self._publishing is None or self._publishing.is_closed:
                self._publishing = await self._connection.channel(publisher_confirms=True)
            return self._publishing

    @asynccontextmanager
    async def _throwaway_channel(self) -> AsyncIterator[AbstractChannel]:
        """A channel for a question whose answer may be the broker refusing.

        Used where a failure is an expected outcome rather than a problem, so
        that the failure costs nothing but the channel it happened on.
        """
        channel = await self._connection.channel()
        try:
            yield channel
        finally:
            if not channel.is_closed:
                await channel.close()

    async def declare_queue(self, name: str, spec: QueueSpec) -> None:
        channel = await self._admin_channel()
        await channel.declare_queue(
            name,
            durable=spec.durable,
            auto_delete=spec.auto_delete,
            exclusive=spec.exclusive,
            arguments=dict(spec.args) or None,
        )

    async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None:
        channel = await self._admin_channel()
        await channel.declare_exchange(
            name,
            type=spec.kind,
            durable=spec.durable,
            auto_delete=spec.auto_delete,
            arguments=dict(spec.args) or None,
        )

    async def bind(self, queue: str, exchange: str, routing_key: str) -> None:
        channel = await self._admin_channel()
        bound = await channel.get_queue(queue)
        await bound.bind(exchange, routing_key)

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        channel = await self._publish_channel()
        # An empty exchange name is the default exchange, which routes to the
        # queue whose name matches the routing key. ensure=False keeps this from
        # declaring anything: publishing is not the place to find out an
        # exchange is missing, because the broker will say so anyway.
        target = (
            channel.default_exchange
            if not exchange
            else await channel.get_exchange(exchange, ensure=False)
        )

        outbound = aio_pika.Message(
            message.body,
            headers=dict(message.headers),
            content_type=message.content_type or None,
            message_id=message.message_id or None,
            # ``None`` rather than an empty string, because AMQP's reply-to is
            # absent or set and "set to nothing" is neither. A responder in
            # another language reads the property and would take "" for an
            # address.
            reply_to=message.reply_to or None,
            delivery_mode=(
                aio_pika.DeliveryMode.PERSISTENT
                if message.persistent
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
        )

        # Typed as object because aio-pika's annotation does not describe what it
        # actually returns: a message the broker sent back as unroutable arrives
        # here as a DeliveredMessage carrying the Basic.Return, not as one of the
        # confirmation frames the signature promises.
        confirmation: object = await target.publish(
            outbound, routing_key=routing_key, mandatory=message.mandatory
        )

        returned = getattr(confirmation, "delivery", None)
        if returned is not None and hasattr(returned, "reply_text"):
            return PublishResult(
                message_id=message.message_id,
                confirmed=False,
                routed=False,
                return_reason=str(returned.reply_text),
            )

        return PublishResult(
            message_id=message.message_id,
            confirmed=type(confirmation).__name__ != "Nack",
            routed=True,
        )

    async def consume(
        self,
        queue: str,
        spec: ConsumeSpec,
        deliver: Callable[[Delivery], Awaitable[None]],
    ) -> _Subscription:
        # A channel of its own, so that one consumer's queue disappearing does
        # not cancel every other consumer on this connection.
        channel = await self._connection.channel()
        if spec.prefetch > 0:
            await channel.set_qos(prefetch_count=spec.prefetch)
        source = await channel.get_queue(queue)

        async def on_message(incoming: AbstractIncomingMessage) -> None:
            await deliver(_delivery(incoming))

        tag = await source.consume(
            on_message,
            consumer_tag=spec.tag or None,
            arguments=dict(spec.args) or None,
        )
        return _Subscription(channel, source, tag)

    async def pull(self, queue: str) -> Delivery | None:
        channel = await self._pull_channel()
        source = await channel.get_queue(queue)
        # fail=False so an empty queue is an answer rather than an exception.
        # no_ack stays off: a pulled message that vanished the moment it was read
        # would make a replay that crashes half way through lose everything it
        # had not yet republished.
        incoming = await source.get(fail=False, no_ack=False)
        return None if incoming is None else _delivery(incoming)

    async def _pull_channel(self) -> AbstractChannel:
        """The channel pulled messages live on.

        Its own, and long-lived, because a pulled message can only be settled on
        the channel it arrived on — and a replay deliberately holds the messages
        it declined unsettled until its pass is over.
        """
        async with self._lock:
            if self._pulling is None or self._pulling.is_closed:
                self._pulling = await self._connection.channel()
            return self._pulling

    async def queue_exists(self, name: str) -> bool:
        # A passive declaration is the only way to ask AMQP whether a queue is
        # there without creating it when it is not, and the answer for a missing
        # queue is the broker closing the channel. Hence a throwaway one.
        async with self._throwaway_channel() as channel:
            try:
                await channel.get_queue(name)
            except ChannelNotFoundEntity:
                return False
            return True

    async def message_count(self, name: str) -> int:
        async with self._throwaway_channel() as channel:
            found = await channel.get_queue(name)
            return int(found.declaration_result.message_count or 0)

    async def delete_queue(self, name: str) -> None:
        channel = await self._admin_channel()
        await channel.queue_delete(name)

    async def close(self) -> None:
        await self._connection.close()


class _Subscription:
    """One running consumer, and the channel it lives on."""

    def __init__(self, channel: AbstractChannel, queue: aio_pika.abc.AbstractQueue, tag: str):
        self._channel = channel
        self._queue = queue
        self._tag = tag

    async def stop(self) -> None:
        """Cancels the consumer, leaving the channel open.

        The cancellation is what makes the promise the consumer above relies on:
        once the broker has acknowledged it, no further delivery will arrive, so
        the workers can be shut down without a message being handed to one that
        has already stopped. The channel stays until they have finished, because
        the messages they are still holding can only be settled on it.
        """
        if not self._channel.is_closed:
            await self._queue.cancel(self._tag)

    async def close(self) -> None:
        """Drops the channel, once nothing on it is unsettled."""
        if not self._channel.is_closed:
            await self._channel.close()


def _delivery(incoming: AbstractIncomingMessage) -> Delivery:
    """Turns an aio-pika delivery into the transport's own shape."""

    async def ack() -> None:
        await incoming.ack()

    async def nack(requeue: bool) -> None:
        await incoming.nack(requeue=requeue)

    return Delivery(
        body=incoming.body,
        # Kept as None when the sender set none. An empty string would say "the
        # sender told us it was nothing", which is a different claim, and the
        # codecs treat the two differently on purpose.
        content_type=incoming.content_type,
        routing_key=incoming.routing_key or "",
        message_id=incoming.message_id or "",
        headers=dict(incoming.headers or {}),
        redelivered=bool(incoming.redelivered),
        ack=ack,
        nack=nack,
        reply_to=incoming.reply_to or "",
    )
