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

"""What a broker has to provide, described without naming one.

Everything above this module — the envelope, the codecs, the retry engine, the
topology — is written against these types rather than against a client library.
That is what makes a fake transport in a test exercise the same code path as
RabbitMQ does in production, rather than a second implementation of it that can
drift.

The types are deliberately thin. A transport moves bytes and settles deliveries;
it does not know what an envelope is, which is why :class:`Outbound` carries a
plain header map and :class:`Delivery` hands one back.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """How a queue should be declared.

    :param durable: survives a broker restart
    :param auto_delete: goes away when its last consumer does
    :param exclusive: usable only by the connection that declared it
    :param args: broker-specific arguments, such as ``x-dead-letter-exchange``
    """

    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    args: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExchangeSpec:
    """How an exchange should be declared.

    :param kind: ``direct``, ``topic``, ``fanout`` or ``headers``
    :param durable: survives a broker restart
    :param auto_delete: goes away when its last binding does
    :param args: broker-specific arguments
    """

    kind: str = "topic"
    durable: bool = True
    auto_delete: bool = False
    args: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ConsumeSpec:
    """How a consumer should be set up.

    :param prefetch: how many unacknowledged messages the broker will send at
        once, or zero for the transport's default
    :param tag: what to call this consumer to the broker, which is what an
        operator sees when working out who is holding a message
    :param args: broker-specific consumer arguments, such as the
        ``x-stream-offset`` a stream consumer needs to say where it starts
    """

    prefetch: int = 0
    tag: str = ""
    args: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Outbound:
    """A message on its way to the broker, already encoded.

    :param body: the encoded payload
    :param content_type: what the body is, for the consumer's codec
    :param message_id: the AMQP message id, conventionally the envelope's
    :param headers: the envelope's headers plus the application's
    :param persistent: asks the broker to write the message to disk, which is
        not a guarantee on its own — a persistent message on a queue that is not
        durable still dies with the broker
    :param mandatory: asks the broker to return the message rather than drop it
        when no queue is bound to receive it
    """

    body: bytes
    content_type: str = ""
    message_id: str = ""
    headers: Mapping[str, Any] = field(default_factory=dict)
    persistent: bool = True
    mandatory: bool = False


@dataclass(frozen=True, slots=True)
class PublishResult:
    """What the broker said about a published message.

    :param message_id: the identifier it went out with
    :param confirmed: the broker taking responsibility for the message. Without
        publisher confirms this is false and nothing has been promised: the
        message reached a socket, which is not the same thing
    :param routed: false when a mandatory message reached no queue at all. A
        message nothing is bound to receive is not an error to the broker; it is
        dropped, silently, which is exactly the failure worth hearing about
    :param return_reason: the broker's explanation when ``routed`` is false
    """

    message_id: str = ""
    confirmed: bool = False
    routed: bool = True
    return_reason: str = ""


@dataclass(frozen=True, slots=True)
class Delivery:
    """A message as it arrived, before any codec has looked at it.

    :param body: the bytes, undecoded
    :param content_type: what the sender said the body is, or ``None`` when the
        sender said nothing — which is a different thing from an empty string
        and is why the codecs distinguish them
    :param routing_key: the key it arrived under
    :param message_id: the AMQP message id
    :param headers: everything the sender wrote, reserved names included
    :param redelivered: the broker saying it has handed this message over
        before. It is the broker's own view and says nothing about which attempt
        this is: a requeue returns the bytes the broker was given, so the
        attempt header still reads whatever the publisher wrote
    :param ack: confirms the message and removes it from the queue
    :param nack: returns the message, requeued or not
    """

    body: bytes
    content_type: str | None
    routing_key: str
    message_id: str
    headers: Mapping[str, Any]
    redelivered: bool
    ack: Callable[[], Awaitable[None]]
    nack: Callable[[bool], Awaitable[None]]


class Subscription(Protocol):
    """A running consumer on a transport.

    Stopping and releasing are two steps rather than one because a message that
    has been delivered still has to be settled, and on AMQP a settlement travels
    on the channel the delivery came in on. Closing that channel while a handler
    is still working means acknowledging on a channel that has gone — which
    RabbitMQ answers by closing the whole connection, taking down every other
    consumer and publisher on it.
    """

    async def stop(self) -> None:
        """Stops delivery, leaving what has already arrived settleable.

        It must not return until the transport has promised no further
        deliveries, because that promise is what makes it safe for the consumer
        above to shut its workers down.
        """

    async def close(self) -> None:
        """Releases what the subscription was holding.

        Called once nothing is left unsettled.
        """


@runtime_checkable
class Transport(Protocol):
    """A connection to a broker.

    Async because that is the shape a broker client has: every one of these is a
    round trip, and a library that hid them behind blocking calls would make one
    slow broker stall a whole process. The blocking API in
    :mod:`acemq_amqp.sync` is built on top of this rather than beside it.
    """

    async def declare_queue(self, name: str, spec: QueueSpec) -> None:
        """Creates a queue if it is not already there."""

    async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None:
        """Creates an exchange if it is not already there."""

    async def bind(self, queue: str, exchange: str, routing_key: str) -> None:
        """Routes messages matching a key from an exchange to a queue."""

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        """Sends one message and reports what the broker said about it."""

    async def consume(
        self,
        queue: str,
        spec: ConsumeSpec,
        deliver: Callable[[Delivery], Awaitable[None]],
    ) -> Subscription:
        """Delivers messages to ``deliver`` until the subscription is closed."""

    async def close(self) -> None:
        """Releases the connection."""


@runtime_checkable
class MessageSource(Protocol):
    """A transport that can be asked for one message instead of subscribed to.

    Separate from :class:`Transport` because it answers a question a
    subscription cannot: *is there anything left*. A consumer is told when a
    message arrives and never told that none will, so a tool that has to work
    through what is on a queue and then stop — a replay, a drain, a one-off
    inspection — cannot be built on one.

    It is for tools rather than for services. Pulling one message at a time is a
    round trip per message where a consumer gets a stream, so a service built on
    this is a slow service.
    """

    async def pull(self, queue: str) -> Delivery | None:
        """Takes the message at the head of a queue, unsettled.

        :param queue: what to read from
        :returns: the delivery, or ``None`` when the queue has nothing waiting.
            It comes back unsettled, so a caller that neither acknowledges nor
            rejects it is holding it until the connection goes
        """


@runtime_checkable
class QueueAdmin(Protocol):
    """A transport that can also report on and remove queues.

    Separate from :class:`Transport` because not every transport has an answer:
    "how many messages are waiting" means nothing to one that does not hold
    them. Asking for it is therefore a question about the transport in hand,
    which is why the connection checks before it asks.
    """

    async def queue_exists(self, name: str) -> bool:
        """Whether a queue is on the broker.

        It must not create the queue. A declaration is the only thing AMQP
        offers, and declaring a queue that is missing creates it — so anything
        built on a declaration would create the very queues it was asked only to
        look for.
        """

    async def message_count(self, name: str) -> int:
        """How many messages are waiting on a queue.

        A number for a dashboard or a test, not a decision to make in a handler:
        it is a snapshot of a queue that is still moving, and by the time it is
        read somewhere else it is already wrong.
        """

    async def delete_queue(self, name: str) -> None:
        """Removes a queue and every message still on it.

        For tests and for tools. A service that deletes queues is usually a
        service that has confused a queue with a session.
        """
