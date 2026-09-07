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

"""Publishing and consuming, on top of a transport.

This is where the envelope, the codec and the retry policy meet a broker. It is
asyncio because a broker client is: every publish is a round trip and every
delivery arrives on its own, and a library that hid that behind blocking calls
would make one slow queue stall a whole process. :mod:`acemq_amqp.sync` is the
blocking API for programs that are not running a loop, and it is built on this
one rather than beside it — one implementation of the retry rules, not two.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import socket
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, TypeAlias
from urllib.parse import urlsplit

from . import naming
from .ack import Ack, Action, FatalError
from .codec import Codec, JsonCodec
from .envelope import Envelope
from .errors import AceMQError, PublishError
from .retry import ZERO, RetryPolicy, no_retry
from .topology import Topology
from .transport import (
    ConsumeSpec,
    Delivery,
    Outbound,
    PublishResult,
    QueueAdmin,
    Subscription,
    Transport,
)

log = logging.getLogger("acemq")

#: How many unacknowledged messages a consumer holds unless it says otherwise.
#: Enough that a handler is never waiting on the network, small enough that a
#: consumer that stalls does not take a queue's worth of work down with it.
DEFAULT_PREFETCH = 20


@dataclass(frozen=True, slots=True)
class Message:
    """A delivery that has been decoded.

    :param payload: the body, read through the codec
    :param envelope: the metadata that travelled with it
    :param routing_key: the key it arrived under
    :param content_type: what the sender said the body was, or ``None``
    :param redelivered: the broker saying it has handed this one over before
    :param body: the undecoded bytes, for a handler that wants to see them
    """

    payload: Any
    envelope: Envelope
    routing_key: str
    content_type: str | None
    redelivered: bool
    body: bytes


#: What a handler is. Returning an :class:`~acemq_amqp.Ack` is the decision.
#:
#: Both shapes are accepted because both are honest: a handler that talks to a
#: database over asyncio is a coroutine, and a handler that only does arithmetic
#: should not have to pretend to be one.
Handler: TypeAlias = Callable[[Message], "Ack | Awaitable[Ack]"]


class Publisher:
    """Sends messages to one destination.

    Cheap to keep and safe to share: build one per message type at start-up
    rather than one per message.
    """

    def __init__(
        self,
        connection: Connection,
        exchange: str,
        routing_key: str,
        *,
        codec: Codec | None = None,
        persistent: bool = True,
        mandatory: bool = False,
    ) -> None:
        self._connection = connection
        self._exchange = exchange
        self._routing_key = routing_key
        self._codec = codec or connection.codec
        self._persistent = persistent
        self._mandatory = mandatory

    async def send(
        self,
        payload: Any,
        *,
        envelope: Envelope | None = None,
        routing_key: str | None = None,
    ) -> PublishResult:
        """Publishes one message.

        The envelope is built here when none is given: an identifier, a
        correlation defaulting to it, this connection's origin, and now. The
        type falls back to the routing key, which is why a publisher bound to
        ``order.placed`` needs no further configuration to say what it sends.

        :param payload: what to send, encoded by this publisher's codec
        :param envelope: metadata to send instead of a fresh one, for a message
            derived from another
        :param routing_key: what to publish under, for a publisher bound to an
            exchange rather than to one key
        :returns: what the broker said
        :raises PublishError: when the publisher is mandatory and the message
            reached no queue
        """
        key = self._routing_key if routing_key is None else routing_key
        outgoing = envelope or Envelope(origin=self._connection.origin)
        body = self._codec.encode(payload)

        result = await self._connection.publish_raw(
            self._exchange,
            key,
            Outbound(
                body=body,
                content_type=self._codec.content_type,
                message_id=outgoing.id,
                headers=outgoing.to_headers(routing_key=key),
                persistent=self._persistent,
                mandatory=self._mandatory,
            ),
        )

        # Raised rather than left in the result, because a caller who does not
        # read the result would otherwise carry on believing the message went
        # somewhere. Unroutable is the quietest failure AMQP has: the publish
        # succeeds, the consumer waits, and nothing anywhere says why.
        if self._mandatory and not result.routed:
            raise PublishError(
                outgoing.id,
                self._exchange,
                key,
                result.return_reason or "no queue is bound to receive it",
                unroutable=True,
            )
        return result


class Consumer:
    """A running subscription. Close it to stop.

    One is built by :meth:`Connection.consume` rather than directly, because it
    has to be started before it is any use and a half-built consumer that looks
    finished is a thing somebody will hold on to.
    """

    def __init__(
        self,
        connection: Connection,
        queue: str,
        handler: Handler,
        *,
        codec: Codec,
        retry: RetryPolicy,
        concurrency: int,
    ) -> None:
        if concurrency < 1:
            raise ValueError(f"acemq: concurrency must be at least 1, got {concurrency}")
        self._connection = connection
        self._queue = queue
        self._handler = handler
        self._codec = codec
        self._retry = retry
        self._concurrency = concurrency
        self._work: asyncio.Queue[Delivery | None] = asyncio.Queue()
        self._workers: list[asyncio.Task[None]] = []
        self._subscription: Subscription | None = None
        self._closed = False

    @property
    def queue(self) -> str:
        """The queue this consumer reads."""
        return self._queue

    async def _start(self, prefetch: int, tag: str, args: Mapping[str, Any]) -> None:
        self._workers = [
            asyncio.create_task(self._work_loop(), name=f"acemq-consumer-{self._queue}-{n}")
            for n in range(self._concurrency)
        ]
        try:
            self._subscription = await self._connection.transport.consume(
                self._queue,
                ConsumeSpec(prefetch=prefetch, tag=tag, args=dict(args)),
                self._deliver,
            )
        except BaseException:
            # Nothing was subscribed, so nothing will arrive; the workers would
            # otherwise sit on an empty queue for the life of the process.
            for _ in self._workers:
                self._work.put_nowait(None)
            await asyncio.gather(*self._workers)
            raise

    async def _deliver(self, delivery: Delivery) -> None:
        """Hands a delivery to the workers.

        Queued rather than handled here, so that ``concurrency`` means what it
        says: aio-pika starts a task per delivery and does not wait for the last
        one, so without a queue of our own a prefetch of twenty would run twenty
        handlers at once whatever the caller asked for. The queue needs no bound
        because prefetch is one: the broker will not send a twenty-first message
        until one of the twenty has been settled.
        """
        self._work.put_nowait(delivery)

    async def _work_loop(self) -> None:
        while True:
            delivery = await self._work.get()
            if delivery is None:
                return
            try:
                await self._handle(delivery)
            except Exception:
                # A failure here is this library's, not the handler's, and the
                # message is left unsettled on purpose: the broker will hand it
                # back rather than it disappearing because our own code broke.
                log.exception("acemq: could not settle a delivery from %s", self._queue)

    async def _handle(self, delivery: Delivery) -> None:
        envelope = Envelope.from_headers(delivery.headers, delivery.routing_key)

        try:
            payload = self._codec.decode(delivery.body, delivery.content_type)
        except Exception as failure:
            # A body that will not decode decodes no better next time, so this
            # goes straight to the dead-letter queue rather than round the retry
            # schedule until it ages out.
            await self._dead_letter(
                delivery, envelope, f"could not be decoded: {_describe(failure)}"
            )
            return

        message = Message(
            payload=payload,
            envelope=envelope,
            routing_key=delivery.routing_key,
            content_type=delivery.content_type,
            redelivered=delivery.redelivered,
            body=delivery.body,
        )

        try:
            returned = self._handler(message)
            decision = await returned if inspect.isawaitable(returned) else returned
        except Exception as failure:
            # An exception is how Python says a thing failed, so a handler that
            # raises is asking for the retry policy rather than confessing a
            # bug — which is what Java does with the same situation, and what a
            # handler written without reading the documentation will expect.
            # Raising FatalError is how it says the opposite, and the retry path
            # below reads that mark before it reads the request.
            decision = Ack(Action.RETRY, failure)

        if not isinstance(decision, Ack):
            await self._dead_letter(
                delivery,
                envelope,
                f"the handler returned {type(decision).__name__} instead of an Ack",
            )
            return

        await self._settle(delivery, envelope, decision)

    async def _settle(self, delivery: Delivery, envelope: Envelope, decision: Ack) -> None:
        if decision.action is Action.ACCEPT:
            await delivery.ack()
            return

        if decision.action is Action.REJECT:
            await self._dead_letter(
                delivery, envelope, f"the handler rejected it: {_describe(decision.error)}"
            )
            return

        if isinstance(decision.error, FatalError):
            # The handler asked for a retry but marked the reason as one that
            # will not change. Honouring the mark rather than the request is the
            # point of having it.
            await self._dead_letter(
                delivery,
                envelope,
                f"the handler reported an unprocessable message: {_describe(decision.error)}",
            )
            return

        delay = self._retry.next_delay(envelope.attempt, envelope.age)
        if delay is None:
            await self._dead_letter(
                delivery,
                envelope,
                f"{self._exhausted(envelope)}: {_describe(decision.error)}",
            )
            return

        if delay > ZERO:
            # Waiting here holds the delivery, and so holds one of this
            # consumer's prefetch slots. That is the honest cost of delaying a
            # retry without a queue per delay: the alternative is a rung of
            # timed queues, which is more moving parts than most services want
            # and is what naming.retry_queue is there for when they do.
            await asyncio.sleep(delay.total_seconds())

        await self._retry_again(delivery, envelope, delay)

    def _exhausted(self, envelope: Envelope) -> str:
        """Why there is no next attempt, in words an operator can act on."""
        if envelope.attempt >= self._retry.max_attempts:
            attempts = self._retry.max_attempts
            return f"exhausted {attempts} attempt{'' if attempts == 1 else 's'}"
        return f"exceeded the maximum message age of {self._retry.max_message_age}"

    async def _retry_again(
        self, delivery: Delivery, envelope: Envelope, delay: timedelta
    ) -> None:
        """Puts the message back on its own queue, one attempt further on.

        Republished rather than requeued, because a requeue returns the bytes
        the broker was given: the attempt header would still read what the
        publisher wrote however many times the message had come round, and the
        count would live only in this process's memory, which is the one place
        it is lost when the process that has been failing restarts.

        The cost is that the message goes to the back of the queue rather than
        the front, so a retry is no longer in order with its neighbours. For a
        message that has already failed once, that is the better trade.
        """
        next_attempt = envelope.with_(attempt=envelope.attempt + 1)
        delivered = await self._republish(delivery, self._queue, next_attempt)
        if not delivered:
            # The queue we are consuming has gone. Requeued rather than
            # acknowledged, because dropping it here would lose a message over a
            # broker change nobody told this consumer about.
            log.error(
                "acemq: cannot republish %s onto %s for attempt %d; returning it to the broker",
                envelope.id,
                self._queue,
                next_attempt.attempt,
            )
            await delivery.nack(True)
            return

        log.info(
            "acemq: retrying %s from %s, attempt %d of %d, after %s",
            envelope.id,
            self._queue,
            next_attempt.attempt,
            self._retry.max_attempts,
            delay,
        )
        await delivery.ack()

    async def _dead_letter(self, delivery: Delivery, envelope: Envelope, reason: str) -> None:
        """Sends the message to ``{queue}.dlq`` with the reason attached, and
        acknowledges the original.

        Acknowledging a message that failed looks wrong and is what makes this
        reliable: the message has already been safely republished somewhere
        else, so acknowledging the original is removing the copy that has been
        dealt with. Rejecting it instead would either requeue it into a hot loop
        or, with a dead-letter exchange configured on the queue, send it
        somewhere this consumer did not choose and without the reason.

        The reason travels as an envelope field, so a consumer of the
        dead-letter queue reads it back through the API rather than having to
        know the wire header name.
        """
        target = naming.dead_letter_queue(self._queue)
        failed = envelope.with_(error=reason)
        delivered = await self._republish(delivery, target, failed)
        if not delivered:
            # Rejected rather than acknowledged: without a dead-letter queue to
            # put it in, the broker's own dead-lettering is the last thing left
            # between this message and nothing.
            log.error(
                "acemq: cannot dead-letter %s to %s (%s); rejecting it to the broker instead",
                envelope.id,
                target,
                reason,
            )
            await delivery.nack(False)
            return

        log.warning(
            "acemq: dead-lettered %s from %s after %d attempts to %s: %s",
            envelope.id,
            self._queue,
            envelope.attempt,
            target,
            reason,
        )
        await delivery.ack()

    async def _republish(self, delivery: Delivery, queue: str, envelope: Envelope) -> bool:
        """Sends the original bytes to a queue by name, and says whether they
        arrived.

        Through the default exchange, which routes to the queue whose name
        matches the routing key, and mandatory so that a queue that is not there
        is an answer rather than a silence. The body goes back exactly as it
        came: re-encoding through a class that has since changed would replace
        what was committed with something else.
        """
        result = await self._connection.publish_raw(
            "",
            queue,
            Outbound(
                body=delivery.body,
                content_type=delivery.content_type or "",
                message_id=envelope.id,
                headers=envelope.to_headers(routing_key=delivery.routing_key),
                persistent=True,
                mandatory=True,
            ),
        )
        return result.routed

    async def close(self) -> None:
        """Stops the consumer and waits for the handlers already running.

        A message being worked on when this is called is finished and settled,
        rather than abandoned for the broker to hand to somebody else. A message
        that had been delivered but not started is given back instead: it is the
        broker's to hand to another consumer, and holding it here only to work
        through a retry delay would make closing take as long as the schedule.

        The subscription is released last, after everything has been settled,
        because a settlement travels on the channel its delivery arrived on.
        """
        if self._closed:
            return
        self._closed = True

        if self._subscription is not None:
            await self._subscription.stop()

        while True:
            try:
                pending = self._work.get_nowait()
            except asyncio.QueueEmpty:
                break
            if pending is not None:
                await pending.nack(True)

        for _ in self._workers:
            self._work.put_nowait(None)
        await asyncio.gather(*self._workers)

        if self._subscription is not None:
            await self._subscription.close()
        self._connection._untrack(self)

    async def __aenter__(self) -> Consumer:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class Connection:
    """A connection to a broker, and the defaults everything on it inherits.

    Built by :func:`connect` in the ordinary case. Taking a transport directly
    is for a fake in a test, or for a connection made with settings a URL cannot
    carry.

    :param transport: how to reach the broker
    :param codec: what publishers and consumers use unless they say otherwise
    :param origin: what to stamp on published messages, conventionally
        ``service@host``
    :param retry: what consumers use unless they say otherwise. The default is
        one delivery and no second chance, so a handler that calls
        ``retry()`` on a connection with no policy dead-letters the message and
        says so in the reason. That is deliberate: the alternative default is an
        immediate requeue, which is a hot loop against a broker that nobody
        asked for
    :param prefetch: how many unacknowledged messages a consumer holds
    """

    def __init__(
        self,
        transport: Transport,
        *,
        codec: Codec | None = None,
        origin: str | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int = DEFAULT_PREFETCH,
    ) -> None:
        if prefetch < 0:
            raise ValueError(f"acemq: prefetch must not be negative, got {prefetch}")
        self._transport = transport
        self._codec = codec or JsonCodec()
        self._origin = origin or default_origin()
        self._retry = retry or no_retry()
        self._prefetch = prefetch
        self._consumers: list[Consumer] = []
        self._closed = False

    @property
    def transport(self) -> Transport:
        """The transport underneath, for anything this library does not wrap."""
        return self._transport

    @property
    def codec(self) -> Codec:
        """The codec publishers and consumers use unless told otherwise."""
        return self._codec

    @property
    def origin(self) -> str:
        """What published messages are stamped with."""
        return self._origin

    @property
    def retry(self) -> RetryPolicy:
        """The retry policy consumers use unless told otherwise."""
        return self._retry

    async def declare(self, topology: Topology) -> None:
        """Applies a topology to this connection's broker.

        :param topology: what the broker should have
        """
        await topology.apply(self._transport)

    def publisher(
        self,
        exchange: str = "",
        routing_key: str = "",
        *,
        codec: Codec | None = None,
        persistent: bool = True,
        mandatory: bool = False,
    ) -> Publisher:
        """Builds a publisher for an exchange and routing key.

        Publish straight to a queue by leaving the exchange empty: the default
        exchange routes to the queue whose name matches the routing key.

        :param exchange: where to publish, empty for the default exchange
        :param routing_key: what to publish under, or a queue name
        :param codec: a codec other than this connection's
        :param persistent: ask the broker to write these messages to disk
        :param mandatory: fail when a message reaches no queue at all, rather
            than letting the broker drop it silently
        :returns: the publisher
        """
        return Publisher(
            self,
            exchange,
            routing_key,
            codec=codec,
            persistent=persistent,
            mandatory=mandatory,
        )

    async def consume(
        self,
        queue: str,
        handler: Handler,
        *,
        codec: Codec | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int | None = None,
        concurrency: int = 1,
        tag: str = "",
        args: Mapping[str, Any] | None = None,
    ) -> Consumer:
        """Reads messages from a queue until the returned consumer is closed.

        :param queue: what to read
        :param handler: what to do with a message; returning an
            :class:`~acemq_amqp.Ack` is the decision
        :param codec: a codec other than this connection's
        :param retry: a policy other than this connection's
        :param prefetch: how many unacknowledged messages to hold
        :param concurrency: how many messages to work on at once. One by
            default, which keeps a queue's messages in order; raising it trades
            that order for throughput, which is the right trade for handlers
            that spend their time waiting on something else
        :param tag: what to call this consumer to the broker
        :param args: broker-specific consumer arguments
        :returns: the running consumer
        """
        if self._closed:
            raise AceMQError("acemq: this connection is closed")

        consumer = Consumer(
            self,
            queue,
            handler,
            codec=codec or self._codec,
            retry=retry or self._retry,
            concurrency=concurrency,
        )
        self._consumers.append(consumer)
        try:
            await consumer._start(
                self._prefetch if prefetch is None else prefetch,
                tag,
                args or {},
            )
        except BaseException:
            self._untrack(consumer)
            raise
        return consumer

    async def publish_raw(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        """Sends bytes that are already encoded, with headers already rendered.

        For the retry and dead-letter hops, and for anything else replaying a
        message recorded earlier: the payload's class may not exist any more,
        and re-encoding it would produce different bytes from the ones that were
        committed. Ordinary publishing goes through :class:`Publisher`.

        :param exchange: where to publish, empty for the default exchange
        :param routing_key: what to publish under
        :param message: the message, encoded
        :returns: what the broker said
        """
        return await self._transport.publish(exchange, routing_key, message)

    async def queue_exists(self, queue: str) -> bool:
        """Whether a queue is on the broker, creating nothing."""
        return await self._admin().queue_exists(queue)

    async def message_count(self, queue: str) -> int:
        """How many messages are waiting on a queue."""
        return await self._admin().message_count(queue)

    async def delete_queue(self, queue: str) -> None:
        """Removes a queue and every message still on it."""
        await self._admin().delete_queue(queue)

    def _admin(self) -> QueueAdmin:
        if not isinstance(self._transport, QueueAdmin):
            raise AceMQError(
                f"acemq: the {type(self._transport).__name__} transport cannot manage queues"
            )
        return self._transport

    def _untrack(self, consumer: Consumer) -> None:
        if consumer in self._consumers:
            self._consumers.remove(consumer)

    async def close(self) -> None:
        """Stops every consumer on this connection and releases it.

        Consumers first, and their handlers allowed to finish, so a message
        being worked on when this is called is settled rather than returned to
        the queue for somebody else to redo.
        """
        if self._closed:
            return
        self._closed = True
        for consumer in list(self._consumers):
            await consumer.close()
        await self._transport.close()

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


async def connect(
    url: str,
    *,
    codec: Codec | None = None,
    origin: str | None = None,
    retry: RetryPolicy | None = None,
    prefetch: int = DEFAULT_PREFETCH,
    **transport_options: Any,
) -> Connection:
    """Opens a connection to a broker.

    The URL's scheme picks the transport. ``amqp://`` and ``amqps://`` need
    aio-pika, which is why it is imported here rather than by the package: a
    program that only reads AceMQ envelopes should not be made to install an
    AMQP client to import this library.

    :param url: where the broker is
    :param codec: what publishers and consumers use unless they say otherwise
    :param origin: what to stamp on published messages
    :param retry: what consumers use unless they say otherwise
    :param prefetch: how many unacknowledged messages a consumer holds
    :param transport_options: passed to the transport, which for RabbitMQ is
        :func:`aio_pika.connect_robust`
    :returns: the connection
    """
    scheme = urlsplit(url).scheme
    if scheme not in ("amqp", "amqps"):
        raise AceMQError(
            f"acemq: no transport knows how to reach {scheme or url!r}; "
            "this library speaks amqp:// and amqps://"
        )

    try:
        from .rabbitmq import RabbitMQTransport
    except ImportError as missing:  # pragma: no cover - depends on how it was installed
        raise AceMQError(
            "acemq: reaching a broker needs aio-pika, which is an optional extra. "
            'Install it with pip install "acemq-amqp[rabbitmq]"'
        ) from missing

    transport = await RabbitMQTransport.connect(url, **transport_options)
    return Connection(transport, codec=codec, origin=origin, retry=retry, prefetch=prefetch)


def default_origin() -> str:
    """``acemq@{hostname}``, which names the machine but not the service.

    Enough to tell two pods apart in a log, which is what an origin is for, and
    deliberately not a guess at the service name: a wrong service name in a
    message that has already been published is worse than an honest hostname.
    """
    return f"acemq@{socket.gethostname()}"


def _describe(failure: BaseException | None) -> str:
    """A failure as the sentence that goes in ``x-acemq-error``."""
    if failure is None:
        return "no reason given"
    text = str(failure)
    return type(failure).__name__ + (f": {text}" if text else "")
