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
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypeAlias
from urllib.parse import urlsplit

from . import naming
from .ack import Ack, Action, FatalError
from .codec import Codec, JsonCodec
from .envelope import Envelope
from .errors import AceMQError, PublishError
from .interceptors import (
    ConsumeContext,
    ConsumeInterceptor,
    PublishContext,
    PublishInterceptor,
    consume_chain,
    publish_chain,
)
from .retry import ZERO, RetryPolicy, no_retry
from .security import Security
from .telemetry import (
    METRIC_ACCEPTED,
    METRIC_CONSUMED,
    METRIC_DEAD_LETTERED,
    METRIC_HANDLER_DURATION,
    METRIC_IN_FLIGHT,
    METRIC_PARKED,
    METRIC_PUBLISH_FAILED,
    METRIC_PUBLISHED,
    METRIC_REJECTED,
    METRIC_RETRIED,
    METRIC_RUNG_MISSING,
    METRIC_SET_ASIDE_FAILED,
    HealthReport,
    HealthStatus,
    NullObserver,
    Observer,
)
from .topology import Topology
from .transport import (
    ConsumeSpec,
    Delivery,
    MessageSource,
    Outbound,
    PublishResult,
    QueueAdmin,
    QueueSpec,
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

#: A handler that is definitely a coroutine function, which every handler
#: :mod:`acemq_amqp.patterns` builds is.
#:
#: It is a :data:`Handler` and can be passed anywhere one can. The narrower name
#: exists so that a caller who wants to run a wrapped handler directly — a test,
#: usually — can await it without mypy pointing out that the wider type might not
#: be awaitable.
AsyncHandler: TypeAlias = Callable[[Message], Awaitable[Ack]]


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
        if not outgoing.origin:
            # A message derived from another — a reply, a pipeline hop, the next
            # stop on a routing slip — is still published by this process, and an
            # origin naming the service before it would be a lie in a log line
            # somebody is going to trust.
            outgoing = outgoing.with_(origin=self._connection.origin)

        context = PublishContext(
            exchange=self._exchange,
            routing_key=key,
            envelope=outgoing,
            payload=payload,
            persistent=self._persistent,
            mandatory=self._mandatory,
        )
        # Read here rather than at construction, so an interceptor registered
        # during start-up applies to the publishers that already exist. Building
        # the chain per message costs a few closures and buys that.
        return await publish_chain(self._connection.publish_interceptors, self._send)(context)

    async def _send(self, context: PublishContext) -> PublishResult:
        """The innermost work: encode what the interceptors left, and send it."""
        # Encoded here rather than before the chain ran, so an interceptor can
        # change the payload as well as its metadata. A codec that has already
        # run leaves an interceptor with bytes and nothing it can do to them.
        body = self._codec.encode(context.payload)
        observer = self._connection.observer
        labels = {"exchange": context.exchange, "key": context.routing_key}

        try:
            result = await self._connection.publish_raw(
                context.exchange,
                context.routing_key,
                Outbound(
                    body=body,
                    content_type=self._codec.content_type,
                    message_id=context.envelope.id,
                    headers=context.envelope.to_headers(routing_key=context.routing_key),
                    persistent=context.persistent,
                    mandatory=context.mandatory,
                ),
            )
        except Exception:
            observer.count(METRIC_PUBLISH_FAILED, 1, labels)
            raise

        # Raised rather than left in the result, because a caller who does not
        # read the result would otherwise carry on believing the message went
        # somewhere. Unroutable is the quietest failure AMQP has: the publish
        # succeeds, the consumer waits, and nothing anywhere says why.
        if context.mandatory and not result.routed:
            observer.count(METRIC_PUBLISH_FAILED, 1, labels)
            raise PublishError(
                context.envelope.id,
                context.exchange,
                context.routing_key,
                result.return_reason or "no queue is bound to receive it",
                unroutable=True,
            )

        observer.count(METRIC_PUBLISHED, 1, labels)
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
        self._in_flight = 0
        self._closed = False

    @property
    def queue(self) -> str:
        """The queue this consumer reads."""
        return self._queue

    @property
    def closed(self) -> bool:
        """Whether this consumer has been stopped."""
        return self._closed

    @property
    def running(self) -> bool:
        """Whether this consumer is subscribed and still has workers to hand a
        delivery to.

        What a health check reads. A consumer whose workers have all finished
        without it being closed is a consumer the broker is still sending
        messages to and nothing is reading — which looks identical to a quiet
        queue from outside, and is the failure worth telling a probe about.
        """
        return (
            not self._closed
            and self._subscription is not None
            and any(not worker.done() for worker in self._workers)
        )

    @property
    def in_flight(self) -> int:
        """How many messages this consumer is working on right now."""
        return self._in_flight

    @property
    def _labels(self) -> dict[str, str]:
        return {"queue": self._queue}

    @property
    def _observer(self) -> Observer:
        return self._connection.observer

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
        self._observer.count(METRIC_CONSUMED, 1, self._labels)
        self._in_flight += 1
        self._observer.gauge(METRIC_IN_FLIGHT, self._in_flight, self._labels)
        try:
            await self._handle_one(delivery, envelope)
        finally:
            self._in_flight -= 1
            self._observer.gauge(METRIC_IN_FLIGHT, self._in_flight, self._labels)

    async def _handle_one(self, delivery: Delivery, envelope: Envelope) -> None:
        try:
            payload = self._codec.decode(delivery.body, delivery.content_type)
        except Exception as failure:
            # A body that will not decode decodes no better next time, so it
            # does not go round the retry schedule until it ages out.
            #
            # Parked rather than dead-lettered: a message that failed five times
            # and a message nothing could read are different problems with
            # different answers — one is usually the world, the other is usually
            # a producer — and whoever drains the dead letters should not have
            # to sort them by hand.
            await self._park(
                delivery, envelope, f"could not be decoded: {_describe(failure)}"
            )
            return

        context = ConsumeContext(
            queue=self._queue,
            envelope=envelope,
            payload=payload,
            body=delivery.body,
            content_type=delivery.content_type,
            routing_key=delivery.routing_key,
            redelivered=delivery.redelivered,
        )

        started = time.monotonic()
        try:
            decision = await consume_chain(
                self._connection.consume_interceptors, self._run_handler
            )(context)
        except Exception as failure:
            # An exception is how Python says a thing failed, so a handler that
            # raises is asking for the retry policy rather than confessing a
            # bug — which is what Java does with the same situation, and what a
            # handler written without reading the documentation will expect.
            # Raising FatalError is how it says the opposite, and the retry path
            # below reads that mark before it reads the request.
            #
            # An interceptor that raises lands here too, and deliberately: a
            # refused message is retried and then dead-lettered rather than
            # acknowledged as though something had processed it.
            decision = Ack(Action.RETRY, failure)
        finally:
            # Timed around the interceptors as well as the handler, because
            # what an operator wants to know is how long a message takes to
            # deal with, and an interceptor that opens a transaction is part of
            # dealing with it.
            self._observer.observe(
                METRIC_HANDLER_DURATION, time.monotonic() - started, self._labels
            )

        # Whatever the interceptors left, rather than what arrived: one that
        # rewrote the envelope on the way in meant that rewrite for the retry
        # and the dead letter as much as for the handler.
        envelope = context.envelope

        if not isinstance(decision, Ack):
            await self._dead_letter(
                delivery,
                envelope,
                f"the handler returned {type(decision).__name__} instead of an Ack",
            )
            return

        await self._settle(delivery, envelope, decision)

    async def _run_handler(self, context: ConsumeContext) -> Ack:
        """The innermost work: build the message the interceptors left, and run
        the handler on it.

        The message is built here rather than before the chain, so that an
        interceptor which changed the envelope or the payload changed what the
        handler is given rather than a copy nobody reads.
        """
        message = Message(
            payload=context.payload,
            envelope=context.envelope,
            routing_key=context.routing_key,
            content_type=context.content_type,
            redelivered=context.redelivered,
            body=context.body,
        )
        returned = self._handler(message)
        # Not an Ack is a mistake worth reporting, but it is not this method's
        # to report: it is returned as it stands and _handle dead-letters it
        # with a reason, which is where every other unhandleable delivery goes.
        # An interceptor between here and there sees the same thing the handler
        # returned, which is the honest thing to show it.
        decision: Ack = await returned if inspect.isawaitable(returned) else returned
        return decision

    async def _settle(self, delivery: Delivery, envelope: Envelope, decision: Ack) -> None:
        if decision.action is Action.ACCEPT:
            self._observer.count(METRIC_ACCEPTED, 1, self._labels)
            await delivery.ack()
            return

        if decision.action is Action.REJECT:
            self._observer.count(METRIC_REJECTED, 1, self._labels)
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

        wait = self._retry.next_wait(envelope.attempt, envelope.age)
        if wait is None:
            await self._dead_letter(
                delivery,
                envelope,
                f"{self._exhausted(envelope)}: {_describe(decision.error)}",
            )
            return

        if wait.in_broker and await self._retry_in_broker(delivery, envelope, wait.delay):
            return

        if wait.delay > ZERO:
            # Waiting here holds the delivery, and so holds one of this
            # consumer's prefetch slots. For a wait of a few seconds that is the
            # cheaper of the two costs; for a longer one it is not, which is why
            # the policy has a threshold and the long waits went to a rung queue
            # a few lines above.
            await asyncio.sleep(wait.delay.total_seconds())

        await self._retry_again(delivery, envelope, wait.delay)

    def _exhausted(self, envelope: Envelope) -> str:
        """Why there is no next attempt, in words an operator can act on."""
        if envelope.attempt >= self._retry.max_attempts:
            attempts = self._retry.max_attempts
            return f"exhausted {attempts} attempt{'' if attempts == 1 else 's'}"
        return f"exceeded the maximum message age of {self._retry.max_message_age}"

    async def _retry_in_broker(
        self, delivery: Delivery, envelope: Envelope, delay: timedelta
    ) -> bool:
        """Parks the message on ``{queue}.retry.{delay}`` and lets the broker
        return it.

        The rung's ``x-message-ttl`` is the delay and its dead-letter target is
        this queue, so the wait costs nothing here: no delivery held, no prefetch
        slot spent, and — the reason it exists — nothing lost when this process
        restarts halfway through. A consumer sleeping on a five-minute backoff
        that dies at minute one does not resume at minute one; the broker
        redelivers the unacknowledged message immediately, and the policy that
        said five minutes delivers in none.

        The obvious simplification is one rung queue and a per-message
        ``expiration`` instead of several with fixed TTLs. It does not work.
        RabbitMQ only ever expires the message at the *head* of a queue, so a
        message with a ten-minute TTL at the front holds back every thirty-second
        one behind it, and the delays that come out bear no resemblance to the
        ones that went in. Never set a per-message TTL for this.

        The attempt advances here exactly as it does on an immediate retry: the
        counter belongs to the message, and a message that has been round the
        broker is no less on its second attempt than one that waited here.

        :returns: whether the rung took it. ``False`` means no such queue, and
            the caller should fall back to waiting here
        """
        rung = naming.retry_queue(self._queue, delay)
        next_attempt = envelope.with_(attempt=envelope.attempt + 1)
        if not await self._republish(delivery, rung, next_attempt):
            # Degraded rather than fatal: the message is still deliverable, and
            # waiting for it here is what this library did before there were
            # rungs. Loud, because a topology that declares the queue without its
            # rungs will otherwise look like it works right up until a long
            # backoff quietly becomes a held prefetch slot.
            # Counted as well as logged, because this is the one failure here
            # that nothing else shows: the message is still retried and the wait
            # still happens, so a dashboard reads as normal while the reason the
            # rung exists is quietly gone.
            self._observer.count(METRIC_RUNG_MISSING, 1, {**self._labels, "rung": rung})
            log.error(
                "acemq: %s is not on the broker, so %s will wait %s in this consumer "
                "instead; declare it with Topology().queue(%r, retry=policy)",
                rung,
                envelope.id,
                delay,
                self._queue,
            )
            return False

        self._observer.count(METRIC_RETRIED, 1, {**self._labels, "where": "broker"})
        log.info(
            "acemq: retrying %s from %s, attempt %d of %d, after %s in %s",
            envelope.id,
            self._queue,
            next_attempt.attempt,
            self._retry.max_attempts,
            delay,
            rung,
        )
        await delivery.ack()
        return True

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

        self._observer.count(METRIC_RETRIED, 1, {**self._labels, "where": "consumer"})
        log.info(
            "acemq: retrying %s from %s, attempt %d of %d, after %s",
            envelope.id,
            self._queue,
            next_attempt.attempt,
            self._retry.max_attempts,
            delay,
        )
        await delivery.ack()

    async def _park(self, delivery: Delivery, envelope: Envelope, reason: str) -> None:
        """Sends the message to ``{queue}.parked`` with the reason attached.

        Where a message goes when it never reached the handler at all. Somebody
        has to look at it, and what they need to know first is that it was
        unreadable rather than unlucky.
        """
        self._observer.count(METRIC_PARKED, 1, self._labels)
        await self._set_aside(delivery, envelope, naming.parked_queue(self._queue), reason)

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
        self._observer.count(METRIC_DEAD_LETTERED, 1, self._labels)
        await self._set_aside(
            delivery, envelope, naming.dead_letter_queue(self._queue), reason
        )

    async def _set_aside(
        self, delivery: Delivery, envelope: Envelope, target: str, reason: str
    ) -> None:
        """Republishes to ``target`` with the reason, then acknowledges."""
        failed = envelope.with_(error=reason)
        delivered = await self._republish(delivery, target, failed)
        if not delivered:
            # Rejected rather than acknowledged: without a dead-letter queue to
            # put it in, the broker's own dead-lettering is the last thing left
            # between this message and nothing.
            self._observer.count(
                METRIC_SET_ASIDE_FAILED, 1, {**self._labels, "target": target}
            )
            log.error(
                "acemq: cannot move %s to %s (%s); rejecting it to the broker instead",
                envelope.id,
                target,
                reason,
            )
            await delivery.nack(False)
            return

        log.warning(
            "acemq: set aside %s from %s after %d attempts, into %s: %s",
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
    :param on_publish: cross-cutting behaviour to wrap every publish on this
        connection in, outermost first. See :mod:`acemq_amqp.interceptors`
    :param on_consume: the same around every handler.
        :meth:`intercept_publish` and :meth:`intercept_consume` add more later
    :param observer: where the numbers go. Nothing by default, so a service
        that wants none pays for none. See :mod:`acemq_amqp.telemetry`
    """

    def __init__(
        self,
        transport: Transport,
        *,
        codec: Codec | None = None,
        origin: str | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int = DEFAULT_PREFETCH,
        on_publish: Sequence[PublishInterceptor] = (),
        on_consume: Sequence[ConsumeInterceptor] = (),
        observer: Observer | None = None,
    ) -> None:
        if prefetch < 0:
            raise ValueError(f"acemq: prefetch must not be negative, got {prefetch}")
        self._transport = transport
        self._observer: Observer = observer or NullObserver()
        self._codec = codec or JsonCodec()
        self._origin = origin or default_origin()
        self._retry = retry or no_retry()
        self._prefetch = prefetch
        self._consumers: list[Consumer] = []
        self._publishing: list[PublishInterceptor] = list(on_publish)
        self._consuming: list[ConsumeInterceptor] = list(on_consume)
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

    @property
    def observer(self) -> Observer:
        """Where this connection's numbers go."""
        return self._observer

    @property
    def consumers(self) -> tuple[Consumer, ...]:
        """The consumers running on this connection.

        A snapshot, and public because a health check that has to say whether
        the consumers are alive should not need private access to find them —
        the same rule an interceptor follows.
        """
        return tuple(self._consumers)

    @property
    def closed(self) -> bool:
        """Whether this connection has been closed."""
        return self._closed

    async def health(self) -> HealthReport:
        """Whether this connection can reach its broker, and its consumers run.

        The broker half is a declaration of a temporary queue, which is the
        cheapest thing AMQP offers that actually proves the connection works. A
        TCP connection that is open but wedged — the broker paused, the network
        black-holing — answers a socket-level check exactly as a healthy one
        does, right up until something is asked of it.

        The consumer half is the one a probe usually wants and nothing outside
        can see. A consumer whose workers have died without it being closed is
        one the broker is still sending messages to and nothing is reading, and
        from outside that is indistinguishable from a quiet queue.

        It costs a round trip, so it is not something to call per request. Wire
        it to a readiness probe and let the probe's interval decide how often.

        :returns: the report. Degraded means working but worth an alert
        """
        checked = datetime.now(timezone.utc)
        consumers = self.consumers
        stalled = [consumer.queue for consumer in consumers if not consumer.running]
        parts: dict[str, Any] = {
            "consumers": len(consumers),
            "in-flight": sum(consumer.in_flight for consumer in consumers),
        }

        if self._closed:
            return HealthReport(
                HealthStatus.DOWN, "the connection has been closed", checked, parts
            )

        # Asking whether a queue nothing has ever declared exists, rather than
        # declaring a temporary one. It is the same round trip and it proves the
        # same thing, and it creates nothing at all: an exclusive queue is only
        # released when the channel that declared it closes, so a probe that
        # declared one would leave a queue on the broker for the life of the
        # connection and a new one behind every restart.
        probe = f"acemq-health-{uuid.uuid4().hex}"
        started = time.monotonic()
        try:
            await self._probe(probe)
        except Exception as failure:
            parts["error"] = _describe(failure)
            return HealthReport(
                HealthStatus.DOWN, f"the broker did not answer: {failure}", checked, parts
            )
        parts["round-trip"] = round(time.monotonic() - started, 4)

        if stalled:
            # Degraded rather than down: the connection works, and a replacement
            # instance would almost certainly stall the same way. Worth waking
            # somebody; not worth taking this one out of rotation.
            return HealthReport(
                HealthStatus.DEGRADED,
                "these consumers have stopped reading: " + ", ".join(sorted(stalled)),
                checked,
                {**parts, "stalled": sorted(stalled)},
            )
        return HealthReport(HealthStatus.UP, "", checked, parts)

    async def _probe(self, name: str) -> None:
        """One round trip to the broker that leaves nothing behind.

        A transport that cannot be asked whether a queue exists gets the next
        cheapest thing: a temporary queue, removed again straight away. Both
        prove the same thing, which is that the connection answers rather than
        merely being open.
        """
        if isinstance(self._transport, QueueAdmin):
            await self._transport.queue_exists(name)
            return
        await self._transport.declare_queue(
            name, QueueSpec(durable=False, auto_delete=True, exclusive=True)
        )

    @property
    def publish_interceptors(self) -> tuple[PublishInterceptor, ...]:
        """What wraps every publish, outermost first.

        A snapshot rather than the list itself, so a publish in progress cannot
        be handed a chain that is being edited underneath it.
        """
        return tuple(self._publishing)

    @property
    def consume_interceptors(self) -> tuple[ConsumeInterceptor, ...]:
        """What wraps every handler, outermost first."""
        return tuple(self._consuming)

    def intercept_publish(self, interceptor: PublishInterceptor) -> Connection:
        """Wraps every publish on this connection in ``interceptor``.

        Registered ones run in the order they were added, so this one is
        innermost until the next is added. Read at publish time rather than
        copied into each publisher, so an interceptor registered during start-up
        applies to publishers that already exist — which is what makes it usable
        from a framework's wiring rather than only from the line that connects.

        Adding at start-up is still what to do. A message already in flight will
        not see it, and an interceptor whose whole job is to stamp every message
        is not doing it if the first few went without.

        :param interceptor: what to wrap the publish in. See
            :mod:`acemq_amqp.interceptors`
        :returns: this connection, so registrations read as one expression
        """
        self._publishing.append(interceptor)
        return self

    def intercept_consume(self, interceptor: ConsumeInterceptor) -> Connection:
        """Wraps every handler on this connection in ``interceptor``.

        :param interceptor: what to wrap the handler in
        :returns: this connection
        """
        self._consuming.append(interceptor)
        return self

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

    async def pull(self, queue: str) -> Delivery | None:
        """Takes one message off a queue, or ``None`` when there is none waiting.

        For tools rather than for services: it is a round trip per message where
        :meth:`consume` gets a stream. What it can do that a consumer cannot is
        say that a queue is empty, which is what anything working through a
        backlog and then stopping — a replay, a drain — has to know.

        The delivery comes back unsettled, so the caller decides whether it is
        gone or goes back.

        :param queue: what to read from
        :returns: the delivery, or ``None``
        """
        return await self._source().pull(queue)

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

    def _source(self) -> MessageSource:
        if not isinstance(self._transport, MessageSource):
            raise AceMQError(
                f"acemq: the {type(self._transport).__name__} transport cannot be "
                "asked for one message at a time"
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


@dataclass(frozen=True, slots=True)
class BrokerHealth:
    """A connection as a :class:`~acemq_amqp.telemetry.HealthCheck`.

    So that a broker check and an application's own — a database, a downstream
    service — can be handed to
    :func:`~acemq_amqp.telemetry.aggregate_health` together, which is what a
    readiness endpoint actually needs.

    :param connection: what to ask
    :param label: what to call it in a combined report
    """

    connection: Connection
    label: str = "broker"

    @property
    def name(self) -> str:
        return self.label

    async def check(self) -> HealthReport:
        return await self.connection.health()


async def connect(
    url: str,
    *,
    codec: Codec | None = None,
    origin: str | None = None,
    retry: RetryPolicy | None = None,
    prefetch: int = DEFAULT_PREFETCH,
    security: Security | None = None,
    on_publish: Sequence[PublishInterceptor] = (),
    on_consume: Sequence[ConsumeInterceptor] = (),
    observer: Observer | None = None,
    **transport_options: Any,
) -> Connection:
    """Opens a connection to a broker.

    The URL's scheme picks the transport, and says whether the connection is
    encrypted: ``amqps://`` is, ``amqp://`` is not. Both need aio-pika, which is
    why it is imported here rather than by the package: a program that only
    reads AceMQ envelopes should not be made to install an AMQP client to import
    this library.

    An ``amqps://`` URL is verified against the machine's trust store and will
    not speak anything older than TLS 1.2, with or without a ``security``. Pass
    one to name a different authority, to present a client certificate, or to
    supply the login separately from the URL — which is how a password stays out
    of a connection string that ends up in a log.

    :param url: where the broker is
    :param codec: what publishers and consumers use unless they say otherwise
    :param origin: what to stamp on published messages
    :param retry: what consumers use unless they say otherwise
    :param prefetch: how many unacknowledged messages a consumer holds
    :param security: how to verify the broker and who to log in as. See
        :class:`~acemq_amqp.Security`
    :param on_publish: what to wrap every publish in, outermost first. See
        :mod:`acemq_amqp.interceptors`
    :param on_consume: what to wrap every handler in
    :param observer: where the numbers go. See :mod:`acemq_amqp.telemetry`
    :param transport_options: passed to the transport, which for RabbitMQ is
        :func:`aio_pika.connect_robust`
    :returns: the connection
    :raises SecurityError: when the security settings cannot be honoured, before
        anything is dialled
    """
    scheme = urlsplit(url).scheme
    if scheme not in ("amqp", "amqps"):
        raise AceMQError(
            f"acemq: no transport knows how to reach {scheme or url!r}; "
            "this library speaks amqp:// and amqps://"
        )

    # Resolved here rather than in the transport, so that a missing certificate
    # or an unset password variable is an exception from connect() with the
    # setting named in it, rather than a handshake failure from somewhere in
    # aio-pika that says only that the socket closed.
    settings = security or Security()
    url = settings.applied_to(url)
    # The caller's own options win: transport_options is the escape hatch for
    # everything this library does not model, and a Security built from defaults
    # should not quietly overrule an ssl_context somebody passed by hand.
    transport_options = {**settings.transport_options(url), **transport_options}

    try:
        from .rabbitmq import RabbitMQTransport
    except ImportError as missing:  # pragma: no cover - depends on how it was installed
        raise AceMQError(
            "acemq: reaching a broker needs aio-pika, which is an optional extra. "
            'Install it with pip install "acemq-amqp[rabbitmq]"'
        ) from missing

    transport = await RabbitMQTransport.connect(url, **transport_options)
    return Connection(
        transport,
        codec=codec,
        origin=origin,
        retry=retry,
        prefetch=prefetch,
        on_publish=on_publish,
        on_consume=on_consume,
        observer=observer,
    )


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
