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
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypeAlias
from urllib.parse import urlsplit

from . import naming
from .ack import (
    OUTCOME_ACKED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_PARKED,
    OUTCOME_REJECTED,
    OUTCOME_RETRIED,
    Ack,
    Action,
    FatalError,
    Settlement,
)
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
from .retry import ZERO, RetryPolicy, Wait, no_retry
from .security import Security
from .telemetry import (
    METRIC_CONSUME_ATTEMPTS,
    METRIC_CONSUME_DURATION,
    METRIC_CONSUME_IN_FLIGHT,
    METRIC_CONSUME_TOTAL,
    METRIC_DEAD_LETTERED_TOTAL,
    METRIC_PUBLISH_DURATION,
    METRIC_PUBLISH_TOTAL,
    METRIC_RETRIED_TOTAL,
    METRIC_RUNG_MISSING,
    METRIC_SET_ASIDE_FAILED,
    OUTCOME_CONFIRMED,
    OUTCOME_FAILED,
    OUTCOME_UNROUTABLE,
    TAG_EXCHANGE,
    TAG_OUTCOME,
    TAG_QUEUE,
    TAG_ROUTING_KEY,
    TAG_TARGET,
    HealthReport,
    HealthStatus,
    NullObserver,
    Observer,
)
from .topology import Topology, declare_where_failures_go
from .transport import (
    BlockedState,
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

#: How many publishes may be waiting for the broker at once, unless a connection
#: says otherwise.
#:
#: Java's ``maxOutstandingPublishes`` and .NET's ``MaxOutstandingPublishes``
#: default to the same thousand, and the number is the same one for the same
#: reason: enough to keep a broker busy, small enough that the memory it can cost
#: is bounded and obvious. A publisher that wants more should say so rather than
#: inherit it.
DEFAULT_MAX_OUTSTANDING_PUBLISHES = 1000

#: How long a publish waits for room when every permit is taken, unless a
#: connection says otherwise. Java's ``confirmTimeout``, and the same ten seconds.
DEFAULT_CONFIRM_TIMEOUT = timedelta(seconds=10)

#: How long :meth:`Connection.health` gives the broker to answer its probe.
#:
#: Shorter than the five seconds :func:`~acemq_amqp.telemetry.aggregate_health`
#: gives a whole set of checks, and deliberately: the connection's own answer for
#: a broker that has stopped answering is the careful one — up when the broker
#: has blocked this connection, down when it has simply gone — and an aggregate
#: whose deadline expired first would replace it with a flat "did not answer
#: within 5s". The inner deadline has to run out first for the outer one to have
#: anything to report.
DEFAULT_HEALTH_TIMEOUT = 3.0

#: What a health report says about a connection the broker has blocked. It is
#: reported **up**, which is the one place this disagrees with a naive reading of
#: the state: a blocked connection is the broker protecting itself from a disk or
#: memory alarm, and an application that fails its own readiness check for it is
#: one an orchestrator restarts into the same blocked broker, having thrown away
#: whatever it was holding. Java's ``AceMqHealthIndicator`` and Go's health check
#: make the same call for the same reason.
BLOCKED_DETAIL = (
    "the broker has blocked this connection, usually because it is low on disk or "
    "memory. Reported up on purpose: restarting into a broker that is still blocked "
    "helps nobody, and this instance is still serving"
)


@dataclass(frozen=True, slots=True)
class Message:
    """A delivery that has been decoded.

    :param payload: the body, read through the codec
    :param envelope: the metadata that travelled with it
    :param routing_key: the key it arrived under
    :param content_type: what the sender said the body was, or ``None``
    :param redelivered: the broker saying it has handed this one over before
    :param body: the undecoded bytes, for a handler that wants to see them
    :param reply_to: AMQP's own ``reply-to`` property, empty when the sender set
        none. A responder reads it as the fallback behind the
        ``acemq-reply-to`` header, which is what lets a Java or .NET caller —
        neither of which writes the header — be answered by a Python responder
    """

    payload: Any
    envelope: Envelope
    routing_key: str
    content_type: str | None
    redelivered: bool
    body: bytes
    reply_to: str = ""


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
        reply_to: str = "",
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
        :param reply_to: where an answer should come back to, written to AMQP's
            own ``reply-to`` property. Only :mod:`~acemq_amqp.patterns` request
            and reply sets it, and it sets the ``acemq-reply-to`` header to the
            same value: the header is what survives a service that rebuilds the
            message, the property is what the other four libraries read
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
            reply_to=reply_to,
        )
        # Read here rather than at construction, so an interceptor registered
        # during start-up applies to the publishers that already exist. Building
        # the chain per message costs a few closures and buys that.
        return await publish_chain(self._connection.publish_interceptors, self._send)(context)

    async def send_all(self, payloads: Iterable[Any]) -> list[PublishResult]:
        """Publishes a batch and waits for every confirm.

        What most bulk publishing actually wants: the throughput of pipelining
        with the safety of having waited. Every message goes out before any
        confirm is awaited, and only then are all of them checked together.
        Awaiting each send in turn is a broker round trip per message — the
        loop a caller could have written for themselves, and none of the
        throughput a batch exists for.

        The results come back in the order the payloads were given, whatever
        order the broker answers in.

        **This is not atomic.** AMQP has no such thing: there is no way to
        publish a hundred messages such that all or none arrive, and a library
        that offered one would be lying. A failure does not abandon the rest
        either — every send is awaited whatever an earlier one did, so a batch
        that half succeeded can say how many arrived. That count is the point:
        a caller told only "it failed" resends the messages the broker already
        has.

        How wide the batch gets on the wire is capped, and by the connection
        rather than by this method:
        :attr:`Connection.max_outstanding_publishes` is a thousand by default,
        and the thousand-and-first task waits for one of the first thousand to
        be answered before its message is written. A list of a million is
        therefore a million tasks and a thousand messages in flight rather than
        a million, which is backpressure instead of a memory leak — but a
        million tasks is still a million tasks, so chunk what comes out of a
        database rather than handing a cursor's worth of rows to one call.

        :param payloads: what to send, in order, each encoded by this
            publisher's codec
        :returns: what the broker said about each, in the order the payloads
            were given
        :raises PublishError: when any message was not confirmed. Its message
            names how many failed and how many did not, and ``__cause__``
            carries the first failure in payload order — which is not
            necessarily the first one the broker answered. A batch wide enough
            that the broker stops keeping up fails this way too, with the first
            failure saying so
        """
        batch = list(payloads)
        # Everything is started before anything is awaited. A task per payload,
        # all of them created before the gather below, is what puts the whole
        # batch on the wire while the broker is still working through the
        # first confirm.
        sends = [asyncio.create_task(self.send(payload)) for payload in batch]
        # return_exceptions, and not for tidiness: without it the first failure
        # comes out of the gather while the rest of the batch is still in
        # flight, which loses the count this method exists to report and leaves
        # the others' exceptions unretrieved for asyncio to complain about
        # later.
        settled = await asyncio.gather(*sends, return_exceptions=True)

        results: list[PublishResult] = []
        first_failure: BaseException | None = None
        failed = 0
        for outcome in settled:
            if isinstance(outcome, BaseException):
                failed += 1
                if first_failure is None:
                    first_failure = outcome
            else:
                results.append(outcome)

        if first_failure is not None:
            # Named rather than printed. A failure with nothing in its message —
            # the CancelledError every task gets when the connection closes
            # underneath the batch is the common one — renders as an empty
            # string, and the sentence then ends "The first failure was: " and
            # stops, promising an explanation it does not give. The class name
            # is not much, but it is the difference between "the sends were
            # cancelled" and nothing at all.
            described = str(first_failure) or type(first_failure).__name__
            # The counts matter. A batch that half succeeded is the ordinary
            # outcome of a broker problem partway through, and this sentence is
            # word for word the one Java and .NET raise, so that an operator
            # reading a log has one thing to recognise rather than three.
            raise PublishError(
                "",
                self._exchange,
                self._routing_key,
                described,
                summary=(
                    f"{failed} of {len(batch)} messages were not confirmed;"
                    f" {len(results)} were. The first failure was: {described}"
                ),
            ) from first_failure
        return results

    async def _send(self, context: PublishContext) -> PublishResult:
        """The innermost work: encode what the interceptors left, and send it."""
        observer = self._connection.observer
        # ``routing.key`` and not ``key``: Java and .NET both tag a publish with
        # the fully-qualified name, and a dashboard that groups by it should
        # read the same in all five languages.
        labels = {TAG_EXCHANGE: context.exchange, TAG_ROUTING_KEY: context.routing_key}
        # Started before the encode, because everything from here on is time the
        # caller spends inside send(): the codec, the wait for a permit and the
        # broker's confirm are all things that make a publish slow, and a timer
        # that began after the encode would hide a slow codec completely.
        started = time.monotonic()

        def record(outcome: str) -> None:
            """The pair, always together and always with the same labels.

            Two calls rather than one because the interface has two methods, and
            in this order because a dashboard dividing the duration's count into
            the total should never see the total lead it.
            """
            observer.observe(
                METRIC_PUBLISH_DURATION,
                time.monotonic() - started,
                {**labels, TAG_OUTCOME: outcome},
            )
            observer.count(METRIC_PUBLISH_TOTAL, 1, {**labels, TAG_OUTCOME: outcome})

        # Encoded here rather than before the chain ran, so an interceptor can
        # change the payload as well as its metadata. A codec that has already
        # run leaves an interceptor with bytes and nothing it can do to them.
        try:
            body = self._codec.encode(context.payload)
            result = await self._connection.publish_raw(
                context.exchange,
                context.routing_key,
                Outbound(
                    body=body,
                    content_type=self._codec.content_type,
                    message_id=context.envelope.id,
                    reply_to=context.reply_to,
                    headers=context.envelope.to_headers(routing_key=context.routing_key),
                    persistent=context.persistent,
                    mandatory=context.mandatory,
                ),
            )
        except Exception:
            record(OUTCOME_FAILED)
            raise

        # Raised rather than left in the result, because a caller who does not
        # read the result would otherwise carry on believing the message went
        # somewhere. Unroutable is the quietest failure AMQP has: the publish
        # succeeds, the consumer waits, and nothing anywhere says why.
        if context.mandatory and not result.routed:
            record(OUTCOME_UNROUTABLE)
            raise PublishError(
                context.envelope.id,
                context.exchange,
                context.routing_key,
                result.return_reason or "no queue is bound to receive it",
                unroutable=True,
            )

        record(OUTCOME_CONFIRMED)
        return result


class Consumer:
    """A running subscription. Close it to stop.

    One is built by :meth:`Connection.consume` rather than directly, because it
    has to be started before it is any use and a half-built consumer that looks
    finished is a thing somebody will hold on to.

    Starting one declares the queues it will need when a message fails — its
    dead-letter queue, its parking lot and its rungs — unless it was told not
    to. See :func:`acemq_amqp.topology.declare_where_failures_go`.
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
        declare: bool = True,
    ) -> None:
        if concurrency < 1:
            raise ValueError(f"acemq: concurrency must be at least 1, got {concurrency}")
        self._connection = connection
        self._queue = queue
        self._handler = handler
        self._codec = codec
        self._retry = retry
        self._concurrency = concurrency
        self._declare = declare
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
        return {TAG_QUEUE: self._queue}

    @property
    def _observer(self) -> Observer:
        return self._connection.observer

    async def _start(self, prefetch: int, tag: str, args: Mapping[str, Any]) -> None:
        if self._declare:
            # Before the subscription rather than after it, so that the first
            # message this consumer gives up on already has somewhere to go. A
            # broker that refuses one of these declarations is a consumer that
            # does not start, which is the honest failure: it would otherwise
            # start and lose its first dead letter.
            await declare_where_failures_go(
                self._connection.transport, self._queue, self._retry
            )

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
        # No counter for "a message arrived": every delivery is counted once
        # when it is settled, on ``acemq.consume.total`` with the outcome, and
        # the sum across the outcomes is how many arrived. One counter that
        # leads the others by however many messages are in flight is a counter
        # that makes an operator wonder which one is lying.
        #
        # The attempt is a distribution rather than a counter, and it is recorded
        # here on arrival rather than at the settlement: it is a fact about the
        # delivery that is true before the handler runs, and recording it after
        # would drop every message this consumer is still working on when the
        # process stops. A rising distribution is a dependency in trouble, and it
        # says so before anything else does — the dead letters only move once the
        # attempts run out.
        self._observer.observe(
            METRIC_CONSUME_ATTEMPTS, float(envelope.attempt), self._labels
        )
        self._in_flight += 1
        self._observer.gauge(METRIC_CONSUME_IN_FLIGHT, self._in_flight, self._labels)
        try:
            await self._handle_one(delivery, envelope)
        finally:
            self._in_flight -= 1
            self._observer.gauge(METRIC_CONSUME_IN_FLIGHT, self._in_flight, self._labels)

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
            #
            # Counted as a settled delivery here rather than in ``_carry_out``,
            # which this path never reaches: no handler ran, so there is no
            # duration to record, but the message did arrive and was dealt with
            # and a total that missed it would not add up.
            self._observer.count(
                METRIC_CONSUME_TOTAL, 1, {**self._labels, TAG_OUTCOME: OUTCOME_PARKED}
            )
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
            # This delivery is being driven by a consumer, so how it ends will
            # be reported. An interceptor that wants to know can wait for it.
            reports_settlement=True,
            reply_to=delivery.reply_to,
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

        # Timed around the interceptors as well as the handler, because what an
        # operator wants to know is how long a message takes to deal with, and
        # an interceptor that opens a transaction is part of dealing with it.
        # Stopped here and recorded a few lines below rather than in a
        # ``finally``, because the duration is tagged with the outcome and the
        # outcome is not known until ``_decide`` has run: how long a message
        # took and what happened to it are one question, and a p99 that mixes
        # the messages that worked with the ones that timed out answers neither
        # half of it.
        elapsed = time.monotonic() - started

        # Whatever the interceptors left, rather than what arrived: one that
        # rewrote the envelope on the way in meant that rewrite for the retry
        # and the dead letter as much as for the handler.
        envelope = context.envelope

        if isinstance(decision, Ack):
            settlement, wait = self._decide(envelope, decision)
        else:
            settlement, wait = (
                Settlement(
                    OUTCOME_DEAD_LETTERED,
                    reason=(
                        f"the handler returned {type(decision).__name__} instead of an Ack"
                    ),
                ),
                None,
            )

        self._observer.observe(
            METRIC_CONSUME_DURATION,
            elapsed,
            {**self._labels, TAG_OUTCOME: settlement.outcome},
        )

        # Announced before it is carried out, and so before a consumer-side
        # retry sleeps out its backoff. A listener told afterwards would be
        # describing the delivery minutes after the decision, and the span it is
        # writing on would have stayed open for a wait that is not work.
        context.settled(settlement)
        await self._carry_out(delivery, envelope, settlement, wait)

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
            reply_to=context.reply_to,
        )
        returned = self._handler(message)
        # Not an Ack is a mistake worth reporting, but it is not this method's
        # to report: it is returned as it stands and _handle dead-letters it
        # with a reason, which is where every other unhandleable delivery goes.
        # An interceptor between here and there sees the same thing the handler
        # returned, which is the honest thing to show it.
        decision: Ack = await returned if inspect.isawaitable(returned) else returned
        return decision

    def _decide(self, envelope: Envelope, decision: Ack) -> tuple[Settlement, Wait | None]:
        """What is going to happen to this delivery, before anything happens to
        it.

        Worked out on its own, and without touching the broker, so that it can
        be announced first. The gap this closes is a small one to describe and a
        wide one to fall into: a handler asking for a retry it has no attempts
        left for is dead-lettered, and anything reading the handler's answer
        instead of this one records a retry that never happened.

        :param envelope: the message's metadata, for the attempt and the age
        :param decision: what the handler answered
        :returns: the settlement, and the wait when there is another attempt —
            which the settlement does not carry, because whether a wait is spent
            here or on a rung queue is this consumer's business and not a
            listener's
        """
        if decision.action is Action.ACCEPT:
            return Settlement(OUTCOME_ACKED), None

        if decision.action is Action.REJECT:
            return (
                Settlement(
                    OUTCOME_REJECTED,
                    reason=f"the handler rejected it: {_describe(decision.error)}",
                ),
                None,
            )

        if decision.action is Action.PARK:
            # The handler read the message and found nothing it could read. The
            # engine reaches the same conclusion for a body its codec refuses,
            # and both end up in the same queue for the same reason: a message
            # nobody can read is a producer problem, and it is only findable if
            # it is not filed with the messages that merely ran out of luck.
            return (
                Settlement(
                    OUTCOME_PARKED,
                    reason=f"the handler parked it: {_describe(decision.error)}",
                ),
                None,
            )

        if isinstance(decision.error, FatalError):
            # The handler asked for a retry but marked the reason as one that
            # will not change. Honouring the mark rather than the request is the
            # point of having it.
            return (
                Settlement(
                    OUTCOME_DEAD_LETTERED,
                    reason=(
                        "the handler reported an unprocessable message: "
                        f"{_describe(decision.error)}"
                    ),
                ),
                None,
            )

        wait = self._retry.next_wait(envelope.attempt, envelope.age)
        if wait is None:
            return (
                Settlement(
                    OUTCOME_DEAD_LETTERED,
                    reason=f"{self._exhausted(envelope)}: {_describe(decision.error)}",
                ),
                None,
            )

        return Settlement(OUTCOME_RETRIED, delay=wait.delay), wait

    async def _carry_out(
        self,
        delivery: Delivery,
        envelope: Envelope,
        settlement: Settlement,
        wait: Wait | None,
    ) -> None:
        """Does what :meth:`_decide` decided, and counts it as decided.

        Every counter here is chosen from the :class:`~acemq_amqp.ack.Settlement`
        and never from the handler's :class:`~acemq_amqp.ack.Ack`, which is the
        same rule the delivery's span follows. A handler asking for a retry it
        has no attempts left for is dead-lettered, and a counter that read the
        request rather than the answer would report a retry for a message
        nothing will ever try again — so the dead letters would be undercounted
        by exactly the messages an operator most wants to find.

        One ``acemq.consume.total`` per delivery, carrying the outcome, and then
        whichever of the standalone counters applies. ``retried`` and
        ``dead_lettered`` are both an outcome here and a counter of their own,
        which is what Java does and is not a redundancy: the outcome says what
        was decided about a delivery, and the counter says how many messages are
        going round again or have been set aside, which is the number an alert
        is written against.
        """
        self._observer.count(
            METRIC_CONSUME_TOTAL, 1, {**self._labels, TAG_OUTCOME: settlement.outcome}
        )

        if settlement.outcome == OUTCOME_ACKED:
            await delivery.ack()
            return

        if settlement.outcome == OUTCOME_PARKED:
            # ``_park`` counts it, exactly as it does for a body that would not
            # decode. There is one parked counter and one parked queue whether
            # the codec or the handler was the one that could not read it.
            await self._park(delivery, envelope, settlement.reason or "")
            return

        if wait is None:
            await self._dead_letter(delivery, envelope, settlement.reason or "")
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
            # Kept even though a consumer declares its own rungs when it starts,
            # because the rung can still be missing: a consumer started with
            # ``declare=False`` declares nothing, and a rung deleted under a
            # running consumer is gone whoever declared it. What has changed is
            # what it means — it used to be an ordinary topology mistake and is
            # now either a deliberate opt-out or somebody removing queues.
            #
            # Degraded rather than fatal: the message is still deliverable, and
            # waiting for it here is what this library did before there were
            # rungs. Counted as well as logged, because this is the one failure
            # here that nothing else shows: the message is still retried and the
            # wait still happens, so a dashboard reads as normal while the reason
            # the rung exists is quietly gone.
            self._observer.count(METRIC_RUNG_MISSING, 1, {**self._labels, "rung": rung})
            log.error(
                "acemq: %s is not on the broker, so %s will wait %s in this consumer "
                "instead; declare it with Topology().queue(%r, retry=policy), or let "
                "the consumer declare it by leaving declare=True",
                rung,
                envelope.id,
                delay,
                self._queue,
            )
            return False

        self._observer.count(METRIC_RETRIED_TOTAL, 1, {**self._labels, "where": "broker"})
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
            #
            # Counted as a retry all the same, and it is one: the settlement
            # said ``retried``, the span says ``retried``, and the broker is
            # about to hand the message back. Leaving it uncounted was the one
            # place where the counters and the span disagreed about the same
            # delivery. The label says which path it took, because a requeue
            # keeps the attempt header it arrived with and the other two do not.
            self._observer.count(METRIC_RETRIED_TOTAL, 1, {**self._labels, "where": "requeued"})
            log.error(
                "acemq: cannot republish %s onto %s for attempt %d; returning it to the broker",
                envelope.id,
                self._queue,
                next_attempt.attempt,
            )
            await delivery.nack(True)
            return

        self._observer.count(METRIC_RETRIED_TOTAL, 1, {**self._labels, "where": "consumer"})
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
        # The same counter a dead-lettering moves, separated by the outcome
        # rather than by a metric of its own — which is what Java settled on, and
        # what makes "how much is this queue giving up on" one number that can
        # then be split into the two problems it is made of.
        self._observer.count(
            METRIC_DEAD_LETTERED_TOTAL, 1, {**self._labels, TAG_OUTCOME: OUTCOME_PARKED}
        )
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
        self._observer.count(
            METRIC_DEAD_LETTERED_TOTAL,
            1,
            {**self._labels, TAG_OUTCOME: OUTCOME_DEAD_LETTERED},
        )
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
                METRIC_SET_ASIDE_FAILED, 1, {**self._labels, TAG_TARGET: target}
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
        for worker in self._workers:
            try:
                await worker
            except asyncio.CancelledError:
                # A worker that was already cancelled — a task group unwinding,
                # a shutdown that cancelled the loop's tasks before closing the
                # connection — is not news to report to a caller who is merely
                # closing. Re-raising it here makes closing look like the
                # *caller* was cancelled, which is what anything inspecting
                # ``task.exception()`` afterwards would be told.
                if not worker.cancelled():
                    raise

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
    :param max_outstanding_publishes: how many publishes may be waiting for the
        broker at once. The ceiling that turns a memory leak into backpressure:
        without one, a caller publishing faster than the broker answers
        accumulates unconfirmed messages until the process dies, which looks
        like throughput right up to the moment it does not. A thousand by
        default, which is what Java's ``maxOutstandingPublishes`` and .NET's
        ``MaxOutstandingPublishes`` default to
    :param confirm_timeout: how long a publish waits for one of those permits
        before giving up. Ten seconds by default, the same as Java's
        ``confirmTimeout``. Reaching it raises rather than waiting on, because a
        publisher stalled for ever behind a broker that has stopped answering is
        the failure this bound exists to make visible
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
        max_outstanding_publishes: int = DEFAULT_MAX_OUTSTANDING_PUBLISHES,
        confirm_timeout: timedelta = DEFAULT_CONFIRM_TIMEOUT,
        on_publish: Sequence[PublishInterceptor] = (),
        on_consume: Sequence[ConsumeInterceptor] = (),
        observer: Observer | None = None,
    ) -> None:
        if prefetch < 0:
            raise ValueError(f"acemq: prefetch must not be negative, got {prefetch}")
        if max_outstanding_publishes < 1:
            raise ValueError(
                "acemq: max_outstanding_publishes must be at least 1, got "
                f"{max_outstanding_publishes}"
            )
        if confirm_timeout <= timedelta(0):
            raise ValueError(
                f"acemq: confirm_timeout must be positive, got {confirm_timeout}"
            )
        self._transport = transport
        self._observer: Observer = observer or NullObserver()
        self._codec = codec or JsonCodec()
        self._origin = origin or default_origin()
        self._retry = retry or no_retry()
        self._prefetch = prefetch
        self._max_outstanding = max_outstanding_publishes
        self._confirm_timeout = confirm_timeout
        # Built here rather than on first use. An asyncio.Semaphore has bound no
        # loop at construction since 3.10, so it is safe to make one outside a
        # running loop, and making it here means every publish on this connection
        # shares the one counter however many tasks are publishing.
        self._outstanding = asyncio.Semaphore(max_outstanding_publishes)
        self._in_flight_publishes = 0
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
    def max_outstanding_publishes(self) -> int:
        """How many publishes may be waiting for the broker at once."""
        return self._max_outstanding

    @property
    def confirm_timeout(self) -> timedelta:
        """How long a publish waits for room before giving up."""
        return self._confirm_timeout

    @property
    def outstanding_publishes(self) -> int:
        """How many publishes are waiting for the broker right now.

        What a stalled publisher looks like from outside: a number pinned at
        :attr:`max_outstanding_publishes` is a broker that has stopped answering,
        and it says so before the first :class:`~acemq_amqp.PublishError` does.
        """
        # Counted here rather than read off the semaphore, which keeps its count
        # private. Raised after the permit is taken and lowered before it is
        # given back, both inside publish_raw, so it never reads high.
        return self._in_flight_publishes

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

    @property
    def blocked(self) -> bool | None:
        """Whether the broker has blocked this connection, or ``None`` when the
        transport cannot be asked.

        RabbitMQ blocks a connection when it is low on memory or disk, and while
        it lasts it stops reading that connection's socket: everything sent —
        a publish, a declaration, :meth:`health`'s own probe — waits rather than
        failing. It is the broker protecting itself and it is not an error, so
        it is worth telling apart from the broker having gone away, which looks
        identical from outside and wants the opposite response.

        ``None`` means the question could not be asked at all, which is a
        different fact from ``False`` and is reported as one: a transport that
        does not implement
        :class:`~acemq_amqp.transport.BlockedState`, or one whose client has
        moved the state out of reach. See :attr:`blocked_reason` for why there
        may be a boolean and no reason to go with it.
        """
        transport = self._transport
        return transport.blocked if isinstance(transport, BlockedState) else None

    @property
    def blocked_reason(self) -> str | None:
        """What the broker said when it blocked this connection, if anything.

        ``None`` when the connection is not blocked, and also ``None`` when the
        transport is blocked but cannot say why — which is what the RabbitMQ
        transport answers, because aio-pika reads RabbitMQ's reason and does not
        keep it. Reporting the block without the reason is the honest half;
        inventing a reason would read exactly like one the broker sent.
        """
        transport = self._transport
        return transport.blocked_reason if isinstance(transport, BlockedState) else None

    async def health(self, timeout: float = DEFAULT_HEALTH_TIMEOUT) -> HealthReport:
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

        **A blocked connection is up, with the reason.** The broker blocks a
        connection when it is low on disk or memory, and an application that
        fails its own readiness check for it is one an orchestrator restarts
        into the same blocked broker, having thrown away whatever it was
        holding. ``parts["blocked"]`` carries the state either way, and is
        ``None`` when the transport could not be asked — see :attr:`blocked`.

        **The probe is bounded.** A blocked broker stops reading the socket, so
        the round trip this makes is precisely the thing that does not come back
        when something is wrong; without a deadline of its own a health check
        hangs, and a readiness endpoint that hangs takes the instance out of
        rotation with no report at all. A probe that runs out of time is
        abandoned rather than waited for, because a request that cannot be
        answered cannot reliably be cancelled either.

        It costs a round trip, so it is not something to call per request. Wire
        it to a readiness probe and let the probe's interval decide how often.

        :param timeout: how long to give the broker's answer, three seconds by
            default. Shorter than the deadline
            :func:`~acemq_amqp.telemetry.aggregate_health` puts around a whole
            set of checks, so that this report rather than that one is what an
            operator reads when a broker stops answering
        :returns: the report. Degraded means working but worth an alert
        """
        checked = datetime.now(timezone.utc)
        consumers = self.consumers
        stalled = [consumer.queue for consumer in consumers if not consumer.running]
        blocked = self.blocked
        parts: dict[str, Any] = {
            "consumers": len(consumers),
            "in-flight": sum(consumer.in_flight for consumer in consumers),
            "blocked": blocked,
        }
        reason = self.blocked_reason
        if reason:
            parts["blocked-reason"] = reason

        if self._closed:
            return HealthReport(
                HealthStatus.DOWN, "the connection has been closed", checked, parts
            )

        if blocked:
            # Asked before the probe rather than after it, because the probe is
            # what a blocked broker will not answer: a round trip started here
            # would spend the whole deadline arriving at the answer this already
            # has.
            return self._blocked_report(checked, parts, stalled, reason)

        # Asking whether a queue nothing has ever declared exists, rather than
        # declaring a temporary one. It is the same round trip and it proves the
        # same thing, and it creates nothing at all: an exclusive queue is only
        # released when the channel that declared it closes, so a probe that
        # declared one would leave a queue on the broker for the life of the
        # connection and a new one behind every restart.
        probe = f"acemq-health-{uuid.uuid4().hex}"
        started = time.monotonic()
        try:
            await self._probe_within(probe, timeout)
        except asyncio.TimeoutError:
            # A round trip that does not come back is the ordinary way a block
            # shows itself, and the notification may have arrived while this was
            # waiting. Asked again rather than assumed, because "the broker is
            # protecting itself" and "the broker has gone" are the same silence.
            blocked = self.blocked
            parts["blocked"] = blocked
            reason = self.blocked_reason
            if reason:
                parts["blocked-reason"] = reason
            if blocked:
                return self._blocked_report(checked, parts, stalled, reason)
            return HealthReport(
                HealthStatus.DOWN,
                f"the broker did not answer within {timeout:g}s",
                checked,
                parts,
            )
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
                _stalled_detail(stalled),
                checked,
                {**parts, "stalled": sorted(stalled)},
            )
        return HealthReport(HealthStatus.UP, "", checked, parts)

    def _blocked_report(
        self,
        checked: datetime,
        parts: dict[str, Any],
        stalled: list[str],
        reason: str | None,
    ) -> HealthReport:
        """What a blocked connection reports: up, with the reason.

        Up for the block itself, and still degraded when a consumer of this
        process has stopped reading — that is this instance's own failure rather
        than the broker's, it outlives the alarm, and a block is not a reason to
        stop saying so.
        """
        detail = self._blocked_detail(reason)
        if stalled:
            return HealthReport(
                HealthStatus.DEGRADED,
                f"{_stalled_detail(stalled)}; and {detail}",
                checked,
                {**parts, "stalled": sorted(stalled)},
            )
        return HealthReport(HealthStatus.UP, detail, checked, parts)

    @staticmethod
    def _blocked_detail(reason: str | None) -> str:
        """The blocked sentence, with the broker's reason when there is one."""
        return BLOCKED_DETAIL if not reason else f"{BLOCKED_DETAIL} ({reason})"

    async def _probe_within(self, name: str, timeout: float) -> None:
        """One round trip to the broker, given ``timeout`` to come back.

        The probe runs as a task that is abandoned rather than awaited when the
        deadline passes, which is the difference between a bounded check and one
        that merely looks bounded: cancelling a request to a broker that has
        stopped reading its socket means waiting for a cancellation that travels
        the same way the request did.

        :raises asyncio.TimeoutError: when the broker did not answer in time
        """
        probe = asyncio.ensure_future(self._probe(name))
        try:
            done, _ = await asyncio.wait({probe}, timeout=timeout)
        except asyncio.CancelledError:
            # The caller gave up on the whole check. Waiting on a task is not
            # cancelling it, so it is cancelled here rather than left running
            # against a broker nobody is listening to any more.
            probe.cancel()
            probe.add_done_callback(_retrieved)
            raise
        if not done:
            probe.cancel()
            # Whatever it ends as is nobody's news now, and an abandoned task
            # whose exception is never retrieved is a warning at collection time
            # about a failure that has already been reported as a timeout.
            probe.add_done_callback(_retrieved)
            raise asyncio.TimeoutError
        # Raises what the probe raised, which is what the caller reports.
        probe.result()

    async def _probe(self, name: str) -> None:
        """One round trip to the broker that leaves nothing behind.

        A transport that cannot be asked whether a queue exists gets the next
        cheapest thing: a temporary queue, removed again straight away. Both
        prove the same thing, which is that the connection answers rather than
        merely being open.

        The temporary queue is classic, and has to be. It is exclusive and
        auto-deleting so that it goes when the channel does, and RabbitMQ
        refuses a quorum queue that is either — a health check that could not be
        declared would report a healthy broker as down.
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
        declare: bool = True,
    ) -> Consumer:
        """Reads messages from a queue until the returned consumer is closed.

        Before it subscribes it declares the queues it will need when a message
        fails: ``{queue}.dlq``, ``{queue}.parked``, ``acemq.dlx`` and, when the
        retry policy has waits the broker holds, ``acemq.retry`` and the rungs.
        See :func:`acemq_amqp.topology.declare_where_failures_go` for exactly
        what and why. The source queue is not among them, and neither is
        anything a producer needs, so this is not a replacement for declaring a
        topology — it is the floor underneath a service that was deployed
        without one.

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
        :param declare: declare the queues above before subscribing. On by
            default, because the alternative default loses messages silently.
            Turn it off for a login with no ``configure`` permission on the
            vhost, which is refused by the broker rather than ignored, and for a
            tool draining a queue it does not own — a one-off reader of
            ``orders.dlq`` has no business creating ``orders.dlq.dlq``. A
            consumer started this way still publishes to those queues, so
            somebody else has to have declared them
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
            declare=declare,
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

        **This is the one place the outstanding-publish bound is taken.** Every
        publish in the library comes through here — a :class:`Publisher`, a
        retry rung, a dead letter, a replay — so one permit counts them all, and
        :meth:`Publisher.send_all` cannot put more messages on the wire at once
        than :attr:`max_outstanding_publishes` allows however long the list it
        was given is.

        :param exchange: where to publish, empty for the default exchange
        :param routing_key: what to publish under
        :param message: the message, encoded
        :returns: what the broker said
        :raises PublishError: when every permit is taken and none came free
            within :attr:`confirm_timeout`
        """
        # Taken before the message is written and not after, which is the whole
        # point: a bound applied afterwards has already let the message into
        # memory. The permit is given back in the ``finally`` below on every
        # path there is — the publish returning, the publish raising, and this
        # task being cancelled while the broker is still thinking — because a
        # permit lost on any one of those is a connection that publishes a
        # thousand more messages and then stops for ever.
        try:
            await asyncio.wait_for(
                self._outstanding.acquire(), self._confirm_timeout.total_seconds()
            )
        except asyncio.TimeoutError as stalled:
            # Said rather than waited out. A publisher parked behind a broker
            # that has stopped answering is indistinguishable from a quiet
            # service, and this sentence is the one Java raises word for word so
            # that an operator reading a log has one thing to recognise.
            raise PublishError(
                message.message_id,
                exchange,
                routing_key,
                (
                    f"{self._max_outstanding} publishes are already waiting for a "
                    f"confirm and none completed within {self._confirm_timeout}. "
                    "The broker is not keeping up; publish more slowly rather than "
                    "buffering more."
                ),
            ) from stalled

        self._in_flight_publishes += 1
        try:
            return await self._transport.publish(exchange, routing_key, message)
        finally:
            self._in_flight_publishes -= 1
            self._outstanding.release()

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

    A blocked broker comes back through here as **up, with the reason**, and
    nothing in between folds it into anything else: the aggregate takes the
    worst status it is given, so a check that reported back pressure as degraded
    would quietly degrade a report that had decided otherwise. The deadline is
    the same argument — :attr:`timeout` is shorter than the aggregate's, so that
    a broker which has stopped answering is described by this check rather than
    by the aggregate giving up on it.

    :param connection: what to ask
    :param label: what to call it in a combined report
    :param timeout: how long to give the broker's answer. See
        :meth:`Connection.health`
    """

    connection: Connection
    label: str = "broker"
    timeout: float = DEFAULT_HEALTH_TIMEOUT

    @property
    def name(self) -> str:
        return self.label

    async def check(self) -> HealthReport:
        return await self.connection.health(self.timeout)


async def connect(
    url: str,
    *,
    codec: Codec | None = None,
    origin: str | None = None,
    retry: RetryPolicy | None = None,
    prefetch: int = DEFAULT_PREFETCH,
    max_outstanding_publishes: int = DEFAULT_MAX_OUTSTANDING_PUBLISHES,
    confirm_timeout: timedelta = DEFAULT_CONFIRM_TIMEOUT,
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
    :param max_outstanding_publishes: how many publishes may be waiting for the
        broker at once, a thousand by default. See :class:`Connection`
    :param confirm_timeout: how long a publish waits for room before raising
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
        max_outstanding_publishes=max_outstanding_publishes,
        confirm_timeout=confirm_timeout,
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


def _stalled_detail(stalled: list[str]) -> str:
    """The sentence naming the consumers that have stopped reading."""
    return "these consumers have stopped reading: " + ", ".join(sorted(stalled))


def _retrieved(task: asyncio.Task[None]) -> None:
    """Reads an abandoned task's outcome so asyncio does not complain about it."""
    if not task.cancelled():
        task.exception()


def _describe(failure: BaseException | None) -> str:
    """A failure as the sentence that goes in ``x-acemq-error``."""
    if failure is None:
        return "no reason given"
    text = str(failure)
    return type(failure).__name__ + (f": {text}" if text else "")
