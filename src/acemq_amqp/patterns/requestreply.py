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

"""Asking a question over a queue, and waiting for the answer.

Messaging is asynchronous and request-reply is a synchronous shape drawn on top
of it, which is a real cost rather than a free convenience: a caller waiting on
a reply is holding a task, a connection and a deadline, and a responder that
slows down turns into a caller that stops responding. Reach for it where the
caller genuinely cannot continue without the answer, and publish an event
otherwise.

The pairing is the envelope's correlation identifier, which does not use AMQP's
own ``correlation-id`` property: that is lost the moment a message passes
through a service that rebuilds it, and the envelope is the thing this library
promises to carry end to end.

The return address is written twice, and read either way round. A request
carries the ``acemq-reply-to`` header *and* AMQP's own ``reply-to`` property,
set to the same queue; a responder reads the header first and falls back to the
property. All five libraries do exactly this, in that order, and the reason is
that they did not use to: Python, Go and Ruby wrote only the header while Java
and .NET read only the property, so a Java caller and a Python responder could
not talk at all. Writing both and reading either makes every one of the
twenty-five caller/responder pairs work, and keeps what the header was for — it
is the one of the two that survives a service rebuilding the message.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
import uuid
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from ..ack import Ack, FatalError, accept, reject, retry
from ..codec import Codec
from ..connection import Connection, Consumer, Message
from ..envelope import Envelope
from ..errors import AceMQError
from ..telemetry import (
    METRIC_REQUEST_DURATION,
    METRIC_REQUEST_TOTAL,
    OUTCOME_ANSWERED,
    OUTCOME_FAILED,
    OUTCOME_TIMED_OUT,
    TAG_OUTCOME,
    TAG_ROUTING_KEY,
    Observer,
)
from ..topology import Topology

log = logging.getLogger("acemq")

#: Where a responder should send its answer.
#:
#: An application header *as well as* AMQP's ``reply-to`` property, and the one
#: a responder reads first. It travels through the same envelope machinery as
#: everything else and survives a hop through a service that rebuilds the
#: message, which the native property does not. It deliberately carries no
#: ``x-acemq-`` prefix: that namespace belongs to the engine and is stripped
#: before a handler sees it, so a responder could never read this one.
HEADER_REPLY_TO = "acemq-reply-to"

#: A responder's failure, carried back to the caller.
HEADER_ERROR = "acemq-error"

#: How long :meth:`Requester.ask` waits unless it is told otherwise.
DEFAULT_TIMEOUT = timedelta(seconds=30)

#: What a responder is: a message in, an answer out, an exception for a failure.
Responder = Callable[[Message], Any]


class RequestTimeoutError(AceMQError, TimeoutError):
    """No reply arrived before the deadline.

    It says nothing about whether the request was handled. A timeout is the
    absence of an answer, not evidence that nothing happened, which is why a
    request that changes anything should be idempotent at the other end.

    Also a :class:`TimeoutError`, so ``except TimeoutError`` catches it — which
    is what a Python caller writes, and how
    :meth:`~acemq_amqp.tracing.OpenTelemetryTracing.request_span` tells a
    deadline apart from a responder that failed without having to import this
    module. ``except AceMQError`` still catches it too; the extra base only adds
    a way to be caught.
    """


class ResponderError(AceMQError):
    """The responder answered, and the answer was that it could not do it.

    Better than a timeout in every way: the caller learns in milliseconds rather
    than at the deadline, and learns why.
    """


class Requester:
    """Sends requests and waits for their replies.

    Built by :meth:`open`, because it has a queue and a consumer of its own and
    both have to exist before it is any use::

        async with await Requester.open(mq, "pricing", "price.request") as ask:
            price = await ask.ask({"sku": "A-1"})

    Close it when done: it holds a queue, a consumer and whatever is still
    waiting.
    """

    def __init__(
        self,
        connection: Connection,
        exchange: str,
        routing_key: str,
        reply_queue: str,
        timeout: timedelta,
        codec: Codec | None,
    ) -> None:
        self._connection = connection
        self._exchange = exchange
        self._routing_key = routing_key
        self._reply_queue = reply_queue
        self._timeout = timeout
        self._codec = codec
        self._publisher = connection.publisher(exchange, routing_key, codec=codec)
        self._waiting: dict[str, asyncio.Future[Message]] = {}
        self._consumer: Consumer | None = None
        self._observer: Observer = connection.observer
        # The destination as one word, which is what Java tags the round trip
        # with: the routing key, or the exchange when publishing without one.
        self._destination = routing_key or exchange
        self._timed_out = 0
        self._unmatched = 0

    @classmethod
    async def open(
        cls,
        connection: Connection,
        exchange: str = "",
        routing_key: str = "",
        *,
        reply_queue: str | None = None,
        timeout: timedelta = DEFAULT_TIMEOUT,
        codec: Codec | None = None,
    ) -> Requester:
        """Declares the reply queue and starts consuming it.

        :param connection: where to send and where to listen
        :param exchange: where requests go, empty for the default exchange
        :param routing_key: what they are published under, or a queue name
        :param reply_queue: where replies come back. One is generated when this
            is not given: transient, exclusive, auto-deleting and classic,
            belonging to this process. Name one only when replies must survive a
            restart — a named reply queue that outlives its requester collects
            answers nobody is waiting for
        :param timeout: how long :meth:`ask` waits
        :param codec: a codec other than the connection's, for both directions
        :returns: the requester, already consuming
        """
        if timeout <= timedelta(0):
            raise ValueError(f"acemq: a request timeout must be positive, got {timeout}")

        generated = not reply_queue
        queue = reply_queue or f"acemq-reply-{uuid.uuid4().hex}"
        # A generated reply queue is classic, and has to be: it is exclusive and
        # auto-deleting so that it goes when this process does, and RabbitMQ
        # refuses a quorum queue that is either. Said here rather than left to
        # the default, because it is the one queue in the library where turning
        # it quorum would stop request/reply working at all rather than merely
        # declare something different. A named one is an ordinary durable queue
        # a responder may also declare, so it is quorum like any other.
        await connection.declare(
            Topology().queue(
                queue,
                durable=not generated,
                auto_delete=generated,
                exclusive=generated,
                quorum=not generated,
            )
        )

        requester = cls(connection, exchange, routing_key, queue, timeout, codec)
        # The one consumer in this library that declares nothing. A reply queue
        # is a mailbox for one process, and a generated one is a different name
        # every restart, so the dead-letter queues a consumer usually declares
        # would be two durable queues per process that nothing ever reads and
        # nothing ever deletes. There is nothing for them to catch either:
        # :meth:`_receive` accepts every reply, including one nobody is waiting
        # for, so no message on this queue is ever dead-lettered or parked.
        requester._consumer = await connection.consume(
            queue, requester._receive, codec=codec, declare=False
        )
        return requester

    @property
    def reply_queue(self) -> str:
        """The queue replies arrive on."""
        return self._reply_queue

    @property
    def timed_out(self) -> int:
        """How many calls reached their deadline with no reply.

        Java's ``requester.timedOut()`` and .NET's ``TimedOut``, and the same
        number: a caller that gave up, counted whether or not the responder
        eventually answered.
        """
        return self._timed_out

    @property
    def unmatched(self) -> int:
        """How many replies arrived with nobody waiting for them.

        Java's ``requester.unmatched()``. Almost always the timeout being too
        short rather than anything broken: this rising alongside
        :attr:`timed_out` is the signature of a responder slower than its
        callers expect.
        """
        return self._unmatched

    async def ask(
        self,
        request: Any,
        *,
        envelope: Envelope | None = None,
        timeout: timedelta | None = None,
    ) -> Any:
        """Sends a request and waits for its reply.

        :param request: what to send
        :param envelope: metadata to send instead of a fresh one. Its
            correlation identifier is replaced: that is what pairs a reply with
            its request, so it belongs to this call rather than to the caller
        :param timeout: how long to wait, overriding this requester's
        :returns: the reply's payload
        :raises RequestTimeoutError: when nothing came back in time
        :raises ResponderError: when the responder said it could not do it
        """
        deadline = timeout or self._timeout
        correlation = str(uuid.uuid4())
        outgoing = envelope or Envelope(origin=self._connection.origin)
        outgoing = outgoing.with_(
            correlation_id=correlation,
            headers={**outgoing.headers, HEADER_REPLY_TO: self._reply_queue},
        )

        waiter: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        # Registered before the request goes out, because a fast responder can
        # reply before send() has returned.
        self._waiting[correlation] = waiter
        # Started before the publish, because the round trip is what the *caller*
        # experienced and the caller's wait begins here. A timer started after
        # the send would leave the publish out of the one number whose whole job
        # is to include it.
        started = time.monotonic()
        outcome = OUTCOME_FAILED
        try:
            # Both addresses, the same value. The header is what a Python, Go or
            # Ruby responder reads first; the native property is what a Java or
            # .NET one reads, and without it those two would answer nowhere.
            await self._publisher.send(
                request, envelope=outgoing, reply_to=self._reply_queue
            )
            answer = await asyncio.wait_for(waiter, deadline.total_seconds())
        except RequestTimeoutError:
            # Already one of ours, from :meth:`close` failing what was still
            # waiting. Caught first because this class is a ``TimeoutError``
            # too, and the clause below would otherwise re-describe "the
            # requester was closed" as "nothing came back in time" — the same
            # exception type carrying the wrong reason.
            self._timed_out += 1
            outcome = OUTCOME_TIMED_OUT
            raise
        except asyncio.TimeoutError as expired:
            self._timed_out += 1
            outcome = OUTCOME_TIMED_OUT
            raise RequestTimeoutError(
                f"acemq: no reply to {correlation} arrived within {deadline}"
            ) from expired
        else:
            outcome = OUTCOME_ANSWERED
        finally:
            self._waiting.pop(correlation, None)
            # Recorded on every path out, including the ones that raise. A
            # duration that only covers the calls that worked is a duration that
            # hides exactly the calls somebody is looking for, and a timed-out
            # request is the slowest data point there is.
            self._record(time.monotonic() - started, outcome)

        failure = answer.envelope.headers.get(HEADER_ERROR)
        if failure:
            raise ResponderError(
                f"acemq: the responder could not answer {correlation}: {failure}"
            )
        return answer.payload

    def _record(self, elapsed: float, outcome: str) -> None:
        """One round trip, on the two metrics Java writes for the same thing.

        ``answered`` counts a reply that came back, whatever it said: a
        responder that answered "I could not do it" answered, and the failure is
        in the reply rather than in the round trip. ``failed`` is for the publish
        itself not getting out, which is the one case where there was never a
        question to be slow about.
        """
        labels = {TAG_ROUTING_KEY: self._destination, TAG_OUTCOME: outcome}
        self._observer.observe(METRIC_REQUEST_DURATION, elapsed, labels)
        self._observer.count(METRIC_REQUEST_TOTAL, 1, labels)

    async def _receive(self, reply: Message) -> Ack:
        """Hands a reply to whoever is waiting for it.

        A reply nobody is waiting for is accepted and dropped, which is what a
        reply to a request that already timed out is. It is not an error and it
        is not worth dead-lettering: the caller has gone. It is counted, though,
        because :attr:`unmatched` rising alongside :attr:`timed_out` is the one
        signature that separates "the responder is broken" from "the timeout is
        too short", and from outside they look the same.
        """
        waiter = self._waiting.pop(reply.envelope.correlation_id, None)
        if waiter is None or waiter.done():
            self._unmatched += 1
        else:
            waiter.set_result(reply)
        return accept()

    async def close(self) -> None:
        """Stops consuming replies and fails anything still waiting.

        Failed rather than left, because a caller awaiting a reply on a
        requester somebody else closed would otherwise wait out its whole
        timeout for an answer that can no longer arrive.
        """
        if self._consumer is not None:
            await self._consumer.close()
            self._consumer = None
        for correlation, waiter in list(self._waiting.items()):
            if not waiter.done():
                waiter.set_exception(
                    RequestTimeoutError(
                        f"acemq: the requester was closed while {correlation} was outstanding"
                    )
                )
        self._waiting.clear()

    async def __aenter__(self) -> Requester:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class ResponderHandle:
    """A running responder, and the two numbers it keeps.

    Built by :func:`serve`. It is a :class:`~acemq_amqp.Consumer` in every way
    that matters — closed the same way, usable as an ``async with``, reporting
    the same ``queue``, ``running`` and ``in_flight`` — with
    :attr:`answered` and :attr:`unanswerable` added, which is what Java's
    ``Responder`` and .NET's ``Responder`` report and what nothing here reported
    before.

    The counters are built before the subscription is, and that ordering is the
    guarantee rather than an implementation detail: a broker may hand the first
    request over from inside the subscribe — which is what a queue with a backlog
    looks like from in here — and the handler reads these on that very delivery.
    Java had to say the same thing about field initialisation and .NET had to
    lift its counters out of the responder to get it. Neither number needs a wait
    before it can be trusted, and code that sleeps before reading one is working
    around a defect that is not here.
    """

    def __init__(self, queue: str) -> None:
        self._queue = queue
        self._answered = 0
        self._unanswerable = 0
        self._consumer: Consumer | None = None

    @property
    def answered(self) -> int:
        """How many requests were answered, counted before each reply left.

        A caller holding a reply can rely on this having counted it: the
        increment happens before the publish, so there is no interleaving in
        which the answer is visible and the number is not. The other order looks
        more natural and is wrong — it leaves a window where the reply is in the
        caller's hands and the responder still says nothing has been answered,
        which is a dashboard reporting an idle service that is demonstrably
        working.

        **A publish that fails hands the increment back**, so this counts replies
        that were sent rather than replies that were attempted.
        """
        return self._answered

    @property
    def unanswerable(self) -> int:
        """How many requests arrived naming nowhere to reply.

        Anything above zero means a caller is publishing where it means to
        request. Counted before the delivery is settled.
        """
        return self._unanswerable

    @property
    def consumer(self) -> Consumer:
        """The consumer underneath, for anything this class does not forward."""
        if self._consumer is None:  # pragma: no cover - serve always sets it
            raise AceMQError("acemq: this responder has not been started")
        return self._consumer

    @property
    def queue(self) -> str:
        """The queue requests arrive on."""
        return self._queue

    @property
    def running(self) -> bool:
        """Whether this responder is still serving."""
        return self._consumer is not None and self._consumer.running

    @property
    def closed(self) -> bool:
        """Whether this responder has been stopped."""
        return self._consumer is None or self._consumer.closed

    @property
    def in_flight(self) -> int:
        """How many requests are being answered right now."""
        return 0 if self._consumer is None else self._consumer.in_flight

    async def close(self) -> None:
        """Stops serving, letting the request being answered finish.

        A request being answered right now has a caller blocked on the other
        side, and dropping it turns their call into a timeout.
        """
        if self._consumer is not None:
            await self._consumer.close()

    async def __aenter__(self) -> ResponderHandle:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


async def serve(
    connection: Connection,
    queue: str,
    respond: Responder,
    *,
    codec: Codec | None = None,
    **consume_options: Any,
) -> ResponderHandle:
    """Answers requests arriving on a queue, until the responder is closed::

        async def price(message: Message) -> dict[str, Any]:
            return {"pence": look_up(message.payload["sku"])}

        async with await serve(mq, "price-requests", price) as responder:
            ...
            responder.answered       # requests answered
            responder.unanswerable   # requests that named nowhere to reply

    The responder returns the answer and raises to fail. A failure is sent back
    to the caller rather than swallowed, because a caller blocked on a reply
    should learn that it failed in milliseconds rather than wait out its whole
    timeout to learn nothing.

    A :class:`ResponderHandle` comes back rather than the bare
    :class:`~acemq_amqp.Consumer` this used to return. It is closed the same way
    and is the same ``async with``; what it adds is the two counters Java and
    .NET have always reported and this library reported nowhere. ``consumer``
    reaches the consumer underneath for anything not forwarded.

    :param connection: where requests arrive and replies go
    :param queue: what to consume
    :param respond: what to answer with
    :param codec: a codec other than the connection's
    :param consume_options: passed to :meth:`~acemq_amqp.Connection.consume`
    :returns: the running responder
    """
    # Before the subscribe, and that is the guarantee rather than tidiness: a
    # broker may deliver the first request from inside connection.consume, and
    # the handler below reads these fields on that delivery.
    responder = ResponderHandle(queue)

    async def handle(request: Message) -> Ack:
        # Header first, native property second — the same order in all five
        # libraries. A caller written against this library sets both; one
        # written against Java or .NET sets only the property; and a request
        # that came through a service which rebuilt the message has only the
        # header left. Reading either answers all three.
        reply_to = str(request.envelope.headers.get(HEADER_REPLY_TO) or "") or request.reply_to
        if not reply_to:
            # Retrying cannot make a return address appear, so this is
            # dead-lettered rather than looped. Counted first: the sender is the
            # thing that is broken, and this number is what says so.
            responder._unanswerable += 1
            log.warning(
                "acemq: a message on %s asked for no reply, so none was sent. It was "
                "published without a reply-to, which usually means the sender used "
                "publish where it meant to use request.",
                queue,
            )
            return reject(
                FatalError(
                    f"acemq: request {request.envelope.id} carries neither a "
                    f"{HEADER_REPLY_TO} header nor a reply-to property, so there "
                    "is nowhere to reply"
                )
            )

        try:
            answered = respond(request)
            answer = await answered if inspect.isawaitable(answered) else answered
        except Exception as failure:
            try:
                # Not counted as answered. The caller gets a reply, and it is the
                # reply that says the responder could not do it — counting it
                # would make a service that fails every request report a
                # perfectly healthy answered rate.
                await _reply(connection, reply_to, request, None, codec, failure)
            except Exception as undeliverable:
                # The caller has not been told, so it is still waiting. Retrying
                # is the only path that can still reach it.
                return retry(undeliverable)
            # Settled rather than retried: the caller has its answer, and a
            # retry would answer the same question twice.
            return reject(failure)

        # Counted before the reply goes out, and that order is the contract. The
        # reply and the counter are two things one caller can see, and publishing
        # first leaves a window in which a caller already holding its answer
        # reads answered as zero. Incrementing first puts the counter ahead of
        # the reply in every interleaving there is, which is the only ordering a
        # reader can rely on — and the increment is handed back below when the
        # publish fails, so the failure incrementing early would otherwise
        # introduce does not exist either.
        responder._answered += 1
        try:
            await _reply(connection, reply_to, request, answer, codec, None)
        except Exception as undeliverable:
            responder._answered -= 1
            # The work is done but the answer did not get out. Retrying repeats
            # the work, which is why a responder should be idempotent.
            return retry(undeliverable)
        return accept()

    responder._consumer = await connection.consume(
        queue, handle, codec=codec, **consume_options
    )
    return responder


async def _reply(
    connection: Connection,
    reply_to: str,
    request: Message,
    answer: Any,
    codec: Codec | None,
    failure: Exception | None,
) -> None:
    """Sends one reply back to where the request said to send it.

    Not mandatory, deliberately. A generated reply queue is auto-deleting and
    belongs to a caller that may have given up, so a reply reaching no queue is
    the ordinary aftermath of a timeout rather than a fault worth retrying.
    """
    headers: Mapping[str, Any] = (
        {HEADER_ERROR: f"{type(failure).__name__}: {failure}"} if failure else {}
    )
    await connection.publisher("", reply_to, codec=codec).send(
        answer,
        envelope=Envelope(
            correlation_id=request.envelope.correlation_id,
            causation_id=request.envelope.id,
            origin=connection.origin,
            headers=headers,
        ),
    )
