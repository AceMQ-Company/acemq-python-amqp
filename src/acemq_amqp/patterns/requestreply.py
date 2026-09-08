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

The pairing is the envelope's correlation identifier, and the return address is
an ordinary application header. Neither uses AMQP's own ``reply-to`` and
``correlation-id`` properties: those are lost the moment a message passes through
a service that rebuilds it, and the envelope is the thing this library promises
to carry end to end.
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from ..ack import Ack, FatalError, accept, reject, retry
from ..codec import Codec
from ..connection import Connection, Consumer, Message
from ..envelope import Envelope
from ..errors import AceMQError
from ..topology import Topology

#: Where a responder should send its answer.
#:
#: An application header rather than AMQP's ``reply-to`` property, so it travels
#: through the same envelope machinery as everything else and survives a hop
#: through a service that rebuilds the message. It deliberately carries no
#: ``x-acemq-`` prefix: that namespace belongs to the engine and is stripped
#: before a handler sees it, so a responder could never read this one.
HEADER_REPLY_TO = "acemq-reply-to"

#: A responder's failure, carried back to the caller.
HEADER_ERROR = "acemq-error"

#: How long :meth:`Requester.ask` waits unless it is told otherwise.
DEFAULT_TIMEOUT = timedelta(seconds=30)

#: What a responder is: a message in, an answer out, an exception for a failure.
Responder = Callable[[Message], Any]


class RequestTimeoutError(AceMQError):
    """No reply arrived before the deadline.

    It says nothing about whether the request was handled. A timeout is the
    absence of an answer, not evidence that nothing happened, which is why a
    request that changes anything should be idempotent at the other end.
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
        try:
            await self._publisher.send(request, envelope=outgoing)
            answer = await asyncio.wait_for(waiter, deadline.total_seconds())
        except asyncio.TimeoutError as expired:
            raise RequestTimeoutError(
                f"acemq: no reply to {correlation} arrived within {deadline}"
            ) from expired
        finally:
            self._waiting.pop(correlation, None)

        failure = answer.envelope.headers.get(HEADER_ERROR)
        if failure:
            raise ResponderError(
                f"acemq: the responder could not answer {correlation}: {failure}"
            )
        return answer.payload

    async def _receive(self, reply: Message) -> Ack:
        """Hands a reply to whoever is waiting for it.

        A reply nobody is waiting for is accepted and dropped, which is what a
        reply to a request that already timed out is. It is not an error and it
        is not worth dead-lettering: the caller has gone.
        """
        waiter = self._waiting.pop(reply.envelope.correlation_id, None)
        if waiter is not None and not waiter.done():
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


async def serve(
    connection: Connection,
    queue: str,
    respond: Responder,
    *,
    codec: Codec | None = None,
    **consume_options: Any,
) -> Consumer:
    """Answers requests arriving on a queue, until the consumer is closed::

        async def price(message: Message) -> dict[str, Any]:
            return {"pence": look_up(message.payload["sku"])}

        async with await serve(mq, "price-requests", price):
            ...

    The responder returns the answer and raises to fail. A failure is sent back
    to the caller rather than swallowed, because a caller blocked on a reply
    should learn that it failed in milliseconds rather than wait out its whole
    timeout to learn nothing.

    An ordinary :class:`~acemq_amqp.Consumer` comes back rather than a wrapper of
    its own: it already knows how to be closed and how to be an ``async with``,
    and a second class that only forwards to it would be one more thing to learn.

    :param connection: where requests arrive and replies go
    :param queue: what to consume
    :param respond: what to answer with
    :param codec: a codec other than the connection's
    :param consume_options: passed to :meth:`~acemq_amqp.Connection.consume`
    :returns: the running consumer
    """

    async def handle(request: Message) -> Ack:
        reply_to = str(request.envelope.headers.get(HEADER_REPLY_TO) or "")
        if not reply_to:
            # Retrying cannot make a return address appear, so this is
            # dead-lettered rather than looped.
            return reject(
                FatalError(
                    f"acemq: request {request.envelope.id} carries no {HEADER_REPLY_TO} "
                    "header, so there is nowhere to reply"
                )
            )

        try:
            answered = respond(request)
            answer = await answered if inspect.isawaitable(answered) else answered
        except Exception as failure:
            try:
                await _reply(connection, reply_to, request, None, codec, failure)
            except Exception as undeliverable:
                # The caller has not been told, so it is still waiting. Retrying
                # is the only path that can still reach it.
                return retry(undeliverable)
            # Settled rather than retried: the caller has its answer, and a
            # retry would answer the same question twice.
            return reject(failure)

        try:
            await _reply(connection, reply_to, request, answer, codec, None)
        except Exception as undeliverable:
            # The work is done but the answer did not get out. Retrying repeats
            # the work, which is why a responder should be idempotent.
            return retry(undeliverable)
        return accept()

    return await connection.consume(queue, handle, codec=codec, **consume_options)


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
