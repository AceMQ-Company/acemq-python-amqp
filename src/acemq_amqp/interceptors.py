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

"""The seam around publishing and handling, for the work that surrounds them.

Every organisation has something every message needs and no library can guess: a
tenant identifier, a trace context, an authorisation token, a schema version, a
correlation identifier in the logging context, a timer, a size limit. Without a
seam these end up copied into every call site, where one of them is eventually
forgotten and nobody finds out until the message that needed it is the one that
went without it.

An interceptor is one function that is handed the message and the rest of the
work::

    async def timed(context: ConsumeContext, handle: ConsumeNext) -> Ack:
        started = time.monotonic()
        try:
            return await handle(context)
        finally:
            log.info("%s took %.3fs", context.queue, time.monotonic() - started)

    mq.intercept(timed)

That shape rather than the pair of before-and-after hooks the Java library uses,
because Python already has the construct: ``try``/``finally`` runs the way out
in the reverse of the way in, nests correctly without anybody having to reverse
a list, and makes an interceptor that opens something and closes it one function
instead of two halves that have to agree. It composes the way ASGI middleware
does, which is the arrangement a Python programmer already knows.

Interceptors run in the order they were registered: the first registered is the
outermost, so it sees the message first on the way in and last on the way out.
That matters as soon as one of them reads what another wrote.

Refusing is raising. A publish interceptor that raises stops the publish and the
caller sees the exception, which is the whole point of being able to intercept
rather than only observe: a message that must not go out can be stopped in one
place rather than in every publisher. A consume interceptor that raises is
treated exactly as a handler that raised — retried, and then dead-lettered —
because the alternative is acknowledging a message nothing processed.

What a handler answered is not what happened to the message. A handler asking
for another attempt when there are none left is dead-lettered, and an
interceptor that wants to record how a delivery really ended has to wait for the
consumer to say — :meth:`ConsumeContext.when_settled` is where it waits, and
:class:`~acemq_amqp.ack.Settlement` is what it is told.

Everything here is built out of the public API and needs no private access. An
interceptor sees the envelope, the payload, the destination and the body, and
may change any of them; that is the same rule the patterns in
:mod:`acemq_amqp.patterns` follow, and it is what keeps this a seam rather than
a privileged back door.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias

from .ack import Ack, Settlement
from .envelope import Envelope
from .transport import PublishResult

log = logging.getLogger("acemq")

#: Told what the consumer did with a delivery, once it had decided and before it
#: had done it. Registered with :meth:`ConsumeContext.when_settled`.
SettlementListener: TypeAlias = Callable[[Settlement], None]


@dataclass(slots=True)
class PublishContext:
    """A message on its way out, before it is encoded.

    Mutable on purpose, and mutable rather than rebuilt-and-returned: an
    interceptor that only wants to add one header should not have to reconstruct
    the rest, and one that reads what an earlier interceptor wrote needs to be
    looking at the same object rather than at a copy of an earlier version.

    :param exchange: where it is going. Changing it redirects the message
    :param routing_key: what it is published under
    :param envelope: the metadata. Reserved header names are still refused when
        the envelope is rendered, whoever wrote them
    :param payload: what is about to be encoded, before the codec sees it, so an
        interceptor can change the message and not only its metadata
    :param persistent: whether the broker is asked to write it to disk
    :param mandatory: whether reaching no queue is an error rather than a
        silence
    """

    exchange: str
    routing_key: str
    envelope: Envelope
    payload: Any
    persistent: bool = True
    mandatory: bool = False

    def set_header(self, name: str, value: Any) -> None:
        """Adds an application header to the message being published.

        A convenience over rebuilding the envelope, which is what the three
        commonest interceptors — a tenant, a trace context, a schema version —
        all want to do and nothing else.
        """
        self.envelope = self.envelope.with_(headers={**self.envelope.headers, name: value})


@dataclass(slots=True)
class ConsumeContext:
    """A message that has arrived and been decoded, before the handler sees it.

    :param queue: what it was read from
    :param envelope: the metadata, as it will reach the handler. Changing it
        changes what the handler is given
    :param payload: the decoded body, likewise
    :param body: the undecoded bytes, for an interceptor that wants to see what
        actually arrived
    :param content_type: what the sender said the body was, or ``None``
    :param routing_key: the key it arrived under
    :param redelivered: the broker saying it has handed this one over before
    :param state: somewhere for an interceptor to leave something for a later
        one, or for its own way out. Empty, and this library never reads it
    :param reports_settlement: whether whatever is driving this delivery will
        say how it was settled. True when a consumer is, and false when an
        interceptor chain is being run by something else — a test, or a caller
        composing the chain by hand
    """

    queue: str
    envelope: Envelope
    payload: Any
    body: bytes
    content_type: str | None
    routing_key: str
    redelivered: bool
    state: dict[str, Any] = field(default_factory=dict)
    reports_settlement: bool = False
    _settlement_listeners: list[SettlementListener] = field(
        default_factory=list, repr=False
    )

    def when_settled(self, listener: SettlementListener) -> bool:
        """Asks to be told how this delivery was settled.

        What the handler answered is not what happened to the message: a
        handler asking for a retry with no attempts left is dead-lettered, and
        an interceptor that read only the :class:`~acemq_amqp.ack.Ack` would
        report a retry that never happened. That gap is exactly what somebody
        querying a trace backend for dead letters falls into, so the consumer
        reports its decision here and the tracing adapter waits for it.

        The listener is called once, on the consumer's task, after the decision
        and before it is carried out. It should not block and must not raise;
        one that does is logged and otherwise ignored, because a delivery still
        has to be settled when whoever was watching it breaks.

        :param listener: called with the :class:`~acemq_amqp.ack.Settlement`
        :returns: whether anything will ever call it. ``False`` when this
            delivery is not being driven by a consumer, in which case the
            caller should not wait for an answer that is not coming
        """
        if not self.reports_settlement:
            return False
        self._settlement_listeners.append(listener)
        return True

    def settled(self, settlement: Settlement) -> None:
        """Tells the listeners what happened. Called by the consumer.

        :param settlement: what the consumer decided to do with the delivery
        """
        for listener in self._settlement_listeners:
            try:
                listener(settlement)
            except Exception:
                # The delivery still has to be settled. A listener is an
                # observer, and an observer that breaks must not take the
                # message with it.
                log.exception(
                    "acemq: a settlement listener failed for a delivery from %s", self.queue
                )


#: The rest of a publish: the next interceptor, or the publish itself.
PublishNext: TypeAlias = Callable[[PublishContext], Awaitable[PublishResult]]

#: The rest of a delivery: the next interceptor, or the handler itself.
ConsumeNext: TypeAlias = Callable[[ConsumeContext], Awaitable[Ack]]

#: Runs around every publish on a connection.
#:
#: Call ``send`` to carry on and return what it returns; do not call it to
#: refuse, and raise to make the refusal the caller's problem rather than a
#: message that quietly went nowhere.
PublishInterceptor: TypeAlias = Callable[
    [PublishContext, PublishNext], Awaitable[PublishResult]
]

#: Runs around every handler on a connection.
#:
#: Call ``handle`` to carry on. Raising means the handler never runs and the
#: delivery is treated as a failed one, which is the honest outcome: the
#: alternative is acknowledging something nothing processed.
ConsumeInterceptor: TypeAlias = Callable[[ConsumeContext, ConsumeNext], Awaitable[Ack]]


def publish_chain(
    interceptors: Sequence[PublishInterceptor], publish: PublishNext
) -> PublishNext:
    """Folds interceptors around a publish, first registered outermost.

    Built backwards because that is what nesting is: the last interceptor wraps
    the publish, the one before wraps that, and the first ends up on the
    outside, seeing the message before anybody and its result after everybody.

    :param interceptors: what to wrap it in, in registration order
    :param publish: the innermost work, which actually sends the message
    :returns: the whole thing as one callable, or ``publish`` when there is
        nothing to wrap it in
    """
    composed = publish
    for interceptor in reversed(interceptors):
        composed = _publish_step(interceptor, composed)
    return composed


def consume_chain(
    interceptors: Sequence[ConsumeInterceptor], handle: ConsumeNext
) -> ConsumeNext:
    """Folds interceptors around a handler, first registered outermost.

    :param interceptors: what to wrap it in, in registration order
    :param handle: the innermost work, which runs the handler
    :returns: the whole thing as one callable, or ``handle`` when there is
        nothing to wrap it in
    """
    composed = handle
    for interceptor in reversed(interceptors):
        composed = _consume_step(interceptor, composed)
    return composed


def _publish_step(interceptor: PublishInterceptor, rest: PublishNext) -> PublishNext:
    """One link, bound in a function of its own.

    Not a closure written inline in the loop above: a closure made in a loop
    captures the variable and not its value, so every link would end up calling
    the last interceptor. It is the oldest bug in Python and it would show up
    here as interceptors that all appear to run and only one of which does.
    """

    async def step(context: PublishContext) -> PublishResult:
        return await interceptor(context, rest)

    return step


def _consume_step(interceptor: ConsumeInterceptor, rest: ConsumeNext) -> ConsumeNext:
    async def step(context: ConsumeContext) -> Ack:
        return await interceptor(context, rest)

    return step
