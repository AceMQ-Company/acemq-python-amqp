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

"""Wrapping a handler in the things every handler needs, and joining them up.

Two ideas that belong together. A chain puts the cross-cutting concerns —
logging, a deadline, the idempotency guard — around one handler in an order you
can read. :func:`then` makes a pipeline out of several services: each consumes,
does its part, and publishes what comes out, carrying the correlation forward so
the whole run can be followed afterwards.

There is no middleware here for turning an exception into a decision, and that
is deliberate. An exception escaping a handler is already a retry — it is how
Python says a thing failed, and most failures it carries are transient — so a
wrapper that caught them would only be able to make that worse.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any, TypeAlias

from ..ack import Ack, accept, retry
from ..connection import AsyncHandler, Handler, Message, Publisher
from ..envelope import Envelope
from ._support import decide
from .idempotency import IdempotencyStore, idempotent
from .ordered import PartitionKey, ordered

log = logging.getLogger("acemq")

#: Wraps a handler in another handler. The order in a :func:`chain` reads
#: outside-in: the first one given is the outermost, so it sees the message
#: first and the decision last.
Middleware: TypeAlias = Callable[[Handler], AsyncHandler]


class _Nothing:
    """The absence of a message to send on. There is exactly one."""

    def __repr__(self) -> str:
        return "NOTHING"


#: What a pipeline step returns when this message does not continue.
#:
#: A sentinel rather than ``None``, because ``None`` is a payload somebody may
#: legitimately want to publish, and rather than an empty instance of the
#: outgoing type, because inventing an empty message to mean "no message" is how
#: a downstream service ends up handling one.
NOTHING = _Nothing()


def chain(handler: Handler, *middleware: Middleware) -> AsyncHandler:
    """Wraps a handler in middleware, outermost first::

        handler = chain(
            handle,
            with_logging(),
            with_timeout(timedelta(seconds=10)),
            with_idempotency(store),
        )

    Logging is outermost, so it records what the deadline and the idempotency
    guard decided rather than only what the handler did.

    :param handler: what does the work
    :param middleware: what goes round it, outermost first
    :returns: the wrapped handler
    """
    # Applied in reverse so the first one named ends up outermost, which is the
    # order somebody reading the list expects.
    composed: Handler = handler
    for outer in reversed(middleware):
        composed = outer(composed)

    async def piped(message: Message) -> Ack:
        return await decide(composed, message)

    return piped


def with_timeout(limit: timedelta) -> Middleware:
    """Gives the handler a deadline.

    A handler that runs past it is reported as a retry whatever it would have
    said, which is a deliberate choice between two imperfect answers: retrying
    work that may have succeeded risks doing it twice, and accepting work that
    may have failed loses it. Duplicates are a problem that can be solved — see
    :func:`~acemq_amqp.patterns.idempotent` — and a lost message is not.

    An ``async`` handler is really cancelled at the deadline, so it stops. A
    blocking handler running on a worker thread — which is what the
    :mod:`acemq_amqp.sync` API gives it — cannot be: the message is settled as a
    retry on time, but the thread carries on to the end of whatever it was
    doing. That is worth knowing before putting a short deadline in front of one.

    :param limit: how long the handler gets
    :returns: the middleware
    """
    if limit <= timedelta(0):
        raise ValueError(f"acemq: a handler timeout must be positive, got {limit}")

    def wrap(handler: Handler) -> AsyncHandler:
        async def within(message: Message) -> Ack:
            try:
                return await asyncio.wait_for(
                    decide(handler, message), limit.total_seconds()
                )
            except asyncio.TimeoutError:
                return retry(
                    TimeoutError(
                        f"acemq: handling message {message.envelope.id} did not "
                        f"finish within {limit}"
                    )
                )

        return within

    return wrap


def with_logging(to: logging.Logger | None = None) -> Middleware:
    """Records what happened to each message, and how long it took.

    It logs the decision rather than the payload. A payload is the one thing on
    a message that may not be safe to write down, and a log line that is unsafe
    to keep is one somebody eventually turns off.

    :param to: where to log, defaulting to the ``acemq`` logger. Passing one is
        how a service files these with the rest of its own logs
    :returns: the middleware
    """
    writer = to or log

    def wrap(handler: Handler) -> AsyncHandler:
        async def recorded(message: Message) -> Ack:
            started = datetime.now(timezone.utc)
            try:
                decision = await decide(handler, message)
            except Exception:
                # Logged and re-raised, not swallowed: the consumer above turns
                # an exception into the retry policy, and catching it here would
                # quietly take that decision away.
                writer.exception(
                    "acemq %s type=%s attempt=%d took=%s raised",
                    message.envelope.id,
                    message.envelope.type,
                    message.envelope.attempt,
                    _since(started),
                )
                raise

            writer.info(
                "acemq %s type=%s attempt=%d took=%s %s%s",
                message.envelope.id,
                message.envelope.type,
                message.envelope.attempt,
                _since(started),
                decision,
                f": {decision.error}" if decision.error else "",
            )
            return decision

        return recorded

    return wrap


def with_idempotency(
    store: IdempotencyStore, *, key: Callable[[Message], str] | None = None
) -> Middleware:
    """:func:`~acemq_amqp.patterns.idempotent` as middleware, so it can sit in a
    :func:`chain`."""

    def wrap(handler: Handler) -> AsyncHandler:
        return idempotent(store, handler, key=key)

    return wrap


def with_ordering(key: PartitionKey) -> Middleware:
    """:func:`~acemq_amqp.patterns.ordered` as middleware."""

    def wrap(handler: Handler) -> AsyncHandler:
        return ordered(key, handler)

    return wrap


def then(publisher: Publisher, step: Callable[[Message], Any]) -> AsyncHandler:
    """Publishes onwards whatever handling a message produced::

        handler = then(
            mq.publisher("shipping-events", "shipment.requested"),
            lambda message: NOTHING if message.payload["digital"] else ship(message),
        )

    The step that turns a set of services into a pipeline. The correlation
    identifier is carried forward and the incoming message becomes the outgoing
    one's causation, so afterwards the whole run can be reconstructed from any
    message in it.

    Returning :data:`NOTHING` publishes nothing and accepts the message, which
    is how a step says "this one does not continue" without inventing an empty
    message for the next service to handle.

    The message is accepted only once the next one is out, so a publish that
    fails retries the step and the work runs again — which is why a step that
    changes anything should be idempotent.

    :param publisher: where the result goes
    :param step: what to do, returning the payload to send on or :data:`NOTHING`
    :returns: the handler
    """

    async def onwards(message: Message) -> Ack:
        produced = await decide(step, message)
        if produced is NOTHING:
            return accept()

        try:
            await publisher.send(
                produced,
                envelope=Envelope(
                    correlation_id=message.envelope.correlation_id,
                    causation_id=message.envelope.id,
                ),
            )
        except Exception as failure:
            return retry(
                RuntimeError(
                    f"acemq: the work for message {message.envelope.id} is done but "
                    f"the next message did not go out: {failure}"
                )
            )
        return accept()

    return onwards


def _since(started: datetime) -> str:
    """How long ago, to the millisecond, which is as fine as this is useful."""
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return f"{elapsed * 1000:.0f}ms"
