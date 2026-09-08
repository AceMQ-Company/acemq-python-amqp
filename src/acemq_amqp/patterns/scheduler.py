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

"""Delivering a message later.

::

    async with await Scheduler.open(mq) as scheduler:
        await scheduler.after(timedelta(hours=4), "billing", "invoice.due", invoice)
        await scheduler.at(renewal_date, "policies", "policy.renew", policy)

Why not a per-message time to live
----------------------------------

The obvious implementation is to set an expiration on the message, drop it in a
queue nobody consumes, and let it dead-letter to its destination. It is what
most articles suggest and it is wrong for anything but a single fixed delay,
because **a classic queue expires messages only at its head**.

Put a four-hour message in, then a one-minute message behind it, and the
one-minute message is delivered in four hours. Nothing reports this: the queue
looks healthy, the message is not lost, it is simply late by a factor nobody
predicted. It is the most common way a home-made scheduler fails, and it fails
in production under mixed load rather than in testing under uniform load.

What this does instead
----------------------

A small ladder of queues, each with a *uniform* time to live, and a message hops
through them until it is due::

    acemq.schedule.1s  acemq.schedule.10s  acemq.schedule.1m
    acemq.schedule.10m  acemq.schedule.1h

Every message in a given rung has the same delay, so head-of-line expiry is not
a problem — the head is always the message due soonest. Each expiry returns the
message to ``acemq.schedule.due``, and this scheduler either delivers it or puts
it in the largest rung that does not overshoot. A four-hour delay is four
one-hour hops; a ninety-second delay is one minute, then three tens.

The cost is honest and worth stating: a long delay is several broker round trips
rather than one, and delivery is accurate to about the smallest rung rather than
to the second. A scheduler that must fire at 09:00:00.000 exactly is a
scheduler, not a message broker.

The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
properly and is a plugin — so it is not available everywhere, and a library that
silently required it would be a library that works on your laptop.

Shared with the other languages
-------------------------------

Every name below is the same as the Java library's, argument for argument. Two
services scheduling on one broker declare the same queues, so a difference would
not be a difference in behaviour — it would be a ``PRECONDITION_FAILED`` on
whichever of them started second. :func:`schedule_topology` is the whole of it,
and it is pinned by a test.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..ack import Ack, accept, reject
from ..codec import JSON_CONTENT_TYPE, BytesCodec, Codec
from ..connection import Connection, Consumer, Message
from ..envelope import Envelope
from ..errors import AceMQError
from ..topology import (
    DEAD_LETTER_EXCHANGE_ARG,
    DEAD_LETTER_ROUTING_KEY_ARG,
    MESSAGE_TTL_ARG,
    Topology,
)

log = logging.getLogger("acemq")

#: The exchange every scheduler queue hangs off. Direct, because every binding
#: on it matches a queue name exactly.
SCHEDULE_EXCHANGE = "acemq.schedule"

#: Where a rung dead-letters to, and the only queue a scheduler consumes.
SCHEDULE_DUE = "acemq.schedule.due"

# Deliberately not the ``x-acemq-`` prefix. That one is reserved:
# :func:`acemq_amqp.headers.is_reserved` matches it, ``Envelope`` refuses it in
# an application's headers outright, and anything carrying it is stripped from
# the application's view on the way in because engine headers are materialised
# as envelope fields instead. A scheduler header using it would be refused on
# publish and, in the libraries that do not refuse it, gone on consume.

#: Where the message is going once it is due.
HEADER_SCHEDULE_EXCHANGE = "x-schedule-exchange"

#: What it is published under once it is due.
HEADER_SCHEDULE_ROUTING_KEY = "x-schedule-routing-key"

#: When it is due, as epoch milliseconds — the same integer encoding as
#: ``x-acemq-first-seen``, and what Java writes from ``Instant.toEpochMilli()``.
HEADER_SCHEDULE_DUE_AT = "x-schedule-due-at"

#: What the payload was encoded as when it was scheduled.
#:
#: Carried because the scheduler republishes bytes rather than objects, and a
#: consumer picks its codec from the content type. Publishing pre-encoded bytes
#: under ``application/octet-stream`` produces a message the intended consumer
#: cannot decode — it arrives, it is the right bytes, and nothing can read it.
HEADER_SCHEDULE_CONTENT_TYPE = "x-schedule-content-type"

#: The rungs, longest first.
#:
#: Five of them, spanning a second to an hour. More rungs mean finer accuracy
#: and more queues; fewer mean more hops for a long delay. This spread delivers
#: a one-day message in twenty-four hops and a one-minute message in one, which
#: is the right way round — short delays are common and want to be cheap.
SCHEDULE_RUNGS: tuple[timedelta, ...] = (
    timedelta(hours=1),
    timedelta(minutes=10),
    timedelta(minutes=1),
    timedelta(seconds=10),
    timedelta(seconds=1),
)

#: How many expired messages the control consumer holds at once. It does no work
#: per message beyond a republish, so it can hold rather more than an ordinary
#: consumer without any of them waiting on it.
DEFAULT_SCHEDULE_PREFETCH = 50

#: The envelope type every message the scheduler moves is published under.
SCHEDULED_TYPE = "ScheduledMessage"


def schedule_rung_name(rung: timedelta) -> str:
    """The queue name for a rung: ``acemq.schedule.1h`` and its four siblings.

    :param rung: how long the rung holds a message
    :returns: the queue name
    """
    return f"{SCHEDULE_EXCHANGE}.{_describe(rung)}"


def _describe(rung: timedelta) -> str:
    """A duration rendered the way the queue names spell it.

    Whole hours as ``{n}h``, whole minutes as ``{n}m``, anything else as
    ``{n}s`` — the same three cases in the same order as Java, because the
    output is a queue name and a queue name is a contract.
    """
    millis = int(rung.total_seconds() * 1000)
    if millis % 3_600_000 == 0:
        return f"{millis // 3_600_000}h"
    if millis % 60_000 == 0:
        return f"{millis // 60_000}m"
    return f"{millis // 1_000}s"


def schedule_rung_args(rung: timedelta) -> dict[str, Any]:
    """The arguments a rung queue must be declared with.

    Exactly three keys, and the same three in every AceMQ library: the rung as
    ``x-message-ttl``, :data:`SCHEDULE_EXCHANGE` as the dead-letter target and
    :data:`SCHEDULE_DUE` as the routing key it is sent under. Nothing consumes a
    rung; the time to live is the only thing that ever takes a message out of
    one.

    :param rung: how long the rung holds a message
    :returns: the argument table, which is a contract and is pinned by a test
    """
    return {
        MESSAGE_TTL_ARG: int(rung.total_seconds() * 1000),
        DEAD_LETTER_EXCHANGE_ARG: SCHEDULE_EXCHANGE,
        DEAD_LETTER_ROUTING_KEY_ARG: SCHEDULE_DUE,
    }


def schedule_topology() -> Topology:
    """Everything a scheduler needs a broker to have.

    One direct exchange, five rung queues and the control queue, all classic and
    all bound on their own names. Every queue is classic rather than quorum, and
    declared by leaving ``x-queue-type`` off entirely, which is the spelling
    Java sends and therefore the only one a broker finds equivalent to it.

    Applied by :meth:`Scheduler.open`. It is public so that a deployment can
    declare the topology from a migration and run its services with a login that
    has no ``configure`` permission at all, and so that anybody wondering what a
    scheduler puts on their broker can print it.

    :returns: the topology
    """
    topology = Topology().exchange(SCHEDULE_EXCHANGE, "direct")
    for rung in SCHEDULE_RUNGS:
        name = schedule_rung_name(rung)
        topology.queue(name, quorum=False, args=schedule_rung_args(rung))
        topology.binding(name, SCHEDULE_EXCHANGE, name)
    topology.queue(SCHEDULE_DUE, quorum=False)
    topology.binding(SCHEDULE_DUE, SCHEDULE_EXCHANGE, SCHEDULE_DUE)
    return topology


class _Verbatim:
    """Writes already-encoded bytes out unchanged, under a given content type.

    Publishing them through an ordinary codec would encode them a second time,
    and what arrives is JSON containing JSON. Publishing them through
    :class:`~acemq_amqp.BytesCodec` loses the content type, and what arrives
    cannot be decoded by the consumer that was waiting for it.
    """

    def __init__(self, content_type: str) -> None:
        self._content_type = content_type

    @property
    def content_type(self) -> str:
        return self._content_type

    def encode(self, payload: Any) -> bytes:
        if isinstance(payload, bytes):
            return payload
        raise TypeError(
            f"acemq: the scheduler moves bytes, not a {type(payload).__name__}"
        )

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        raise NotImplementedError("acemq: the scheduler only publishes")

    def can_decode(self, content_type: str | None) -> bool:
        return False


class Scheduler:
    """Delivers messages later, through a ladder of time-to-live queues.

    Built by :meth:`open`, because it declares its topology and starts a
    consumer, and both have to have happened before it is any use. Close it when
    done: it holds a consumer.

    One per process is plenty and several are harmless — they are all consuming
    the same control queue, and the broker gives each expired message to exactly
    one of them.
    """

    def __init__(self, connection: Connection, codec: Codec | None = None) -> None:
        self._connection = connection
        self._codec = codec or connection.codec
        self._consumer: Consumer | None = None
        self._scheduled = 0
        self._delivered = 0
        self._hops = 0

    @classmethod
    async def open(
        cls,
        connection: Connection,
        *,
        codec: Codec | None = None,
        prefetch: int = DEFAULT_SCHEDULE_PREFETCH,
    ) -> Scheduler:
        """Declares the topology and starts consuming the control queue.

        :param connection: an open connection, which the scheduler publishes and
            consumes on
        :param codec: what to encode scheduled payloads with, instead of the
            connection's. Whatever it is travels with the message as
            :data:`HEADER_SCHEDULE_CONTENT_TYPE`, so the eventual consumer can choose the
            matching one
        :param prefetch: how many expired messages to hold at once
        :returns: the scheduler, already consuming
        """
        await connection.declare(schedule_topology())
        scheduler = cls(connection, codec)
        # Raw bytes: this scheduler never looks inside a payload, and decoding
        # one it has no business understanding is how a scheduler acquires
        # opinions about message formats.
        #
        # ``declare=False`` because a consumer declares its dead-letter queues
        # at start-up, and the control queue's would be ``acemq.schedule.due.dlq``
        # and ``acemq.schedule.due.parked`` — two durable queues per service
        # running a scheduler, that nothing publishes to, nothing reads and
        # nobody deletes. Everything this consumer needs is in
        # :func:`schedule_topology`, which has already been applied above.
        scheduler._consumer = await connection.consume(
            SCHEDULE_DUE,
            scheduler._forward,
            codec=BytesCodec(),
            prefetch=prefetch,
            declare=False,
        )
        log.debug(
            "acemq: scheduler declared with rungs %s",
            [_describe(rung) for rung in SCHEDULE_RUNGS],
        )
        return scheduler

    @property
    def scheduled(self) -> int:
        """Messages handed to this scheduler."""
        return self._scheduled

    @property
    def delivered(self) -> int:
        """Messages this scheduler sent on to their destination."""
        return self._delivered

    @property
    def hops(self) -> int:
        """How many times a message moved between rungs.

        Divided by :attr:`delivered` this is the average number of hops, which
        is the number to look at if the scheduler is busier than expected: long
        delays cost hops.
        """
        return self._hops

    async def after(
        self, delay: timedelta, exchange: str, routing_key: str, payload: Any
    ) -> None:
        """Delivers a message after a delay.

        :param delay: how long to wait; zero or negative delivers immediately
        :param exchange: where it is going, empty for the default exchange
        :param routing_key: what to publish it under, or a queue name
        :param payload: what to send
        """
        await self.at(_now() + delay, exchange, routing_key, payload)

    async def at(
        self, when: datetime, exchange: str, routing_key: str, payload: Any
    ) -> None:
        """Delivers a message at a moment.

        :param when: when it is due. It must carry a time zone: a naive datetime
            would be read as the local time of whichever machine scheduled it,
            and a scheduler is the last place to discover that two of them
            disagree
        :param exchange: where it is going, empty for the default exchange
        :param routing_key: what to publish it under, or a queue name
        :param payload: what to send
        :raises ValueError: when ``when`` has no time zone
        """
        if when.tzinfo is None:
            raise ValueError(
                f"acemq: {when} has no time zone, and a scheduled message needs one. "
                "Use datetime.now(timezone.utc) rather than datetime.now()"
            )

        headers: dict[str, Any] = {
            HEADER_SCHEDULE_EXCHANGE: exchange,
            HEADER_SCHEDULE_ROUTING_KEY: routing_key,
            HEADER_SCHEDULE_DUE_AT: _millis(when),
            # Encoded once, here, and carried as bytes from then on. The content
            # type goes with them, because that is how the eventual consumer
            # chooses a codec.
            HEADER_SCHEDULE_CONTENT_TYPE: self._codec.content_type,
        }
        self._scheduled += 1
        await self._route(self._codec.encode(payload), headers, when)

    async def _forward(self, message: Message) -> Ack:
        """Handles one message that has come out of a rung."""
        headers = dict(message.envelope.headers)
        due_at = headers.get(HEADER_SCHEDULE_DUE_AT)
        if (
            due_at is None
            or HEADER_SCHEDULE_EXCHANGE not in headers
            or HEADER_SCHEDULE_ROUTING_KEY not in headers
        ):
            # Rejected rather than retried: no number of attempts adds a header.
            return reject(
                AceMQError(
                    f"acemq: a message reached {SCHEDULE_DUE} without the headers a scheduled "
                    "message carries. Something else is publishing into the scheduler's "
                    "queues, which it must not: they are an implementation detail"
                )
            )
        try:
            when = _from_millis(_number(due_at))
        except AceMQError as unreadable:
            return reject(unreadable)
        await self._route(message.body, headers, when)
        return accept()

    async def _route(self, body: bytes, headers: dict[str, Any], when: datetime) -> None:
        """Delivers if it is due, and otherwise puts it in the largest rung that
        does not overshoot."""
        remaining = when - _now()
        if remaining < SCHEDULE_RUNGS[-1]:
            # Due, or so nearly due that another hop would cost more than the
            # accuracy it buys.
            await self._deliver(body, headers)
            return

        rung = next(
            (candidate for candidate in SCHEDULE_RUNGS if candidate <= remaining),
            SCHEDULE_RUNGS[-1],
        )
        self._hops += 1
        await self._connection.publisher(
            SCHEDULE_EXCHANGE, schedule_rung_name(rung), codec=BytesCodec()
        ).send(body, envelope=Envelope(type=SCHEDULED_TYPE, headers=headers))

    async def _deliver(self, body: bytes, headers: dict[str, Any]) -> None:
        """Sends the message on to where it was always going."""
        exchange = _text(headers.get(HEADER_SCHEDULE_EXCHANGE))
        routing_key = _text(headers.get(HEADER_SCHEDULE_ROUTING_KEY))
        content_type = _text(headers.get(HEADER_SCHEDULE_CONTENT_TYPE)) or JSON_CONTENT_TYPE

        self._delivered += 1
        # The scheduler's own headers are not passed on: they are bookkeeping,
        # and a consumer that started depending on them would be depending on
        # how a message got to it.
        await self._connection.publisher(
            exchange, routing_key, codec=_Verbatim(content_type)
        ).send(body, envelope=Envelope(type=SCHEDULED_TYPE))

    async def close(self) -> None:
        """Stops consuming the control queue.

        It declares nothing and deletes nothing on the way out. The rungs are
        shared with every other scheduler on the broker, and a message already
        in one is delivered by whoever is consuming ``acemq.schedule.due`` when
        it expires — which may well be another process.
        """
        if self._consumer is not None:
            await self._consumer.close()
            self._consumer = None

    async def __aenter__(self) -> Scheduler:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def __str__(self) -> str:
        return (
            f"Scheduler(rungs={[_describe(rung) for rung in SCHEDULE_RUNGS]}, "
            f"scheduled={self._scheduled}, delivered={self._delivered}, hops={self._hops})"
        )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _millis(when: datetime) -> int:
    return int(when.timestamp() * 1000)


def _from_millis(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _text(value: Any) -> str:
    """A header as a string.

    RabbitMQ's Java client sends strings as ``LongString`` and some clients send
    them as bytes, so this decodes rather than assuming — the same reading the
    envelope does, and it has to be, because the header may well have been
    written by one of those clients.
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _number(value: Any) -> int:
    """A header as an integer.

    :raises AceMQError: when it is not one. A due time that cannot be read is
        not a message to guess about: guessing early delivers it now and
        guessing late holds it forever.
    """
    if isinstance(value, bool):
        raise AceMQError(f"acemq: {HEADER_SCHEDULE_DUE_AT} is not a number: {value!r}")
    if isinstance(value, int):
        return value
    try:
        return int(_text(value))
    except (TypeError, ValueError) as unreadable:
        raise AceMQError(
            f"acemq: {HEADER_SCHEDULE_DUE_AT} is not a number: {value!r}"
        ) from unreadable
