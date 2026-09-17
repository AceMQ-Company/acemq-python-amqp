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

"""A queue that keeps what it has already handed out.

A queue forgets a message when it is acknowledged, so exactly one consumer ever
sees it and nobody can look at it again. A stream keeps everything until its
retention policy discards it, so several consumers read the same stream
independently and a new one can start from the beginning.

That changes what an acknowledgement means, and it is the thing to understand
before using one. Acknowledging does not remove the message; it advances *this*
consumer's position, and the message stays on the stream for somebody else to
read tomorrow.

What a failure means changes with it. A copy of a refused message can still be
put somewhere a person will look — :func:`~acemq_amqp.reject` puts one in
``{stream}.dlq`` and :func:`~acemq_amqp.park` one in ``{stream}.parked``, both
ordinary queues beside the stream — because neither of those touches the log.

**A retry cannot be.** Retrying means republishing, and republishing onto a
stream *appends*: a second copy of the message that every other consumer of that
log reads as a new one, for as long as the retention policy keeps it. So a
handler reading a stream is not allowed to ask for one. :func:`read_stream`
refuses :func:`~acemq_amqp.retry` with a :class:`StreamRetryError` naming the two
honest alternatives — park the message, or accept it and checkpoint past the
failure — and it runs its consumer on :func:`~acemq_amqp.no_retry` whatever the
connection's own policy says, so an exception escaping a handler cannot append a
copy either. Nothing this library does adds to a stream that a handler did not
publish itself.

:meth:`Connection.message_count <acemq_amqp.Connection.message_count>` says
nothing useful about a stream either: the broker reports zero, because a message
on a stream is not waiting for anybody. Depth is a property of a queue, and a
stream is not one. How far behind a reader is comes from the ``x-stream-offset``
header each delivery carries.
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from ..ack import Ack, Action, FatalError
from ..codec import Codec
from ..connection import AsyncHandler, Connection, Consumer, Handler, Message
from ..errors import AceMQError
from ..retry import no_retry
from ..topology import QUEUE_TYPE_ARG as _QUEUE_TYPE_ARG
from ..topology import Topology

log = logging.getLogger("acemq")

#: Marks a queue as a stream at declaration. It cannot be changed afterwards.
#: It is the same argument that makes an ordinary queue quorum rather than
#: classic, so it is defined once, in :mod:`acemq_amqp.topology`, and named
#: again here because a stream is what a reader of this module came for.
QUEUE_TYPE_ARG = _QUEUE_TYPE_ARG

#: Where a stream consumer starts, given when it subscribes.
STREAM_OFFSET_ARG = "x-stream-offset"

#: How much of a stream to keep, in time and in bytes.
MAX_AGE_ARG = "x-max-age"
MAX_LENGTH_BYTES_ARG = "x-max-length-bytes"
SEGMENT_BYTES_ARG = "x-stream-max-segment-size-bytes"

#: RabbitMQ refuses a stream consumer without a prefetch, and the message it
#: gives back does not mention streams. Ten is enough to keep one busy and small
#: enough that a slow handler is not holding a batch nobody else can have.
DEFAULT_STREAM_PREFETCH = 10


class StreamRetryError(AceMQError, FatalError):
    """A stream handler asked for a retry, which a stream cannot give it.

    Retrying means republishing, and republishing onto a stream appends: a
    second copy of the message on the log, which every other consumer of that
    stream reads as a new one and which stays there for the retention. A handler
    that failed three times would leave three copies behind, and the copies
    outlive the outage that caused them.

    There is no third outcome that quietly works, so this names the two that do.
    **Park it** — :func:`~acemq_amqp.park` puts a copy in ``{stream}.parked``,
    which is a queue beside the stream rather than part of it, and leaves the log
    untouched. **Or checkpoint and move on** — :func:`~acemq_amqp.accept`
    advances this consumer's position past the message and the failure becomes
    the handler's to record, which is the outcome most projections want: the
    message is still on the log and can be read again from an earlier offset
    once whatever broke is fixed.

    Both an :class:`~acemq_amqp.AceMQError` and a
    :class:`~acemq_amqp.FatalError`, because it is both: something this library
    refused to do, and a request no number of further attempts would make
    reasonable. The second is what makes the delivery end up in ``{stream}.dlq``
    with this sentence on it rather than going round a retry ladder that would
    do the very thing being refused.
    """


def _refusing_retries(stream: str, handler: Handler) -> AsyncHandler:
    """A handler that cannot ask for a retry, whatever the one inside it says.

    The constraint sits here, on the handler, rather than on a stream-specific
    consumer class, and that is a deliberate choice worth stating. Java needs a
    separate ``StreamConsumer`` because its ``MessageConsumer`` exposes the retry
    ladder as configuration — a type that offered a dead-letter policy for a
    stream would be a type where half the settings are wrong. Python's
    :class:`~acemq_amqp.Consumer` exposes no such thing: it is ``queue``,
    ``running``, ``in_flight`` and ``close``, every one of which means exactly
    what it says on a stream. The three-outcome settle path comes from the
    :class:`~acemq_amqp.Ack` a handler returns, so the refusal belongs where the
    ``Ack`` is read, and a second consumer class would be a copy of the first
    with nothing removed from it.
    """

    async def read(message: Message) -> Ack:
        returned = handler(message)
        decision: Ack = await returned if inspect.isawaitable(returned) else returned
        if isinstance(decision, Ack) and decision.action is Action.RETRY:
            # Logged here as well as carried on the dead letter, because the two
            # reach different people. Whoever drains ``{stream}.dlq`` finds the
            # reason on the message; whoever wrote the handler is reading logs.
            log.error(
                "acemq: the handler for stream %s asked to retry %s, which a stream "
                "cannot do. Return park(...) or accept() instead.",
                stream,
                message.envelope.id,
            )
            raise StreamRetryError(
                f"acemq: a handler reading the stream {stream!r} asked to retry "
                f"message {message.envelope.id}. A stream never removes a message, "
                "so retrying one means appending a second copy of it to the log for "
                "every other consumer to read as well. Park it instead — park(...) "
                f"puts a copy in {stream}.parked and leaves the log alone — or "
                "accept() to checkpoint past it and record the failure yourself."
            )
        return decision

    return read


@dataclass(frozen=True, slots=True)
class StreamOffset:
    """Where a stream consumer starts reading.

    Built by the functions below rather than directly: the broker takes a
    different kind of value for each, and a constructor that accepted any of
    them would be a constructor nobody could read.
    """

    kind: str
    value: Any = None

    def to_arg(self) -> Any:
        """What the broker is told."""
        return self.kind if self.value is None else self.value

    def __str__(self) -> str:
        return self.kind if self.value is None else f"{self.kind}({self.value})"


def from_first() -> StreamOffset:
    """Start at the oldest message the stream still holds.

    For building a projection from nothing, or a new consumer that needs the
    history. Note *still holds*: a stream has a retention policy, so the oldest
    message it has is not necessarily the first one ever written to it.
    """
    return StreamOffset("first")


def from_next() -> StreamOffset:
    """Start at the next message published, ignoring everything already there.

    The default, and what most consumers want.
    """
    return StreamOffset("next")


def from_last() -> StreamOffset:
    """Start at the last chunk the stream holds.

    Roughly "the recent past" rather than an exact number of messages: a stream
    is stored in segments and this starts at the beginning of the last one.
    """
    return StreamOffset("last")


def from_offset(offset: int) -> StreamOffset:
    """Start at an exact position, which is what a consumer that records its own
    progress uses to carry on where it left off."""
    return StreamOffset("offset", offset)


def from_timestamp(at: datetime) -> StreamOffset:
    """Start at the first message published at or after a time."""
    return StreamOffset("timestamp", at)


@dataclass(frozen=True, slots=True)
class StreamRetention:
    """How much of a stream to keep.

    Unbounded by default, which for a stream means until the disk is full. Set
    at least one of these on anything that will run for longer than an
    afternoon.

    :param max_age: discard messages older than this
    :param max_bytes: discard the oldest once the stream exceeds this
    :param segment_bytes: how large each file on disk gets. Retention happens a
        whole segment at a time, so a very large segment makes retention coarse
    """

    max_age: timedelta | None = None
    max_bytes: int | None = None
    segment_bytes: int | None = None

    def to_args(self) -> dict[str, Any]:
        """The declaration arguments this retention means."""
        args: dict[str, Any] = {}
        if self.max_age is not None:
            args[MAX_AGE_ARG] = _duration(self.max_age)
        if self.max_bytes is not None:
            args[MAX_LENGTH_BYTES_ARG] = self.max_bytes
        if self.segment_bytes is not None:
            args[SEGMENT_BYTES_ARG] = self.segment_bytes
        return args


def stream(name: str, retention: StreamRetention | None = None) -> Topology:
    """A topology declaring one stream.

    A stream is durable and can be neither exclusive nor auto-deleting. Those
    are set here rather than left to the caller, because getting one of them
    wrong is answered by the broker with a message that never mentions streams.

    :param name: what to call it
    :param retention: how much to keep
    :returns: a topology, so a stream can be declared alongside everything else
        rather than through a call of its own
    """
    args = {QUEUE_TYPE_ARG: "stream"}
    args.update((retention or StreamRetention()).to_args())
    return Topology().queue(name, durable=True, args=args)


async def declare_stream(
    connection: Connection, name: str, retention: StreamRetention | None = None
) -> None:
    """Declares a stream on the broker::

        await declare_stream(
            mq, "events", StreamRetention(max_age=timedelta(days=7))
        )

    :param connection: where to declare it
    :param name: what to call it
    :param retention: how much to keep
    """
    await connection.declare(stream(name, retention))


async def read_stream(
    connection: Connection,
    name: str,
    handler: Handler,
    *,
    offset: StreamOffset | None = None,
    prefetch: int = DEFAULT_STREAM_PREFETCH,
    consumer_name: str = "",
    codec: Codec | None = None,
    concurrency: int = 1,
    declare: bool = True,
) -> Consumer:
    """Reads a stream from a chosen position::

        consumer = await read_stream(mq, "events", project, offset=from_first())

    **Nothing this consumer does appends to the stream.** There is no retry
    policy to pass and there is no policy it can inherit: it runs on
    :func:`~acemq_amqp.no_retry` whatever the connection's own default is, and a
    handler that returns :func:`~acemq_amqp.retry` is refused with a
    :class:`StreamRetryError`. Both close the same hole from the two directions
    a retry could arrive from — a handler asking for one, and an exception
    escaping a handler onto a connection that has a retry policy — because
    retrying on a stream means republishing to it, which appends a second copy
    for every other consumer of that log to read as well.

    What a failing handler has instead is two outcomes, and they are enough.
    :func:`~acemq_amqp.park` puts a copy in ``{name}.parked``, a queue beside the
    stream rather than part of it, and the log is untouched.
    :func:`~acemq_amqp.accept` checkpoints past the message and leaves the
    failure for the handler to record — which is what most projections want,
    because the message is still on the log and can be read again from an earlier
    offset once whatever broke is fixed. :func:`~acemq_amqp.reject` is the third
    and it works too, putting a copy in ``{name}.dlq``; it is not offered as one
    of the two because "this message is bad" is rarely what a stream failure is.

    An ordinary :class:`~acemq_amqp.Consumer` comes back rather than a
    stream-specific class. See :func:`_refusing_retries` for why: every method on
    it means what it says on a stream, so there is nothing a second type would
    take away.

    :param connection: where the stream is
    :param name: which stream
    :param handler: what to do with each message. It may not ask for a retry
    :param offset: where to start, the next message by default
    :param prefetch: how many messages to hold. It cannot be zero: RabbitMQ
        refuses a stream consumer without one
    :param consumer_name: what to call this consumer to the broker, which is
        what makes server-side offset tracking possible
    :param codec: a codec other than the connection's
    :param concurrency: how many messages to work on at once. One by default,
        because a stream's order is usually why it is a stream
    :param declare: declare ``{name}.dlq`` and ``{name}.parked`` before
        subscribing. On by default; see :meth:`acemq_amqp.Connection.consume`.
        No retry rungs are declared, because nothing here retries
    :returns: the running consumer
    """
    if prefetch < 1:
        raise ValueError(
            f"acemq: a stream consumer needs a prefetch of at least 1, got {prefetch}"
        )

    return await connection.consume(
        name,
        _refusing_retries(name, handler),
        codec=codec,
        # Said rather than inherited. A connection carrying a retry policy for
        # its queues would otherwise turn every exception out of a stream handler
        # into a copy on the log, which is the one thing a stream consumer must
        # never do — and it would do it silently, because from outside a stream
        # that is growing looks like a stream that is busy.
        retry=no_retry(),
        prefetch=prefetch,
        concurrency=concurrency,
        tag=consumer_name,
        args={STREAM_OFFSET_ARG: (offset or from_next()).to_arg()},
        declare=declare,
    )


def _duration(length: timedelta) -> str:
    """A duration as RabbitMQ wants it: a number and a unit suffix.

    It refuses a plain number here, and the units are its own — ``D`` for days
    but lowercase for the rest.
    """
    seconds = int(length.total_seconds())
    if seconds % 86400 == 0:
        return f"{seconds // 86400}D"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"
