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
consumer's position. And rejecting does not dead-letter, because there is
nothing to remove the message from. A message a handler cannot deal with has to
be dealt with by the handler — logged, copied elsewhere, counted — and the
stream moves on regardless. Nothing is lost, and nothing is retried for you.

For the same reason
:meth:`Connection.message_count <acemq_amqp.Connection.message_count>` says
nothing useful about a stream: the broker reports zero, because a message on a
stream is not waiting for anybody. Depth is a property of a queue, and a stream
is not one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from ..codec import Codec
from ..connection import Connection, Consumer, Handler
from ..topology import Topology

#: Marks a queue as a stream at declaration. It cannot be changed afterwards.
QUEUE_TYPE_ARG = "x-queue-type"

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
) -> Consumer:
    """Reads a stream from a chosen position::

        consumer = await read_stream(mq, "events", project, offset=from_first())

    The retry policy is deliberately not a parameter. Retrying on a stream means
    republishing to it, which appends a second copy of the message for every
    other consumer to read as well — so a stream's failures belong to its
    handler, and pretending otherwise would put the surprise somewhere it cannot
    be seen.

    :param connection: where the stream is
    :param name: which stream
    :param handler: what to do with each message
    :param offset: where to start, the next message by default
    :param prefetch: how many messages to hold. It cannot be zero: RabbitMQ
        refuses a stream consumer without one
    :param consumer_name: what to call this consumer to the broker, which is
        what makes server-side offset tracking possible
    :param codec: a codec other than the connection's
    :param concurrency: how many messages to work on at once. One by default,
        because a stream's order is usually why it is a stream
    :returns: the running consumer
    """
    if prefetch < 1:
        raise ValueError(
            f"acemq: a stream consumer needs a prefetch of at least 1, got {prefetch}"
        )

    return await connection.consume(
        name,
        handler,
        codec=codec,
        prefetch=prefetch,
        concurrency=concurrency,
        tag=consumer_name,
        args={STREAM_OFFSET_ARG: (offset or from_next()).to_arg()},
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
