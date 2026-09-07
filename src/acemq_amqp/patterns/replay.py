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

"""Putting dead letters back, when the thing that killed them is fixed.

This is the pattern somebody actually needs at three in the morning: a
dead-letter queue has two thousand messages in it, the bug is fixed, and they
have to go back through — but not all of them, not silently, and not in a way
that cannot be stopped half way.

So a replay takes a filter, a limit and a deadline, and reports what it did and
why it stopped. "Moved 500" means something quite different when the limit was
500, which is why the reason comes back with the count.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, TypeAlias

from ..connection import Connection
from ..envelope import Envelope
from ..errors import AceMQError
from ..transport import Delivery, Outbound

log = logging.getLogger("acemq")

#: The queue a message was replayed out of.
HEADER_REPLAYED_FROM = "acemq-replayed-from"

#: When it was replayed, as RFC 3339.
HEADER_REPLAYED_AT = "acemq-replayed-at"

#: How many times it has been replayed, from 1.
HEADER_REPLAY_COUNT = "acemq-replay-count"

#: Decides whether a message goes back. Returning ``False`` leaves it where it
#: is, which is what makes a replay something that can be done in stages.
ReplayFilter: TypeAlias = Callable[[Envelope, bytes], bool]


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """What a replay did, and why it stopped.

    :param moved: how many messages were republished
    :param skipped: how many the filter declined, and so left where they were
    :param reason: ``drained``, ``limit`` or ``deadline``
    """

    moved: int
    skipped: int
    reason: str

    def __str__(self) -> str:
        return f"moved {self.moved}, skipped {self.skipped} ({self.reason})"


class ReplayError(AceMQError):
    """A replay stopped because something failed.

    It carries the :class:`ReplayResult` it had reached, because "it failed" and
    "it failed after moving four hundred" are different things to be told at
    three in the morning.
    """

    def __init__(self, result: ReplayResult, what: str) -> None:
        self.result = result
        super().__init__(f"acemq: {what} (after {result})")


async def replay(
    connection: Connection,
    queue: str,
    *,
    exchange: str = "",
    routing_key: str | None = None,
    limit: int = 0,
    deadline: timedelta | None = None,
    only: ReplayFilter | None = None,
    restart: bool = True,
) -> ReplayResult:
    """Moves messages from a queue back onto an exchange::

        result = await replay(
            mq,
            dead_letter_queue("orders.new"),
            limit=500,
            only=lambda envelope, body: "timeout" in envelope.error,
        )
        print(result)   # moved 137, skipped 363 (drained)

    Each message is stamped with :data:`HEADER_REPLAYED_FROM`,
    :data:`HEADER_REPLAYED_AT` and :data:`HEADER_REPLAY_COUNT`, so a consumer
    that needs to treat a replayed message differently can, and one that does
    not is unaffected.

    :param connection: where to read from and publish to
    :param queue: where the messages are now, usually a dead-letter queue
    :param exchange: where they go back to, empty to publish to a queue by name
    :param routing_key: what to publish them under. ``None`` keeps each
        message's own, so a message goes back where it came from rather than
        everywhere
    :param limit: stop after this many. Zero means no limit, which against a
        queue something is still writing to may mean it does not stop at all —
        prefer a limit
    :param deadline: stop after this long. ``None`` means no deadline
    :param only: which messages to move. ``None`` moves all of them
    :param restart: give each message a fresh set of attempts, clearing the
        counter and the error that dead-lettered it. On by default because it is
        what a replay is for: a message that arrives back on attempt five of a
        five-attempt policy is dead-lettered again before a handler sees it, and
        the operator who has just fixed the bug has moved two thousand messages
        from one queue to the same queue. Turn it off to put back exactly what
        was there — for an audit, or for a queue read by something that counts
        attempts itself
    :returns: what it did and why it stopped
    :raises ReplayError: when reading or republishing failed. Whatever had
        already been moved has been moved; the rest is still on the queue
    """
    if not queue:
        raise ValueError("acemq: a replay needs a queue to read from")

    clock = asyncio.get_running_loop()
    expires = None if deadline is None else clock.time() + deadline.total_seconds()
    moved = 0
    skipped = 0

    # Messages the filter declines are held unsettled until the pass is over
    # rather than returned one at a time. Returning one immediately does not
    # work: the broker puts it back where it was, at the head of the queue, so
    # the next pull hands over the same message and nothing behind it is ever
    # looked at. Holding them takes them out of the way for the length of the
    # pass, and because the broker is the one holding them, a crash returns them
    # rather than losing them.
    declined: list[Delivery] = []

    try:
        while True:
            if limit > 0 and moved >= limit:
                return ReplayResult(moved, skipped, "limit")
            if expires is not None and clock.time() >= expires:
                return ReplayResult(moved, skipped, "deadline")

            try:
                message = await connection.pull(queue)
            except Exception as failure:
                raise ReplayError(
                    ReplayResult(moved, skipped, "failed"),
                    f"cannot read {queue!r} to replay it: {failure}",
                ) from failure

            if message is None:
                # Everything available has been offered. What is left is what
                # this pass declined, and it goes back as the pass ends.
                return ReplayResult(moved, skipped, "drained")

            envelope = Envelope.from_headers(message.headers, message.routing_key)
            if only is not None and not only(envelope, message.body):
                skipped += 1
                declined.append(message)
                continue

            try:
                await _move(
                    connection, message, envelope, queue, exchange, routing_key, restart
                )
            except Exception as failure:
                raise ReplayError(
                    ReplayResult(moved, skipped, "failed"),
                    f"cannot republish a message from {queue!r}: {failure}",
                ) from failure
            moved += 1
    finally:
        for held in declined:
            # Nothing to report if this fails: the pass is over, and a message
            # that cannot be returned is one the broker returns itself when this
            # connection goes.
            await _return_quietly(held)


async def _move(
    connection: Connection,
    message: Delivery,
    envelope: Envelope,
    queue: str,
    exchange: str,
    routing_key: str | None,
    restart: bool,
) -> None:
    """Republishes one message and then acknowledges the original.

    That order, and not the other one. Acknowledging first would lose the
    message whenever the publish failed; this way a failure in the gap replays
    it twice, which is the right way round for a queue somebody is putting back
    by hand.
    """
    going_back = envelope.with_(attempt=1, error="") if restart else envelope
    key = message.routing_key if routing_key is None else routing_key

    headers: dict[str, Any] = dict(going_back.to_headers(routing_key=key))
    headers[HEADER_REPLAYED_FROM] = queue
    headers[HEADER_REPLAYED_AT] = datetime.now(timezone.utc).isoformat()
    headers[HEADER_REPLAY_COUNT] = _replays_so_far(envelope) + 1

    try:
        await connection.publish_raw(
            exchange,
            key,
            Outbound(
                body=message.body,
                content_type=message.content_type or "",
                message_id=going_back.id,
                headers=headers,
                persistent=True,
            ),
        )
    except Exception:
        # Returned rather than dropped, and the replay stops. A replay that
        # loses messages is worse than one that stops early.
        await _return_quietly(message)
        raise

    await message.ack()


async def _return_quietly(message: Delivery) -> None:
    """Puts a message back without letting that failure hide the first one.

    Logged rather than raised: by the time this is called the replay is already
    stopping, and the message is one the broker returns itself when this
    connection goes.
    """
    try:
        await message.nack(True)
    except Exception:
        log.warning("acemq: could not return a message to the queue it was replayed from")


def _replays_so_far(envelope: Envelope) -> int:
    """How many times this message has been replayed already.

    Read leniently, because the header may have been written by another
    language's client or by a broker that renders integers as strings.
    """
    written = envelope.headers.get(HEADER_REPLAY_COUNT)
    if isinstance(written, bool):
        return 0
    if isinstance(written, int):
        return written
    try:
        return int(str(written))
    except (TypeError, ValueError):
        return 0
