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

"""What a handler says about a message, and what the consumer then did.

Two different statements, and the difference is the point of
:class:`Settlement`. A handler saying :func:`retry` is a request; whether there
is an attempt left to grant it is the retry policy's answer, and a message that
asked to be retried once too often is dead-lettered. Anything watching a
delivery — the tracing adapter is the one that matters — needs the answer rather
than the request, which is why the consumer reports it separately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

#: The consumer acknowledged the message; the handler was happy with it.
OUTCOME_ACKED = "acked"

#: The message goes round again, after :attr:`Settlement.delay`.
OUTCOME_RETRIED = "retried"

#: The handler refused it outright. It is dead-lettered, and the word says the
#: decision was deliberate rather than the end of a losing streak.
OUTCOME_REJECTED = "rejected"

#: It ran out of attempts, aged out, or was unprocessable.
OUTCOME_DEAD_LETTERED = "dead_lettered"

#: It went to ``{queue}.parked`` rather than to the dead letters, because
#: nothing could read it. The same word the ``parked`` outcome on
#: ``acemq.messages.dead.lettered.total`` uses, and the word the engine already
#: used for a body that would not decode.
OUTCOME_PARKED = "parked"


class Action(Enum):
    """The four things that can be done with a delivered message."""

    ACCEPT = "accept"
    RETRY = "retry"
    REJECT = "reject"
    PARK = "park"


@dataclass(frozen=True, slots=True)
class Ack:
    """A handler's decision: it worked, try it again, never again, or nobody
    can read it.

    Returned rather than performed, so a handler that forgets to decide is a
    handler that returns ``None`` and is caught immediately, rather than one
    whose message sits unacknowledged until the connection drops and then comes
    back — usually to the same handler, with the same outcome.
    """

    action: Action
    error: BaseException | None = None

    def __str__(self) -> str:
        return self.action.value


@dataclass(frozen=True, slots=True)
class Settlement:
    """What the consumer did with a delivery, once it had decided.

    Reported before it is carried out rather than after, because one of them
    takes time: a consumer-side retry sleeps out the backoff before the message
    is republished, and a listener told afterwards would be describing the
    delivery minutes later, on a span that had been open the whole while.

    :param outcome: one of :data:`OUTCOME_ACKED`, :data:`OUTCOME_RETRIED`,
        :data:`OUTCOME_REJECTED`, :data:`OUTCOME_DEAD_LETTERED` and
        :data:`OUTCOME_PARKED` — the same words the other libraries put on a
        span
    :param reason: why it was set aside, when it was
    :param delay: how long until the next attempt, when there is one
    """

    outcome: str
    reason: str | None = None
    delay: timedelta | None = None

    @property
    def dead_lettered(self) -> bool:
        """Whether the message went to the dead-letter queue.

        True for an outright rejection as well as for an exhausted one: both
        end up in the same queue, and the difference between the two words is
        *who decided*, which :attr:`outcome` records and this does not.

        False for :data:`OUTCOME_PARKED`, and that is the whole point of the
        parked queue: a message nobody could read is a different problem with a
        different answer from one that failed five times, and whoever drains
        the dead letters should not have to sort the two apart by hand.
        """
        return self.outcome in (OUTCOME_REJECTED, OUTCOME_DEAD_LETTERED)

    @property
    def parked(self) -> bool:
        """Whether the message went to the parked queue."""
        return self.outcome == OUTCOME_PARKED

    def __str__(self) -> str:
        return self.outcome


def accept() -> Ack:
    """Confirms the message. It will not be delivered again."""
    return Ack(Action.ACCEPT)


def retry(error: BaseException | None = None) -> Ack:
    """Returns the message to be tried again.

    The retry policy decides whether there is another attempt left; when there
    is not, the message is dead-lettered with ``error`` as the reason. A
    :class:`Fatal` error skips the remaining attempts, because they would all
    fail the same way.
    """
    return Ack(Action.RETRY, error)


def reject(error: BaseException | None = None) -> Ack:
    """Dead-letters the message without trying again.

    For when the message itself is the problem — a field that cannot be missing
    is missing, a reference points at nothing — rather than when the world is
    temporarily unhelpful.
    """
    return Ack(Action.REJECT, error)


def park(error: BaseException | None = None) -> Ack:
    """Sets the message aside on ``{queue}.parked`` without trying again.

    For when the message is not readable at all: a body in a format nothing
    here understands, a version this service was never taught. The engine
    already parks a body its codec cannot decode; this is how a handler that
    worked out the same thing one layer further in says so.

    :func:`reject` is the neighbouring word and the difference is worth
    keeping. A rejected message is a *bad request* — understood, and refused —
    and it belongs with the other dead letters. A parked one was never
    understood, and mixing the two means whoever drains the dead-letter queue
    has to sort them by hand to find the producer that is emitting rubbish.
    """
    return Ack(Action.PARK, error)


class FatalError(Exception):
    """An error no number of retries will fix.

    Wrapping a cause in this says "stop now" without the handler having to know
    how many attempts remain, which is knowledge handlers should not need.

    The other languages call this ``Fatal``; the name is Python's because a
    class name is ergonomics rather than wire contract, and Python expects an
    exception to say that it is one.
    """
