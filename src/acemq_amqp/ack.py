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

"""What a handler says about a message."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Action(Enum):
    """The three things that can be done with a delivered message."""

    ACCEPT = "accept"
    RETRY = "retry"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class Ack:
    """A handler's decision: it worked, try it again, or never again.

    Returned rather than performed, so a handler that forgets to decide is a
    handler that returns ``None`` and is caught immediately, rather than one
    whose message sits unacknowledged until the connection drops and then comes
    back — usually to the same handler, with the same outcome.
    """

    action: Action
    error: BaseException | None = None

    def __str__(self) -> str:
        return self.action.value


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


class FatalError(Exception):
    """An error no number of retries will fix.

    Wrapping a cause in this says "stop now" without the handler having to know
    how many attempts remain, which is knowledge handlers should not need.

    The other languages call this ``Fatal``; the name is Python's because a
    class name is ergonomics rather than wire contract, and Python expects an
    exception to say that it is one.
    """
