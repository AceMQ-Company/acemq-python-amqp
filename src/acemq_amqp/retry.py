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

"""When to try again, and when to stop.

The schedule arithmetic here is part of the cross-language contract rather than
a local choice: the same policy must produce the same delays in Java, Go, .NET
and Python, because the same message can be retried by a consumer written in
any of them. The jitter is the one exception — it is random by definition —
so :meth:`RetryPolicy.schedule` exposes the delays *without* it, which is what
to read when deciding whether a policy is the one you meant.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import timedelta

ZERO = timedelta(0)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times to try, and how long to wait between attempts.

    :param max_attempts: total deliveries including the first. 1 means no retry
    :param initial_delay: the wait before the second attempt
    :param multiplier: what the delay is multiplied by each time
    :param max_delay: the ceiling, or zero for none
    :param jitter_factor: how far a delay may move either side, 0 to 1
    :param max_message_age: give up on anything older, or zero for never
    """

    max_attempts: int = 1
    initial_delay: timedelta = ZERO
    multiplier: float = 2.0
    max_delay: timedelta = ZERO
    jitter_factor: float = 0.0
    max_message_age: timedelta = ZERO

    def give_up_after(self, age: timedelta) -> RetryPolicy:
        """A copy that abandons a message older than ``age``.

        The honest limit when a queue has been paused: attempts say nothing
        about how long a message has been waiting, and a message four days old
        is usually one nobody wants delivered now.
        """
        return replace(self, max_message_age=age)

    def with_jitter(self, factor: float) -> RetryPolicy:
        """A copy using a different jitter factor, between 0 and 1."""
        return replace(self, jitter_factor=factor)

    def next_delay(self, attempt: int, message_age: timedelta = ZERO) -> timedelta | None:
        """How long to wait before the next attempt, or ``None`` to give up.

        :param attempt: the attempt that has just failed, starting at 1
        :param message_age: how old the message is
        :returns: the delay, or ``None`` when there should be no further attempt
        """
        if attempt >= self.max_attempts:
            return None
        if self.max_message_age > ZERO and message_age >= self.max_message_age:
            return None

        delay = self.initial_delay
        for _ in range(1, attempt):
            delay = delay * self.multiplier
            if self.max_delay > ZERO and delay > self.max_delay:
                delay = self.max_delay
                break
        if self.max_delay > ZERO and delay > self.max_delay:
            delay = self.max_delay

        if self.jitter_factor > 0 and delay > ZERO:
            # Both directions, so a fleet of consumers that failed together
            # does not come back together. One-sided jitter only ever delays,
            # which turns a thundering herd into a slower thundering herd.
            factor = 1 + ((random.random() * 2 - 1) * self.jitter_factor)
            delay = delay * factor

        return max(delay, ZERO)

    def schedule(self) -> list[timedelta]:
        """The delays this policy would use, without jitter.

        What to look at when deciding whether a policy is the one you meant:
        ``ExponentialRetry(5, 1s, 1m).schedule()`` is ``[1s, 2s, 4s, 8s]``, and
        four numbers are easier to argue with than three parameters.
        """
        delays: list[timedelta] = []
        delay = self.initial_delay
        for _ in range(1, max(self.max_attempts, 0)):
            capped = delay
            if self.max_delay > ZERO and capped > self.max_delay:
                capped = self.max_delay
            delays.append(capped)
            delay = delay * self.multiplier
        return delays


def no_retry() -> RetryPolicy:
    """One delivery, no second chance."""
    return RetryPolicy(max_attempts=1)


def exponential_retry(
    max_attempts: int,
    initial_delay: timedelta,
    max_delay: timedelta = ZERO,
) -> RetryPolicy:
    """Doubling delays with 20% jitter, which is the sane default.

    :param max_attempts: total deliveries including the first
    :param initial_delay: the wait before the second attempt
    :param max_delay: the ceiling, or zero for none
    """
    return RetryPolicy(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        multiplier=2.0,
        max_delay=max_delay,
        jitter_factor=0.2,
    )


def fixed_retry(max_attempts: int, delay: timedelta) -> RetryPolicy:
    """The same wait every time, with no jitter."""
    return RetryPolicy(max_attempts=max_attempts, initial_delay=delay, multiplier=1.0)
