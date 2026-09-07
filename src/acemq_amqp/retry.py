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

"""When to try again, where to wait, and when to stop.

The schedule arithmetic here is part of the cross-language contract rather than
a local choice: the same policy must produce the same delays in Java, Go, .NET
and Python, because the same message can be retried by a consumer written in
any of them. The jitter is the one exception — it is random by definition —
so :meth:`RetryPolicy.schedule` exposes the delays *without* it, which is what
to read when deciding whether a policy is the one you meant.

Where the waiting happens is the other half. A short wait is spent in the
consumer, holding the delivery; a long one is spent in the broker, in a queue
whose message TTL is the wait. The threshold between them is
:data:`DEFAULT_BROKER_WAIT_THRESHOLD` unless a policy says otherwise, and the
reason for having a threshold at all rather than one rule is in
:class:`RetryPolicy`.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, replace
from datetime import timedelta

ZERO = timedelta(0)

#: Waits this long or longer are spent in the broker rather than in the
#: consumer. Thirty seconds is roughly where the two costs cross: below it, the
#: seconds a restart loses are only seconds and a held prefetch slot is cheap;
#: above it, a consumer that restarts mid-wait loses the wait entirely, because
#: the broker redelivers the unacknowledged message at once and a five-minute
#: backoff becomes instant.
DEFAULT_BROKER_WAIT_THRESHOLD = timedelta(seconds=30)


@dataclass(frozen=True, slots=True)
class Wait:
    """How long before the next attempt, and where the message spends it.

    Two answers rather than one because they cannot be worked out separately:
    jitter applies only to a wait spent in the consumer, so a caller given a
    delay alone could not tell whether it had already been moved — and a
    jittered delay does not name a rung queue.

    :param delay: how long the message waits
    :param in_broker: whether it waits on a rung queue rather than here
    """

    delay: timedelta
    in_broker: bool


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times to try, how long to wait, and where.

    Waits below ``broker_wait_threshold`` are spent in the consumer, which holds
    the delivery and one prefetch slot for the duration. Waits at or above it
    are spent in a ``{queue}.retry.{delay}`` queue whose ``x-message-ttl`` is the
    wait and whose dead-letter target is the source queue, so the broker returns
    the message when the time is up and a consumer restart cannot shorten it.

    Splitting at a threshold rather than picking one of the two takes the
    durability where it is worth its complexity and leaves the simplicity where
    it is not. A queue's TTL is fixed at declaration and so cannot express a
    jittered delay, which is why jitter applies below the threshold only; above
    it the spread comes free, because each message's TTL starts when it enters
    the rung, so a fleet that failed over ten seconds is released over ten
    seconds.

    :param max_attempts: total deliveries including the first. 1 means no retry
    :param initial_delay: the wait before the second attempt
    :param multiplier: what the delay is multiplied by each time
    :param max_delay: the ceiling, or zero for none
    :param jitter_factor: how far a delay may move either side, 0 to 1
    :param max_message_age: give up on anything older, or zero for never
    :param broker_wait_threshold: waits this long or longer are spent in the
        broker, or zero to spend every wait in the consumer
    """

    max_attempts: int = 1
    initial_delay: timedelta = ZERO
    multiplier: float = 2.0
    max_delay: timedelta = ZERO
    jitter_factor: float = 0.0
    max_message_age: timedelta = ZERO
    broker_wait_threshold: timedelta = DEFAULT_BROKER_WAIT_THRESHOLD

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

    def wait_in_broker_from(self, threshold: timedelta) -> RetryPolicy:
        """A copy that moves the line between waiting here and waiting there.

        Zero is the way out: with no threshold nothing is long enough to reach
        the broker, so every wait is spent in the consumer and no rung queue is
        needed. That is the right setting for a service whose broker it may not
        declare queues on, and the wrong one for a policy with delays measured
        in minutes.

        :param threshold: waits this long or longer go to a rung queue, or zero
            for none of them
        :returns: a new policy
        """
        return replace(self, broker_wait_threshold=threshold)

    def waits_in_broker(self, delay: timedelta) -> bool:
        """Whether a wait of this length belongs on a rung queue.

        :param delay: an unjittered delay, as :meth:`schedule` reports them
        :returns: whether the broker rather than the consumer should hold it
        """
        return (
            self.broker_wait_threshold > ZERO
            and delay > ZERO
            and delay >= self.broker_wait_threshold
        )

    def broker_rungs(self) -> list[timedelta]:
        """The delays this policy needs a rung queue for, longest last.

        Exactly the entries of :meth:`schedule` that are at or above the
        threshold, with repeats removed — a fixed policy that waits a minute
        three times needs one queue, not three. It is a finite list because the
        schedule is, which is what makes the queues declarable up front rather
        than conjured by a consumer at the moment it first fails.

        :returns: one delay per queue, in the order the schedule reaches them
        """
        return list(dict.fromkeys(d for d in self.schedule() if self.waits_in_broker(d)))

    def next_delay(self, attempt: int, message_age: timedelta = ZERO) -> timedelta | None:
        """How long to wait before the next attempt, or ``None`` to give up.

        :param attempt: the attempt that has just failed, starting at 1
        :param message_age: how old the message is
        :returns: the delay, or ``None`` when there should be no further attempt
        """
        wait = self.next_wait(attempt, message_age)
        return None if wait is None else wait.delay

    def next_wait(self, attempt: int, message_age: timedelta = ZERO) -> Wait | None:
        """The whole answer: how long to wait, and where.

        :param attempt: the attempt that has just failed, starting at 1
        :param message_age: how old the message is
        :returns: the wait, or ``None`` when there should be no further attempt
        """
        if attempt >= self.max_attempts:
            return None
        if self.max_message_age > ZERO and message_age >= self.max_message_age:
            return None

        delay = self._unjittered(attempt)

        if self.waits_in_broker(delay):
            # Deliberately not jittered. A rung queue's TTL is fixed when it is
            # declared, so a moved delay would name a queue that does not exist;
            # and the spread jitter buys is already there, because each message's
            # TTL starts when it arrives rather than when the batch failed.
            return Wait(delay, in_broker=True)

        if self.jitter_factor > 0 and delay > ZERO:
            # Both directions, so a fleet of consumers that failed together
            # does not come back together. One-sided jitter only ever delays,
            # which turns a thundering herd into a slower thundering herd.
            factor = 1 + ((random.random() * 2 - 1) * self.jitter_factor)
            delay = delay * factor

        return Wait(max(delay, ZERO), in_broker=False)

    def _unjittered(self, attempt: int) -> timedelta:
        """The delay after ``attempt``, before jitter and before the threshold."""
        delay = self.initial_delay
        for _ in range(1, attempt):
            delay = delay * self.multiplier
            if self.max_delay > ZERO and delay > self.max_delay:
                delay = self.max_delay
                break
        if self.max_delay > ZERO and delay > self.max_delay:
            delay = self.max_delay
        return delay

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
