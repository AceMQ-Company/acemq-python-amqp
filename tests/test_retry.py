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

"""The retry schedule, which has to match the other languages exactly."""

from __future__ import annotations

from datetime import timedelta

from acemq_amqp import RetryPolicy, exponential_retry, fixed_retry, no_retry
from acemq_amqp.naming import dead_letter_queue, parked_queue, retry_queue

SECOND = timedelta(seconds=1)


def test_the_schedule_doubles_and_is_the_same_everywhere() -> None:
    # The same four numbers Java, Go and .NET produce for this policy. A
    # message retried by a Python consumer and then by a Go one must not wait
    # different amounts for the same attempt.
    assert exponential_retry(5, SECOND, timedelta(minutes=1)).schedule() == [
        timedelta(seconds=1),
        timedelta(seconds=2),
        timedelta(seconds=4),
        timedelta(seconds=8),
    ]


def test_the_ceiling_holds() -> None:
    assert exponential_retry(6, SECOND, timedelta(seconds=4)).schedule() == [
        timedelta(seconds=1),
        timedelta(seconds=2),
        timedelta(seconds=4),
        timedelta(seconds=4),
        timedelta(seconds=4),
    ]


def test_no_retry_means_one_delivery() -> None:
    assert no_retry().schedule() == []
    assert no_retry().next_delay(1) is None


def test_fixed_waits_the_same_every_time() -> None:
    assert fixed_retry(4, timedelta(seconds=30)).schedule() == [
        timedelta(seconds=30),
        timedelta(seconds=30),
        timedelta(seconds=30),
    ]


def test_the_last_attempt_has_no_next_delay() -> None:
    policy = exponential_retry(3, SECOND)

    assert policy.next_delay(1) is not None
    assert policy.next_delay(2) is not None
    # The third delivery is the last one. Asking for a fourth is how a message
    # is retried for ever by a library that counts wrong.
    assert policy.next_delay(3) is None


def test_an_old_message_is_given_up_on_however_few_attempts_it_has_had() -> None:
    policy = exponential_retry(10, SECOND).give_up_after(timedelta(hours=1))

    assert policy.next_delay(1, timedelta(minutes=59)) is not None
    # One attempt, four hours old: a paused queue produces exactly this, and
    # delivering it now helps nobody.
    assert policy.next_delay(1, timedelta(hours=4)) is None


def test_a_short_wait_is_spent_in_the_consumer() -> None:
    # Below the threshold the seconds a restart loses are only seconds, and a
    # held prefetch slot is cheaper than a queue nobody asked for.
    wait = fixed_retry(2, timedelta(seconds=5)).next_wait(1)

    assert wait is not None
    assert wait.in_broker is False


def test_a_long_wait_is_spent_in_the_broker() -> None:
    # A consumer sleeping on a five-minute backoff loses the whole wait when it
    # restarts: the broker redelivers the unacknowledged message at once, so a
    # five-minute policy delivers in none.
    wait = fixed_retry(2, timedelta(minutes=5)).next_wait(1)

    assert wait is not None
    assert wait.in_broker is True
    assert wait.delay == timedelta(minutes=5)


def test_the_threshold_is_reached_rather_than_passed() -> None:
    at_it = fixed_retry(2, timedelta(seconds=30)).next_wait(1)
    below_it = fixed_retry(2, timedelta(seconds=29)).next_wait(1)

    assert at_it is not None and at_it.in_broker is True
    assert below_it is not None and below_it.in_broker is False


def test_a_broker_wait_is_never_jittered() -> None:
    # A rung queue's TTL is fixed when it is declared, so a moved delay would
    # name a queue that is not there. The spread is free anyway: each message's
    # TTL starts when it arrives rather than when the batch failed.
    policy = exponential_retry(2, timedelta(minutes=1))
    waits = [policy.next_wait(1) for _ in range(50)]

    assert all(w is not None and w.delay == timedelta(minutes=1) for w in waits)


def test_the_threshold_moves_and_zero_turns_the_rungs_off() -> None:
    policy = fixed_retry(2, timedelta(seconds=5)).wait_in_broker_from(timedelta(seconds=1))
    moved = policy.next_wait(1)
    assert moved is not None and moved.in_broker is True

    # Zero is the way out, for a service that may not declare queues on its
    # broker: nothing is long enough to reach one.
    off = policy.wait_in_broker_from(timedelta(0)).next_wait(1)
    assert off is not None and off.in_broker is False
    assert off.delay == timedelta(seconds=5)


def test_the_rungs_are_the_schedule_above_the_threshold_without_repeats() -> None:
    # Finite, because the schedule is. That is what lets the queues be declared
    # with the topology rather than conjured when a consumer first fails.
    policy = exponential_retry(6, timedelta(seconds=10))

    assert policy.schedule() == [
        timedelta(seconds=10),
        timedelta(seconds=20),
        timedelta(seconds=40),
        timedelta(seconds=80),
        timedelta(seconds=160),
    ]
    assert policy.broker_rungs() == [
        timedelta(seconds=40),
        timedelta(seconds=80),
        timedelta(seconds=160),
    ]

    # A fixed policy that waits a minute three times needs one queue, not three.
    assert fixed_retry(4, timedelta(minutes=1)).broker_rungs() == [timedelta(minutes=1)]
    assert no_retry().broker_rungs() == []


def test_jitter_moves_both_ways_and_stays_inside_the_factor() -> None:
    policy = exponential_retry(2, timedelta(seconds=10))
    delays = [policy.next_delay(1) for _ in range(200)]

    assert all(d is not None for d in delays)
    seconds = [d.total_seconds() for d in delays if d is not None]

    # 20% either side of ten seconds, and genuinely either side: jitter that
    # only ever delays turns a thundering herd into a slower thundering herd.
    assert min(seconds) >= 8.0
    assert max(seconds) <= 12.0
    assert any(s < 10 for s in seconds)
    assert any(s > 10 for s in seconds)


def test_a_delay_is_never_negative() -> None:
    reckless = RetryPolicy(max_attempts=2, initial_delay=SECOND, jitter_factor=5.0)
    delays = [reckless.next_delay(1) for _ in range(200)]
    assert all(d is not None and d >= timedelta(0) for d in delays)


def test_where_a_message_goes_when_it_cannot_be_handled() -> None:
    # Convention rather than protocol, which is why it has to be identical
    # everywhere: an operator looking for the dead letters of orders.new should
    # not have to know which language gave up on them.
    assert dead_letter_queue("orders.new") == "orders.new.dlq"
    assert parked_queue("orders.new") == "orders.new.parked"


def test_a_retry_queue_is_named_for_its_delay() -> None:
    # The wait is fixed at declaration by x-message-ttl, so a policy with four
    # different waits needs four queues, and the name is how an operator tells
    # them apart.
    assert retry_queue("orders.new", timedelta(seconds=30)) == "orders.new.retry.30s"
    assert retry_queue("orders.new", timedelta(minutes=5)) == "orders.new.retry.5m"
    assert retry_queue("orders.new", timedelta(hours=2)) == "orders.new.retry.2h"
    assert retry_queue("orders.new", timedelta(seconds=90)) == "orders.new.retry.90s"
