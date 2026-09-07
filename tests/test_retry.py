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
