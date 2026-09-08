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

"""Steps that must all happen, and what is left when one of them does not."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

import pytest

from acemq_amqp.patterns import Saga, SagaResult


def note(log: list[str], entry: str) -> None:
    log.append(entry)


def failing(message: str) -> Callable[[Any], None]:
    def blow_up(_: Any) -> None:
        raise RuntimeError(message)

    return blow_up


async def test_every_step_runs_in_order_and_the_result_lists_them() -> None:
    done: list[str] = []
    saga: Saga[list[str]] = (
        Saga("place order")
        .step("reserve", lambda log: note(log, "reserve"))
        .step("charge", lambda log: note(log, "charge"))
        .step("ship", lambda log: note(log, "ship"))
    )

    result = await saga.run(done)

    assert done == ["reserve", "charge", "ship"]
    assert result.complete
    assert not result.compensated
    assert result.completed == ("reserve", "charge", "ship")
    assert result.failed_at is None
    assert result.failure is None
    assert not result.has_unresolved


async def test_a_step_may_be_a_coroutine_because_one_that_publishes_will_be() -> None:
    done: list[str] = []

    async def publish(log: list[str]) -> None:
        await asyncio.sleep(0)
        log.append("published")

    saga: Saga[list[str]] = Saga("notify").step("publish", publish)
    result = await saga.run(done)

    assert done == ["published"]
    assert result.complete


async def test_compensations_run_in_reverse_order() -> None:
    done: list[str] = []
    saga: Saga[list[str]] = (
        Saga("place order")
        .step("reserve", lambda log: note(log, "reserve"), lambda log: note(log, "release"))
        .step("charge", lambda log: note(log, "charge"), lambda log: note(log, "refund"))
        .step("ship", failing("the carrier said no"))
    )

    result = await saga.run(done)

    # Reverse, because that is the order the world was changed in: the refund
    # has to happen before the stock goes back, not after it.
    assert done == ["reserve", "charge", "refund", "release"]
    assert result.compensated
    assert not result.complete
    assert result.failed_at == "ship"
    assert isinstance(result.failure, RuntimeError)
    assert str(result.failure) == "the carrier said no"
    # The failing step is not in ``completed``: its action did not finish, so
    # there is nothing to say it did.
    assert result.completed == ("reserve", "charge")
    assert not result.has_unresolved


async def test_a_completed_step_with_no_compensation_is_skipped_rather_than_an_error() -> None:
    done: list[str] = []
    saga: Saga[list[str]] = (
        Saga("quote")
        .step("read price", lambda log: note(log, "read"))
        .step("reserve", lambda log: note(log, "reserve"), lambda log: note(log, "release"))
        .step("charge", failing("declined"))
    )

    result = await saga.run(done)

    # "read price" only read something. Nothing to undo is not a failure to
    # undo, so it is not in ``unresolved``.
    assert done == ["read", "reserve", "release"]
    assert result.completed == ("read price", "reserve")
    assert result.unresolved == ()
    assert not result.has_unresolved


async def test_a_compensation_that_fails_does_not_stop_the_others() -> None:
    done: list[str] = []
    saga: Saga[list[str]] = (
        Saga("place order")
        .step("reserve", lambda log: note(log, "reserve"), lambda log: note(log, "release"))
        .step(
            "charge",
            lambda log: note(log, "charge"),
            failing("the payment gateway is down"),
        )
        .step("label", lambda log: note(log, "label"), lambda log: note(log, "void label"))
        .step("ship", failing("the carrier said no"))
    )

    result = await saga.run(done)

    # The refund blew up between the two that worked, and both of those still
    # ran. Stopping at the refund would have left the label printed and the
    # stock reserved as well.
    assert done == ["reserve", "charge", "label", "void label", "release"]
    assert result.failed_at == "ship"
    assert result.completed == ("reserve", "charge", "label")
    assert result.unresolved == ("charge",)
    assert result.has_unresolved


async def test_an_async_compensation_that_fails_is_collected_too() -> None:
    done: list[str] = []

    async def refund(_: list[str]) -> None:
        await asyncio.sleep(0)
        raise RuntimeError("the payment gateway is down")

    saga: Saga[list[str]] = (
        Saga("place order")
        .step("charge", lambda log: note(log, "charge"), refund)
        .step("ship", failing("no"))
    )

    result = await saga.run(done)

    assert result.unresolved == ("charge",)
    assert result.has_unresolved


async def test_every_compensation_failing_leaves_every_one_of_them_unresolved() -> None:
    saga: Saga[list[str]] = (
        Saga("place order")
        .step("a", lambda log: None, failing("no"))
        .step("b", lambda log: None, failing("no"))
        .step("c", failing("stop"))
    )

    result = await saga.run([])

    # In the order compensation was attempted, which is the reverse of the
    # order they ran in — an operator reconciles them the same way round.
    assert result.unresolved == ("b", "a")


async def test_the_unresolved_list_is_logged_loudly_enough_to_alert_on(
    caplog: pytest.LogCaptureFixture,
) -> None:
    saga: Saga[list[str]] = (
        Saga("place order")
        .step("charge", lambda log: None, failing("gateway down"))
        .step("ship", failing("no"))
    )

    with caplog.at_level(logging.ERROR, logger="acemq"):
        result = await saga.run([])

    assert result.has_unresolved
    assert "could not compensate step charge" in caplog.text


async def test_a_saga_returns_rather_than_raises_what_a_step_threw() -> None:
    class DeclinedError(Exception):
        pass

    def decline(_: object) -> None:
        raise DeclinedError("insufficient funds")

    saga: Saga[object] = Saga("charge").step("charge", decline)
    result = await saga.run(object())

    assert isinstance(result.failure, DeclinedError)
    assert result.failed_at == "charge"


async def test_a_cancellation_is_not_a_step_failing() -> None:
    compensated: list[str] = []

    async def taken_away(_: object) -> None:
        raise asyncio.CancelledError

    saga: Saga[object] = (
        Saga("place order")
        .step("reserve", lambda _: None, lambda _: compensated.append("release"))
        .step("charge", taken_away)
    )

    with pytest.raises(asyncio.CancelledError):
        await saga.run(object())
    # Nothing was compensated: the task is going away and there is nobody left
    # to hear about the report.
    assert compensated == []


def test_two_steps_cannot_share_a_name() -> None:
    saga: Saga[object] = Saga("place order").step("charge", lambda _: None)

    with pytest.raises(ValueError, match="already has a step called 'charge'"):
        saga.step("charge", lambda _: None)


def test_a_saga_needs_a_name_and_a_step_does_too() -> None:
    with pytest.raises(ValueError, match="a saga needs a name"):
        Saga("")
    with pytest.raises(ValueError, match="needs a name"):
        Saga("place order").step("", lambda _: None)


async def test_a_saga_with_no_steps_is_refused_rather_than_reported_as_a_success() -> None:
    empty: Saga[object] = Saga("place order")
    with pytest.raises(ValueError, match="has no steps"):
        await empty.run(object())


def test_a_result_reads_as_a_line_in_a_log() -> None:
    assert str(SagaResult("place order", completed=("a", "b"))) == (
        "place order completed: a -> b"
    )
    failed = SagaResult(
        "place order",
        completed=("a", "b"),
        failed_at="c",
        failure=RuntimeError("no"),
        unresolved=("b",),
    )
    assert str(failed) == (
        "place order failed at c, compensated ['a', 'b'], UNRESOLVED ['b']"
    )


def test_the_steps_are_readable_and_in_order() -> None:
    saga: Saga[object] = (
        Saga("place order")
        .step("reserve", lambda _: None, lambda _: None)
        .step("charge", lambda _: None)
    )

    assert [step.name for step in saga.steps] == ["reserve", "charge"]
    assert saga.steps[0].compensation is not None
    assert saga.steps[1].compensation is None
    assert str(saga) == "Saga(place order: reserve -> charge)"
