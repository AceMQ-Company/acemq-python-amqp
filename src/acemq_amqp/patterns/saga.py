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

"""Several things that must all happen, across systems that share no transaction.

Booking a flight, charging a card and issuing a ticket are three writes to three
systems, and there is no transaction spanning them. A saga is the answer that
does not pretend otherwise: do them in order, and when one fails, undo the ones
that already happened, most recent first.

Nothing here touches a broker. A saga is arithmetic over functions, and the
functions are usually publishes — which is why a step is allowed to be a
coroutine, and why the compensation for "published the order" is "publish the
cancellation" rather than anything the broker could roll back for you.

Two things about it are easy to get wrong and are settled here:

- **A compensation that fails does not stop the others.** It is logged, the
  step is recorded in :attr:`SagaResult.unresolved`, and the rest still run.
  Stopping at the first would leave more undone than continuing.
- **A step with no compensation is skipped, not an error.** A step that only
  read something needs no undo. The cost of that leniency is that a step which
  *should* have had one looks identical, which is the argument for writing the
  compensation first and the action second.

:meth:`Saga.run` returns rather than raises. A failed saga is not an exceptional
condition to a caller that has to decide what happens next, and the interesting
part is not the exception but :attr:`SagaResult.unresolved`.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Generic, TypeAlias, TypeVar

log = logging.getLogger("acemq")

T = TypeVar("T")

#: What a step does, and what undoes it.
#:
#: Either shape is accepted, because both are honest: a step that publishes a
#: message is a coroutine, and a step that adds a line to a list should not have
#: to pretend to be one. Whatever it returns is ignored — a saga step is there
#: for its effect, and the subject it is handed is where a result belongs.
SagaAction: TypeAlias = Callable[[T], "object | Awaitable[object]"]


@dataclass(frozen=True)
class SagaStep(Generic[T]):
    """One thing a saga does, and optionally the thing that undoes it.

    :param name: what the step is called, which is how it is identified in the
        result and therefore in whatever alert reads the result
    :param action: the work
    :param compensation: how to undo it, or ``None`` for a step with nothing to
        undo
    """

    name: str
    action: SagaAction[T]
    compensation: SagaAction[T] | None = None


@dataclass(frozen=True)
class SagaResult:
    """What a saga did.

    :param saga: the saga's name
    :param completed: the steps whose action ran, in the order they ran
    :param failed_at: the step that failed, or ``None`` when none did
    :param failure: what it failed with
    :param unresolved: the steps whose compensation failed
    """

    saga: str
    completed: tuple[str, ...] = ()
    failed_at: str | None = None
    failure: Exception | None = None
    unresolved: tuple[str, ...] = field(default=())

    @property
    def complete(self) -> bool:
        """Whether every step ran."""
        return self.failed_at is None

    @property
    def compensated(self) -> bool:
        """Whether a step failed and the earlier ones were undone."""
        return self.failed_at is not None

    @property
    def has_unresolved(self) -> bool:
        """Whether anything was left half-done.

        **This is the flag to alert on.** Everything else a saga reports is
        recoverable by construction; these are real-world effects that happened,
        were meant to be undone, and were not. Nothing else in the system knows
        about them and no retry resolves them — a person has to.
        """
        return bool(self.unresolved)

    def __str__(self) -> str:
        if self.complete:
            return f"{self.saga} completed: {' -> '.join(self.completed)}"
        undone = "" if not self.unresolved else f", UNRESOLVED {list(self.unresolved)}"
        return (
            f"{self.saga} failed at {self.failed_at}"
            f", compensated {list(self.completed)}{undone}"
        )


class Saga(Generic[T]):
    """An ordered list of steps, and what to do when one of them fails.

    Built by adding steps, each of which returns the saga, so a description
    reads as one expression::

        saga = (
            Saga("place order")
            .step("reserve stock", reserve, release)
            .step("charge card", charge, refund)
            .step("notify", notify)
        )
        result = await saga.run(order)
        if result.has_unresolved:
            alert(result)

    :param name: what this saga is called, in logs and in results
    """

    def __init__(self, name: str) -> None:
        if not name:
            raise ValueError("acemq: a saga needs a name")
        self._name = name
        self._steps: list[SagaStep[T]] = []

    @property
    def name(self) -> str:
        """What this saga is called."""
        return self._name

    @property
    def steps(self) -> tuple[SagaStep[T], ...]:
        """The steps, in the order they run."""
        return tuple(self._steps)

    def step(
        self,
        name: str,
        action: SagaAction[T],
        compensation: SagaAction[T] | None = None,
    ) -> Saga[T]:
        """Adds a step.

        :param name: what the step is called. It identifies the step in the
            result, so two steps cannot share one: a compensation report naming
            a step twice tells an operator nothing
        :param action: the work
        :param compensation: how to undo it. Leaving it out is legitimate for a
            step that changed nothing and a mistake for one that did, and there
            is no warning for the second case because a library cannot tell
            them apart
        :returns: this saga
        :raises ValueError: when the name is empty or already used
        """
        if not name:
            raise ValueError(f"acemq: a step of saga {self._name!r} needs a name")
        if any(existing.name == name for existing in self._steps):
            raise ValueError(
                f"acemq: saga {self._name!r} already has a step called {name!r}. Names "
                "identify a step in the compensation report, so two of them would make "
                "that report ambiguous"
            )
        self._steps.append(SagaStep(name, action, compensation))
        return self

    async def run(self, subject: T) -> SagaResult:
        """Runs the steps in order, compensating in reverse if one fails.

        :param subject: what the steps operate on
        :returns: what happened. It does not raise for a step failure, because
            a caller needs the compensation report more than it needs a
            traceback — the traceback is in :attr:`SagaResult.failure`
        :raises ValueError: when the saga has no steps
        """
        if not self._steps:
            raise ValueError(f"acemq: saga {self._name!r} has no steps")

        completed: list[str] = []
        for step in self._steps:
            try:
                await _call(step.action, subject)
            except Exception as failure:
                # Exception rather than BaseException: a cancellation is not a
                # step failing, it is this task being taken away, and running
                # compensations on the way out of one would be doing work
                # nobody is left to hear about.
                log.warning(
                    "acemq: saga %s failed at step %s: %r", self._name, step.name, failure
                )
                unresolved = await self._compensate(subject, completed)
                return SagaResult(
                    saga=self._name,
                    completed=tuple(completed),
                    failed_at=step.name,
                    failure=failure,
                    unresolved=tuple(unresolved),
                )
            completed.append(step.name)
            log.debug("acemq: saga %s completed step %s", self._name, step.name)

        return SagaResult(saga=self._name, completed=tuple(completed))

    async def _compensate(self, subject: T, completed: list[str]) -> list[str]:
        """Undoes what was done, most recent first.

        Reverse order because that is the order the world was changed in, and a
        compensation often depends on state a later step has not yet altered.

        :returns: the steps whose compensation failed, in the order they were
            attempted, which is what a human has to reconcile
        """
        unresolved: list[str] = []
        for name in reversed(completed):
            step = self._step_named(name)
            if step.compensation is None:
                # Legitimate: a step that only read something, or one whose
                # effect is harmless, needs no undo.
                continue
            try:
                await _call(step.compensation, subject)
            except Exception as failure:
                # Logged and carried on. Stopping here would leave more undone
                # than continuing, and the caller is told exactly which ones
                # did not come back.
                log.error(
                    "acemq: saga %s could not compensate step %s: %r",
                    self._name,
                    name,
                    failure,
                )
                unresolved.append(name)
                continue
            log.debug("acemq: saga %s compensated step %s", self._name, name)
        return unresolved

    def _step_named(self, name: str) -> SagaStep[T]:
        for step in self._steps:
            if step.name == name:
                return step
        raise AssertionError(f"acemq: saga {self._name!r} has no step called {name!r}")

    def __str__(self) -> str:
        return f"Saga({self._name}: {' -> '.join(step.name for step in self._steps)})"


async def _call(work: SagaAction[T], subject: T) -> None:
    """Runs a step of either shape."""
    returned = work(subject)
    if inspect.isawaitable(returned):
        await returned
