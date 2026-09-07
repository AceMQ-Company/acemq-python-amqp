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

"""What the library is doing, and whether it is still working.

Two questions an operator asks about a service that consumes a queue, and the
library is the only thing that can answer either. How much is going through, how
much is failing and how long a handler takes are numbers nobody outside can see;
whether the connection is really up — as opposed to a socket that is open and
wedged — is a question only something holding the connection can ask.

The metric names are the same in Java, Go, .NET and here, so a dashboard built
against one library reads against another. Java publishes them through
Micrometer and .NET through ``System.Diagnostics.Metrics``; Python's standard
library has no metrics interface at all, which is why this module defines a
small one and calls it. Implement :class:`Observer` against Prometheus,
OpenTelemetry, statsd or a log line — or use :class:`Metrics`, which keeps the
numbers in memory and is enough for a health endpoint, a test, or a signal
handler that prints them.

Taking a dependency on a metrics library instead would put every user of this
package on the one it chose. The transport already makes that argument and wins
it: aio-pika is an extra, and so is anything here that needs more than the
standard library.

Health is a :class:`HealthReport` with three states rather than a boolean.
``degraded`` exists because "working, but not as well as it should" is worth an
alert and is not worth taking an instance out of rotation — the replacement will
almost certainly be degraded too.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol, runtime_checkable

#: Messages handed to the broker.
METRIC_PUBLISHED = "acemq.messages.published"

#: Publishes that did not succeed, including one an interceptor refused and one
#: the broker could not route.
METRIC_PUBLISH_FAILED = "acemq.messages.publish.failed"

#: Messages delivered to a handler.
METRIC_CONSUMED = "acemq.messages.consumed"

#: What handlers decided. Three counters rather than one with an outcome label,
#: because that is what the other libraries publish and a shared dashboard has
#: to read the same.
METRIC_ACCEPTED = "acemq.messages.accepted"
METRIC_RETRIED = "acemq.messages.retried"
METRIC_REJECTED = "acemq.messages.rejected"

#: Messages that ran out of attempts, or were given up on for any other reason,
#: and went to ``{queue}.dlq``.
METRIC_DEAD_LETTERED = "acemq.messages.dead.lettered"

#: Messages that never reached the handler at all and went to
#: ``{queue}.parked``. Separate from the dead letters on purpose: a message that
#: failed five times and a message nothing could read are different problems
#: with different answers, and whoever drains the queue should not have to sort
#: them by hand.
METRIC_PARKED = "acemq.messages.parked"

#: How long handlers take, in seconds.
METRIC_HANDLER_DURATION = "acemq.handler.duration"

#: How many messages are being handled right now.
METRIC_IN_FLIGHT = "acemq.messages.in.flight"

#: Long retries that had to wait in the consumer because the rung queue they
#: were meant to wait on is not on the broker.
#:
#: Worth an alert. Nothing breaks — the message is still retried and the wait
#: still happens — but the reason the rung exists is gone: a consumer restart
#: mid-wait now shortens a five-minute backoff to nothing, and the only other
#: sign of it is one log line on a path nobody is watching.
METRIC_RUNG_MISSING = "acemq.retry.rung.missing"

#: Messages that could not be moved to a dead-letter or parking queue, usually
#: because it was never declared. The message is rejected to the broker instead,
#: which is the last thing between it and nothing.
METRIC_SET_ASIDE_FAILED = "acemq.messages.set.aside.failed"


@runtime_checkable
class Observer(Protocol):
    """Told what the library is doing.

    Every method is called on the path a message takes, so none of them may
    block and all of them have to be safe to call from several tasks at once.
    An observer that talks to the network on each call is an observer that makes
    every publish as slow as the network it talks to; buffer, and flush
    somewhere else.

    Nothing here is awaitable, deliberately. A metrics call that could suspend
    would reorder the code around it and turn "count this" into a scheduling
    decision, and no metrics library needs it.
    """

    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
        """Adds to a counter."""

    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
        """Records a duration, in seconds."""

    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
        """Sets a current value."""


class NullObserver:
    """An observer that does nothing, which is what a connection has by default.

    A real object rather than ``None`` checked for at each call site, because
    the alternative is a branch on every publish and every delivery, and one of
    them is eventually written the wrong way round.
    """

    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
        return None

    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
        return None

    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
        return None


@dataclass(frozen=True, slots=True)
class DurationSummary:
    """What a :class:`Metrics` knows about a timing.

    Deliberately not percentiles. Computing those needs either every sample kept
    or a sketch, and a library that quietly did either would be making a
    decision about memory that belongs to the application.
    """

    count: int = 0
    total: float = 0.0
    fastest: float = 0.0
    slowest: float = 0.0

    @property
    def mean(self) -> float:
        """The average, or zero when nothing has been recorded."""
        return self.total / self.count if self.count else 0.0


class Metrics:
    """An :class:`Observer` that keeps the numbers in memory.

    Enough to serve from a health endpoint, assert on in a test, or print on a
    signal. It is not a substitute for a real metrics system: no histograms, no
    percentiles, and nothing is exported anywhere. :func:`prometheus_text`
    renders what it has for a scraper, which needs no dependency at all.

    Locked rather than relying on the event loop, because the blocking API in
    :mod:`acemq_amqp.sync` runs handlers on a thread pool and a counter read
    from two threads at once is a counter that loses increments.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
        self._gauges: dict[str, int] = {}
        self._durations: dict[str, DurationSummary] = {}

    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
        key = metric_key(metric, labels)
        with self._lock:
            self._counts[key] = self._counts.get(key, 0) + delta

    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
        key = metric_key(metric, labels)
        with self._lock:
            self._gauges[key] = value

    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
        key = metric_key(metric, labels)
        with self._lock:
            existing = self._durations.get(key)
            if existing is None:
                self._durations[key] = DurationSummary(1, seconds, seconds, seconds)
                return
            self._durations[key] = DurationSummary(
                count=existing.count + 1,
                total=existing.total + seconds,
                fastest=min(existing.fastest, seconds),
                slowest=max(existing.slowest, seconds),
            )

    @property
    def counts(self) -> dict[str, int]:
        """Every counter, keyed by metric and labels."""
        with self._lock:
            return dict(self._counts)

    @property
    def gauges(self) -> dict[str, int]:
        """Every gauge."""
        with self._lock:
            return dict(self._gauges)

    @property
    def durations(self) -> dict[str, DurationSummary]:
        """A summary per timing: how many, the total, the fastest, the slowest."""
        with self._lock:
            return dict(self._durations)

    def __str__(self) -> str:
        lines = [f"{name} {value}" for name, value in sorted(self.counts.items())]
        lines += [f"{name} {value}" for name, value in sorted(self.gauges.items())]
        lines += [
            f"{name} count={summary.count} mean={summary.mean:.4f}s"
            for name, summary in sorted(self.durations.items())
        ]
        return "\n".join(lines) or "nothing recorded yet"


def metric_key(metric: str, labels: Mapping[str, str]) -> str:
    """A metric and its labels as one string.

    Sorted, so the same labels always give the same key however the mapping was
    built. Unsorted, one counter quietly becomes several that each hold part of
    the answer.
    """
    if not labels:
        return metric
    rendered = ",".join(f'{name}="{labels[name]}"' for name in sorted(labels))
    return f"{metric}{{{rendered}}}"


def prometheus_text(metrics: Metrics) -> str:
    """Renders a :class:`Metrics` in the Prometheus text format.

    Written out rather than through a client library, so a service that only
    wants a scrape endpoint needs nothing installed. The format is small and
    stable: a TYPE line, then samples, with dots in the metric name replaced by
    underscores because Prometheus does not allow them.

    A real registry — exemplars, native histograms, a shared process collector —
    is what :mod:`acemq_amqp.prometheus` is for, and it needs the extra.

    :param metrics: what to render
    :returns: the body of a ``/acemq-metrics`` response
    """
    lines: list[str] = []
    for name, value in sorted(metrics.counts.items()):
        metric, labels = _split_key(name)
        base = _prometheus_name(metric)
        lines += [f"# TYPE {base} counter", f"{base}{labels} {value}"]
    for name, value in sorted(metrics.gauges.items()):
        metric, labels = _split_key(name)
        base = _prometheus_name(metric)
        lines += [f"# TYPE {base} gauge", f"{base}{labels} {value}"]
    for name, summary in sorted(metrics.durations.items()):
        metric, labels = _split_key(name)
        base = _prometheus_name(metric)
        lines += [
            f"# TYPE {base} summary",
            f"{base}_count{labels} {summary.count}",
            f"{base}_sum{labels} {summary.total}",
        ]
    return "\n".join(lines) + ("\n" if lines else "")


def _split_key(key: str) -> tuple[str, str]:
    """A key back into its metric and its rendered labels."""
    if "{" not in key:
        return key, ""
    metric, _, labels = key.partition("{")
    return metric, "{" + labels


def _prometheus_name(metric: str) -> str:
    return metric.replace(".", "_")


class HealthStatus(str, Enum):
    """How a check turned out."""

    #: Working.
    UP = "up"

    #: Not working. A readiness probe should fail on this.
    DOWN = "down"

    #: Working, but not as well as it should. Worth an alert; not worth taking
    #: the instance out of rotation, because the replacement will almost
    #: certainly be degraded too.
    DEGRADED = "degraded"


@dataclass(frozen=True, slots=True)
class HealthReport:
    """What a check found.

    :param status: up, down or degraded
    :param detail: why, when it is not simply up
    :param checked: when this was worked out, in UTC
    :param parts: whatever the check wants to show — a consumer count, a
        round-trip time, the reports of the checks that were combined
    """

    status: HealthStatus
    detail: str = ""
    checked: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    parts: Mapping[str, Any] = field(default_factory=dict)

    @property
    def healthy(self) -> bool:
        """Whether a readiness probe should pass.

        Degraded passes. An instance that is slow is still an instance that
        works, and taking it out of rotation moves the traffic to one that will
        be just as slow with more of it.
        """
        return self.status is not HealthStatus.DOWN

    def __str__(self) -> str:
        return self.status.value if not self.detail else f"{self.status.value}: {self.detail}"


@runtime_checkable
class HealthCheck(Protocol):
    """Anything that can report on itself.

    The library provides one for the connection; an application adds its own — a
    database, a downstream service — and :func:`aggregate_health` combines them.
    """

    @property
    def name(self) -> str:
        """What this check is called in a combined report."""

    async def check(self) -> HealthReport:
        """The current state.

        It must return quickly and must not raise: a check that hangs makes a
        readiness probe hang with it, and a probe that hangs is a pod that never
        comes back.
        """


async def aggregate_health(*checks: HealthCheck, timeout: float = 5.0) -> HealthReport:
    """Runs every check at once and combines the answers.

    The combined status is the worst of them: one thing being down makes the
    whole report down, because a service that cannot reach its broker is not
    ready however healthy the rest of it is.

    At once rather than in turn, so a slow check does not add its latency to the
    others, and under a deadline, so one that ignores the instruction above
    still cannot hang the probe. A check that times out is reported as down with
    the reason, which is the honest reading: something that will not answer in
    five seconds is not something a request can depend on.

    :param checks: what to ask
    :param timeout: how long to give all of them together
    :returns: the combined report
    """
    if not checks:
        return HealthReport(HealthStatus.UP)

    async def run(check: HealthCheck) -> tuple[str, HealthReport]:
        try:
            return check.name, await check.check()
        except Exception as failure:
            return check.name, HealthReport(
                HealthStatus.DOWN, f"the check itself failed: {failure}"
            )

    tasks = [asyncio.ensure_future(run(check)) for check in checks]
    try:
        done, pending = await asyncio.wait(tasks, timeout=timeout)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        raise

    parts: dict[str, Any] = {}
    worst = HealthStatus.UP
    reasons: list[str] = []

    for task in pending:
        task.cancel()
    for index, task in enumerate(tasks):
        if task in done:
            name, report = task.result()
        else:
            name = checks[index].name
            report = HealthReport(HealthStatus.DOWN, f"did not answer within {timeout}s")
        parts[name] = report
        if report.status is HealthStatus.DOWN:
            worst = HealthStatus.DOWN
            reasons.append(name)
        elif report.status is HealthStatus.DEGRADED:
            if worst is not HealthStatus.DOWN:
                worst = HealthStatus.DEGRADED
            reasons.append(name)

    return HealthReport(worst, ", ".join(reasons), parts=parts)
