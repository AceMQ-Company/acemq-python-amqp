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

"""The numbers the library reports, and whether it says it is working.

The metric names are a cross-language contract as much as the headers are: a
dashboard built against the Java library has to read against this one. So they
are pinned here rather than merely used.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    METRIC_ACCEPTED,
    METRIC_CONSUMED,
    METRIC_DEAD_LETTERED,
    METRIC_HANDLER_DURATION,
    METRIC_IN_FLIGHT,
    METRIC_PARKED,
    METRIC_PUBLISH_FAILED,
    METRIC_PUBLISHED,
    METRIC_REJECTED,
    METRIC_RETRIED,
    METRIC_RUNG_MISSING,
    METRIC_SET_ASIDE_FAILED,
    Ack,
    BrokerHealth,
    Connection,
    Envelope,
    HealthReport,
    HealthStatus,
    Message,
    Metrics,
    NullObserver,
    Observer,
    accept,
    aggregate_health,
    fixed_retry,
    prometheus_text,
    reject,
    retry,
)
from acemq_amqp.connection import Handler
from acemq_amqp.telemetry import DurationSummary, metric_key
from acemq_amqp.topology import Topology

QUEUE = "orders.new"


@asynccontextmanager
async def running(
    handler: Handler,
    *,
    policy: Any = None,
    declare_dead_letter: bool = True,
    declare_rungs: bool = True,
    declare: bool = True,
) -> AsyncIterator[tuple[FakeTransport, Metrics, Connection]]:
    """A consumer reporting into a Metrics, closed again afterwards.

    ``declare`` is the consumer's own declaration, on by default, which would
    otherwise put back whatever the topology flags left out.
    """
    transport = FakeTransport()
    policy = policy or fixed_retry(1, timedelta(0))
    await (
        Topology()
        .queue(
            QUEUE,
            dead_letter=declare_dead_letter,
            retry=policy if declare_rungs else None,
        )
        .apply(transport)
    )
    metrics = Metrics()
    connection = Connection(transport, retry=policy, observer=metrics)
    await connection.consume(QUEUE, handler, declare=declare)
    try:
        yield transport, metrics, connection
    finally:
        await connection.close()


def wire() -> dict[str, Any]:
    return Envelope().to_headers(routing_key=QUEUE)


def test_the_metric_names_are_the_ones_the_other_libraries_publish() -> None:
    # A dashboard built against Java or Go has to read against this. Pinned
    # rather than merely used, because a rename here is silent everywhere else
    # until somebody notices a panel has gone blank.
    assert METRIC_PUBLISHED == "acemq.messages.published"
    assert METRIC_PUBLISH_FAILED == "acemq.messages.publish.failed"
    assert METRIC_CONSUMED == "acemq.messages.consumed"
    assert METRIC_ACCEPTED == "acemq.messages.accepted"
    assert METRIC_RETRIED == "acemq.messages.retried"
    assert METRIC_REJECTED == "acemq.messages.rejected"
    assert METRIC_DEAD_LETTERED == "acemq.messages.dead.lettered"
    assert METRIC_PARKED == "acemq.messages.parked"
    assert METRIC_HANDLER_DURATION == "acemq.handler.duration"
    assert METRIC_IN_FLIGHT == "acemq.messages.in.flight"
    assert METRIC_RUNG_MISSING == "acemq.retry.rung.missing"
    assert METRIC_SET_ASIDE_FAILED == "acemq.messages.set.aside.failed"


def test_a_key_is_the_same_however_the_labels_were_built() -> None:
    # Unsorted, one counter quietly becomes several that each hold part of the
    # answer, and the total on the dashboard is wrong rather than missing.
    assert metric_key("acemq.messages.published", {"key": "a", "exchange": "b"}) == (
        'acemq.messages.published{exchange="b",key="a"}'
    )
    assert metric_key("acemq.messages.published", {}) == "acemq.messages.published"


def test_a_duration_summary_keeps_the_count_the_total_and_the_extremes() -> None:
    metrics = Metrics()
    for seconds in (0.1, 0.5, 0.2):
        metrics.observe(METRIC_HANDLER_DURATION, seconds, {"queue": QUEUE})

    summary = metrics.durations[metric_key(METRIC_HANDLER_DURATION, {"queue": QUEUE})]
    assert summary.count == 3
    assert summary.fastest == 0.1
    assert summary.slowest == 0.5
    assert summary.mean == pytest.approx(0.2666, abs=0.001)


def test_a_summary_of_nothing_has_a_mean_of_zero_rather_than_dividing_by_it() -> None:
    assert DurationSummary().mean == 0.0


def test_the_null_observer_satisfies_the_interface_and_does_nothing() -> None:
    # Real object rather than None checked for at each call site, because the
    # alternative is a branch on every publish and one is eventually wrong.
    observer: Observer = NullObserver()
    observer.count(METRIC_PUBLISHED, 1, {})
    observer.gauge(METRIC_IN_FLIGHT, 1, {})
    observer.observe(METRIC_HANDLER_DURATION, 1.0, {})


async def test_a_publish_is_counted_with_where_it_went() -> None:
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    metrics = Metrics()
    mq = Connection(transport, observer=metrics)

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    assert metrics.counts[metric_key(METRIC_PUBLISHED, {"exchange": "", "key": QUEUE})] == 1


async def test_a_publish_that_reached_no_queue_is_counted_as_a_failure() -> None:
    # Unroutable is the quietest failure AMQP has, which is exactly why it gets
    # a counter rather than only an exception the caller may not read.
    transport = FakeTransport()
    metrics = Metrics()
    mq = Connection(transport, observer=metrics)

    with pytest.raises(Exception, match="no queue"):
        await mq.publisher(routing_key="nowhere", mandatory=True).send({"id": "1"})

    key = metric_key(METRIC_PUBLISH_FAILED, {"exchange": "", "key": "nowhere"})
    assert metrics.counts[key] == 1
    published = metric_key(METRIC_PUBLISHED, {"exchange": "", "key": "nowhere"})
    assert published not in metrics.counts


async def test_a_handled_message_is_counted_timed_and_its_decision_recorded() -> None:
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    labels = {"queue": QUEUE}
    assert metrics.counts[metric_key(METRIC_CONSUMED, labels)] == 1
    assert metrics.counts[metric_key(METRIC_ACCEPTED, labels)] == 1
    assert metrics.durations[metric_key(METRIC_HANDLER_DURATION, labels)].count == 1
    # Back to nothing in flight: the gauge is set on the way out as well as on
    # the way in, or it only ever goes up.
    assert metrics.gauges[metric_key(METRIC_IN_FLIGHT, labels)] == 0


async def test_a_rejected_message_is_counted_as_rejected_and_dead_lettered() -> None:
    async def handler(message: Message) -> Ack:
        return reject(ValueError("nothing to do with this"))

    async with running(handler) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    labels = {"queue": QUEUE}
    assert metrics.counts[metric_key(METRIC_REJECTED, labels)] == 1
    assert metrics.counts[metric_key(METRIC_DEAD_LETTERED, labels)] == 1


async def test_a_message_that_would_not_decode_is_counted_as_parked() -> None:
    # Separate from the dead letters on purpose: a message that failed five
    # times and a message nothing could read are different problems.
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (transport, metrics, _):
        await transport.deliver(QUEUE, b"not json at all", headers=wire())

    labels = {"queue": QUEUE}
    assert metrics.counts[metric_key(METRIC_PARKED, labels)] == 1
    assert metric_key(METRIC_DEAD_LETTERED, labels) not in metrics.counts


async def test_a_retry_says_where_the_wait_happened() -> None:
    policy = fixed_retry(3, timedelta(0))

    async def handler(message: Message) -> Ack:
        return retry(RuntimeError("the warehouse is not answering"))

    async with running(handler, policy=policy) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    here = metric_key(METRIC_RETRIED, {"queue": QUEUE, "where": "consumer"})
    assert metrics.counts[here] == 1


async def test_a_long_retry_reaching_the_rung_is_counted_against_the_broker() -> None:
    policy = fixed_retry(3, timedelta(minutes=2))

    async def handler(message: Message) -> Ack:
        return retry(RuntimeError("the warehouse is not answering"))

    async with running(handler, policy=policy) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    there = metric_key(METRIC_RETRIED, {"queue": QUEUE, "where": "broker"})
    assert metrics.counts[there] == 1
    assert METRIC_RUNG_MISSING not in str(metrics)


async def test_a_missing_rung_is_counted_because_nothing_else_shows_it() -> None:
    # The message is still retried and the wait still happens, so a dashboard
    # reads as normal while the reason the rung exists is quietly gone: a
    # consumer restart mid-wait now shortens a long backoff to nothing.
    #
    # A millisecond rather than the two minutes a real policy would use,
    # because the fallback really does wait here and a test that proves it
    # should not take as long as the thing it is proving.
    policy = fixed_retry(3, timedelta(milliseconds=1)).wait_in_broker_from(
        timedelta(milliseconds=1)
    )

    async def handler(message: Message) -> Ack:
        return retry(RuntimeError("the warehouse is not answering"))

    async with running(handler, policy=policy, declare_rungs=False, declare=False) as (
        transport,
        metrics,
        _,
    ):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    rung = f"{QUEUE}.retry.0s"
    missing = metric_key(METRIC_RUNG_MISSING, {"queue": QUEUE, "rung": rung})
    assert metrics.counts[missing] == 1
    # And it fell back to waiting here, which is the whole point of the counter.
    here = metric_key(METRIC_RETRIED, {"queue": QUEUE, "where": "consumer"})
    assert metrics.counts[here] == 1


async def test_a_dead_letter_queue_that_is_not_there_is_counted() -> None:
    # A consumer that declared its own would have this queue, so what is counted
    # here is the one that was told not to and found nobody else had.
    async def handler(message: Message) -> Ack:
        return reject(ValueError("no"))

    async with running(handler, declare_dead_letter=False, declare=False) as (
        transport,
        metrics,
        _,
    ):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    key = metric_key(METRIC_SET_ASIDE_FAILED, {"queue": QUEUE, "target": f"{QUEUE}.dlq"})
    assert metrics.counts[key] == 1


async def test_a_connection_with_no_observer_records_nothing_and_still_works() -> None:
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport)

    assert isinstance(mq.observer, NullObserver)
    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    assert len(transport.sent_to(QUEUE)) == 1


def test_metrics_render_in_the_prometheus_text_format_with_nothing_installed() -> None:
    # A service that only wants a scrape endpoint should not have to install a
    # client library to get one, which is what this is for.
    metrics = Metrics()
    metrics.count(METRIC_PUBLISHED, 3, {"exchange": "events", "key": "order.placed"})
    metrics.gauge(METRIC_IN_FLIGHT, 2, {"queue": QUEUE})
    metrics.observe(METRIC_HANDLER_DURATION, 0.25, {"queue": QUEUE})

    body = prometheus_text(metrics)

    assert '# TYPE acemq_messages_published counter' in body
    assert 'acemq_messages_published{exchange="events",key="order.placed"} 3' in body
    assert 'acemq_messages_in_flight{queue="orders.new"} 2' in body
    assert 'acemq_handler_duration_count{queue="orders.new"} 1' in body
    assert 'acemq_handler_duration_sum{queue="orders.new"} 0.25' in body


def test_rendering_nothing_is_empty_rather_than_broken() -> None:
    assert prometheus_text(Metrics()) == ""


async def test_health_is_up_when_the_broker_answers_and_the_consumers_read() -> None:
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (_, _, connection):
        report = await connection.health()

    assert report.status is HealthStatus.UP
    assert report.healthy is True
    assert report.parts["consumers"] == 1
    assert "round-trip" in report.parts


async def test_health_is_down_once_the_connection_is_closed() -> None:
    connection = Connection(FakeTransport())
    await connection.close()

    report = await connection.health()

    assert report.status is HealthStatus.DOWN
    assert report.healthy is False
    assert "closed" in report.detail


async def test_health_is_down_when_the_broker_does_not_answer() -> None:
    # A socket that is open but wedged answers a socket-level check exactly as a
    # healthy one does, right up until something is asked of it. So something is
    # asked of it.
    class Wedged(FakeTransport):
        async def queue_exists(self, name: str) -> bool:
            raise ConnectionResetError("the broker went away")

    report = await Connection(Wedged()).health()

    assert report.status is HealthStatus.DOWN
    assert "the broker went away" in report.detail


async def test_health_is_degraded_when_a_consumer_has_stopped_reading() -> None:
    # From outside, a consumer whose workers have died is indistinguishable from
    # a quiet queue. Degraded rather than down: the connection works, and a
    # replacement instance would almost certainly stall the same way.
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (_, _, connection):
        consumer = connection.consumers[0]
        assert consumer.running is True

        # Reaching in, because there is no public way to break a consumer and
        # this is the failure the check exists for. The workers are ended the
        # way close() ends them, which leaves them finished rather than
        # cancelled — a stalled consumer, not a closed one.
        for _ in consumer._workers:
            consumer._work.put_nowait(None)
        await asyncio.gather(*consumer._workers)

        assert consumer.running is False
        report = await connection.health()

    assert report.status is HealthStatus.DEGRADED
    assert report.healthy is True
    assert report.parts["stalled"] == [QUEUE]


async def test_a_health_report_reads_as_the_line_a_probe_would_log() -> None:
    assert str(HealthReport(HealthStatus.UP)) == "up"
    assert str(HealthReport(HealthStatus.DOWN, "no broker")) == "down: no broker"


async def test_aggregating_takes_the_worst_of_the_answers() -> None:
    # A service that cannot reach its broker is not ready however healthy the
    # rest of it is.
    connection = Connection(FakeTransport())
    await connection.close()

    report = await aggregate_health(BrokerHealth(connection), _Always("database"))

    assert report.status is HealthStatus.DOWN
    assert "broker" in report.detail
    assert report.parts["database"].status is HealthStatus.UP


async def test_aggregating_nothing_is_up_rather_than_undecided() -> None:
    assert (await aggregate_health()).status is HealthStatus.UP


async def test_a_check_that_will_not_answer_cannot_hang_the_probe() -> None:
    # A readiness probe that hangs is a pod that never comes back, so the
    # deadline is here rather than in the caller's hands.
    report = await aggregate_health(_Hangs("slow"), timeout=0.05)

    assert report.status is HealthStatus.DOWN
    assert "did not answer" in report.parts["slow"].detail


async def test_a_check_that_raises_is_reported_rather_than_thrown() -> None:
    report = await aggregate_health(_Raises("broken"))

    assert report.status is HealthStatus.DOWN
    assert "the check itself failed" in report.parts["broken"].detail


class _Always:
    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    async def check(self) -> HealthReport:
        return HealthReport(HealthStatus.UP)


class _Hangs(_Always):
    async def check(self) -> HealthReport:
        await asyncio.sleep(30)
        return HealthReport(HealthStatus.UP)


class _Raises(_Always):
    async def check(self) -> HealthReport:
        raise RuntimeError("the database driver is not loaded")


def test_the_observer_protocol_accepts_anything_with_the_three_methods() -> None:
    class Counting:
        def __init__(self) -> None:
            self.seen: list[str] = []

        def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
            self.seen.append(metric)

        def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
            self.seen.append(metric)

        def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
            self.seen.append(metric)

    assert isinstance(Counting(), Observer)
