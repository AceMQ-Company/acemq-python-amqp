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
    METRIC_CONSUME_ATTEMPTS,
    METRIC_CONSUME_DURATION,
    METRIC_CONSUME_IN_FLIGHT,
    METRIC_CONSUME_TOTAL,
    METRIC_DEAD_LETTERED_TOTAL,
    METRIC_OUTBOX_LAG,
    METRIC_OUTBOX_TOTAL,
    METRIC_PIPELINE_RUN_DURATION,
    METRIC_PIPELINE_RUN_TOTAL,
    METRIC_PUBLISH_DURATION,
    METRIC_PUBLISH_TOTAL,
    METRIC_REQUEST_DURATION,
    METRIC_REQUEST_TOTAL,
    METRIC_RETRIED_TOTAL,
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
    PublishError,
    accept,
    aggregate_health,
    fixed_retry,
    prometheus_text,
    reject,
    retry,
)
from acemq_amqp.ack import (
    OUTCOME_ACKED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_PARKED,
    OUTCOME_REJECTED,
    OUTCOME_RETRIED,
)
from acemq_amqp.connection import Handler
from acemq_amqp.telemetry import (
    OUTCOME_CONFIRMED,
    OUTCOME_UNROUTABLE,
    DurationSummary,
    metric_key,
)
from acemq_amqp.topology import Topology
from acemq_amqp.transport import ExchangeSpec, Outbound, PublishResult, QueueSpec

QUEUE = "orders.new"


class RefusingTransport(FakeTransport):
    """A broker that will not take anything at all."""

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        raise RuntimeError("the broker said no")


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
    # These are Java's, spelled out in acemq-amqp-api MetricNames. A dashboard
    # built against Java or Go has to read against this. Pinned rather than
    # merely used, because a rename here is silent everywhere else until
    # somebody notices a panel has gone blank.
    assert METRIC_PUBLISH_TOTAL == "acemq.publish.total"
    assert METRIC_CONSUME_TOTAL == "acemq.consume.total"
    assert METRIC_CONSUME_DURATION == "acemq.consume.duration"
    assert METRIC_CONSUME_IN_FLIGHT == "acemq.consume.in.flight"
    assert METRIC_RETRIED_TOTAL == "acemq.messages.retried.total"
    assert METRIC_DEAD_LETTERED_TOTAL == "acemq.messages.dead.lettered.total"
    assert METRIC_SET_ASIDE_FAILED == "acemq.messages.set.aside.failed"
    assert METRIC_RUNG_MISSING == "acemq.retry.rung.missing"
    assert METRIC_PUBLISH_DURATION == "acemq.publish.duration"
    assert METRIC_CONSUME_ATTEMPTS == "acemq.consume.attempts"
    assert METRIC_REQUEST_DURATION == "acemq.request.duration"
    assert METRIC_REQUEST_TOTAL == "acemq.request.total"
    assert METRIC_PIPELINE_RUN_DURATION == "acemq.pipeline.run.duration"
    assert METRIC_PIPELINE_RUN_TOTAL == "acemq.pipeline.run.total"


def test_every_name_java_publishes_is_written_somewhere_here() -> None:
    # The list used to be six names short, and the six were documented as
    # absent. They are all written now, so this pins the whole set rather than
    # the part that happened to be implemented.
    assert {
        METRIC_PUBLISH_TOTAL,
        METRIC_PUBLISH_DURATION,
        METRIC_CONSUME_TOTAL,
        METRIC_CONSUME_DURATION,
        METRIC_CONSUME_ATTEMPTS,
        METRIC_CONSUME_IN_FLIGHT,
        METRIC_RETRIED_TOTAL,
        METRIC_DEAD_LETTERED_TOTAL,
        METRIC_SET_ASIDE_FAILED,
        METRIC_REQUEST_TOTAL,
        METRIC_REQUEST_DURATION,
        METRIC_OUTBOX_TOTAL,
        METRIC_OUTBOX_LAG,
        METRIC_PIPELINE_RUN_TOTAL,
        METRIC_PIPELINE_RUN_DURATION,
    } == {
        "acemq.publish.total",
        "acemq.publish.duration",
        "acemq.consume.total",
        "acemq.consume.duration",
        "acemq.consume.attempts",
        "acemq.consume.in.flight",
        "acemq.messages.retried.total",
        "acemq.messages.dead.lettered.total",
        "acemq.messages.set.aside.failed",
        "acemq.request.total",
        "acemq.request.duration",
        "acemq.outbox.total",
        "acemq.outbox.lag",
        "acemq.pipeline.run.total",
        "acemq.pipeline.run.duration",
    }


def test_the_outcome_values_are_the_ones_the_other_libraries_tag_with() -> None:
    # One counter with an outcome, not one counter per outcome — so the words
    # are as much a part of the contract as the metric names are.
    assert (OUTCOME_CONFIRMED, OUTCOME_UNROUTABLE) == ("confirmed", "unroutable")
    assert (OUTCOME_ACKED, OUTCOME_RETRIED, OUTCOME_REJECTED) == (
        "acked",
        "retried",
        "rejected",
    )
    assert (OUTCOME_DEAD_LETTERED, OUTCOME_PARKED) == ("dead_lettered", "parked")


def test_a_key_is_the_same_however_the_labels_were_built() -> None:
    # Unsorted, one counter quietly becomes several that each hold part of the
    # answer, and the total on the dashboard is wrong rather than missing.
    assert metric_key("acemq.publish.total", {"key": "a", "exchange": "b"}) == (
        'acemq.publish.total{exchange="b",key="a"}'
    )
    assert metric_key("acemq.publish.total", {}) == "acemq.publish.total"


def test_a_duration_summary_keeps_the_count_the_total_and_the_extremes() -> None:
    metrics = Metrics()
    for seconds in (0.1, 0.5, 0.2):
        metrics.observe(METRIC_CONSUME_DURATION, seconds, {"queue": QUEUE})

    summary = metrics.durations[metric_key(METRIC_CONSUME_DURATION, {"queue": QUEUE})]
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
    observer.count(METRIC_PUBLISH_TOTAL, 1, {})
    observer.gauge(METRIC_CONSUME_IN_FLIGHT, 1, {})
    observer.observe(METRIC_CONSUME_DURATION, 1.0, {})


async def test_a_publish_is_counted_with_where_it_went() -> None:
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    metrics = Metrics()
    mq = Connection(transport, observer=metrics)

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    # ``routing.key`` and not ``key``: the fully-qualified name Java and .NET
    # already tag a publish with, so one dashboard reads across all five. The
    # outcome is a label rather than a second metric, for the same reason.
    labels = {"exchange": "", "routing.key": QUEUE, "outcome": OUTCOME_CONFIRMED}
    assert metrics.counts[metric_key(METRIC_PUBLISH_TOTAL, labels)] == 1
    assert (
        metric_key(METRIC_PUBLISH_TOTAL, {"exchange": "", "key": QUEUE})
        not in metrics.counts
    )


async def test_a_publish_that_reached_no_queue_is_counted_as_a_failure() -> None:
    # Unroutable is the quietest failure AMQP has, which is exactly why it gets
    # a counter rather than only an exception the caller may not read.
    transport = FakeTransport()
    metrics = Metrics()
    mq = Connection(transport, observer=metrics)

    with pytest.raises(Exception, match="no queue"):
        await mq.publisher(routing_key="nowhere", mandatory=True).send({"id": "1"})

    where = {"exchange": "", "routing.key": "nowhere"}
    key = metric_key(METRIC_PUBLISH_TOTAL, {**where, "outcome": OUTCOME_UNROUTABLE})
    assert metrics.counts[key] == 1
    # Unroutable and not merely failed: the broker took the message and had
    # nowhere to put it, which is a topology problem rather than a broker one.
    confirmed = metric_key(METRIC_PUBLISH_TOTAL, {**where, "outcome": OUTCOME_CONFIRMED})
    assert confirmed not in metrics.counts


async def test_a_handled_message_is_counted_timed_and_its_decision_recorded() -> None:
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    labels = {"queue": QUEUE}
    acked = {**labels, "outcome": OUTCOME_ACKED}
    assert metrics.counts[metric_key(METRIC_CONSUME_TOTAL, acked)] == 1
    # Timed with the outcome on it too: how long a message took and what
    # happened to it are one question, and a p99 that mixes the messages that
    # worked with the ones that failed answers neither half.
    assert metrics.durations[metric_key(METRIC_CONSUME_DURATION, acked)].count == 1
    # Back to nothing in flight: the gauge is set on the way out as well as on
    # the way in, or it only ever goes up.
    assert metrics.gauges[metric_key(METRIC_CONSUME_IN_FLIGHT, labels)] == 0


async def test_a_rejected_message_is_counted_as_rejected_and_dead_lettered() -> None:
    async def handler(message: Message) -> Ack:
        return reject(ValueError("nothing to do with this"))

    async with running(handler) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    labels = {"queue": QUEUE}
    rejected = metric_key(METRIC_CONSUME_TOTAL, {**labels, "outcome": OUTCOME_REJECTED})
    assert metrics.counts[rejected] == 1
    # And the standalone counter as well, which Java also keeps: the outcome
    # says what was decided, this says how many messages have been set aside.
    dead = {**labels, "outcome": OUTCOME_DEAD_LETTERED}
    assert metrics.counts[metric_key(METRIC_DEAD_LETTERED_TOTAL, dead)] == 1


async def test_a_message_that_would_not_decode_is_counted_as_parked() -> None:
    # Separate from the dead letters on purpose: a message that failed five
    # times and a message nothing could read are different problems.
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (transport, metrics, _):
        await transport.deliver(QUEUE, b"not json at all", headers=wire())

    labels = {"queue": QUEUE}
    # The same counter a dead-lettering moves, told apart by the outcome — which
    # is the whole of the difference, and enough of it: a message that failed
    # five times and a message nothing could read are different problems, and
    # they are two series of one metric rather than two metrics.
    parked = {**labels, "outcome": OUTCOME_PARKED}
    dead = {**labels, "outcome": OUTCOME_DEAD_LETTERED}
    assert metrics.counts[metric_key(METRIC_DEAD_LETTERED_TOTAL, parked)] == 1
    assert metric_key(METRIC_DEAD_LETTERED_TOTAL, dead) not in metrics.counts
    # Settled without a handler ever running, and still counted once: a total
    # that missed the messages nothing could read would not add up.
    assert metrics.counts[metric_key(METRIC_CONSUME_TOTAL, parked)] == 1


async def test_a_retry_says_where_the_wait_happened() -> None:
    policy = fixed_retry(3, timedelta(0))

    async def handler(message: Message) -> Ack:
        return retry(RuntimeError("the warehouse is not answering"))

    async with running(handler, policy=policy) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    here = metric_key(METRIC_RETRIED_TOTAL, {"queue": QUEUE, "where": "consumer"})
    assert metrics.counts[here] == 1


async def test_a_long_retry_reaching_the_rung_is_counted_against_the_broker() -> None:
    policy = fixed_retry(3, timedelta(minutes=2))

    async def handler(message: Message) -> Ack:
        return retry(RuntimeError("the warehouse is not answering"))

    async with running(handler, policy=policy) as (transport, metrics, _):
        await transport.deliver(QUEUE, b'{"id": "1"}', headers=wire())

    there = metric_key(METRIC_RETRIED_TOTAL, {"queue": QUEUE, "where": "broker"})
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
    here = metric_key(METRIC_RETRIED_TOTAL, {"queue": QUEUE, "where": "consumer"})
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
    metrics.count(METRIC_PUBLISH_TOTAL, 3, {"exchange": "events", "key": "order.placed"})
    metrics.gauge(METRIC_CONSUME_IN_FLIGHT, 2, {"queue": QUEUE})
    metrics.observe(METRIC_CONSUME_DURATION, 0.25, {"queue": QUEUE})

    body = prometheus_text(metrics)

    assert "# TYPE acemq_publish_total counter" in body
    assert 'acemq_publish_total{exchange="events",key="order.placed"} 3' in body
    assert 'acemq_consume_in_flight{queue="orders.new"} 2' in body
    assert 'acemq_consume_duration_count{queue="orders.new"} 1' in body
    assert 'acemq_consume_duration_sum{queue="orders.new"} 0.25' in body


def test_a_dotted_label_name_is_rendered_as_prometheus_spells_it() -> None:
    # ``routing.key`` is a legal tag name in Micrometer and OpenTelemetry and an
    # illegal label name here: Prometheus allows [a-zA-Z_][a-zA-Z0-9_]* and
    # nothing else, and one unparseable line loses the whole scrape rather than
    # that sample. The value keeps its dots, because a routing key is where the
    # dots mean something.
    metrics = Metrics()
    metrics.count(
        METRIC_PUBLISH_TOTAL,
        1,
        {"exchange": "orders", "routing.key": "order.placed", "outcome": "confirmed"},
    )

    body = prometheus_text(metrics)

    assert (
        'acemq_publish_total{exchange="orders",outcome="confirmed",'
        'routing_key="order.placed"} 1'
    ) in body
    assert "routing.key=" not in body


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


async def test_health_says_whether_the_broker_has_blocked_the_connection() -> None:
    # The state a report has to carry, because a blocked broker and a broker
    # that has gone away look identical from outside and want opposite
    # responses: one is waited for, the other is failed over from.
    connection = Connection(FakeTransport())

    assert connection.blocked is False
    assert (await connection.health()).parts["blocked"] is False


async def test_blocked_is_none_rather_than_false_when_nobody_could_look() -> None:
    # "Not blocked" and "the transport cannot be asked" are different facts, and
    # a report that says false because it never looked is the one that gets
    # believed in an incident.
    class Bare:
        """A transport with no blocked state at all."""

        async def declare_queue(self, name: str, spec: QueueSpec) -> None: ...

        async def declare_exchange(self, name: str, spec: ExchangeSpec) -> None: ...

        async def bind(self, queue: str, exchange: str, routing_key: str) -> None: ...

        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            return PublishResult(confirmed=True)

        async def consume(self, queue: str, spec: Any, deliver: Any) -> Any: ...

        async def close(self) -> None: ...

    connection = Connection(Bare())

    assert connection.blocked is None
    assert connection.blocked_reason is None
    assert (await connection.health()).parts["blocked"] is None


async def test_a_blocked_connection_is_up_with_the_reason() -> None:
    # A blocked connection is the broker protecting itself from a disk or memory
    # alarm. Reporting it down gets the instance restarted into the same blocked
    # broker, having thrown away whatever it was holding — which is why Java's
    # health indicator and Go's report it up, and why this one does.
    transport = FakeTransport(blocked=True, blocked_reason="low on disk space")
    connection = Connection(transport)

    report = await connection.health()

    assert report.status is HealthStatus.UP
    assert report.healthy is True
    assert report.parts["blocked"] is True
    assert report.parts["blocked-reason"] == "low on disk space"
    assert "low on disk space" in report.detail


async def test_a_blocked_broker_is_not_probed_at_all() -> None:
    # The probe is the round trip a blocked broker will not answer: it stops
    # reading the socket. Asking first turns a deadline's worth of waiting into
    # an answer that was already known.
    class NeverAnswers(FakeTransport):
        asked = False

        async def queue_exists(self, name: str) -> bool:
            type(self).asked = True
            await asyncio.Event().wait()
            raise AssertionError("unreachable")  # pragma: no cover

    connection = Connection(NeverAnswers(blocked=True))

    report = await asyncio.wait_for(connection.health(), 1.0)

    assert report.status is HealthStatus.UP
    assert NeverAnswers.asked is False


async def test_a_health_check_that_cannot_be_answered_is_bounded_rather_than_hung() -> None:
    # A readiness endpoint that hangs is worse than one that reports unknown: it
    # takes the instance out of rotation with no report at all. So the deadline
    # is inside the library rather than in whatever called it.
    class Wedged(FakeTransport):
        async def queue_exists(self, name: str) -> bool:
            await asyncio.Event().wait()  # never returns, like a blocked broker
            raise AssertionError("unreachable")  # pragma: no cover

    report = await asyncio.wait_for(Connection(Wedged()).health(timeout=0.05), 2.0)

    assert report.status is HealthStatus.DOWN
    assert report.detail == "the broker did not answer within 0.05s"
    assert report.parts["blocked"] is False


async def test_a_broker_that_goes_quiet_because_it_blocked_is_still_up() -> None:
    # The ordinary way a block shows itself: the notification and the silence
    # arrive together, and the round trip started a moment earlier never comes
    # back. So the state is asked again before the timeout is called a failure.
    class BlocksWhileAsked(FakeTransport):
        async def queue_exists(self, name: str) -> bool:
            self.blocked = True
            await asyncio.Event().wait()
            raise AssertionError("unreachable")  # pragma: no cover

    report = await asyncio.wait_for(Connection(BlocksWhileAsked()).health(timeout=0.05), 2.0)

    assert report.status is HealthStatus.UP
    assert report.parts["blocked"] is True


async def test_a_blocked_broker_survives_being_aggregated() -> None:
    # The flaw worth checking for: a careful answer — up, for a broker applying
    # back pressure — folded into a combined report as degraded or down by
    # something that took the worst of what it was given. The connection's own
    # deadline is shorter than the aggregate's so that this report, and not the
    # aggregate giving up, is what an operator reads.
    class BlockedAndSilent(FakeTransport):
        async def queue_exists(self, name: str) -> bool:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")  # pragma: no cover

    connection = Connection(BlockedAndSilent(blocked=True))

    report = await aggregate_health(
        BrokerHealth(connection, timeout=0.05), _Always("database"), timeout=1.0
    )

    assert report.status is HealthStatus.UP
    assert report.parts["broker"].status is HealthStatus.UP
    assert report.parts["broker"].parts["blocked"] is True


async def test_a_stalled_consumer_is_still_degraded_while_the_broker_is_blocked() -> None:
    # The block is the broker's problem and clears with the alarm. A consumer of
    # ours that has stopped reading is ours, outlives it, and is not something to
    # stop reporting because something else is also wrong.
    async def handler(message: Message) -> Ack:
        return accept()

    async with running(handler) as (transport, _, connection):
        consumer = connection.consumers[0]
        for _ in consumer._workers:
            consumer._work.put_nowait(None)
        await asyncio.gather(*consumer._workers)
        transport.blocked = True

        report = await connection.health()

    assert report.status is HealthStatus.DEGRADED
    assert report.parts["blocked"] is True
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


async def test_a_publish_is_timed_as_well_as_counted() -> None:
    transport = FakeTransport()
    metrics = Metrics()
    connection = Connection(transport, observer=metrics)

    await connection.publisher("orders-events", "order.placed").send({"id": "7"})

    labels = {
        "exchange": "orders-events",
        "routing.key": "order.placed",
        "outcome": OUTCOME_CONFIRMED,
    }
    # The same labels as the total beside it, deliberately: a dashboard dividing
    # one into the other needs both series cut the same way.
    assert metrics.counts[metric_key(METRIC_PUBLISH_TOTAL, labels)] == 1
    assert metrics.durations[metric_key(METRIC_PUBLISH_DURATION, labels)].count == 1


async def test_a_publish_that_fails_is_timed_too() -> None:
    # The slow publishes are the interesting ones, and a timing that covered
    # only the successes would drop exactly those.
    transport = RefusingTransport()
    metrics = Metrics()
    connection = Connection(transport, observer=metrics)

    with pytest.raises(RuntimeError):
        await connection.publisher(routing_key="orders.new").send({"id": "7"})

    labels = {"exchange": "", "routing.key": "orders.new", "outcome": "failed"}
    assert metrics.counts[metric_key(METRIC_PUBLISH_TOTAL, labels)] == 1
    assert metrics.durations[metric_key(METRIC_PUBLISH_DURATION, labels)].count == 1


async def test_an_unroutable_publish_is_timed_too() -> None:
    transport = FakeTransport()
    metrics = Metrics()
    connection = Connection(transport, observer=metrics)

    with pytest.raises(PublishError):
        await connection.publisher(routing_key="nowhere", mandatory=True).send({"id": "7"})

    labels = {"exchange": "", "routing.key": "nowhere", "outcome": OUTCOME_UNROUTABLE}
    assert metrics.counts[metric_key(METRIC_PUBLISH_TOTAL, labels)] == 1
    assert metrics.durations[metric_key(METRIC_PUBLISH_DURATION, labels)].count == 1


async def test_which_attempt_a_delivery_was_on_is_recorded() -> None:
    # A rising distribution is a dependency in trouble, and it says so before
    # anything else does: the dead letters only move once the attempts run out.
    async def handle(message: Message) -> Ack:
        return accept()

    async with running(handle) as (transport, metrics, _):
        await transport.deliver(QUEUE, b"{}", headers=wire())
        await transport.deliver(
            QUEUE, b"{}", headers=Envelope(attempt=3).to_headers(routing_key=QUEUE)
        )

    summary = metrics.durations[metric_key(METRIC_CONSUME_ATTEMPTS, {"queue": QUEUE})]
    assert summary.count == 2
    assert (summary.fastest, summary.slowest) == (1.0, 3.0)
    assert summary.total == 4.0


async def test_the_attempt_is_recorded_even_when_the_body_will_not_decode() -> None:
    # Recorded on arrival rather than at the settlement, because it is a fact
    # about the delivery that is true before the handler runs — and this
    # delivery never reaches one.
    async def handle(message: Message) -> Ack:
        raise AssertionError("nothing decodable arrived")

    async with running(handle) as (transport, metrics, _):
        await transport.deliver(QUEUE, b"not json at all", headers=wire())

    assert metrics.durations[metric_key(METRIC_CONSUME_ATTEMPTS, {"queue": QUEUE})].count == 1
