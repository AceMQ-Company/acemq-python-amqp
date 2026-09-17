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

"""The adapter that puts the numbers into somebody else's registry.

Skipped rather than failed when prometheus-client is absent. It is an extra on
purpose: the library depends on no metrics client, and a laptop that has not
installed one should still get a green suite.
"""

from __future__ import annotations

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    METRIC_CONSUME_ATTEMPTS,
    METRIC_CONSUME_DURATION,
    METRIC_CONSUME_IN_FLIGHT,
    METRIC_PIPELINE_RUN_DURATION,
    METRIC_PIPELINE_RUN_TOTAL,
    METRIC_PUBLISH_DURATION,
    METRIC_PUBLISH_TOTAL,
    METRIC_REQUEST_DURATION,
    METRIC_REQUEST_TOTAL,
    AceMQError,
    Connection,
    Observer,
)
from acemq_amqp.topology import Topology

prometheus_client = pytest.importorskip(
    "prometheus_client", reason='needs pip install "acemq-amqp[prometheus]"'
)

from acemq_amqp.prometheus import PrometheusObserver  # noqa: E402

QUEUE = "orders.new"


@pytest.fixture
def registry() -> object:
    """A registry of this test's own, so nothing collides with the default one."""
    return prometheus_client.CollectorRegistry()


def rendered(registry: object) -> str:
    body: bytes = prometheus_client.generate_latest(registry)
    return body.decode()


def test_it_is_an_observer(registry: object) -> None:
    assert isinstance(PrometheusObserver(registry), Observer)


def test_a_counter_reaches_the_registry_with_its_labels(registry: object) -> None:
    observer = PrometheusObserver(registry)

    observer.count(METRIC_PUBLISH_TOTAL, 2, {"exchange": "events", "key": "order.placed"})

    body = rendered(registry)
    # ``acemq.publish.total`` and not ``acemq_publish_total_total``: the client
    # strips the suffix it is about to add back, so the name a dashboard queries
    # is the AceMQ one with the dots swapped.
    assert 'acemq_publish_total{exchange="events",key="order.placed"} 2.0' in body


def test_a_gauge_and_a_histogram_reach_it_too(registry: object) -> None:
    observer = PrometheusObserver(registry)

    observer.gauge(METRIC_CONSUME_IN_FLIGHT, 3, {"queue": QUEUE})
    observer.observe(METRIC_CONSUME_DURATION, 0.25, {"queue": QUEUE})

    body = rendered(registry)
    assert 'acemq_consume_in_flight{queue="orders.new"} 3.0' in body
    assert 'acemq_consume_duration_count{queue="orders.new"} 1.0' in body
    assert 'acemq_consume_duration_sum{queue="orders.new"} 0.25' in body


def test_the_collector_is_made_once_however_many_messages_go_through(
    registry: object,
) -> None:
    # Prometheus refuses a collector registered twice, so a second message must
    # find the first one rather than build another.
    observer = PrometheusObserver(registry)

    for _ in range(3):
        observer.count(METRIC_PUBLISH_TOTAL, 1, {"exchange": "", "key": QUEUE})

    assert 'acemq_publish_total{exchange="",key="orders.new"} 3.0' in rendered(
        registry
    )


def test_a_namespace_prefixes_everything(registry: object) -> None:
    observer = PrometheusObserver(registry, namespace="shipping")

    observer.count(METRIC_PUBLISH_TOTAL, 1, {"exchange": "", "key": QUEUE})

    assert "shipping_acemq_publish_total" in rendered(registry)


def test_the_same_metric_with_different_labels_is_refused_loudly(registry: object) -> None:
    # Prometheus cannot hold both, and half the samples disappearing quietly is
    # worse than an exception naming the metric. It would be a bug in this
    # library rather than in the caller, which is what the message says.
    observer = PrometheusObserver(registry)
    observer.count(METRIC_PUBLISH_TOTAL, 1, {"exchange": "", "key": QUEUE})

    with pytest.raises(AceMQError, match="bug in acemq-amqp"):
        observer.count(METRIC_PUBLISH_TOTAL, 1, {"queue": QUEUE})


async def test_a_connection_reports_through_it(registry: object) -> None:
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport, observer=PrometheusObserver(registry))

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    # The tag is ``routing.key``, which Prometheus spells ``routing_key`` — the
    # same label Java and .NET already export, so one dashboard reads across all
    # five. Rewritten by this library rather than left to the client: a
    # prometheus-client old enough to validate label names refuses the collector
    # outright, which would take the publisher down at the first message.
    assert (
        'acemq_publish_total{exchange="",outcome="confirmed",routing_key="orders.new"} 1.0'
        in rendered(registry)
    )


def test_the_attempt_distribution_gets_buckets_that_are_not_seconds(
    registry: object,
) -> None:
    # The one thing reported through observe() that is not a number of seconds.
    # On the duration buckets a delivery on attempt 2 would be recorded as a
    # handler that took two seconds, and every attempt count would pile into the
    # sub-five-second end.
    observer = PrometheusObserver(registry)
    for attempt in (1.0, 1.0, 3.0, 7.0):
        observer.observe(METRIC_CONSUME_ATTEMPTS, attempt, {"queue": QUEUE})

    body = rendered(registry)
    assert "acemq_consume_attempts_count{" in body
    # Whole-number buckets, so the first one answers "how many got it right
    # first time" exactly rather than approximately.
    assert 'acemq_consume_attempts_bucket{le="1.0",queue="orders.new"} 2.0' in body
    assert 'acemq_consume_attempts_bucket{le="3.0",queue="orders.new"} 3.0' in body
    assert 'acemq_consume_attempts_bucket{le="10.0",queue="orders.new"} 4.0' in body
    # And no seconds-shaped bucket anywhere on it.
    assert 'acemq_consume_attempts_bucket{le="0.005"' not in body


def test_durations_keep_their_own_buckets(registry: object) -> None:
    observer = PrometheusObserver(registry)
    observer.observe(METRIC_CONSUME_DURATION, 0.02, {"queue": QUEUE})

    body = rendered(registry)
    assert 'acemq_consume_duration_bucket{le="0.025",queue="orders.new"} 1.0' in body


def test_the_help_line_says_what_each_of_the_new_names_means(registry: object) -> None:
    # A name with no HELP is a name a scraper shows as itself, which is the same
    # as no documentation at the moment somebody needs it.
    observer = PrometheusObserver(registry)
    observer.observe(METRIC_PUBLISH_DURATION, 0.01, {"exchange": "", "outcome": "confirmed"})
    observer.observe(METRIC_CONSUME_ATTEMPTS, 1.0, {"queue": QUEUE})
    observer.observe(METRIC_REQUEST_DURATION, 0.01, {"outcome": "answered"})
    observer.count(METRIC_REQUEST_TOTAL, 1, {"outcome": "answered"})
    observer.observe(METRIC_PIPELINE_RUN_DURATION, 0.01, {"pipeline": "fulfilment"})
    observer.count(METRIC_PIPELINE_RUN_TOTAL, 1, {"pipeline": "fulfilment"})

    body = rendered(registry)
    for name in (
        "acemq_publish_duration",
        "acemq_consume_attempts",
        "acemq_request_duration",
        "acemq_request_total",
        "acemq_pipeline_run_duration",
        "acemq_pipeline_run_total",
    ):
        line = next(li for li in body.splitlines() if li.startswith(f"# HELP {name} "))
        assert line != f"# HELP {name} {name}"
