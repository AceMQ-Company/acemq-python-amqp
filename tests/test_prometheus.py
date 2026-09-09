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
    METRIC_HANDLER_DURATION,
    METRIC_IN_FLIGHT,
    METRIC_PUBLISHED,
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

    observer.count(METRIC_PUBLISHED, 2, {"exchange": "events", "key": "order.placed"})

    body = rendered(registry)
    assert 'acemq_messages_published_total{exchange="events",key="order.placed"} 2.0' in body


def test_a_gauge_and_a_histogram_reach_it_too(registry: object) -> None:
    observer = PrometheusObserver(registry)

    observer.gauge(METRIC_IN_FLIGHT, 3, {"queue": QUEUE})
    observer.observe(METRIC_HANDLER_DURATION, 0.25, {"queue": QUEUE})

    body = rendered(registry)
    assert 'acemq_messages_in_flight{queue="orders.new"} 3.0' in body
    assert 'acemq_handler_duration_count{queue="orders.new"} 1.0' in body
    assert 'acemq_handler_duration_sum{queue="orders.new"} 0.25' in body


def test_the_collector_is_made_once_however_many_messages_go_through(
    registry: object,
) -> None:
    # Prometheus refuses a collector registered twice, so a second message must
    # find the first one rather than build another.
    observer = PrometheusObserver(registry)

    for _ in range(3):
        observer.count(METRIC_PUBLISHED, 1, {"exchange": "", "key": QUEUE})

    assert 'acemq_messages_published_total{exchange="",key="orders.new"} 3.0' in rendered(
        registry
    )


def test_a_namespace_prefixes_everything(registry: object) -> None:
    observer = PrometheusObserver(registry, namespace="shipping")

    observer.count(METRIC_PUBLISHED, 1, {"exchange": "", "key": QUEUE})

    assert "shipping_acemq_messages_published_total" in rendered(registry)


def test_the_same_metric_with_different_labels_is_refused_loudly(registry: object) -> None:
    # Prometheus cannot hold both, and half the samples disappearing quietly is
    # worse than an exception naming the metric. It would be a bug in this
    # library rather than in the caller, which is what the message says.
    observer = PrometheusObserver(registry)
    observer.count(METRIC_PUBLISHED, 1, {"exchange": "", "key": QUEUE})

    with pytest.raises(AceMQError, match="bug in acemq-amqp"):
        observer.count(METRIC_PUBLISHED, 1, {"queue": QUEUE})


async def test_a_connection_reports_through_it(registry: object) -> None:
    transport = FakeTransport()
    await Topology().queue(QUEUE).apply(transport)
    mq = Connection(transport, observer=PrometheusObserver(registry))

    await mq.publisher(routing_key=QUEUE).send({"id": "1"})

    # The tag is ``routing.key``, which Prometheus spells ``routing_key`` — the
    # same label Java and .NET already export, so one dashboard reads across all
    # five.
    assert (
        'acemq_messages_published_total{exchange="",routing_key="orders.new"} 1.0'
        in rendered(registry)
    )
