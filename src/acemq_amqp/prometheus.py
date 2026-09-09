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

"""An :class:`~acemq_amqp.telemetry.Observer` that writes to a Prometheus registry.

Behind an extra, the way the broker client is::

    pip install "acemq-amqp[prometheus]"

    from acemq_amqp.prometheus import PrometheusObserver

    mq = await connect(url, observer=PrometheusObserver())

The library itself depends on nothing: it calls
:class:`~acemq_amqp.telemetry.Observer`, and this module is one implementation of
it. That is the same argument the transport already makes and wins — a service
that reads an AceMQ envelope should not be made to install an AMQP client, and a
service that publishes one should not be made to install a metrics client either.
Choosing here would put every user of this package on whichever one was picked.

For a service that wants a scrape endpoint and nothing installed at all,
:func:`~acemq_amqp.telemetry.prometheus_text` renders an in-memory
:class:`~acemq_amqp.telemetry.Metrics` in the same format using only the
standard library. This module is for the case where the numbers have to join a
registry the rest of the application is already writing to.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .errors import AceMQError
from .telemetry import (
    METRIC_CONSUME_DURATION,
    METRIC_CONSUME_IN_FLIGHT,
    METRIC_CONSUME_TOTAL,
    METRIC_DEAD_LETTERED_TOTAL,
    METRIC_OUTBOX_LAG,
    METRIC_OUTBOX_TOTAL,
    METRIC_PUBLISH_TOTAL,
    METRIC_RETRIED_TOTAL,
    METRIC_RUNG_MISSING,
    METRIC_SET_ASIDE_FAILED,
)

#: Buckets for :data:`~acemq_amqp.telemetry.METRIC_CONSUME_DURATION`, in
#: seconds.
#:
#: Reaching to a minute because a handler that talks to something slow really
#: does take that long, and a histogram whose top bucket is one second reports
#: every one of those as "over a second" and nothing more useful.
#:
#: They serve every duration this observer is given, which includes
#: :data:`~acemq_amqp.telemetry.METRIC_OUTBOX_LAG` — where a minute is not the
#: interesting end of the range, because a relay that has been down for an hour
#: is publishing hour-old records. The overflow bucket still shows it, and the
#: histogram's ``_sum`` over ``_count`` gives the mean lag exactly; a process
#: that wants the shape of a long lag as well passes its own ``buckets``.
DEFAULT_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)


class PrometheusObserver:
    """Writes what the library reports into a Prometheus registry.

    Collectors are made the first time a metric is seen rather than up front,
    because the label names are not known until then: a counter reported with a
    queue label and one reported with an exchange label are different
    collectors, and Prometheus refuses a collector registered twice.

    A metric reported with two different sets of label names is a mistake in
    this library rather than in the caller, and it is refused loudly here
    instead of quietly dropping half the samples.

    :param registry: where to register, defaulting to the process-wide one that
        ``prometheus_client.start_http_server`` serves
    :param namespace: a prefix for every metric name, for a process that already
        namespaces its metrics
    :param buckets: the histogram buckets for the handler duration
    :raises AceMQError: when prometheus-client is not installed
    """

    def __init__(
        self,
        registry: Any = None,
        *,
        namespace: str = "",
        buckets: tuple[float, ...] = DEFAULT_DURATION_BUCKETS,
    ) -> None:
        try:
            import prometheus_client
        except ImportError as missing:  # pragma: no cover - depends on the install
            raise AceMQError(
                "acemq: PrometheusObserver needs prometheus-client, which is an "
                'optional extra. Install it with pip install "acemq-amqp[prometheus]"'
            ) from missing

        self._client = prometheus_client
        self._registry = registry if registry is not None else prometheus_client.REGISTRY
        self._namespace = namespace
        self._buckets = buckets
        self._collectors: dict[str, Any] = {}
        self._label_names: dict[str, tuple[str, ...]] = {}

    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
        self._for(metric, labels, self._counter).labels(**_named(labels)).inc(delta)

    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
        self._for(metric, labels, self._gauge).labels(**_named(labels)).set(value)

    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
        self._for(metric, labels, self._histogram).labels(**_named(labels)).observe(seconds)

    def _for(self, metric: str, labels: Mapping[str, str], make: Any) -> Any:
        """The collector for a metric, made once and kept.

        Not thread-safe by lock, and it does not need to be: the worst a race
        can do is build the same collector twice, and the second registration
        raises rather than corrupting anything. Everything after the first
        message reads a dictionary.
        """
        names = tuple(sorted(_named(labels)))
        known = self._label_names.get(metric)
        if known is None:
            self._label_names[metric] = names
        elif known != names:
            raise AceMQError(
                f"acemq: {metric} was reported with labels {known} and now with "
                f"{names}; Prometheus cannot hold both, and this is a bug in "
                "acemq-amqp rather than in your code"
            )

        collector = self._collectors.get(metric)
        if collector is None:
            collector = make(metric, names)
            self._collectors[metric] = collector
        return collector

    def _counter(self, metric: str, names: tuple[str, ...]) -> Any:
        return self._client.Counter(
            _prometheus_name(metric),
            _documentation(metric),
            names,
            namespace=self._namespace,
            registry=self._registry,
        )

    def _gauge(self, metric: str, names: tuple[str, ...]) -> Any:
        return self._client.Gauge(
            _prometheus_name(metric),
            _documentation(metric),
            names,
            namespace=self._namespace,
            registry=self._registry,
        )

    def _histogram(self, metric: str, names: tuple[str, ...]) -> Any:
        return self._client.Histogram(
            _prometheus_name(metric),
            _documentation(metric),
            names,
            buckets=self._buckets,
            namespace=self._namespace,
            registry=self._registry,
        )


#: What each metric means, for the HELP line a scraper shows beside it.
_HELP = {
    METRIC_PUBLISH_TOTAL: "Publishes, by outcome",
    METRIC_CONSUME_TOTAL: "Deliveries settled, by outcome",
    METRIC_CONSUME_DURATION: "How long a delivery takes, in seconds",
    METRIC_CONSUME_IN_FLIGHT: "Messages being handled right now",
    METRIC_RETRIED_TOTAL: "Messages given another attempt",
    METRIC_DEAD_LETTERED_TOTAL: "Messages sent to a dead-letter or parking queue",
    METRIC_RUNG_MISSING: "Long retries that waited in the consumer for want of a rung queue",
    METRIC_SET_ASIDE_FAILED: "Messages that could not be moved out of the way",
    METRIC_OUTBOX_TOTAL: "Outbox records the relay has handled, by outcome",
    METRIC_OUTBOX_LAG: "How long an outbox record waited to be published, in seconds",
}


def _documentation(metric: str) -> str:
    return _HELP.get(metric, metric)


def _prometheus_name(metric: str) -> str:
    """``acemq.consume.total`` becomes ``acemq_consume_total``.

    Prometheus does not allow dots in a metric name, and the AceMQ names use
    them because Micrometer and OpenTelemetry do.
    """
    return metric.replace(".", "_")


def _named(labels: Mapping[str, str]) -> dict[str, str]:
    """The same labels under names Prometheus will accept.

    ``routing.key`` and ``message.type`` are legal tag names everywhere else in
    the family and illegal here: a Prometheus label name is
    ``[a-zA-Z_][a-zA-Z0-9_]*`` and nothing else, and prometheus-client refuses
    the collector rather than the sample — so a publish counter tagged with the
    routing key would take the whole publisher down at the first message rather
    than quietly under-report.

    Values are left exactly as they are. A routing key is a value, and its dots
    are the meaning.
    """
    return {name.replace(".", "_"): value for name, value in labels.items()}
