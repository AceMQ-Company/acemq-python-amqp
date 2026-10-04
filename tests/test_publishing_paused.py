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

"""A publish refused for back pressure is told apart from one that failed.

Go, .NET and Java each have a type that means "the broker declined, nothing was
sent, try again later". Without one a caller counting outcomes has to call a
blocked broker a lost message, which is the wrong response to the wrong event.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from fake_transport import FakeTransport
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from acemq_amqp import (
    METRIC_PUBLISH_DURATION,
    METRIC_PUBLISH_TOTAL,
    Metrics,
    Outbound,
    PublishError,
    PublishingPausedError,
    PublishResult,
)
from acemq_amqp.connection import Connection
from acemq_amqp.telemetry import OUTCOME_FAILED, OUTCOME_REFUSED, metric_key
from acemq_amqp.tracing import ATTR_OUTCOME, OpenTelemetryTracing


async def test_a_blocked_connection_refuses_the_publish_and_sends_nothing() -> None:
    transport = FakeTransport()
    transport.blocked = True
    transport.blocked_reason = "low on disk space"
    connection = Connection(transport)

    with pytest.raises(PublishingPausedError) as refused:
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    # Still a PublishError, so every existing ``except PublishError`` keeps working.
    assert isinstance(refused.value, PublishError)
    assert refused.value.routing_key == "orders.new"
    assert "low on disk space" in str(refused.value)
    # Declined, not lost: nothing reached the wire.
    assert transport.sent == []


async def test_a_blocked_connection_without_a_reason_still_refuses() -> None:
    # The RabbitMQ transport cannot recover the broker's reason, so this is the
    # case production actually sees.
    transport = FakeTransport()
    transport.blocked = True
    connection = Connection(transport)

    with pytest.raises(PublishingPausedError, match="blocked this connection"):
        await connection.publish_raw("", "q", Outbound(body=b"x", message_id="m-1"))

    assert transport.sent == []


async def test_a_connection_that_cannot_say_whether_it_is_blocked_still_publishes() -> None:
    # ``None`` is "could not look", not "blocked": refusing on it would stop every
    # publish on a client that has moved the state out of reach.
    transport = FakeTransport()
    transport.blocked = None
    connection = Connection(transport)

    await connection.publisher(routing_key="orders.new").send({"id": "1"})

    assert len(transport.sent) == 1


async def test_publishing_resumes_once_the_broker_unblocks() -> None:
    transport = FakeTransport()
    transport.blocked = True
    connection = Connection(transport)
    publisher = connection.publisher(routing_key="orders.new")

    with pytest.raises(PublishingPausedError):
        await publisher.send({"id": "1"})
    transport.blocked = False
    await publisher.send({"id": "1"})

    assert len(transport.sent) == 1


class NackingTransport(FakeTransport):
    """A broker that takes the message and declines responsibility for it."""

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        await super().publish(exchange, routing_key, message)
        return PublishResult(
            message_id=message.message_id, confirmed=False, routed=True, return_reason=""
        )


async def test_a_nack_is_a_failure_not_a_refusal() -> None:
    connection = Connection(NackingTransport())

    with pytest.raises(PublishError) as failed:
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    assert not isinstance(failed.value, PublishingPausedError)


async def test_an_unroutable_message_is_a_failure_not_a_refusal() -> None:
    connection = Connection(FakeTransport())

    with pytest.raises(PublishError) as failed:
        await connection.publisher(routing_key="nobody.listens", mandatory=True).send({})

    assert failed.value.unroutable is True
    assert not isinstance(failed.value, PublishingPausedError)


async def test_running_out_of_permits_is_a_failure_not_a_refusal() -> None:
    # The same as Java, .NET and Go, all of which report this as a transport
    # failure: a broker that stopped confirming may be dead rather than busy.
    class Silent(FakeTransport):
        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    connection = Connection(
        Silent(), max_outstanding_publishes=1, confirm_timeout=timedelta(milliseconds=20)
    )
    publisher = connection.publisher(routing_key="orders.new")

    first = asyncio.create_task(publisher.send({"id": "1"}))
    await asyncio.sleep(0)
    with pytest.raises(PublishError) as failed:
        await publisher.send({"id": "2"})
    first.cancel()

    assert not isinstance(failed.value, PublishingPausedError)


# -------------------------------------------------------------- telemetry
#
# ``refused`` is the outcome for a publish the library declined before writing
# anything; ``failed`` keeps meaning "may have been lost". The same contract in
# all five libraries, so a dashboard can alert on ``failed`` without paging
# for a broker alarm that lost nothing.

WHERE = {"exchange": "", "routing.key": "orders.new"}


def test_the_refused_outcome_is_the_word_the_other_libraries_tag_with() -> None:
    assert OUTCOME_REFUSED == "refused"


async def test_a_refused_publish_is_counted_as_refused_not_failed() -> None:
    transport = FakeTransport()
    transport.blocked = True
    metrics = Metrics()
    connection = Connection(transport, observer=metrics)

    with pytest.raises(PublishingPausedError):
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    refused = {**WHERE, "outcome": OUTCOME_REFUSED}
    assert metrics.counts[metric_key(METRIC_PUBLISH_TOTAL, refused)] == 1
    assert metrics.durations[metric_key(METRIC_PUBLISH_DURATION, refused)].count == 1
    failed = metric_key(METRIC_PUBLISH_TOTAL, {**WHERE, "outcome": OUTCOME_FAILED})
    assert failed not in metrics.counts


async def test_a_nack_is_still_counted_as_failed() -> None:
    metrics = Metrics()
    connection = Connection(NackingTransport(), observer=metrics)

    with pytest.raises(PublishError):
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    failed = {**WHERE, "outcome": OUTCOME_FAILED}
    assert metrics.counts[metric_key(METRIC_PUBLISH_TOTAL, failed)] == 1


async def test_a_refused_publish_span_says_refused() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracing = OpenTelemetryTracing(tracer_provider=provider)
    transport = FakeTransport()
    transport.blocked = True
    connection = Connection(transport)
    connection.intercept_publish(tracing.publish_interceptor())

    with pytest.raises(PublishingPausedError):
        await connection.publisher(routing_key="orders.new").send({"id": "1"})

    (span,) = exporter.get_finished_spans()
    assert span.attributes is not None
    assert span.attributes[ATTR_OUTCOME] == OUTCOME_REFUSED
    # Still red: the caller's publish did not happen and it raised.
    assert span.status.status_code is StatusCode.ERROR
