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

"""The OpenTelemetry adapter, asserted against spans that were really emitted.

Every test here runs a real ``TracerProvider`` with an
``InMemorySpanExporter`` and reads the finished spans back. Nothing is mocked.
That distinction matters more than usual for tracing: a test against a mock
tracer asserts that the adapter *called* something, which is true of an adapter
whose spans never end, are never parented, and never reach an exporter. The
questions worth answering — did the consumer's span end up in the publisher's
trace, is the status set on the right outcomes, is the context on the message —
can only be answered by looking at what came out.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from acemq_amqp.ack import Ack, Action, accept, reject, retry
from acemq_amqp.envelope import Envelope
from acemq_amqp.headers import TRACEPARENT, TRACESTATE
from acemq_amqp.interceptors import ConsumeContext, PublishContext
from acemq_amqp.tracing import (
    ATTR_ATTEMPT,
    ATTR_CONVERSATION_ID,
    ATTR_DESTINATION,
    ATTR_MESSAGE_ID,
    ATTR_MESSAGE_TYPE,
    ATTR_OPERATION,
    ATTR_OUTCOME,
    ATTR_ROUTING_KEY,
    ATTR_SYSTEM,
    EVENT_MESSAGE_DEAD_LETTERED,
    EVENT_MESSAGE_RETRIED,
    EVENT_OUTBOX_PUBLISH_FAILED,
    EVENT_PIPELINE_RUN_FINISHED,
    OpenTelemetryTracing,
)
from acemq_amqp.transport import PublishResult


@pytest.fixture()
def provider() -> Iterator[TracerProvider]:
    """A real SDK exporting into memory, handed to the adapter explicitly.

    Explicitly rather than by replacing the global one: the global provider can
    only be set once per process and a test that reached in and swapped it would
    leak into every test that ran after it. Passing it is also the supported way
    an application would do this, so the parameter gets exercised rather than
    assumed.
    """
    exporter = InMemorySpanExporter()
    built = TracerProvider()
    built.add_span_processor(SimpleSpanProcessor(exporter))
    built.exporter = exporter  # type: ignore[attr-defined]
    try:
        yield built
    finally:
        built.shutdown()


@pytest.fixture()
def spans(provider: TracerProvider) -> InMemorySpanExporter:
    exporter: InMemorySpanExporter = provider.exporter  # type: ignore[attr-defined]
    return exporter


@pytest.fixture()
def tracing(provider: TracerProvider) -> OpenTelemetryTracing:
    return OpenTelemetryTracing(system="rabbitmq", tracer_provider=provider)


def finished(spans: InMemorySpanExporter) -> list[Any]:
    return list(spans.get_finished_spans())


def named(spans: InMemorySpanExporter, name: str) -> Any:
    for span in finished(spans):
        if span.name == name:
            return span
    seen = [span.name for span in finished(spans)]
    raise AssertionError(f"no span called {name!r}; there were {seen}")


def publishing(**changes: Any) -> PublishContext:
    fields: dict[str, Any] = {
        "exchange": "orders",
        "routing_key": "order.placed",
        "envelope": Envelope(
            id="m-1", type="order.placed", correlation_id="c-1", attempt=1
        ),
        "payload": {"id": 1},
    }
    fields.update(changes)
    return PublishContext(**fields)


def delivering(envelope: Envelope, **changes: Any) -> ConsumeContext:
    fields: dict[str, Any] = {
        "queue": "orders.new",
        "envelope": envelope,
        "payload": {"id": 1},
        "body": b"{}",
        "content_type": "application/json",
        "routing_key": "order.placed",
        "redelivered": False,
    }
    fields.update(changes)
    return ConsumeContext(**fields)


async def _sent(_: PublishContext) -> PublishResult:
    return PublishResult(message_id="m-1", confirmed=True, routed=True)


# --------------------------------------------------------------------------
# Publishing


async def test_a_publish_produces_a_producer_span_named_after_its_destination(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    await tracing.publish_interceptor()(publishing(), _sent)

    span = named(spans, "orders publish")
    assert span.kind is SpanKind.PRODUCER
    assert span.attributes[ATTR_SYSTEM] == "rabbitmq"
    assert span.attributes[ATTR_DESTINATION] == "orders"
    assert span.attributes[ATTR_OPERATION] == "publish"
    assert span.attributes[ATTR_MESSAGE_ID] == "m-1"
    assert span.attributes[ATTR_CONVERSATION_ID] == "c-1"
    assert span.attributes[ATTR_ROUTING_KEY] == "order.placed"
    assert span.attributes[ATTR_MESSAGE_TYPE] == "order.placed"
    assert span.attributes[ATTR_OUTCOME] == "confirmed"
    assert span.status.status_code is not StatusCode.ERROR


async def test_a_publish_with_no_exchange_is_named_after_the_queue(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """Publishing to the default exchange is publishing to a queue by name.

    Naming the span ``" publish"`` — the empty exchange plus the suffix — would
    be the literal reading and would collapse every direct-to-queue publish in
    the system into one row.
    """
    await tracing.publish_interceptor()(
        publishing(exchange="", routing_key="orders.new"), _sent
    )

    assert named(spans, "orders.new publish").attributes[ATTR_DESTINATION] == "orders.new"


async def test_an_unroutable_message_is_an_error(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """A message nothing was bound to receive.

    The broker does not consider this an error — it drops the message and says
    nothing — which is exactly why the span has to.
    """

    async def nowhere(_: PublishContext) -> PublishResult:
        return PublishResult(confirmed=True, routed=False, return_reason="NO_ROUTE")

    await tracing.publish_interceptor()(publishing(), nowhere)

    span = named(spans, "orders publish")
    assert span.attributes[ATTR_OUTCOME] == "unroutable"
    assert span.status.status_code is StatusCode.ERROR


async def test_a_publish_that_raises_is_recorded_and_re_raised(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    async def broken(_: PublishContext) -> PublishResult:
        raise RuntimeError("the broker went away")

    with pytest.raises(RuntimeError, match="went away"):
        await tracing.publish_interceptor()(publishing(), broken)

    span = named(spans, "orders publish")
    assert span.attributes[ATTR_OUTCOME] == "failed"
    assert span.status.status_code is StatusCode.ERROR
    assert [event.name for event in span.events] == ["exception"]


async def test_a_publish_without_confirms_says_only_that_it_went_out(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """``published`` and not ``confirmed``. Nothing has been promised.

    The message reached a socket, which is a different claim from the broker
    taking responsibility for it, and a trace that called both "confirmed" would
    make the difference invisible in the one place somebody goes to find it.
    """

    async def unconfirmed(_: PublishContext) -> PublishResult:
        return PublishResult(confirmed=False, routed=True)

    await tracing.publish_interceptor()(publishing(), unconfirmed)

    assert named(spans, "orders publish").attributes[ATTR_OUTCOME] == "published"


# --------------------------------------------------------------------------
# The headers


async def test_the_trace_travels_in_the_w3c_headers_other_tooling_reads(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """``traceparent``, unprefixed, and in the version-00 format.

    Not ``x-acemq-traceparent``: the whole reason to use the W3C names is that a
    consumer that has never heard of this library still joins the trace.
    """
    carried: dict[str, Any] = {}

    async def capture(context: PublishContext) -> PublishResult:
        carried.update(context.envelope.headers)
        return PublishResult(confirmed=True)

    await tracing.publish_interceptor()(publishing(), capture)

    assert TRACEPARENT in carried
    assert str(carried[TRACEPARENT]).startswith("00-")
    assert not any(name.startswith("x-acemq-") for name in carried)


async def test_the_context_on_the_message_is_the_publish_span_itself(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """Injected inside the span, not before it.

    Carrying the *parent* instead would make the publish and the process
    siblings rather than parent and child — a trace that looks joined up and
    cannot say which publish caused which delivery when a service publishes
    more than one message.
    """
    carried: dict[str, Any] = {}

    async def capture(context: PublishContext) -> PublishResult:
        carried.update(context.envelope.headers)
        return PublishResult(confirmed=True)

    await tracing.publish_interceptor()(publishing(), capture)

    span = named(spans, "orders publish")
    _, trace_id, span_id, _ = str(carried[TRACEPARENT]).split("-")
    assert int(trace_id, 16) == span.context.trace_id
    assert int(span_id, 16) == span.context.span_id


def test_propagation_headers_are_a_fresh_carrier_each_time(
    tracing: OpenTelemetryTracing, provider: TracerProvider
) -> None:
    """Nothing already on the message can be overwritten, and nothing from a
    previous message left behind."""
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("outer"):
        first = tracing.propagation_headers()
        second = tracing.propagation_headers()

    assert first == second
    assert first is not second
    assert TRACEPARENT in first


def test_propagation_headers_are_empty_when_nothing_is_being_traced(
    tracing: OpenTelemetryTracing,
) -> None:
    assert tracing.propagation_headers() == {}


# --------------------------------------------------------------------------
# Consuming, and the join


async def test_a_handler_span_is_a_child_of_the_publish_that_caused_it(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """The assertion the whole module exists for.

    Two spans, produced by two calls with nothing in common but the headers on
    the message, and they end up in one trace with the right one on top.
    """
    carried: dict[str, Any] = {}

    async def capture(context: PublishContext) -> PublishResult:
        carried.update(context.envelope.headers)
        return PublishResult(confirmed=True)

    await tracing.publish_interceptor()(publishing(), capture)

    async def handle(_: ConsumeContext) -> Ack:
        return accept()

    delivered = delivering(Envelope(id="m-1", type="order.placed", headers=dict(carried)))
    await tracing.consume_interceptor()(delivered, handle)

    publish = named(spans, "orders publish")
    process = named(spans, "orders.new process")

    assert process.parent.span_id == publish.context.span_id
    assert process.context.trace_id == publish.context.trace_id
    assert process.kind is SpanKind.CONSUMER
    assert process.attributes[ATTR_OPERATION] == "process"
    assert process.attributes[ATTR_OUTCOME] == "acked"


async def test_the_parent_comes_from_the_message_and_not_from_what_is_current(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter, provider: TracerProvider
) -> None:
    """The distinction that makes this correct rather than merely plausible.

    An unrelated span is current while the delivery is handled — which is
    exactly what a consumer loop looks like in practice, where the ambient
    context belongs to the connection or to the *previous* message. The handler
    span must ignore it and join the message's trace instead.
    """
    carried: dict[str, Any] = {}

    async def capture(context: PublishContext) -> PublishResult:
        carried.update(context.envelope.headers)
        return PublishResult(confirmed=True)

    await tracing.publish_interceptor()(publishing(), capture)
    publish = named(spans, "orders publish")

    async def handle(_: ConsumeContext) -> Ack:
        return accept()

    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("something else entirely") as ambient:
        unrelated = ambient.get_span_context()
        delivered = delivering(Envelope(id="m-1", headers=dict(carried)))
        await tracing.consume_interceptor()(delivered, handle)

    process = named(spans, "orders.new process")
    assert process.context.trace_id == publish.context.trace_id
    assert process.context.trace_id != unrelated.trace_id
    assert process.parent.span_id == publish.context.span_id


async def test_a_message_with_no_trace_context_starts_its_own_trace(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter, provider: TracerProvider
) -> None:
    """Published by something that does not propagate, so it is a root.

    Not a child of whatever this process happened to be doing, which would
    attach an unrelated message to an unrelated trace and is worse than having
    no link at all.
    """

    async def handle(_: ConsumeContext) -> Ack:
        return accept()

    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("unrelated work") as ambient:
        unrelated = ambient.get_span_context()
        await tracing.consume_interceptor()(delivering(Envelope(id="m-9")), handle)

    process = named(spans, "orders.new process")
    assert process.parent is None
    assert process.context.trace_id != unrelated.trace_id


async def test_tracestate_travels_with_traceparent(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """The vendor half of the context, which is dropped by anything that only
    knows about ``traceparent``."""
    headers = {
        TRACEPARENT: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        TRACESTATE: "vendor=opaque",
    }

    async def handle(_: ConsumeContext) -> Ack:
        return accept()

    await tracing.consume_interceptor()(delivering(Envelope(id="m-1", headers=headers)), handle)

    process = named(spans, "orders.new process")
    assert format(process.context.trace_id, "032x") == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert format(process.parent.span_id, "016x") == "00f067aa0ba902b7"
    assert process.context.trace_state.get("vendor") == "opaque"


async def test_the_attempt_is_on_the_handler_span(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    async def handle(_: ConsumeContext) -> Ack:
        return accept()

    await tracing.consume_interceptor()(delivering(Envelope(id="m-1", attempt=4)), handle)

    assert named(spans, "orders.new process").attributes[ATTR_ATTEMPT] == 4


@pytest.mark.parametrize(
    ("decision", "outcome", "is_error"),
    [
        (accept, "acked", False),
        (retry, "retried", False),
        (reject, "rejected", False),
    ],
)
async def test_what_the_handler_decided_becomes_the_outcome(
    tracing: OpenTelemetryTracing,
    spans: InMemorySpanExporter,
    decision: Any,
    outcome: str,
    is_error: bool,
) -> None:
    """And a retry is not an error.

    A retry is the system working — the message will be tried again and usually
    succeeds — and colouring the trace red for it means a wall of red traces
    that turned out fine, which is how people learn to ignore the colour. A
    rejection is a decision somebody made deliberately.
    """

    async def handle(_: ConsumeContext) -> Ack:
        result: Ack = decision()
        return result

    await tracing.consume_interceptor()(delivering(Envelope(id="m-1")), handle)

    span = named(spans, "orders.new process")
    assert span.attributes[ATTR_OUTCOME] == outcome
    assert (span.status.status_code is StatusCode.ERROR) is is_error


async def test_a_handler_that_raises_is_recorded_and_re_raised(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    async def broken(_: ConsumeContext) -> Ack:
        raise ValueError("downstream unavailable")

    with pytest.raises(ValueError, match="downstream"):
        await tracing.consume_interceptor()(delivering(Envelope(id="m-1")), broken)

    span = named(spans, "orders.new process")
    assert span.attributes[ATTR_OUTCOME] == "failed"
    assert span.status.status_code is StatusCode.ERROR
    assert "downstream unavailable" in str(span.events[0].attributes["exception.message"])


async def test_the_reason_for_a_retry_is_recorded_without_the_span_failing(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """A handler that asked for another go and said why.

    The exception is on the span, because it is the useful part; the status is
    not error, because the message has not failed yet.
    """

    async def handle(_: ConsumeContext) -> Ack:
        return Ack(Action.RETRY, TimeoutError("no answer from pricing"))

    await tracing.consume_interceptor()(delivering(Envelope(id="m-1")), handle)

    span = named(spans, "orders.new process")
    assert span.attributes[ATTR_OUTCOME] == "retried"
    assert span.status.status_code is not StatusCode.ERROR
    assert [event.name for event in span.events] == ["exception"]


# --------------------------------------------------------------------------
# Requests


async def test_a_request_is_a_client_span_because_it_waits_for_an_answer(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """CLIENT rather than PRODUCER, and the reason is the duration.

    A slow publish is a slow broker; a slow request is a slow responder. A
    backend that knows the kind shows them apart instead of averaging one into
    the other.
    """
    envelope = Envelope(id="r-1", type="price.requested", correlation_id="c-9")

    with tracing.request_span("pricing", envelope):
        await asyncio.sleep(0)

    span = named(spans, "pricing request")
    assert span.kind is SpanKind.CLIENT
    assert span.attributes[ATTR_OPERATION] == "request"
    assert span.attributes[ATTR_DESTINATION] == "pricing"
    assert span.attributes[ATTR_MESSAGE_ID] == "r-1"
    assert span.attributes[ATTR_CONVERSATION_ID] == "c-9"


async def test_a_request_that_times_out_is_an_error(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    with pytest.raises(TimeoutError), tracing.request_span("pricing", Envelope(id="r-1")):
        raise TimeoutError("no answer in 5s")

    span = named(spans, "pricing request")
    assert span.attributes[ATTR_OUTCOME] == "failed"
    assert span.status.status_code is StatusCode.ERROR


# --------------------------------------------------------------------------
# Events rather than spans


def test_the_four_events_land_on_the_span_that_was_already_running(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter, provider: TracerProvider
) -> None:
    """Not spans of their own.

    A zero-length span at the end of a trace adds a row to the waterfall and no
    information. An event is attached to the span that was doing the work, which
    is where whoever is reading the trace is already looking — so this asserts
    both that the events are there and that they did *not* produce spans.
    """
    envelope = Envelope(id="m-1", attempt=3)
    tracer = provider.get_tracer("test")

    with tracer.start_as_current_span("orders.new process"):
        tracing.outbox_publish_failed("orders", "the broker refused it")
        tracing.pipeline_run_finished("enrichment", "geocode", "failed", 1200)
        tracing.message_retried("orders.new", envelope, 5000)
        tracing.message_dead_lettered("orders.new", envelope, "out of attempts")

    assert len(finished(spans)) == 1
    span = finished(spans)[0]
    assert [event.name for event in span.events] == [
        EVENT_OUTBOX_PUBLISH_FAILED,
        EVENT_PIPELINE_RUN_FINISHED,
        EVENT_MESSAGE_RETRIED,
        EVENT_MESSAGE_DEAD_LETTERED,
    ]
    retried = span.events[2]
    assert retried.attributes["messaging.acemq.retry_delay_ms"] == 5000
    assert retried.attributes[ATTR_ATTEMPT] == 3
    assert span.events[3].attributes["messaging.acemq.reason"] == "out of attempts"


def test_an_event_with_no_span_to_land_on_is_dropped_rather_than_raising(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter
) -> None:
    """A relay running outside any trace still runs."""
    tracing.outbox_publish_failed("orders", "nothing is listening")
    tracing.message_retried("orders.new", Envelope(id="m-1"), 100)

    assert finished(spans) == []


def test_the_outbox_lag_is_an_attribute_rather_than_an_event(
    tracing: OpenTelemetryTracing, spans: InMemorySpanExporter, provider: TracerProvider
) -> None:
    """It measures the publish that is happening, not a thing that happened
    during it."""
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("orders publish"):
        tracing.outbox_published("orders", 4200)

    span = finished(spans)[0]
    assert span.attributes["messaging.acemq.outbox_lag_ms"] == 4200
    assert span.events == ()


# --------------------------------------------------------------------------
# Wiring


def test_installing_registers_both_interceptors() -> None:
    registered: dict[str, Any] = {}

    class Recording:
        def intercept_publish(self, interceptor: Any) -> Recording:
            registered["publish"] = interceptor
            return self

        def intercept_consume(self, interceptor: Any) -> Recording:
            registered["consume"] = interceptor
            return self

    connection = Recording()

    tracing = OpenTelemetryTracing()
    returned: Any = tracing.install(connection)  # type: ignore[arg-type]

    assert returned is connection
    assert set(registered) == {"publish", "consume"}


def test_the_system_reported_can_be_changed() -> None:
    assert repr(OpenTelemetryTracing(system="in-memory")) == (
        "OpenTelemetryTracing(system='in-memory')"
    )
