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

"""OpenTelemetry spans for publishing and consuming::

    pip install "acemq-amqp[opentelemetry]"

    from acemq_amqp.tracing import OpenTelemetryTracing

    mq = await connect(url)
    OpenTelemetryTracing().install(mq)

:mod:`acemq_amqp.telemetry` answers *how much*; this answers *what happened to
this message*. They are different questions and neither substitutes for the
other: a counter says a thousand messages were dead-lettered, and a trace says
this one was published by the checkout service, retried twice over four minutes
and given up on — which is the question somebody is actually holding when they
open a dashboard.

**The join across processes is the whole point.** A consumer's span is a child of
the *publish* that caused it, taken from the message's own headers rather than
from whatever context happened to be current when the delivery arrived. Those
are different traces, minutes and machines apart, and joining them is the one
thing a messaging system needs from tracing that an HTTP client does not.

The headers
-----------

``traceparent`` and ``tracestate``, deliberately **not** ``x-acemq-`` prefixed.
They are W3C names that every other piece of tracing tooling already recognises,
so a message this library publishes is readable by a consumer written with no
knowledge of AceMQ at all, and one published by such a consumer joins up here.
Prefixing them would have been tidy and would have made the context private to
this library, which is the opposite of what a propagation format is for. Java, Go
and .NET write the same two names.

Spans
-----

============================  ==========  ==========================
name                          kind        when
============================  ==========  ==========================
``<destination> publish``     PRODUCER    a message goes out
``<queue> process``           CONSUMER    a handler runs
``<destination> request``     CLIENT      a request waits for a reply
============================  ==========  ==========================

``request`` is CLIENT rather than PRODUCER because that span waits for an answer.
Its duration means something different as a result — a slow publish is a slow
broker, and a slow request is a slow *responder* — and the kind is what makes a
tracing backend show them apart rather than averaging one into the other.

Events, not spans
-----------------

``outbox.publish_failed``, ``pipeline.run_finished``, ``message.retried`` and
``message.dead_lettered`` are recorded as events on whatever span is current. A
zero-length span at the end of a trace adds a row to the waterfall and no
information; an event is attached to the span that was actually doing the work,
which is where somebody reading the trace is already looking.

The last two are emitted by the consumer itself, on the delivery's own
``process`` span, at the moment it decides. That is later than it sounds: what
the handler answered is not what happens to the message, because a handler
asking for another attempt when there are none left is dead-lettered instead. So
the process span stays open past the handler and takes its outcome from the
consumer's decision — which is why a message that ran out of attempts reads
``dead_lettered`` rather than ``retried``, and why searching a backend for
dead letters finds them.

The dependency
--------------

The ``opentelemetry-api`` package, and not the SDK. That is the package a library
is supposed to depend on: without an SDK installed and configured by the
application, the API's no-op implementation is what runs, so importing this can
never start exporting anything nobody asked for. The application chooses the SDK,
the exporter and the sampler; this module only describes what happened.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from .ack import (
    OUTCOME_ACKED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_REJECTED,
    OUTCOME_RETRIED,
    Ack,
    Action,
    Settlement,
)
from .envelope import Envelope
from .errors import AceMQError
from .headers import TRACEPARENT, TRACESTATE
from .interceptors import (
    ConsumeContext,
    ConsumeInterceptor,
    ConsumeNext,
    PublishContext,
    PublishInterceptor,
    PublishNext,
    SettlementListener,
)
from .transport import PublishResult

if TYPE_CHECKING:  # pragma: no cover - imported for types alone
    from .connection import Connection

#: What is being traced. ``rabbitmq`` by default, and the same value the other
#: libraries put here, so one dashboard reads across all four.
DEFAULT_SYSTEM = "rabbitmq"

#: The name this library's spans are recorded under. The reverse-domain name
#: Java, Ruby, Go and .NET all register, and not a Python module path: spans
#: from five libraries are meant to group under one instrumentation scope in a
#: trace backend, and they do not if one of them answers to something else.
INSTRUMENTATION_NAME = "org.acemq.amqp"

#: Appended to the destination, with a space. ``orders publish``.
SPAN_PUBLISH_SUFFIX = " publish"

#: Appended to the queue. ``orders.new process``.
SPAN_PROCESS_SUFFIX = " process"

#: Appended to the destination. ``pricing request``.
SPAN_REQUEST_SUFFIX = " request"

#: The OpenTelemetry semantic conventions for messaging, plus three of this
#: library's own under an ``acemq`` namespace — attempt, message type and
#: outcome have no standard names and are the three things most often wanted.
ATTR_SYSTEM = "messaging.system"
ATTR_DESTINATION = "messaging.destination.name"
ATTR_OPERATION = "messaging.operation"
ATTR_MESSAGE_ID = "messaging.message.id"
ATTR_CONVERSATION_ID = "messaging.message.conversation_id"
ATTR_ROUTING_KEY = "messaging.rabbitmq.destination.routing_key"
ATTR_MESSAGE_TYPE = "messaging.acemq.message_type"
ATTR_ATTEMPT = "messaging.acemq.attempt"
ATTR_OUTCOME = "messaging.acemq.outcome"
ATTR_REASON = "messaging.acemq.reason"
ATTR_RETRY_DELAY = "messaging.acemq.retry_delay_ms"
ATTR_RUN_AGE = "messaging.acemq.run_age_ms"
ATTR_OUTBOX_LAG = "messaging.acemq.outbox_lag_ms"

#: The ``pipeline.run_finished`` event's own keys, and bare rather than
#: namespaced. They are the metric tag names — Java, Ruby and Go all write
#: ``pipeline``, ``step`` and ``outcome`` on this event — so a query that finds
#: the event in one library finds it in all five. The span *attribute* for an
#: outcome stays :data:`ATTR_OUTCOME`, which is a different thing in a different
#: place and keeps its namespace.
ATTR_PIPELINE = "pipeline"
ATTR_STEP = "step"
ATTR_PIPELINE_OUTCOME = "outcome"

#: A message that reached no queue at all.
OUTCOME_UNROUTABLE = "unroutable"

#: The broker took responsibility for the message.
OUTCOME_CONFIRMED = "confirmed"

#: It went out, and nothing promised anything about it. Publisher confirms were
#: not on.
OUTCOME_PUBLISHED = "published"

#: The publish or the handler raised.
OUTCOME_FAILED = "failed"

# The four a delivery can end in — ``acked``, ``retried``, ``rejected`` and
# ``dead_lettered`` — are defined in :mod:`acemq_amqp.ack` and re-exported here,
# because the consumer decides them and this only writes them down. They keep
# their names: ``tracing.OUTCOME_DEAD_LETTERED`` is where people look for them.

#: The outcomes that make a span an error, and only these.
#:
#: ``retried`` and ``rejected`` are deliberately absent. A retry is the system
#: working — the message will be tried again and very often succeeds — and
#: marking it an error paints a trace red for something that turned out fine. A
#: rejection is a decision the handler made on purpose. What is left is the three
#: that mean a message did not get where it was going.
ERROR_OUTCOMES = frozenset({OUTCOME_UNROUTABLE, OUTCOME_FAILED, OUTCOME_DEAD_LETTERED})

#: The events recorded on the current span rather than as spans of their own.
EVENT_OUTBOX_PUBLISH_FAILED = "outbox.publish_failed"
EVENT_PIPELINE_RUN_FINISHED = "pipeline.run_finished"
EVENT_MESSAGE_RETRIED = "message.retried"
EVENT_MESSAGE_DEAD_LETTERED = "message.dead_lettered"

_ACK_OUTCOMES = {
    Action.ACCEPT: OUTCOME_ACKED,
    Action.RETRY: OUTCOME_RETRIED,
    Action.REJECT: OUTCOME_REJECTED,
}


class OpenTelemetryTracing:
    """Emits spans for what this library does, and joins them up across
    processes.

    :param system: what to report as ``messaging.system``
    :param tracer_provider: where to get the tracer, or ``None`` for the global
        one the application configured
    :param propagator: how to read and write trace context, or ``None`` for the
        global propagator — which is W3C ``traceparent``/``tracestate`` unless
        the application changed it
    :raises AceMQError: when ``opentelemetry-api`` is not installed
    """

    def __init__(
        self,
        *,
        system: str = DEFAULT_SYSTEM,
        tracer_provider: Any | None = None,
        propagator: Any | None = None,
    ) -> None:
        try:
            from opentelemetry import propagate, trace
        except ImportError as missing:  # pragma: no cover - depends on the install
            raise AceMQError(
                "acemq: OpenTelemetryTracing needs opentelemetry-api, which is an optional "
                'extra. Install it with pip install "acemq-amqp[opentelemetry]"'
            ) from missing

        self._trace: Any = trace
        self._propagate: Any = propagate
        self._propagator = propagator
        self._system = system
        self._tracer: Any = trace.get_tracer(
            INSTRUMENTATION_NAME, tracer_provider=tracer_provider
        )

    # ----------------------------------------------------------------- wiring

    def install(self, connection: Connection) -> Connection:
        """Registers both interceptors on a connection.

        :param connection: what to trace
        :returns: the connection, so this reads as one expression
        """
        connection.intercept_publish(self.publish_interceptor())
        connection.intercept_consume(self.consume_interceptor())
        return connection

    def publish_interceptor(self) -> PublishInterceptor:
        """A span around every publish, and the trace context into the message.

        :returns: an interceptor for
            :meth:`~acemq_amqp.connection.Connection.intercept_publish`
        """

        async def traced(context: PublishContext, send: PublishNext) -> PublishResult:
            destination = context.exchange or context.routing_key
            with self._tracer.start_as_current_span(
                destination + SPAN_PUBLISH_SUFFIX,
                kind=self._trace.SpanKind.PRODUCER,
                attributes={
                    ATTR_SYSTEM: self._system,
                    ATTR_DESTINATION: destination,
                    ATTR_OPERATION: "publish",
                    ATTR_MESSAGE_ID: context.envelope.id,
                    ATTR_CONVERSATION_ID: context.envelope.correlation_id,
                    ATTR_ROUTING_KEY: context.routing_key,
                    ATTR_MESSAGE_TYPE: context.envelope.type,
                },
                # The adapter records the exception and sets the status
                # itself, in one place, for every way a span can end badly. Left
                # on, the SDK would record a second identical exception event on
                # the way out and a trace would show every failure twice.
                record_exception=False,
                set_status_on_exception=False,
            ) as span:
                # Injected *inside* the span, so what the message carries is this
                # span and not its parent. A consumer joining to the parent would
                # produce a trace where the publish and the process are siblings,
                # which is the wrong shape and hides which publish caused which
                # delivery when a service publishes more than one message.
                for name, value in self.propagation_headers().items():
                    context.set_header(name, value)

                try:
                    result = await send(context)
                except BaseException as failure:
                    self._failed(span, failure)
                    raise
                self._outcome(
                    span, OUTCOME_CONFIRMED if result.confirmed else OUTCOME_PUBLISHED
                )
                if not result.routed:
                    # Reached no queue at all, which the broker does not consider
                    # an error and which is very often the whole problem.
                    self._outcome(span, OUTCOME_UNROUTABLE)
                    if result.return_reason:
                        span.set_attribute(ATTR_REASON, result.return_reason)
                return result

        return traced

    def consume_interceptor(self) -> ConsumeInterceptor:
        """A span around every handler, parented by the publish that caused it.

        **The span outlives the handler on purpose.** What the handler answered
        is not what happened to the message: a handler asking for another
        attempt when there are none left is dead-lettered, and a span ended on
        the handler's answer says ``retried`` for a message nobody will ever
        try again. So where the consumer has promised to say how the delivery
        was settled — :meth:`~acemq_amqp.interceptors.ConsumeContext.when_settled`
        — the span waits for that, takes its outcome from it, and ends. Where
        nothing has promised, because the chain is being run by something other
        than a consumer, it ends here with what the handler said.

        The wait itself is not in the span. The consumer announces its decision
        before it acts on it, so a five-second backoff spent holding the
        delivery does not turn into a five-second handler.

        :returns: an interceptor for
            :meth:`~acemq_amqp.connection.Connection.intercept_consume`
        """

        async def traced(context: ConsumeContext, handle: ConsumeNext) -> Ack:
            span = self._tracer.start_span(
                context.queue + SPAN_PROCESS_SUFFIX,
                context=self._parent_of(context.envelope),
                kind=self._trace.SpanKind.CONSUMER,
                attributes={
                    ATTR_SYSTEM: self._system,
                    ATTR_DESTINATION: context.queue,
                    ATTR_OPERATION: "process",
                    ATTR_MESSAGE_ID: context.envelope.id,
                    ATTR_CONVERSATION_ID: context.envelope.correlation_id,
                    ATTR_ROUTING_KEY: context.routing_key,
                    ATTR_MESSAGE_TYPE: context.envelope.type,
                    ATTR_ATTEMPT: context.envelope.attempt,
                },
                # The adapter records the exception and sets the status
                # itself, in one place, for every way a span can end badly. Left
                # on, the SDK would record a second identical exception event on
                # the way out and a trace would show every failure twice.
                record_exception=False,
                set_status_on_exception=False,
            )
            settled = context.when_settled(self._settlement_recorder(context, span))
            with self._trace.use_span(
                span,
                # Ended by the settlement listener instead, which knows the
                # outcome. Not ending it here is the whole point; not ending it
                # anywhere would be a leak, which is why this is asked rather
                # than assumed.
                end_on_exit=not settled,
                record_exception=False,
                set_status_on_exception=False,
            ):
                try:
                    ack = await handle(context)
                except BaseException as failure:
                    self._failed(span, failure)
                    raise
                self._outcome(span, _ACK_OUTCOMES.get(ack.action, OUTCOME_ACKED))
                if ack.error is not None:
                    span.record_exception(ack.error)
                return ack

        return traced

    def _settlement_recorder(
        self, context: ConsumeContext, span: Any
    ) -> SettlementListener:
        """Writes what the consumer did onto the delivery's span, and ends it.

        The event and the outcome together, because separately they mislead: an
        outcome of ``dead_lettered`` with no event does not say why, and a
        ``message.dead_lettered`` event on a span whose outcome still reads
        ``retried`` is the bug this exists to fix.
        """

        def settled(settlement: Settlement) -> None:
            try:
                if settlement.delay is not None:
                    span.add_event(
                        EVENT_MESSAGE_RETRIED,
                        _retry_attributes(
                            context.queue, context.envelope, _millis(settlement.delay)
                        ),
                    )
                elif settlement.dead_lettered:
                    span.add_event(
                        EVENT_MESSAGE_DEAD_LETTERED,
                        _dead_letter_attributes(
                            context.queue, context.envelope, settlement.reason or ""
                        ),
                    )
                # Last, and overwriting whatever the handler's answer put there:
                # this is the one that is true.
                self._outcome(span, settlement.outcome)
            finally:
                span.end()

        return settled

    @contextmanager
    def request_span(self, destination: str, envelope: Envelope) -> Iterator[Any]:
        """A CLIENT span around a request that waits for its reply::

            with tracing.request_span("pricing", envelope):
                answer = await requester.ask(...)

        CLIENT rather than PRODUCER because this one waits. Its duration is a
        round trip and not a handover, and a backend that knows the difference
        will show it against the responder's latency rather than the broker's.

        :param destination: what is being asked
        :param envelope: the request's metadata
        :returns: the span, so a caller can add to it
        """
        with self._tracer.start_as_current_span(
            destination + SPAN_REQUEST_SUFFIX,
            kind=self._trace.SpanKind.CLIENT,
            attributes={
                ATTR_SYSTEM: self._system,
                ATTR_DESTINATION: destination,
                ATTR_OPERATION: "request",
                ATTR_MESSAGE_ID: envelope.id,
                ATTR_CONVERSATION_ID: envelope.correlation_id,
                ATTR_MESSAGE_TYPE: envelope.type,
            },
            # See the publish interceptor.
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                yield span
            except BaseException as failure:
                self._failed(span, failure)
                raise

    # ------------------------------------------------------------ propagation

    def propagation_headers(self) -> dict[str, str]:
        """The current trace context, as headers to put on a message.

        A fresh carrier each time rather than writing into something the caller
        owns, so nothing that was already on the message can be overwritten and
        nothing from a previous message can be left behind.

        Useful on its own for a message this library does not publish — one
        going out through a different client, or into a database row that will be
        published later by an outbox relay.

        :returns: ``traceparent`` and ``tracestate``, or nothing at all when no
            span is current
        """
        carrier: dict[str, str] = {}
        self._injector().inject(carrier)
        return carrier

    def _parent_of(self, envelope: Envelope) -> Any:
        """The context a delivery's span belongs under.

        Read from the message's own headers, never from whatever happened to be
        current. The delivery arrives on a connection's task, whose ambient
        context is at best unrelated to the message and at worst the *previous*
        message's — which produces a trace that looks joined up and joins the
        wrong things together.
        """
        carrier = {
            name: str(value)
            for name, value in envelope.headers.items()
            if name in (TRACEPARENT, TRACESTATE) and value is not None
        }
        if not carrier:
            # No context on the message. Returning an empty Context rather than
            # None starts a new trace, which is right: a message published by
            # something that does not propagate is the root of its own trace,
            # not a child of whatever this process was doing.
            return self._empty_context()
        return self._injector().extract(carrier, context=self._empty_context())

    def _empty_context(self) -> Any:
        from opentelemetry.context import Context

        return Context()

    def _injector(self) -> Any:
        if self._propagator is not None:
            return self._propagator
        return self._propagate.get_global_textmap()

    # ---------------------------------------------------------------- events

    def outbox_publish_failed(self, destination: str, reason: str) -> None:
        """Records that a relay could not publish a record it had committed.

        :param destination: where it was going
        :param reason: why it did not get there
        """
        self._event(
            EVENT_OUTBOX_PUBLISH_FAILED,
            {ATTR_DESTINATION: destination, ATTR_REASON: reason},
        )

    def outbox_published(self, destination: str, lag_ms: int) -> None:
        """Records how far behind the outbox relay is running.

        An attribute on the current span rather than an event: it is a
        measurement of the publish that is happening, not a thing that happened
        during it.

        :param destination: where the message went
        :param lag_ms: how long the record sat before it was published
        """
        span = self._trace.get_current_span()
        if span.is_recording():
            span.set_attribute(ATTR_OUTBOX_LAG, lag_ms)

    def pipeline_run_finished(
        self, pipeline: str, step: str, outcome: str, age_ms: int
    ) -> None:
        """Records the end of a pipeline run.

        :param pipeline: which pipeline
        :param step: the step it ended on
        :param outcome: how it ended
        :param age_ms: how old the run was by then
        """
        self._event(
            EVENT_PIPELINE_RUN_FINISHED,
            {
                ATTR_PIPELINE: pipeline,
                ATTR_STEP: step,
                ATTR_PIPELINE_OUTCOME: outcome,
                ATTR_RUN_AGE: age_ms,
            },
        )

    def message_retried(self, queue: str, envelope: Envelope, delay_ms: int) -> None:
        """Records that a message will be tried again, and how long from now.

        A consumer on a traced connection records this itself, on the delivery's
        own span. This is for a retry something else arranged.

        :param queue: where it came from
        :param envelope: its metadata, for the attempt count
        :param delay_ms: how long it waits first
        """
        self._event(EVENT_MESSAGE_RETRIED, _retry_attributes(queue, envelope, delay_ms))

    def message_dead_lettered(self, queue: str, envelope: Envelope, reason: str) -> None:
        """Records that a message was given up on.

        A consumer on a traced connection records this itself, on the delivery's
        own span. This is for a message something else gave up on.

        :param queue: where it came from
        :param envelope: its metadata, for the attempt count
        :param reason: why
        """
        self._event(
            EVENT_MESSAGE_DEAD_LETTERED, _dead_letter_attributes(queue, envelope, reason)
        )

    def _event(self, name: str, attributes: Mapping[str, Any]) -> None:
        span = self._trace.get_current_span()
        if span.is_recording():
            span.add_event(name, dict(attributes))

    # --------------------------------------------------------------- outcomes

    def _outcome(self, span: Any, outcome: str) -> None:
        span.set_attribute(ATTR_OUTCOME, outcome)
        if outcome in ERROR_OUTCOMES:
            span.set_status(self._trace.Status(self._trace.StatusCode.ERROR, outcome))

    def _failed(self, span: Any, failure: BaseException) -> None:
        span.record_exception(failure)
        self._outcome(span, OUTCOME_FAILED)
        span.set_status(
            self._trace.Status(self._trace.StatusCode.ERROR, str(failure) or "failed")
        )

    def __repr__(self) -> str:
        return f"OpenTelemetryTracing(system={self._system!r})"


def _retry_attributes(queue: str, envelope: Envelope, delay_ms: int) -> dict[str, Any]:
    """What a ``message.retried`` event says, wherever it is recorded from."""
    return {
        ATTR_DESTINATION: queue,
        ATTR_RETRY_DELAY: delay_ms,
        ATTR_ATTEMPT: envelope.attempt,
    }


def _dead_letter_attributes(
    queue: str, envelope: Envelope, reason: str
) -> dict[str, Any]:
    """What a ``message.dead_lettered`` event says, wherever it is recorded from.

    The reason is unbounded text, which a span tolerates and a metric does not.
    """
    return {
        ATTR_DESTINATION: queue,
        ATTR_REASON: reason,
        ATTR_ATTEMPT: envelope.attempt,
    }


def _millis(delay: timedelta) -> int:
    """A delay in whole milliseconds, which is the unit every attribute uses."""
    return int(delay.total_seconds() * 1000)
