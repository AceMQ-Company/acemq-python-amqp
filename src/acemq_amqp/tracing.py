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
tracing backend show them apart rather than averaging one into the other. It
ends ``answered`` or ``timed_out``, the two words Java writes on the same span.

The routing key is a ``publish`` attribute and only a ``publish`` attribute. A
delivery's span does not carry one, because no other library's does — Java is
not even handed one at ``consumeStarted`` — and an attribute present on one
library's process spans and absent from the other four's is worse than an
attribute nobody has: a query written against it quietly returns the Python
services and calls that the answer.

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

import asyncio
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from .ack import (
    OUTCOME_ACKED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_PARKED,
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
from .telemetry import (
    OUTCOME_ANSWERED,
    OUTCOME_CONFIRMED,
    OUTCOME_FAILED,
    OUTCOME_PUBLISHED,
    OUTCOME_TIMED_OUT,
    OUTCOME_UNROUTABLE,
)
from .transport import PublishResult

if TYPE_CHECKING:  # pragma: no cover - imported for types alone
    from .connection import Connection

#: What is being traced. ``rabbitmq`` by default, and the same value the other
#: libraries put here, so one dashboard reads across all five.
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

# The words a publish or a request can end in — ``confirmed``, ``published``,
# ``unroutable``, ``failed``, ``answered`` and ``timed_out`` — are defined in
# :mod:`acemq_amqp.telemetry` beside the metrics that are tagged with them, and
# re-exported here: a span and a counter describing the same publish have to say
# the same word, and the only way to be sure of that is for there to be one
# word. ``tracing.OUTCOME_CONFIRMED`` still resolves, and is the same string.
#
# The five a delivery can end in — ``acked``, ``retried``, ``rejected``,
# ``dead_lettered`` and ``parked`` — are defined in :mod:`acemq_amqp.ack` and
# re-exported here, because the consumer decides them and this only writes them
# down. They keep their names: ``tracing.OUTCOME_DEAD_LETTERED`` is where
# people look for them.

#: The outcomes that make a span an error, and only these.
#:
#: ``retried`` and ``rejected`` are deliberately absent. A retry is the system
#: working — the message will be tried again and very often succeeds — and
#: marking it an error paints a trace red for something that turned out fine. A
#: rejection is a decision the handler made on purpose. What is left is the three
#: that mean a message did not get where it was going.
#:
#: ``timed_out`` is absent for a different reason. It is not the outcome that
#: makes that span red, the exception is: a request which reached its deadline
#: raised, and :meth:`~OpenTelemetryTracing.request_span` records that exception
#: and sets the error status from it. Same colour, with the deadline in the
#: description rather than the bare word — and the set stays character-identical
#: to Java's.
#:
#: ``parked`` is absent for the same reason ``rejected`` is: a handler that
#: parks a message decided to, on purpose, having read it. The ``parked`` outcome
#: on ``acemq.messages.dead.lettered.total`` and the queue itself are what an
#: operator watches for those; painting the trace red as well would make a
#: deliberate decision look like a fault.
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
    Action.PARK: OUTCOME_PARKED,
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
                    # The outcome is said here rather than inside ``_failed``,
                    # which only knows that something threw. Java splits it the
                    # same way — ``Scope.failed`` records the exception and the
                    # status, and the caller, which knows what kind of operation
                    # this was, says the word.
                    self._outcome(span, OUTCOME_FAILED)
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
                    ATTR_MESSAGE_TYPE: context.envelope.type,
                    ATTR_ATTEMPT: context.envelope.attempt,
                    # No routing key. It is a publish-side attribute in every
                    # other library — Java's ``consumeStarted`` is not even given
                    # one — and an attribute that exists on one library's process
                    # spans and not on the other four's is worse than one that
                    # exists nowhere: a query written against it silently returns
                    # only the Python services.
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
                    # Provisional, and usually overwritten: the consumer turns a
                    # handler that raised into a retry request, and the
                    # settlement listener below writes down what it actually
                    # decided. It is set at all for the case where nothing is
                    # driving the chain and this is the last word.
                    self._outcome(span, OUTCOME_FAILED)
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

        The span ends with an outcome either way. ``answered`` when the reply
        came back, ``timed_out`` when the deadline did — the two words Java's
        ``MetricNames`` spells and its requester writes — and ``failed`` for
        anything else. The first of those is the one worth having: a request span
        that carried an outcome only when it went wrong left every successful
        round trip with no outcome at all, which is not a thing a dashboard can
        divide by.

        A timeout is told apart by its type rather than by its message.
        :class:`~acemq_amqp.patterns.RequestTimeoutError` is a
        :class:`TimeoutError`, so is anything else that ran out of time —
        :func:`asyncio.wait_for` included — and a caller who waits some other way
        is understood without this module having to know how.

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
                self._outcome(
                    span,
                    OUTCOME_TIMED_OUT if _is_deadline(failure) else OUTCOME_FAILED,
                )
                # Recorded even for a timeout, and so is the error status: the
                # outcome vocabulary is Java's, but a round trip that never got
                # its answer is a failure from where the caller is standing and a
                # green span would say otherwise.
                self._failed(span, failure)
                raise
            self._outcome(span, OUTCOME_ANSWERED)

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

        **Called by an application, never by this library**, and that is a
        limitation rather than an oversight. Java's relay calls its equivalent
        because its publish opens a span the attribute can land on;
        :class:`~acemq_amqp.patterns.OutboxRelay` has no such span to write on.
        It publishes with
        :meth:`~acemq_amqp.connection.Connection.publish_raw`, which is beneath
        the interceptor chain and so beneath the ``publish`` span, and it sweeps
        on a task of its own where nothing else is current either. A hook wired
        into the relay would compute a lag and hand it to a span that does not
        exist.

        The lag itself is *not* lost by that. The relay reports it on
        ``acemq.outbox.lag`` through the connection's observer, tagged and
        measured from the record's commit, because a metric needs no span to
        land on. What this method adds is the number *on the trace*, beside the
        work that caused it, which is worth having and which only a caller
        holding a span can arrange.

        So it stays where it works: call it from inside a span you are holding,
        which is what a ``sweep()`` at the end of a request is::

            with tracer.start_as_current_span("checkout"):
                ...
                await relay.sweep()
                tracing.outbox_published("orders", lag_ms=elapsed)

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
        """The exception and the error status, and deliberately not the outcome.

        Java's ``Scope.failed`` draws the line in the same place. What threw is
        knowable here; what the operation *was* is not, and a method that
        stamped ``failed`` on every span it touched would overwrite the
        ``timed_out`` a request had just earned. The caller says the word.
        """
        span.record_exception(failure)
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


def _is_deadline(failure: BaseException) -> bool:
    """Whether a request ran out of time rather than going wrong.

    Both spellings, because they are only the same class from Python 3.11: on
    3.10 :func:`asyncio.wait_for` raises an ``asyncio.TimeoutError`` that is not
    the builtin, and a span that said ``failed`` on one interpreter and
    ``timed_out`` on the next would be the least useful kind of difference.
    """
    return isinstance(failure, (TimeoutError, asyncio.TimeoutError))


def _millis(delay: timedelta) -> int:
    """A delay in whole milliseconds, which is the unit every attribute uses."""
    return int(delay.total_seconds() * 1000)
