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

"""An itinerary the message carries, instead of an orchestrator that knows it.

Each service does its part and sends the message to the next stop written on the
slip, so the route is decided once — by whoever started the work — and travels
with the message rather than living in a component every service has to be able
to reach.

What it costs is that no single place says what the whole route is while it is
running, so a route that is wrong is discovered one hop at a time. Worth it when
the steps vary per message: an order over a threshold visits an approver, a
document goes to whichever reviewer owns it. Not worth it when every message
goes the same way, where a fixed chain of consumers is simpler and easier to
follow.

The slip travels in a header rather than in the payload, because a step that
rewrites the payload must not be able to lose the itinerary by accident.

Two forms, and this library reads and writes both
------------------------------------------------

**The JSON slip**, ``acemq-routing-slip``, is what Python writes by default and
what Go and Ruby write too. It is self-describing: each step carries its own
exchange and routing key, so a message can be followed by a service that knows
nothing about the route except how to read the header, and a route can be built
per message.

**The declared route**, ``x-acemq-route``, is what Java's ``Pipeline`` writes: a
comma-separated list of *step names*, with ``x-acemq-route-position`` saying
which one the message is for and ``x-acemq-route-id`` naming the run. Where a
step goes is not on the wire at all; it is resolved against a pipeline declared
in advance, whose name is its exchange and whose steps are bound by name. That
buys a slip somebody can read in a management console — ``validate,enrich,dispatch``
at position 1 — and costs the ability to route a message anywhere the
declaration does not already know about.

:func:`slip_from` reads whichever the message carries and :func:`follow_slip`
writes back the same one, so a Python step drops into a Java-declared pipeline
without either side being told about the other. :class:`SlipForm` is how to ask
for one deliberately: pass ``pipeline=`` the exchange those step names are bound
to, which is the one thing the declared form does not carry.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, TypeAlias

from .. import headers as wire
from ..ack import Ack, FatalError, accept, reject, retry
from ..codec import Codec
from ..connection import AsyncHandler, Connection, Message
from ..envelope import Envelope
from ._support import decide

#: The itinerary, as JSON. An application header, so anything may set it.
HEADER_ROUTING_SLIP = "acemq-routing-slip"

#: The step names of a declared route. A reserved header, so it is carried on
#: :class:`~acemq_amqp.Envelope` fields rather than in the application's map —
#: :attr:`~acemq_amqp.Envelope.route`, ``route_position`` and ``route_id``.
HEADER_ROUTE = wire.ROUTE


class SlipForm(str, Enum):
    """Which of the two wire forms a slip is written in.

    A slip read off a message remembers the form it arrived in, and
    :func:`follow_slip` writes the same one back unless it is told otherwise. A
    step that read a Java pipeline's route and answered with a JSON slip would
    hand the next Java step a message with no route on it at all, which is a
    pipeline that stops in the middle for no visible reason.
    """

    #: ``acemq-routing-slip``: exchange, routing key and name per step, as JSON.
    #: What a slip built here starts out as.
    JSON = "json"

    #: ``x-acemq-route``: the step names, comma-separated, with the position and
    #: the run identifier beside them. What Java's ``Pipeline`` writes.
    STEP_NAMES = "step-names"

#: What one stop does: a message in, the payload for the next stop out, an
#: exception for a failure. Raising :class:`~acemq_amqp.FatalError` says the
#: message will never get past this step and skips the attempts that are left.
Stage: TypeAlias = Callable[[Message], Any]


@dataclass(frozen=True, slots=True)
class Step:
    """One stop on a routing slip.

    :param exchange: where this step's message goes, empty for the default
        exchange
    :param routing_key: what it is published under, or a queue name
    :param name: what to call this step when a slip is read in a log
    :param completed_at: when it finished, filled in as the slip advances
    """

    exchange: str
    routing_key: str
    name: str = ""
    completed_at: str = ""

    def __str__(self) -> str:
        return self.name or f"{self.exchange}/{self.routing_key}"

    def to_json(self) -> dict[str, Any]:
        """The step as it goes on the wire.

        The field names are camelCase and not Python's, because this header is
        read by services written in four other languages and the wire is where
        they have to agree.
        """
        written = {"exchange": self.exchange, "routingKey": self.routing_key}
        if self.name:
            written["name"] = self.name
        if self.completed_at:
            written["completedAt"] = self.completed_at
        return written

    @classmethod
    def from_json(cls, raw: Any) -> Step:
        if not isinstance(raw, dict):
            raise ValueError(f"acemq: a routing slip step must be an object, not {type(raw)}")
        return cls(
            exchange=str(raw.get("exchange") or ""),
            routing_key=str(raw.get("routingKey") or ""),
            name=str(raw.get("name") or ""),
            completed_at=str(raw.get("completedAt") or ""),
        )


@dataclass(frozen=True, slots=True)
class RoutingSlip:
    """Where a message still has to go, and where it has been::

        slip = (
            RoutingSlip()
            .then("orders-events", "order.validate", name="validate")
            .then("orders-events", "order.charge", name="charge")
            .then("orders-events", "order.ship", name="ship")
        )
        await start(mq, slip, order)

    Immutable, and :meth:`advance` returns a new one. A slip is on a message
    that has already been published by the time anybody reads it, so changing
    one in place would describe a journey that did not happen.

    :param steps: what is still to do, in order
    :param done: what has already happened, oldest first, so a slip that fails
        halfway says how far it got
    :param run_id: identifies one run through the route across every hop. Only
        the declared form carries it on the wire; the JSON form has no field for
        it, and inventing one would be a change to a header three other
        libraries already read
    :param form: which wire form :meth:`onto` writes. A slip read off a message
        remembers the one it arrived in
    """

    steps: tuple[Step, ...] = ()
    done: tuple[Step, ...] = ()
    run_id: str = ""
    form: SlipForm = SlipForm.JSON

    def then(self, exchange: str, routing_key: str, *, name: str = "") -> RoutingSlip:
        """A slip with one more stop on the end.

        :param exchange: where that step's message goes
        :param routing_key: what it is published under
        :param name: what to call it in a log
        :returns: a new slip
        """
        return replace(
            self, steps=(*self.steps, Step(exchange, routing_key, name=name))
        )

    @property
    def next(self) -> Step | None:
        """The stop this message is going to, or ``None`` at the end."""
        return self.steps[0] if self.steps else None

    @property
    def finished(self) -> bool:
        """Whether every step has been done."""
        return not self.steps

    def advance(self) -> RoutingSlip:
        """A copy with the first step moved to :attr:`done`, stamped with now."""
        if not self.steps:
            return self
        completed = replace(
            self.steps[0], completed_at=datetime.now(timezone.utc).isoformat()
        )
        return replace(self, steps=self.steps[1:], done=(*self.done, completed))

    def to_header(self) -> str:
        """The slip rendered as JSON, whatever form it is in.

        The value of :data:`HEADER_ROUTING_SLIP`. :meth:`onto` is what a caller
        usually wants, because the declared form is not one header and cannot be
        returned as a string.
        """
        return json.dumps(
            {
                "steps": [step.to_json() for step in self.steps],
                "done": [step.to_json() for step in self.done],
            }
        )

    def written_as(self, form: SlipForm) -> RoutingSlip:
        """A copy that will be written in ``form``.

        Switching to :attr:`SlipForm.STEP_NAMES` gives the run an identifier if
        it has none, because that form has a header for one and a run without an
        identifier cannot be followed across its hops.

        :param form: which wire form to write
        :returns: a new slip
        :raises ValueError: when the steps cannot be written in that form. Asked
            here rather than at publish time, so a route that can never be
            written the way it was asked for fails where it was built
        """
        if form is SlipForm.JSON:
            return replace(self, form=form)
        switched = replace(self, form=form, run_id=self.run_id or str(uuid.uuid4()))
        switched.step_names()
        return switched

    def step_names(self) -> tuple[str, ...]:
        """The name of every step, done and still to do, in order.

        What the declared form puts on the wire. A step's name is its routing
        key on the route's exchange, which is how Java resolves one: the
        pipeline's name is the exchange, the step's name is the binding, and the
        queue behind it is ``{pipeline}.{step}``.

        :returns: the names, oldest first
        :raises ValueError: when the steps do not describe a declared route —
            when they do not all share one exchange, when one has no name, when
            a name carries the comma the wire form separates on, or when a step
            has a name and a routing key that disagree. The last of those is a
            slip that means two different things depending on which form it is
            written in, and picking one silently is how a message ends up on a
            queue nobody expected
        """
        every = (*self.done, *self.steps)
        if not every:
            raise ValueError("acemq: this routing slip has no steps in it")

        exchanges = {step.exchange for step in every}
        if len(exchanges) > 1:
            raise ValueError(
                "acemq: a declared route is one exchange with a queue per step, but "
                f"these steps span {sorted(exchanges)}; write this slip as JSON, "
                "which carries an exchange per step"
            )

        names: list[str] = []
        for step in every:
            name = step.name or step.routing_key
            if not name:
                raise ValueError(
                    "acemq: a declared route carries step names and nothing else, "
                    "and one of these steps has neither a name nor a routing key"
                )
            if step.name and step.routing_key and step.name != step.routing_key:
                raise ValueError(
                    f"acemq: step {step.name!r} is published under {step.routing_key!r}, "
                    "and a declared route has one word for both — the name is the "
                    "routing key. Name the step after its routing key, or write this "
                    "slip as JSON"
                )
            if "," in name:
                raise ValueError(
                    f"acemq: step name {name!r} contains a comma, which is what the "
                    "declared route separates its steps with"
                )
            names.append(name)
        return tuple(names)

    def onto(self, envelope: Envelope) -> Envelope:
        """The envelope with this slip written on it, in this slip's form.

        :param envelope: what the message is going out with
        :returns: a copy carrying the slip
        :raises ValueError: when the steps cannot be written in this slip's form
        """
        if self.form is SlipForm.JSON:
            return envelope.with_(
                headers={**envelope.headers, HEADER_ROUTING_SLIP: self.to_header()}
            )
        return envelope.with_(
            route=",".join(self.step_names()),
            route_position=len(self.done),
            route_id=self.run_id or str(uuid.uuid4()),
        )

    def __str__(self) -> str:
        done = " -> ".join(str(step) for step in self.done)
        todo = " -> ".join(str(step) for step in self.steps)
        return f"RoutingSlip[done: {done} | next: {todo}]"


def route_of(pipeline: str, *names: str, run_id: str = "") -> RoutingSlip:
    """A slip for a route declared in advance::

        slip = route_of("fulfilment", "validate", "enrich", "dispatch")
        await start(mq, slip, order)

    The declared form Java's ``Pipeline`` writes: the pipeline's name is the
    exchange, each step's name is the routing key it is bound on, and the queue
    behind it is ``{pipeline}.{step}``. A Java step consuming
    ``fulfilment.enrich`` reads what this publishes.

    :param pipeline: the pipeline's name, which is also its exchange
    :param names: the step names, in order
    :param run_id: an identifier for this run, generated when none is given
    :returns: a slip at the beginning of that route
    :raises ValueError: when there are no steps
    """
    if not names:
        raise ValueError("acemq: a declared route needs at least one step")
    return RoutingSlip(
        steps=tuple(Step(pipeline, name, name=name) for name in names),
        run_id=run_id or str(uuid.uuid4()),
        form=SlipForm.STEP_NAMES,
    )


def slip_from(envelope: Envelope, *, pipeline: str = "") -> RoutingSlip | None:
    """Reads the itinerary off a message, whichever form it is in.

    The JSON slip is looked for first, because it is the one that says where its
    steps go: a message carrying both is carrying one that can be followed and
    one that has to be resolved, and following the first needs nothing from the
    caller.

    A declared route needs ``pipeline``, which is the one thing that form does
    not put on the wire. Without it the steps come back on the default exchange
    under their own names, which is right for a route Python declared and wrong
    for one Java did — Java's queues are ``{pipeline}.{step}``, reached through
    the pipeline's exchange.

    :param envelope: what arrived
    :param pipeline: the exchange a declared route's step names are bound to
    :returns: the slip, or ``None`` when the message carries none
    :raises FatalError: when it carries one that cannot be read. Fatal because a
        slip that will not parse will not parse on the next attempt either, and
        spending five retries on it only delays the person who has to look
    """
    raw = envelope.headers.get(HEADER_ROUTING_SLIP)
    if raw is None:
        return _declared_route(envelope, pipeline)

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        raise FatalError(
            f"acemq: the routing slip on message {envelope.id} is a "
            f"{type(raw).__name__}, not text"
        )

    try:
        written = json.loads(raw)
        return RoutingSlip(
            steps=tuple(Step.from_json(step) for step in written.get("steps") or ()),
            done=tuple(Step.from_json(step) for step in written.get("done") or ()),
        )
    except (ValueError, AttributeError, TypeError) as failure:
        raise FatalError(
            f"acemq: cannot read the routing slip on message {envelope.id}: {failure}"
        ) from failure


def _declared_route(envelope: Envelope, pipeline: str) -> RoutingSlip | None:
    """The ``x-acemq-route`` form, as a slip.

    The steps before the position are the ones already done, and the rest are
    still to come — the same shape the JSON slip has, so everything downstream
    of here works on either without knowing which it was given.

    Nothing raises. A route with a position past its end is a finished route
    rather than an error, which is what Java reads it as, and an empty list of
    names is no route at all.
    """
    names = tuple(name.strip() for name in envelope.route.split(",") if name.strip())
    if not names:
        return None

    at = min(envelope.route_position, len(names))
    steps = tuple(Step(pipeline, name, name=name) for name in names)
    return RoutingSlip(
        steps=steps[at:],
        done=steps[:at],
        run_id=envelope.route_id,
        form=SlipForm.STEP_NAMES,
    )


async def start(
    connection: Connection,
    slip: RoutingSlip,
    payload: Any,
    *,
    envelope: Envelope | None = None,
    codec: Codec | None = None,
    form: SlipForm | None = None,
) -> None:
    """Sends a payload to the first stop on a slip.

    :param connection: where to publish
    :param slip: the itinerary
    :param payload: what to send
    :param envelope: metadata to send instead of a fresh one
    :param codec: a codec other than the connection's
    :param form: which wire form to write, defaulting to the slip's own —
        :data:`SlipForm.JSON` for one built with :meth:`RoutingSlip.then`, the
        declared form for one built by :func:`route_of`
    :raises ValueError: when the slip has no steps, which would publish a
        message with an itinerary that is already finished and no first stop to
        send it to, or when it cannot be written in the form asked for
    """
    first = slip.next
    if first is None:
        raise ValueError("acemq: this routing slip has no steps in it")

    written = slip if form is None else slip.written_as(form)
    outgoing = envelope or Envelope(origin=connection.origin)
    await connection.publisher(first.exchange, first.routing_key, codec=codec).send(
        payload, envelope=written.onto(outgoing)
    )


def follow_slip(
    connection: Connection,
    step: Stage,
    *,
    codec: Codec | None = None,
    pipeline: str = "",
    form: SlipForm | None = None,
) -> AsyncHandler:
    """Wraps a handler so the message goes on to its next stop::

        consumer = await mq.consume("charge-queue", follow_slip(mq, charge))

        # A step in a pipeline Java declared, whose queues are fulfilment.*:
        consumer = await mq.consume(
            "fulfilment.enrich", follow_slip(mq, enrich, pipeline="fulfilment")
        )

    The handler returns the payload to send onwards, which may be the one it was
    given or a changed copy, and raises to fail. When the slip has no steps left
    the work is finished and nothing more is published.

    Either wire form is followed, and by default the next message carries the
    same one it arrived in. That is what lets a Python step sit in the middle of
    a Java-declared pipeline: answering a declared route with a JSON slip would
    hand the next Java step a message with no route on it, and the run would
    stop halfway with nothing anywhere saying why.

    The message is accepted only once the next one is out, so a failure to
    publish retries this step — which is the reason a step that changes anything
    should be idempotent.

    :param connection: where the next message goes
    :param step: what this stop does, returning the payload to send on
    :param codec: a codec other than the connection's
    :param pipeline: the exchange a declared route's step names are bound to,
        which is the pipeline's own name. Needed only for the declared form,
        which does not carry it
    :param form: the wire form to write, defaulting to the one that arrived
    :returns: the wrapped handler
    """

    async def travel(message: Message) -> Ack:
        try:
            slip = slip_from(message.envelope, pipeline=pipeline)
        except FatalError as unreadable:
            return reject(unreadable)
        if slip is None:
            return reject(
                FatalError(
                    f"acemq: message {message.envelope.id} has no routing slip, so "
                    "there is nowhere to send it next"
                )
            )

        try:
            payload = await decide(step, message)
        except FatalError as unfixable:
            return reject(unfixable)
        except Exception as failure:
            return retry(failure)

        advanced = slip.advance()
        onwards = advanced.next
        if onwards is None:
            # The end of the itinerary. Nothing to publish, and the work is done.
            return accept()

        try:
            written = advanced if form is None else advanced.written_as(form)
            outgoing = written.onto(
                Envelope(
                    correlation_id=message.envelope.correlation_id,
                    causation_id=message.envelope.id,
                    origin=connection.origin,
                )
            )
        except ValueError as unwritable:
            # A route that cannot be written in the form asked for cannot be
            # written on the next attempt either, so this is the handler's
            # answer rather than the retry policy's.
            return reject(
                FatalError(
                    f"acemq: {slip.steps[0]} is done for message {message.envelope.id} "
                    f"but its slip cannot be written on: {unwritable}"
                )
            )

        try:
            await connection.publisher(
                onwards.exchange, onwards.routing_key, codec=codec
            ).send(payload, envelope=outgoing)
        except Exception as failure:
            return retry(
                RuntimeError(
                    f"acemq: {slip.steps[0]} is done for message {message.envelope.id} "
                    f"but the next step did not go out: {failure}"
                )
            )
        return accept()

    return travel
