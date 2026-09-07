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

The slip travels as JSON in an application header rather than in the payload,
because a step that rewrites the payload must not be able to lose the itinerary
by accident.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, TypeAlias

from ..ack import Ack, FatalError, accept, reject, retry
from ..codec import Codec
from ..connection import AsyncHandler, Connection, Message
from ..envelope import Envelope
from ._support import decide

#: The itinerary, as JSON.
HEADER_ROUTING_SLIP = "acemq-routing-slip"

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
    """

    steps: tuple[Step, ...] = ()
    done: tuple[Step, ...] = ()

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
        return RoutingSlip(steps=self.steps[1:], done=(*self.done, completed))

    def to_header(self) -> str:
        """The slip rendered for the wire."""
        return json.dumps(
            {
                "steps": [step.to_json() for step in self.steps],
                "done": [step.to_json() for step in self.done],
            }
        )

    def __str__(self) -> str:
        done = " -> ".join(str(step) for step in self.done)
        todo = " -> ".join(str(step) for step in self.steps)
        return f"RoutingSlip[done: {done} | next: {todo}]"


def slip_from(envelope: Envelope) -> RoutingSlip | None:
    """Reads the itinerary off a message, if it has one.

    :param envelope: what arrived
    :returns: the slip, or ``None`` when the message carries none
    :raises FatalError: when it carries one that cannot be read. Fatal because a
        slip that will not parse will not parse on the next attempt either, and
        spending five retries on it only delays the person who has to look
    """
    raw = envelope.headers.get(HEADER_ROUTING_SLIP)
    if raw is None:
        return None

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


async def start(
    connection: Connection,
    slip: RoutingSlip,
    payload: Any,
    *,
    envelope: Envelope | None = None,
    codec: Codec | None = None,
) -> None:
    """Sends a payload to the first stop on a slip.

    :param connection: where to publish
    :param slip: the itinerary
    :param payload: what to send
    :param envelope: metadata to send instead of a fresh one
    :param codec: a codec other than the connection's
    :raises ValueError: when the slip has no steps, which would publish a
        message with an itinerary that is already finished and no first stop to
        send it to
    """
    first = slip.next
    if first is None:
        raise ValueError("acemq: this routing slip has no steps in it")

    outgoing = envelope or Envelope(origin=connection.origin)
    await connection.publisher(first.exchange, first.routing_key, codec=codec).send(
        payload,
        envelope=outgoing.with_(
            headers={**outgoing.headers, HEADER_ROUTING_SLIP: slip.to_header()}
        ),
    )


def follow_slip(
    connection: Connection, step: Stage, *, codec: Codec | None = None
) -> AsyncHandler:
    """Wraps a handler so the message goes on to its next stop::

        consumer = await mq.consume("charge-queue", follow_slip(mq, charge))

    The handler returns the payload to send onwards, which may be the one it was
    given or a changed copy, and raises to fail. When the slip has no steps left
    the work is finished and nothing more is published.

    The message is accepted only once the next one is out, so a failure to
    publish retries this step — which is the reason a step that changes anything
    should be idempotent.

    :param connection: where the next message goes
    :param step: what this stop does, returning the payload to send on
    :param codec: a codec other than the connection's
    :returns: the wrapped handler
    """

    async def travel(message: Message) -> Ack:
        try:
            slip = slip_from(message.envelope)
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
            await connection.publisher(
                onwards.exchange, onwards.routing_key, codec=codec
            ).send(
                payload,
                envelope=Envelope(
                    correlation_id=message.envelope.correlation_id,
                    causation_id=message.envelope.id,
                    origin=connection.origin,
                    headers={HEADER_ROUTING_SLIP: advanced.to_header()},
                ),
            )
        except Exception as failure:
            return retry(
                RuntimeError(
                    f"acemq: {slip.steps[0]} is done for message {message.envelope.id} "
                    f"but the next step did not go out: {failure}"
                )
            )
        return accept()

    return travel
