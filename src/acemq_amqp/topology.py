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

"""The exchanges, queues and bindings a service expects to find.

Declaring them one call at a time works, and stops working the moment somebody
needs to know what a service will do to a broker *before* it does it. A topology
can be printed, checked and applied, which is the difference between a
deployment somebody approves and one they find out about::

    topology = (
        Topology()
        .exchange("orders-events", "topic")
        .queue("shipping-orders", dead_letter=True)
        .binding("shipping-orders", "orders-events", "order.placed")
    )
    print(topology)
    await topology.apply(transport)

Mistakes are raised where they are made rather than collected for later. Go
accumulates them on the builder because a Go builder has nowhere else to put
them; Python has an exception and a traceback that points at the line that is
wrong, which is more useful than a message that says a topology is bad.

Two exchanges are this library's own rather than the caller's:
:data:`RETRY_EXCHANGE` returns an expired rung message to the queue it came
from, and :data:`DEAD_LETTER_EXCHANGE` reaches the dead-letter and parking
queues. Both are declared, and bound, by :meth:`Topology.queue` when it is asked
for retries or for dead-lettering, so that the whole arrangement appears in
:meth:`Topology.plan` and nothing depends on a caller remembering a binding.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from . import naming
from .retry import RetryPolicy
from .transport import ExchangeSpec, QueueSpec, Transport

#: Where the broker sends a message this queue rejects or expires.
DEAD_LETTER_EXCHANGE_ARG = "x-dead-letter-exchange"

#: What routing key it is sent under, which a direct exchange matches against
#: its bindings.
DEAD_LETTER_ROUTING_KEY_ARG = "x-dead-letter-routing-key"

#: How long a message may sit on a queue before the broker expires it, in
#: milliseconds. On the queue, never on the message: see :meth:`Topology.queue`.
MESSAGE_TTL_ARG = "x-message-ttl"

#: The exchange a rung queue dead-letters through on its way back to the queue
#: the message came from, bound on the source queue's own name.
#:
#: This constant and :data:`DEAD_LETTER_EXCHANGE` are the only two places that
#: name an exchange this library manages, which is deliberate. A rung is a
#: contract rather than a preference: two services consuming the same queue
#: declare the same ``{queue}.retry.{delay}`` by name, and a rung declared with
#: different arguments answers the second service ``PRECONDITION_FAILED`` and
#: leaves it unable to consume at all. Python used to dead-letter a rung through
#: the default exchange, which needs no exchange and no binding and works
#: perfectly well on its own — but Java has always used a named exchange, most
#: of the released code follows Java, and two libraries cannot both be right
#: about one queue. This is the settled answer.
RETRY_EXCHANGE = "acemq.retry"

#: The exchange the dead-letter and parking queues are reached through, each
#: bound on its own name.
DEAD_LETTER_EXCHANGE = "acemq.dlx"

#: How both managed exchanges are declared: direct, because every binding on
#: them matches a queue name exactly, and durable, because a topology that
#: vanished with the broker would leave rungs dead-lettering into nothing.
_MANAGED_EXCHANGE = ExchangeSpec(kind="direct", durable=True)


def rung_args(source: str, delay: timedelta) -> dict[str, Any]:
    """The arguments a ``{source}.retry.{delay}`` queue must be declared with.

    Exactly three keys, and the same three in every AceMQ library: the delay as
    ``x-message-ttl``, :data:`RETRY_EXCHANGE` as the dead-letter target, and the
    source queue as the routing key it is sent under. Nothing consumes a rung;
    the time-to-live is the only thing that ever takes a message out of one, and
    the binding made by :meth:`Topology.queue` is what brings it home.

    The TTL is on the queue rather than on each message, which is the whole
    reason a policy needs several queues instead of one. RabbitMQ expires
    messages only from the head of a queue, so a single queue holding
    per-message TTLs releases nothing while a message with a long one sits at
    the front: a thirty-second wait queued behind a ten-minute wait becomes a
    ten-minute wait. The delays that come out would bear no relation to the ones
    that went in, and the bug would only appear under the load that puts two
    different waits on the queue at once.

    :param source: the queue being consumed, which is where an expired message
        goes back to
    :param delay: how long the rung holds a message
    :returns: the argument table, which is a contract and is pinned by a test
    """
    return {
        MESSAGE_TTL_ARG: int(delay.total_seconds() * 1000),
        DEAD_LETTER_EXCHANGE_ARG: RETRY_EXCHANGE,
        DEAD_LETTER_ROUTING_KEY_ARG: source,
    }


@dataclass(frozen=True, slots=True)
class Binding:
    """Messages matching a routing key going from an exchange to a queue."""

    queue: str
    exchange: str
    routing_key: str = ""

    def __str__(self) -> str:
        return f"{self.exchange} -> {self.queue} ({self.routing_key})"


@dataclass(frozen=True, slots=True)
class PlanAction:
    """One thing applying a topology would do."""

    kind: str
    name: str
    detail: str = ""

    def __str__(self) -> str:
        if not self.detail:
            return f"declare {self.kind} {self.name}"
        return f"declare {self.kind} {self.name} ({self.detail})"


@dataclass(frozen=True, slots=True)
class _NamedQueue:
    name: str
    spec: QueueSpec


@dataclass(frozen=True, slots=True)
class _NamedExchange:
    name: str
    spec: ExchangeSpec


@dataclass
class Topology:
    """A description of what a service needs a broker to have.

    Every method that adds something returns the topology, so a description
    reads as one expression. The order things are added in does not matter:
    :meth:`apply` declares exchanges first, then queues, then the bindings
    between them, because that is the order a broker will accept them in.
    """

    _exchanges: list[_NamedExchange] = field(default_factory=list)
    _queues: list[_NamedQueue] = field(default_factory=list)
    _bindings: list[Binding] = field(default_factory=list)

    def exchange(
        self,
        name: str,
        kind: str = "topic",
        *,
        durable: bool = True,
        auto_delete: bool = False,
        args: Mapping[str, Any] | None = None,
    ) -> Topology:
        """Adds an exchange.

        :param name: what to call it
        :param kind: ``direct``, ``topic``, ``fanout`` or ``headers``
        :param durable: survives a broker restart
        :param auto_delete: goes away when its last binding does
        :param args: broker-specific arguments
        :returns: this topology
        """
        if not name:
            raise ValueError("acemq: an exchange needs a name")
        if not kind:
            raise ValueError(f"acemq: exchange {name!r} needs a kind (direct, topic, fanout)")
        self._exchanges.append(
            _NamedExchange(
                name,
                ExchangeSpec(
                    kind=kind,
                    durable=durable,
                    auto_delete=auto_delete,
                    args=dict(args or {}),
                ),
            )
        )
        return self

    def queue(
        self,
        name: str,
        *,
        durable: bool = True,
        auto_delete: bool = False,
        exclusive: bool = False,
        dead_letter: bool = False,
        retry: RetryPolicy | None = None,
        args: Mapping[str, Any] | None = None,
    ) -> Topology:
        """Adds a queue, and the queues that catch what it cannot handle.

        ``dead_letter`` declares ``{name}.dlq`` alongside it and points the
        broker at it, so a message this queue rejects or lets expire lands
        somewhere an operator can find by name rather than disappearing. It
        declares ``{name}.parked`` too, because a consumer on this queue sends
        anything it cannot decode there — and a library that parks messages into
        a queue nobody declared has only moved the disappearance somewhere else.

        Neither dead-letters itself: a dead-letter queue that dead-letters is a
        loop, and a loop is how a poison message becomes an outage. Both are
        reached through :data:`DEAD_LETTER_EXCHANGE`, bound on their own names,
        which is what Java has always done and is therefore what a broker shared
        with a Java service already has. A service that wants its own
        dead-letter exchange instead should leave ``dead_letter`` alone and pass
        the arguments in ``args``.

        ``retry`` declares the rung queues that policy's long waits need: one
        ``{name}.retry.{delay}`` per distinct delay at or above the policy's
        threshold, each declared with :func:`rung_args` — the delay as
        ``x-message-ttl``, :data:`RETRY_EXCHANGE` as the dead-letter target, and
        this queue as the routing key. The binding that brings an expired
        message home, ``{name}`` to :data:`RETRY_EXCHANGE` on ``{name}``, is
        made here and not left to a caller to remember: without it every rung
        expires into an exchange that routes nowhere, and a retry that is
        dropped by the broker looks exactly like one that is still waiting.

        It takes the policy rather than a list of delays on purpose. The rungs a
        consumer will publish to are derived from the policy it is running, so
        anything else here would be a second copy of the same list, free to drift
        from the first — and the way that drift shows up is a retry published to
        a queue nobody declared, at the moment the service is already failing.
        One value produces both, or the topology is not a description of what the
        service needs.

        :param name: what to call it
        :param durable: survives a broker restart
        :param auto_delete: goes away when its last consumer does
        :param exclusive: usable only by the connection that declared it
        :param dead_letter: also declare and wire ``{name}.dlq``, and declare
            ``{name}.parked``
        :param retry: also declare the rung queues this policy's long waits use
        :param args: broker-specific arguments
        :returns: this topology
        """
        if not name:
            raise ValueError("acemq: a queue needs a name")

        arguments: dict[str, Any] = dict(args or {})
        if dead_letter:
            # Refused rather than resolved, because either answer would be a
            # guess about which of two conflicting instructions was meant.
            conflicting = sorted(
                key
                for key in arguments
                if key in (DEAD_LETTER_EXCHANGE_ARG, DEAD_LETTER_ROUTING_KEY_ARG)
            )
            if conflicting:
                raise ValueError(
                    f"acemq: queue {name!r} asks for dead_letter=True and also sets "
                    + ", ".join(conflicting)
                    + "; pick one"
                )
            arguments[DEAD_LETTER_EXCHANGE_ARG] = DEAD_LETTER_EXCHANGE
            arguments[DEAD_LETTER_ROUTING_KEY_ARG] = naming.dead_letter_queue(name)

        self._queues.append(
            _NamedQueue(
                name,
                QueueSpec(
                    durable=durable,
                    auto_delete=auto_delete,
                    exclusive=exclusive,
                    args=arguments,
                ),
            )
        )
        if dead_letter:
            self._managed_exchange(DEAD_LETTER_EXCHANGE)
            for target in (naming.dead_letter_queue(name), naming.parked_queue(name)):
                self._queues.append(_NamedQueue(target, QueueSpec(durable=durable)))
                self._bindings.append(
                    Binding(queue=target, exchange=DEAD_LETTER_EXCHANGE, routing_key=target)
                )
        if retry is not None:
            rungs = retry.broker_rungs()
            if rungs:
                self._managed_exchange(RETRY_EXCHANGE)
                for rung in rungs:
                    self._queues.append(
                        _NamedQueue(
                            naming.retry_queue(name, rung),
                            QueueSpec(durable=durable, args=rung_args(name, rung)),
                        )
                    )
                # One binding brings every expired message back to the queue it
                # came from. Made here rather than offered as something to add,
                # because a missing binding loses every retry silently: the
                # broker drops what a direct exchange cannot route and says
                # nothing, so the queue looks quiet rather than broken.
                self._bindings.append(
                    Binding(queue=name, exchange=RETRY_EXCHANGE, routing_key=name)
                )
        return self

    def _managed_exchange(self, name: str) -> None:
        """Declares one of this library's own exchanges, once.

        Several queues in one topology each need :data:`RETRY_EXCHANGE` and
        :data:`DEAD_LETTER_EXCHANGE`, and declaring either twice is what
        :meth:`validate` refuses. A caller who has already named the same
        exchange keeps their own spec: theirs is the deliberate one.
        """
        if any(entry.name == name for entry in self._exchanges):
            return
        self._exchanges.append(_NamedExchange(name, _MANAGED_EXCHANGE))

    def binding(self, queue: str, exchange: str, routing_key: str = "") -> Topology:
        """Routes messages matching a key from an exchange to a queue.

        :param queue: where messages end up, which this topology must declare
        :param exchange: where they come from, which this topology must declare
        :param routing_key: what to match, empty for a fanout
        :returns: this topology
        """
        self._bindings.append(Binding(queue=queue, exchange=exchange, routing_key=routing_key))
        return self

    @property
    def queues(self) -> list[str]:
        """The queue names this topology declares, in declaration order."""
        return [entry.name for entry in self._queues]

    @property
    def exchanges(self) -> list[str]:
        """The exchange names this topology declares, in declaration order."""
        return [entry.name for entry in self._exchanges]

    @property
    def bindings(self) -> list[Binding]:
        """The bindings this topology declares."""
        return list(self._bindings)

    def validate(self) -> None:
        """Reports what is wrong with the description itself.

        A binding naming a queue the topology does not declare is the mistake
        worth catching here. The broker would accept it if the queue happened to
        exist already, and the service would then depend on something nothing
        declares — which works until the day it is deployed somewhere new.

        :raises ValueError: when the topology cannot be right
        """
        seen_queues: set[str] = set()
        for queue in self._queues:
            if queue.name in seen_queues:
                raise ValueError(f"acemq: the topology declares queue {queue.name!r} twice")
            seen_queues.add(queue.name)

        seen_exchanges: set[str] = set()
        for exchange in self._exchanges:
            if exchange.name in seen_exchanges:
                raise ValueError(
                    f"acemq: the topology declares exchange {exchange.name!r} twice"
                )
            seen_exchanges.add(exchange.name)

        for binding in self._bindings:
            if binding.queue not in seen_queues:
                raise ValueError(
                    f"acemq: binding {binding} names queue {binding.queue!r}, "
                    "which this topology does not declare"
                )
            if not binding.exchange:
                raise ValueError(
                    f"acemq: binding {binding} names the default exchange, "
                    "which cannot be bound to"
                )
            if binding.exchange not in seen_exchanges:
                raise ValueError(
                    f"acemq: binding {binding} names exchange {binding.exchange!r}, "
                    "which this topology does not declare"
                )

    def plan(self) -> list[PlanAction]:
        """What :meth:`apply` would do, without doing it.

        Deliberately not a difference against the live broker: AMQP offers no
        way to enumerate what is there without the management API, and a plan
        that quietly guessed would be worse than one that is honest about being
        a statement of intent.

        :returns: one action per thing that would be declared
        :raises ValueError: when the topology cannot be right
        """
        self.validate()
        actions = [
            PlanAction("exchange", entry.name, _describe(entry.spec))
            for entry in self._exchanges
        ]
        actions += [
            PlanAction("queue", entry.name, _describe(entry.spec)) for entry in self._queues
        ]
        actions += [
            PlanAction(
                "binding", binding.queue, f"from {binding.exchange} on {binding.routing_key}"
            )
            for binding in self._bindings
        ]
        return actions

    async def apply(self, transport: Transport) -> None:
        """Declares everything, in the order a broker needs.

        It stops at the first failure. A queue that already exists with
        different settings is refused by the broker with ``PRECONDITION_FAILED``,
        and that refusal is passed on rather than swallowed: it means this
        service and the broker disagree about what the queue is, and carrying on
        would leave the service using a queue that is not the one it asked for.

        :param transport: where to declare it
        :raises ValueError: when the topology cannot be right
        """
        self.validate()
        for exchange in self._exchanges:
            await transport.declare_exchange(exchange.name, exchange.spec)
        for queue in self._queues:
            await transport.declare_queue(queue.name, queue.spec)
        for binding in self._bindings:
            await transport.bind(binding.queue, binding.exchange, binding.routing_key)

    def __str__(self) -> str:
        try:
            actions = self.plan()
        except ValueError as failure:
            return f"Topology(invalid: {failure})"
        header = (
            f"Topology: {len(self._exchanges)} exchanges, "
            f"{len(self._queues)} queues, {len(self._bindings)} bindings"
        )
        return "\n".join([header, *(f"  {action}" for action in actions)])


def _describe(spec: QueueSpec | ExchangeSpec) -> str:
    """A spec as the line an operator would want in a deployment log."""
    parts: list[str] = []
    if isinstance(spec, ExchangeSpec):
        parts.append(spec.kind)
    parts.append("durable" if spec.durable else "transient")
    if spec.auto_delete:
        parts.append("auto-delete")
    if isinstance(spec, QueueSpec) and spec.exclusive:
        parts.append("exclusive")
    parts += [f"{key}={spec.args[key]!r}" for key in sorted(spec.args)]
    return ", ".join(parts)
