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

"""What a topology describes, and what it refuses to describe."""

from __future__ import annotations

import pytest
from fake_transport import FakeTransport

from acemq_amqp import dead_letter_queue
from acemq_amqp.topology import (
    DEAD_LETTER_EXCHANGE_ARG,
    DEAD_LETTER_ROUTING_KEY_ARG,
    Topology,
)


def test_dead_letter_declares_the_queue_it_names() -> None:
    topology = Topology().queue("orders.new", dead_letter=True)

    assert topology.queues == ["orders.new", "orders.new.dlq", "orders.new.parked"]
    assert topology.queues[1] == dead_letter_queue("orders.new")


def test_dead_letter_wiring_uses_the_default_exchange_and_the_conventional_name() -> None:
    topology = Topology().queue("orders.new", dead_letter=True)
    plan = topology.plan()

    source = next(action for action in plan if action.name == "orders.new")
    assert f"{DEAD_LETTER_EXCHANGE_ARG}=''" in source.detail
    assert f"{DEAD_LETTER_ROUTING_KEY_ARG}='orders.new.dlq'" in source.detail


def test_the_dead_letter_queue_does_not_dead_letter() -> None:
    # A dead-letter queue that dead-letters is a loop, and a loop is how a
    # poison message becomes an outage.
    plan = Topology().queue("orders.new", dead_letter=True).plan()

    target = next(action for action in plan if action.name == "orders.new.dlq")
    assert DEAD_LETTER_EXCHANGE_ARG not in target.detail


def test_asking_for_dead_lettering_twice_is_refused_rather_than_resolved() -> None:
    with pytest.raises(ValueError, match="pick one"):
        Topology().queue(
            "orders.new",
            dead_letter=True,
            args={DEAD_LETTER_EXCHANGE_ARG: "orders-dead"},
        )


def test_a_queue_needs_a_name() -> None:
    with pytest.raises(ValueError, match="a queue needs a name"):
        Topology().queue("")


def test_an_exchange_needs_a_kind() -> None:
    with pytest.raises(ValueError, match="needs a kind"):
        Topology().exchange("orders-events", "")


def test_a_queue_declared_twice_is_a_mistake_worth_naming() -> None:
    topology = Topology().queue("orders.new").queue("orders.new")
    with pytest.raises(ValueError, match=r"declares queue 'orders\.new' twice"):
        topology.validate()


def test_a_binding_to_a_queue_nothing_declares_is_refused() -> None:
    # The broker would accept it if the queue happened to exist already, and the
    # service would then depend on something nothing declares.
    topology = Topology().exchange("orders-events").binding("shipping", "orders-events", "#")
    with pytest.raises(ValueError, match="which this topology does not declare"):
        topology.validate()


def test_a_binding_to_an_exchange_nothing_declares_is_refused() -> None:
    topology = Topology().queue("shipping").binding("shipping", "orders-events", "#")
    with pytest.raises(ValueError, match="names exchange 'orders-events'"):
        topology.validate()


def test_the_default_exchange_cannot_be_bound_to() -> None:
    topology = Topology().queue("shipping").binding("shipping", "", "#")
    with pytest.raises(ValueError, match="default exchange"):
        topology.validate()


async def test_applying_declares_exchanges_then_queues_then_bindings() -> None:
    # The order is the one a broker will accept: a binding names both ends, so
    # both ends have to exist by the time it is made.
    transport = FakeTransport()
    topology = (
        Topology()
        .binding("shipping.orders", "orders-events", "order.placed")
        .queue("shipping.orders", dead_letter=True)
        .exchange("orders-events", "topic")
    )

    await topology.apply(transport)

    assert list(transport.exchanges) == ["orders-events"]
    assert list(transport.queues) == [
        "shipping.orders",
        "shipping.orders.dlq",
        "shipping.orders.parked",
    ]
    assert transport.bindings == [("shipping.orders", "orders-events", "order.placed")]


async def test_applying_an_invalid_topology_changes_nothing() -> None:
    transport = FakeTransport()
    topology = Topology().queue("shipping").binding("shipping", "orders-events", "#")

    with pytest.raises(ValueError):
        await topology.apply(transport)

    assert transport.queues == {}


def test_a_topology_prints_as_something_worth_putting_in_a_deployment_log() -> None:
    printed = str(
        Topology()
        .exchange("orders-events", "topic")
        .queue("shipping.orders")
        .binding("shipping.orders", "orders-events", "order.placed")
    )

    assert "Topology: 1 exchanges, 1 queues, 1 bindings" in printed
    assert "declare exchange orders-events (topic, durable)" in printed
    assert "declare binding shipping.orders (from orders-events on order.placed)" in printed


def test_an_invalid_topology_says_so_rather_than_pretending_to_print() -> None:
    assert "invalid" in str(Topology().binding("nowhere", "orders-events", "#"))
