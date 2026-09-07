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

from datetime import timedelta

import pytest
from fake_transport import FakeTransport

from acemq_amqp import dead_letter_queue, exponential_retry, fixed_retry
from acemq_amqp.topology import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_EXCHANGE_ARG,
    DEAD_LETTER_ROUTING_KEY_ARG,
    MESSAGE_TTL_ARG,
    RETRY_EXCHANGE,
    Binding,
    Topology,
    rung_args,
)

MINUTE = timedelta(minutes=1)


def test_dead_letter_declares_the_queue_it_names() -> None:
    topology = Topology().queue("orders.new", dead_letter=True)

    assert topology.queues == ["orders.new", "orders.new.dlq", "orders.new.parked"]
    assert topology.queues[1] == dead_letter_queue("orders.new")


def test_dead_letter_wiring_goes_through_the_named_exchange() -> None:
    topology = Topology().queue("orders.new", dead_letter=True)
    plan = topology.plan()

    source = next(action for action in plan if action.name == "orders.new")
    assert f"{DEAD_LETTER_EXCHANGE_ARG}='{DEAD_LETTER_EXCHANGE}'" in source.detail
    assert f"{DEAD_LETTER_ROUTING_KEY_ARG}='orders.new.dlq'" in source.detail


def test_the_dead_letter_exchange_reaches_both_queues_by_their_own_names() -> None:
    # Bound on their own names, which is what makes the routing key on the
    # source queue above mean anything, and is what Java does.
    topology = Topology().queue("orders.new", dead_letter=True)

    assert DEAD_LETTER_EXCHANGE in topology.exchanges
    assert (
        Binding("orders.new.dlq", DEAD_LETTER_EXCHANGE, "orders.new.dlq") in topology.bindings
    )
    assert (
        Binding("orders.new.parked", DEAD_LETTER_EXCHANGE, "orders.new.parked")
        in topology.bindings
    )


def test_the_managed_exchanges_are_declared_direct_and_durable() -> None:
    plan = Topology().queue("orders.new", dead_letter=True, retry=fixed_retry(3, MINUTE)).plan()

    for name in (DEAD_LETTER_EXCHANGE, RETRY_EXCHANGE):
        declared = next(
            action for action in plan if action.kind == "exchange" and action.name == name
        )
        assert declared.detail == "direct, durable"


def test_the_managed_exchanges_are_declared_once_however_many_queues_need_them() -> None:
    # Declaring an exchange twice is what validate() refuses, and two queues
    # asking for dead-lettering is the ordinary case rather than a corner one.
    topology = (
        Topology()
        .queue("orders.new", dead_letter=True, retry=fixed_retry(3, MINUTE))
        .queue("orders.paid", dead_letter=True, retry=fixed_retry(3, MINUTE))
    )

    topology.validate()
    assert topology.exchanges == [DEAD_LETTER_EXCHANGE, RETRY_EXCHANGE]


def test_the_dead_letter_queue_does_not_dead_letter() -> None:
    # A dead-letter queue that dead-letters is a loop, and a loop is how a
    # poison message becomes an outage.
    plan = Topology().queue("orders.new", dead_letter=True).plan()

    target = next(action for action in plan if action.name == "orders.new.dlq")
    assert DEAD_LETTER_EXCHANGE_ARG not in target.detail


def test_a_policy_declares_a_rung_for_each_of_its_long_waits() -> None:
    # One value produces both the queues and the delays a consumer publishes to,
    # so the two cannot drift into a retry addressed to a queue nobody declared.
    policy = exponential_retry(6, timedelta(seconds=10))
    topology = Topology().queue("orders.new", dead_letter=True, retry=policy)

    assert topology.queues == [
        "orders.new",
        "orders.new.dlq",
        "orders.new.parked",
        "orders.new.retry.40s",
        "orders.new.retry.80s",
        "orders.new.retry.160s",
    ]


def test_a_rung_is_declared_with_exactly_these_three_arguments() -> None:
    # The contract, pinned. Two services consuming orders.new declare the same
    # rung by name, so a rung declared with anything other than this answers the
    # second one PRECONDITION_FAILED and leaves it unable to consume at all.
    # Written through the constants so a rename cannot silently pass, and with
    # the values spelled out so a change of mind cannot silently pass either.
    assert rung_args("orders.new", timedelta(minutes=2)) == {
        MESSAGE_TTL_ARG: 120_000,
        DEAD_LETTER_EXCHANGE_ARG: RETRY_EXCHANGE,
        DEAD_LETTER_ROUTING_KEY_ARG: "orders.new",
    }
    assert MESSAGE_TTL_ARG == "x-message-ttl"
    assert DEAD_LETTER_EXCHANGE_ARG == "x-dead-letter-exchange"
    assert DEAD_LETTER_ROUTING_KEY_ARG == "x-dead-letter-routing-key"
    assert RETRY_EXCHANGE == "acemq.retry"
    assert DEAD_LETTER_EXCHANGE == "acemq.dlx"


def test_a_rung_holds_a_message_for_its_delay_and_then_returns_it() -> None:
    topology = Topology().queue("orders.new", retry=fixed_retry(3, timedelta(minutes=2)))
    rung = next(
        action
        for action in topology.plan()
        if action.kind == "queue" and "retry" in action.name
    )

    # The TTL is on the queue, never on the message: RabbitMQ expires only from
    # the head, so one queue of per-message TTLs would let a long wait at the
    # front hold back every shorter one behind it.
    assert f"{MESSAGE_TTL_ARG}=120000" in rung.detail
    assert f"{DEAD_LETTER_EXCHANGE_ARG}='{RETRY_EXCHANGE}'" in rung.detail
    assert f"{DEAD_LETTER_ROUTING_KEY_ARG}='orders.new'" in rung.detail


def test_the_binding_that_brings_an_expired_retry_home_is_not_optional() -> None:
    # Without it a rung expires into an exchange that routes nowhere, the broker
    # drops the message and says nothing, and the queue looks quiet rather than
    # broken. So it is declared with the rungs, not offered as something to add.
    topology = Topology().queue("orders.new", retry=fixed_retry(3, MINUTE))

    assert RETRY_EXCHANGE in topology.exchanges
    assert Binding("orders.new", RETRY_EXCHANGE, "orders.new") in topology.bindings
    topology.validate()


def test_a_policy_with_no_long_waits_declares_no_retry_exchange() -> None:
    topology = Topology().queue("orders.new", retry=fixed_retry(5, timedelta(seconds=2)))

    assert topology.exchanges == []
    assert topology.bindings == []


def test_a_policy_whose_waits_are_all_short_needs_no_rungs() -> None:
    topology = Topology().queue("orders.new", retry=fixed_retry(5, timedelta(seconds=2)))

    assert topology.queues == ["orders.new"]


async def test_the_rungs_reach_the_broker_when_the_topology_is_applied() -> None:
    transport = FakeTransport()
    policy = fixed_retry(3, MINUTE)

    await Topology().queue("orders.new", dead_letter=True, retry=policy).apply(transport)

    assert "orders.new.retry.1m" in transport.queues
    assert transport.queues["orders.new.retry.1m"].args == rung_args("orders.new", MINUTE)
    assert transport.exchanges[RETRY_EXCHANGE].kind == "direct"
    assert ("orders.new", RETRY_EXCHANGE, "orders.new") in transport.bindings


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

    assert list(transport.exchanges) == [DEAD_LETTER_EXCHANGE, "orders-events"]
    assert list(transport.queues) == [
        "shipping.orders",
        "shipping.orders.dlq",
        "shipping.orders.parked",
    ]
    assert transport.bindings == [
        ("shipping.orders", "orders-events", "order.placed"),
        ("shipping.orders.dlq", DEAD_LETTER_EXCHANGE, "shipping.orders.dlq"),
        ("shipping.orders.parked", DEAD_LETTER_EXCHANGE, "shipping.orders.parked"),
    ]


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
