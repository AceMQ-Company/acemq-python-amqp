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

"""One entity's messages in order, everything else at once."""

from __future__ import annotations

import asyncio

from acemq_amqp import Ack, Envelope, Message, accept
from acemq_amqp.patterns import (
    by_correlation,
    by_header,
    ordered,
    partition,
    partitioned_routing_key,
)
from acemq_amqp.patterns.ordered import _KeyedLocks

ORDER = "order-id"


def message(key: str | None, *, tag: str = "", correlation: str = "") -> Message:
    application = {} if key is None else {ORDER: key}
    return Message(
        payload={"tag": tag},
        envelope=Envelope(correlation_id=correlation or "", headers=application),
        routing_key="orders",
        content_type="application/json",
        redelivered=False,
        body=b"{}",
    )


async def test_messages_about_one_entity_are_never_handled_at_once() -> None:
    running = 0
    overlapped = False

    async def handler(incoming: Message) -> Ack:
        nonlocal running, overlapped
        running += 1
        overlapped = overlapped or running > 1
        # An await in the middle is the whole test: without a lock the second
        # message would start here, while the first is still going.
        await asyncio.sleep(0.01)
        running -= 1
        return accept()

    in_order = ordered(by_header(ORDER), handler)
    await asyncio.gather(*(in_order(message("A-1", tag=str(n))) for n in range(5)))

    assert overlapped is False


async def test_messages_about_one_entity_are_handled_in_the_order_they_arrived() -> None:
    handled: list[str] = []

    async def handler(incoming: Message) -> Ack:
        await asyncio.sleep(0.005)
        handled.append(incoming.payload["tag"])
        return accept()

    in_order = ordered(by_header(ORDER), handler)
    await asyncio.gather(*(in_order(message("A-1", tag=str(n))) for n in range(5)))

    assert handled == ["0", "1", "2", "3", "4"]


async def test_messages_about_different_entities_still_run_at_once() -> None:
    # Giving up concurrency altogether is the answer this pattern exists to
    # avoid: only messages about the same thing must not overtake each other.
    started = asyncio.Event()
    both = asyncio.Event()
    running = 0

    async def handler(incoming: Message) -> Ack:
        nonlocal running
        running += 1
        if running == 2:
            both.set()
        started.set()
        await asyncio.wait_for(both.wait(), 2.0)
        running -= 1
        return accept()

    in_order = ordered(by_header(ORDER), handler)
    await asyncio.gather(in_order(message("A-1")), in_order(message("B-2")))

    assert both.is_set() is True


async def test_a_message_with_no_key_is_not_ordered_against_anything() -> None:
    # Treating "no key" as one shared key would serialise every message that
    # happens to be missing the header, turning a producer's bug into a
    # consumer's outage.
    running = 0
    together = False

    async def handler(incoming: Message) -> Ack:
        nonlocal running, together
        running += 1
        together = together or running > 1
        await asyncio.sleep(0.01)
        running -= 1
        return accept()

    in_order = ordered(by_header(ORDER), handler)
    await asyncio.gather(in_order(message(None)), in_order(message(None)))

    assert together is True


async def test_correlation_keeps_one_business_action_in_sequence() -> None:
    handled: list[str] = []

    async def handler(incoming: Message) -> Ack:
        await asyncio.sleep(0.005)
        handled.append(incoming.payload["tag"])
        return accept()

    in_order = ordered(by_correlation(), handler)
    await asyncio.gather(
        *(in_order(message(None, tag=str(n), correlation="cart-9")) for n in range(3))
    )

    assert handled == ["0", "1", "2"]


async def test_a_key_is_forgotten_once_nothing_is_holding_it() -> None:
    # A key is usually an order or a customer, so a map that kept one lock per
    # key ever seen would grow for the life of the process.
    locks = _KeyedLocks()

    async def hold(key: str) -> None:
        async with locks.hold(key):
            await asyncio.sleep(0.005)

    await asyncio.gather(*(hold(f"order-{n}") for n in range(50)))

    assert len(locks) == 0


async def test_the_handlers_decision_is_the_one_that_comes_back() -> None:
    async def handler(incoming: Message) -> Ack:
        return accept()

    assert await ordered(by_header(ORDER), handler)(message("A-1")) == accept()


def test_a_partition_is_the_same_number_in_every_language() -> None:
    # These are the numbers Go's hash/fnv produces for the same keys. Python's
    # own hash() is randomised per process, so two workers in one deployment
    # would disagree about where a key belongs — the one thing a partition
    # function must never do.
    assert [partition(key, 8) for key in ("", "a", "order-1", "customer-99")] == [5, 4, 5, 2]
    assert [partition(key, 3) for key in ("", "a", "order-1", "customer-99")] == [1, 1, 1, 0]
    assert partition("ünïcode", 8) == 3
    assert partition("the quick brown fox", 1024) == 450


def test_one_partition_is_always_the_first_one() -> None:
    assert partition("anything", 1) == 0
    assert partition("anything", 0) == 0


def test_a_partitioned_routing_key_names_the_queue_a_key_belongs_to() -> None:
    assert partitioned_routing_key("orders", "order-1", 8) == "orders.5"
    # The same key always lands in the same place, which is what makes ordering
    # hold across processes rather than only within one.
    assert partitioned_routing_key("orders", "order-1", 8) == "orders.5"
