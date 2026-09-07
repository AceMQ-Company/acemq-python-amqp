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

"""Several consumers over one queue, started and stopped together."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest
from fake_transport import FakeSubscription, FakeTransport

from acemq_amqp import Ack, Connection, Message, accept
from acemq_amqp.patterns import ConsumerGroup
from acemq_amqp.transport import ConsumeSpec, Delivery

QUEUE = "orders.new"


async def handler(message: Message) -> Ack:
    return accept()


class Counting(FakeTransport):
    """Records the consume calls, which is what a group is made of."""

    tags: list[str]
    fail_after: int

    def __init__(self, fail_after: int = 0) -> None:
        super().__init__()
        self.tags = []
        self.fail_after = fail_after
        self.subscriptions: list[FakeSubscription] = []

    async def consume(
        self,
        queue: str,
        spec: ConsumeSpec,
        deliver: Callable[[Delivery], Awaitable[None]],
    ) -> FakeSubscription:
        if self.fail_after and len(self.tags) >= self.fail_after:
            raise ConnectionError("the broker will not open another channel")
        self.tags.append(spec.tag)
        subscription = FakeSubscription(self, f"{queue}-{len(self.tags)}")
        self.subscriptions.append(subscription)
        return subscription


async def test_a_group_starts_the_number_it_was_asked_for() -> None:
    transport = Counting()
    mq = Connection(transport)

    async with await ConsumerGroup.start(mq, QUEUE, 4, handler) as group:
        assert group.size == 4
        assert group.queue == QUEUE

    assert len(transport.tags) == 4
    assert group.size == 0


async def test_each_consumer_is_named_so_the_broker_can_tell_them_apart() -> None:
    # Four identical rows in the management interface answer nothing about
    # which consumer is holding a message.
    transport = Counting()
    mq = Connection(transport)

    async with await ConsumerGroup.start(mq, QUEUE, 3, handler):
        pass

    assert transport.tags == [
        "acemq-orders.new-1",
        "acemq-orders.new-2",
        "acemq-orders.new-3",
    ]


async def test_a_tag_of_your_own_is_still_numbered() -> None:
    transport = Counting()
    mq = Connection(transport)

    async with await ConsumerGroup.start(mq, QUEUE, 2, handler, tag="shipping"):
        pass

    assert transport.tags == ["shipping-1", "shipping-2"]


async def test_a_group_that_cannot_start_leaves_nothing_running() -> None:
    # A half-started group holds messages nothing is going to finish handling,
    # and the caller that saw the exception has no handle to close it with.
    transport = Counting(fail_after=2)
    mq = Connection(transport)

    with pytest.raises(ConnectionError):
        await ConsumerGroup.start(mq, QUEUE, 4, handler)

    assert len(transport.subscriptions) == 2
    assert all(subscription.released for subscription in transport.subscriptions)


async def test_a_group_needs_at_least_one_consumer() -> None:
    mq = Connection(FakeTransport())
    with pytest.raises(ValueError, match="at least one consumer"):
        await ConsumerGroup.start(mq, QUEUE, 0, handler)


async def test_closing_twice_is_not_an_error() -> None:
    transport = Counting()
    mq = Connection(transport)
    group = await ConsumerGroup.start(mq, QUEUE, 2, handler)

    await group.close()
    await group.close()

    assert group.size == 0
