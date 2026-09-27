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

"""A broker that refuses a message says so, and is heard.

A nack is the broker declining responsibility for a message it was asked to take:
it reached the socket, and the broker will not keep it. Every other library in this
family raises on one. This one used to return it as data -- ``PublishResult.confirmed``
was ``False`` and nothing in the library ever read that field except a telemetry
label -- so a caller who did not inspect the result carried on believing the message
had gone somewhere.

The outbox is where that became message loss rather than merely a wrong belief. Its
relay publishes and then removes the record, and it removes it on the strength of the
publish not raising. A nack raised nothing, so the record was deleted for a message
the broker had refused: at-most-once, in the one pattern whose entire purpose is not
losing anything.
"""

from __future__ import annotations

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    Connection,
    Outbound,
    PublishError,
    PublishResult,
)
from acemq_amqp.patterns import InMemoryOutboxStore, OutboxRelay, record


class RefusingTransport(FakeTransport):
    """A broker that takes the message and declines responsibility for it.

    Routed, which is the point: an unroutable message was already reported and is a
    different failure. This is the broker saying it will not keep a message it could
    perfectly well route -- a full quorum queue, a disk alarm mid-publish -- which is
    the case that went unheard.
    """

    def __init__(self, refuse_after: int = 0) -> None:
        super().__init__()
        self.refuse_after = refuse_after
        self.published = 0

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        await super().publish(exchange, routing_key, message)
        self.published += 1
        confirmed = self.published <= self.refuse_after
        return PublishResult(
            message_id=message.message_id,
            confirmed=confirmed,
            routed=True,
            return_reason="",
        )


async def test_a_nacked_publish_raises() -> None:
    transport = RefusingTransport()
    mq = Connection(transport, origin="checkout@pod-7")

    publisher = mq.publisher(routing_key="orders")

    with pytest.raises(PublishError) as refused:
        await publisher.send({"order": "A-1"})

    # The sentence has to say which message and that it was refused, because the
    # caller's next decision -- retry, park, alert -- depends on knowing the broker
    # declined rather than that the network broke.
    message = str(refused.value)
    assert "refus" in message.lower() or "not confirm" in message.lower(), message
    assert refused.value.unroutable is False, (
        "a nack is not an unroutable message: the broker could route it and would "
        "not keep it, and the two want different responses"
    )


async def test_a_nacked_publish_raw_raises() -> None:
    """The path the retry hops, the dead letters, the replays and the outbox use.

    ``publish_raw`` is documented as the one place every publish in the library comes
    through, which is exactly why the check belongs there: a nack on a dead-letter hop
    or an outbox record has to be heard by whoever is about to settle or delete
    something on the strength of it.
    """
    transport = RefusingTransport()
    mq = Connection(transport, origin="checkout@pod-7")

    with pytest.raises(PublishError):
        await mq.publish_raw(
            "", "orders", Outbound(body=b"{}", content_type="application/json")
        )


async def test_a_confirmed_publish_still_returns_normally() -> None:
    """The other half: nothing about this may make a good publish noisy."""
    transport = RefusingTransport(refuse_after=5)
    mq = Connection(transport, origin="checkout@pod-7")

    result = await mq.publish_raw(
        "", "orders", Outbound(body=b"{}", content_type="application/json")
    )
    assert result.confirmed is True


async def test_the_outbox_keeps_a_record_the_broker_refused() -> None:
    """The loss this fixes, at the level it was lost.

    The relay publishes, then removes the record. It removes it because the publish
    did not raise -- so a nack that raised nothing deleted a record for a message the
    broker had refused to keep. The record must survive, so the next sweep tries again.
    """
    transport = RefusingTransport()
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore()

    await store.add(record(mq, "", "orders", {"order": "A-1"}))
    assert len(store) == 1

    relay = OutboxRelay(mq, store)

    # The sweep is documented as raising whatever the broker raised, and as leaving
    # the records alone when it does: "a relay that fails loses nothing".
    with pytest.raises(PublishError):
        await relay.sweep()

    assert len(store) == 1, (
        "the record was deleted for a message the broker refused to keep. The outbox "
        "is the pattern that exists to make publishing survive a failure, and this is "
        "at-most-once"
    )


async def test_the_outbox_removes_a_record_the_broker_confirmed() -> None:
    """And the good path, so the fix cannot be a relay that never makes progress."""
    transport = RefusingTransport(refuse_after=1)
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore()

    await store.add(record(mq, "", "orders", {"order": "A-1"}))

    relay = OutboxRelay(mq, store)
    published = await relay.sweep()

    assert published == 1
    assert len(store) == 0
