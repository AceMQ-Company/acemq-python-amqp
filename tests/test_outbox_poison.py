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

"""One record the broker will never take must not stop every record behind it.

The relay publishes in the order the records were written and stops at the first
failure, which is right: the writer chose that order, and skipping ahead would
deliver later messages before earlier ones.

Stopping for ever is a different thing. A record the broker refuses permanently —
an exchange that was deleted, a payload one policy will always reject — is retried
on every sweep and blocks everything behind it indefinitely. Ruby bounds this by
counting attempts and leaving a record alone after ten; this is that bound for
Python.

It is worth being precise about the history here. Until 0.7.2 a refused record was
*deleted*, because the nack raised nothing and the relay took silence for success:
the queue kept moving and the message was gone. Fixing that turned a silent loss
into a head-of-line block, which is better — visible, and recoverable — and still
not finished. This is the rest of it.
"""

from __future__ import annotations

import pytest
from fake_transport import FakeTransport

from acemq_amqp import Connection, Outbound, PublishError, PublishResult
from acemq_amqp.patterns import InMemoryOutboxStore, OutboxRelay, record


class RefusingTransport(FakeTransport):
    """A broker that refuses one routing key for ever and takes everything else."""

    def __init__(self, poison: str) -> None:
        super().__init__()
        self.poison = poison
        self.delivered: list[str] = []

    async def publish(
        self, exchange: str, routing_key: str, message: Outbound
    ) -> PublishResult:
        await super().publish(exchange, routing_key, message)
        if routing_key == self.poison:
            return PublishResult(
                message_id=message.message_id,
                confirmed=False,
                routed=True,
                return_reason="",
            )
        self.delivered.append(routing_key)
        return PublishResult(
            message_id=message.message_id, confirmed=True, routed=True, return_reason=""
        )


async def test_a_poison_record_is_left_alone_and_the_rest_go_out() -> None:
    transport = RefusingTransport(poison="always-refused")
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore(max_attempts=3)

    await store.add(record(mq, "", "always-refused", {"n": 1}))
    await store.add(record(mq, "", "orders", {"n": 2}))
    await store.add(record(mq, "", "orders", {"n": 3}))

    relay = OutboxRelay(mq, store)

    # Each sweep stops at the poison record, as it should: order is what the writer
    # asked for. After max_attempts it is left alone and the queue moves.
    for _ in range(3):
        with pytest.raises(PublishError):
            await relay.sweep()

    published = await relay.sweep()

    assert published == 2, (
        "the two good records behind the poison one never went out. One record the "
        "broker will never take blocks the entire outbox for ever"
    )
    assert transport.delivered == ["orders", "orders"]


async def test_a_retired_record_is_kept_rather_than_deleted() -> None:
    """Left alone, not thrown away.

    A record nothing could publish is evidence: somebody has to be able to read it,
    fix whatever refuses it, and release it. Deleting it would be the silent loss
    this pattern exists to prevent, arrived at by a different route.
    """
    transport = RefusingTransport(poison="always-refused")
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore(max_attempts=2)

    await store.add(record(mq, "", "always-refused", {"n": 1}))
    relay = OutboxRelay(mq, store)

    for _ in range(2):
        with pytest.raises(PublishError):
            await relay.sweep()

    assert await relay.sweep() == 0
    assert len(store) == 1, "the record the broker refused was thrown away"


async def test_the_failure_is_recorded_on_the_record() -> None:
    """Why it was left alone, readable from the record itself.

    An operator finding a stuck record needs the broker's reason without having to
    correlate a log line from ten sweeps ago.
    """
    transport = RefusingTransport(poison="always-refused")
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore(max_attempts=1)

    await store.add(record(mq, "", "always-refused", {"n": 1}))
    relay = OutboxRelay(mq, store)

    with pytest.raises(PublishError):
        await relay.sweep()

    retired = store.retired()
    assert len(retired) == 1
    assert retired[0].attempts >= 1
    assert retired[0].last_error, "nothing says why the record was left alone"


async def test_a_record_that_succeeds_is_not_counted_against_its_attempts() -> None:
    """The good path stays the good path."""
    transport = RefusingTransport(poison="nothing-is-poison-here")
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore(max_attempts=1)

    await store.add(record(mq, "", "orders", {"n": 1}))
    relay = OutboxRelay(mq, store)

    assert await relay.sweep() == 1
    assert len(store) == 0


async def test_a_record_nothing_is_bound_to_stays_in_the_outbox() -> None:
    """Unroutable is a failed publish, not a published one.

    The broker confirms a message no queue is bound to receive and then drops it.
    A relay that publishes without ``mandatory`` takes that confirm as success,
    marks the record published, and the message is gone with nothing anywhere
    saying so. Ruby's relay did exactly this until it started publishing
    mandatory; Java's and .NET's always have.
    """
    transport = FakeTransport()
    mq = Connection(transport, origin="checkout@pod-7")
    store = InMemoryOutboxStore(max_attempts=2)

    # The default exchange with a queue nobody declared: nothing will receive it.
    await store.add(record(mq, "", "nobody-is-bound-here", {"n": 1}))
    relay = OutboxRelay(mq, store)

    with pytest.raises(PublishError) as raised:
        await relay.sweep()

    assert raised.value.unroutable
    assert transport.sent[0].message.mandatory, "the relay published without mandatory"
    assert len(store) == 1, "an unroutable record was marked published and lost"

    # Counted against the record like any other failure, so it retires rather
    # than blocking everything behind it for ever -- and is kept when it does.
    with pytest.raises(PublishError):
        await relay.sweep()
    assert await relay.sweep() == 0
    retired = store.retired()
    assert [entry.attempts for entry in retired] == [2]
    assert "reached no queue" in retired[0].last_error
