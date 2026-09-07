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

"""Keeping one entity's messages in order while everything else runs at once.

A queue delivers in order and a consumer with ``concurrency`` above one stops
honouring that. Usually the right trade; wrong where a later message about the
same thing must not overtake an earlier one — an "order cancelled" handled
before the "order placed" it cancels leaves an order that exists and should not.

The answer is not to give up concurrency, because the messages that must not
overtake each other are only the ones about the same entity. Ordering per key
and concurrency across keys is what this buys.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import TypeAlias

from ..ack import Ack
from ..connection import AsyncHandler, Handler, Message
from ._support import decide

#: The FNV-1a 32-bit offset basis and prime. Named rather than inlined because
#: the two constants are the whole of the agreement between languages.
_FNV_OFFSET = 2166136261
_FNV_PRIME = 16777619
_FNV_MASK = 0xFFFFFFFF

#: Picks the ordering key for a message. Messages sharing a key are handled one
#: at a time and in the order they arrived; messages with different keys may be
#: handled at once.
PartitionKey: TypeAlias = Callable[[Message], str]


def by_header(name: str) -> PartitionKey:
    """Orders by an application header — a tenant, a customer, an aggregate.

    :param name: which header carries the entity's identity
    :returns: a key function
    """

    def key(message: Message) -> str:
        value = message.envelope.headers.get(name)
        return "" if value is None else str(value)

    return key


def by_correlation() -> PartitionKey:
    """Orders by correlation identifier, which keeps one business action's
    messages in sequence."""

    def key(message: Message) -> str:
        return message.envelope.correlation_id

    return key


def ordered(key: PartitionKey, handler: Handler) -> AsyncHandler:
    """Wraps a handler so messages sharing a key are never handled at once::

        consumer = await mq.consume(
            "orders", ordered(by_header("order-id"), handle), concurrency=16
        )

    **What it does not do.** It orders the handling of messages that have
    already been delivered. It cannot reorder ones the broker delivered out of
    order, and with several consumers on one queue it orders only within each
    process — ordering across processes needs the messages to reach the same
    process in the first place, which is a routing decision rather than a
    handler one. :func:`partitioned_routing_key` is for making that decision.

    A message whose key is empty is handled with no ordering at all, because
    there is nothing to order it against. That is a deliberate choice over
    treating "no key" as one shared key, which would serialise every message
    that happens to be missing the header — turning a producer's bug into a
    consumer's outage.

    :param key: what identifies the entity a message is about
    :param handler: what to do with the message
    :returns: the wrapped handler
    """
    locks = _KeyedLocks()

    async def in_order(message: Message) -> Ack:
        identity = key(message)
        if not identity:
            return await decide(handler, message)
        async with locks.hold(identity):
            return await decide(handler, message)

    return in_order


def partition(key: str, count: int) -> int:
    """Maps a key onto one of ``count`` slots, the same way in every language.

    For deciding which queue or which worker a message belongs to, when the
    ordering has to hold across processes rather than within one. FNV-1a rather
    than :func:`hash`, because Python's string hash is randomised per process by
    default: two workers in the same deployment would disagree about where a key
    belongs, which is the one thing a partition function must not do.

    :param key: what identifies the entity
    :param count: how many slots there are
    :returns: the slot, from 0
    """
    if count <= 1:
        return 0
    digest = _FNV_OFFSET
    for byte in key.encode("utf-8"):
        digest ^= byte
        digest = (digest * _FNV_PRIME) & _FNV_MASK
    return digest % count


def partitioned_routing_key(base: str, key: str, partitions: int) -> str:
    """``orders`` and an order id become ``orders.3``.

    For publishing into a queue-per-partition arrangement, which is how ordering
    is made to hold across processes: every message about one entity is routed
    to one queue, and one queue is consumed in order.

    :param base: the routing key without the partition
    :param key: what identifies the entity
    :param partitions: how many there are
    :returns: the routing key
    """
    return f"{base}.{partition(key, partitions)}"


class _KeyedLocks:
    """One lock per key, forgotten once nothing is holding it.

    The forgetting is the point. A key is usually an order or a customer, so a
    map that kept one lock per key ever seen would grow for the life of the
    process — the leak that only shows up in the service that has been running
    longest.
    """

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._holders: dict[str, int] = {}

    @asynccontextmanager
    async def hold(self, key: str) -> AsyncIterator[None]:
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = asyncio.Lock()
        # Counted before the wait rather than after it, so a key with somebody
        # queued on it is not forgotten by the holder that releases first.
        self._holders[key] = self._holders.get(key, 0) + 1
        try:
            await lock.acquire()
        except BaseException:
            self._let_go(key)
            raise
        try:
            yield
        finally:
            lock.release()
            self._let_go(key)

    def _let_go(self, key: str) -> None:
        remaining = self._holders[key] - 1
        if remaining:
            self._holders[key] = remaining
            return
        del self._holders[key]
        del self._locks[key]

    def __len__(self) -> int:
        """How many keys are being held, for a test that wants to know the
        forgetting works."""
        return len(self._locks)
