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

"""Handling a message once, however many times it arrives.

At-least-once delivery is not a flaw to be worked around; it is the only
guarantee a broker can give cheaply, and every AceMQ retry is a redelivery on
purpose. So a handler that changes anything needs a way to tell that it has seen
a message before. ``x-acemq-id`` is stable across every redelivery of the same
message, which makes it the natural key.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Protocol, runtime_checkable

from ..ack import Ack, Action, FatalError, accept, reject, retry
from ..connection import AsyncHandler, Handler, Message
from ._support import decide

log = logging.getLogger("acemq")

#: How long the in-memory store remembers a key when nothing says otherwise.
DEFAULT_MEMORY = timedelta(hours=1)


@runtime_checkable
class IdempotencyStore(Protocol):
    """Remembers which messages have been handled."""

    async def first_time(self, key: str) -> bool:
        """Records a key and says whether it had not been seen before.

        It has to be atomic. Two consumers handling the same message at the same
        moment must not both be told they are first, which is the entire job:
        a check followed by a write is not this, however short the gap looks.

        :param key: what identifies the message
        :returns: whether this is the first time
        """

    async def forget(self, key: str) -> None:
        """Removes a key, so a message that failed can be tried again.

        :param key: what identifies the message
        """


class InMemoryIdempotencyStore:
    """Remembers keys in this process, for a while.

    Right behind one consumer and wrong the moment there are two: each has its
    own memory, so both will believe they are first. It is also lost on restart,
    which turns every message in flight into a duplicate.

    Use it in tests and in a single-worker service. Anything else wants a store
    the workers share — ideally the same database the work is written to, in the
    same transaction, which is the only arrangement that actually holds.

    :param ttl: how long a key is remembered. Without a window the memory grows
        for as long as the process lives, so there has to be one; it should be
        comfortably longer than the longest a message can go on being retried
    """

    def __init__(self, ttl: timedelta = DEFAULT_MEMORY) -> None:
        if ttl <= timedelta(0):
            raise ValueError(f"acemq: an idempotency window must be positive, got {ttl}")
        self._ttl = ttl
        self._seen: dict[str, datetime] = {}
        # A plain lock rather than an asyncio one: it is held for a dictionary
        # operation and never across an await, and this way a store shared by
        # the blocking API's worker threads is safe too.
        self._lock = threading.Lock()

    async def first_time(self, key: str) -> bool:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._sweep(now)
            if key in self._seen:
                return False
            self._seen[key] = now
            return True

    async def forget(self, key: str) -> None:
        with self._lock:
            self._seen.pop(key, None)

    def _sweep(self, now: datetime) -> None:
        """Drops what has aged out, from the front.

        Swept here rather than on a timer, so the store owns no task and has
        nothing to close. It costs only what it removes: keys are inserted in
        the order they are first seen and never reordered, so everything expired
        is a prefix and the walk stops at the first key still inside the window.
        """
        cutoff = now - self._ttl
        while self._seen:
            oldest = next(iter(self._seen))
            if self._seen[oldest] > cutoff:
                return
            del self._seen[oldest]

    def __len__(self) -> int:
        """How many keys are remembered, for a test that wants to know the
        window is doing its job."""
        with self._lock:
            return len(self._seen)


def idempotent(
    store: IdempotencyStore,
    handler: Handler,
    *,
    key: Callable[[Message], str] | None = None,
) -> AsyncHandler:
    """Wraps a handler so a message already handled is accepted, not redone::

        consumer = await mq.consume("orders", idempotent(store, place))

    A duplicate is **accepted** rather than rejected. The work was done, so the
    message has been handled, and dead-lettering it would raise an alarm about
    something that went right.

    When the handler does not accept, the key is forgotten so the retry can
    actually run. When it does accept and the store has a ``confirm`` method,
    that is called: a store which hands out a *lease* rather than a fact — so a
    consumer that dies holding a message does not block its redelivery — needs
    to be told when the lease becomes a fact. A store without one, such as
    :class:`InMemoryIdempotencyStore`, is unaffected.

    That ordering is what makes this a guard against duplicates
    rather than a promise of exactly-once: between the handler finishing and the
    acknowledgement reaching the broker there is still a gap where a crash leaves
    a message that will be delivered again. Only a store written in the same
    transaction as the work closes it, which is why :class:`IdempotencyStore` is
    an interface and not a class.

    :param store: where handled keys are remembered
    :param handler: what to do with a message that has not been handled yet
    :param key: what identifies a message, when the envelope's id is not it.
        Use it where the natural key is in the payload — an order identifier two
        different messages both carry, where handling either twice is the thing
        to prevent
    :returns: the wrapped handler
    """

    async def guarded(message: Message) -> Ack:
        identity = message.envelope.id if key is None else key(message)
        if not identity:
            # Refused rather than guessed at. Falling back to the envelope id
            # would silently stop deduplicating on the thing that was asked for.
            return reject(
                FatalError(
                    f"acemq: message {message.envelope.id} produced an empty "
                    "idempotency key"
                )
            )

        try:
            first = await store.first_time(identity)
        except Exception as failure:
            # The store is what is broken, not the message. Retrying is right;
            # carrying on and risking a duplicate is not.
            return retry(failure)

        if not first:
            return accept()

        decision = await decide(handler, message)
        if decision.action is not Action.ACCEPT:
            # It did not work, so it has not been handled. Forgetting is what
            # lets the retry do anything at all.
            await _forget_quietly(store, identity)
        else:
            await _confirm_quietly(store, identity)
        return decision

    return guarded


async def _confirm_quietly(store: IdempotencyStore, key: str) -> None:
    """Tells a store that keeps a claim separate from a fact that the work is done.

    :class:`IdempotencyStore` has two methods and this is not one of them,
    because most stores do not need it: recording a key *is* remembering the
    message, as it is in :class:`InMemoryIdempotencyStore`. A store that hands
    out a **lease** rather than a fact — which is what
    :class:`~acemq_amqp.patterns.sql.SqlIdempotencyStore` does, so a consumer
    that dies holding a message does not block its redelivery — needs to be told
    when the lease becomes a fact, and this is where it is told.

    Duck-typed rather than a second Protocol, so a store that does not have it
    is unaffected and nobody has to implement a method meaning "nothing".

    A failure here is logged rather than raised: the handler accepted, the work
    happened, and turning a bookkeeping failure into a rejection would undo it.
    The lease expiring is the safe outcome — the message may be handled twice,
    which is what an idempotent handler is for.
    """
    confirm = getattr(store, "confirm", None)
    if confirm is None:
        return
    try:
        await confirm(key)
    except Exception:
        log.exception(
            "acemq: could not confirm the idempotency key %s; its claim will expire "
            "and a redelivery may be handled again",
            key,
        )


async def _forget_quietly(store: IdempotencyStore, key: str) -> None:
    """Forgets a key without letting the store's failure replace the handler's.

    The handler has already decided and its reason is the one worth carrying, so
    this failure is logged rather than raised. It is worth logging loudly: a key
    that could not be forgotten turns every remaining attempt at that message
    into a silent no-op.
    """
    try:
        await store.forget(key)
    except Exception:
        log.exception("acemq: could not forget the idempotency key %s after a failure", key)
