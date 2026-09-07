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

"""Handling a message once, however many times it arrives."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from acemq_amqp import Ack, Envelope, Message, accept, reject, retry
from acemq_amqp.patterns import InMemoryIdempotencyStore, idempotent

CONTENT_TYPE = "application/json"


def message(identifier: str, payload: object = None) -> Message:
    return Message(
        payload=payload if payload is not None else {"id": identifier},
        envelope=Envelope(id=identifier),
        routing_key="orders.new",
        content_type=CONTENT_TYPE,
        redelivered=False,
        body=b"{}",
    )


async def test_the_second_delivery_of_a_message_does_not_run_the_handler() -> None:
    ran: list[str] = []

    async def handler(incoming: Message) -> Ack:
        ran.append(incoming.envelope.id)
        return accept()

    guarded = idempotent(InMemoryIdempotencyStore(), handler)

    first = await guarded(message("order-1"))
    second = await guarded(message("order-1"))

    assert ran == ["order-1"]
    # Accepted, not rejected: the work was done, so the message has been
    # handled, and dead-lettering it would raise an alarm about something that
    # went right.
    assert first == accept()
    assert second == accept()


async def test_a_handler_that_failed_is_run_again_next_time() -> None:
    attempts: list[int] = []

    async def handler(incoming: Message) -> Ack:
        attempts.append(incoming.envelope.attempt)
        return retry(RuntimeError("the database is down"))

    store = InMemoryIdempotencyStore()
    guarded = idempotent(store, handler)

    await guarded(message("order-1"))
    await guarded(message("order-1"))

    # Remembering a message that then failed would mean the retry silently does
    # nothing, which is the worst of both answers.
    assert len(attempts) == 2
    assert len(store) == 0


async def test_a_rejection_is_forgotten_too() -> None:
    async def handler(incoming: Message) -> Ack:
        return reject(ValueError("no customer"))

    store = InMemoryIdempotencyStore()
    await idempotent(store, handler)(message("order-1"))

    assert len(store) == 0


async def test_a_key_of_your_own_deduplicates_on_what_the_payload_says() -> None:
    ran: list[str] = []

    async def handler(incoming: Message) -> Ack:
        ran.append(incoming.envelope.id)
        return accept()

    guarded = idempotent(
        InMemoryIdempotencyStore(), handler, key=lambda m: str(m.payload["order"])
    )

    # Two different messages about the same order: handling either twice is the
    # thing to prevent, and the envelope id would not have caught it.
    await guarded(message("message-1", {"order": "order-9"}))
    await guarded(message("message-2", {"order": "order-9"}))

    assert ran == ["message-1"]


async def test_an_empty_key_is_refused_rather_than_quietly_replaced() -> None:
    async def handler(incoming: Message) -> Ack:
        raise AssertionError("the handler should not have been reached")

    guarded = idempotent(InMemoryIdempotencyStore(), handler, key=lambda m: "")
    decision = await guarded(message("order-1"))

    # Falling back to the envelope id would silently stop deduplicating on the
    # thing that was asked for.
    assert decision.action.value == "reject"
    assert "empty idempotency key" in str(decision.error)


async def test_a_store_that_is_down_asks_for_a_retry_rather_than_risking_a_duplicate() -> (
    None
):
    class Broken:
        async def first_time(self, key: str) -> bool:
            raise ConnectionError("the store is not answering")

        async def forget(self, key: str) -> None:
            return None

    ran: list[str] = []

    async def handler(incoming: Message) -> Ack:
        ran.append(incoming.envelope.id)
        return accept()

    decision = await idempotent(Broken(), handler)(message("order-1"))

    assert ran == []
    assert decision.action.value == "retry"
    assert "the store is not answering" in str(decision.error)


async def test_a_plain_handler_is_wrapped_as_readily_as_a_coroutine_one() -> None:
    def handler(incoming: Message) -> Ack:
        return accept()

    guarded = idempotent(InMemoryIdempotencyStore(), handler)

    assert await guarded(message("order-1")) == accept()
    assert await guarded(message("order-1")) == accept()


async def test_a_key_is_forgotten_once_its_window_has_passed() -> None:
    store = InMemoryIdempotencyStore(ttl=timedelta(milliseconds=20))
    ran: list[str] = []

    async def handler(incoming: Message) -> Ack:
        ran.append(incoming.envelope.id)
        return accept()

    guarded = idempotent(store, handler)
    await guarded(message("order-1"))
    await asyncio.sleep(0.05)
    # The sweep happens on the next call rather than on a timer, so the store
    # owns no task and has nothing to close.
    await guarded(message("order-2"))

    assert ran == ["order-1", "order-2"]
    assert len(store) == 1
