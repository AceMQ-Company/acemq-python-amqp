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

"""Asking a question over a queue, and what happens when nobody answers."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from typing import Any

import pytest
from fake_transport import FakeTransport, Sent

from acemq_amqp import Connection, Envelope, Message, Outbound, PublishResult, headers
from acemq_amqp.patterns import (
    HEADER_ERROR,
    HEADER_REPLY_TO,
    Requester,
    RequestTimeoutError,
    ResponderError,
    serve,
)
from acemq_amqp.topology import Topology

REQUESTS = "price-requests"
REPLIES = "price-replies"


async def sent_to(transport: FakeTransport, queue: str) -> Sent:
    """The next message published to a queue, once it has been."""
    for _ in range(200):
        published = transport.sent_to(queue)
        if published:
            return published[0]
        await asyncio.sleep(0.005)
    raise AssertionError(f"nothing was published to {queue}")


def request(reply_to: str | None = REPLIES, **fields: Any) -> dict[str, Any]:
    """The headers a request arrives with."""
    application = {} if reply_to is None else {HEADER_REPLY_TO: reply_to}
    return Envelope(headers=application, **fields).to_headers(routing_key=REQUESTS)


async def serving(transport: FakeTransport, respond: Any) -> Connection:
    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))
    await serve(mq, REQUESTS, respond)
    return mq


async def test_an_answer_goes_back_correlated_and_caused_by_the_request() -> None:
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250, "sku": message.payload["sku"]}

    mq = await serving(transport, price)
    try:
        await transport.deliver(
            REQUESTS,
            b'{"sku": "A-1"}',
            headers=request(id="request-1", correlation_id="cart-9"),
        )
    finally:
        await mq.close()

    reply = transport.sent_to(REPLIES)[0]
    assert json.loads(reply.message.body) == {"pence": 250, "sku": "A-1"}
    # The correlation is what pairs the two, and the causation says which
    # message produced this one.
    assert reply.headers[headers.CORRELATION] == "cart-9"
    assert reply.headers[headers.CAUSATION] == "request-1"
    assert HEADER_ERROR not in reply.headers


async def test_a_responder_that_fails_says_so_rather_than_letting_the_caller_wait() -> None:
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        raise LookupError("no such sku")

    mq = await serving(transport, price)
    try:
        settlement = await transport.deliver(REQUESTS, b'{"sku": "A-1"}', headers=request())
    finally:
        await mq.close()

    reply = transport.sent_to(REPLIES)[0]
    assert "LookupError: no such sku" in reply.headers[HEADER_ERROR]
    # Settled rather than retried: the caller has its answer, and another
    # attempt would answer the same question twice.
    assert settlement.acked is True
    assert "the handler rejected it" in transport.sent_to(f"{REQUESTS}.dlq")[0].headers[
        headers.ERROR
    ]


async def test_a_request_with_nowhere_to_reply_is_dead_lettered_not_looped() -> None:
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        raise AssertionError("the responder should not have been reached")

    mq = await serving(transport, price)
    try:
        await transport.deliver(REQUESTS, b"{}", headers=request(reply_to=None))
    finally:
        await mq.close()

    # Retrying cannot make a return address appear.
    reason = transport.sent_to(f"{REQUESTS}.dlq")[0].headers[headers.ERROR]
    assert HEADER_REPLY_TO in reason
    assert transport.sent_to(REPLIES) == []


async def test_an_answer_that_could_not_be_sent_is_tried_again() -> None:
    # The work is done but the answer did not get out. Retrying repeats the
    # work, which is why a responder should be idempotent.
    class Refusing(FakeTransport):
        async def publish(
            self, exchange: str, routing_key: str, message: Outbound
        ) -> PublishResult:
            if routing_key == REPLIES:
                raise ConnectionError("the broker is not answering")
            return await super().publish(exchange, routing_key, message)

    transport = Refusing()

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    mq = await serving(transport, price)
    try:
        await transport.deliver(REQUESTS, b"{}", headers=request())
    finally:
        await mq.close()

    # No policy on this connection, so the retry runs out immediately and says
    # what it could not do — which is the loud version of the same outcome.
    reason = transport.sent_to(f"{REQUESTS}.dlq")[0].headers[headers.ERROR]
    assert "the broker is not answering" in reason


async def test_a_reply_finishes_the_request_that_was_waiting_for_it() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(mq, "", REQUESTS, timeout=timedelta(seconds=5)) as caller:
        asking = asyncio.create_task(caller.ask({"sku": "A-1"}))
        went_out = await sent_to(transport, REQUESTS)

        # The return address travels as an ordinary application header, so it
        # survives a hop through a service that rebuilds the message.
        assert went_out.headers[HEADER_REPLY_TO] == caller.reply_queue

        correlation = str(went_out.headers[headers.CORRELATION])
        await transport.deliver(
            caller.reply_queue,
            b'{"pence": 250}',
            headers=Envelope(correlation_id=correlation).to_headers(),
        )
        assert await asking == {"pence": 250}

    await mq.close()


async def test_a_failure_carried_back_is_raised_rather_than_returned() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(mq, "", REQUESTS, timeout=timedelta(seconds=5)) as caller:
        asking = asyncio.create_task(caller.ask({"sku": "A-1"}))
        went_out = await sent_to(transport, REQUESTS)
        correlation = str(went_out.headers[headers.CORRELATION])

        await transport.deliver(
            caller.reply_queue,
            b"null",
            headers=Envelope(
                correlation_id=correlation, headers={HEADER_ERROR: "LookupError: no such sku"}
            ).to_headers(),
        )

        with pytest.raises(ResponderError, match="no such sku"):
            await asking

    await mq.close()


async def test_silence_becomes_a_timeout_and_says_nothing_about_the_work() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(
        mq, "", REQUESTS, timeout=timedelta(milliseconds=30)
    ) as caller:
        with pytest.raises(RequestTimeoutError, match="no reply"):
            await caller.ask({"sku": "A-1"})

    await mq.close()


async def test_a_reply_nobody_is_waiting_for_is_dropped_rather_than_dead_lettered() -> None:
    # Which is what a reply to a request that already timed out is. The caller
    # has gone; nothing here is wrong.
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(mq, "", REQUESTS) as caller:
        settlement = await transport.deliver(
            caller.reply_queue,
            b'{"pence": 250}',
            headers=Envelope(correlation_id="nobody").to_headers(),
        )

    assert settlement.acked is True
    assert transport.sent == []
    await mq.close()


async def test_closing_a_requester_does_not_leave_a_caller_waiting_out_its_timeout() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    caller = await Requester.open(mq, "", REQUESTS, timeout=timedelta(seconds=30))

    asking = asyncio.create_task(caller.ask({"sku": "A-1"}))
    await sent_to(transport, REQUESTS)
    await caller.close()

    with pytest.raises(RequestTimeoutError, match="closed"):
        await asking
    await mq.close()


async def test_a_generated_reply_queue_belongs_to_this_process_alone() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(mq, "", REQUESTS) as caller:
        spec = transport.queues[caller.reply_queue]

    # Transient, exclusive and auto-deleting: a reply queue that outlived its
    # requester would collect answers nobody is waiting for.
    assert spec.durable is False
    assert spec.exclusive is True
    assert spec.auto_delete is True
    await mq.close()


async def test_a_named_reply_queue_is_declared_to_survive_a_restart() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(mq, "", REQUESTS, reply_queue=REPLIES) as caller:
        assert caller.reply_queue == REPLIES
        spec = transport.queues[REPLIES]

    assert spec.durable is True
    assert spec.exclusive is False
    await mq.close()
