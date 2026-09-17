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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest
from fake_transport import FakeSubscription, FakeTransport, Sent

from acemq_amqp import (
    Connection,
    ConsumeSpec,
    Delivery,
    Envelope,
    Message,
    Metrics,
    Outbound,
    PublishResult,
    headers,
)
from acemq_amqp.patterns import (
    HEADER_ERROR,
    HEADER_REPLY_TO,
    Requester,
    RequestTimeoutError,
    ResponderError,
    serve,
)
from acemq_amqp.telemetry import (
    METRIC_REQUEST_DURATION,
    METRIC_REQUEST_TOTAL,
    OUTCOME_ANSWERED,
    OUTCOME_TIMED_OUT,
    TAG_OUTCOME,
    TAG_ROUTING_KEY,
    metric_key,
)
from acemq_amqp.topology import QUEUE_TYPE_ARG, QUORUM_QUEUE_TYPE, Topology

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


@dataclass
class EagerTransport(FakeTransport):
    """A broker that hands a message over from inside the subscribe.

    Which is what a queue with a backlog really looks like from in here, and the
    one case where a counter built after ``connection.consume`` returns is a
    counter the first delivery reads as missing.
    """

    backlog: tuple[str, bytes, Mapping[str, Any]] | None = None
    _settled: asyncio.Event = field(default_factory=asyncio.Event)

    async def consume(
        self,
        queue: str,
        spec: ConsumeSpec,
        deliver: Callable[[Delivery], Awaitable[None]],
    ) -> FakeSubscription:
        subscription = await super().consume(queue, spec, deliver)
        if self.backlog is not None:
            name, body, headers_ = self.backlog
            self.backlog = None

            async def settle(*_: Any) -> None:
                self._settled.set()

            await deliver(
                Delivery(
                    body=body,
                    content_type="application/json",
                    routing_key=name,
                    message_id="",
                    headers=dict(headers_),
                    redelivered=False,
                    ack=settle,
                    nack=settle,
                )
            )
        return subscription

    async def settled(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self._settled.wait(), timeout)


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
        # Neither address: no header, and no native property either.
        await transport.deliver(REQUESTS, b"{}", headers=request(reply_to=None))
    finally:
        await mq.close()

    # Retrying cannot make a return address appear.
    reason = transport.sent_to(f"{REQUESTS}.dlq")[0].headers[headers.ERROR]
    assert HEADER_REPLY_TO in reason
    assert transport.sent_to(REPLIES) == []


async def test_a_request_carrying_only_the_native_property_is_answered() -> None:
    # What a Java or .NET caller sends: those two write AMQP's own reply-to and
    # not the header, so a Python responder that read the header alone could
    # never answer one of them.
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        assert message.reply_to == REPLIES
        return {"pence": 250}

    mq = await serving(transport, price)
    try:
        await transport.deliver(
            REQUESTS, b"{}", headers=request(reply_to=None), reply_to=REPLIES
        )
    finally:
        await mq.close()

    assert json.loads(transport.sent_to(REPLIES)[0].message.body) == {"pence": 250}
    assert transport.sent_to(f"{REQUESTS}.dlq") == []


async def test_a_request_carrying_only_the_header_is_answered() -> None:
    # What a request looks like after a hop through a service that rebuilt the
    # message: the native property is gone and the header is all that is left.
    # It is also what a Go or Ruby caller of an older version sends.
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        assert message.reply_to == ""
        return {"pence": 250}

    mq = await serving(transport, price)
    try:
        await transport.deliver(REQUESTS, b"{}", headers=request(), reply_to="")
    finally:
        await mq.close()

    assert json.loads(transport.sent_to(REPLIES)[0].message.body) == {"pence": 250}
    assert transport.sent_to(f"{REQUESTS}.dlq") == []


async def test_the_header_wins_when_a_request_carries_both_addresses() -> None:
    # Header first, native second — the same order in all five libraries. The
    # order only shows when the two disagree, which is exactly what a service
    # that republished the request under a reply queue of its own produces.
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    mq = await serving(transport, price)
    try:
        await transport.deliver(
            REQUESTS, b"{}", headers=request(), reply_to="somewhere-else"
        )
    finally:
        await mq.close()

    assert len(transport.sent_to(REPLIES)) == 1
    assert transport.sent_to("somewhere-else") == []


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

        # The return address travels twice, and to the same queue. The header is
        # what survives a hop through a service that rebuilds the message; the
        # native property is what a Java or .NET responder reads, and without it
        # neither of those two could answer this caller at all.
        assert went_out.headers[HEADER_REPLY_TO] == caller.reply_queue
        assert went_out.message.reply_to == caller.reply_queue

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
    # And therefore classic. RabbitMQ refuses a quorum queue that is exclusive
    # or auto-deleting, so a reply queue that picked up the quorum default would
    # not be declared at all and request/reply would stop working outright.
    assert QUEUE_TYPE_ARG not in spec.args
    await mq.close()


async def test_a_named_reply_queue_is_declared_to_survive_a_restart() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(mq, "", REQUESTS, reply_queue=REPLIES) as caller:
        assert caller.reply_queue == REPLIES
        spec = transport.queues[REPLIES]

    assert spec.durable is True
    assert spec.exclusive is False
    # A named reply queue is an ordinary durable queue that a responder in
    # another language may declare too, so it is quorum like any other.
    assert spec.args[QUEUE_TYPE_ARG] == QUORUM_QUEUE_TYPE
    await mq.close()


async def test_a_round_trip_is_timed_and_counted_as_answered() -> None:
    transport = FakeTransport()
    metrics = Metrics()
    mq = Connection(transport, observer=metrics)

    async with await Requester.open(
        mq, "", REQUESTS, timeout=timedelta(seconds=5)
    ) as caller:
        asking = asyncio.create_task(caller.ask({"sku": "A-1"}))
        went_out = await sent_to(transport, REQUESTS)
        correlation = str(went_out.headers[headers.CORRELATION])
        await transport.deliver(
            caller.reply_queue,
            b'{"pence": 250}',
            headers=Envelope(correlation_id=correlation).to_headers(),
        )
        assert await asking == {"pence": 250}

    labels = {TAG_ROUTING_KEY: REQUESTS, TAG_OUTCOME: OUTCOME_ANSWERED}
    assert metrics.counts[metric_key(METRIC_REQUEST_TOTAL, labels)] == 1
    # The duration is the round trip as the caller experienced it, so the
    # publish is inside it and the number is real rather than zero.
    timing = metrics.durations[metric_key(METRIC_REQUEST_DURATION, labels)]
    assert timing.count == 1
    assert timing.total > 0

    await mq.close()


async def test_a_call_that_times_out_is_still_timed_and_counted() -> None:
    # A p99 that quietly drops the slowest calls says a service is fast right up
    # to the point where nothing answers at all.
    transport = FakeTransport()
    metrics = Metrics()
    mq = Connection(transport, observer=metrics)

    async with await Requester.open(
        mq, "", REQUESTS, timeout=timedelta(milliseconds=30)
    ) as caller:
        with pytest.raises(RequestTimeoutError):
            await caller.ask({"sku": "A-1"})
        assert caller.timed_out == 1

    labels = {TAG_ROUTING_KEY: REQUESTS, TAG_OUTCOME: OUTCOME_TIMED_OUT}
    assert metrics.counts[metric_key(METRIC_REQUEST_TOTAL, labels)] == 1
    assert metrics.durations[metric_key(METRIC_REQUEST_DURATION, labels)].count == 1

    await mq.close()


async def test_a_reply_that_nobody_is_waiting_for_is_counted_as_unmatched() -> None:
    # unmatched rising alongside timed_out is what separates "the responder is
    # broken" from "the timeout is too short", and from outside they look alike.
    transport = FakeTransport()
    mq = Connection(transport)

    async with await Requester.open(
        mq, "", REQUESTS, timeout=timedelta(milliseconds=30)
    ) as caller:
        with pytest.raises(RequestTimeoutError):
            await caller.ask({"sku": "A-1"})

        await transport.deliver(
            caller.reply_queue,
            b'{"pence": 250}',
            headers=Envelope(correlation_id="nobody-is-waiting").to_headers(),
        )
        assert caller.unmatched == 1
        assert caller.timed_out == 1

    await mq.close()


async def test_a_responder_counts_an_answer_before_the_reply_leaves() -> None:
    # The ordering is the contract. Publishing first leaves a window in which a
    # caller already holding its answer reads answered as zero, which is a
    # dashboard reporting an idle service that is demonstrably working.
    transport = FakeTransport()
    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))
    seen: list[int] = []

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    responder = await serve(mq, REQUESTS, price)

    # The reply publish records what the counter said at the moment it ran.
    original = transport.publish

    async def watching(exchange: str, routing_key: str, message: Outbound) -> PublishResult:
        if routing_key == REPLIES:
            seen.append(responder.answered)
        return await original(exchange, routing_key, message)

    transport.publish = watching  # type: ignore[method-assign]
    try:
        await transport.deliver(REQUESTS, b'{"sku": "A-1"}', headers=request())
    finally:
        await mq.close()

    # Counted before the reply was written, not after.
    assert seen == [1]
    assert responder.answered == 1
    assert responder.unanswerable == 0


async def test_a_reply_that_cannot_be_published_hands_its_increment_back() -> None:
    # So the number counts replies that were sent rather than replies that were
    # attempted, which is what Java and .NET promise about the same counter.
    transport = FakeTransport()
    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    responder = await serve(mq, REQUESTS, price)

    original = transport.publish

    async def refusing(exchange: str, routing_key: str, message: Outbound) -> PublishResult:
        if routing_key == REPLIES:
            raise RuntimeError("the reply queue has gone")
        return await original(exchange, routing_key, message)

    transport.publish = refusing  # type: ignore[method-assign]
    try:
        await transport.deliver(REQUESTS, b'{"sku": "A-1"}', headers=request())
    finally:
        await mq.close()

    assert responder.answered == 0


async def test_a_responder_that_fails_counts_no_answer() -> None:
    # The caller gets a reply, and the reply is what says the responder could
    # not do it. Counting it would make a service failing every request report a
    # perfectly healthy answered rate.
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        raise LookupError("no such sku")

    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))
    responder = await serve(mq, REQUESTS, price)
    try:
        await transport.deliver(REQUESTS, b'{"sku": "A-1"}', headers=request())
    finally:
        await mq.close()

    assert responder.answered == 0
    # The caller was still told, in milliseconds rather than at its deadline.
    assert HEADER_ERROR in transport.sent_to(REPLIES)[0].headers


async def test_a_request_naming_nowhere_to_reply_is_counted_as_unanswerable() -> None:
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))
    responder = await serve(mq, REQUESTS, price)
    try:
        await transport.deliver(REQUESTS, b'{"sku": "A-1"}', headers=request(reply_to=None))
    finally:
        await mq.close()

    # Anything above zero means a caller is publishing where it means to request.
    assert responder.unanswerable == 1
    assert responder.answered == 0


async def test_the_counters_exist_before_the_responder_subscribes() -> None:
    # A broker may hand the first request over from inside the subscribe, which
    # is what a queue with a backlog looks like from in here. The handler reads
    # the counters on that very delivery, so they have to be there already.
    transport = EagerTransport()

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))
    transport.backlog = (REQUESTS, b'{"sku": "A-1"}', request())
    responder = await serve(mq, REQUESTS, price)
    try:
        await transport.settled()
    finally:
        await mq.close()

    # Counted like any other, with no wait before it could be read.
    assert responder.answered == 1
    assert responder.running or responder.closed


async def test_a_responder_is_closed_and_entered_like_the_consumer_it_wraps() -> None:
    transport = FakeTransport()

    async def price(message: Message) -> dict[str, Any]:
        return {"pence": 250}

    mq = Connection(transport)
    await mq.declare(Topology().queue(REQUESTS, dead_letter=True))

    async with await serve(mq, REQUESTS, price) as responder:
        assert responder.queue == REQUESTS
        assert responder.running
        assert responder.in_flight == 0
        assert responder.consumer.queue == REQUESTS
    assert responder.closed
    await mq.close()
