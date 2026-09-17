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

"""A queue that keeps what it has already handed out."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    AceMQError,
    Ack,
    Connection,
    FatalError,
    Message,
    accept,
    fixed_retry,
    headers,
    park,
    reject,
    retry,
)
from acemq_amqp.patterns import (
    DEFAULT_STREAM_PREFETCH,
    StreamRetention,
    StreamRetryError,
    declare_stream,
    from_first,
    from_last,
    from_next,
    from_offset,
    from_timestamp,
    read_stream,
    stream,
)
from acemq_amqp.patterns.streams import (
    MAX_AGE_ARG,
    MAX_LENGTH_BYTES_ARG,
    QUEUE_TYPE_ARG,
    SEGMENT_BYTES_ARG,
    STREAM_OFFSET_ARG,
)

NAME = "events"


async def handler(message: Message) -> Ack:
    return accept()


def test_a_stream_is_a_queue_declared_as_one() -> None:
    plan = stream(NAME).plan()

    assert len(plan) == 1
    assert f"{QUEUE_TYPE_ARG}='stream'" in plan[0].detail
    assert "durable" in plan[0].detail


async def test_a_stream_is_never_exclusive_or_auto_deleting() -> None:
    # Getting one of those wrong is answered by the broker with a message that
    # never mentions streams.
    transport = FakeTransport()
    await declare_stream(Connection(transport), NAME)

    spec = transport.queues[NAME]
    assert (spec.durable, spec.exclusive, spec.auto_delete) == (True, False, False)


async def test_retention_is_written_the_way_the_broker_wants_it() -> None:
    transport = FakeTransport()
    await declare_stream(
        Connection(transport),
        NAME,
        StreamRetention(
            max_age=timedelta(days=7), max_bytes=10_000_000, segment_bytes=500_000
        ),
    )

    args = transport.queues[NAME].args
    # A number and a unit suffix, and the units are RabbitMQ's own: D for days
    # but lowercase for the rest.
    assert args[MAX_AGE_ARG] == "7D"
    assert args[MAX_LENGTH_BYTES_ARG] == 10_000_000
    assert args[SEGMENT_BYTES_ARG] == 500_000


def test_every_duration_gets_the_largest_unit_that_fits() -> None:
    def age(length: timedelta) -> str:
        return str(StreamRetention(max_age=length).to_args()[MAX_AGE_ARG])

    assert age(timedelta(days=2)) == "2D"
    assert age(timedelta(hours=3)) == "3h"
    assert age(timedelta(minutes=90)) == "90m"
    assert age(timedelta(seconds=45)) == "45s"


async def test_an_unbounded_stream_says_nothing_about_retention() -> None:
    # Which for a stream means until the disk is full, and is why the docstring
    # says to set one on anything that will run for long.
    transport = FakeTransport()
    await declare_stream(Connection(transport), NAME)

    assert transport.queues[NAME].args == {QUEUE_TYPE_ARG: "stream"}


def test_an_offset_is_the_value_the_broker_understands() -> None:
    at = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)

    assert from_first().to_arg() == "first"
    assert from_next().to_arg() == "next"
    assert from_last().to_arg() == "last"
    assert from_offset(4200).to_arg() == 4200
    assert from_timestamp(at).to_arg() == at

    assert str(from_first()) == "first"
    assert str(from_offset(4200)) == "offset(4200)"


async def test_a_consumer_says_where_it_is_starting() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    consumer = await read_stream(
        mq, NAME, handler, offset=from_first(), prefetch=100, consumer_name="projector"
    )
    await consumer.close()

    spec = transport.specs[NAME]
    assert spec.args == {STREAM_OFFSET_ARG: "first"}
    assert spec.prefetch == 100
    # The name is what makes server-side offset tracking possible.
    assert spec.tag == "projector"


async def test_the_default_is_the_next_message_rather_than_the_history() -> None:
    transport = FakeTransport()
    mq = Connection(transport)

    consumer = await read_stream(mq, NAME, handler)
    await consumer.close()

    assert transport.specs[NAME].args == {STREAM_OFFSET_ARG: "next"}
    assert transport.specs[NAME].prefetch == DEFAULT_STREAM_PREFETCH


async def test_a_stream_consumer_cannot_have_no_prefetch() -> None:
    # RabbitMQ refuses one, and the message it gives back does not mention
    # streams.
    mq = Connection(FakeTransport())
    with pytest.raises(ValueError, match="prefetch of at least 1"):
        await read_stream(mq, NAME, handler, prefetch=0)


async def test_a_handler_asking_to_retry_is_refused_and_nothing_is_appended() -> None:
    # The whole point. A retry on a stream is a republish, and a republish onto
    # a stream appends: a second copy of the message that every other consumer
    # of that log reads as a new one.
    transport = FakeTransport()
    mq = Connection(transport, retry=fixed_retry(3, timedelta(0)))
    await declare_stream(mq, NAME)

    async def gives_up(message: Message) -> Ack:
        return retry(RuntimeError("the pricing service is down"))

    consumer = await read_stream(mq, NAME, gives_up)
    settlement = await transport.deliver(NAME, b'{"id": "7"}')
    await consumer.close()

    # Nothing was published back onto the stream, which is the assertion this
    # test exists for: before the refusal there were three copies here.
    assert transport.sent_to(NAME) == []

    # The copy went to the dead letters instead, carrying the sentence that says
    # what to do about it.
    dead = transport.sent_to(f"{NAME}.dlq")
    assert len(dead) == 1
    reason = str(dead[0].headers[headers.ERROR])
    assert "asked to retry" in reason
    assert f"puts a copy in {NAME}.parked" in reason
    assert "accept() to checkpoint past it" in reason
    assert settlement.acked


async def test_a_handler_that_raises_is_not_retried_onto_the_stream_either() -> None:
    # The second way a retry could arrive. A connection with a retry policy on
    # it used to turn every exception out of a stream handler into a copy on the
    # log, silently: from outside, a stream that is growing looks like one that
    # is busy.
    transport = FakeTransport()
    mq = Connection(transport, retry=fixed_retry(3, timedelta(0)))
    await declare_stream(mq, NAME)

    async def raises(message: Message) -> Ack:
        raise RuntimeError("the pricing service is down")

    consumer = await read_stream(mq, NAME, raises)
    await transport.deliver(NAME, b'{"id": "7"}')
    await consumer.close()

    assert transport.sent_to(NAME) == []
    assert len(transport.sent_to(f"{NAME}.dlq")) == 1


async def test_a_stream_consumer_declares_no_retry_rungs() -> None:
    # It runs on no_retry(), so there are no rungs to declare. A rung queue for
    # a consumer that will never use one is a durable queue nothing ever reads.
    transport = FakeTransport()
    mq = Connection(transport, retry=fixed_retry(3, timedelta(minutes=5)))
    await declare_stream(mq, NAME)

    consumer = await read_stream(mq, NAME, handler)
    await consumer.close()

    assert f"{NAME}.dlq" in transport.queues
    assert f"{NAME}.parked" in transport.queues
    assert [name for name in transport.queues if ".retry." in name] == []


async def test_parking_and_rejecting_still_work_and_leave_the_log_alone() -> None:
    # The two outcomes a failing stream handler does have, and the third that is
    # not offered but works. None of them touches the stream.
    transport = FakeTransport()
    mq = Connection(transport)
    await declare_stream(mq, NAME)

    decisions = iter([park(ValueError("version 9")), reject(ValueError("no such sku"))])

    async def gives_up(message: Message) -> Ack:
        return next(decisions)

    consumer = await read_stream(mq, NAME, gives_up)
    await transport.deliver(NAME, b'{"id": "7"}')
    await transport.deliver(NAME, b'{"id": "8"}')
    await consumer.close()

    assert transport.sent_to(NAME) == []
    assert len(transport.sent_to(f"{NAME}.parked")) == 1
    assert len(transport.sent_to(f"{NAME}.dlq")) == 1


async def test_a_refused_retry_is_a_fatal_error_and_an_acemq_error() -> None:
    # Both, and both are load-bearing. AceMQError is how a caller catches what
    # this library refused; FatalError is what stops the delivery going round a
    # retry ladder that would do the very thing being refused.
    assert issubclass(StreamRetryError, AceMQError)
    assert issubclass(StreamRetryError, FatalError)


async def test_accepting_advances_the_position_and_publishes_nothing() -> None:
    transport = FakeTransport()
    mq = Connection(transport)
    await declare_stream(mq, NAME)

    consumer = await read_stream(mq, NAME, handler)
    settlement = await transport.deliver(NAME, b'{"id": "7"}')
    await consumer.close()

    assert settlement.acked
    assert transport.sent == []
