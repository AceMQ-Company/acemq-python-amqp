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

"""A pipeline declared in one place: an exchange, a queue per step, a consumer on each."""

from __future__ import annotations

import json
import threading
from typing import Any

import pytest
from fake_transport import FakeTransport

from acemq_amqp import (
    METRIC_PIPELINE_RUN_TOTAL,
    Connection,
    Message,
    Metrics,
    headers,
    sync,
)
from acemq_amqp.patterns import HEADER_ROUTING_SLIP, NOTHING, Pipeline, RoutingSlip
from acemq_amqp.telemetry import (
    OUTCOME_ENDED_EARLY,
    TAG_OUTCOME,
    TAG_PIPELINE,
    TAG_STEP,
    metric_key,
)

STEPS = ("validate", "enrich", "dispatch")


def same(message: Message) -> Any:
    return message.payload


async def built(mq: Connection, handler: Any = same) -> Any:
    builder = mq.pipeline("fulfilment")
    for step in STEPS:
        builder.step(step, handler)
    return await builder.build()


async def test_building_declares_the_java_topology_and_a_consumer_per_step() -> None:
    transport = FakeTransport()
    pipeline = await built(Connection(transport))

    assert transport.exchanges["fulfilment"].kind == "direct"
    for step in STEPS:
        assert f"fulfilment.{step}" in transport.queues
        assert (f"fulfilment.{step}", "fulfilment", step) in transport.bindings
        assert f"fulfilment.{step}" in transport.consumers
    assert pipeline.steps == STEPS
    assert pipeline.queue_for("enrich") == "fulfilment.enrich"
    await pipeline.close()


async def test_sending_starts_a_declared_route_and_returns_its_run() -> None:
    transport = FakeTransport()
    pipeline = await built(Connection(transport))

    run_id = await pipeline.send({"order": "A-1"})

    sent = transport.sent[-1]
    assert (sent.exchange, sent.routing_key) == ("fulfilment", "validate")
    assert sent.headers[headers.ROUTE] == "validate,enrich,dispatch"
    assert sent.headers[headers.ROUTE_POSITION] == 0
    assert sent.headers[headers.ROUTE_ID] == run_id != ""
    await pipeline.close()


async def test_a_step_moves_the_route_on_and_names_its_cause() -> None:
    transport = FakeTransport()
    pipeline = await built(Connection(transport))

    settled = await transport.deliver(
        "fulfilment.validate",
        b'{"order": "A-1"}',
        headers={
            headers.ROUTE: "validate,enrich,dispatch",
            headers.ROUTE_POSITION: 0,
            headers.ROUTE_ID: "run-7",
            headers.CORRELATION: "cart-9",
        },
        message_id="m-1",
    )

    assert settled.acked
    sent = transport.sent[-1]
    assert (sent.exchange, sent.routing_key) == ("fulfilment", "enrich")
    assert sent.headers[headers.ROUTE_POSITION] == 1
    assert sent.headers[headers.ROUTE_ID] == "run-7"
    assert sent.headers[headers.CORRELATION] == "cart-9"
    assert sent.headers[headers.CAUSATION] == "m-1"
    await pipeline.close()


async def test_an_itinerary_is_followed_wherever_it_goes() -> None:
    transport = FakeTransport()
    pipeline = await built(Connection(transport))
    slip = RoutingSlip().then("fulfilment", "enrich").then("audit", "audit.write")

    await transport.deliver(
        "fulfilment.enrich", b"{}", headers={HEADER_ROUTING_SLIP: slip.to_header()}
    )

    sent = transport.sent[-1]
    assert (sent.exchange, sent.routing_key) == ("audit", "audit.write")
    assert headers.ROUTE not in sent.headers
    assert [s["routingKey"] for s in json.loads(sent.headers[HEADER_ROUTING_SLIP])["done"]] == [
        "enrich"
    ]
    await pipeline.close()


async def test_a_message_with_no_slip_is_at_the_step_whose_queue_it_is_on() -> None:
    transport = FakeTransport()
    pipeline = await built(Connection(transport))

    await transport.deliver("fulfilment.enrich", b"{}")

    sent = transport.sent[-1]
    assert (sent.exchange, sent.routing_key) == ("fulfilment", "dispatch")
    assert sent.headers[headers.ROUTE_POSITION] == 2
    await pipeline.close()


async def test_returning_nothing_ends_the_run_early_and_says_so() -> None:
    transport = FakeTransport()
    metrics = Metrics()
    pipeline = await built(Connection(transport, observer=metrics), lambda _: NOTHING)

    settled = await transport.deliver("fulfilment.validate", b"{}")

    assert settled.acked
    assert transport.sent == []
    labels = {
        TAG_PIPELINE: "fulfilment",
        TAG_STEP: "validate",
        TAG_OUTCOME: OUTCOME_ENDED_EARLY,
    }
    assert metrics.counts[metric_key(METRIC_PIPELINE_RUN_TOTAL, labels)] == 1
    await pipeline.close()


def test_a_pipeline_that_cannot_work_fails_where_it_is_written() -> None:
    mq = Connection(FakeTransport())
    with pytest.raises(ValueError, match="needs a name"):
        mq.pipeline("")
    with pytest.raises(ValueError, match="cannot be a routing key"):
        mq.pipeline("p").step("has space", same)
    with pytest.raises(ValueError, match="twice"):
        mq.pipeline("p").step("a", same).step("a", same)
    with pytest.raises(ValueError, match="no step 'b'"):
        Pipeline(mq, "p", ("a",)).queue_for("b")


async def test_a_pipeline_with_no_steps_is_refused() -> None:
    with pytest.raises(ValueError, match="has no steps"):
        await Connection(FakeTransport()).pipeline("p").build()


def test_the_blocking_api_runs_each_step_on_a_thread() -> None:
    transport = FakeTransport()
    loop = sync._LoopThread(name="acemq-test-loop")

    async def connect() -> Connection:
        return Connection(transport)

    mq = sync.SyncConnection(loop, loop.run(connect()))
    threads: list[str] = []

    def step(message: Message) -> Any:
        threads.append(threading.current_thread().name)
        return message.payload

    try:
        with mq.pipeline("fulfilment").step("validate", step).step("ship", step).build() as p:
            assert p.send({"order": "A-1"})
            loop.run(transport.deliver("fulfilment.validate", b"{}"))
        assert transport.sent[-1].routing_key == "ship"
        assert threads and threads[0] != "acemq-test-loop"
    finally:
        mq.close(timeout=0)
