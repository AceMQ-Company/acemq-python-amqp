"""The AceMQ TCK driver for this library.

Started by the TCK runner (github.com/AceMQ-Company/acemq-amqp-tck), which
writes one JSON request per line to stdin and reads one JSON answer per line
from stdout. Every operation goes through the public API; nothing here asserts,
because the runner checks the outcome on the broker. The protocol is
docs/driver-contract.md in the TCK repository.

    python tests/tck/driver.py
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import acemq_amqp
from acemq_amqp import (
    Ack,
    Connection,
    Consumer,
    Envelope,
    FatalError,
    Message,
    PublishError,
    PublishingPausedError,
    Topology,
    accept,
    connect,
    fixed_retry,
)
from acemq_amqp.codec import JsonCodec
from acemq_amqp.patterns import (
    ClaimCheckCodec,
    ConsumerGroup,
    FilesystemClaimCheckStore,
    InMemoryIdempotencyStore,
    InMemoryOutboxStore,
    OutboxRelay,
    Pipeline,
    Requester,
    Scheduler,
    StreamRetention,
    declare_stream,
    from_first,
    from_next,
    idempotent,
    read_stream,
    record,
    replay,
)
from acemq_amqp.patterns.requestreply import RequestTimeoutError, ResponderError

OPS = [
    "hello",
    "begin",
    "end",
    "publish",
    "consume",
    "handled",
    "schedule",
    "request",
    "declareStream",
    "readStream",
    "metrics",
    "replay",
    "pipeline",
    "claimCheck",
    "outbox",
    "consumeGroup",
]


def envelope_from(fields: Mapping[str, Any] | None) -> Envelope | None:
    """The contract's ``envelope`` argument as the library's envelope."""
    if not fields:
        return None
    given = {
        "id": fields.get("id"),
        "type": fields.get("type"),
        "version": fields.get("version"),
        "correlation_id": fields.get("correlationId"),
        "causation_id": fields.get("causationId"),
        "headers": fields.get("headers"),
    }
    return Envelope(**{k: v for k, v in given.items() if v is not None})


class UnsupportedOpError(Exception):
    pass


class Recorder:
    """The telemetry seam, recording every metric name with its labels."""

    def __init__(self) -> None:
        self.samples: list[tuple[str, dict[str, str]]] = []

    def count(self, metric: str, delta: int, labels: Mapping[str, str]) -> None:
        self.samples.append((metric, dict(labels)))

    def observe(self, metric: str, seconds: float, labels: Mapping[str, str]) -> None:
        self.samples.append((metric, dict(labels)))

    def gauge(self, metric: str, value: int, labels: Mapping[str, str]) -> None:
        self.samples.append((metric, dict(labels)))


class Driver:
    def __init__(self, url: str) -> None:
        self.url = url
        self.connection: Connection | None = None
        self.consumers: dict[str, Consumer] = {}
        self.counts: dict[str, Counter[str]] = {}
        self.redelivered: dict[str, Counter[str]] = {}
        self.order: dict[str, list[str]] = {}
        self.recorder = Recorder()
        self.scheduler: Scheduler | None = None
        self.requesters: dict[tuple[str, str], Requester] = {}
        self.pipelines: list[Pipeline] = []
        self.origin = ""
        self.outbox_store: InMemoryOutboxStore | None = None
        self.relay: OutboxRelay | None = None
        self.groups: list[ConsumerGroup] = []

    @property
    def mq(self) -> Connection:
        assert self.connection is not None, "begin was not called"
        return self.connection

    async def hello(self, _: dict[str, Any]) -> dict[str, Any]:
        return {"library": "acemq-python-amqp", "version": acemq_amqp.__version__, "ops": OPS}

    async def begin(self, args: dict[str, Any]) -> dict[str, Any]:
        await self.end({})
        origin = f"{args.get('clientName', 'tck')}@{socket.gethostname()}"
        # Publisher confirms cannot be turned off in this library, so a case's
        # ``confirms: false`` changes nothing here.
        self.recorder = Recorder()
        self.origin = origin
        self.connection = await connect(self.url, origin=origin, observer=self.recorder)
        return {}

    async def end(self, _: dict[str, Any]) -> dict[str, Any]:
        for requester in self.requesters.values():
            await requester.close()
        self.requesters.clear()
        if self.scheduler is not None:
            await self.scheduler.close()
            self.scheduler = None
        for consumer in self.consumers.values():
            await consumer.close()
        self.consumers.clear()
        if self.relay is not None:
            await self.relay.close()
            self.relay = None
        self.outbox_store = None
        for group in self.groups:
            await group.close()
        self.groups.clear()
        for pipeline in self.pipelines:
            await pipeline.close()
        self.pipelines.clear()
        self.counts.clear()
        self.redelivered.clear()
        self.order.clear()
        if self.connection is not None:
            await self.connection.close()
            self.connection = None
        return {}

    async def publish(self, args: dict[str, Any]) -> dict[str, Any]:
        envelope = envelope_from(args.get("envelope"))
        # ``mandatory`` absent is the library's default, which is mandatory.
        options = {} if args.get("mandatory") is None else {"mandatory": args["mandatory"]}
        publisher = self.mq.publisher(args.get("exchange", ""), args["routingKey"], **options)
        result = await publisher.send(args["body"], envelope=envelope)
        return {"messageId": result.message_id}

    def _counting(self, handle: str, behaviour: Mapping[str, Any]) -> Any:
        """The TCK handler: counts ``body.id`` under ``handle``, sleeps, and
        fails the first ``n`` deliveries of an id named in ``failFirst``."""
        fail_first: dict[str, int] = behaviour.get("failFirst", {})
        sleep = behaviour.get("sleepMs", 0) / 1000
        counts: Counter[str] = self.counts.setdefault(handle, Counter())
        redelivered: Counter[str] = self.redelivered.setdefault(handle, Counter())
        order: list[str] = self.order.setdefault(handle, [])

        async def handle_one(message: Message) -> Ack:
            ident = str(message.payload.get("id"))
            counts[ident] += 1
            order.append(ident)
            if message.redelivered:
                redelivered[ident] += 1
            if sleep:
                await asyncio.sleep(sleep)
            if counts[ident] <= fail_first.get(ident, 0):
                raise RuntimeError(f"tck: failing {ident} on purpose, delivery {counts[ident]}")
            return accept()

        return handle_one

    async def consume(self, args: dict[str, Any]) -> dict[str, Any]:
        queue = args["queue"]
        retry = args.get("retry")
        # ``retry`` absent is no policy at all: the consumer exactly as the
        # library builds it when the caller configures none.
        policy = (
            None
            if retry is None
            else fixed_retry(
                retry.get("maxAttempts", 3), timedelta(milliseconds=retry.get("delayMs", 1000))
            )
        )
        handler = self._counting(args["handle"], args.get("behaviour", {}))
        if args.get("idempotent"):
            # Counted inside the wrapper, so a suppressed duplicate is not counted.
            handler = idempotent(InMemoryIdempotencyStore(), handler)

        await self.mq.declare(Topology().queue(queue, dead_letter=True, retry=policy))
        consumer = await self.mq.consume(
            queue, handler, retry=policy, prefetch=args.get("prefetch", 10)
        )
        self.consumers[args["handle"]] = consumer
        return {}

    async def pipeline(self, args: dict[str, Any]) -> dict[str, Any]:
        name = args["name"]
        handle = args["handle"]
        behaviour: Mapping[str, Any] = args.get("behaviour") or {}

        def identity_for(step: str) -> Any:
            # Fails inside the step's own handler, as the contract requires;
            # no retry policy is set on the step or the pipeline.
            counted = self._counting(handle, behaviour.get(step) or {})

            async def identity(message: Message) -> Any:
                await counted(message)
                return message.payload

            return identity

        builder = self.mq.pipeline(name)
        for step in args["steps"]:
            builder.step(step, identity_for(step))
        self.pipelines.append(await builder.build())
        return {}

    async def claimCheck(self, args: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
        # Reopened rather than wrapped: the codec is the connection's, so
        # everything later in the case encodes and decodes through it.
        await self.mq.close()
        codec = ClaimCheckCodec(
            JsonCodec(),
            FilesystemClaimCheckStore(args["directory"]),
            threshold=args["thresholdBytes"],
        )
        self.connection = await connect(
            self.url, origin=self.origin, observer=self.recorder, codec=codec
        )
        return {}

    async def outbox(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.outbox_store is None or self.relay is None:
            self.outbox_store = InMemoryOutboxStore()
            self.relay = OutboxRelay(self.mq, self.outbox_store)
            self.relay.start()
        entry = record(
            self.mq,
            args.get("exchange", ""),
            args["routingKey"],
            args["body"],
            envelope=envelope_from(args.get("envelope")),
        )
        await self.outbox_store.add(entry)
        return {"id": entry.id}

    async def consumeGroup(self, args: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
        handler = self._counting(args["handle"], args.get("behaviour") or {})
        await self.mq.declare(Topology().queue(args["queue"], dead_letter=True, retry=None))
        group = await ConsumerGroup.start(
            self.mq, args["queue"], args["size"], handler, prefetch=args.get("prefetch", 10)
        )
        self.groups.append(group)
        return {}

    async def handled(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = args["handle"]
        return {
            "counts": dict(self.counts[handle]),
            "redelivered": dict(self.redelivered[handle]),
            "order": list(self.order[handle]),
        }

    async def declareStream(self, args: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
        retention = StreamRetention(max_age=timedelta(milliseconds=args["maxAgeMs"]))
        await declare_stream(self.mq, args["queue"], retention)
        return {}

    async def readStream(self, args: dict[str, Any]) -> dict[str, Any]:  # noqa: N802
        handler = self._counting(args["handle"], args.get("behaviour") or {})
        offset = from_first() if args.get("offset", "first") == "first" else from_next()
        consumer = await read_stream(self.mq, args["queue"], handler, offset=offset)
        self.consumers[args["handle"]] = consumer
        return {}

    async def metrics(self, _: dict[str, Any]) -> dict[str, Any]:
        samples = [{"name": name, "tags": tags} for name, tags in self.recorder.samples]
        names = sorted({name for name, _ in self.recorder.samples})
        return {"names": names, "samples": samples}

    async def replay(self, args: dict[str, Any]) -> dict[str, Any]:
        ids = set(args["ids"])

        def only(_: Envelope, body: bytes) -> bool:
            return json.loads(body).get("id") in ids

        result = await replay(
            self.mq, args["from"], exchange="", routing_key=args["queue"], only=only
        )
        return {"replayed": result.moved}

    async def schedule(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.scheduler is None:
            self.scheduler = await Scheduler.open(self.mq)
        await self.scheduler.after(
            timedelta(milliseconds=args["delayMs"]),
            args.get("exchange", ""),
            args["routingKey"],
            args["body"],
        )
        return {}

    async def request(self, args: dict[str, Any]) -> dict[str, Any]:
        key = (args.get("exchange", ""), args["routingKey"])
        requester = self.requesters.get(key)
        if requester is None:
            requester = await Requester.open(
                self.mq, key[0], key[1], timeout=timedelta(milliseconds=args["timeoutMs"])
            )
            self.requesters[key] = requester
        return {"reply": await requester.ask(args["body"])}


def kind_of(failure: BaseException) -> str:
    if isinstance(failure, UnsupportedOpError):
        return "unsupported"
    if isinstance(failure, RequestTimeoutError):
        return "timeout"
    if isinstance(failure, ResponderError):
        return "responder"
    if isinstance(failure, PublishingPausedError):
        return "paused"
    if isinstance(failure, PublishError):
        return "unroutable" if failure.unroutable else "nacked"
    if isinstance(failure, FatalError):
        return "decode"
    return "other"


async def main() -> None:
    driver = Driver(os.environ.get("ACEMQ_TCK_AMQP_URL", "amqp://guest:guest@localhost:5672/"))
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            break
        request = json.loads(line)
        op = request.get("op", "")
        answer: dict[str, Any] = {"id": request.get("id")}
        try:
            if op not in OPS:
                raise UnsupportedOpError(op)
            answer["result"] = await getattr(driver, op)(request)
            answer["ok"] = True
        except Exception as failure:  # every failure is an answer
            answer["ok"] = False
            answer["error"] = {
                "kind": kind_of(failure),
                "type": type(failure).__name__,
                "message": str(failure),
            }
        sys.stdout.write(json.dumps(answer) + "\n")
        sys.stdout.flush()
    await driver.end({})


if __name__ == "__main__":
    asyncio.run(main())
