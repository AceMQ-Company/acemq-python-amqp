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
    Topology,
    accept,
    connect,
    fixed_retry,
)
from acemq_amqp.patterns import Requester, Scheduler
from acemq_amqp.patterns.requestreply import RequestTimeoutError, ResponderError

OPS = ["hello", "begin", "end", "publish", "consume", "handled", "schedule", "request"]


class UnsupportedOpError(Exception):
    pass


class Driver:
    def __init__(self, url: str) -> None:
        self.url = url
        self.connection: Connection | None = None
        self.consumers: dict[str, Consumer] = {}
        self.counts: dict[str, Counter[str]] = {}
        self.redelivered: dict[str, Counter[str]] = {}
        self.scheduler: Scheduler | None = None
        self.requesters: dict[tuple[str, str], Requester] = {}

    @property
    def mq(self) -> Connection:
        assert self.connection is not None, "begin was not called"
        return self.connection

    async def hello(self, _: dict[str, Any]) -> dict[str, Any]:
        return {"library": "acemq-python-amqp", "version": acemq_amqp.__version__, "ops": OPS}

    async def begin(self, args: dict[str, Any]) -> dict[str, Any]:
        await self.end({})
        origin = f"{args.get('clientName', 'tck')}@{socket.gethostname()}"
        self.connection = await connect(self.url, origin=origin)
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
        self.counts.clear()
        self.redelivered.clear()
        if self.connection is not None:
            await self.connection.close()
            self.connection = None
        return {}

    async def publish(self, args: dict[str, Any]) -> dict[str, Any]:
        fields = args.get("envelope")
        envelope = None
        if fields:
            given = {
                "id": fields.get("id"),
                "type": fields.get("type"),
                "version": fields.get("version"),
                "correlation_id": fields.get("correlationId"),
                "causation_id": fields.get("causationId"),
                "headers": fields.get("headers"),
            }
            envelope = Envelope(**{k: v for k, v in given.items() if v is not None})
        publisher = self.mq.publisher(args.get("exchange", ""), args["routingKey"])
        result = await publisher.send(args["body"], envelope=envelope)
        return {"messageId": result.message_id}

    async def consume(self, args: dict[str, Any]) -> dict[str, Any]:
        queue = args["queue"]
        retry = args.get("retry", {})
        policy = fixed_retry(
            retry.get("maxAttempts", 3), timedelta(milliseconds=retry.get("delayMs", 1000))
        )
        behaviour = args.get("behaviour", {})
        fail_first: dict[str, int] = behaviour.get("failFirst", {})
        sleep = behaviour.get("sleepMs", 0) / 1000
        counts: Counter[str] = Counter()
        redelivered: Counter[str] = Counter()

        async def handle(message: Message) -> Ack:
            ident = str(message.payload.get("id"))
            counts[ident] += 1
            if message.redelivered:
                redelivered[ident] += 1
            if sleep:
                await asyncio.sleep(sleep)
            if counts[ident] <= fail_first.get(ident, 0):
                raise RuntimeError(f"tck: failing {ident} on purpose, delivery {counts[ident]}")
            return accept()

        await self.mq.declare(Topology().queue(queue, dead_letter=True, retry=policy))
        consumer = await self.mq.consume(
            queue, handle, retry=policy, prefetch=args.get("prefetch", 10)
        )
        self.consumers[args["handle"]] = consumer
        self.counts[args["handle"]] = counts
        self.redelivered[args["handle"]] = redelivered
        return {}

    async def handled(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = args["handle"]
        return {
            "counts": dict(self.counts[handle]),
            "redelivered": dict(self.redelivered[handle]),
        }

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
    if isinstance(failure, PublishError):
        return "unroutable" if failure.unroutable else "refused"
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
