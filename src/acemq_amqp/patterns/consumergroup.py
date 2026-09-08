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

"""Several consumers over one queue, started and stopped together.

Two things it saves. Starting consumers by hand means remembering to close every
one, and a partial shutdown leaves messages held by a consumer nobody is waiting
for. And a group can be sized from configuration, which is the number most often
changed after a service is already running.

**Concurrency, or a group?** ``concurrency`` runs several handlers on one
consumer and one channel, sharing its prefetch. A group runs several consumers,
each with its own channel and its own prefetch. Reach for a group when the
handlers are slow enough that one channel's prefetch is the limit, or when a
fair share across processes matters: the broker round-robins between consumers,
so four here compete evenly with four in another instance rather than one
process taking half the queue.
"""

from __future__ import annotations

from typing import Any

from ..codec import Codec
from ..connection import Connection, Consumer, Handler
from ..retry import RetryPolicy


class ConsumerGroup:
    """Consumers reading one queue, closed as one thing.

    Built by :meth:`start`, because a group that has not started is a group
    holding nothing::

        async with await ConsumerGroup.start(mq, "orders", 4, handle):
            ...
    """

    def __init__(self, queue: str, consumers: list[Consumer]) -> None:
        self._queue = queue
        self._consumers = consumers

    @classmethod
    async def start(
        cls,
        connection: Connection,
        queue: str,
        size: int,
        handler: Handler,
        *,
        codec: Codec | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int | None = None,
        concurrency: int = 1,
        tag: str = "",
        args: Any = None,
        declare: bool = True,
    ) -> ConsumerGroup:
        """Starts ``size`` consumers over one queue.

        If one fails to start, the ones already running are closed before the
        failure is raised. A half-started group is worse than none: it holds
        messages that nothing is going to finish handling, and the caller that
        saw the exception has no handle to close it with.

        :param connection: whose consumers these are
        :param queue: what they read
        :param size: how many
        :param handler: what each does with a message
        :param codec: a codec other than the connection's
        :param retry: a policy other than the connection's
        :param prefetch: how many unacknowledged messages each holds
        :param concurrency: how many messages each works on at once
        :param tag: what to call them to the broker, numbered from 1. Each gets
            its own name so the management interface shows which consumer is
            holding a message rather than four identical rows
        :param args: broker-specific consumer arguments
        :param declare: declare the queues a failed message goes to before
            subscribing. On by default; see
            :meth:`acemq_amqp.Connection.consume`. Every consumer in the group
            declares the same things, which is a few extra round trips at
            start-up and nothing else — the declarations are identical, so all
            but the first are no-ops
        :returns: the running group
        """
        if size < 1:
            raise ValueError(f"acemq: a consumer group needs at least one consumer, got {size}")

        base = tag or f"acemq-{queue}"
        started: list[Consumer] = []
        group = cls(queue, started)
        try:
            for number in range(1, size + 1):
                started.append(
                    await connection.consume(
                        queue,
                        handler,
                        codec=codec,
                        retry=retry,
                        prefetch=prefetch,
                        concurrency=concurrency,
                        tag=f"{base}-{number}",
                        args=args,
                        declare=declare,
                    )
                )
        except BaseException:
            await group.close()
            raise
        return group

    @property
    def queue(self) -> str:
        """What the group is reading."""
        return self._queue

    @property
    def size(self) -> int:
        """How many consumers are running."""
        return len(self._consumers)

    async def close(self) -> None:
        """Stops every consumer and waits for the handlers already running.

        All of them are closed even when one fails, and the first failure is
        raised afterwards. Stopping at the first one would leave the rest
        running after a shutdown the caller believes happened, which is worse
        than the failure that started it.
        """
        stopping = list(self._consumers)
        self._consumers.clear()

        first: BaseException | None = None
        for consumer in stopping:
            try:
                await consumer.close()
            except BaseException as failure:
                first = first or failure
        if first is not None:
            raise first

    async def __aenter__(self) -> ConsumerGroup:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
