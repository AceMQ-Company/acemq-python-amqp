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

"""The same library, for programs that are not running an event loop.

Plenty of Python is not asyncio and has no reason to become asyncio because it
sends a message. This is a blocking API for those programs::

    with sync.connect("amqp://localhost/") as mq:
        mq.publisher(routing_key="orders").send({"id": 7})

It is a facade rather than a second implementation. A loop runs on a thread of
its own and every call here is handed to it, so the envelope rules, the codec
negotiation and the retry engine are the ones in
:mod:`acemq_amqp.connection` — there is one set of retry arithmetic in this
library, and it is not this file's.

Handlers run on a worker thread rather than on the loop. A blocking handler
called from the loop would stop every other consumer on the connection, and a
blocking handler is exactly what somebody using this API has: that is why they
are here.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine, Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeAlias, TypeVar

from .ack import Ack
from .codec import Codec
from .connection import DEFAULT_PREFETCH, Connection, Consumer, Message, Publisher
from .connection import connect as _connect_async
from .envelope import Envelope
from .retry import RetryPolicy
from .security import Security
from .topology import Topology
from .transport import PublishResult

T = TypeVar("T")

#: What a blocking handler is: a message in, a decision out.
SyncHandler: TypeAlias = Callable[[Message], Ack]


class _LoopThread:
    """An event loop on a thread, and the way to get work onto it.

    One per connection rather than one per process, so two connections cannot
    starve each other and closing one does not take the other's loop with it.
    """

    def __init__(self, name: str) -> None:
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.call_soon(self._ready.set)
        self._loop.run_forever()

    def run(self, work: Coroutine[Any, Any, T]) -> T:
        """Runs a coroutine on the loop and waits for its answer here."""
        return asyncio.run_coroutine_threadsafe(work, self._loop).result()

    def stop(self) -> None:
        """Stops the loop and waits for its thread.

        Joined rather than left to the daemon flag, so that a connection closed
        in a test is really gone by the time the next test starts rather than
        still holding a socket.
        """
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()


class SyncPublisher:
    """A :class:`~acemq_amqp.Publisher` you can call without awaiting."""

    def __init__(self, loop: _LoopThread, publisher: Publisher) -> None:
        self._loop = loop
        self._publisher = publisher

    def send(
        self,
        payload: Any,
        *,
        envelope: Envelope | None = None,
        routing_key: str | None = None,
    ) -> PublishResult:
        """Publishes one message and waits for the broker to answer.

        :param payload: what to send
        :param envelope: metadata to send instead of a fresh one
        :param routing_key: what to publish under, overriding the publisher's
        :returns: what the broker said
        """
        return self._loop.run(
            self._publisher.send(payload, envelope=envelope, routing_key=routing_key)
        )


class SyncConsumer:
    """A running subscription. Close it to stop."""

    def __init__(
        self, connection: SyncConnection, consumer: Consumer, workers: ThreadPoolExecutor
    ) -> None:
        self._connection = connection
        self._consumer = consumer
        self._workers = workers

    @property
    def queue(self) -> str:
        """The queue this consumer reads."""
        return self._consumer.queue

    def close(self) -> None:
        """Stops the consumer and waits for the handlers already running."""
        self._connection._loop.run(self._consumer.close())
        self._workers.shutdown(wait=True)

    def __enter__(self) -> SyncConsumer:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class SyncConnection:
    """A connection to a broker, without an event loop in sight.

    Everything on it blocks until the broker has answered, which is the point:
    a program that is not running a loop should not have to start one to send a
    message.
    """

    def __init__(self, loop: _LoopThread, connection: Connection) -> None:
        self._loop = loop
        self._connection = connection
        self._closed = False

    @property
    def connection(self) -> Connection:
        """The asyncio connection underneath, for code that has a loop after all."""
        return self._connection

    def declare(self, topology: Topology) -> None:
        """Applies a topology to this connection's broker."""
        self._loop.run(self._connection.declare(topology))

    def publisher(
        self,
        exchange: str = "",
        routing_key: str = "",
        *,
        codec: Codec | None = None,
        persistent: bool = True,
        mandatory: bool = False,
    ) -> SyncPublisher:
        """Builds a publisher for an exchange and routing key."""
        return SyncPublisher(
            self._loop,
            self._connection.publisher(
                exchange,
                routing_key,
                codec=codec,
                persistent=persistent,
                mandatory=mandatory,
            ),
        )

    def consume(
        self,
        queue: str,
        handler: SyncHandler,
        *,
        codec: Codec | None = None,
        retry: RetryPolicy | None = None,
        prefetch: int | None = None,
        concurrency: int = 1,
        tag: str = "",
        args: Mapping[str, Any] | None = None,
    ) -> SyncConsumer:
        """Reads messages from a queue until the returned consumer is closed.

        The handler is called on a worker thread, not on the loop, so it may
        block for as long as it needs to without stopping anything else. There
        are ``concurrency`` threads and ``concurrency`` messages in flight, so
        the number means the same thing here as it does on the async API.

        :param queue: what to read
        :param handler: what to do with a message
        :param codec: a codec other than this connection's
        :param retry: a policy other than this connection's
        :param prefetch: how many unacknowledged messages to hold
        :param concurrency: how many messages to work on at once
        :param tag: what to call this consumer to the broker
        :param args: broker-specific consumer arguments
        :returns: the running consumer
        """
        workers = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix=f"acemq-{queue}"
        )

        async def run_on_a_thread(message: Message) -> Ack:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(workers, handler, message)

        consumer = self._loop.run(
            self._connection.consume(
                queue,
                run_on_a_thread,
                codec=codec,
                retry=retry,
                prefetch=prefetch,
                concurrency=concurrency,
                tag=tag,
                args=args,
            )
        )
        return SyncConsumer(self, consumer, workers)

    def queue_exists(self, queue: str) -> bool:
        """Whether a queue is on the broker, creating nothing."""
        return self._loop.run(self._connection.queue_exists(queue))

    def message_count(self, queue: str) -> int:
        """How many messages are waiting on a queue."""
        return self._loop.run(self._connection.message_count(queue))

    def delete_queue(self, queue: str) -> None:
        """Removes a queue and every message still on it."""
        self._loop.run(self._connection.delete_queue(queue))

    def close(self) -> None:
        """Stops every consumer, releases the connection and stops the loop."""
        if self._closed:
            return
        self._closed = True
        try:
            self._loop.run(self._connection.close())
        finally:
            self._loop.stop()

    def __enter__(self) -> SyncConnection:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def connect(
    url: str,
    *,
    codec: Codec | None = None,
    origin: str | None = None,
    retry: RetryPolicy | None = None,
    prefetch: int = DEFAULT_PREFETCH,
    security: Security | None = None,
    **transport_options: Any,
) -> SyncConnection:
    """Opens a connection to a broker, blocking until it is open.

    Takes the same arguments as :func:`acemq_amqp.connect` and returns the
    blocking shape of the same thing — including ``security``, because a program
    that is not running an event loop has exactly the same broker to reach::

        with sync.connect(
            "amqps://broker.internal:5671/",
            security=Security(
                certificate_authority="certs/ca.crt",
                credentials=credentials_from_environment(),
            ),
        ) as mq:
            ...

    :param url: where the broker is
    :param codec: what publishers and consumers use unless they say otherwise
    :param origin: what to stamp on published messages
    :param retry: what consumers use unless they say otherwise
    :param prefetch: how many unacknowledged messages a consumer holds
    :param security: how to verify the broker and who to log in as
    :param transport_options: passed to the transport
    :returns: the connection
    """
    loop = _LoopThread(name="acemq-loop")
    try:
        connection = loop.run(
            _connect_async(
                url,
                codec=codec,
                origin=origin,
                retry=retry,
                prefetch=prefetch,
                security=security,
                **transport_options,
            )
        )
    except BaseException:
        # The loop outlives a failed connect otherwise, and a thread nobody has
        # a reference to is a thread nobody can stop.
        loop.stop()
        raise
    return SyncConnection(loop, connection)
