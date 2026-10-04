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
from collections.abc import Callable, Coroutine, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any, TypeAlias, TypeVar

from .ack import Ack
from .codec import Codec
from .connection import (
    DEFAULT_CONFIRM_TIMEOUT,
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_MAX_OUTSTANDING_PUBLISHES,
    DEFAULT_PREFETCH,
    Connection,
    Consumer,
    Message,
    Publisher,
)
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

    def send_all(self, payloads: Iterable[Any]) -> list[PublishResult]:
        """Publishes a batch and waits for every confirm.

        Every message goes out before any confirm is awaited, which is the
        whole point of it: a loop calling :meth:`send` pays a broker round trip
        per message. See :meth:`acemq_amqp.Publisher.send_all` for what a
        partial failure reports and why it is not atomic.

        :param payloads: what to send, in order
        :returns: what the broker said about each, in the order the payloads
            were given
        :raises PublishError: when any message was not confirmed
        """
        return self._loop.run(self._publisher.send_all(payloads))


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

    def close(self, *, timeout: float | None = DEFAULT_DRAIN_TIMEOUT) -> bool:
        """Stops the consumer and waits, for up to ``timeout``, for the handlers
        already running.

        Delivery stops first, and a message that had been delivered but not
        started is given back to the broker. A handler that finishes in time is
        settled as usual — see :meth:`acemq_amqp.Consumer.close`.

        **A handler still running at the deadline is abandoned, not stopped.**
        Python cannot cancel a thread, so it keeps running until the handler
        returns of its own accord, and whatever it returns is thrown away. Its
        message is left unsettled — never acknowledged, never rejected, never
        dead-lettered — so the broker hands it out again once the channel goes:
        a handler that was cut off may have done its work, and the message may
        be handled twice, which is the at-least-once promise and not a loss.
        The settlement belongs to the consumer, not to the thread, so a thread
        that finishes late has nothing to acknowledge with and cannot settle a
        message on a channel that has closed or been replaced.

        The abandoned thread is a normal, non-daemon thread: the interpreter
        waits for it at exit. A handler that can hang should carry its own
        timeout.

        :param timeout: seconds to wait for every running handler together;
            ``None`` waits for as long as they take
        :returns: ``True`` if every handler finished, ``False`` if the time ran
            out first and some were abandoned — the consumer is stopped either way
        """
        try:
            return self._connection._loop.run(self._consumer.close(timeout=timeout))
        finally:
            # Not waited for: a thread still running here is one the deadline
            # has already given up on.
            self._workers.shutdown(wait=False, cancel_futures=True)

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
        self._workers: list[ThreadPoolExecutor] = []

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
        declare: bool = True,
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
        :param declare: declare ``{queue}.dlq``, ``{queue}.parked`` and the
            rungs before subscribing. On by default; see
            :meth:`acemq_amqp.Connection.consume`
        :returns: the running consumer
        """
        workers = ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix=f"acemq-{queue}"
        )
        self._workers.append(workers)

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
                declare=declare,
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

    def close(self, *, timeout: float | None = DEFAULT_DRAIN_TIMEOUT) -> bool:
        """Stops every consumer, releases the connection and stops the loop.

        Every consumer's running handlers share one deadline, ``timeout``. A
        handler still running at it is abandoned exactly as
        :meth:`SyncConsumer.close` describes: its thread keeps running, its
        result is discarded and its message is left unsettled for the broker
        to redeliver.

        :param timeout: seconds to wait for the running handlers; ``None``
            waits for as long as they take
        :returns: ``True`` if every handler finished, ``False`` if the time ran
            out first. Closing again returns ``True``
        """
        if self._closed:
            return True
        self._closed = True
        try:
            return self._loop.run(self._connection.close(timeout=timeout))
        finally:
            self._loop.stop()
            for workers in self._workers:
                workers.shutdown(wait=False, cancel_futures=True)

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
    max_outstanding_publishes: int = DEFAULT_MAX_OUTSTANDING_PUBLISHES,
    confirm_timeout: timedelta = DEFAULT_CONFIRM_TIMEOUT,
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
    :param max_outstanding_publishes: how many publishes may be waiting for the
        broker at once, a thousand by default. See
        :class:`acemq_amqp.Connection`
    :param confirm_timeout: how long a publish waits for room before raising
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
                max_outstanding_publishes=max_outstanding_publishes,
                confirm_timeout=confirm_timeout,
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
