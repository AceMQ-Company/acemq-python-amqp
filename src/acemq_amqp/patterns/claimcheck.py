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

"""Keeping large payloads off the broker.

A forty-megabyte message is possible and is a mistake: it fills the broker's
memory, it is copied to every bound queue, it makes a dead-letter queue
impossible to inspect, and it turns a broker into a filesystem with worse tools.
What travels instead is a *claim check* — the payload goes to a
:class:`ClaimCheckStore` and the message carries the key.

Below the threshold the payload travels inline, exactly as it would without this
codec. That matters more than it sounds: offloading a two-hundred-byte message
turns one broker round trip into a store round trip *and* a broker round trip,
so an unconditional claim check makes the common case slower to fix the rare
one.

The framing therefore says which of the two it is, and a consumer handles both
without being told. That is what allows the threshold to be changed, or this
codec to be introduced, without a flag day: messages written before the change
are still readable after it.

What is on the wire — byte for byte what the Java library writes, because a
document a Java service put aside has to be readable by a Python one::

    0xAC  0x01  0x00  payload      inline, and identical to what the delegate wrote
    0xAC  0x01  0x01  key          a claim check

The key is the store's own, UTF-8, with no scheme and no punctuation around it:
whatever :meth:`ClaimCheckStore.put` returned. It is not a URI, and it is not
the ``x-acemq-claim`` header either — that header is reserved for an application
that wants to say where a payload went in a form an operator can read, and a
consumer decides what a message is from the three bytes at the front of the
body, never from a header. A header can be dropped by a shovel or a plugin; the
body cannot.

The content type is the delegate's, unchanged — unlike encryption, where the
bytes really are something else. A claim-checked message is still a document; it
is a document that is somewhere else, and a consumer that lacks the store gets a
clear failure rather than a parser error.
"""

from __future__ import annotations

import os
import re
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from ..ack import FatalError
from ..codec import Codec
from ..errors import AceMQError

#: Marks this codec's framing.
_MAGIC = 0xAC

_VERSION = 0x01
_INLINE = 0x00
_CHECKED = 0x01
_HEADER = 3

#: Below this many bytes, payloads travel inline.
#:
#: 64 KiB: comfortably above an ordinary event and comfortably below the size at
#: which a broker starts to care. RabbitMQ will accept far larger, which is the
#: problem — nothing refuses a 40 MB message, it simply makes everything worse
#: afterwards. The same number as Java's ``ClaimCheckCodec.DEFAULT_THRESHOLD``,
#: because a threshold that differs by language means the two disagree about
#: which messages are claim checks.
DEFAULT_THRESHOLD = 64 * 1024


@runtime_checkable
class ClaimCheckStore(Protocol):
    """Where a payload too large for a broker actually goes.

    Three methods, so a store backed by S3, Azure Blob Storage, a filesystem or
    a database table is a small class. Nothing here knows about messaging: the
    store holds bytes under a key and hands them back, and the codec is what
    turns that into a claim check on the wire.

    Synchronous, unlike the idempotency and outbox seams, because
    :class:`~acemq_amqp.codec.Codec` is synchronous — a codec is called from
    inside the publish and consume paths of both the async and the blocking API,
    and a store that had to be awaited could not serve the second. A store
    talking to a network should hold a small pool and block on it.

    **Retention is the part that goes wrong.** The store and the queue have
    different lifetimes and nothing enforces a relationship between them. A
    message replayed a month later carries a key, and if the store expired that
    key the replay produces a message nobody can read — worse than a lost
    message, because it looks like a message and fails deep inside a consumer
    rather than visibly. So the store's retention must exceed every retention
    that could bring a message back: queue TTLs, dead-letter queues, and however
    long somebody might sit on a message before replaying it by hand. When in
    doubt, longer.

    Implementations must be safe to use from several threads, because one is
    shared by every publisher and consumer on a connection.
    """

    def put(self, content: bytes) -> str:
        """Stores a payload.

        :param content: the bytes
        :returns: the key the message will carry, which must be unique for the
            life of the store
        """

    def get(self, key: str) -> bytes | None:
        """Redeems a claim check.

        :param key: what the message carried
        :returns: the payload, or ``None`` when the store no longer holds it —
            which is retention having expired underneath a message that outlived
            it
        """

    def delete(self, key: str) -> None:
        """Removes a payload.

        Not called by the codec. Deleting on read would break the second
        consumer of the same message, and deleting on acknowledgement would
        break a replay — so when a payload may be removed is a retention
        decision, and retention decisions belong to whoever owns the data.

        :param key: what to remove
        """


class InMemoryClaimCheckStore:
    """A claim-check store in a dictionary.

    **Not for production, and the reason is the point of the pattern.** The
    payloads are held in the publisher's own memory — which is where they were
    going to be anyway, so this takes them off the broker and does nothing else.
    A claim check that does not outlive the process that wrote it is a message
    nobody else can read, and every consumer in another process gets "the claim
    check is not in the store".

    It is genuinely useful in a test, where the publisher and the consumer are
    the same process and the thing being proved is the framing rather than the
    storage.
    """

    def __init__(self) -> None:
        self._contents: dict[str, bytes] = {}
        # A plain lock, as in the idempotency store: it is held for a dictionary
        # operation and never across an await, so a store shared with the
        # blocking API's worker threads is safe too.
        self._lock = threading.Lock()

    def put(self, content: bytes) -> str:
        key = str(uuid.uuid4())
        with self._lock:
            # bytes() rather than the object handed over, because the caller owns
            # what it gave us and a codec is entitled to reuse a buffer. A store
            # that keeps somebody else's bytearray is a store whose contents
            # change after they were stored.
            self._contents[key] = bytes(content)
        return key

    def get(self, key: str) -> bytes | None:
        with self._lock:
            return self._contents.get(key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._contents.pop(key, None)

    def clear(self) -> None:
        """Empties the store, which is what a test between cases wants."""
        with self._lock:
            self._contents.clear()

    def __len__(self) -> int:
        """How many payloads are held."""
        with self._lock:
            return len(self._contents)

    def __repr__(self) -> str:
        return f"InMemoryClaimCheckStore({len(self)} held)"


class FilesystemClaimCheckStore:
    """A claim-check store on a filesystem.

    Useful where the filesystem is shared and durable — an NFS mount, a
    persistent volume — and the honest middle ground between a dictionary and
    object storage. On a container's local disk it is the in-memory store with
    extra steps: the consumer is on another host and finds nothing.

    Object storage is the usual right answer, and a store in front of S3 or
    Azure Blob Storage is three short methods. This one exists because "write it
    to the mount everything already has" is a real deployment and not a bad one.

    **Writes are atomic.** The payload is written to a temporary file and moved
    into place. Without that, a consumer fast enough to read the key before the
    writer finished gets a truncated payload and a parse error somewhere
    unhelpful — and messaging is exactly the arrangement that makes a consumer
    that fast normal rather than unlikely.

    :param directory: where payloads are written; created if it is not there
    """

    #: A key reaches the filesystem as a path segment, so it is checked rather
    #: than trusted. Every key this store issues is a UUID; one arriving from a
    #: message is whatever a publisher put there, and ``../../etc/passwd`` is a
    #: key too. The same expression as Java's, so the two stores accept and
    #: refuse the same keys over one shared mount.
    _SAFE_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self._directory = Path(directory)
        try:
            self._directory.mkdir(parents=True, exist_ok=True)
        except OSError as failure:
            raise AceMQError(
                f"acemq: could not create the claim-check directory "
                f"{self._directory}: {failure}"
            ) from failure

    @property
    def directory(self) -> Path:
        """Where the payloads are."""
        return self._directory

    def put(self, content: bytes) -> str:
        key = str(uuid.uuid4())
        target = self._directory / key
        try:
            handle, staging = tempfile.mkstemp(
                prefix=key, suffix=".partial", dir=self._directory
            )
            try:
                with os.fdopen(handle, "wb") as writing:
                    writing.write(content)
                # Moved into place, so a reader sees the whole payload or no
                # payload. os.replace is atomic within a filesystem, which is
                # the case here because the staging file was made in the same
                # directory on purpose.
                os.replace(staging, target)
            except BaseException:
                # The staging file is ours and nothing refers to it, so a failed
                # write leaves nothing behind rather than a .partial somebody
                # has to explain later.
                Path(staging).unlink(missing_ok=True)
                raise
        except OSError as failure:
            raise AceMQError(
                f"acemq: could not write the claim-check payload {key}: {failure}"
            ) from failure
        return key

    def get(self, key: str) -> bytes | None:
        try:
            return self._path_for(key).read_bytes()
        except FileNotFoundError:
            # Absent rather than failed: a key the store no longer holds is a
            # retention answer, and the codec turns it into a message that
            # explains itself.
            return None
        except OSError as failure:
            raise AceMQError(
                f"acemq: could not read the claim-check payload {key}: {failure}"
            ) from failure

    def delete(self, key: str) -> None:
        try:
            self._path_for(key).unlink(missing_ok=True)
        except OSError as failure:
            raise AceMQError(
                f"acemq: could not delete the claim-check payload {key}: {failure}"
            ) from failure

    def _path_for(self, key: str) -> Path:
        if not self._SAFE_KEY.fullmatch(key):
            raise AceMQError(
                f"acemq: {key!r} is not a key this store issued. A key becomes a path "
                "segment, so one arriving from a message is checked rather than trusted."
            )
        return self._directory / key

    def __repr__(self) -> str:
        return f"FilesystemClaimCheckStore({self._directory})"


class ClaimCheckCodec:
    """Puts a large payload aside and sends the key instead::

        from acemq_amqp.codec import JsonCodec
        from acemq_amqp.patterns import ClaimCheckCodec, FilesystemClaimCheckStore

        store = FilesystemClaimCheckStore("/mnt/payloads")
        codec = ClaimCheckCodec(JsonCodec(), store)

        mq = await Connection.open(url, codec=codec)

    A payload smaller than ``threshold`` goes on the wire inline and a larger
    one goes to the store, and the three bytes at the front of the body say
    which. A body this codec did not write is handed to the delegate untouched,
    which is what makes it safe to introduce on a queue that already has
    messages in it.

    :param delegate: the codec that turns payloads into bytes. What gets stored
        or inlined is its output
    :param store: where large payloads go
    :param threshold: payloads of at least this many bytes are offloaded. Zero
        offloads everything, which is occasionally what a store-backed audit
        trail wants
    """

    def __init__(
        self,
        delegate: Codec,
        store: ClaimCheckStore,
        *,
        threshold: int = DEFAULT_THRESHOLD,
    ) -> None:
        if threshold < 0:
            raise ValueError(
                f"acemq: a claim-check threshold cannot be negative, got {threshold}"
            )
        self._delegate = delegate
        self._store = store
        self._threshold = threshold

    @property
    def delegate(self) -> Codec:
        """The wrapped codec, whose output is what gets stored or inlined."""
        return self._delegate

    @property
    def store(self) -> ClaimCheckStore:
        """Where large payloads go."""
        return self._store

    @property
    def threshold(self) -> int:
        """Payloads of at least this many bytes are offloaded."""
        return self._threshold

    @property
    def content_type(self) -> str:
        """The delegate's. A claim-checked document is still a document."""
        return self._delegate.content_type

    def encode(self, payload: Any) -> bytes:
        encoded = self._delegate.encode(payload)
        if len(encoded) < self._threshold:
            return _frame(_INLINE, encoded)
        return _frame(_CHECKED, self._store.put(encoded).encode("utf-8"))

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        if not _is_framed(body):
            # Written before this codec was introduced, or by a publisher that
            # does not use it. Reading it as the delegate would is the only
            # useful answer, and it is what makes adding a claim check to a live
            # queue safe.
            return self._delegate.decode(body, content_type)

        rest = body[_HEADER:]
        if body[2] == _INLINE:
            return self._delegate.decode(rest, content_type)

        key = rest.decode("utf-8")
        content = self._store.get(key)
        if content is None:
            # Fatal, not retryable: the payload is not coming back, and a
            # message that keeps being redelivered for it only holds a queue
            # open until it ages out.
            raise FatalError(
                f"acemq: the claim check {key!r} is not in the store, so this message "
                "cannot be read. The payload was removed while a message referring to it "
                "was still deliverable — the store's retention has to outlast every queue, "
                "every dead-letter queue, and any replay somebody might do by hand."
            )
        return self._delegate.decode(content, content_type)

    def can_decode(self, content_type: str | None) -> bool:
        """Whatever the delegate accepts. A claim check does not change what the
        message is."""
        return self._delegate.can_decode(content_type)

    def __repr__(self) -> str:
        return f"ClaimCheckCodec({self._delegate!r}, above={self._threshold} bytes)"


def claim_key_of(body: bytes) -> str | None:
    """Reads the key a message refers to, without fetching it.

    For the operator looking at a dead-letter queue: which object does this need,
    and is it still in the store? That one line is the difference between a
    five-minute check and restoring a backup.

    :param body: a message body
    :returns: the key, or ``None`` when the payload travelled inline or this
        codec did not write the message
    """
    if not _is_framed(body) or body[2] != _CHECKED:
        return None
    return body[_HEADER:].decode("utf-8")


def is_claim_check(body: bytes) -> bool:
    """Whether a body is a claim check rather than a payload.

    The question a consumer answers from the body's first three bytes and never
    from a header, so that a message which lost its headers on the way through a
    shovel is still read correctly.

    :param body: a message body
    :returns: whether the payload is in a store rather than in the message
    """
    return claim_key_of(body) is not None


def _is_framed(body: bytes | None) -> bool:
    return (
        body is not None
        and len(body) >= _HEADER
        and body[0] == _MAGIC
        and body[1] == _VERSION
        and body[2] in (_INLINE, _CHECKED)
    )


def _frame(kind: int, rest: bytes) -> bytes:
    return bytes((_MAGIC, _VERSION, kind)) + rest
