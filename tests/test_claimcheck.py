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

"""The claim check, and the framing that has to match Java byte for byte."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from acemq_amqp import FatalError
from acemq_amqp.codec import BytesCodec, JsonCodec
from acemq_amqp.errors import AceMQError
from acemq_amqp.patterns import (
    DEFAULT_THRESHOLD,
    ClaimCheckCodec,
    ClaimCheckStore,
    FilesystemClaimCheckStore,
    InMemoryClaimCheckStore,
    claim_key_of,
    is_claim_check,
)

# What the Java library writes, copied out of
# acemq-amqp-patterns/src/main/java/org/acemq/amqp/patterns/ClaimCheckCodec.java:
#
#     private static final byte MAGIC = (byte) 0xAC;
#     private static final byte VERSION = 0x01;
#     private static final byte INLINE = 0x00;
#     private static final byte CHECKED = 0x01;
#     private static final int HEADER = 3;
#     public static final int DEFAULT_THRESHOLD = 64 * 1024;
#
# Written out here rather than imported from the module under test, so that
# changing the module cannot quietly change what this file claims Java does.
JAVA_MAGIC = 0xAC
JAVA_VERSION = 0x01
JAVA_INLINE = 0x00
JAVA_CHECKED = 0x01
JAVA_HEADER = 3
JAVA_DEFAULT_THRESHOLD = 64 * 1024


def test_the_threshold_is_the_number_java_uses() -> None:
    assert DEFAULT_THRESHOLD == JAVA_DEFAULT_THRESHOLD == 65536


def test_an_inline_body_is_framed_the_way_java_frames_one() -> None:
    codec = ClaimCheckCodec(JsonCodec(), InMemoryClaimCheckStore())

    body = codec.encode({"id": "o-1"})

    assert body[0] == JAVA_MAGIC
    assert body[1] == JAVA_VERSION
    assert body[2] == JAVA_INLINE
    # And after the three-byte header, byte for byte what the delegate wrote.
    assert body[JAVA_HEADER:] == JsonCodec().encode({"id": "o-1"})
    assert body[:JAVA_HEADER] == b"\xac\x01\x00"


def test_a_claim_check_body_is_the_header_and_the_bare_key() -> None:
    store = InMemoryClaimCheckStore()
    codec = ClaimCheckCodec(JsonCodec(), store, threshold=16)

    order = {"id": "o-1", "note": "long enough to be offloaded"}
    body = codec.encode(order)

    assert body[:JAVA_HEADER] == b"\xac\x01\x01"
    key = body[JAVA_HEADER:].decode("utf-8")
    # No scheme, no punctuation, no URI: whatever put() returned, in UTF-8. This
    # is the half of the contract that decides whether Java and Python can
    # exchange a large message at all.
    assert "://" not in key
    assert store.get(key) == JsonCodec().encode(order)


def test_a_body_java_wrote_is_read_here() -> None:
    """A hand-built body in exactly Java's framing, redeemed by this codec."""
    store = InMemoryClaimCheckStore()
    payload = json.dumps({"id": "o-9"}).encode("utf-8")
    key = store.put(payload)

    java_wrote = bytes((JAVA_MAGIC, JAVA_VERSION, JAVA_CHECKED)) + key.encode("utf-8")

    codec = ClaimCheckCodec(JsonCodec(), store)
    assert codec.decode(java_wrote, "application/json") == {"id": "o-9"}
    assert claim_key_of(java_wrote) == key
    assert is_claim_check(java_wrote)


def test_an_inline_body_java_wrote_is_read_here() -> None:
    java_wrote = bytes((JAVA_MAGIC, JAVA_VERSION, JAVA_INLINE)) + b'{"id":"o-9"}'

    codec = ClaimCheckCodec(JsonCodec(), InMemoryClaimCheckStore())
    assert codec.decode(java_wrote, "application/json") == {"id": "o-9"}
    assert claim_key_of(java_wrote) is None
    assert not is_claim_check(java_wrote)


@pytest.mark.parametrize("size", [0, 1, 63, 64])
def test_the_boundary_is_at_or_above_rather_than_above(size: int) -> None:
    """Java offloads at ``encoded.length >= threshold``, so 64 bytes with a
    threshold of 64 is a claim check and 63 is not."""
    store = InMemoryClaimCheckStore()
    codec = ClaimCheckCodec(_SizedCodec(), store, threshold=64)

    body = codec.encode(size)

    assert is_claim_check(body) is (size >= 64)


def test_a_round_trip_gives_the_payload_back() -> None:
    store = InMemoryClaimCheckStore()
    codec = ClaimCheckCodec(JsonCodec(), store, threshold=8)
    order = {"id": "o-2", "lines": list(range(50))}

    assert codec.decode(codec.encode(order), codec.content_type) == order
    assert len(store) == 1


def test_a_body_this_codec_did_not_write_goes_to_the_delegate() -> None:
    codec = ClaimCheckCodec(JsonCodec(), InMemoryClaimCheckStore())

    # What a publisher that has never heard of a claim check sends. Introducing
    # the codec on a live queue has to keep working on the messages already in it.
    assert codec.decode(b'{"id":"o-3"}', "application/json") == {"id": "o-3"}


def test_a_body_that_starts_like_the_framing_but_is_not_goes_to_the_delegate() -> None:
    codec = ClaimCheckCodec(BytesCodec(), InMemoryClaimCheckStore())

    # Right magic, wrong version: not ours, so it is passed through whole rather
    # than having three bytes eaten off the front of somebody's binary payload.
    assert codec.decode(b"\xac\x02\x00stuff") == b"\xac\x02\x00stuff"
    # And a third byte that is neither kind, which is the same argument.
    assert codec.decode(b"\xac\x01\x09stuff") == b"\xac\x01\x09stuff"

    assert claim_key_of(b"\xac\x02\x01somekey") is None
    assert claim_key_of(b"\xac") is None
    assert claim_key_of(b"") is None


def test_a_missing_claim_is_fatal_and_says_why() -> None:
    store = InMemoryClaimCheckStore()
    codec = ClaimCheckCodec(JsonCodec(), store, threshold=0)
    body = codec.encode({"id": "o-4"})

    store.clear()  # retention expired underneath a message that outlived it

    with pytest.raises(FatalError) as raised:
        codec.decode(body, codec.content_type)
    assert "not in the store" in str(raised.value)
    assert "retention" in str(raised.value)


def test_the_content_type_is_the_delegates() -> None:
    codec = ClaimCheckCodec(JsonCodec(), InMemoryClaimCheckStore())

    assert codec.content_type == "application/json"
    assert codec.can_decode("application/json")
    assert not codec.can_decode("text/plain")


def test_a_negative_threshold_is_refused() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        ClaimCheckCodec(JsonCodec(), InMemoryClaimCheckStore(), threshold=-1)


def test_the_in_memory_store_holds_bytes_of_its_own() -> None:
    store = InMemoryClaimCheckStore()
    given = bytearray(b"payload")

    key = store.put(bytes(given))
    given[0] = ord("P")

    assert store.get(key) == b"payload"
    assert store.get("no-such-key") is None
    store.delete(key)
    assert len(store) == 0
    assert isinstance(store, ClaimCheckStore)


def test_the_filesystem_store_round_trips(tmp_path: Path) -> None:
    store = FilesystemClaimCheckStore(tmp_path / "payloads")

    key = store.put(b"a document")

    assert store.get(key) == b"a document"
    assert (tmp_path / "payloads" / key).exists()
    # Nothing left behind by the atomic write.
    assert list((tmp_path / "payloads").glob("*.partial")) == []

    store.delete(key)
    assert store.get(key) is None
    store.delete(key)  # deleting twice is not an error
    assert isinstance(store, ClaimCheckStore)


def test_the_filesystem_store_refuses_a_key_it_did_not_issue(tmp_path: Path) -> None:
    store = FilesystemClaimCheckStore(tmp_path)

    for hostile in ("../../etc/passwd", "/etc/passwd", "", ".hidden", "a" * 200):
        with pytest.raises(AceMQError, match="not a key this store issued"):
            store.get(hostile)


def test_the_filesystem_store_serves_a_codec_end_to_end(tmp_path: Path) -> None:
    store = FilesystemClaimCheckStore(tmp_path)
    codec = ClaimCheckCodec(JsonCodec(), store, threshold=8)
    order = {"id": "o-5", "lines": list(range(100))}

    body = codec.encode(order)

    assert is_claim_check(body)
    assert codec.decode(body, codec.content_type) == order


class _SizedCodec:
    """Encodes an integer into that many bytes, so a test can sit on the boundary."""

    @property
    def content_type(self) -> str:
        return "application/octet-stream"

    def encode(self, payload: object) -> bytes:
        assert isinstance(payload, int)
        return b"x" * payload

    def decode(self, body: bytes, content_type: str | None = None) -> object:
        return len(body)

    def can_decode(self, content_type: str | None) -> bool:
        return True
