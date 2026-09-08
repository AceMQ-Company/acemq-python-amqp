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

"""The encrypting codec, and the promises it is only worth having if it keeps.

Round-tripping its own output would prove almost nothing here. The two things
that matter are that the bytes are the ones Java writes — pinned below by a body
Java actually produced — and that every way of failing to decrypt looks the
same and says nothing. Both are asserted directly.

``JAVA_BODY`` was produced by ``org.acemq.amqp.crypto.EncryptedCodec`` compiled
from ``acemq-java-amqp/acemq-amqp-crypto``, wrapping a pass-through codec, with
key ``2026-01`` set to the bytes ``00 01 02 ... 1f`` and the plaintext
``hello``. Nothing in this repository produced it, which is the point of having
it.
"""

from __future__ import annotations

import re

import pytest

from acemq_amqp.ack import FatalError
from acemq_amqp.codec import BytesCodec, JsonCodec
from acemq_amqp.codecs.encrypted import (
    ENCRYPTED_CONTENT_TYPE,
    MAGIC,
    NONCE_BYTES,
    TAG_BYTES,
    VERSION,
    EncryptedCodec,
    EncryptionKey,
    Keyring,
    frame,
    generate_key,
    key_from_base64,
    key_id_of,
    key_to_base64,
)
from acemq_amqp.errors import AceMQError

#: The 32 bytes ``00`` through ``1f``. A fixed key, so the Java body below can
#: be decrypted here; never a key to use for anything.
TEST_KEY = bytes(range(32))

#: A body written by the Java library. See the module docstring.
JAVA_BODY = bytes.fromhex(
    "ae0107323032362d303175345ecc4a26ac55fcb992696364875909f4a91111aa73a811e509a7fedf911916"
)

#: A body written by the *Go* library for the same key and plaintext. Go frames
#: it differently — no magic byte, and a two-byte length — so this is here to be
#: refused rather than read. The divergence is recorded in the module docstring
#: of ``acemq_amqp.codecs.encrypted`` and in ``docs/serialization.md``.
GO_BODY = bytes.fromhex(
    "010007323032362d30311abb60483ecd9b10f2817d6bbc6716a5813db519e5fe797c96d6c725baf9ddbbe6"
)

#: A fixed nonce, so one message can be written down in full. Never a nonce to
#: use for anything: reusing one under GCM forfeits the encryption outright, and
#: this file uses it exactly once.
FIXED_NONCE = bytes.fromhex("000102030405060708090a0b")

#: The whole of one message, byte for byte, for key ``2026-01`` set to
#: ``00 01 ... 1f``, the nonce above, and the plaintext ``hello``.
#:
#: This is the test vector the other libraries have to converge against. It was
#: handed to the compiled Java codec, which read ``hello`` out of it, so the
#: layout below is Java's and not merely this library's opinion of Java's::
#:
#:     ae            magic
#:     01            version
#:     07            length of the key identifier, in UTF-8 bytes
#:     323032362d3031      "2026-01"
#:     000102...0b   the 12-byte nonce
#:     2f67ba7...    5 bytes of ciphertext, then the 16-byte GCM tag
#:
#: The associated data is the first ten bytes — everything before the nonce.
VECTOR = bytes.fromhex(
    "ae0107323032362d3031"
    "000102030405060708090a0b"
    "2f67ba77aa632797b83b1f88ef1394bb9ff6e85641"
)


@pytest.fixture()
def keyring() -> Keyring:
    return Keyring.of("2026-01", TEST_KEY)


@pytest.fixture()
def codec(keyring: Keyring) -> EncryptedCodec:
    return EncryptedCodec(BytesCodec(), keyring)


# --------------------------------------------------------------------------
# The framing


def test_the_framing_is_magic_version_length_key_id_nonce_then_ciphertext(
    codec: EncryptedCodec,
) -> None:
    body = codec.encode(b"hello")

    assert body[0] == MAGIC == 0xAE
    assert body[1] == VERSION == 0x01
    assert body[2] == len(b"2026-01")
    assert body[3:10] == b"2026-01"
    # Nonce, then ciphertext and a 16-byte tag. Five bytes of plaintext.
    assert len(body) == 3 + 7 + NONCE_BYTES + 5 + TAG_BYTES


def test_java_reads_what_this_writes_because_the_framing_is_the_same() -> None:
    """The half of the interop this test file can assert on its own.

    The other half — Java decrypting a body written here — was run against the
    compiled Java codec and is what fixed ``JAVA_BODY``'s framing as the one to
    match. What can be pinned in Python is that the two framings are identical
    byte for byte in everything that is not the nonce or the ciphertext.
    """
    written = EncryptedCodec(BytesCodec(), Keyring.of("2026-01", TEST_KEY)).encode(b"hello")

    assert written[:10] == JAVA_BODY[:10]
    assert len(written) == len(JAVA_BODY)


def test_the_whole_message_is_exactly_these_bytes(
    codec: EncryptedCodec, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The test vector, written out in full and asserted on as one string.

    Every other test here checks one field. This checks all of them at once and,
    more usefully, checks the *ciphertext and tag* — which no assertion about
    field offsets can reach, and which is where an AAD that was wrong or absent
    would show up. Given the key, the nonce and the plaintext there is exactly
    one correct answer, and it is this one.

    It is here to be converged against. Java, Go and .NET currently write three
    different framings under one content type; whichever of them is being made to
    agree, this is the string it has to produce.
    """
    monkeypatch.setattr(
        "acemq_amqp.codecs.encrypted.os.urandom",
        lambda size: FIXED_NONCE[:size],
    )

    assert codec.encode(b"hello") == VECTOR


def test_the_vector_decrypts_back_to_its_plaintext(codec: EncryptedCodec) -> None:
    """The other direction, with nothing patched.

    Worth having separately: the assertion above would still pass if encoding and
    the vector were both wrong in the same way, which is exactly what happens when
    somebody regenerates a fixture from the code it is meant to be checking.
    """
    assert codec.decode(VECTOR, ENCRYPTED_CONTENT_TYPE) == b"hello"


def test_the_associated_data_is_the_header_and_nothing_else(codec: EncryptedCodec) -> None:
    """Pins *which* bytes are authenticated, which the vector alone cannot say.

    Encrypting the same plaintext under the same key and nonce with the ten
    header bytes as associated data reproduces the vector's ciphertext and tag
    exactly. With a different span — the key identifier alone, say, or nothing —
    the tag comes out different, so this fails.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    header = VECTOR[:10]
    assert header == frame("2026-01")

    sealed = AESGCM(TEST_KEY).encrypt(FIXED_NONCE, b"hello", header)

    assert header + FIXED_NONCE + sealed == VECTOR


def test_a_body_java_wrote_is_decrypted_here(codec: EncryptedCodec) -> None:
    assert codec.decode(JAVA_BODY, ENCRYPTED_CONTENT_TYPE) == b"hello"


def test_the_key_identifier_can_be_read_without_holding_any_key() -> None:
    """What an operator does with a dead-letter queue they cannot read.

    Deliberately a module function rather than a method: answering "which key
    does this need?" must not require constructing a codec, because the whole
    situation is that nobody has the keyring to construct one with.
    """
    assert key_id_of(JAVA_BODY) == "2026-01"


def test_a_body_this_codec_did_not_write_has_no_key_identifier() -> None:
    assert key_id_of(b'{"order": 1}') is None
    assert key_id_of(b"") is None
    assert key_id_of(bytes([MAGIC, VERSION, 9, 1, 2])) is None


def test_the_go_framing_is_refused_rather_than_misread(codec: EncryptedCodec) -> None:
    """Go writes a different header, and this says so instead of guessing.

    Go omits the magic byte and writes the length as two big-endian bytes, so
    its first three bytes are ``01 00 07`` where Java's are ``ae 01 07``. Both
    headers happen to be the same length for a seven-byte identifier, which is
    exactly the sort of coincidence that would let a lenient reader decrypt
    against the wrong associated data and fail somewhere less obvious.
    """
    assert key_id_of(GO_BODY) is None
    with pytest.raises(FatalError, match="not written by EncryptedCodec"):
        codec.decode(GO_BODY, ENCRYPTED_CONTENT_TYPE)


def test_the_content_type_is_not_a_json_suffix() -> None:
    """``application/vnd.acemq.encrypted``, and nothing claiming to be JSON.

    A ``+json`` suffix would make every JSON-aware consumer volunteer to parse
    ciphertext.
    """
    assert ENCRYPTED_CONTENT_TYPE == "application/vnd.acemq.encrypted"
    assert "+json" not in ENCRYPTED_CONTENT_TYPE


# --------------------------------------------------------------------------
# The cryptography


def test_a_round_trip_through_a_real_codec_returns_the_payload(keyring: Keyring) -> None:
    codec = EncryptedCodec(JsonCodec(), keyring)
    order = {"id": "A-1", "total": 4200}

    assert codec.decode(codec.encode(order), ENCRYPTED_CONTENT_TYPE) == order


def test_the_plaintext_is_nowhere_in_the_body(keyring: Keyring) -> None:
    codec = EncryptedCodec(JsonCodec(), keyring)

    body = codec.encode({"card": "4111111111111111"})

    assert b"4111111111111111" not in body
    assert b"card" not in body


def test_every_message_gets_a_fresh_nonce(codec: EncryptedCodec) -> None:
    """The one mistake that turns AES-GCM into no encryption at all.

    Two messages under one key and one nonce leak their difference outright, so
    this asserts on the nonces themselves rather than on the bodies differing —
    bodies would differ for a hundred reasons and would keep passing after
    somebody replaced the random source with a counter.
    """
    nonces = {codec.encode(b"same")[10 : 10 + NONCE_BYTES] for _ in range(64)}

    assert len(nonces) == 64


def test_the_key_identifier_is_bound_to_the_ciphertext(keyring: Keyring) -> None:
    """Rewriting the header must break the message, not redirect it.

    The header is authenticated as associated data, so an identifier swapped in
    flight — to name a key the attacker knows the reader holds — fails to open
    rather than opening as something else. Both keys here hold the *same* bytes,
    so the only thing that can make this fail is the associated data. That is
    the assertion: with AAD wrong or absent, this test passes when it should
    not.
    """
    under_another_name = Keyring.of("2026-02", TEST_KEY)
    written = EncryptedCodec(BytesCodec(), keyring).encode(b"hello")
    relabelled = frame("2026-02") + written[10:]

    with pytest.raises(FatalError):
        EncryptedCodec(BytesCodec(), under_another_name).decode(
            relabelled, ENCRYPTED_CONTENT_TYPE
        )


@pytest.mark.parametrize("position", [-1, -17, 12, 20])
def test_a_single_altered_bit_anywhere_refuses_the_message(
    codec: EncryptedCodec, position: int
) -> None:
    body = bytearray(codec.encode(b"a message worth tampering with"))
    body[position] ^= 0b0000_0001

    with pytest.raises(FatalError):
        codec.decode(bytes(body), ENCRYPTED_CONTENT_TYPE)


def test_the_wrong_key_and_a_tampered_body_fail_identically(keyring: Keyring) -> None:
    """The property that is lost by adding one helpful check.

    GCM gives it for free: authentication happens before anything is returned,
    so "this is not the key" and "these are not the bytes" reach the same line
    with the same information. An error that told them apart would tell an
    attacker whether a guessed key was right, one guess at a time.
    """
    written = EncryptedCodec(BytesCodec(), keyring).encode(b"hello")

    tampered = bytearray(written)
    tampered[-1] ^= 1
    with pytest.raises(FatalError) as from_tampering:
        EncryptedCodec(BytesCodec(), keyring).decode(bytes(tampered), ENCRYPTED_CONTENT_TYPE)

    other_key = Keyring.of("2026-01", bytes(32))
    with pytest.raises(FatalError) as from_wrong_key:
        EncryptedCodec(BytesCodec(), other_key).decode(written, ENCRYPTED_CONTENT_TYPE)

    assert str(from_tampering.value) == str(from_wrong_key.value)
    assert type(from_tampering.value) is type(from_wrong_key.value)


def test_a_truncated_message_is_refused_before_any_key_is_looked_up(
    codec: EncryptedCodec,
) -> None:
    written = codec.encode(b"hello")

    with pytest.raises(FatalError, match="too short"):
        codec.decode(written[:20], ENCRYPTED_CONTENT_TYPE)


def test_a_failure_to_decrypt_is_fatal_rather_than_retryable(codec: EncryptedCodec) -> None:
    """A message that will not decrypt will not decrypt on the next attempt.

    Retrying it holds the queue open until it ages out and produces the same
    failure a dozen more times in the log. :class:`FatalError` sends it to the
    dead-letter queue at once, where somebody can read its key identifier.
    """
    with pytest.raises(FatalError):
        codec.decode(b"not encrypted at all", ENCRYPTED_CONTENT_TYPE)


# --------------------------------------------------------------------------
# Nothing secret ever comes out


def test_no_failure_message_contains_the_plaintext_or_the_key(keyring: Keyring) -> None:
    """The requirement the whole module exists to keep.

    Every failure path is walked and its message is searched for the plaintext,
    for the key in every rendering somebody might have used to build a message
    out of it, and for anything that looks like key material. The test is
    written as a sweep rather than as one assertion per path so that a new
    failure path added later without thought is caught by it.
    """
    secret = b"4111111111111111"
    codec = EncryptedCodec(JsonCodec(), keyring)
    written = codec.encode({"card": secret.decode()})

    failures: list[str] = []

    tampered = bytearray(written)
    tampered[-1] ^= 1
    for attempt in (
        lambda: codec.decode(bytes(tampered), ENCRYPTED_CONTENT_TYPE),
        lambda: codec.decode(b"plaintext, wrongly routed here", ENCRYPTED_CONTENT_TYPE),
        lambda: codec.decode(written[:14], ENCRYPTED_CONTENT_TYPE),
        lambda: EncryptedCodec(JsonCodec(), Keyring.of("other", TEST_KEY)).decode(
            written, ENCRYPTED_CONTENT_TYPE
        ),
        lambda: codec.encode(object()),
    ):
        with pytest.raises(Exception) as raised:
            attempt()
        failures.append(str(raised.value))

    forbidden = (
        secret.decode(),
        TEST_KEY.hex(),
        key_to_base64(TEST_KEY),
        repr(TEST_KEY),
        str(list(TEST_KEY)),
    )
    for message in failures:
        for never in forbidden:
            assert never not in message
        # Nor anything that merely looks like key material: a long run of hex
        # or Base64 in an error message is a key however it got there.
        assert not re.search(r"[A-Za-z0-9+/=]{24,}", message), message


def test_a_key_never_renders_itself() -> None:
    """A key in a log, a traceback or a debugger prints its name and nothing else."""
    key = EncryptionKey("2026-01", TEST_KEY)

    assert repr(key) == "EncryptionKey(id='2026-01')"
    assert str(key) == "EncryptionKey(id='2026-01')"
    assert TEST_KEY.hex() not in repr(key)


def test_a_keyring_renders_its_names_and_not_its_keys(keyring: Keyring) -> None:
    keyring.add(EncryptionKey("2026-02", generate_key()))

    rendered = repr(keyring)

    assert "2026-01" in rendered
    assert "2026-02" in rendered
    assert TEST_KEY.hex() not in rendered


def test_a_codec_renders_the_key_name_and_not_the_key(codec: EncryptedCodec) -> None:
    rendered = repr(codec)

    assert "2026-01" in rendered
    assert TEST_KEY.hex() not in rendered


def test_a_malformed_base64_key_is_refused_without_being_quoted() -> None:
    with pytest.raises(AceMQError) as raised:
        key_from_base64("this is not base64 !!!")

    assert "not base64" not in str(raised.value)
    assert "this is not base64" not in str(raised.value)


# --------------------------------------------------------------------------
# Rotation


def test_a_message_written_with_a_retired_key_is_still_readable() -> None:
    """The reason the identifier is on the wire at all.

    A key is rotated by adding the new one everywhere first and only then making
    it current, and during the overlap a consumer reads both. If it assumed the
    current key, every message already queued would be lost the moment the new
    one was introduced.
    """
    old, new = generate_key(), generate_key()
    ring = Keyring(EncryptionKey("2026-01", old))
    written_under_the_old_key = EncryptedCodec(BytesCodec(), ring).encode(b"still queued")

    ring.add(EncryptionKey("2026-02", new))
    ring.use("2026-02")
    codec = EncryptedCodec(BytesCodec(), ring)

    assert key_id_of(codec.encode(b"new")) == "2026-02"
    assert codec.decode(written_under_the_old_key, ENCRYPTED_CONTENT_TYPE) == b"still queued"


def test_a_key_that_is_not_on_the_ring_is_a_fatal_failure_naming_it(
    codec: EncryptedCodec,
) -> None:
    written = EncryptedCodec(BytesCodec(), Keyring.of("2025-12", generate_key())).encode(b"old")

    with pytest.raises(FatalError) as raised:
        codec.decode(written, ENCRYPTED_CONTENT_TYPE)

    assert "2025-12" in str(raised.value)
    assert "2026-01" in str(raised.value)


def test_using_a_key_the_ring_does_not_hold_is_refused(keyring: Keyring) -> None:
    with pytest.raises(AceMQError, match="no key '2027-01'"):
        keyring.use("2027-01")


def test_a_keyring_needs_at_least_one_key() -> None:
    with pytest.raises(AceMQError, match="at least one key"):
        Keyring()


# --------------------------------------------------------------------------
# Keys


def test_a_generated_key_is_aes_256_from_the_operating_system() -> None:
    assert len(generate_key()) == 32
    assert generate_key() != generate_key()


def test_a_key_survives_base64() -> None:
    key = generate_key()

    assert key_from_base64(key_to_base64(key)) == key
    assert key_from_base64(f"  {key_to_base64(key)}\n") == key


@pytest.mark.parametrize("size", [16, 24, 32])
def test_the_aes_key_sizes_are_accepted(size: int) -> None:
    assert EncryptionKey("k", bytes(size)).key == bytes(size)


@pytest.mark.parametrize("size", [0, 8, 15, 31, 64])
def test_anything_that_is_not_an_aes_key_size_is_refused_rather_than_padded(
    size: int,
) -> None:
    """Padding or hashing a short key here would make a weak one look strong.

    A passphrase is the case worth naming, and the message names it: the answer
    is a key derivation function, not this library quietly stretching sixteen
    characters of English into something with the shape of a key.
    """
    with pytest.raises(AceMQError, match="key derivation function"):
        EncryptionKey("k", bytes(size))


def test_a_key_identifier_is_a_name_rather_than_a_description() -> None:
    with pytest.raises(AceMQError, match="at most 255 bytes"):
        EncryptionKey("k" * 256, TEST_KEY)


def test_a_key_identifier_cannot_be_empty() -> None:
    with pytest.raises(AceMQError, match="cannot be empty"):
        EncryptionKey("", TEST_KEY)


def test_a_key_identifier_with_a_null_byte_is_refused() -> None:
    with pytest.raises(AceMQError, match="null byte"):
        EncryptionKey("2026\x0001", TEST_KEY)


def test_a_non_ascii_key_identifier_is_framed_by_its_utf8_length() -> None:
    """The length byte counts bytes, not characters.

    Counting characters would frame a message whose identifier held anything
    outside ASCII one byte short per character, and the reader would take the
    first byte of the nonce as the last byte of the name.
    """
    codec = EncryptedCodec(BytesCodec(), Keyring.of("clé-2026", generate_key()))

    body = codec.encode(b"payload")

    assert body[2] == len("clé-2026".encode()) == 9
    assert key_id_of(body) == "clé-2026"
    assert codec.decode(body, ENCRYPTED_CONTENT_TYPE) == b"payload"


# --------------------------------------------------------------------------
# Behaving like a codec


def test_it_claims_only_its_own_content_type(codec: EncryptedCodec) -> None:
    assert codec.can_decode(ENCRYPTED_CONTENT_TYPE)
    assert codec.can_decode("application/vnd.acemq.encrypted; charset=utf-8")
    assert codec.can_decode("APPLICATION/VND.ACEMQ.ENCRYPTED")

    assert not codec.can_decode("application/json")
    assert not codec.can_decode("application/octet-stream")


def test_it_never_claims_a_message_whose_sender_said_nothing(codec: EncryptedCodec) -> None:
    """Volunteering for an untyped message means trying to decrypt plaintext.

    The failure would then be reported as a decryption error, which sends
    whoever is debugging it looking for a key problem that does not exist.
    """
    assert not codec.can_decode(None)
    assert not codec.can_decode("")


def test_the_delegate_decides_the_format_and_stays_invisible_on_the_wire(
    keyring: Keyring,
) -> None:
    codec = EncryptedCodec(JsonCodec(), keyring)

    assert codec.content_type == ENCRYPTED_CONTENT_TYPE
    assert isinstance(codec.delegate, JsonCodec)
    assert b"json" not in codec.encode({"a": 1}).lower()
