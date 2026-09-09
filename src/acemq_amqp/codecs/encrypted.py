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

"""Encrypts whatever another codec produced, so the broker holds ciphertext::

    pip install "acemq-amqp[crypto]"

    from acemq_amqp.codecs.encrypted import EncryptedCodec, Keyring, generate_key

    keyring = Keyring.of("2026-01", generate_key())
    mq = await connect(url, codec=EncryptedCodec(JsonCodec(), keyring))

It wraps a delegate rather than serialising anything itself, so choosing a
format and choosing to encrypt stay independent: JSON in, AES-GCM out, and Avro
just as well.

What is on the wire
-------------------

::

    0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag

**The key identifier is in the message, in the clear.** That is deliberate, and
it is what makes rotation possible: a consumer reads which key a message needs
rather than assuming the current one, so a new key can be introduced while
messages written with the old one are still queued. Putting it in an AMQP header
instead would have been tidier and would have lost it — headers are dropped by
shovels, rewritten by federation, and absent from a message recovered out of a
backup, and a ciphertext whose key nobody can name is gone.

The header is authenticated but not encrypted: GCM binds all of it — magic,
version, length and identifier — as associated data, so an altered key
identifier makes the message fail to open rather than quietly opening as
something else. :func:`key_id_of` reads it back without needing any key, which
is what an operator staring at an unreadable dead-letter queue actually wants.

This is the Java framing, byte for byte, and Java and Ruby are the libraries it
interoperates with. **The five AceMQ libraries write three different things
under this one content type**, which is recorded here rather than smoothed over
because a consumer cannot tell which it is about to be handed:

===========  ======  =======  ==========  ==========  ================  ==========
library      magic   version  id length   iv          cipher            tag
===========  ======  =======  ==========  ==========  ================  ==========
Java         0xAE    0x01     1 byte      12-byte     AES-GCM           16 bytes
**Python**   0xAE    0x01     1 byte      12-byte     AES-GCM           16 bytes
Ruby         0xAE    0x01     1 byte      12-byte     AES-GCM           16 bytes
Go           none    0x01     2, big-end  12-byte     AES-GCM           16 bytes
.NET         none    0x01     1 byte      16-byte     AES-256-CBC       HMAC-SHA-256,
                                                      then HMAC         32 bytes
===========  ======  =======  ==========  ==========  ================  ==========

Java's is the one implemented here, and it is the right one to converge on: it
is the only framing whose first byte identifies the format at all, which is what
lets a body that was never encrypted be refused rather than misparsed. Go needs
the magic byte and a one-byte length; .NET needs both of those and AES-GCM in
place of encrypt-then-MAC. Until then, a message from Go or .NET is refused here
— visibly, naming the framing — rather than being decrypted into something else.

``tests/test_encrypted.py`` holds a complete test vector for a known key, nonce
and plaintext. That is the string to converge against.

What this does not do
---------------------

The broker can no longer read the message, and neither can the people who
operate it. **Decide what they do instead before turning this on**: a
dead-letter queue full of ciphertext is a queue nobody can triage, and the
answer is usually a small internal tool holding the keyring rather than the
management UI.

Encryption is not authorisation. Every service holding the keyring can read
every message encrypted with those keys; the granularity is the key, so separate
audiences mean separate keys. Anybody holding a key can also *write* a message
this codec will decrypt without complaint, so a key is a shared secret and not a
signature.

Nor does it hide the routing. Exchange, routing key, headers and message size
stay in the clear, and for many systems the routing key is the sensitive part.

Nothing here ever puts a plaintext, a key, or any part of either into an
exception, a log line or a :func:`repr`. A failure says which key identifier the
message named and stops there, and the tests pin it.
"""

from __future__ import annotations

import base64
import os
from typing import Any

from ..ack import FatalError
from ..codec import Codec
from ..errors import AceMQError

#: What this codec writes, and what Java writes.
#:
#: Deliberately not ``...+json``, whatever the plaintext underneath is. A
#: ``+json`` suffix is a promise that the bytes on the wire are JSON, and every
#: JSON-aware consumer reads it that way. These bytes are ciphertext. Naming
#: them ``+json`` makes the JSON codec volunteer to decode them, which is how a
#: message ends up failing in a parser rather than being refused by a codec that
#: knows it cannot help.
ENCRYPTED_CONTENT_TYPE = "application/vnd.acemq.encrypted"

#: Marks the framing as this codec's, so a message from elsewhere is refused
#: rather than decrypted.
MAGIC = 0xAE

#: Version 1. Present so a later framing can be told apart from this one by its
#: first two bytes.
VERSION = 0x01

#: GCM's nonce, in bytes. Twelve is the size the construction is defined for;
#: any other length sends the nonce through an extra hash and buys nothing.
NONCE_BYTES = 12

#: The authentication tag, in bytes. 128 bits, which is what Java and Go use and
#: the only length worth using: a truncated tag is a cheaper forgery.
TAG_BYTES = 16

#: The longest a key identifier may be, in UTF-8 bytes. It is one byte of
#: framing in front of every message, so it is a name rather than a description.
MAX_KEY_ID_BYTES = 255

#: The key lengths AES takes. 32 bytes — AES-256 — is what :func:`generate_key`
#: produces; 16 and 24 are accepted because Java accepts them and a key that
#: already exists should not need re-issuing to be readable here.
KEY_SIZES = (16, 24, 32)


def generate_key() -> bytes:
    """Draws a fresh 32-byte key from the operating system's random source.

    :returns: a new AES-256 key
    """
    return os.urandom(32)


def key_from_base64(encoded: str) -> bytes:
    """Reads a key out of Base64, which is how one travels in configuration.

    :param encoded: the Base64 text, with surrounding whitespace ignored
    :returns: the key bytes
    :raises AceMQError: when the text is not Base64, or does not decode to a
        length AES takes. The message never quotes the input, because the input
        is a key
    """
    try:
        key = base64.b64decode(encoded.strip(), validate=True)
    except (ValueError, TypeError) as failure:
        # Deliberately not echoing ``encoded``: it is the key, and an exception
        # is the one place it must never be written down.
        raise AceMQError(
            "acemq: this is not Base64, so no key could be read from it: "
            f"{type(failure).__name__}"
        ) from None
    _check_key_size(key)
    return key


def key_to_base64(key: bytes) -> str:
    """Renders a key as Base64.

    The inverse of :func:`key_from_base64`, for writing a generated key into a
    secret store. Whatever this returns is the key itself, so it belongs in the
    same place the database password does and nowhere else.

    :param key: the key bytes
    :returns: the Base64 text
    """
    return base64.b64encode(key).decode("ascii")


def _check_key_size(key: bytes) -> None:
    if len(key) not in KEY_SIZES:
        raise AceMQError(
            f"acemq: an AES key is 16, 24 or 32 bytes and this one is {len(key)}. If it came "
            "from a passphrase it needs a key derivation function such as PBKDF2, scrypt or "
            "Argon2 rather than being used as it stands"
        )


class EncryptionKey:
    """One key, and the name a message calls it by.

    :param id: what the key is called on the wire. Something datelike —
        ``2026-01`` — reads well when somebody is working out what to rotate.
        At most 255 UTF-8 bytes, because it travels in front of every message
    :param key: 16, 24 or 32 bytes of key material
    :raises AceMQError: when the identifier is empty or too long, or the key is
        not a length AES takes
    """

    __slots__ = ("_id", "_key")

    def __init__(self, id: str, key: bytes) -> None:
        if not id:
            raise AceMQError(
                "acemq: a key identifier cannot be empty: it is what a reader looks "
                "the key up by"
            )
        encoded = id.encode("utf-8")
        if len(encoded) > MAX_KEY_ID_BYTES:
            raise AceMQError(
                f"acemq: a key identifier is at most {MAX_KEY_ID_BYTES} bytes and {id!r} is "
                f"{len(encoded)}. It travels in front of every message, so it is a name rather "
                "than a description"
            )
        if "\x00" in id:
            raise AceMQError(f"acemq: the key identifier {id!r} contains a null byte")
        _check_key_size(key)
        self._id = id
        self._key = bytes(key)

    @property
    def id(self) -> str:
        """What a message calls this key."""
        return self._id

    @property
    def key(self) -> bytes:
        """The key material."""
        return self._key

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, EncryptionKey):
            return NotImplemented
        return self._id == other._id and self._key == other._key

    def __hash__(self) -> int:
        return hash((self._id, self._key))

    def __repr__(self) -> str:
        """Names the key and never shows it.

        A key logged by accident — in a dataclass dump, in an exception, in a
        debugger's variables pane — prints its identifier and nothing else. That
        is the whole reason this is a type rather than a tuple, and a test pins
        it.
        """
        return f"EncryptionKey(id={self._id!r})"

    __str__ = __repr__


class Keyring:
    """The keys a process can use, and which one it writes with.

    More than one, because rotation needs an overlap: new messages are written
    with the newest key while messages written with the old one are still being
    read. A keyring with one key cannot rotate without an outage, and the order
    to do it in is *add the new key everywhere first*, so every consumer can read
    it, and only then :meth:`use` it somewhere.

    :param keys: the keys to hold. The first is the one written with, unless
        :meth:`use` says otherwise
    :raises AceMQError: when given no keys at all
    """

    __slots__ = ("_current", "_keys")

    def __init__(self, *keys: EncryptionKey) -> None:
        if not keys:
            raise AceMQError(
                "acemq: a keyring needs at least one key; there is nothing to write with"
            )
        self._keys: dict[str, EncryptionKey] = {}
        for key in keys:
            self._keys[key.id] = key
        self._current = keys[0].id

    @classmethod
    def of(cls, id: str, key: bytes) -> Keyring:
        """A keyring holding one key, which is where every rotation starts.

        :param id: what the key is called on the wire
        :param key: the key material
        :returns: the keyring
        """
        return cls(EncryptionKey(id, key))

    def add(self, key: EncryptionKey) -> None:
        """Puts a key on the ring without making it the one written with.

        :param key: the key to hold
        """
        self._keys[key.id] = key

    def use(self, id: str) -> None:
        """Makes a key the one new messages are written with.

        :param id: which key
        :raises AceMQError: when that key is not on the ring
        """
        if id not in self._keys:
            raise AceMQError(f"acemq: there is no key {id!r} on this keyring")
        self._current = id

    @property
    def current(self) -> EncryptionKey:
        """The key new messages are written with."""
        return self._keys[self._current]

    @property
    def ids(self) -> tuple[str, ...]:
        """The keys on the ring, in the order they were added. Never the keys
        themselves — this is for a health endpoint or a line at start-up."""
        return tuple(self._keys)

    def key_for(self, id: str) -> EncryptionKey:
        """Looks a key up by the name a message called it.

        :param id: the identifier read off the message
        :returns: the key
        :raises FatalError: when the ring does not hold it. Fatal rather than
            retryable, because the same bytes will name the same missing key on
            every attempt and retrying only holds the queue open until the
            message ages out
        """
        key = self._keys.get(id)
        if key is None:
            raise FatalError(
                f"acemq: this message was encrypted with key {id!r}, which is not on this "
                f"keyring (it holds {', '.join(self._keys) or 'nothing'}). Either the key was "
                "retired while messages written with it were still queued, or the message came "
                "from a service using a different keyring"
            )
        return key

    def __repr__(self) -> str:
        return f"Keyring(current={self._current!r}, holds={list(self._keys)!r})"


def frame(key_id: str) -> bytes:
    """The header a message written with ``key_id`` carries.

    Magic, version, one length byte and the identifier in UTF-8 — the bytes
    ahead of the nonce, and the bytes GCM authenticates. Public because a test
    that wants to prove the framing has to be able to build it, and because a
    tool reading a queue may want to recognise it.

    :param key_id: the identifier to frame
    :returns: the header bytes
    """
    encoded = key_id.encode("utf-8")
    return bytes((MAGIC, VERSION, len(encoded))) + encoded


def key_id_of(body: bytes) -> str | None:
    """Reads which key a message needs, without needing the key.

    For the operator looking at a dead-letter queue they can no longer read. The
    identifier is in the clear in front of the ciphertext, so this answers "which
    key does this need?" from the bytes alone — which is usually the question,
    because a queue full of undecryptable messages is normally a key that was
    retired too early rather than anything wrong with the messages.

    :param body: a message body
    :returns: the key identifier, or ``None`` when this was not written by this
        codec
    """
    if len(body) < 3 or body[0] != MAGIC or body[1] != VERSION:
        return None
    length = body[2]
    if length == 0 or len(body) < 3 + length:
        return None
    try:
        return body[3 : 3 + length].decode("utf-8")
    except UnicodeDecodeError:
        return None


class EncryptedCodec:
    """Encrypts what another codec produced, and decrypts before handing it back.

    :param delegate: the codec that turns objects into bytes; those bytes are
        what gets encrypted
    :param keyring: the keys to write with and read with
    :raises AceMQError: when ``cryptography`` is not installed
    """

    def __init__(self, delegate: Codec, keyring: Keyring) -> None:
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as missing:  # pragma: no cover - depends on the install
            raise AceMQError(
                "acemq: EncryptedCodec needs cryptography, which is an optional extra. "
                'Install it with pip install "acemq-amqp[crypto]"'
            ) from missing

        # AES and GCM come from cryptography, which wraps OpenSSL. Writing
        # either by hand is how a side channel gets into a message path, and
        # there is no version of that decision that ends well.
        self._aesgcm: Any = AESGCM
        self._delegate = delegate
        self._keyring = keyring

    @property
    def content_type(self) -> str:
        return ENCRYPTED_CONTENT_TYPE

    @property
    def delegate(self) -> Codec:
        """The wrapped codec, whose output is what gets encrypted.

        Its content type is not visible on the wire, because saying "this is
        encrypted JSON" tells an observer more than they need. A consumer knows
        what to expect from its own configuration.
        """
        return self._delegate

    def encode(self, payload: Any) -> bytes:
        key = self._keyring.current
        header = frame(key.id)

        # A fresh nonce per message, from the operating system. Reusing one
        # under GCM does not weaken the encryption, it forfeits it: two messages
        # under the same key and nonce leak their difference outright, and a
        # counter that restarts with the process — which every counter in a
        # container eventually does — reuses them by design.
        nonce = os.urandom(NONCE_BYTES)

        plaintext = self._delegate.encode(payload)
        try:
            sealed: bytes = self._aesgcm(key.key).encrypt(nonce, plaintext, header)
        except Exception as failure:
            # Without the payload, and without the key. An exception that
            # helpfully printed what could not be encrypted would write the
            # plaintext to the log, which is the one place it was never supposed
            # to reach. The type of the failure is all that comes out.
            raise AceMQError(
                f"acemq: could not encrypt a {type(payload).__name__} with key "
                f"{key.id!r}: {type(failure).__name__}"
            ) from None
        return header + nonce + sealed

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        key_id = key_id_of(body)
        if key_id is None:
            raise FatalError(
                "acemq: this message was not written by EncryptedCodec — it does not "
                "start with the framing this codec writes. A consumer configured to "
                "decrypt has been pointed at a queue carrying something else"
            )

        header_length = 3 + len(key_id.encode("utf-8"))
        if len(body) < header_length + NONCE_BYTES + TAG_BYTES:
            raise FatalError(
                "acemq: this message is too short to hold a nonce and an authentication "
                "tag, so it was truncated somewhere between being written and being read"
            )

        key = self._keyring.key_for(key_id)
        header = body[:header_length]
        nonce = body[header_length : header_length + NONCE_BYTES]
        sealed = body[header_length + NONCE_BYTES :]

        try:
            plaintext: bytes = self._aesgcm(key.key).decrypt(nonce, sealed, header)
        except Exception:
            # One answer for every way this can fail. GCM authenticates as well
            # as encrypts, so a wrong key and a tampered ciphertext are the same
            # event here, and they are told apart by nothing — no separate
            # message, no separate type, no earlier check that would have
            # distinguished them. An error that said which one it was would be a
            # padding oracle with better manners.
            raise FatalError(
                f"acemq: this message did not decrypt with key {key_id!r}. Either that is "
                "not the key it was written with, or it was altered after it was written"
            ) from None

        return self._delegate.decode(plaintext, self._delegate.content_type)

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts only its own content type.

        Volunteering for anything else means trying to decrypt plaintext and
        reporting the failure as a decode error, which sends whoever is
        debugging it in precisely the wrong direction. Never a message with no
        content type either, for the same reason.
        """
        if not content_type:
            return False
        return content_type.lower().startswith(ENCRYPTED_CONTENT_TYPE)

    def __repr__(self) -> str:
        """Names the codec and the current key, and shows neither key nor body."""
        return f"EncryptedCodec({self._delegate!r}, key={self._keyring.current.id!r})"
