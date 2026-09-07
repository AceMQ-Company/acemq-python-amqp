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

"""What each codec claims, and what a composite does with the claims."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from acemq_amqp import FatalError
from acemq_amqp.codec import (
    BYTES_CONTENT_TYPE,
    JSON_CONTENT_TYPE,
    TEXT_CONTENT_TYPE,
    BytesCodec,
    Codec,
    CompositeCodec,
    JsonCodec,
    TextCodec,
    codec_by_name,
    codec_names,
    register_codec,
)


@dataclass
class Order:
    id: str
    total_cents: int


def test_json_round_trips_a_mapping() -> None:
    codec = JsonCodec()
    assert codec.content_type == JSON_CONTENT_TYPE
    assert codec.decode(codec.encode({"id": "7"})) == {"id": "7"}


def test_json_encodes_a_dataclass_by_its_field_names() -> None:
    assert JsonCodec().decode(JsonCodec().encode(Order("7", 900))) == {
        "id": "7",
        "total_cents": 900,
    }


def test_json_refuses_a_payload_it_cannot_carry() -> None:
    with pytest.raises(TypeError, match="JSON can carry"):
        JsonCodec().encode(object())


def test_a_body_that_is_not_json_is_fatal_rather_than_retryable() -> None:
    # Fatal is the whole point: the same bytes fail identically on every
    # attempt, so the message belongs in the dead-letter queue immediately.
    with pytest.raises(FatalError):
        JsonCodec().decode(b"{not json")


@pytest.mark.parametrize(
    ("content_type", "claimed"),
    [
        (None, True),
        ("", True),
        ("application/json", True),
        ("application/json; charset=utf-8", True),
        ("APPLICATION/JSON", True),
        ("text/json", True),
        ("application/vnd.acemq.order+json", True),
        ("text/plain", False),
        ("application/octet-stream", False),
    ],
)
def test_what_json_answers_for(content_type: str | None, claimed: bool) -> None:
    assert JsonCodec().can_decode(content_type) is claimed


def test_text_does_not_answer_for_a_message_with_no_content_type() -> None:
    # A codec that turns any bytes into a string never fails, so answering for
    # an untyped message would hand a handler the printable rendering of a body
    # that should have gone somewhere else.
    codec = TextCodec()
    assert codec.can_decode(None) is False
    assert codec.can_decode("text/plain; charset=utf-8") is True
    assert codec.content_type == TEXT_CONTENT_TYPE
    assert codec.decode(codec.encode("a line")) == "a line"


def test_bytes_answers_for_everything_and_changes_nothing() -> None:
    codec = BytesCodec()
    assert codec.content_type == BYTES_CONTENT_TYPE
    assert codec.can_decode(None) is True
    assert codec.can_decode("application/x-anything") is True
    assert codec.decode(codec.encode(b"\x00\x01")) == b"\x00\x01"
    assert codec.encode("text") == b"text"


def test_a_composite_writes_with_its_first_codec() -> None:
    composite = CompositeCodec(TextCodec(), JsonCodec())
    assert composite.content_type == TEXT_CONTENT_TYPE
    assert composite.encode("hello") == b"hello"


def test_a_composite_reads_with_whichever_codec_claims_the_content_type() -> None:
    composite = CompositeCodec(JsonCodec(), TextCodec())
    assert composite.decode(b'{"id": "7"}', "application/json") == {"id": "7"}
    assert composite.decode(b"a line", "text/plain") == "a line"


def test_with_no_content_type_every_codec_is_a_candidate() -> None:
    # Nothing has been ruled out, so the first that reads the body wins rather
    # than the first in the list being assumed right. Text would happily turn
    # this into a string, and JSON gets there first because it is first.
    composite = CompositeCodec(JsonCodec(), TextCodec())
    assert composite.decode(b'{"id": "7"}', None) == {"id": "7"}

    # And a body JSON refuses still gets read, by the codec after it.
    assert composite.decode(b"not json at all", None) == "not json at all"


def test_a_composite_says_what_it_holds_when_nothing_will_read_a_message() -> None:
    composite = CompositeCodec(JsonCodec(), TextCodec())
    with pytest.raises(FatalError, match="application/json, text/plain"):
        composite.decode(b"\x00", "application/octet-stream")


def test_a_composite_collects_every_refusal_rather_than_the_last() -> None:
    composite = CompositeCodec(JsonCodec())
    with pytest.raises(FatalError, match="JsonCodec"):
        composite.decode(b"{not json", None)


def test_a_composite_needs_at_least_one_codec() -> None:
    with pytest.raises(ValueError, match="at least one codec"):
        CompositeCodec()


def test_a_composite_claims_what_any_of_its_codecs_claims() -> None:
    composite = CompositeCodec(JsonCodec(), TextCodec())
    assert composite.can_decode("text/plain") is True
    assert composite.can_decode("application/octet-stream") is False


def test_the_registry_builds_a_codec_by_name() -> None:
    assert codec_names() == ["bytes", "json", "text"]
    assert isinstance(codec_by_name("json"), JsonCodec)


def test_an_unknown_codec_name_says_which_ones_there_are() -> None:
    with pytest.raises(KeyError, match="json"):
        codec_by_name("yaml")


def test_registering_a_name_twice_replaces_the_first() -> None:
    class Loud(JsonCodec):
        pass

    try:
        register_codec("json", Loud)
        assert isinstance(codec_by_name("json"), Loud)
    finally:
        register_codec("json", JsonCodec)


def test_the_codecs_satisfy_the_protocol() -> None:
    # runtime_checkable only checks the names exist; the value of this is that
    # mypy checks the signatures at the same time the assertion checks the shape.
    codecs: list[Codec] = [JsonCodec(), TextCodec(), BytesCodec(), CompositeCodec(JsonCodec())]
    for codec in codecs:
        assert isinstance(codec, Codec)


def test_a_codec_can_be_something_this_library_never_heard_of() -> None:
    class Reversed:
        @property
        def content_type(self) -> str:
            return "application/x-reversed"

        def encode(self, payload: Any) -> bytes:
            return str(payload).encode()[::-1]

        def decode(self, body: bytes, content_type: str | None = None) -> Any:
            return body[::-1].decode()

        def can_decode(self, content_type: str | None) -> bool:
            return content_type == "application/x-reversed"

    codec: Codec = Reversed()
    assert codec.decode(codec.encode("abc")) == "abc"
