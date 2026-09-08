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

"""Reading messages the Java and Go libraries wrote.

A codec that encodes and decodes its own output proves nothing about the thing
these codecs exist for. The gap they close is not that Python could not parse
YAML — it always could — but that a Java or Go service publishing YAML produced
a message nothing here would claim. So the bodies asserted against in this file
were not produced by this library.

``tests/fixtures/codec-interop-fixtures.json`` holds them, base64-encoded, with
their provenance recorded in the file itself: a Go program calling the same
functions ``acemq-go-amqp/codec/*`` calls at the versions its ``go.mod`` files
pin, and a Java program whose Jackson mappers are the bodies of the Java codecs'
``defaultMapper()`` methods at the versions ``acemq-java-amqp/pom.xml``
declares. Java and Go turned out to agree byte for byte on XML and on Avro in
both framings; their YAML differs only in list indentation and their TOML only
in quote style, which is exactly the sort of difference a decoder has to absorb
and a round-trip test would never have produced.

The protobuf message is built here from a descriptor rather than from a
``protoc``-generated module, so the suite needs no code generation step and no
generated file in the tree. It is the same ``OrderPlaced`` the .NET library's
protobuf tests use, and the bytes it produces were checked against the ones Go
emitted: identical.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from acemq_amqp import CompositeCodec, FatalError, JsonCodec
from acemq_amqp.codecs.avro import (
    AVRO_CONTENT_TYPE,
    AVRO_REGISTERED_CONTENT_TYPE,
    AvroCodec,
)
from acemq_amqp.codecs.protobuf import PROTOBUF_CONTENT_TYPE, ProtobufCodec
from acemq_amqp.codecs.toml import TOML_CONTENT_TYPE, TomlCodec
from acemq_amqp.codecs.xml import XML_CONTENT_TYPE, XmlCodec
from acemq_amqp.codecs.yaml import YAML_CONTENT_TYPE, YamlCodec

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "codec-interop-fixtures.json").read_text("utf-8")
)
SAMPLES: list[dict[str, Any]] = FIXTURES["samples"]
AVRO_SCHEMA: str = FIXTURES["avro_schema"]


def order_placed() -> type[Any]:
    """The ``.proto`` from the workspace, as a message class at runtime.

    ``message OrderPlaced { string order_id = 1; int64 total_cents = 2; string
    tenant = 3; }`` in package ``acemq.test``.
    """
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file = descriptor_pb2.FileDescriptorProto()
    file.name = "order.proto"
    file.package = "acemq.test"
    file.syntax = "proto3"
    message = file.message_type.add()
    message.name = "OrderPlaced"
    for name, number, kind in (
        ("order_id", 1, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("total_cents", 2, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("tenant", 3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    ):
        field = message.field.add()
        field.name = name
        field.number = number
        field.type = kind
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    built: type[Any] = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("acemq.test.OrderPlaced")
    )
    return built


def codec_for(sample: dict[str, Any]) -> Any:
    kind = sample["format"]
    if kind == "yaml":
        return YamlCodec()
    if kind == "toml":
        return TomlCodec()
    if kind == "xml":
        return XmlCodec()
    if kind == "protobuf":
        return ProtobufCodec(order_placed())
    if kind == "avro":
        return AvroCodec(AVRO_SCHEMA)
    if kind == "avro-registered":
        return AvroCodec(AVRO_SCHEMA, schema_id=7)
    raise AssertionError(f"no codec for {kind}")


def as_dict(decoded: Any) -> dict[str, Any]:
    """A decoded payload as a mapping, whatever the codec handed back."""
    if isinstance(decoded, dict):
        return decoded
    # The protobuf codec returns the generated message itself, which is the
    # point of it: the fields are typed rather than a bag of strings.
    return {field.name: value for field, value in decoded.ListFields()}


def sample_id(sample: dict[str, Any]) -> str:
    return f"{sample['producer']}-{sample['format']}"


@pytest.mark.parametrize("sample", SAMPLES, ids=sample_id)
def test_a_message_from_java_or_go_is_decoded(sample: dict[str, Any]) -> None:
    body = base64.b64decode(sample["body_base64"])
    codec = codec_for(sample)

    assert codec.can_decode(sample["content_type"]), (
        f"a {sample['producer']} message saying {sample['content_type']} was refused"
    )
    assert as_dict(codec.decode(body, sample["content_type"])) == sample["expected"]


@pytest.mark.parametrize("sample", SAMPLES, ids=sample_id)
def test_a_composite_finds_the_codec_for_a_foreign_message(sample: dict[str, Any]) -> None:
    # The realistic arrangement: a consumer holding JSON and one other format,
    # handed a message it did not publish. JSON is first, so this also shows the
    # foreign codec is reached rather than JSON swallowing it.
    codec = CompositeCodec(JsonCodec(), codec_for(sample))
    body = base64.b64decode(sample["body_base64"])
    assert as_dict(codec.decode(body, sample["content_type"])) == sample["expected"]


def test_java_and_go_agree_where_the_fixture_says_they_do() -> None:
    """The fixture's own claim, asserted rather than left as a comment."""
    by_key = {(s["producer"], s["format"]): s["body_base64"] for s in SAMPLES}
    for shape in ("xml", "avro", "avro-registered"):
        assert by_key[("java", shape)] == by_key[("go", shape)], (
            f"Java and Go no longer write identical {shape}"
        )
    # And where they do not agree, both are still readable — which is the whole
    # reason the accept-set matters more than the write type.
    for shape in ("yaml", "toml"):
        assert by_key[("java", shape)] != by_key[("go", shape)]


def test_the_registered_framing_is_confluents() -> None:
    """One zero byte, four bytes of identifier big-endian, then the body."""
    framed = base64.b64decode(
        next(s for s in SAMPLES if s["format"] == "avro-registered")["body_base64"]
    )
    fixed = base64.b64decode(next(s for s in SAMPLES if s["format"] == "avro")["body_base64"])

    assert framed[0] == 0
    assert int.from_bytes(framed[1:5], "big") == 7
    # The framing is a prefix and nothing else: the body after it is byte for
    # byte the body a fixed-schema codec writes.
    assert framed[5:] == fixed

    # And this library frames it the same way.
    written = AvroCodec(AVRO_SCHEMA, schema_id=7).encode(
        {"orderId": "o-1", "totalCents": 4250, "tenant": "acme"}
    )
    assert written == framed


# --------------------------------------------------------------------------
# The accept-sets. Getting these wrong means a message that should have been
# decodable is refused, which is the failure these codecs exist to prevent.
# --------------------------------------------------------------------------

ACCEPTED = [
    # YAML: application/yaml is RFC 9512; the other three predate it and are
    # what most senders still write. Java and Go accept the same four.
    ("yaml", "application/yaml"),
    ("yaml", "application/x-yaml"),
    ("yaml", "text/yaml"),
    ("yaml", "text/x-yaml"),
    ("yaml", "application/vnd.acme.order+yaml"),
    ("yaml", "APPLICATION/YAML"),
    ("yaml", "application/yaml; charset=utf-8"),
    ("toml", "application/toml"),
    ("toml", "text/toml"),
    ("toml", "application/vnd.acme.config+toml"),
    ("toml", "TEXT/TOML"),
    ("xml", "application/xml"),
    ("xml", "text/xml"),
    ("xml", "application/atom+xml"),
    ("xml", "application/xml; charset=utf-8"),
    ("protobuf", "application/x-protobuf"),
    ("protobuf", "application/protobuf"),
    # Go accepts this one and Java does not. Accepted here; see the changelog.
    ("protobuf", "application/vnd.google.protobuf"),
    ("protobuf", "application/vnd.acme.order+protobuf"),
    ("avro", "avro/binary"),
    ("avro", "application/avro"),
    ("avro", "avro/json"),
    ("avro", "application/vnd.acme.order+avro"),
    ("avro-registered", "application/vnd.acemq.avro"),
    ("avro-registered", "application/avro"),
]

REFUSED = [
    # None of them volunteers for a message whose sender said nothing. Only
    # JsonCodec does that, and it is the only one that should.
    ("yaml", None),
    ("toml", None),
    ("xml", None),
    ("protobuf", None),
    ("avro", None),
    ("avro-registered", None),
    ("yaml", ""),
    # An unrelated type is not claimed by anything here.
    ("yaml", "application/json"),
    ("yaml", "text/plain"),
    ("toml", "application/json"),
    ("toml", "application/yaml"),
    ("xml", "application/json"),
    ("xml", "text/html"),
    ("protobuf", "application/json"),
    ("protobuf", "application/octet-stream"),
    ("avro", "application/json"),
    # The two Avro framings are not interchangeable and the difference is
    # invisible in the bytes, so each mode claims only its own type. This is
    # Java's rule; Go's codec claims both in either mode.
    ("avro", "application/vnd.acemq.avro"),
    ("avro-registered", "avro/binary"),
]


@pytest.mark.parametrize(("kind", "content_type"), ACCEPTED)
def test_a_content_type_another_stack_might_write_is_accepted(
    kind: str, content_type: str
) -> None:
    assert codec_for({"format": kind}).can_decode(content_type)


@pytest.mark.parametrize(("kind", "content_type"), REFUSED)
def test_an_unrelated_content_type_is_refused(kind: str, content_type: str | None) -> None:
    assert not codec_for({"format": kind}).can_decode(content_type)


def test_an_alternative_content_type_decodes_a_real_message() -> None:
    """Claiming a type is not enough; the message has to come out the far end."""
    sample = next(s for s in SAMPLES if s["format"] == "yaml" and s["producer"] == "java")
    body = base64.b64decode(sample["body_base64"])
    for spelling in ("text/yaml", "application/x-yaml", "text/x-yaml"):
        codec = CompositeCodec(JsonCodec(), YamlCodec())
        assert codec.decode(body, spelling) == sample["expected"]


def test_a_composite_refuses_a_message_no_codec_claims() -> None:
    codec = CompositeCodec(YamlCodec(), TomlCodec(), XmlCodec())
    with pytest.raises(FatalError, match="no codec here reads"):
        codec.decode(b"anything", "application/vnd.ms-excel")


def test_the_write_types_are_the_ones_java_and_go_write() -> None:
    """The contract, in one place, so a change to it has to be deliberate."""
    assert YAML_CONTENT_TYPE == "application/yaml"
    assert TOML_CONTENT_TYPE == "application/toml"
    assert XML_CONTENT_TYPE == "application/xml"
    assert PROTOBUF_CONTENT_TYPE == "application/x-protobuf"
    assert AVRO_CONTENT_TYPE == "avro/binary"
    assert AVRO_REGISTERED_CONTENT_TYPE == "application/vnd.acemq.avro"

    assert YamlCodec().content_type == YAML_CONTENT_TYPE
    assert TomlCodec().content_type == TOML_CONTENT_TYPE
    assert XmlCodec().content_type == XML_CONTENT_TYPE
    assert ProtobufCodec(order_placed()).content_type == PROTOBUF_CONTENT_TYPE
    assert AvroCodec(AVRO_SCHEMA).content_type == AVRO_CONTENT_TYPE
    assert AvroCodec(AVRO_SCHEMA, schema_id=7).content_type == AVRO_REGISTERED_CONTENT_TYPE
