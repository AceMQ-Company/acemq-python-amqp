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

"""What the five optional codecs do on their own.

The cross-language half is in ``test_codecs_interop.py``; this is the behaviour
that has to hold before that half means anything — the protocol, the refusals,
the XML parser's posture towards a document that arrived from a queue, and the
two Avro framings.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from test_codecs_interop import order_placed

from acemq_amqp import Codec, CompositeCodec, FatalError, JsonCodec, codec_by_name, codec_names
from acemq_amqp.codecs.avro import AvroCodec
from acemq_amqp.codecs.protobuf import ProtobufCodec
from acemq_amqp.codecs.toml import TomlCodec
from acemq_amqp.codecs.xml import DEFAULT_ROOT, XmlCodec
from acemq_amqp.codecs.yaml import YamlCodec
from acemq_amqp.errors import AceMQError
from acemq_amqp.patterns.schema import InMemorySchemaRegistry

AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "OrderPlaced",
        "namespace": "org.acemq.test",
        "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "totalCents", "type": "long"},
            {"name": "tenant", "type": "string"},
        ],
    }
)

# One field added, with a default, which is the change a registry exists to
# survive: a consumer holding this schema reads a message written with the one
# above and gets the default rather than a failure.
AVRO_SCHEMA_V2 = json.dumps(
    {
        "type": "record",
        "name": "OrderPlaced",
        "namespace": "org.acemq.test",
        "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "totalCents", "type": "long"},
            {"name": "tenant", "type": "string"},
            {"name": "channel", "type": "string", "default": "web"},
        ],
    }
)

ORDER = {"orderId": "o-1", "totalCents": 4250, "tenant": "acme"}


@dataclass
class OrderPlaced:
    order_id: str
    total_cents: int


def test_every_codec_satisfies_the_protocol() -> None:
    codecs: list[Codec] = [
        YamlCodec(),
        TomlCodec(),
        XmlCodec(),
        ProtobufCodec(order_placed()),
        AvroCodec(AVRO_SCHEMA),
    ]
    for codec in codecs:
        assert isinstance(codec, Codec)


def test_the_three_that_need_no_arguments_are_in_the_registry() -> None:
    # Importing the module registers it, which is how Go's init() does it.
    assert {"yaml", "toml", "xml"} <= set(codec_names())
    assert codec_by_name("yaml").content_type == "application/yaml"
    assert codec_by_name("toml").content_type == "application/toml"
    assert codec_by_name("xml").content_type == "application/xml"


def test_protobuf_and_avro_are_not_in_the_registry() -> None:
    # Deliberate: neither format's bytes describe themselves, so a codec needs a
    # message type or a schema and a no-argument factory has nothing to give.
    assert "protobuf" not in codec_names()
    assert "avro" not in codec_names()


# --------------------------------------------------------------------------
# YAML
# --------------------------------------------------------------------------


def test_yaml_writes_block_style_which_is_the_reason_to_pick_it() -> None:
    body = YamlCodec().encode({"items": ["widget", "gasket"], "total": 2})
    assert b"- widget" in body
    assert b"[" not in body  # flow style would be JSON with extra steps


def test_yaml_keeps_the_order_the_payload_was_built_in() -> None:
    body = YamlCodec().encode({"zebra": 1, "apple": 2})
    assert body == b"zebra: 1\napple: 2\n"
    assert YamlCodec(sort_keys=True).encode({"zebra": 1, "apple": 2}) == b"apple: 2\nzebra: 1\n"


def test_yaml_writes_no_document_start_marker() -> None:
    assert not YamlCodec().encode({"a": 1}).startswith(b"---")


def test_yaml_encodes_a_dataclass_by_its_field_names() -> None:
    assert YamlCodec().decode(YamlCodec().encode(OrderPlaced("o-1", 4250))) == {
        "order_id": "o-1",
        "total_cents": 4250,
    }


def test_yaml_writes_non_ascii_as_itself() -> None:
    assert "Söderström".encode() in YamlCodec().encode({"name": "Söderström"})


def test_yaml_never_builds_a_python_object_out_of_a_message() -> None:
    # The reason safe_load is not configurable. Under the default loader this
    # body constructs an object; under safe_load it is refused.
    attack = b"!!python/object/apply:os.system ['echo pwned']\n"
    with pytest.raises(FatalError, match="not YAML"):
        YamlCodec().decode(attack)


def test_a_body_that_is_not_yaml_is_fatal() -> None:
    with pytest.raises(FatalError, match="not YAML"):
        YamlCodec().decode(b"{unclosed: [")


# --------------------------------------------------------------------------
# TOML
# --------------------------------------------------------------------------


def test_toml_round_trips_a_table() -> None:
    codec = TomlCodec()
    assert codec.decode(codec.encode(ORDER)) == ORDER


def test_toml_encodes_a_dataclass_by_its_field_names() -> None:
    assert TomlCodec().decode(TomlCodec().encode(OrderPlaced("o-1", 4250))) == {
        "order_id": "o-1",
        "total_cents": 4250,
    }


@pytest.mark.parametrize("payload", [["a", "b"], 42, "text", None])
def test_toml_refuses_a_payload_that_is_not_a_table(payload: Any) -> None:
    # Refused at the publisher rather than at the consumer. The alternative is a
    # message nothing can read, discovered at the wrong end of the wire — which
    # is exactly what Java and Go guard against in their own encoders.
    with pytest.raises(TypeError, match=r"top level has to be a mapping"):
        TomlCodec().encode(payload)


def test_a_body_that_is_not_toml_is_fatal() -> None:
    with pytest.raises(FatalError, match="not TOML"):
        TomlCodec().decode(b"[[[not toml")


# --------------------------------------------------------------------------
# XML, and what it does with a body that arrived from a queue
# --------------------------------------------------------------------------


def test_xml_round_trips_a_mapping_as_strings() -> None:
    codec = XmlCodec()
    body = codec.encode({"orderId": "o-1", "totalCents": 4250})
    assert body == b"<message><orderId>o-1</orderId><totalCents>4250</totalCents></message>"
    # XML has no types, so everything comes back a string. A consumer that wants
    # an int converts it, where it can decide what an unparseable field means.
    assert codec.decode(body) == {"orderId": "o-1", "totalCents": "4250"}


def test_xml_names_the_root_after_a_dataclass_the_way_jackson_does() -> None:
    assert XmlCodec().encode(OrderPlaced("o-1", 4250)).startswith(b"<OrderPlaced>")
    assert XmlCodec().root == DEFAULT_ROOT
    assert XmlCodec("Order").encode({"id": "1"}).startswith(b"<Order>")


def test_xml_writes_a_repeated_tag_for_a_list_and_reads_it_back() -> None:
    codec = XmlCodec()
    body = codec.encode({"items": ["widget", "gasket"]})
    assert body == b"<message><items>widget</items><items>gasket</items></message>"
    assert codec.decode(body) == {"items": ["widget", "gasket"]}


def test_xml_writes_booleans_the_way_java_and_go_write_them() -> None:
    # str(True) would put "True" on the wire and nothing else would read it.
    assert XmlCodec().encode({"paid": True, "shipped": False}) == (
        b"<message><paid>true</paid><shipped>false</shipped></message>"
    )


def test_xml_reads_attributes_as_fields() -> None:
    assert XmlCodec().decode(b'<order id="o-1"><total>42</total></order>') == {
        "id": "o-1",
        "total": "42",
    }


def test_xml_reads_a_namespaced_document_by_its_local_names() -> None:
    body = b'<ns:order xmlns:ns="urn:acme"><ns:total>42</ns:total></ns:order>'
    assert XmlCodec().decode(body) == {"total": "42"}


def test_xml_refuses_a_payload_that_has_no_root_element() -> None:
    with pytest.raises(TypeError, match="has to be a mapping"):
        XmlCodec().encode(["a", "b"])


def test_a_body_that_is_not_xml_is_fatal() -> None:
    with pytest.raises(FatalError, match="not XML"):
        XmlCodec().decode(b"<unclosed>")


def test_xml_refuses_the_billion_laughs_expansion() -> None:
    """The one Python is actually exposed to, and the reason for the posture.

    ``xml.etree.ElementTree.fromstring`` expands internal entities. Nothing has
    to be fetched and no file has to be readable for this to take a consumer
    down, so the codec refuses the declaration rather than the expansion.
    """
    bomb = (
        b'<?xml version="1.0"?>\n'
        b'<!DOCTYPE lolz [<!ENTITY lol "lol">'
        b'<!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">'
        b'<!ENTITY lol2 "&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;">]>\n'
        b"<root>&lol2;</root>"
    )
    with pytest.raises(FatalError, match="document type declaration"):
        XmlCodec().decode(bomb)


def test_xml_refuses_an_external_entity() -> None:
    attack = (
        b'<?xml version="1.0"?>\n'
        b'<!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>\n'
        b"<r>&x;</r>"
    )
    with pytest.raises(FatalError, match="document type declaration"):
        XmlCodec().decode(attack)


def test_xml_refuses_a_dtd_that_does_nothing_at_all() -> None:
    # The refusal is of the construct, not of a particular attack, so there is
    # nothing to be clever about getting past.
    with pytest.raises(FatalError, match="document type declaration"):
        XmlCodec().decode(b'<!DOCTYPE r SYSTEM "r.dtd"><r><a>1</a></r>')


def test_the_refusal_of_a_dtd_is_not_configurable() -> None:
    # There is no constructor argument that turns it back on. If one is ever
    # added, this fails and somebody has to argue for it.
    signature = XmlCodec.__init__.__code__
    arguments = set(signature.co_varnames[: signature.co_argcount])
    assert arguments == {"self", "root"}


def test_a_doctype_inside_character_data_is_just_text() -> None:
    # The guard is the parser's, not a substring search, so it does not fire on
    # a document that merely mentions one.
    assert XmlCodec().decode(b"<r><![CDATA[<!DOCTYPE evil>]]></r>") == "<!DOCTYPE evil>"


# --------------------------------------------------------------------------
# Protobuf
# --------------------------------------------------------------------------


def test_protobuf_round_trips_a_generated_message() -> None:
    order_type = order_placed()
    codec = ProtobufCodec(order_type)
    message = order_type(order_id="o-1", total_cents=4250, tenant="acme")
    decoded = codec.decode(codec.encode(message))
    assert (decoded.order_id, decoded.total_cents, decoded.tenant) == ("o-1", 4250, "acme")


def test_protobuf_refuses_a_payload_that_is_not_its_message_type() -> None:
    with pytest.raises(TypeError, match="was given a dict"):
        ProtobufCodec(order_placed()).encode({"order_id": "o-1"})


def test_protobuf_refuses_a_class_protoc_did_not_generate() -> None:
    with pytest.raises(AceMQError, match="protoc generated"):
        ProtobufCodec(dict)


def test_a_body_that_is_not_this_protobuf_message_is_fatal() -> None:
    with pytest.raises(FatalError, match="not a OrderPlaced"):
        ProtobufCodec(order_placed()).decode(b"\xff\xff\xff\xff\xff")


# --------------------------------------------------------------------------
# Avro, and the two framings
# --------------------------------------------------------------------------


def test_avro_round_trips_against_a_fixed_schema() -> None:
    codec = AvroCodec(AVRO_SCHEMA)
    assert codec.is_registered is False
    assert codec.decode(codec.encode(ORDER), "avro/binary") == ORDER


def test_avro_takes_a_schema_as_text_or_as_a_parsed_mapping() -> None:
    from_text = AvroCodec(AVRO_SCHEMA)
    from_mapping = AvroCodec(json.loads(AVRO_SCHEMA))
    assert from_mapping.decode(from_text.encode(ORDER), "avro/binary") == ORDER


def test_avro_refuses_a_schema_that_is_not_avro() -> None:
    with pytest.raises(AceMQError, match="not a usable Avro schema"):
        AvroCodec('{"type": "nonsense"}')


def test_a_registered_codec_frames_the_identifier_and_a_fixed_one_does_not() -> None:
    fixed = AvroCodec(AVRO_SCHEMA).encode(ORDER)
    framed = AvroCodec(AVRO_SCHEMA, schema_id=9).encode(ORDER)
    assert framed == b"\x00\x00\x00\x00\x09" + fixed


def test_a_registered_codec_refuses_a_message_with_no_identifier_on_it() -> None:
    fixed = AvroCodec(AVRO_SCHEMA).encode(ORDER)
    with pytest.raises(FatalError, match="no schema identifier"):
        AvroCodec(AVRO_SCHEMA, schema_id=7).decode(fixed, "application/vnd.acemq.avro")


def test_a_fixed_codec_refuses_a_message_that_says_it_carries_an_identifier() -> None:
    # Reading it would not throw: the five framing bytes decode as the start of
    # the first field and every value comes out wrong, quietly.
    framed = AvroCodec(AVRO_SCHEMA, schema_id=7).encode(ORDER)
    with pytest.raises(FatalError, match="silently produce the wrong values"):
        AvroCodec(AVRO_SCHEMA).decode(framed, "application/vnd.acemq.avro")


def test_a_fixed_codec_reads_a_body_whose_first_byte_is_zero_when_told_the_type() -> None:
    """Where this improves on Java rather than copying it.

    Java's fixed-schema decode refuses any body of five bytes or more starting
    with a zero byte, on the grounds that it might be framed. But a legitimate
    Avro body starts with a zero byte whenever its first field encodes to one —
    an empty string does, and so do ``0``, ``false`` and the first branch of a
    union — so that rule refuses real messages. The content type is the better
    signal and it is right there on the message, so it is used first.
    """
    schema = json.dumps(
        {
            "type": "record",
            "name": "Counter",
            "fields": [
                {"name": "label", "type": "string"},
                {"name": "count", "type": "long"},
                {"name": "tenant", "type": "string"},
            ],
        }
    )
    codec = AvroCodec(schema)
    counter = {"label": "", "count": 123456, "tenant": "acme"}
    body = codec.encode(counter)
    # An empty first field encodes to a single zero byte, which is exactly what
    # Java's heuristic mistakes for the front of a schema identifier.
    assert body[0] == 0 and len(body) >= 5
    assert codec.decode(body, "avro/binary") == counter

    # Any content type that names Avro is believed, whatever the first byte is:
    # the sender said what the bytes are, and the heuristic exists only for when
    # nobody did.
    assert codec.decode(body, "application/avro") == counter
    assert codec.decode(body, "application/vnd.acme.counter+avro") == counter

    # With nothing said at all there is no signal but the byte, so it refuses
    # rather than guessing — which is what a CompositeCodec with no content type
    # would hand it.
    with pytest.raises(FatalError, match="nothing said they were Avro"):
        codec.decode(body, None)

    # And a content type that says nothing useful is silence with extra steps.
    # application/octet-stream names no framing and no format, so the last-resort
    # guess applies to it exactly as it does to an absent one — the alternative
    # is reading five bytes of someone else's schema identifier as a field.
    with pytest.raises(FatalError, match="nothing said they were Avro"):
        codec.decode(body, "application/octet-stream")


def test_an_unknown_schema_identifier_says_which_one_and_how_to_teach_it() -> None:
    codec = AvroCodec(AVRO_SCHEMA, schema_id=7)
    stranger = b"\x00\x00\x00\x00\x63" + AvroCodec(AVRO_SCHEMA).encode(ORDER)
    with pytest.raises(FatalError, match="written with schema 99"):
        codec.decode(stranger, "application/vnd.acemq.avro")

    codec.learn(99, AVRO_SCHEMA)
    assert codec.decode(stranger, "application/vnd.acemq.avro") == ORDER


def test_a_registered_codec_resolves_a_writer_schema_against_its_own() -> None:
    """The whole reason for the registered mode.

    A producer on the old schema writes three fields; a consumer built against
    the new one reads four, with the field it added filled in from its default
    rather than the message being refused.
    """
    producer = AvroCodec(AVRO_SCHEMA, schema_id=1)
    consumer = AvroCodec(AVRO_SCHEMA_V2, schema_id=2)
    consumer.learn(1, AVRO_SCHEMA)

    decoded = consumer.decode(producer.encode(ORDER), "application/vnd.acemq.avro")
    assert decoded == {**ORDER, "channel": "web"}


async def test_a_codec_can_be_built_from_the_registry_this_library_ships() -> None:
    registry = InMemorySchemaRegistry()
    producer = await AvroCodec.from_registry(registry, "order.placed", AVRO_SCHEMA)
    assert producer.is_registered
    assert producer.content_type == "application/vnd.acemq.avro"

    body = producer.encode(ORDER)
    identifier = int.from_bytes(body[1:5], "big")
    assert (await registry.by_id(identifier)).subject == "order.placed"

    # A consumer that has never seen this identifier looks it up once, outside
    # the message path, because the registry here is async and a codec is not.
    consumer = AvroCodec(AVRO_SCHEMA_V2)
    consumer = AvroCodec(AVRO_SCHEMA_V2, schema_id=identifier)
    await consumer.learn_from(registry, identifier)
    assert consumer.decode(body, "application/vnd.acemq.avro") == {**ORDER, "channel": "web"}


async def test_learning_from_a_registry_refuses_a_schema_of_another_format() -> None:
    registry = InMemorySchemaRegistry()
    definition = await registry.register("order.placed", "json-schema", "{}")
    codec = AvroCodec(AVRO_SCHEMA, schema_id=1)
    with pytest.raises(AceMQError, match="registered as json-schema"):
        await codec.learn_from(registry, definition.id)


def test_a_schema_identifier_has_to_be_positive() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        AvroCodec(AVRO_SCHEMA, schema_id=0)


def test_avro_refuses_a_payload_the_schema_cannot_carry() -> None:
    with pytest.raises(TypeError, match="cannot write"):
        AvroCodec(AVRO_SCHEMA).encode({"orderId": "o-1"})


# --------------------------------------------------------------------------
# In a composite, which is where a mixed queue meets them
# --------------------------------------------------------------------------


def test_a_composite_of_all_of_them_picks_by_content_type() -> None:
    codec = CompositeCodec(JsonCodec(), YamlCodec(), TomlCodec(), XmlCodec())
    assert codec.content_type == "application/json"

    assert codec.decode(b'{"a": 1}', "application/json") == {"a": 1}
    assert codec.decode(b"a: 1\n", "application/yaml") == {"a": 1}
    assert codec.decode(b"a = 1\n", "application/toml") == {"a": 1}
    assert codec.decode(b"<m><a>1</a></m>", "application/xml") == {"a": "1"}


def test_yaml_does_not_swallow_a_message_that_said_nothing() -> None:
    # YAML parses JSON, so a YAML codec that answered for an untyped message
    # would be right about the value and wrong about the format. JsonCodec is
    # first in the candidate list and gets it.
    codec = CompositeCodec(JsonCodec(), YamlCodec())
    assert codec.decode(b'{"a": 1}', None) == {"a": 1}
    assert YamlCodec().can_decode(None) is False
