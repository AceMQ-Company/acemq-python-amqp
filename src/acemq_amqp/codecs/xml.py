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

"""Reads and writes XML, with no extra to install::

    from acemq_amqp.codecs.xml import XmlCodec

The only one of the five that needs nothing: it is written against
:mod:`xml.parsers.expat` and :mod:`xml.etree.ElementTree`, both standard
library. There is deliberately no ``[xml]`` extra, because there would be
nothing in it. ``defusedxml`` was considered and rejected for the same reason —
what it does is turn handlers off, and this module turns the same handlers off
directly and in a way nobody can turn back on.

**Every document with a DTD is refused, and that is not configurable.**

A message body arrives from a queue, which is exactly the sort of place a
message from somewhere unexpected turns up, and an XML parser handed one is a
well-known way to read files off the machine and open connections on behalf of
whoever sent it. Python's position is better than it is often given credit for
and worse than it needs to be:

* External entities are already inert. ``<!ENTITY x SYSTEM "file:///etc/passwd">``
  is not resolved by :mod:`xml.etree.ElementTree`; the reference fails as an
  undefined entity. So plain XXE and DTD retrieval are not live hazards.
* **Internal** entity expansion is not inert. The billion-laughs attack needs no
  network access and no file access — a few nested internal entities expand to
  gigabytes inside the parser and take a consumer down — and
  ``ElementTree.fromstring`` expands them happily. This was verified against the
  interpreter this library is tested on rather than taken from a table.

So rather than disabling the individual hazards, this refuses the whole
construct they all need: the parser's ``StartDoctypeDeclHandler``,
``EntityDeclHandler``, ``UnparsedEntityDeclHandler`` and
``ExternalEntityRefHandler`` each raise. A body carrying ``<!DOCTYPE`` is a
:class:`~acemq_amqp.ack.FatalError` naming the reason, whatever the DTD would
have said. There is no constructor argument to relax it, matching the Java
codec, which says the same thing about the same decision: the configuration
would only ever be wrong.

A message body has no legitimate use for a DTD. It is one document, produced by
a serializer at the other end, and neither Jackson's ``XmlMapper`` nor Go's
``encoding/xml`` writes one.

**The Python payload is a dict, and XML has no types.** Everything decodes to a
string, a nested dict, or a list where a tag repeats — the same thing Jackson
gives when an XML message is read into a ``Map``. A consumer that wants an
``int`` converts it, where it can decide what a missing or unparseable field
means.
"""

from __future__ import annotations

import dataclasses
import xml.etree.ElementTree as ElementTree
from typing import Any
from xml.parsers import expat

from ..ack import FatalError
from ..codec import register_codec

#: What this codec writes, and what Java, Go and .NET write.
XML_CONTENT_TYPE = "application/xml"

#: Every content type this codec answers for, besides any ``+xml`` suffix —
#: ``application/atom+xml`` and the rest. Java and Go accept exactly these.
XML_ACCEPTS = (XML_CONTENT_TYPE, "text/xml")

#: The root element name used for a payload that does not carry one — a plain
#: dict has no class name to take it from.
DEFAULT_ROOT = "message"

_DTD_REFUSED = (
    "acemq: this message carries a document type declaration. A message body has no "
    "use for one, and a parser that reads DTDs will expand entities on behalf of "
    "whoever sent the message. This codec refuses every DTD and the refusal is not "
    "configurable."
)


def _refuse_dtd(*_: object, **__: object) -> None:
    """Every DTD-shaped callback expat has, wired to the same refusal."""
    raise FatalError(_DTD_REFUSED)


class XmlCodec:
    """Reads and writes XML.

    :param root: the element name to wrap a payload in when it has no name of
        its own. A dataclass uses its class name, the way Jackson uses the
        class's simple name and Go uses the struct's; a plain mapping has
        nothing to take one from, so this is used
    """

    def __init__(self, root: str = DEFAULT_ROOT) -> None:
        if not root:
            raise ValueError("acemq: an XML codec needs a root element name")
        self._root = root

    @property
    def content_type(self) -> str:
        return XML_CONTENT_TYPE

    @property
    def root(self) -> str:
        """The root element name used for payloads that carry no name."""
        return self._root

    def encode(self, payload: Any) -> bytes:
        name = self._root
        if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
            # The class name, which is what Jackson and encoding/xml write, so a
            # dataclass published here reaches Java looking like the record it
            # would have come from.
            name = type(payload).__name__
            payload = dataclasses.asdict(payload)

        if not isinstance(payload, dict):
            raise TypeError(
                f"acemq: cannot encode a {type(payload).__name__} as XML: an XML document "
                "has one root element with named children, so the top level has to be a "
                "mapping or a dataclass. Wrap a list or a scalar in a mapping with a "
                "named key, or use JsonCodec."
            )

        element = ElementTree.Element(name)
        _fill(element, payload)
        written: bytes = ElementTree.tostring(element, encoding="utf-8", xml_declaration=False)
        return written

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        return _to_value(_parse(body))

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts ``application/xml``, ``text/xml`` and any ``+xml`` type.

        Never a message with no content type. XML is rarely what arrives
        unannounced, and a codec that guesses wrong here turns a readable
        message into a rejected one.
        """
        if not content_type:
            return False
        lowered = content_type.lower()
        return lowered.startswith(XML_ACCEPTS) or "+xml" in lowered

    def __repr__(self) -> str:
        return f"XmlCodec({self._root!r})"


def _parse(body: bytes) -> ElementTree.Element:
    """Parses a body with a parser that will not read a DTD.

    Built here rather than reused, because an expat parser is single-use and
    because a shared one would be a shared mutable across every consumer thread.
    """
    builder = ElementTree.TreeBuilder()
    # Namespace processing on, so a namespaced document is read rather than
    # refused for an undeclared prefix, and so xmlns attributes do not arrive as
    # data. Tags come through as "uri}local" and are cut back to the local name
    # below: a consumer wants the field, not the namespace it was declared in,
    # which is also what Jackson gives when XML is read into a Map.
    # Any, because the handler attributes expat exposes are typed for the
    # signatures it calls them with and every one of these is the same refusal.
    parser: Any = expat.ParserCreate(namespace_separator="}")
    parser.StartDoctypeDeclHandler = _refuse_dtd
    parser.EntityDeclHandler = _refuse_dtd
    parser.UnparsedEntityDeclHandler = _refuse_dtd
    parser.ExternalEntityRefHandler = _refuse_dtd
    parser.buffer_text = True
    parser.StartElementHandler = lambda tag, attributes: builder.start(tag, attributes)
    parser.EndElementHandler = builder.end
    parser.CharacterDataHandler = builder.data

    try:
        parser.Parse(body, True)
        return builder.close()
    except FatalError:
        raise
    except (expat.ExpatError, ValueError) as failure:
        # Fatal rather than retryable: bytes that are not XML will not become
        # XML on a redelivery.
        raise FatalError(f"acemq: this message is not XML: {failure}") from failure


def _fill(element: ElementTree.Element, mapping: dict[Any, Any]) -> None:
    """Writes a mapping's items as child elements of ``element``."""
    for key, value in mapping.items():
        _append(element, str(key), value)


def _append(parent: ElementTree.Element, name: str, value: Any) -> None:
    if isinstance(value, list | tuple):
        # A repeated tag, which is how every XML serializer writes a sequence
        # and how this codec reads one back.
        for item in value:
            _append(parent, name, item)
        return

    child = ElementTree.SubElement(parent, name)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = dataclasses.asdict(value)
    if isinstance(value, dict):
        _fill(child, value)
    elif value is None:
        # An empty element, which reads back as the empty string. XML has no
        # null, and inventing xsi:nil here would produce something Java and Go
        # do not write.
        child.text = None
    elif isinstance(value, bool):
        # Lowercase, which is what Jackson and encoding/xml write. Python's
        # str(True) would put "True" on the wire and nothing else would read it.
        child.text = "true" if value else "false"
    else:
        child.text = str(value)


def _local(tag: str) -> str:
    """``uri}local`` back to ``local``."""
    _, separator, local = tag.rpartition("}")
    return local if separator else tag


def _to_value(element: ElementTree.Element) -> Any:
    """One element as a string, or as a dict when it has children or attributes."""
    result: dict[str, Any] = {}
    for key, value in element.attrib.items():
        result[_local(key)] = value

    for child in element:
        name = _local(child.tag)
        value = _to_value(child)
        existing = result.get(name)
        if existing is None and name not in result:
            result[name] = value
        elif isinstance(existing, list):
            existing.append(value)
        else:
            # The second one of a repeated tag turns the first into a list,
            # which is the only point at which a repeat is visible.
            result[name] = [existing, value]

    if result:
        return result
    return element.text or ""


register_codec("xml", XmlCodec)
