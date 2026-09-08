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

"""The formats the other AceMQ libraries ship, each behind its own extra.

:mod:`acemq_amqp.codec` carries JSON, text and bytes, and nothing there needs
anything installed. The Java and Go libraries ship five more as separate
optional modules — YAML, TOML, XML, Protobuf and Avro — and a Java or Go service
publishing any of those produced a message this library could not read. That is
the gap these close. It is an interoperability gap rather than a capability one:
the formats were always available in Python, but the *content types* were not
claimed by anything here, so a perfectly readable message was refused.

Each one is a module of its own and none of them is imported by
``acemq_amqp``, so the core keeps its promise of depending on nothing::

    pip install "acemq-amqp[yaml]"

    from acemq_amqp.codecs.yaml import YamlCodec

    mq = await connect(url, codec=CompositeCodec(JsonCodec(), YamlCodec()))

XML is the exception and needs no extra: it is written against
:mod:`xml.etree.ElementTree` and :mod:`xml.parsers.expat` from the standard
library. See :mod:`acemq_amqp.codecs.xml` for what was done about external
entities, which a message body from a queue very much invites.

Importing a module registers its codec by name where a codec can be built with
no arguments — ``yaml``, ``toml`` and ``xml``. Protobuf and Avro cannot be:
neither format's bytes describe themselves, so a codec needs a message type or a
schema before it can read anything, and there is nothing sensible for a no-
argument factory to hand back.

The write types are the contract, and they are the ones Java and Go write:

============  ==============================
``yaml``      ``application/yaml``
``toml``      ``application/toml``
``xml``       ``application/xml``
``protobuf``  ``application/x-protobuf``
``avro``      ``avro/binary``, or
              ``application/vnd.acemq.avro`` when a schema identifier is framed
              into the message
============  ==============================

What each *reads* is deliberately wider, because a producer in another stack
writes whichever spelling its own library picked. See each module.
"""

from __future__ import annotations

__all__: list[str] = []
