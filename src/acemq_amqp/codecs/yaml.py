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

"""Reads and writes YAML::

    pip install "acemq-amqp[yaml]"

    from acemq_amqp.codecs.yaml import YamlCodec

Chosen when a message is meant to be read by a person as much as by a program: a
configuration change broadcast to a fleet, a deployment instruction, a command
replayed by hand from a dead-letter queue. It is worth saying plainly that YAML
costs more to parse than JSON and is a poor choice for high volume; it earns its
place where somebody will actually look at the message.

Written in block style rather than flow style, which is the whole reason to pick
YAML. Flow style would produce something very close to JSON and would leave
nothing to justify the cost. That is the same choice the Java codec makes with
Jackson's ``YAMLGenerator`` and the Go one with ``yaml.v3``.

**This codec never volunteers for a message whose sender set no content type.**
YAML is a superset of JSON, so its parser accepts JSON bytes quite happily and
would answer for messages meant for :class:`~acemq_amqp.codec.JsonCodec`. It
would even give the right value — while recording that a YAML message had
arrived, which is the sort of wrong that is discovered much later. So it claims
only content types that say YAML.

**Loading is always safe loading.** A message body is untrusted input, and
PyYAML's default loader constructs arbitrary Python objects out of tags like
``!!python/object/apply``. There is no option to turn that back on, for the same
reason the XML codec's entity handling is not configurable: the configuration
would only ever be wrong.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from ..ack import FatalError
from ..codec import register_codec
from ..errors import AceMQError

#: What this codec writes, and what Java, Go and .NET write.
YAML_CONTENT_TYPE = "application/yaml"

#: Every content type this codec answers for. ``application/yaml`` is the
#: registered one as of RFC 9512; the other three predate it and are what most
#: senders still write. Java accepts exactly these four and any ``+yaml``
#: suffix, and so does Go.
YAML_ACCEPTS = (
    YAML_CONTENT_TYPE,
    "application/x-yaml",
    "text/yaml",
    "text/x-yaml",
)


class YamlCodec:
    """Reads and writes YAML.

    :param sort_keys: whether to sort mapping keys when writing. Off by default,
        so a payload goes out in the order it was built — which is what Jackson
        and ``yaml.v3`` do, and what makes a message a person reads legible
    :param allow_unicode: whether to write non-ASCII characters as themselves
        rather than escaping them. On by default, for the same reason
    :raises AceMQError: when PyYAML is not installed
    """

    def __init__(self, *, sort_keys: bool = False, allow_unicode: bool = True) -> None:
        try:
            import yaml
        except ImportError as missing:  # pragma: no cover - depends on the install
            raise AceMQError(
                "acemq: YamlCodec needs PyYAML, which is an optional extra. "
                'Install it with pip install "acemq-amqp[yaml]"'
            ) from missing

        self._yaml: Any = yaml
        self._sort_keys = sort_keys
        self._allow_unicode = allow_unicode

    @property
    def content_type(self) -> str:
        return YAML_CONTENT_TYPE

    def encode(self, payload: Any) -> bytes:
        if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
            payload = dataclasses.asdict(payload)
        try:
            written: str = self._yaml.safe_dump(
                payload,
                # Block style. Flow style is valid YAML and looks like JSON, so
                # a message written that way gives up the only thing YAML was
                # chosen for.
                default_flow_style=False,
                sort_keys=self._sort_keys,
                allow_unicode=self._allow_unicode,
                # No leading "---". It is valid and it is noise, and a message
                # body is a single document by definition, so the marker
                # separates nothing from nothing. Java disables it too.
                explicit_start=False,
            )
            return written.encode("utf-8")
        except self._yaml.YAMLError as failure:
            raise TypeError(
                f"acemq: a {type(payload).__name__} is not something YAML can carry: {failure}"
            ) from failure

    def decode(self, body: bytes, content_type: str | None = None) -> Any:
        try:
            # safe_load, never load. The full loader builds arbitrary Python
            # objects from tags in the document, and the document arrived from
            # a queue.
            return self._yaml.safe_load(body)
        except (self._yaml.YAMLError, UnicodeDecodeError) as failure:
            # Fatal rather than retryable: a body that is not YAML is not YAML
            # the next time either, and retrying it only holds a queue open
            # until the message ages out.
            raise FatalError(f"acemq: this message is not YAML: {failure}") from failure

    def can_decode(self, content_type: str | None) -> bool:
        """Accepts the four spellings of YAML and any ``+yaml`` type.

        Never a message with no content type, for the reason in the module
        docstring: YAML reads JSON, so it would claim messages that were not
        sent as YAML and be right about the value while wrong about the format.
        """
        if not content_type:
            return False
        lowered = content_type.lower()
        return lowered.startswith(YAML_ACCEPTS) or "+yaml" in lowered

    def __repr__(self) -> str:
        return "YamlCodec()"


register_codec("yaml", YamlCodec)
