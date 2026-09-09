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

"""The envelope, against the fixtures every other language is held to."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from acemq_amqp import Envelope, headers

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "envelope-fixtures.json").read_text()
)


def test_the_fixtures_are_the_ones_the_other_languages_use() -> None:
    # Produced by the Java implementation and shared with Go and .NET. If this
    # file is ever rebuilt against a different contract, every language should
    # find out from its own test suite rather than from production.
    assert FIXTURES["generatedBy"] == "acemq-java-amqp FixtureGen"
    assert FIXTURES["cases"], "no cases to check"


@pytest.mark.parametrize("case", FIXTURES["cases"], ids=lambda c: c["case"])
def test_reads_what_java_wrote(case: dict[str, Any]) -> None:
    """Every header Java writes is understood, with the right type and meaning."""
    envelope = Envelope.from_headers(case["headers"], case["routingKey"])
    expected = case["headers"]

    assert envelope.id == expected[headers.ID]
    assert envelope.type == expected[headers.TYPE]
    assert envelope.version == expected[headers.VERSION]
    assert envelope.correlation_id == expected[headers.CORRELATION]
    assert envelope.attempt == expected[headers.ATTEMPT]
    assert envelope.origin == expected.get(headers.ORIGIN, "")

    # Epoch milliseconds, not seconds and not an ISO string. Getting this wrong
    # by a factor of a thousand puts every message in 1970 or in the far future,
    # and age-based give-up then either never fires or always does.
    assert int(envelope.first_seen.timestamp() * 1000) == expected[headers.FIRST_SEEN]


@pytest.mark.parametrize("case", FIXTURES["cases"], ids=lambda c: c["case"])
def test_writes_what_java_would_write(case: dict[str, Any]) -> None:
    """The round trip: what is read back out is what came in."""
    envelope = Envelope.from_headers(case["headers"], case["routingKey"])
    written = envelope.to_headers(case["routingKey"])

    for name, value in case["headers"].items():
        assert name in written, f"{name} was not written back"
        assert written[name] == value, f"{name} changed in the round trip"


def test_application_headers_are_kept_apart_from_ours() -> None:
    envelope = Envelope.from_headers(
        {
            headers.ID: "abc",
            headers.ATTEMPT: 3,
            "tenant": "acme",
            "x-acemq-something-newer": "from a later version",
        },
        "orders.placed",
    )

    # An application asking for its headers gets its own, and nothing that
    # would be written twice if it handed them back on a new message.
    assert envelope.headers == {"tenant": "acme"}
    assert envelope.attempt == 3


def test_a_reserved_name_in_application_headers_is_refused() -> None:
    with pytest.raises(ValueError, match="x-acemq-id"):
        Envelope(headers={"x-acemq-id": "mine now"})


def test_correlation_defaults_to_the_id_rather_than_being_absent() -> None:
    # The whole point of a correlation id is that a chain of messages shares
    # one. A message that starts a chain correlates to itself, so that every
    # hop after it has something to copy.
    envelope = Envelope(id="the-first-one")
    assert envelope.correlation_id == "the-first-one"
    assert envelope.to_headers()[headers.CORRELATION] == "the-first-one"


def test_type_falls_back_to_the_routing_key() -> None:
    assert Envelope().to_headers("order.placed")[headers.TYPE] == "order.placed"
    assert Envelope(type="order.placed.v2").to_headers("order.placed")[headers.TYPE] == (
        "order.placed.v2"
    )


def test_empty_values_are_absent_rather_than_empty() -> None:
    written = Envelope(id="i").to_headers("k")

    # A header carrying "" is a header somebody has to write a special case for.
    for name in (headers.CAUSATION, headers.ORIGIN, headers.ERROR, headers.CLAIM):
        assert name not in written


def test_a_header_of_the_wrong_type_does_not_lose_the_message() -> None:
    # A producer in another language sending the attempt as a string is wrong,
    # and refusing to deliver its messages would turn its bug into our outage.
    envelope = Envelope.from_headers({headers.ID: "x", headers.ATTEMPT: "4"}, "k")
    assert envelope.attempt == 4

    unreadable = Envelope.from_headers({headers.ID: "x", headers.ATTEMPT: "soon"}, "k")
    assert unreadable.attempt == 1


def test_bytes_headers_are_decoded() -> None:
    # Some clients put strings on the wire as bytes; str() on those gives
    # "b'orders'", which then travels onward as the type of the message.
    envelope = Envelope.from_headers({headers.ID: b"an-id", headers.TYPE: b"orders"}, "k")
    assert envelope.id == "an-id"
    assert envelope.type == "orders"


def test_age_is_measured_from_when_it_was_first_published() -> None:
    an_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    envelope = Envelope(first_seen=an_hour_ago)

    # Attempts say nothing about how long a message has been waiting: a paused
    # queue produces messages on attempt one that are days old.
    assert timedelta(minutes=59) < envelope.age < timedelta(minutes=61)


def test_an_envelope_cannot_be_changed_underneath_a_log_line() -> None:
    envelope = Envelope(id="one")
    later = envelope.with_(attempt=2)

    assert envelope.attempt == 1
    assert later.attempt == 2
    assert later.id == "one"


def test_a_route_is_a_field_because_its_headers_are_ours() -> None:
    # ``x-acemq-route`` is a reserved name, so it never reaches the application
    # map — which is why a pattern that needs it reads it from here. Java's
    # Envelope carries the same three, for the same reason.
    envelope = Envelope.from_headers(
        {
            headers.ID: "m-1",
            headers.ROUTE: "validate,enrich,dispatch",
            headers.ROUTE_POSITION: 1,
            headers.ROUTE_ID: "run-7",
        },
        "enrich",
    )

    assert envelope.route == "validate,enrich,dispatch"
    assert envelope.route_position == 1
    assert envelope.route_id == "run-7"
    assert envelope.headers == {}

    written = envelope.to_headers("enrich")
    assert written[headers.ROUTE] == "validate,enrich,dispatch"
    assert written[headers.ROUTE_POSITION] == 1
    assert written[headers.ROUTE_ID] == "run-7"


def test_a_message_on_no_route_writes_no_route_headers() -> None:
    # Absent rather than empty, like every other optional header here: one
    # carrying "" is one somebody has to write a special case for.
    written = Envelope(id="m-1").to_headers("orders.placed")

    assert headers.ROUTE not in written
    assert headers.ROUTE_POSITION not in written
    assert headers.ROUTE_ID not in written


def test_the_first_hop_of_a_route_still_says_where_it_is() -> None:
    written = Envelope(id="m-1", route="validate,enrich").to_headers("validate")

    assert written[headers.ROUTE_POSITION] == 0


def test_a_position_before_the_start_is_the_start() -> None:
    # A negative position would send the message to an arbitrary step.
    assert Envelope(route="a,b", route_position=-4).route_position == 0


def test_a_route_survives_the_copy_a_retry_makes() -> None:
    # Which is what makes replaying a dead-lettered message resume its route
    # rather than start it again: the consumer republishes ``with_``.
    original = Envelope(id="m-1", route="validate,enrich", route_position=1, route_id="run-7")

    carried = original.with_(attempt=2)

    assert (carried.route, carried.route_position, carried.route_id) == (
        "validate,enrich",
        1,
        "run-7",
    )
