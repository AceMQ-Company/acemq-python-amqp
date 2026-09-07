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

"""What the security settings promise, without a broker to promise it to.

A TLS context can be built and read on its own, which is most of what is worth
checking here: that naming an authority really does replace the system trust
store rather than adding to it, that the opt-out really does turn verification
off and that nothing else does, and that a secret never comes back out of a
:class:`Credentials` by being printed. The handshake itself is the integration
suite's job, against a broker with a real TLS listener.
"""

from __future__ import annotations

import ssl
import subprocess
from pathlib import Path

import pytest

from acemq_amqp import (
    Credentials,
    Security,
    SecurityError,
    Verification,
    credentials_from_environment,
    credentials_from_file,
    without_verifying_the_broker,
)


@pytest.fixture(scope="module")
def authority(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A PEM certificate authority, generated rather than committed.

    A certificate checked into a repository expires, and the day it does it
    fails a test suite for a reason that has nothing to do with the change being
    tested. This one is made when the tests run and lasts a day.
    """
    directory = tmp_path_factory.mktemp("certificates")
    certificate = directory / "ca.crt"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(directory / "ca.key"),
            "-out", str(certificate),
            "-days", "1",
            "-subj", "/CN=AceMQ Python Unit Test CA",
        ],
        check=True,
        capture_output=True,
    )
    return certificate


def test_a_secret_never_appears_in_a_repr_or_a_str() -> None:
    credentials = Credentials("app", "hunter2")

    assert "hunter2" not in repr(credentials)
    assert "hunter2" not in str(credentials)
    assert "hunter2" not in f"{credentials}"
    assert "hunter2" not in "{}".format(credentials)  # noqa: UP032 - the point is the path
    # The username is there, because that is the part somebody debugging a
    # rejected login actually needs to see.
    assert "app" in repr(credentials)


def test_a_secret_does_not_leak_through_the_security_it_is_carried_in() -> None:
    settings = Security(
        credentials=Credentials("app", "hunter2"), client_key_password="letmein"
    )

    assert "hunter2" not in repr(settings)
    assert "letmein" not in repr(settings)


def test_credentials_from_the_environment_are_read_each_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = credentials_from_environment("MQ_USER", "MQ_PASSWORD")
    monkeypatch.setenv("MQ_USER", "app")
    monkeypatch.setenv("MQ_PASSWORD", "first")

    assert source() == Credentials("app", "first")

    # Read again rather than remembered, which is the whole reason a source is a
    # callable: a password rotated by a sidecar is only useful to something that
    # asks for it a second time.
    monkeypatch.setenv("MQ_PASSWORD", "second")
    assert source() == Credentials("app", "second")


def test_a_missing_environment_variable_says_which_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MQ_PASSWORD", raising=False)
    monkeypatch.setenv("MQ_USER", "app")

    with pytest.raises(SecurityError, match="MQ_PASSWORD"):
        credentials_from_environment("MQ_USER", "MQ_PASSWORD")()


def test_credentials_from_a_file_lose_the_trailing_newline(tmp_path: Path) -> None:
    secret = tmp_path / "password"
    secret.write_text("hunter2\n")

    assert credentials_from_file(secret, username="app")() == Credentials("app", "hunter2")


def test_a_credentials_file_can_carry_both_parts(tmp_path: Path) -> None:
    secret = tmp_path / "login"
    secret.write_text("app:hunter2\n")

    assert credentials_from_file(secret)() == Credentials("app", "hunter2")


def test_a_credentials_file_with_no_username_and_none_given_is_refused(
    tmp_path: Path,
) -> None:
    secret = tmp_path / "password"
    secret.write_text("hunter2\n")

    with pytest.raises(SecurityError, match="holds no username"):
        credentials_from_file(secret)()


def test_an_empty_credentials_file_is_reported_rather_than_used(tmp_path: Path) -> None:
    secret = tmp_path / "password"
    secret.write_text("\n")

    with pytest.raises(SecurityError, match="is empty"):
        credentials_from_file(secret, username="app")()


def test_a_missing_credentials_file_names_the_path(tmp_path: Path) -> None:
    with pytest.raises(SecurityError, match="cannot read the credentials file"):
        credentials_from_file(tmp_path / "not-there", username="app")()


def test_credentials_replace_whatever_the_url_carried() -> None:
    settings = Security(credentials=Credentials("app", "hunter2"))

    assert (
        settings.applied_to("amqp://guest:guest@localhost:5672/prod")
        == "amqp://app:hunter2@localhost:5672/prod"
    )


def test_a_password_with_punctuation_in_it_does_not_rewrite_the_host() -> None:
    settings = Security(credentials=Credentials("app", "p@ss:w/rd"))

    applied = settings.applied_to("amqp://localhost:5672/")

    # Spliced in raw, that password would make "ss:w" the host. Percent-encoded,
    # the URL still points where it did.
    assert applied == "amqp://app:p%40ss%3Aw%2Frd@localhost:5672/"


def test_a_url_with_no_credentials_configured_is_left_exactly_alone() -> None:
    url = "amqp://guest:guest@localhost:5672/%2F?heartbeat=30"

    assert Security().applied_to(url) == url


def test_tls_settings_against_a_plaintext_url_are_refused_not_ignored(
    authority: Path,
) -> None:
    settings = Security(certificate_authority=authority)

    # The failure this refusal exists to prevent is a service that was given a
    # certificate authority, connected in plaintext, and reported success.
    with pytest.raises(SecurityError, match="not encrypted"):
        settings.applied_to("amqp://localhost:5672/")


def test_credentials_alone_are_welcome_on_a_plaintext_url() -> None:
    settings = Security(credentials=Credentials("app", "hunter2"))

    assert settings.applied_to("amqp://localhost:5672/").startswith("amqp://app:")


def test_a_plaintext_url_is_given_no_tls_context_at_all() -> None:
    assert Security().transport_options("amqp://localhost:5672/") == {}


def test_an_amqps_url_is_verified_and_modern_without_being_asked() -> None:
    options = Security().transport_options("amqps://localhost:5671/")
    context = options["ssl_context"]

    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname is True
    # TLS 1.0 and 1.1 are withdrawn, and a broker still offering them is not a
    # reason to speak them.
    assert context.minimum_version is ssl.TLSVersion.TLSv1_2


def test_naming_an_authority_replaces_the_system_trust_store(authority: Path) -> None:
    named = Security(certificate_authority=authority).ssl_context()
    default = Security().ssl_context()

    # One certificate, not one more than the machine already trusted. This is
    # the line between "encrypted" and "encrypted to the right party": a broker
    # with a certificate from a public authority is not your broker.
    assert len(named.get_ca_certs()) == 1
    assert named.get_ca_certs()[0]["subject"] == (
        (("commonName", "AceMQ Python Unit Test CA"),),
    )
    assert len(default.get_ca_certs()) > 1


def test_an_authority_that_is_not_there_is_reported_with_its_path() -> None:
    settings = Security(certificate_authority="/nowhere/ca.crt")

    with pytest.raises(SecurityError, match=r"certificate authority '/nowhere/ca\.crt'"):
        settings.ssl_context()


def test_a_file_that_is_not_a_certificate_is_reported(tmp_path: Path) -> None:
    pretend = tmp_path / "ca.crt"
    pretend.write_text("this is not a certificate\n")

    with pytest.raises(SecurityError, match="cannot load the certificate authority"):
        Security(certificate_authority=pretend).ssl_context()


def test_a_client_key_with_no_certificate_is_refused(tmp_path: Path) -> None:
    with pytest.raises(SecurityError, match="key on its own proves nothing"):
        Security(client_key=tmp_path / "client.key").ssl_context()


def test_a_client_certificate_that_cannot_be_read_names_both_files(
    tmp_path: Path,
) -> None:
    with pytest.raises(SecurityError, match="client certificate"):
        Security(
            client_certificate=tmp_path / "client.crt", client_key=tmp_path / "client.key"
        ).ssl_context()


def test_the_opt_out_turns_verification_off_and_says_so_in_its_name() -> None:
    context = without_verifying_the_broker().ssl_context()

    assert context.verify_mode is ssl.CERT_NONE
    assert context.check_hostname is False
    # And it is still encryption: the traffic cannot be read, it just cannot be
    # attributed to anybody.
    assert context.minimum_version is ssl.TLSVersion.TLSv1_2


def test_nothing_short_of_asking_for_it_turns_verification_off() -> None:
    # Every other way of building a Security still verifies, which is the
    # property that makes the opt-out hard to reach by accident: there is no
    # boolean to flip and no partially-configured state that lands here.
    assert Security().verification is Verification.CERTIFICATE
    assert Security(client_certificate="client.crt").verification is Verification.CERTIFICATE
    assert without_verifying_the_broker().verification is Verification.NOTHING_AT_ALL


def test_the_opt_out_still_carries_a_login_and_a_client_certificate() -> None:
    settings = without_verifying_the_broker(credentials=Credentials("app", "hunter2"))

    assert settings.resolve_credentials() == Credentials("app", "hunter2")
    assert settings.applied_to("amqps://localhost:5671/") == "amqps://app:hunter2@localhost:5671/"


def test_a_server_name_overrides_the_one_the_handshake_would_have_used() -> None:
    context = Security(server_name="broker.internal").ssl_context()

    # asyncio takes the name from the host in the URL, which is right until the
    # broker is reached by IP address or through a tunnel. The name checked is
    # the one configured, whatever the socket was dialled with.
    handshake = context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname="10.0.0.7")
    assert handshake.server_hostname == "broker.internal"


def test_the_name_checked_is_the_url_host_when_nothing_says_otherwise() -> None:
    context = Security().ssl_context()

    handshake = context.wrap_bio(ssl.MemoryBIO(), ssl.MemoryBIO(), server_hostname="10.0.0.7")
    assert handshake.server_hostname == "10.0.0.7"


def test_a_credentials_source_is_asked_at_the_moment_it_is_needed() -> None:
    asked: list[int] = []

    def source() -> Credentials:
        asked.append(1)
        return Credentials("app", "hunter2")

    settings = Security(credentials=source)
    assert asked == []

    settings.applied_to("amqp://localhost:5672/")
    settings.applied_to("amqp://localhost:5672/")
    assert len(asked) == 2


def test_a_source_that_returns_the_wrong_thing_is_caught_here() -> None:
    settings = Security(credentials=lambda: "app:hunter2")  # type: ignore[arg-type,return-value]

    with pytest.raises(SecurityError, match="must return Credentials"):
        settings.resolve_credentials()


def test_empty_credentials_leave_the_url_as_it_was() -> None:
    # A source that legitimately has nothing to say — an anonymous broker, a
    # connection authenticated by client certificate alone — should not blank
    # out a login the URL was carrying.
    settings = Security(credentials=Credentials())

    assert settings.applied_to("amqp://guest:guest@localhost:5672/") == (
        "amqp://guest:guest@localhost:5672/"
    )
