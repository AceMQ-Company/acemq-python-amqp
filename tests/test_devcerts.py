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

"""The development certificate generator, and the refusal that makes it safe.

Two halves of one mechanism, so they are tested together. The generator stamps
every certificate with a marker; :mod:`acemq_amqp.security` refuses anything
carrying it. Either half on its own is useless — a marker nothing enforces is a
comment, and an enforcement with nothing to enforce against is untested code —
so the tests below check that what one writes is what the other refuses.

The handshakes here are real ones, against a TLS server started in the test with
the generated certificates. A test that only read the files back would pass on a
certificate that no ``openssl`` would ever complete a handshake with.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import ssl
import sys
from pathlib import Path

import pytest
from cryptography import x509

from acemq_amqp.devcerts import (
    AUTHORITY_NAME,
    CLIENT_NAME,
    GeneratedCertificates,
    broker_configuration,
    generate,
    main,
)
from acemq_amqp.errors import AceMQError, SecurityError
from acemq_amqp.security import (
    DEVELOPMENT_MARKER,
    Security,
    Verification,
    is_development_certificate,
    without_verifying_the_broker,
)


@pytest.fixture(scope="module")
def certificates(tmp_path_factory: pytest.TempPathFactory) -> GeneratedCertificates:
    """One set for the whole module. Generating three key pairs is not free."""
    return generate(tmp_path_factory.mktemp("certs"))


def _read(path: Path) -> x509.Certificate:
    return x509.load_pem_x509_certificate(path.read_bytes())


def _organisation(certificate: x509.Certificate) -> str:
    attribute = certificate.subject.get_attributes_for_oid(x509.oid.NameOID.ORGANIZATION_NAME)
    return str(attribute[0].value)


def _common_name(certificate: x509.Certificate) -> str:
    attribute = certificate.subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)
    return str(attribute[0].value)


# --------------------------------------------------------------------------
# What gets written


def test_the_files_are_the_ones_the_go_generator_writes(
    certificates: GeneratedCertificates,
) -> None:
    """Same names, so this is a drop-in replacement for ``acemq-certs``.

    ``scripts/tls-broker.sh`` looks for exactly these, and a generator that
    wrote ``ca.pem`` instead would be correct and useless.
    """
    written = {path.name for path in certificates.files}

    assert written == {
        "ca.crt",
        "ca.key",
        "server.crt",
        "server.key",
        "client.crt",
        "client.key",
        "rabbitmq.conf",
    }
    for path in certificates.files:
        assert path.exists(), path


def test_every_certificate_carries_the_marker_in_its_organisation(
    certificates: GeneratedCertificates,
) -> None:
    """The whole safety mechanism, and it has to be on all three.

    The authority especially: a marked leaf issued by an unmarked authority
    would let somebody keep the CA, trust it in production and issue whatever
    they liked under it.
    """
    for path in (certificates.authority, certificates.server, certificates.client):
        assert _organisation(_read(path)) == DEVELOPMENT_MARKER


def test_the_names_are_the_ones_the_other_libraries_write(
    certificates: GeneratedCertificates,
) -> None:
    assert AUTHORITY_NAME == "AceMQ development CA"
    assert _common_name(_read(certificates.authority)) == AUTHORITY_NAME
    assert _common_name(_read(certificates.server)) == "localhost"
    assert _common_name(_read(certificates.client)) == CLIENT_NAME == "acemq-client"


def test_the_authority_is_one_and_can_sign_nothing_below_it(
    certificates: GeneratedCertificates,
) -> None:
    """``pathlen:0``, so the generated CA cannot mint another CA.

    It changes nothing about what a laptop does and it means a leaked
    development authority cannot be used to build a chain that hides where it
    came from.
    """
    constraints = _read(certificates.authority).extensions.get_extension_for_class(
        x509.BasicConstraints
    )

    assert constraints.value.ca is True
    assert constraints.value.path_length == 0
    assert constraints.critical


def test_the_leaves_are_not_authorities(certificates: GeneratedCertificates) -> None:
    for path in (certificates.server, certificates.client):
        constraints = _read(path).extensions.get_extension_for_class(x509.BasicConstraints)
        assert constraints.value.ca is False


def test_each_leaf_has_the_one_purpose_it_is_for(
    certificates: GeneratedCertificates,
) -> None:
    """Serving and authenticating, kept apart, as Go and .NET write them."""
    purposes = x509.ExtendedKeyUsage
    server = _read(certificates.server).extensions.get_extension_for_class(purposes)
    client = _read(certificates.client).extensions.get_extension_for_class(purposes)

    assert list(server.value) == [x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]
    assert list(client.value) == [x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]


def test_the_broker_certificate_names_localhost_and_both_loopback_addresses(
    certificates: GeneratedCertificates,
) -> None:
    """Half the tooling dials the name and the other half dials the address."""
    names = _read(certificates.server).extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    )

    assert set(names.value.get_values_for_type(x509.DNSName)) == {"localhost", "rabbitmq"}
    addresses = {str(address) for address in names.value.get_values_for_type(x509.IPAddress)}
    assert addresses == {"127.0.0.1", "::1"}


def test_an_address_is_written_as_an_address_rather_than_as_a_name(tmp_path: Path) -> None:
    """A certificate listing ``10.0.0.4`` under DNS is valid for nothing.

    It names a *host called* ``10.0.0.4``, and every client that dials the
    address rejects it — for a reason indistinguishable from the certificate
    being wrong, which it now is.
    """
    written = generate(tmp_path / "by-address", broker_host="10.0.0.4")

    names = _read(written.server).extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    )

    assert names.value.get_values_for_type(x509.DNSName) == []
    assert [str(a) for a in names.value.get_values_for_type(x509.IPAddress)] == ["10.0.0.4"]


def test_the_leaves_are_signed_by_the_authority(certificates: GeneratedCertificates) -> None:
    authority = _read(certificates.authority)

    for path in (certificates.server, certificates.client):
        assert _read(path).issuer == authority.subject


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")
def test_the_private_keys_are_readable_only_by_their_owner(
    certificates: GeneratedCertificates,
) -> None:
    for path in (certificates.authority_key, certificates.server_key, certificates.client_key):
        assert path.stat().st_mode & 0o777 == 0o600, path
    for path in (certificates.authority, certificates.server, certificates.client):
        assert path.stat().st_mode & 0o777 == 0o644, path


def test_the_dates_leave_room_for_a_container_clock(
    certificates: GeneratedCertificates,
) -> None:
    """An hour of slack at the front.

    A broker in a container and the host that generated its certificate do not
    share a clock, and a certificate that is not valid *yet* fails in a way that
    reads exactly like one that is wrong.
    """
    server = _read(certificates.server)
    now = dt.datetime.now(dt.timezone.utc)

    assert server.not_valid_before_utc < now - dt.timedelta(minutes=30)
    assert server.not_valid_after_utc > now + dt.timedelta(days=29)


def test_a_certificate_has_to_last_at_least_a_day(tmp_path: Path) -> None:
    with pytest.raises(AceMQError, match="at least a day"):
        generate(tmp_path / "none", validity_days=0)


def test_the_broker_configuration_points_at_the_files_inside_the_container() -> None:
    """The path the broker reads is not the path they were written to."""
    written = broker_configuration("/certs")

    assert "ssl_options.cacertfile = /certs/ca.crt" in written
    assert "ssl_options.certfile   = /certs/server.crt" in written
    assert "ssl_options.keyfile    = /certs/server.key" in written
    # Both listeners, so the same broker can be reached either way while
    # somebody works out which of the two is broken.
    assert "listeners.ssl.default = 5671" in written
    assert "listeners.tcp.default = 5672" in written
    assert "ssl_options.fail_if_no_peer_cert = false" in written


# --------------------------------------------------------------------------
# The refusal these exist to trip


def test_what_the_generator_writes_is_what_the_library_refuses(
    certificates: GeneratedCertificates,
) -> None:
    """The two halves meeting. If this ever fails, one of them moved."""
    for path in (certificates.authority, certificates.server, certificates.client):
        assert is_development_certificate(path.read_bytes()), path


def test_a_pem_file_is_not_searched_as_it_stands(
    certificates: GeneratedCertificates,
) -> None:
    """The mistake this test exists to prevent.

    A PEM file is Base64, so the marker is nowhere in its text — searching the
    file as it arrives finds nothing and the refusal silently never fires. The
    check has to decode first, and this pins that it does.
    """
    text = certificates.authority.read_text()

    assert DEVELOPMENT_MARKER not in text
    assert is_development_certificate(text.encode())


def test_naming_a_generated_authority_is_refused_before_a_connection_is_tried(
    certificates: GeneratedCertificates,
) -> None:
    """The failure worth catching earliest: trusting the development CA.

    Its private key is in the directory next to it, so trusting it in production
    means trusting everybody who can read that directory. Caught while the
    context is built, where the error can name the file somebody has to change.
    """
    settings = Security(certificate_authority=certificates.authority)

    with pytest.raises(SecurityError, match="DEVELOPMENT ONLY"):
        settings.ssl_context()


def test_presenting_a_generated_client_certificate_is_refused_too(
    certificates: GeneratedCertificates,
) -> None:
    settings = Security(
        client_certificate=certificates.client, client_key=certificates.client_key
    )

    with pytest.raises(SecurityError, match="DEVELOPMENT ONLY"):
        settings.ssl_context()


def test_saying_so_deliberately_is_what_gets_through(
    certificates: GeneratedCertificates,
) -> None:
    context = Security(
        certificate_authority=certificates.authority, allow_development_certificates=True
    ).ssl_context()

    assert context.minimum_version is ssl.TLSVersion.TLSv1_2


def test_allowing_them_is_a_tls_setting_and_a_plaintext_url_refuses_it() -> None:
    """A setting that only affects TLS cannot be honoured by ``amqp://``.

    The same rule the rest of this module follows, and for the same reason: an
    instruction that quietly does nothing is worse than one that is refused.
    """
    settings = Security(allow_development_certificates=True)

    assert settings.describes_tls
    with pytest.raises(SecurityError, match="not encrypted"):
        settings.applied_to("amqp://localhost:5672/")


# --------------------------------------------------------------------------
# Against a real handshake


async def _serve(certificates: GeneratedCertificates) -> tuple[asyncio.Server, int]:
    """A TLS listener using the generated broker certificate."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(certificates.server), str(certificates.server_key))

    async def ignore(_: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    server = await asyncio.start_server(ignore, "127.0.0.1", 0, ssl=context)
    return server, server.sockets[0].getsockname()[1]


async def _connect(port: int, settings: Security) -> None:
    _, writer = await asyncio.open_connection(
        "localhost", port, ssl=settings.ssl_context(), server_hostname="localhost"
    )
    writer.close()


async def test_the_generated_certificates_complete_a_real_handshake(
    certificates: GeneratedCertificates,
) -> None:
    """The assertion no amount of reading the files back can make.

    The chain verifies, the name on the certificate matches the name dialled,
    and the key goes with the certificate. A generator can get every field in
    this file right and still produce something no ``openssl`` will shake hands
    with.
    """
    server, port = await _serve(certificates)
    try:
        await _connect(
            port,
            Security(
                certificate_authority=certificates.authority,
                allow_development_certificates=True,
            ),
        )
    finally:
        server.close()
        await server.wait_closed()


async def test_the_handshake_is_refused_when_the_marker_is_not_allowed(
    certificates: GeneratedCertificates,
) -> None:
    """The same handshake, without the opt-out. It gets no further than the
    certificate."""
    server, port = await _serve(certificates)
    try:
        with pytest.raises((SecurityError, ssl.SSLCertVerificationError)):
            await _connect(port, Security(certificate_authority=certificates.authority))
    finally:
        server.close()
        await server.wait_closed()


async def test_it_is_refused_with_verification_turned_off_as_well(
    certificates: GeneratedCertificates,
) -> None:
    """The configuration a development certificate is most likely to be reached
    for, and therefore the one the refusal most has to survive.

    There is no verified chain here at all — nothing was checked — so the marker
    is read off the certificate the broker presented, in whatever encoding it
    arrived in. A check written against a verified chain would find nothing to
    look at and let this through.
    """
    server, port = await _serve(certificates)
    try:
        with pytest.raises(ssl.SSLCertVerificationError, match="DEVELOPMENT ONLY"):
            await _connect(port, without_verifying_the_broker())
    finally:
        server.close()
        await server.wait_closed()


async def test_allowing_it_gets_through_with_verification_turned_off(
    certificates: GeneratedCertificates,
) -> None:
    server, port = await _serve(certificates)
    try:
        await _connect(
            port,
            Security(
                verification=Verification.NOTHING_AT_ALL,
                allow_development_certificates=True,
            ),
        )
    finally:
        server.close()
        await server.wait_closed()


# --------------------------------------------------------------------------
# The command line


def test_the_command_line_writes_the_same_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = main(
        ["--directory", str(tmp_path / "cli"), "--broker", "broker.test", "--days", "7"]
    )

    assert status == 0
    written = capsys.readouterr()
    for name in ("ca.crt", "server.crt", "client.crt", "rabbitmq.conf"):
        assert name in written.out
    # The warning goes to standard error, so a script capturing the file list
    # gets file paths and nothing else.
    assert DEVELOPMENT_MARKER in written.err
    assert _common_name(_read(tmp_path / "cli" / "server.crt")) == "broker.test"


def test_the_command_line_can_skip_the_broker_configuration(tmp_path: Path) -> None:
    main(["--directory", str(tmp_path / "bare"), "--no-broker-config"])

    assert not (tmp_path / "bare" / "rabbitmq.conf").exists()
    assert (tmp_path / "bare" / "ca.crt").exists()


def test_the_command_line_reports_a_refusal_rather_than_a_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = main(["--directory", str(tmp_path / "nope"), "--days", "0"])

    assert status == 1
    assert "at least a day" in capsys.readouterr().err
