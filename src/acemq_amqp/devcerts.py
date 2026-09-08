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

"""Certificates for a broker on a laptop, and only for a broker on a laptop::

    pip install "acemq-amqp[crypto]"

    python -m acemq_amqp.devcerts --directory certs --broker localhost

An authority, a broker certificate and a client certificate, written as PEM
alongside a ``rabbitmq.conf`` that points the broker at them. The alternative is
six ``openssl`` invocations with a hand-written extensions file, which is how
people end up developing against a plaintext broker instead — and then finding
out on the day TLS is turned on that nothing was ever tested through it.

Everything written here carries
:data:`~acemq_amqp.security.DEVELOPMENT_MARKER` — ``ACEMQ DEVELOPMENT ONLY - DO
NOT TRUST`` — in its subject organisation, and **the TLS code in
:mod:`acemq_amqp.security` refuses any certificate carrying it**, whatever the
trust settings say and including :func:`~acemq_amqp.security.without_verifying_the_broker`.
That is not a warning in a docstring somebody has to read. It is the mechanism:
a generated authority's private key sits in the same directory as its
certificate and usually ends up in a repository, so a certificate that could
reach production would be an authority anybody who can read the repository can
issue against.

Java, Go and .NET generate the same three certificates with the same marker and
enforce it the same way, so a broker set up by any of the four is usable from all
of them and none of them will speak to it without being told the certificates are
development ones. The file names match Go's ``acemq-certs`` exactly — ``ca.crt``,
``ca.key``, ``server.crt``, ``server.key``, ``client.crt``, ``client.key`` — so
this is a drop-in replacement for it in a script such as ``tls-broker.sh``.

The keys are ECDSA on P-256, which is what Go writes. Java uses RSA-4096 and
.NET RSA-2048; nothing reading these certificates cares, and P-256 keeps
generation instant, which matters for something that runs at the top of a test
script.

Needs the ``crypto`` extra, for the same reason
:mod:`acemq_amqp.codecs.encrypted` does: the standard library has hashes and
HMAC and does not have an X.509 builder.
"""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import os
import secrets
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import AceMQError
from .security import DEVELOPMENT_MARKER

#: What the authority calls itself. The same string in Java, Go and .NET.
AUTHORITY_NAME = "AceMQ development CA"

#: What the client certificate calls itself. Java and Go both write this; .NET
#: writes ``acemq-dev-client``, and nothing reads either.
CLIENT_NAME = "acemq-client"

#: How long a generated certificate lasts, in days.
#:
#: Thirty, matching Go and Java. Long enough not to interrupt a piece of work and
#: short enough that a set of these cannot quietly become load-bearing: a
#: development certificate that outlives the reason it was made is a development
#: certificate somebody eventually deploys.
DEFAULT_VALIDITY_DAYS = 30

#: Where the broker will find the certificates *inside its container*, which is
#: rarely where they were written on the host.
DEFAULT_BROKER_CERTIFICATE_PATH = "/certs"


@dataclass(frozen=True, slots=True)
class GeneratedCertificates:
    """What :func:`generate` wrote.

    :param directory: where the files are
    :param authority: the ``ca.crt`` path
    :param authority_key: the ``ca.key`` path
    :param server: the ``server.crt`` path
    :param server_key: the ``server.key`` path
    :param client: the ``client.crt`` path
    :param client_key: the ``client.key`` path
    :param broker_configuration: the ``rabbitmq.conf`` path, or ``None`` when one
        was not asked for
    :param expires: when all three certificates stop being valid, in UTC
    :param broker_host: the name the broker certificate is valid for
    :param marker: the string stamped into every subject, so a caller can print
        it without importing it from somewhere else
    """

    directory: Path
    authority: Path
    authority_key: Path
    server: Path
    server_key: Path
    client: Path
    client_key: Path
    broker_configuration: Path | None
    expires: dt.datetime
    broker_host: str
    marker: str = DEVELOPMENT_MARKER
    files: tuple[Path, ...] = field(default_factory=tuple)


def generate(
    directory: str | os.PathLike[str] = "certs",
    *,
    broker_host: str = "localhost",
    validity_days: int = DEFAULT_VALIDITY_DAYS,
    broker_certificate_path: str = DEFAULT_BROKER_CERTIFICATE_PATH,
    write_broker_configuration: bool = True,
) -> GeneratedCertificates:
    """Writes an authority, a broker certificate and a client certificate.

    :param directory: where to write them. Created if it is not there
    :param broker_host: the name the broker certificate is valid for. An IP
        address is recognised and written as one, because a certificate that
        names ``10.0.0.4`` in its DNS names is valid for a host called
        ``10.0.0.4`` and for no address at all
    :param validity_days: how long they last
    :param broker_certificate_path: where the broker will find them, which for a
        broker in a container is a path on the container's filesystem rather than
        this one. Only used in the generated ``rabbitmq.conf``
    :param write_broker_configuration: whether to write that ``rabbitmq.conf``
    :returns: what was written
    :raises AceMQError: when ``cryptography`` is not installed, when
        ``validity_days`` is not at least a day, or when a file cannot be written
    """
    if validity_days < 1:
        raise AceMQError(
            f"acemq: a certificate has to last at least a day and {validity_days} is not one"
        )

    x509, ec, hashes, serialization = _cryptography()

    where = Path(directory)
    try:
        where.mkdir(parents=True, exist_ok=True)
    except OSError as unwritable:
        raise AceMQError(f"acemq: cannot create {str(where)!r}: {unwritable}") from unwritable

    now = dt.datetime.now(dt.timezone.utc)
    # An hour of slack at the front, because a container's clock and the host's
    # are not the same clock and a certificate that is not valid yet fails in a
    # way that reads exactly like a certificate that is wrong.
    valid_from = now - dt.timedelta(hours=1)
    valid_until = now + dt.timedelta(days=validity_days)

    authority_key = ec.generate_private_key(ec.SECP256R1())
    authority = (
        _builder(x509, AUTHORITY_NAME, AUTHORITY_NAME, valid_from, valid_until, authority_key)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(authority_key, hashes.SHA256())
    )

    server_key = ec.generate_private_key(ec.SECP256R1())
    server = _leaf(
        x509,
        hashes,
        common_name=broker_host,
        key=server_key,
        authority=authority,
        authority_key=authority_key,
        valid_from=valid_from,
        valid_until=valid_until,
        purpose=x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
        names=_names_for(x509, broker_host),
    )

    client_key = ec.generate_private_key(ec.SECP256R1())
    client = _leaf(
        x509,
        hashes,
        common_name=CLIENT_NAME,
        key=client_key,
        authority=authority,
        authority_key=authority_key,
        valid_from=valid_from,
        valid_until=valid_until,
        purpose=x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
        names=None,
    )

    written: list[Path] = []
    for name, contents, mode in (
        ("ca.crt", _certificate_pem(serialization, authority), 0o644),
        ("ca.key", _key_pem(serialization, authority_key), 0o600),
        ("server.crt", _certificate_pem(serialization, server), 0o644),
        ("server.key", _key_pem(serialization, server_key), 0o600),
        ("client.crt", _certificate_pem(serialization, client), 0o644),
        ("client.key", _key_pem(serialization, client_key), 0o600),
    ):
        written.append(_write(where / name, contents, mode))

    configuration: Path | None = None
    if write_broker_configuration:
        configuration = _write(
            where / "rabbitmq.conf",
            broker_configuration(broker_certificate_path).encode("utf-8"),
            0o644,
        )
        written.append(configuration)

    return GeneratedCertificates(
        directory=where,
        authority=where / "ca.crt",
        authority_key=where / "ca.key",
        server=where / "server.crt",
        server_key=where / "server.key",
        client=where / "client.crt",
        client_key=where / "client.key",
        broker_configuration=configuration,
        expires=valid_until,
        broker_host=broker_host,
        files=tuple(written),
    )


def broker_configuration(certificate_path: str = DEFAULT_BROKER_CERTIFICATE_PATH) -> str:
    """A ``rabbitmq.conf`` pointing the broker at the generated certificates.

    ``verify_peer`` with ``fail_if_no_peer_cert = false``, which is the setting
    that lets one broker serve both cases: a client presenting no certificate
    logs in with a password, and one presenting a certificate has it checked
    against the authority. Turning ``fail_if_no_peer_cert`` on is what makes it
    mutual TLS, and is a one-line change here rather than a different broker.

    The plaintext listener stays on so the same broker can be reached either way
    while somebody is working out which of the two is broken.

    :param certificate_path: where the broker will find the files, on its own
        filesystem
    :returns: the configuration file's contents
    """
    return f"""# Written by acemq_amqp.devcerts. Development only.
listeners.tcp.default = 5672
listeners.ssl.default = 5671

ssl_options.cacertfile = {certificate_path}/ca.crt
ssl_options.certfile   = {certificate_path}/server.crt
ssl_options.keyfile    = {certificate_path}/server.key

# A client that presents a certificate has it checked; one that presents none
# logs in with a password. Setting fail_if_no_peer_cert to true is what turns
# this into mutual TLS.
ssl_options.verify               = verify_peer
ssl_options.fail_if_no_peer_cert = false
"""


def _cryptography() -> tuple[Any, Any, Any, Any]:
    """The four pieces of ``cryptography`` this module uses, imported lazily."""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
    except ImportError as missing:  # pragma: no cover - depends on the install
        raise AceMQError(
            "acemq: generating development certificates needs cryptography, which is an "
            'optional extra. Install it with pip install "acemq-amqp[crypto]"'
        ) from missing
    return x509, ec, hashes, serialization


def _subject(x509: Any, common_name: str) -> Any:
    """A distinguished name carrying the marker in its organisation.

    ``O=`` rather than ``OU=``, which is where Go and .NET put it. Java uses
    ``OU=`` and every library detects the marker by searching the whole
    distinguished name, so the two are interchangeable in practice — but two of
    the three write ``O=`` and the difference is not worth having.
    """
    return x509.Name(
        [
            x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, common_name),
            x509.NameAttribute(x509.oid.NameOID.ORGANIZATION_NAME, DEVELOPMENT_MARKER),
        ]
    )


def _builder(
    x509: Any,
    subject: str,
    issuer: str,
    valid_from: dt.datetime,
    valid_until: dt.datetime,
    key: Any,
) -> Any:
    return (
        x509.CertificateBuilder()
        .subject_name(_subject(x509, subject))
        .issuer_name(_subject(x509, issuer))
        .public_key(key.public_key())
        .serial_number(int.from_bytes(secrets.token_bytes(16), "big") >> 1 or 1)
        .not_valid_before(valid_from)
        .not_valid_after(valid_until)
    )


def _leaf(
    x509: Any,
    hashes: Any,
    *,
    common_name: str,
    key: Any,
    authority: Any,
    authority_key: Any,
    valid_from: dt.datetime,
    valid_until: dt.datetime,
    purpose: Any,
    names: Any,
) -> Any:
    builder = (
        x509.CertificateBuilder()
        .subject_name(_subject(x509, common_name))
        .issuer_name(authority.subject)
        .public_key(key.public_key())
        .serial_number(int.from_bytes(secrets.token_bytes(16), "big") >> 1 or 1)
        .not_valid_before(valid_from)
        .not_valid_after(valid_until)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        # One purpose each, as Go and .NET write them: the broker certificate is
        # for serving and the client one for authenticating. Java gives both to
        # both, which works and says less about what each is for.
        .add_extension(x509.ExtendedKeyUsage([purpose]), critical=False)
    )
    if names is not None:
        builder = builder.add_extension(names, critical=False)
    return builder.sign(authority_key, hashes.SHA256())


def _names_for(x509: Any, broker_host: str) -> Any:
    """The names the broker certificate is valid for.

    An IP address goes in as an address rather than a DNS name — a certificate
    listing ``10.0.0.4`` under DNS is valid for a *host called* ``10.0.0.4``,
    which nothing is, and the connection fails for a reason that reads like the
    certificate being wrong rather than like this.

    ``localhost`` picks up both loopback addresses, because half the tooling
    dials the name and the other half dials ``127.0.0.1``. ``rabbitmq`` comes
    along too, which is what the broker is called on a Docker network and is what
    Java adds for the same reason.
    """
    try:
        address = ipaddress.ip_address(broker_host)
    except ValueError:
        pass
    else:
        return x509.SubjectAlternativeName([x509.IPAddress(address)])

    names = [x509.DNSName(broker_host)]
    if broker_host != "localhost":
        names.append(x509.DNSName("localhost"))
    if broker_host != "rabbitmq":
        names.append(x509.DNSName("rabbitmq"))
    names.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
    names.append(x509.IPAddress(ipaddress.ip_address("::1")))
    return x509.SubjectAlternativeName(names)


def _certificate_pem(serialization: Any, certificate: Any) -> bytes:
    written: bytes = certificate.public_bytes(serialization.Encoding.PEM)
    return written


def _key_pem(serialization: Any, key: Any) -> bytes:
    written: bytes = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        # Unencrypted, because a passphrase held in the same script that reads
        # the key protects nothing, and one held anywhere else turns a
        # development broker into something with a setup step.
        encryption_algorithm=serialization.NoEncryption(),
    )
    return written


def _write(path: Path, contents: bytes, mode: int) -> Path:
    """Writes a file, then narrows it. Private keys end up ``0600``.

    Narrowed after writing rather than by opening with the mode, because an
    existing file keeps the permissions it already had and a key regenerated over
    a world-readable one would quietly stay world-readable.
    """
    try:
        path.write_bytes(contents)
        os.chmod(path, mode)
    except OSError as unwritable:
        raise AceMQError(f"acemq: cannot write {str(path)!r}: {unwritable}") from unwritable
    return path


def main(argv: list[str] | None = None) -> int:
    """``python -m acemq_amqp.devcerts``.

    :param argv: the arguments, or ``None`` to read :data:`sys.argv`
    :returns: the process exit status
    """
    parser = argparse.ArgumentParser(
        prog="python -m acemq_amqp.devcerts",
        description=(
            "Write a development certificate authority, a broker certificate and a client "
            "certificate. Everything is stamped "
            f'"{DEVELOPMENT_MARKER}" and the AceMQ libraries refuse it however trust is '
            "configured, which is what keeps it off anything real."
        ),
    )
    parser.add_argument(
        "--directory", "-d", default="certs", help="where to write them (default: certs)"
    )
    parser.add_argument(
        "--broker",
        "-b",
        default="localhost",
        help="the name or address the broker certificate is valid for (default: localhost)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_VALIDITY_DAYS,
        help=f"how long they last (default: {DEFAULT_VALIDITY_DAYS})",
    )
    parser.add_argument(
        "--broker-certificate-path",
        default=DEFAULT_BROKER_CERTIFICATE_PATH,
        help=(
            "where the broker will find them on its own filesystem, for the generated "
            f"rabbitmq.conf (default: {DEFAULT_BROKER_CERTIFICATE_PATH})"
        ),
    )
    parser.add_argument(
        "--no-broker-config",
        action="store_true",
        help="do not write rabbitmq.conf",
    )
    arguments = parser.parse_args(argv)

    try:
        result = generate(
            arguments.directory,
            broker_host=arguments.broker,
            validity_days=arguments.days,
            broker_certificate_path=arguments.broker_certificate_path,
            write_broker_configuration=not arguments.no_broker_config,
        )
    except AceMQError as failure:
        print(failure, file=sys.stderr)
        return 1

    for path in result.files:
        print(path)
    print(
        f'valid for {result.broker_host} until {result.expires:%Y-%m-%d}. Stamped "'
        f'{result.marker}", so every AceMQ library refuses these unless it is told to '
        "allow them.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - the entry point itself
    raise SystemExit(main())
