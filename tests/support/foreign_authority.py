"""Writes an authority that is real, is well-formed, and signed nothing we use.

Two of the TLS tests need a certificate that fails for exactly one reason: it
came from the wrong authority. Everything else about it has to be unimpeachable.

That rules out :mod:`acemq_amqp.devcerts`, which stamps ``ACEMQ DEVELOPMENT ONLY
- DO NOT TRUST`` into every subject it writes. The library refuses those before
it gets as far as checking who signed anything, so a "wrong authority" built
from devcerts is refused for being a development certificate and the test passes
without ever exercising the thing it names. That is worse than no test: it is a
green one asserting the wrong property.

So this writes an ordinary authority, unmarked, and one client certificate under
it:

    other-ca.crt    an authority nothing here trusts
    stranger.crt    a client certificate it signed
    stranger.key    that certificate's key

Development only, like everything else under tests/. It is deliberately *not*
marked as such, because being unmarked is the entire point.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

#: Long enough that a run never straddles the expiry, short enough that a copy
#: left on a disk somewhere stops working.
VALIDITY_DAYS = 2


def _name(common_name: str) -> x509.Name:
    # No organisation naming AceMQ anywhere. This authority is meant to read as
    # a stranger's, and a test that failed because the subject looked familiar
    # would be confusing for the wrong reason.
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def write(directory: str | Path) -> Path:
    """Writes the authority and its one client certificate.

    :param directory: where to put them; created if it is not there
    :returns: the directory
    """
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)

    now = dt.datetime.now(dt.timezone.utc)
    expires = now + dt.timedelta(days=VALIDITY_DAYS)

    authority_key = ec.generate_private_key(ec.SECP256R1())
    authority = (
        x509.CertificateBuilder()
        .subject_name(_name("Unrelated Certificate Authority"))
        .issuer_name(_name("Unrelated Certificate Authority"))
        .public_key(authority_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(expires)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(authority_key, hashes.SHA256())
    )

    client_key = ec.generate_private_key(ec.SECP256R1())
    client = (
        x509.CertificateBuilder()
        .subject_name(_name("a client this broker has never heard of"))
        .issuer_name(authority.subject)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(expires)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
        .sign(authority_key, hashes.SHA256())
    )

    (out / "other-ca.crt").write_bytes(authority.public_bytes(serialization.Encoding.PEM))
    (out / "stranger.crt").write_bytes(client.public_bytes(serialization.Encoding.PEM))
    key_path = out / "stranger.key"
    key_path.write_bytes(
        client_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    # Readable by the broker's user in a container, which is not the user that
    # wrote it. devcerts' own output needs the same treatment for the same
    # reason; a key the broker cannot read is reported as a listener that never
    # started.
    key_path.chmod(0o644)
    return out


if __name__ == "__main__":
    where = write(sys.argv[1] if len(sys.argv) > 1 else "certs")
    print(f"wrote an unrelated authority and one client certificate into {where}")
