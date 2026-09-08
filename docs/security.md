# Security

Two things are decided here, and they are deliberately not the same knob.

**The URL scheme decides whether the connection is encrypted.** `amqps://` is,
`amqp://` is not. That is the part somebody reads in a configuration file and
believes, so it is the part that governs.

Everything in `acemq_amqp.security` describes *how* an encrypted connection is
checked and who it logs in as. It cannot turn encryption on, and it cannot turn
it off.

## The default is the one to want

```python
mq = await connect("amqps://broker.internal:5671/")
```

No `Security` at all: the broker is verified against the machine's trust store,
the name on the certificate must match the host in the URL, and nothing older
than TLS 1.2 is spoken — whatever the machine's `openssl` would otherwise allow.
TLS 1.0 and 1.1 are withdrawn, and a broker still offering them is not a reason
to speak them.

## Trusting one authority

Most brokers are not on a public certificate authority, because no public
authority issues certificates for `broker.internal`. Name the authority instead:

```python
from acemq_amqp import Security, connect, credentials_from_environment

mq = await connect(
    "amqps://broker.internal:5671/",
    security=Security(
        certificate_authority="/etc/acemq/ca.crt",
        credentials=credentials_from_environment("MQ_USER", "MQ_PASSWORD"),
    ),
)
```

Naming an authority **replaces** the system trust store rather than adding to
it. That is the point rather than a side effect: a broker holding a certificate
from a public authority is not your broker, and the several hundred authorities
a machine trusts by default are several hundred ways to be wrong. Verification
succeeding therefore means the chain reached *this* authority.

A self-signed broker certificate works here too. Put the broker's own
certificate in the file and it becomes the one authority you trust — which is
the answer to a self-signed certificate, not
[the way out](#the-way-out-and-what-it-costs).

| | |
|---|---|
| `certificate_authority` | The PEM authority to verify the broker against, and no other |
| `client_certificate` | What to present, for a broker that authenticates clients by certificate |
| `client_key` | The PEM private key for it. Defaults to reading the key out of `client_certificate`, which is where it lives when the two are in one file |
| `client_key_password` | The passphrase on that key. Kept out of `repr` |
| `server_name` | The name to check the certificate against, when it is not the one in the URL |
| `credentials` | The login, or a source asked for it at connection time |
| `verification` | `Verification.CERTIFICATE`, or `NOTHING_AT_ALL` |

Everything here works the same on the blocking API — `sync.connect(url,
security=...)` — because a program that is not running an event loop has exactly
the same broker to reach.

### The URL has to agree

```python
await connect("amqp://broker:5672/", security=Security(certificate_authority="ca.crt"))
# SecurityError: this Security configures TLS but the URL is amqp://, which is not
# encrypted. Nothing here can make a plaintext connection safe: change the URL to
# amqps:// or take the TLS settings off
```

Refused rather than ignored. A service that was handed a certificate authority,
connected in plaintext and reported success is the failure this whole module
exists to prevent — and it is a failure that leaves a certificate file sitting
next to a plaintext connection, which is exactly what makes it convincing in a
review.

Credentials alone are welcome on either scheme, because keeping a password out
of a connection string is worth doing whether or not the connection is
encrypted.

### When the address is not the name

```python
Security(
    certificate_authority="/etc/acemq/ca.crt",
    server_name="broker.internal",
)
```

The name a certificate is checked against is normally taken from the host in the
URL. That is the right default and the wrong answer for a broker reached by IP
address, through an SSH tunnel, or at a container's internal name, where the
address dialled and the name on the certificate are different strings on
purpose. `server_name` says which one to check.

It still checks. This is not a way to skip verification with extra steps — the
chain must still reach the named authority, and the certificate must still carry
the name given here.

### Client certificates

For a broker that authenticates clients by certificate rather than by password:

```python
Security(
    certificate_authority="/etc/acemq/ca.crt",
    client_certificate="/etc/acemq/client.crt",
    client_key="/etc/acemq/client.key",
    client_key_password=os.environ.get("CLIENT_KEY_PASSPHRASE"),
)
```

Both halves in one file is common, and then `client_key` can be left out.

A key with no certificate is refused: a broker authenticates the certificate,
and a key on its own proves nothing. A certificate or key that cannot be read
raises `SecurityError` **naming the setting that was wrong** — `ssl` reports a
missing file as `FileNotFoundError` and a malformed one as `[SSL] PEM lib`, and
neither of those tells anybody which of four paths to look at.

## Credentials

A password in a connection string reaches every log line, crash report and `ps`
listing that URL ever appears in. Supply it separately and the URL carries a
host and nothing else:

```python
from acemq_amqp import Credentials, credentials_from_environment, credentials_from_file

Credentials("app", password)                         # from wherever you already had it
credentials_from_environment("MQ_USER", "MQ_PASSWORD")
credentials_from_file("/run/secrets/broker", username="app")
```

Credentials given here **replace** whatever the URL carried, and are
percent-encoded on the way in — a password is chosen by a person or a generator,
neither is under any obligation to avoid `@`, `/` or `:`, and one of those
spliced in raw produces a URL that parses into a different host.

`credentials_from_environment` raises `SecurityError` naming the variable when
either is unset, rather than logging in as an empty string.

`credentials_from_file` is how a mounted Kubernetes secret and a Docker secret
arrive. The file holds the password alone when `username=` is given, and
`username:password` when it is not. Trailing whitespace is trimmed, because a
file written by an editor almost always ends in a newline and a password with a
newline on the end fails in a way nobody enjoys diagnosing.

### They are read per connection

Both file-reading sources are asked **each time a connection is made**, not once
at import. That is what makes a password rotated by a sidecar, or a remounted
Kubernetes secret, take effect on the next reconnection without a restart.

A source is just a callable — `Callable[[], Credentials]` — so a complete
implementation of one is:

```python
security = Security(credentials=lambda: Credentials("app", vault.read()))
```

A callable rather than a class to subclass, because in Python a function is the
interface.

### The secret does not print

```python
print(Credentials("app", "hunter2"))
# Credentials(username='app', secret=<secret>)
```

`repr`, `str` and an f-string all give back the same thing, and a test in the
suite asserts it. That is the whole reason this is a type rather than two
strings: the value of it is that it holds under a `print()` somebody added at
three in the morning, in a dataclass dump, in an exception, in a debugger's
variables pane.

`client_key_password` is kept out of `Security`'s own `repr` for the same
reason.

## The way out, and what it costs

```python
from acemq_amqp import without_verifying_the_broker

mq = await connect("amqps://localhost:5671/", security=without_verifying_the_broker())
```

This encrypts the traffic and checks **nothing at all** about who is on the
other end. Any certificate is accepted, from any issuer, for any name, so
somebody who can answer on the address in your URL receives every message you
publish and every password you log in with — over a connection that looks
encrypted in every log and every metric.

It exists because there is one situation where the alternative is worse: a
broker with a self-signed certificate on a laptop or in a test fixture, where
the choice is otherwise between this and turning encryption off entirely.

It is a function with a long name rather than a `verify=False` on purpose.
**There is no keyword argument anywhere in this library that turns verification
off**, so it should be impossible to end up here by pasting one, and hard to
leave in a file nobody rereads.

**It is wrong in production, always.** The alternative is one line —
`Security(certificate_authority="ca.crt")` — and gives back everything this
gives up.

## Development certificates

A broker on a laptop needs a certificate, and the alternative to generating one
is six `openssl` invocations with a hand-written extensions file — which is how
people end up developing against a plaintext broker instead, and finding out on
the day TLS is switched on that nothing was ever tested through it.

```bash
pip install "acemq-amqp[crypto]"
python -m acemq_amqp.devcerts --directory certs --broker localhost
```

That writes `ca.crt`, `ca.key`, `server.crt`, `server.key`, `client.crt`,
`client.key` and a `rabbitmq.conf` pointing the broker at them. The same file
names Go's `acemq-certs` writes, so it is a drop-in replacement for it.

```python
from acemq_amqp.devcerts import generate

written = generate("certs", broker_host="localhost", validity_days=30)
```

### The marker, which is the whole point

Every certificate written carries `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` in its
subject organisation, and **this library refuses any certificate carrying it,
however trust is configured — including `without_verifying_the_broker()`**:

```python
mq = await connect("amqps://broker:5671/", security=Security(certificate_authority="certs/ca.crt"))
# SecurityError: the certificate authority 'certs/ca.crt' is marked 'ACEMQ DEVELOPMENT
# ONLY - DO NOT TRUST'. It was generated for development and its signing key is not a
# secret, so it is not trusted here. Use a real certificate, or say so deliberately with
# Security(allow_development_certificates=True)
```

That is not a warning somebody has to read. It is the mechanism. A generated
authority's private key sits in the same directory as its certificate and
usually ends up in a repository, so a certificate that *could* reach production
would be an authority anybody who can read that repository can issue against —
and the failure would be silent, because the connection succeeds.

Java, Go and .NET stamp the same string and enforce it the same way, so a broker
set up by any of the four is reachable from all of them and none of them will
speak to it without being told.

Two checks, because a development certificate arrives from two directions. The
files this configuration *names* are read when the TLS context is built, where
the error can point at the setting that is wrong. What the broker actually
*presents* is checked at the handshake, where no trust setting can avoid it —
that is the path that matters under `without_verifying_the_broker()`, because
there is no verified chain there for a check to hang on and it is the
configuration in which a development certificate is most likely to be reached
for.

### Saying so deliberately

```python
mq = await connect(
    "amqps://localhost:5671/",
    security=Security(
        certificate_authority="certs/ca.crt",
        allow_development_certificates=True,
    ),
)
```

A named argument, spelled out, that a reviewer will see. It belongs in a test
fixture and a developer's compose file and nowhere else — put it in the
production checklist below as something to `grep` for.

## A production checklist

- `amqps://` in the URL. Nothing else makes the connection encrypted
- `certificate_authority` naming your own authority, not the system store
- `credentials` rather than a password in the URL, from a source that is read
  per connection
- No `without_verifying_the_broker` anywhere — `grep` for it in the deployed
  configuration, not only in the code
- No `allow_development_certificates=True` anywhere either, and for the same
  reason. Without it a development certificate cannot reach production; with it,
  one can
- `client_certificate` too, if the broker authenticates clients that way
- The account the service logs in as has permissions on the queues it uses and
  nothing else. That is a broker configuration and this library cannot help with
  it

## What this library does not do

TLS does not encrypt message **bodies**. It protects the connection, and a
message sitting in a queue is plaintext to anyone with access to the broker. A
payload that must not be readable there is
[`EncryptedCodec`'s](serialization.md#encryption) problem, not this module's.

It does not manage certificates or rotate them. `ssl` is the standard library,
and the context this module builds is handed to aio-pika rather than
reimplemented around it. It generates development ones and refuses them
everywhere else, which is a different job from being a certificate authority.

It does not authorise. Which queues an account may read and write is a broker
setting, and a library that appeared to enforce it would be enforcing it in the
one place an attacker is not.
