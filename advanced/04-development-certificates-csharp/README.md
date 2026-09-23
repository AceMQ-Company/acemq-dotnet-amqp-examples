# advanced/04 — development certificates and TLS, in C#

A TLS broker on a laptop, and the reason its certificates cannot reach
production.

Every developer who has needed TLS locally has generated a self-signed
certificate and then turned verification off to use it. The certificate works, so
it stays; the flag that made it work stays too; and the two travel together into
an environment where that flag hands every message and every password to whoever
answers on the address in the URL.

The same example exists in VB.NET at
[advanced/04-development-certificates-vbnet](../04-development-certificates-vbnet).

## What it shows

- **What `acemq-certs` writes**: an authority, a broker certificate, a client
  PKCS#12, and a `rabbitmq.conf` pointing the broker at them — five files, and
  the absence of a sixth.
- **A connection over `amqps://`** that publishes and consumes, and a second one
  presenting a client certificate.
- **Five refusals at the handshake**, each asserted: the marker without the flag,
  the marker **under `Insecure()`**, revocation checking left on, a different
  authority, and the system trust store alone.
- **Four refusals before any socket opens**, which is where the .NET-specific
  answers live: a directory as the authority, a PKCS#12 as the authority, a
  `.crt` as the client certificate, and TLS settings on an `amqp://` URL.
- **That a PEM bundle contributes only its first certificate.**

## Running it

This is the one example that needs a broker of its own, because it needs a TLS
listener holding certificates generated on this machine:

```bash
curl -O https://acemq.org/nuget/v3/flatcontainer/acemq.amqp.devcerts/0.7.2/acemq.amqp.devcerts.0.7.2.nupkg
dotnet tool install --global AceMq.Amqp.DevCerts --version 0.7.2 --add-source .
acemq-certs --out certs --broker localhost
chmod 644 certs/server.key
docker compose --profile tls up -d
dotnet run --project advanced/04-development-certificates-csharp
```

Two of those lines are worth understanding rather than copying.

**The `curl` is not optional.** `dotnet tool install --add-source
https://acemq.org/nuget/index.json` does not work: the AceMQ feed is a static
directory tree that offers only the flat container — enough for `dotnet restore`,
which is why every other example needs nothing special, and not enough for the
tool installer. It fails with a bare `Unhandled exception: Object reference not
set to an instance of an object.` and no indication that the feed is the problem.
Downloading the package and installing from the current directory is the way
through.

**The `chmod`** is because the generator writes private keys `0600`, which is
right for a key and wrong for a container that runs as another user. RabbitMQ
reports an unreadable key as a listener that failed to start — the node comes up
perfectly, without the listener, and the first sign is a connection refused from
something that looks wrong.

## What to look for

```
acemq-certs wrote 5 files into /path/to/certs
  ca.crt
  client.pfx
  rabbitmq.conf
  server.crt
  server.key
client.pfx is UserWrite, UserRead

authority: CN=AceMQ development CA, O=ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
broker:    CN=localhost, O=ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
client:    CN=acemq-dev-client, O=ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
valid until 2026-10-23

TlsOptions[Required, ca=custom, clientCerts=0, revocation=False, development certificates allowed]
published and consumed A-7 over TLS
and again presenting a client certificate

  refused    without AllowDevelopmentCertificates
  refused    even under Insecure
  refused    with revocation checking left on
  refused    trusting a different authority
  refused    system trust only
  all five refusals arrive as AuthenticationException: The remote certificate was rejected by the provided RemoteCertificateValidationCallback.

  refused    a directory instead of a file: no certificate authority file at /path/to/certs
  refused    a PKCS#12 as the authority: could not read a certificate from /path/to/certs/client.pfx
  refused    a .crt as the client certificate: a client certificate needs its private key; load a .pfx rather than a .cer
  refused    TLS settings on an amqp:// URL: TLS was configured but the URL scheme is 'amqp'. Use amqps:// — the two schemes are different ports and the broker will not upgrade one.
  CONNECTED  a bundle with our authority first
  refused    a bundle with our authority second
```

## The trust store here is one certificate in one file

This is the paragraph to read before porting anything from another language.

`TrustCertificateAuthority` takes **a single PEM or DER file holding one
certificate**. Not a store, not a directory, not a bundle:

| given | result |
|---|---|
| `certs/ca.crt` | works |
| `certs/` (a directory) | `SecurityConfigurationException: no certificate authority file at …` |
| `certs/client.pfx` | `SecurityConfigurationException: could not read a certificate from …` |
| a PEM with the right authority **first** | works |
| a PEM with the right authority **second** | handshake refused |

`TlsOptions.CertificateAuthority` is a single `X509Certificate2?`, not a
collection, and the string overload is `new X509Certificate2(path)` — which reads
the *first* certificate in a PEM and ignores the rest. So a bundle silently
becomes whichever certificate happens to be at the top of the file, and the
failure is a handshake rejection with nothing in it about bundles. The example
builds both orderings and asserts each outcome, because this is not the kind of
thing to take anyone's word for.

**The Java library wants the opposite shape** — a *directory* holding
`keystore.p12` and `truststore.p12` — and a page written for it describes
something this API refuses outright. The two are not variants of one idea; the
first row of that table is the whole API here.

Two further differences from the sibling repositories, both asserted rather than
described:

- **Go's and Python's pages say naming an authority *replaces* the machine's
  trust store.** That is true of their APIs and not of this one. Here the named
  authority is consulted only after the platform's own validation has already
  failed, so it widens trust rather than narrowing it. If trusting exactly one
  authority and nothing else is the requirement, this API does not express it.
- **The refusal does not name the marker.** Go's error says which certificate it
  objected to. .NET surfaces every rejection from a validation callback as the
  same `AuthenticationException` — "the remote certificate was rejected by the
  provided RemoteCertificateValidationCallback" — whether the cause was the
  marker, the wrong authority, an unknown authority or an unanswerable revocation
  check. The example asserts that all five arrive identically, so nobody builds
  log-matching on a distinction that is not there. Telling them apart means
  removing one setting at a time.

## `WithoutRevocationChecking` is not optional here

`TlsOptions.Required()` turns revocation checking **on**, which is correct: a
revoked certificate is one known to be in the wrong hands, and not checking is
how a compromise stays usable.

A generated development authority publishes no CRL and runs no responder, so the
check cannot come back at all and the chain is rejected. The example asserts that
refusal explicitly, because the alternative is discovering it from a handshake
error that says nothing about revocation.

This is the one line of the four that belongs *only* in development. The other
three — trusting the authority, allowing development certificates, pointing at
`amqps://` — have production analogues. This one is a statement that the network
cannot reach the issuer, and its real use is an isolated network where the check
does not fail closed so much as hang.

## `even under Insecure` is the example

`TlsOptions.Insecure()` encrypts and verifies nothing: it accepts any certificate
from anybody. It still refuses this one.

That is not a courtesy. A generated authority's private key usually ends up in a
repository, so a development certificate that could reach production would be an
authority **anybody who can read that repository can issue against** — and the
connection would succeed. The marker makes that impossible on every path rather
than on the paths somebody remembered.

The way through is `AllowDevelopmentCertificates()`: a named method a reviewer
will see in a diff, and one more thing to `grep` for in a deployed configuration.

Java, .NET, Python, Go and Ruby stamp the same string and enforce it the same
way, so a certificate generated by any of the five generators is refused by all
five libraries. The check is an exact, case-insensitive substring match on
`ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` against every subject and issuer in the
chain — so a private CA with a *differently worded* development notice is not
caught by it, and should not be: the marker is a contract between these
generators and these libraries, not a guess about what looks like a test
certificate.

## Five files, and the sixth that is missing

```
ca.crt         the authority to trust
client.pfx     certificate and key together, password acemq-dev
rabbitmq.conf  mount at /etc/rabbitmq/rabbitmq.conf
server.crt     the broker's certificate
server.key     the broker's key
```

Go and Python write seven. Two of the differences matter:

- **There is no `ca.key`.** The authority's private key is never written to disk,
  so this authority cannot issue anything after the moment it was generated. A
  checked-in `ca.key` is an authority anybody with the repository can issue
  against, which is the failure the marker exists to contain — and not writing it
  at all contains it earlier.
- **The client is one `client.pfx`, not a `.crt` and a `.key`.** That is what
  `X509Certificate2` loads, and a certificate without its private key cannot
  complete a handshake. `WithClientCertificate` catches that case with a message
  naming `.pfx`; without it, the failure is a bare connection reset from the
  broker.

The client certificate half is shown but not exercised: the generated
`rabbitmq.conf` sets `ssl_options.verify = verify_none`, so the broker never asks
for one. What the example proves is that presenting a client certificate is
harmless, not that EXTERNAL authentication happened. Turning on `verify_peer` is
the other half, and it is left off because a broker that demands a certificate
nobody configured refuses every connection with an error naming none of this.

## Thirty days

Long enough not to be a nuisance, short enough that one of these reaching a
server is a problem that expires by itself. `--days` changes it, and a reason to
raise it is usually a reason to use a real certificate instead.

## How this stays honest

The file list, the absent `ca.key`, the permissions on `client.pfx`, the marker
on all three certificates, the authority having no private key and the client
having one, the message surviving a TLS round trip, all five handshake refusals
and their single exception type, all four configuration refusals and the text
each carries, and both orderings of the bundle are asserted. The example exits
non-zero if any of them is wrong. CI generates certificates, starts a TLS broker
and runs it on every push.
