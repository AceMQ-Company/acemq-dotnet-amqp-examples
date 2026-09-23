# advanced/04 — development certificates and TLS, in VB.NET

A TLS broker on a laptop, and the reason its certificates cannot reach
production.

Every developer who has needed TLS locally has generated a self-signed
certificate and then turned verification off to use it. The certificate works, so
it stays; the flag that made it work stays too; and the two travel together into
an environment where that flag hands every message and every password to whoever
answers on the address in the URL.

The same example exists in C# at
[advanced/04-development-certificates-csharp](../04-development-certificates-csharp),
and the reasoning is written out in full there — in particular the table of what
`TrustCertificateAuthority` accepts, which is the part most likely to be ported
wrongly from another language. This page covers the same ground with the VB.NET
specifics.

## What it shows

- **What `acemq-certs` writes**: five files, and the absence of a sixth.
- **A connection over `amqps://`** that publishes and consumes, and a second one
  presenting a client certificate.
- **Five refusals at the handshake** and **four before any socket opens**, each
  asserted.
- **That a PEM bundle contributes only its first certificate.**

## Running it

This is the one example that needs a broker of its own, because it needs a TLS
listener holding certificates generated on this machine:

```bash
curl -O https://acemq.org/nuget/v3/flatcontainer/acemq.amqp.devcerts/0.7.2/acemq.amqp.devcerts.0.7.2.nupkg
dotnet tool install --global AceMq.Amqp.DevCerts --version 0.7.2 --source .
acemq-certs --out certs --broker localhost
chmod 644 certs/server.key
docker compose --profile tls up -d
dotnet run --project advanced/04-development-certificates-vbnet
```

**The `curl`, and `--source` rather than `--add-source`.** `dotnet tool install`
cannot read the AceMQ feed directly: it is a static directory tree offering only
the flat container, which is enough for `dotnet restore` and not enough for the
tool installer — which fails with a bare `Unhandled exception: Object reference
not set to an instance of an object.` and no indication that the feed is why.

`--source` **replaces** the configured package sources. `--add-source` adds to
them, and this repository's `nuget.config` puts the AceMQ feed back in scope — so
`--add-source .` fails the same way from inside the repository while working
anywhere else.

**The `chmod`** is because the generator writes private keys `0600`, which is
right for a key and wrong for a container running as another user. RabbitMQ
reports an unreadable key as a listener that failed to start.

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

published and consumed A-7 over TLS
and again presenting a client certificate

  refused    without AllowDevelopmentCertificates
  refused    even under Insecure
  refused    with revocation checking left on
  refused    trusting a different authority
  refused    system trust only

  refused    a directory instead of a file: no certificate authority file at /path/to/certs
  refused    a .crt as the client certificate: a client certificate needs its private key; load a .pfx rather than a .cer
  CONNECTED  a bundle with our authority first
  refused    a bundle with our authority second
```

## The trust store here is one certificate in one file

`TrustCertificateAuthority` takes **a single PEM or DER file holding one
certificate**. Not a store, not a directory, not a bundle — a directory is
refused outright, and a PEM with the right authority second is a handshake
rejection with nothing in it about bundles.

The Java library wants the opposite shape, a *directory* holding `keystore.p12`
and `truststore.p12`. A page written for it describes something this API refuses.
The [C# page](../04-development-certificates-csharp/README.md#the-trust-store-here-is-one-certificate-in-one-file)
has the full table and the two further divergences from the Go and Python
repositories.

## Configuring TLS in VB.NET

```vb
Dim trusting = TlsOptions.Required() _
    .TrustCertificateAuthority(ca) _
    .AllowDevelopmentCertificates() _
    .WithoutRevocationChecking()

Dim mq = Await AceMqConnection.ConnectAsync(
    ConnectionConfig.ForUrl(TlsUrl()).Tls(trusting) _
        .ClientName("examples/04-development-certificates-vbnet").Build(),
    New JsonCodec(),
    token)
```

`WithoutRevocationChecking` is **not optional here**. `Required()` turns
revocation checking on, which is correct, and a generated development authority
publishes no CRL and runs no responder — so the check cannot come back and the
chain is rejected. The example asserts that refusal rather than leaving it to be
discovered from a handshake error.

## Three names this example could not use

VB.NET is case-insensitive and has a larger keyword list than C#:

- **`Event`** is a keyword, so the message type's property is `Detail`. The C#
  twin calls it `Detail` too, so the two publish the same shape.
- **`directory`** shadows `System.IO.Directory` for the rest of the method, and
  every later `Directory.GetFiles` becomes an error about the variable. It is
  `certificates`.
- **`bundle`** was worse. With a `String` named `bundle` in scope, the call
  `Bundle(caFirst)` to the helper function is parsed as *indexing that string*,
  and the compiler reports "Option Strict On disallows implicit conversions from
  String to Integer" — which names neither the function nor the variable. It is
  `scratch`.

The last one is the general shape of this trap: VB resolves the identifier before
deciding whether it is a call or an index, so a variable sharing a name with a
method turns every call into an indexing expression and the error lands on the
argument.

## `even under Insecure` is the example

`TlsOptions.Insecure()` encrypts and verifies nothing: it accepts any certificate
from anybody. It still refuses this one, because a generated authority's private
key usually ends up in a repository — a development certificate that could reach
production would be an authority anybody who can read that repository can issue
against, and the connection would succeed.

The way through is `AllowDevelopmentCertificates()`: a named method a reviewer
sees in a diff and an operator can grep a deployment for.

## Five files, and the sixth that is missing

There is **no `ca.key`** — the authority's private key is never written, so this
authority cannot issue anything after the moment it was generated. And the client
is one `client.pfx` rather than a `.crt` and a `.key`, because that is what
`X509Certificate2` loads and a certificate without its private key cannot
complete a handshake.

## How this stays honest

The file list, the absent `ca.key`, the permissions on `client.pfx`, the marker
on all three certificates, the message surviving a TLS round trip, all five
handshake refusals and their single exception type, all four configuration
refusals and the text each carries, and both orderings of the bundle are
asserted. The example exits non-zero if any of them is wrong. CI generates
certificates, starts a TLS broker and runs it on every push.
