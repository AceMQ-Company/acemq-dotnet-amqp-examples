// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A TLS broker on a laptop, and the reason its certificates cannot reach
// production. In C#.
//
// This is the one example that needs a broker of its own, because it needs a
// TLS listener holding certificates generated on this machine:
//
//   curl -O https://acemq.org/nuget/v3/flatcontainer/acemq.amqp.devcerts/0.7.2/acemq.amqp.devcerts.0.7.2.nupkg
//   dotnet tool install --global AceMq.Amqp.DevCerts --version 0.7.2 --add-source .
//   acemq-certs --out certs --broker localhost
//   chmod 644 certs/server.key
//   docker compose --profile tls up -d
//   dotnet run --project advanced/04-development-certificates-csharp
//
// Every developer who has needed TLS locally has generated a self-signed
// certificate and then turned verification off to use it. The certificate
// works, so it stays; the flag that made it work stays too; and the two travel
// together into an environment where that flag hands every message and every
// password to whoever answers on the address in the URL.
//
// So everything acemq-certs writes is stamped ACEMQ DEVELOPMENT ONLY - DO NOT
// TRUST in its subject organisation, and this library refuses any chain
// carrying it, however trust is configured -- including under Insecure(). The
// way through is AllowDevelopmentCertificates(): a named method a reviewer sees
// in a diff and an operator can grep a deployment for.
//
// WHAT .NET WANTS FOR A TRUST STORE IS NOT WHAT THE OTHER LANGUAGES WANT, and
// the difference is asserted below rather than described. The Java library
// takes a *directory* holding keystore.p12 and truststore.p12. This one takes
// a single PEM or DER file holding exactly one certificate: a directory is
// refused outright, and a PEM bundle contributes only its first certificate.
// "Trust store" is the wrong word here -- it is one authority.

using System.Diagnostics;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Audit
{
    // Not named "Event": the VB.NET twin of this example cannot have a
    // property by that name, because Event is a keyword there.
    public string Detail { get; set; } = "";
}

public static class Program
{
    private const string Queue = "dev-certs-csharp";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = cancellation.Token;

        var certificates = Path.GetFullPath(
            Environment.GetEnvironmentVariable("ACEMQ_CERTS") is { Length: > 0 } d ? d : "certs");
        var password = Environment.GetEnvironmentVariable("ACEMQ_CERT_PASSWORD") is { Length: > 0 } p
            ? p
            : "acemq-dev";

        Check(Directory.Exists(certificates),
            $"no certificates at {certificates}. Generate them first -- the commands are at the " +
            "top of this file, and in this directory's README.");

        var ca = Path.Combine(certificates, "ca.crt");
        var clientPfx = Path.Combine(certificates, "client.pfx");

        // ---- what the generator wrote --------------------------------------
        var written = Directory.GetFiles(certificates).Select(Path.GetFileName).OrderBy(n => n).ToList();
        Console.WriteLine($"acemq-certs wrote {written.Count} files into {certificates}");
        foreach (var name in written) Console.WriteLine($"  {name}");

        // Five, not the seven Go and Python write. The difference is the point
        // rather than an oversight: .NET keeps the client's certificate and key
        // together in one PKCS#12, because that is what X509Certificate2 loads
        // and a separate .crt/.key pair cannot complete a handshake here.
        Check(written.SequenceEqual(new[] { "ca.crt", "client.pfx", "rabbitmq.conf", "server.crt", "server.key" }),
            $"the generator wrote {string.Join(" ", written)}");

        // AND NO ca.key. The authority's private key is never written to disk,
        // so this authority cannot issue anything after the moment it was
        // generated -- which is the single largest difference between a
        // generated development CA and a liability. Go writes ca.key and a
        // checked-in copy of it is an authority anybody with the repository can
        // issue against.
        Check(!written.Contains("ca.key"),
            "the authority's private key was written to disk, which makes it an authority anybody can use");

        // A private key readable by every account on the machine is a bad habit
        // to teach even in development. server.key is exempt because the
        // documented chmod 644 above deliberately relaxes it -- RabbitMQ runs
        // as another user in the container and reports an unreadable key as a
        // listener that simply failed to start.
        if (!OperatingSystem.IsWindows())
        {
            var mode = File.GetUnixFileMode(clientPfx);
            Console.WriteLine($"client.pfx is {mode}");
            Check(mode == (UnixFileMode.UserRead | UnixFileMode.UserWrite),
                $"client.pfx is {mode}, not owner-only");
        }

        // ---- the marker is on every certificate, including the authority ----
        //
        // Which is what makes the refusal reach a server certificate that does
        // not carry the marker itself but was issued by one that does.
        using var authority = new X509Certificate2(ca);
        using var server = new X509Certificate2(Path.Combine(certificates, "server.crt"));
        using var client = new X509Certificate2(clientPfx, password);

        Console.WriteLine($"\nauthority: {authority.Subject}");
        Console.WriteLine($"broker:    {server.Subject}");
        Console.WriteLine($"client:    {client.Subject}");
        Console.WriteLine($"valid until {authority.NotAfter:yyyy-MM-dd}");

        foreach (var (what, certificate) in new[]
                 {
                     ("authority", authority), ("broker", server), ("client", client),
                 })
        {
            Check(certificate.Subject.Contains(TlsOptions.DevelopmentMarker, StringComparison.OrdinalIgnoreCase),
                $"the {what} certificate does not carry the marker: {certificate.Subject}");
        }

        Check(!authority.HasPrivateKey, "ca.crt carries a private key, which it must not");
        Check(client.HasPrivateKey, "client.pfx has no private key, so it cannot finish a handshake");

        // ---- connecting ----------------------------------------------------
        //
        // WithoutRevocationChecking is not optional here, and it is the step
        // that is easiest to leave out. Required() turns revocation checking on
        // by default -- correct, because a revoked certificate is one known to
        // be in the wrong hands. A generated development authority publishes no
        // CRL and runs no responder, so the check cannot come back at all and
        // the chain is rejected. The refusal below asserts exactly that, so
        // nobody has to discover it from a handshake error.
        var trusting = TlsOptions.Required()
            .TrustCertificateAuthority(ca)
            .AllowDevelopmentCertificates()
            .WithoutRevocationChecking();

        Console.WriteLine($"\n{trusting}");

        var received = new TaskCompletionSource<Audit>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(TlsUrl()).Tls(trusting)
                .ClientName("examples/04-development-certificates-csharp").Build(),
            new JsonCodec(),
            token);

        await mq.DeclareQueueAsync(Queue);
        using var consumer = await mq.ConsumeAsync<Audit>(Queue, message =>
        {
            received.TrySetResult(message.Payload);
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Audit>("", Queue).SendAsync(new Audit { Detail = "A-7" });
        var back = await received.Task.WaitAsync(token);
        Console.WriteLine($"published and consumed {back.Detail} over TLS");
        Check(back.Detail == "A-7", $"the message came back as {back.Detail}");

        // And again presenting a client certificate. This broker is configured
        // verify_none, so it does not ask for one -- what is proved here is
        // that offering one is harmless, not that EXTERNAL auth happened. The
        // other half of that is ssl_options.verify = verify_peer on the broker,
        // which is left off because a broker demanding a certificate nobody
        // configured refuses every connection with an error naming none of it.
        using (var withClientCertificate = await AceMqConnection.ConnectAsync(
                   ConnectionConfig.ForUrl(TlsUrl())
                       .Tls(TlsOptions.Required()
                           .TrustCertificateAuthority(ca)
                           .WithClientCertificate(clientPfx, password)
                           .AllowDevelopmentCertificates()
                           .WithoutRevocationChecking())
                       .ClientName("examples/04-development-certificates-csharp/client-cert").Build(),
                   new JsonCodec(),
                   token))
        {
            await withClientCertificate.Publisher<Audit>("", Queue).SendAsync(new Audit { Detail = "A-8" });
            Console.WriteLine("and again presenting a client certificate");
        }

        // ---- every way of getting this wrong, refused ----------------------
        Console.WriteLine();

        var withoutFlag = await Refused("without AllowDevelopmentCertificates",
            TlsOptions.Required().TrustCertificateAuthority(ca).WithoutRevocationChecking(), token);
        var underInsecure = await Refused("even under Insecure",
            TlsOptions.Insecure(), token);
        var revocation = await Refused("with revocation checking left on",
            TlsOptions.Required().TrustCertificateAuthority(ca).AllowDevelopmentCertificates(), token);
        var otherCa = await Refused("trusting a different authority",
            TlsOptions.Required().TrustCertificateAuthority(SomebodyElse())
                .AllowDevelopmentCertificates().WithoutRevocationChecking(), token);
        var noCa = await Refused("system trust only",
            TlsOptions.Required().AllowDevelopmentCertificates().WithoutRevocationChecking(), token);

        // Insecure() encrypts and verifies nothing -- it accepts any
        // certificate from anybody. It still refuses this one, and that is not
        // a courtesy. A development authority that could reach production would
        // be an authority anybody who can read the repository can issue
        // against, and the connection would succeed. The marker closes that on
        // every path rather than on the paths somebody remembered.
        Check(underInsecure != null,
            "a development certificate was accepted under Insecure, which is the whole point");
        Check(withoutFlag != null, "a development certificate was accepted without the flag");
        Check(revocation != null,
            "revocation checking against an authority that publishes no CRL somehow succeeded");
        Check(otherCa != null, "a certificate from a different authority was accepted");
        Check(noCa != null, "a privately issued certificate was accepted by the system trust store");

        // All five arrive as the same thing, which is worth knowing before
        // trying to tell them apart in a log. .NET reports every rejection from
        // a validation callback as one AuthenticationException, and the reason
        // -- marker, wrong authority, unknown authority, no revocation answer
        // -- stays inside the callback. Unlike Go, the message does not name
        // the marker, so "which of these is it?" is answered by removing one
        // setting at a time rather than by reading the error.
        foreach (var refusal in new[] { withoutFlag, underInsecure, revocation, otherCa, noCa })
        {
            Check(Innermost(refusal!) is System.Security.Authentication.AuthenticationException,
                $"a TLS refusal arrived as {Innermost(refusal!).GetType().Name}");
        }
        Console.WriteLine($"  all five refusals arrive as {Innermost(withoutFlag!).GetType().Name}: " +
                          Innermost(withoutFlag!).Message);

        // ---- and the mistakes that are caught before any socket opens -------
        Console.WriteLine();

        // THE ANSWER TO "WHAT IS THE TRUST STORE HERE". A directory is what the
        // Java library wants and it is refused outright by this one.
        var directory = Misconfigured("a directory instead of a file",
            () => TlsOptions.Required().TrustCertificateAuthority(certificates));
        Check(directory!.Message.Contains("no certificate authority file"),
            $"a directory was refused for the wrong reason: {directory.Message}");

        // And a PKCS#12 is not an authority file either, although it is exactly
        // what the Java truststore holds.
        Check(Misconfigured("a PKCS#12 as the authority",
            () => TlsOptions.Required().TrustCertificateAuthority(clientPfx)) != null,
            "a PKCS#12 was accepted as a certificate authority");

        // A client certificate needs its private key, so a .crt is refused with
        // a message that says what to load instead. Without this the failure is
        // a bare connection reset from the broker.
        var bareCrt = Misconfigured("a .crt as the client certificate",
            () => TlsOptions.Required().WithClientCertificate(
                Path.Combine(certificates, "server.crt"), null));
        Check(bareCrt!.Message.Contains(".pfx"),
            $"the refusal does not say what to load instead: {bareCrt.Message}");

        // TLS settings against a plaintext URL are refused rather than ignored.
        // A service handed a certificate authority, connecting in plaintext and
        // reporting success is the failure this refusal exists to prevent.
        var plaintext = Misconfigured("TLS settings on an amqp:// URL",
            () => ConnectionConfig.ForUrl(PlainUrl()).Tls(trusting).Build());
        Check(plaintext!.Message.Contains("amqps://"),
            $"the amqp:// refusal is about something else: {plaintext.Message}");

        // ---- one file, one certificate --------------------------------------
        //
        // The last thing "trust store" might have meant: a bundle. It does not.
        // TrustCertificateAuthority reads a single X509Certificate2 from the
        // path, which is the FIRST certificate in a PEM -- so a bundle with the
        // right authority second is a connection that fails for a reason
        // nothing in it explains. Two files, differing only in order.
        var bundle = Path.Combine(Path.GetTempPath(), "acemq-dev-certs-csharp");
        Directory.CreateDirectory(bundle);
        using var stranger = SomebodyElse();
        var caPem = File.ReadAllText(ca);
        var strangerPem = new string(PemEncoding.Write("CERTIFICATE", stranger.RawData)) + "\n";

        var caFirst = Path.Combine(bundle, "ca-first.pem");
        var caSecond = Path.Combine(bundle, "ca-second.pem");
        File.WriteAllText(caFirst, caPem + strangerPem);
        File.WriteAllText(caSecond, strangerPem + caPem);

        var first = await Refused("a bundle with our authority first", Bundle(caFirst), token);
        var second = await Refused("a bundle with our authority second", Bundle(caSecond), token);
        Check(first == null, "a bundle whose first certificate is our authority was refused");
        Check(second != null,
            "a bundle whose SECOND certificate is our authority was accepted, so more than one is read");

        Directory.Delete(bundle, true);

        await mq.DeleteQueueAsync(Queue);
        await mq.DeleteQueueAsync(Queue + ".dlq");
        await mq.DeleteQueueAsync(Queue + ".parked");

        return 0;

        TlsOptions Bundle(string path) =>
            TlsOptions.Required().TrustCertificateAuthority(path)
                .AllowDevelopmentCertificates().WithoutRevocationChecking();
    }

    // Returns the exception, or null if the connection succeeded. Printing both
    // outcomes and asserting them afterwards keeps the output readable and the
    // failure specific.
    private static async Task<Exception?> Refused(string what, TlsOptions tls, CancellationToken token)
    {
        try
        {
            using var mq = await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(TlsUrl()).Tls(tls).ClientName("examples/04-refusals").Build(),
                new JsonCodec(),
                token);
            await mq.DeclareQueueAsync(Queue);
            Console.WriteLine($"  CONNECTED  {what}");
            return null;
        }
        catch (Exception e)
        {
            Console.WriteLine($"  refused    {what}");
            return e;
        }
    }

    private static SecurityConfigurationException? Misconfigured(string what, Action attempt)
    {
        try
        {
            attempt();
            Console.WriteLine($"  ACCEPTED   {what}");
            return null;
        }
        catch (SecurityConfigurationException e)
        {
            Console.WriteLine($"  refused    {what}: {e.Message}");
            return e;
        }
    }

    private static Exception Innermost(Exception e)
    {
        while (e.InnerException != null) e = e.InnerException;
        return e;
    }

    // An authority this broker's certificate has nothing to do with. Trusting
    // one authority has to mean trusting THAT authority: a chain that is merely
    // self-consistent is what "turn verification off" already gives you.
    private static X509Certificate2 SomebodyElse()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(
            $"CN=Some Other CA, O={TlsOptions.DevelopmentMarker}",
            key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
    }

    // An example that prints the right answer whatever happened is an example
    // that cannot fail, and CI running it proves nothing. This throws, which
    // makes the process exit non-zero.
    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    private static string TlsUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_TLS_URL") is { Length: > 0 } url
            ? url
            : "amqps://guest:guest@localhost:5671/";

    private static string PlainUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
