' Copyright 2026 AceMQ.
'
' Licensed under the Apache License, Version 2.0 (the "License");
' you may not use this file except in compliance with the License.
' You may obtain a copy of the License at
'
'     https://www.apache.org/licenses/LICENSE-2.0
'
' Unless required by applicable law or agreed to in writing, software
' distributed under the License is distributed on an "AS IS" BASIS,
' WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
' See the License for the specific language governing permissions and
' limitations under the License.

' A TLS broker on a laptop, and the reason its certificates cannot reach
' production. In VB.NET.
'
' This is the one example that needs a broker of its own, because it needs a TLS
' listener holding certificates generated on this machine:
'
'   curl -O https://acemq.org/nuget/v3/flatcontainer/acemq.amqp.devcerts/0.7.2/acemq.amqp.devcerts.0.7.2.nupkg
'   dotnet tool install --global AceMq.Amqp.DevCerts --version 0.7.2 --source .
'   acemq-certs --out certs --broker localhost
'   chmod 644 certs/server.key
'   docker compose --profile tls up -d
'   dotnet run --project advanced/04-development-certificates-vbnet
'
' Every developer who has needed TLS locally has generated a self-signed
' certificate and then turned verification off to use it. The certificate works,
' so it stays; the flag that made it work stays too; and the two travel together
' into an environment where that flag hands every message and every password to
' whoever answers on the address in the URL.
'
' So everything acemq-certs writes is stamped ACEMQ DEVELOPMENT ONLY - DO NOT
' TRUST in its subject organisation, and this library refuses any chain carrying
' it, however trust is configured -- including under Insecure(). The way through
' is AllowDevelopmentCertificates(): a named method a reviewer sees in a diff and
' an operator can grep a deployment for.
'
' WHAT .NET WANTS FOR A TRUST STORE IS NOT WHAT THE OTHER LANGUAGES WANT, and the
' difference is asserted below rather than described. The Java library takes a
' *directory* holding keystore.p12 and truststore.p12. This one takes a single
' PEM or DER file holding exactly one certificate: a directory is refused
' outright, and a PEM bundle contributes only its first certificate. "Trust
' store" is the wrong word here -- it is one authority.

Imports System.Collections.Generic
Imports System.IO
Imports System.Linq
Imports System.Security.Cryptography
Imports System.Security.Cryptography.X509Certificates
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Audit
    ' Not named "Event", which is a VB.NET keyword. The C# twin of this example
    ' calls it the same thing so the two publish the same shape.
    Public Property Detail As String = ""
End Class

Module Program

    ' Distinct from the C# example's name: both run against one broker.
    Private Const QueueName As String = "dev-certs-vbnet"

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(120))
            Dim token = cancellation.Token

            ' Named "certificates" rather than "directory": VB.NET is
            ' case-insensitive, and a variable called directory shadows
            ' System.IO.Directory for the rest of the method.
            Dim certificates = Environment.GetEnvironmentVariable("ACEMQ_CERTS")
            If String.IsNullOrEmpty(certificates) Then certificates = "certs"
            certificates = Path.GetFullPath(certificates)

            Dim password = Environment.GetEnvironmentVariable("ACEMQ_CERT_PASSWORD")
            If String.IsNullOrEmpty(password) Then password = "acemq-dev"

            Check(Directory.Exists(certificates),
                  $"no certificates at {certificates}. Generate them first -- the commands are " &
                  "at the top of this file, and in this directory's README.")

            Dim ca = Path.Combine(certificates, "ca.crt")
            Dim clientPfx = Path.Combine(certificates, "client.pfx")

            ' ---- what the generator wrote ------------------------------------
            Dim written = Directory.GetFiles(certificates) _
                .Select(Function(f) Path.GetFileName(f)).OrderBy(Function(n) n).ToList()
            Console.WriteLine($"acemq-certs wrote {written.Count} files into {certificates}")
            For Each name In written
                Console.WriteLine($"  {name}")
            Next

            ' Five, not the seven Go and Python write. The difference is the
            ' point rather than an oversight: .NET keeps the client's certificate
            ' and key together in one PKCS#12, because that is what
            ' X509Certificate2 loads and a separate .crt/.key pair cannot
            ' complete a handshake here.
            Check(written.SequenceEqual(New String() {
                      "ca.crt", "client.pfx", "rabbitmq.conf", "server.crt", "server.key"}),
                  $"the generator wrote {String.Join(" ", written)}")

            ' AND NO ca.key. The authority's private key is never written to
            ' disk, so this authority cannot issue anything after the moment it
            ' was generated -- which is the single largest difference between a
            ' generated development CA and a liability. Go writes ca.key and a
            ' checked-in copy of it is an authority anybody with the repository
            ' can issue against.
            Check(Not written.Contains("ca.key"),
                  "the authority's private key was written to disk, " &
                  "which makes it an authority anybody can use")

            ' A private key readable by every account on the machine is a bad
            ' habit to teach even in development. server.key is exempt because
            ' the documented chmod 644 above deliberately relaxes it -- RabbitMQ
            ' runs as another user in the container and reports an unreadable key
            ' as a listener that simply failed to start.
            If Not OperatingSystem.IsWindows() Then
                Dim mode = File.GetUnixFileMode(clientPfx)
                Console.WriteLine($"client.pfx is {mode}")
                Check(mode = (UnixFileMode.UserRead Or UnixFileMode.UserWrite),
                      $"client.pfx is {mode}, not owner-only")
            End If

            ' ---- the marker is on every certificate, including the authority --
            '
            ' Which is what makes the refusal reach a server certificate that
            ' does not carry the marker itself but was issued by one that does.
            Using authority As New X509Certificate2(ca),
                  broker As New X509Certificate2(Path.Combine(certificates, "server.crt")),
                  client As New X509Certificate2(clientPfx, password)

                Console.WriteLine($"{vbLf}authority: {authority.Subject}")
                Console.WriteLine($"broker:    {broker.Subject}")
                Console.WriteLine($"client:    {client.Subject}")
                Console.WriteLine($"valid until {authority.NotAfter:yyyy-MM-dd}")

                Marked("authority", authority)
                Marked("broker", broker)
                Marked("client", client)

                Check(Not authority.HasPrivateKey, "ca.crt carries a private key, which it must not")
                Check(client.HasPrivateKey,
                      "client.pfx has no private key, so it cannot finish a handshake")
            End Using

            ' ---- connecting ---------------------------------------------------
            '
            ' WithoutRevocationChecking is not optional here, and it is the step
            ' that is easiest to leave out. Required() turns revocation checking
            ' on by default -- correct, because a revoked certificate is one known
            ' to be in the wrong hands. A generated development authority
            ' publishes no CRL and runs no responder, so the check cannot come
            ' back at all and the chain is rejected. The refusal below asserts
            ' exactly that, so nobody has to discover it from a handshake error.
            Dim trusting = TlsOptions.Required() _
                .TrustCertificateAuthority(ca) _
                .AllowDevelopmentCertificates() _
                .WithoutRevocationChecking()

            Console.WriteLine($"{vbLf}{trusting}")

            Dim received As New TaskCompletionSource(Of Audit)(
                TaskCreationOptions.RunContinuationsAsynchronously)

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(TlsUrl()).Tls(trusting) _
                    .ClientName("examples/04-development-certificates-vbnet").Build(),
                New JsonCodec(),
                token)

            Try
                Await mq.DeclareQueueAsync(QueueName)

                Using consumer = Await mq.ConsumeAsync(Of Audit)(
                    QueueName,
                    Function(message)
                        received.TrySetResult(message.Payload)
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Await mq.Publisher(Of Audit)("", QueueName).SendAsync(
                        New Audit With {.Detail = "A-7"})
                    Dim back = Await received.Task.WaitAsync(token)
                    Console.WriteLine($"published and consumed {back.Detail} over TLS")
                    Check(back.Detail = "A-7", $"the message came back as {back.Detail}")
                End Using

                ' And again presenting a client certificate. This broker is
                ' configured verify_none, so it does not ask for one -- what is
                ' proved here is that offering one is harmless, not that EXTERNAL
                ' auth happened. The other half of that is ssl_options.verify =
                ' verify_peer on the broker, which is left off because a broker
                ' demanding a certificate nobody configured refuses every
                ' connection with an error naming none of it.
                Dim presenting = Await AceMqConnection.ConnectAsync(
                    ConnectionConfig.ForUrl(TlsUrl()) _
                        .Tls(TlsOptions.Required() _
                            .TrustCertificateAuthority(ca) _
                            .WithClientCertificate(clientPfx, password) _
                            .AllowDevelopmentCertificates() _
                            .WithoutRevocationChecking()) _
                        .ClientName("examples/04-development-certificates-vbnet/client-cert").Build(),
                    New JsonCodec(),
                    token)
                Try
                    Await presenting.Publisher(Of Audit)("", QueueName).SendAsync(
                        New Audit With {.Detail = "A-8"})
                    Console.WriteLine("and again presenting a client certificate")
                Finally
                    presenting.Dispose()
                End Try

                ' ---- every way of getting this wrong, refused ----------------
                Console.WriteLine()

                Dim withoutFlag = Await Refused("without AllowDevelopmentCertificates",
                    TlsOptions.Required().TrustCertificateAuthority(ca).WithoutRevocationChecking(),
                    token)
                Dim underInsecure = Await Refused("even under Insecure",
                    TlsOptions.Insecure(), token)
                Dim revocation = Await Refused("with revocation checking left on",
                    TlsOptions.Required().TrustCertificateAuthority(ca).AllowDevelopmentCertificates(),
                    token)
                Dim otherCa = Await Refused("trusting a different authority",
                    TlsOptions.Required().TrustCertificateAuthority(SomebodyElse()) _
                        .AllowDevelopmentCertificates().WithoutRevocationChecking(), token)
                Dim noCa = Await Refused("system trust only",
                    TlsOptions.Required().AllowDevelopmentCertificates().WithoutRevocationChecking(),
                    token)

                ' Insecure() encrypts and verifies nothing -- it accepts any
                ' certificate from anybody. It still refuses this one, and that
                ' is not a courtesy. A development authority that could reach
                ' production would be an authority anybody who can read the
                ' repository can issue against, and the connection would succeed.
                ' The marker closes that on every path rather than on the paths
                ' somebody remembered.
                Check(underInsecure IsNot Nothing,
                      "a development certificate was accepted under Insecure, " &
                      "which is the whole point")
                Check(withoutFlag IsNot Nothing,
                      "a development certificate was accepted without the flag")
                Check(revocation IsNot Nothing,
                      "revocation checking against an authority that publishes no CRL somehow succeeded")
                Check(otherCa IsNot Nothing,
                      "a certificate from a different authority was accepted")
                Check(noCa IsNot Nothing,
                      "a privately issued certificate was accepted by the system trust store")

                ' All five arrive as the same thing, which is worth knowing
                ' before trying to tell them apart in a log. .NET reports every
                ' rejection from a validation callback as one
                ' AuthenticationException, and the reason -- marker, wrong
                ' authority, unknown authority, no revocation answer -- stays
                ' inside the callback. Unlike Go, the message does not name the
                ' marker, so "which of these is it?" is answered by removing one
                ' setting at a time rather than by reading the error.
                For Each refusal In New Exception() {withoutFlag, underInsecure, revocation, otherCa, noCa}
                    Check(TypeOf Innermost(refusal) Is
                              System.Security.Authentication.AuthenticationException,
                          $"a TLS refusal arrived as {Innermost(refusal).GetType().Name}")
                Next
                Console.WriteLine(
                    $"  all five refusals arrive as {Innermost(withoutFlag).GetType().Name}: " &
                    Innermost(withoutFlag).Message)

                ' ---- and the mistakes caught before any socket opens ----------
                Console.WriteLine()

                ' THE ANSWER TO "WHAT IS THE TRUST STORE HERE". A directory is
                ' what the Java library wants and it is refused outright by this
                ' one.
                Dim asDirectory = Misconfigured("a directory instead of a file",
                    Sub() TlsOptions.Required().TrustCertificateAuthority(certificates))
                Check(asDirectory IsNot Nothing AndAlso
                      asDirectory.Message.Contains("no certificate authority file"),
                      "a directory was refused for the wrong reason")

                ' And a PKCS#12 is not an authority file either, although it is
                ' exactly what the Java truststore holds.
                Check(Misconfigured("a PKCS#12 as the authority",
                          Sub() TlsOptions.Required().TrustCertificateAuthority(clientPfx)) IsNot Nothing,
                      "a PKCS#12 was accepted as a certificate authority")

                ' A client certificate needs its private key, so a .crt is
                ' refused with a message that says what to load instead. Without
                ' this the failure is a bare connection reset from the broker.
                Dim bareCrt = Misconfigured("a .crt as the client certificate",
                    Sub() TlsOptions.Required().WithClientCertificate(
                        Path.Combine(certificates, "server.crt"), Nothing))
                Check(bareCrt IsNot Nothing AndAlso bareCrt.Message.Contains(".pfx"),
                      "the refusal does not say what to load instead")

                ' TLS settings against a plaintext URL are refused rather than
                ' ignored. A service handed a certificate authority, connecting
                ' in plaintext and reporting success is the failure this refusal
                ' exists to prevent.
                Dim plaintext = Misconfigured("TLS settings on an amqp:// URL",
                    Sub() ConnectionConfig.ForUrl(PlainUrl()).Tls(trusting).Build())
                Check(plaintext IsNot Nothing AndAlso plaintext.Message.Contains("amqps://"),
                      "the amqp:// refusal is about something else")

                ' ---- one file, one certificate ---------------------------------
                '
                ' The last thing "trust store" might have meant: a bundle. It
                ' does not. TrustCertificateAuthority reads a single
                ' X509Certificate2 from the path, which is the FIRST certificate
                ' in a PEM -- so a bundle with the right authority second is a
                ' connection that fails for a reason nothing in it explains. Two
                ' files, differing only in order.
                ' Named "scratch" rather than "bundle": VB.NET is
                ' case-insensitive, so a String called bundle turns the call
                ' Bundle(caFirst) below into an attempt to index that string,
                ' and the error is about converting String to Integer.
                Dim scratch = Path.Combine(Path.GetTempPath(), "acemq-dev-certs-vbnet")
                Directory.CreateDirectory(scratch)

                Dim caFirst = Path.Combine(scratch, "ca-first.pem")
                Dim caSecond = Path.Combine(scratch, "ca-second.pem")
                Using stranger = SomebodyElse()
                    Dim caPem = File.ReadAllText(ca)
                    Dim strangerPem = New String(PemEncoding.Write("CERTIFICATE", stranger.RawData)) & vbLf
                    File.WriteAllText(caFirst, caPem & strangerPem)
                    File.WriteAllText(caSecond, strangerPem & caPem)
                End Using

                Dim first = Await Refused("a bundle with our authority first", Bundle(caFirst), token)
                Dim second = Await Refused("a bundle with our authority second", Bundle(caSecond), token)
                Check(first Is Nothing,
                      "a bundle whose first certificate is our authority was refused")
                Check(second IsNot Nothing,
                      "a bundle whose SECOND certificate is our authority was accepted, " &
                      "so more than one is read")

                Directory.Delete(scratch, True)

                Await mq.DeleteQueueAsync(QueueName)
                Await mq.DeleteQueueAsync(QueueName & ".dlq")
                Await mq.DeleteQueueAsync(QueueName & ".parked")
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Function Bundle(path As String) As TlsOptions
        Return TlsOptions.Required().TrustCertificateAuthority(path) _
            .AllowDevelopmentCertificates().WithoutRevocationChecking()
    End Function

    Private Sub Marked(what As String, certificate As X509Certificate2)
        Check(certificate.Subject.IndexOf(
                  TlsOptions.DevelopmentMarker, StringComparison.OrdinalIgnoreCase) >= 0,
              $"the {what} certificate does not carry the marker: {certificate.Subject}")
    End Sub

    ' Returns the exception, or Nothing if the connection succeeded. Printing
    ' both outcomes and asserting them afterwards keeps the output readable and
    ' the failure specific.
    Private Async Function Refused(what As String, tls As TlsOptions,
                                   token As CancellationToken) As Task(Of Exception)
        Try
            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(TlsUrl()).Tls(tls).ClientName("examples/04-refusals").Build(),
                New JsonCodec(),
                token)
            Try
                Await mq.DeclareQueueAsync(QueueName)
                Console.WriteLine($"  CONNECTED  {what}")
                Return Nothing
            Finally
                mq.Dispose()
            End Try
        Catch e As Exception
            Console.WriteLine($"  refused    {what}")
            Return e
        End Try
    End Function

    Private Function Misconfigured(what As String, attempt As Action) As SecurityConfigurationException
        Try
            attempt()
            Console.WriteLine($"  ACCEPTED   {what}")
            Return Nothing
        Catch e As SecurityConfigurationException
            Console.WriteLine($"  refused    {what}: {e.Message}")
            Return e
        End Try
    End Function

    Private Function Innermost(e As Exception) As Exception
        Dim deepest = e
        Do While deepest.InnerException IsNot Nothing
            deepest = deepest.InnerException
        Loop
        Return deepest
    End Function

    ' An authority this broker's certificate has nothing to do with. Trusting one
    ' authority has to mean trusting THAT authority: a chain that is merely
    ' self-consistent is what "turn verification off" already gives you.
    Private Function SomebodyElse() As X509Certificate2
        Using key = RSA.Create(2048)
            Dim request As New CertificateRequest(
                $"CN=Some Other CA, O={TlsOptions.DevelopmentMarker}",
                key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
            request.CertificateExtensions.Add(
                New X509BasicConstraintsExtension(True, False, 0, True))
            Return request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30))
        End Using
    End Function

    ' An example that prints the right answer whatever happened is an example
    ' that cannot fail, and CI running it proves nothing. This throws, which
    ' makes the process exit non-zero.
    Private Sub Check(held As Boolean, wrong As String)
        If Not held Then Throw New InvalidOperationException(wrong)
    End Sub

    Private Function TlsUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_TLS_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqps://guest:guest@localhost:5671/"
        End If
        Return url
    End Function

    Private Function PlainUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
