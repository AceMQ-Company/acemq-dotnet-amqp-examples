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

' Encrypting the message body, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project advanced/01-encrypting-payloads-vbnet
'
' Headers are not encrypted: the envelope is how the library routes and retries.
' Do not put anything secret in a header.

Imports System.Text
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class CardPayment
    Public Property PaymentId As String = ""
    Public Property Pan As String = ""
End Class

Module Program

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(30))
            ' Rotation needs an overlap: add the new key everywhere first, then
            ' make it current.
            Dim lastYear = EncryptionKey.Generate("2025-06")
            Dim current = EncryptionKey.Generate("2026-01")
            ' Named "ring" rather than "keyring": VB.NET is case-insensitive,
            ' so a variable called keyring collides with the Keyring type and
            ' the compiler reports it as a type it cannot infer.
            Dim ring = Keyring.Builder().Add(lastYear).Current(current).Build()

            Dim codec = EncryptedCodec.Wrapping(New JsonCodec(), ring)

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/01-encrypting-payloads-vbnet") _
                    .Build(),
                codec,
                cancellation.Token)

            Try
                Await mq.DeclareQueueAsync("payments")

                Dim arrived As New TaskCompletionSource(Of IMessage(Of CardPayment))(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using consumer = Await mq.ConsumeAsync(Of CardPayment)(
                    "payments",
                    Function(message)
                        arrived.TrySetResult(message)
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim payment As New CardPayment With {.PaymentId = "p-1", .Pan = "4111111111111111"}
                    Await mq.Publisher(Of CardPayment)("", "payments").SendAsync(payment)

                    Dim received = Await arrived.Task.WaitAsync(cancellation.Token)
                    Console.WriteLine($"the consumer read {received.Payload.PaymentId} / {received.Payload.Pan}")

                    Dim onTheWire = codec.Encode(payment)
                    Dim asText = Encoding.UTF8.GetString(onTheWire)
                    Console.WriteLine($"the body is {onTheWire.Length} bytes of ciphertext")
                    Console.WriteLine($"  contains the card number: {asText.Contains("4111111111111111")}")
                    Console.WriteLine($"  names the key that opens it: {EncryptedCodec.KeyIdOf(onTheWire)}")
                End Using
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
