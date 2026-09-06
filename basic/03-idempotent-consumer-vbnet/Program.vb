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

' Handling a message once, even when it arrives twice, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/03-idempotent-consumer-vbnet

Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Charge
    Public Property ChargeId As String = ""
    Public Property Cents As Long
End Class

Module Program

    Private _charged As Integer = 0
    Private _deliveries As Integer = 0

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(45))
            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/03-idempotent-consumer-vbnet") _
                    .Build())

            Try
                Await mq.DeclareQueueAsync("charges")

                ' In this process only. Two workers would each have their own
                ' memory and both would believe they were first.
                Dim store As New InMemoryIdempotencyStore(TimeSpan.FromHours(1))

                Dim options = ConsumerOptions.Defaults() _
                    .WithRetry(RetryPolicy.Fixed(5, TimeSpan.FromMilliseconds(200)).WithJitter(0)) _
                    .Idempotent(store)

                Using consumer = Await mq.ConsumeAsync(Of Charge)(
                    "charges", options,
                    Function(message)
                        Interlocked.Increment(_deliveries)

                        If Volatile.Read(_deliveries) = 1 Then
                            Console.WriteLine("delivery 1: failing on purpose")
                            Return Task.FromResult(
                                Ack.Retry(TimeSpan.Zero, "the card processor timed out"))
                        End If

                        Interlocked.Increment(_charged)
                        Console.WriteLine(
                            $"charging {message.Payload.ChargeId} for {message.Payload.Cents} cents")
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim publisher = mq.Publisher(Of Charge)("", "charges")
                    Dim charge As New Charge With {.ChargeId = "c-1", .Cents = 4250}

                    For i = 0 To 2
                        Await publisher.SendAsync(charge, Envelope.Of("charge").Id("charge-c-1").Build())
                    Next

                    Await Task.Delay(TimeSpan.FromSeconds(3), cancellation.Token)

                    Console.WriteLine(
                        $"delivered {Volatile.Read(_deliveries)} times, " &
                        $"charged {Volatile.Read(_charged)} time(s)")
                End Using
            Finally
                mq.Dispose()
            End Try

            If Volatile.Read(_charged) = 1 Then
                Return 0
            End If
            Return 1
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
