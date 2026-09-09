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

' Retrying a message, and giving up on it, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/02-retries-and-dead-letters-vbnet
'
' The attempt counter is the thing to watch. A broker requeues the bytes it was
' given, so the header on the wire still says 1 however many times the message
' has come back — the count comes from the redelivery flag instead.

Imports System.Collections.Generic
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Payment
    Public Property PaymentId As String = ""
End Class

Module Program

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/02-retries-and-dead-letters-vbnet") _
                    .Build())

            Try
                ' The name is not a choice: when a retry policy runs out of
                ' attempts the library republishes the message to {queue}.dlq
                ' and acknowledges the original, rather than nacking it and
                ' leaving the route to the broker. A consumer declares that
                ' queue when it starts, so nothing here has to.
                Await mq.DeclareQueueAsync("payments")
                Const deadLetters As String = "payments.dlq"

                Dim attempts As New List(Of Integer)
                Dim dead As New TaskCompletionSource(Of IMessage(Of Payment))(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Dim options = ConsumerOptions.Defaults() _
                    .WithRetry(RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(300)).WithJitter(0))

                Using consumer = Await mq.ConsumeAsync(Of Payment)(
                    "payments", options,
                    Function(message)
                        ' message.Attempt, not message.Envelope.Attempt: the
                        ' envelope carries what the publisher wrote, and a
                        ' broker redelivers the original bytes.
                        SyncLock attempts
                            attempts.Add(message.Attempt)
                        End SyncLock
                        Console.WriteLine(
                            $"handling {message.Payload.PaymentId}, attempt {message.Attempt}")
                        Return Task.FromResult(
                            Ack.Retry(TimeSpan.Zero, "the payment gateway is not answering"))
                    End Function)

                    Using deadConsumer = Await mq.ConsumeAsync(Of Payment)(
                        deadLetters,
                        Function(message)
                            dead.TrySetResult(message)
                            Return Task.FromResult(Ack.Accept())
                        End Function)

                        Dim payment As New Payment With {.PaymentId = "p-1"}
                        Await mq.Publisher(Of Payment)("", "payments").SendAsync(payment)

                        Dim deadLettered = Await dead.Task.WaitAsync(token)
                        ' message.Attempt, not message.Envelope.Attempt: the
                        ' envelope carries what the publisher wrote, and a
                        ' broker redelivers the original bytes.
                        SyncLock attempts
                            Console.WriteLine(
                                $"dead-lettered after attempts [{String.Join(", ", attempts)}]")
                        End SyncLock
                        Console.WriteLine($"the envelope kept its history: id={deadLettered.Envelope.Id}")
                    End Using
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
