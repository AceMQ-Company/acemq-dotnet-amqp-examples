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

' Putting dead-lettered messages back, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/04-replay-vbnet
'
' Dead-lettering is only half the story. The messages are still there, and the
' point of keeping them is that they get another run once whatever broke has
' been fixed. This is that second half.

Imports System.Collections.Generic
Imports System.Text
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Invoice
    Public Property InvoiceId As String = ""
    Public Property Tenant As String = ""
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
                    .ClientName("examples/04-replay-vbnet") _
                    .Build())

            Try
                Await mq.DeclareQueueAsync("replay-invoices-vb-dead")

                Dim deadLettering As New Dictionary(Of String, Object) From {
                    {"x-dead-letter-exchange", ""},
                    {"x-dead-letter-routing-key", "replay-invoices-vb-dead"}}
                Await mq.DeclareQueueAsync("replay-invoices-vb", QueueType.Classic, deadLettering)

                ' ---- something breaks ----------------------------------------
                '
                ' A downstream service is down, so every invoice is dead-lettered.
                ' Ack.DeadLetter is the handler saying this will not succeed by
                ' being tried again; Ack.Retry would spin it round the same broken
                ' handler.
                Dim brokenDeadline As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)
                Dim rejected = 0

                Dim broken = Await mq.ConsumeAsync(Of Invoice)(
                    "replay-invoices-vb",
                    Function(message)
                        If Interlocked.Increment(rejected) = 3 Then
                            brokenDeadline.TrySetResult(True)
                        End If
                        Return Task.FromResult(Ack.DeadLetter("the ledger service is down"))
                    End Function)

                Dim publisher = mq.Publisher(Of Invoice)("", "replay-invoices-vb")
                Await publisher.SendAsync(New Invoice With {.InvoiceId = "inv-1", .Tenant = "acme"})
                Await publisher.SendAsync(New Invoice With {.InvoiceId = "inv-2", .Tenant = "globex"})
                Await publisher.SendAsync(New Invoice With {.InvoiceId = "inv-3", .Tenant = "acme"})

                Await brokenDeadline.Task.WaitAsync(token)

                ' The broken consumer has to go before anything is replayed.
                ' Replaying into a queue somebody is still rejecting from would put
                ' the messages straight back where they came from.
                broken.Dispose()

                ' Named "again" rather than "replay": VB.NET is case-insensitive,
                ' so a variable called replay collides with the Replay type.
                Dim again = mq.Replay("replay-invoices-vb-dead").Into("replay-invoices-vb")
                Console.WriteLine($"{Await again.PendingAsync()} message(s) waiting on {again.From}")

                ' ---- the fix is deployed, but only for one tenant -------------
                '
                ' What the filter passes over is left where it was rather than
                ' discarded, so replaying selectively is not a way to lose
                ' messages.
                Dim handled As New List(Of String)()
                Dim acme As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using fixedConsumer = Await mq.ConsumeAsync(Of Invoice)(
                    "replay-invoices-vb",
                    Function(message)
                        SyncLock handled
                            handled.Add(message.Payload.InvoiceId)
                            If handled.Count = 2 Then acme.TrySetResult(True)
                        End SyncLock
                        Console.WriteLine($"  handled {message.Payload.InvoiceId} for {message.Payload.Tenant}")
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim replayed = Await again.ReplayAsync(10, AddressOf ForAcme)
                    Console.WriteLine($"replayed {replayed} message(s) for acme")

                    Await acme.Task.WaitAsync(token)
                End Using

                ' globex was passed over, and is still on the dead-letter queue
                ' rather than gone.
                Console.WriteLine(
                    $"{Await again.PendingAsync()} message(s) still waiting for the rest of the fix")

                ' The example cleans up after itself so a second run reports the
                ' same numbers as the first. A service would leave the queues
                ' alone.
                Await mq.DeleteQueueAsync("replay-invoices-vb")
                Await mq.DeleteQueueAsync("replay-invoices-vb-dead")
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    ' The filter sees the delivery rather than a decoded payload: the messages on
    ' a dead-letter queue are not guaranteed to be anything a codec can read,
    ' which is why it is handed the bytes.
    Private Function ForAcme(delivery As InboundDelivery) As Boolean
        Return Encoding.UTF8.GetString(delivery.Body).Contains("""acme""")
    End Function

    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
