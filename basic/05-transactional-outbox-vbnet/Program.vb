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

' The transactional outbox, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/05-transactional-outbox-vbnet
'
' The problem it solves is the one nobody notices until it happens: a service
' writes a row and publishes a message, and the process dies between the two.
' Either the row exists and nothing was announced, or the announcement went out
' about something that was rolled back. No ordering of the two calls fixes it,
' because they are two systems.
'
' The outbox makes it one system. The message is written in the same transaction
' as the row, and a relay publishes it afterwards — so the message exists if and
' only if the row does.

Imports System.Collections.Generic
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class OrderPlaced
    Public Property OrderId As String = ""
    Public Property TotalCents As Long
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
                    .ClientName("examples/05-transactional-outbox-vbnet") _
                    .Build())

            Try
                Await mq.DeclareQueueAsync("outbox-orders-vb")

                Dim arrived As New List(Of String)()
                Dim both As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using consumer = Await mq.ConsumeAsync(Of OrderPlaced)(
                    "outbox-orders-vb",
                    Function(message)
                        SyncLock arrived
                            arrived.Add(message.Payload.OrderId)
                            If arrived.Count = 2 Then both.TrySetResult(True)
                        End SyncLock
                        Console.WriteLine($"  consumed {message.Payload.OrderId}")
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    ' In a real service this is DbOutboxStore, over the same
                    ' database connection — and the same transaction — as the
                    ' business tables. In memory it shares the process's lifetime,
                    ' so it is not an outbox at all: it is lost on exactly the
                    ' restart the pattern exists to survive.
                    Dim outbox As New InMemoryOutboxStore()

                    ' ---- the transaction ------------------------------------
                    '
                    ' Pretend a BEGIN here, the order row written, these records
                    ' added, and a COMMIT. Nothing has been published and nothing
                    ' needs to be: the records are as durable as the row they were
                    ' written beside.
                    '
                    ' For encodes with the connection's codec, which is the codec
                    ' the consumer will read with.
                    Await outbox.AddAsync(OutboxRecord.For(
                        mq, "", "outbox-orders-vb",
                        New OrderPlaced With {.OrderId = "o-1", .TotalCents = 1999}))
                    Await outbox.AddAsync(OutboxRecord.For(
                        mq, "", "outbox-orders-vb",
                        New OrderPlaced With {.OrderId = "o-2", .TotalCents = 4500}))

                    Console.WriteLine(
                        $"{Await outbox.PendingCountAsync()} record(s) committed, nothing published yet")

                    ' ---- the relay ------------------------------------------
                    '
                    ' A background job that publishes what was written down.
                    ' Start() polls; DrainOnceAsync is that same work done once,
                    ' which is what makes the pattern testable without waiting on
                    ' a timer.
                    Using relay As New OutboxRelay(mq, outbox)
                        Dim moved = Await relay.DrainOnceAsync()
                        Console.WriteLine($"the relay published {moved} record(s)")

                        Await both.Task.WaitAsync(token)
                        Console.WriteLine(
                            $"{Await outbox.PendingCountAsync()} record(s) left in the outbox")

                        ' Draining again publishes nothing: a record is marked
                        ' published once the broker has confirmed it.
                        '
                        ' Marked *after*, deliberately. A relay that died in
                        ' between would publish the message twice, which is why
                        ' this is at-least-once and why consumers of anything sent
                        ' this way have to tolerate duplicates — the envelope's id
                        ' is the idempotency key for that.
                        Console.WriteLine(
                            $"a second drain moved {Await relay.DrainOnceAsync()} record(s)")
                    End Using
                End Using

                ' The example cleans up after itself so a second run reports the
                ' same numbers as the first. A service would leave the queue alone.
                Await mq.DeleteQueueAsync("outbox-orders-vb")
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
