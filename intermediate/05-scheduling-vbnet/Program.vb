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

' Delivering a message later, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/05-scheduling-vbnet
'
' Three reminders: one already overdue, one in three seconds, one in five. No
' scheduler process, no cron, and no broker plugin. It takes about five seconds,
' because it is waiting for real delays.
'
' Read the two columns of the table it prints together. A three-second delay
' lands at about two, and that is the design rather than a defect -- see the
' note further down.

Imports System.Collections.Generic
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Reminder
    Public Property ReminderId As String = ""
End Class

Public Class Arrival
    Public Property ReminderId As String = ""
    Public Property After As TimeSpan
    Public Property ContentType As String = Nothing
End Class

Module Program

    ' Named "Destination" and "DueQueue" rather than "Exchange" and "Queue":
    ' VB.NET is case-insensitive, and Queue would collide with
    ' System.Collections.Generic.Queue(Of T).
    '
    ' Distinct from the C# example's names for the usual reason: both run
    ' against one broker on CI.
    Private Const Destination As String = "scheduling-reminders-vb"
    Private Const DueQueue As String = "scheduling-reminders-vb-due"

    Private ReadOnly Arrivals As New List(Of Arrival)

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(120))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/05-scheduling-vbnet") _
                    .Build())

            Try
                Await mq.DeclareExchangeAsync(Destination, "direct")
                Await mq.DeclareQueueAsync(DueQueue)
                Await mq.BindAsync(DueQueue, Destination, "reminder.due")

                Dim all As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)
                Dim started = DateTimeOffset.UtcNow

                Using consumer = Await mq.ConsumeAsync(Of Reminder)(
                    DueQueue,
                    Function(message)
                        ' message.ContentType is worth recording. The scheduler
                        ' republishes bytes rather than objects, so it carries
                        ' the content type the payload was encoded as and puts
                        ' it back on the message it finally delivers. Without
                        ' that, what arrives is the right bytes under
                        ' application/octet-stream and nothing can decode it.
                        SyncLock Arrivals
                            Arrivals.Add(New Arrival With {
                                .ReminderId = message.Payload.ReminderId,
                                .After = DateTimeOffset.UtcNow - started,
                                .ContentType = message.ContentType})
                            If Arrivals.Count = 3 Then all.TrySetResult(True)
                        End SyncLock
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    ' Starting a scheduler declares its exchange, its five rungs
                    ' and its control queue. They are shared, so a second
                    ' scheduler on the same broker declares the same seven
                    ' objects rather than a set of its own -- and every name and
                    ' argument is a wire contract shared with the Java, Go,
                    ' Python and Ruby libraries. A rung declared with a
                    ' different time to live is a PRECONDITION_FAILED for
                    ' whichever service starts second.
                    '
                    ' Named "later" rather than "scheduler": VB.NET is
                    ' case-insensitive, so a variable called scheduler collides
                    ' with the Scheduler type.
                    Using later = Await Scheduler.OnAsync(mq)
                        Console.WriteLine(
                            "rungs: " &
                            String.Join(", ", Scheduler.Rungs.Select(
                                Function(rung) Scheduler.RungName(rung))))

                        ' Already overdue. Anything in the past is delivered
                        ' immediately rather than refused, which is what makes a
                        ' schedule read out of a database after an outage safe
                        ' to replay.
                        Await later.AtAsync(started.AddSeconds(-5), Destination, "reminder.due",
                                            New Reminder With {.ReminderId = "R-0"})
                        Await later.InAsync(TimeSpan.FromSeconds(3), Destination, "reminder.due",
                                            New Reminder With {.ReminderId = "R-1"})
                        Await later.InAsync(TimeSpan.FromSeconds(5), Destination, "reminder.due",
                                            New Reminder With {.ReminderId = "R-2"})

                        Await all.Task.WaitAsync(token)

                        Console.WriteLine(
                            $"scheduled {later.Scheduled}, delivered {later.Delivered}, " &
                            $"hops {later.Hops}")
                        Console.WriteLine()
                        Console.WriteLine("asked for   arrived at")

                        Dim seen As List(Of Arrival)
                        SyncLock Arrivals
                            seen = Arrivals.OrderBy(Function(a) a.ReminderId).ToList()
                        End SyncLock

                        Dim asked As New Dictionary(Of String, String) From {
                            {"R-0", "past"}, {"R-1", "3s"}, {"R-2", "5s"}}
                        For Each arrival In seen
                            Console.WriteLine(
                                asked(arrival.ReminderId).PadLeft(9) & "   " &
                                arrival.After.TotalSeconds.ToString("F1").PadLeft(7) & "s   " &
                                arrival.ReminderId)
                        Next

                        Console.WriteLine()
                        Console.WriteLine($"content type on arrival: {seen(0).ContentType}")

                        Check(seen.Count = 3, $"{seen.Count} reminders arrived, not three")
                        Check(later.Delivered = 3,
                              $"the scheduler delivered {later.Delivered}, not three")
                        Check(later.Hops > 0, "nothing hopped, so nothing waited in the broker")
                        Check(seen.Select(Function(a) a.ReminderId).SequenceEqual(
                                  New String() {"R-0", "R-1", "R-2"}),
                              "the reminders that arrived were not the three that were scheduled")

                        ' R-0 was due already, so it takes no hops and arrives
                        ' about now.
                        Between(seen(0), TimeSpan.Zero, TimeSpan.FromSeconds(1.5))

                        ' The two real delays. The bands are wide on purpose:
                        ' this is the accuracy the design buys, and asserting to
                        ' the tenth of a second would be asserting that the
                        ' machine running CI is not busy.
                        Between(seen(1), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(4))
                        Between(seen(2), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(6.5))

                        Check(seen.All(Function(a) a.ContentType = "application/json"),
                              $"a reminder arrived as {seen(0).ContentType}, " &
                              "which no consumer would decode")

                        ' What the table is saying. A three-second delay lands at
                        ' about two, because a message is delivered as soon as
                        ' less than one second is left: another hop through the
                        ' smallest rung would cost more than the accuracy it
                        ' buys. Delivery is accurate to about the smallest rung,
                        ' and something that must fire at 09:00:00.000 wants a
                        ' scheduler rather than a message broker.
                        '
                        ' The hop count is the other number. A long delay is
                        ' several broker round trips rather than one -- a one-day
                        ' message is twenty-four hops through the one-hour rung
                        ' -- which is the honest cost of not requiring the
                        ' delayed-message-exchange plugin.
                        '
                        ' Why not simply set an expiration on the message and let
                        ' it dead-letter? Because a classic queue expires
                        ' messages only from its HEAD. Put a four-hour message in
                        ' and a one-minute message behind it, and the one-minute
                        ' message is delivered in four hours -- and nothing
                        ' reports it: the queue looks healthy and the message is
                        ' not lost, it is just late by a factor nobody predicted.
                        ' The ladder avoids that by giving every message in a
                        ' rung the same delay, so the head is always the message
                        ' due soonest.
                    End Using
                End Using

                ' The example cleans up its own destination. The scheduler's
                ' queues stay where they are: they are shared with every other
                ' service on this broker, and they may be holding somebody
                ' else's message.
                Await mq.DeleteQueueAsync(DueQueue)
                Await mq.DeleteQueueAsync(DueQueue & ".dlq")
                Await mq.DeleteQueueAsync(DueQueue & ".parked")
                Await mq.DeleteExchangeAsync(Destination)
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Sub Between(arrival As Arrival, lower As TimeSpan, upper As TimeSpan)
        Check(arrival.After >= lower AndAlso arrival.After <= upper,
              $"{arrival.ReminderId} arrived after {arrival.After.TotalSeconds:F1}s, " &
              $"outside {lower.TotalSeconds:F1}s..{upper.TotalSeconds:F1}s")
    End Sub

    ' An example that prints the right answer whatever happened is an example
    ' that cannot fail, and CI running it proves nothing. This throws, which
    ' makes the process exit non-zero.
    Private Sub Check(held As Boolean, wrong As String)
        If Not held Then Throw New InvalidOperationException(wrong)
    End Sub

    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
