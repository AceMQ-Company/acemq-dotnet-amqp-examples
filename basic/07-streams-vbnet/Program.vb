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

' A log that is read rather than emptied, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/07-streams-vbnet
'
' A queue is destructive: one consumer takes a message and it is gone. A stream
' keeps everything until retention removes it, every reader holds its own
' position in it, and reading changes nothing.
'
' Four readers run over one stream here. Three of them read messages that an
' earlier reader had already read, which is the whole point and is impossible
' with a queue.

Imports System.Collections.Generic
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Order
    Public Property OrderId As String = ""
    Public Property Total As Integer
End Class

Module Program

    ' Named "OrderLog" rather than "Log": VB.NET is case-insensitive, and a
    ' member called Log collides with Microsoft.VisualBasic's Log function.
    '
    ' Distinct from the C# example's stream for the usual reason -- both run
    ' against one broker on CI -- and distinct from the other languages'
    ' examples, since a stream redeclared with different retention is a
    ' PRECONDITION_FAILED rather than an adjustment.
    Private Const OrderLog As String = "dotnet-vbnet-orders-log"

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(120))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/07-streams-vbnet") _
                    .Build())

            Try
                ' A stream is append-only and nothing consumes it away, so a
                ' second run would read this run's messages as well as its own.
                ' Deleting first is what makes the example re-runnable; a real
                ' log is not deleted.
                Await mq.DeleteQueueAsync(OrderLog)

                ' Retention is the argument that matters. Both limits accept
                ' Nothing, which is legal and almost always wrong: a stream with
                ' neither grows until the disk is full, and on RabbitMQ a full
                ' disk is not a stream problem but a broker-wide alarm that
                ' blocks every publisher on the node.
                '
                ' The fourth argument is the segment size, and it is opt-in
                ' because retention happens a whole segment at a time. A stream
                ' bounded at 20 MB with 20 MB segments keeps rather more than
                ' 20 MB, since nothing can be discarded until an entire segment
                ' can be.
                Await mq.DeclareStreamAsync(
                    OrderLog, TimeSpan.FromHours(1), 20L * 1024 * 1024, 1L * 1024 * 1024)

                Dim publisher = mq.Publisher(Of Order)("", OrderLog)
                For i = 0 To 4
                    Await publisher.SendAsync(New Order With {.OrderId = $"o-{i}", .Total = i * 10})
                Next
                Console.WriteLine("wrote      o-0 .. o-4")

                ' A projection being built for the first time has to see
                ' history, so it says FromFirst(). A reader that is not told
                ' where to start reads from FromNext() -- right for a live
                ' consumer, and silently wrong for this: it would come up empty
                ' and look perfectly healthy.
                Dim first As New List(Of String)
                Dim checkpoint As Long

                Dim readerOne = Await mq.Stream(Of Order)(OrderLog) _
                    .FromFirst() _
                    .Prefetch(10) _
                    .ConsumeAsync(
                        Function(message As IMessage(Of Order)) As Task
                            SyncLock first
                                first.Add(message.Payload.OrderId)
                            End SyncLock
                            Return Task.CompletedTask
                        End Function)
                Try
                    Await WaitFor(Function() HandledCount(first) = 5, token)

                    ' The offset of the last message handled, not the count of
                    ' them -- and nothing stores it for you. The broker
                    ' remembers no reader's position, which is exactly what
                    ' makes a stream cheap to have several readers on. Saving
                    ' this next to whatever the projection wrote is the
                    ' application's job.
                    If Not readerOne.LastHandledOffset.HasValue Then
                        Throw New InvalidOperationException("the reader handled nothing")
                    End If
                    checkpoint = readerOne.LastHandledOffset.Value
                    Console.WriteLine(
                        $"read       {readerOne.Handled}, checkpoint offset {checkpoint}")
                Finally
                    readerOne.Dispose()
                End Try

                ' Five more while that reader is not running, which is what a
                ' deploy or a crash looks like from the stream's side.
                For i = 5 To 9
                    Await publisher.SendAsync(New Order With {.OrderId = $"o-{i}", .Total = i * 10})
                Next
                Console.WriteLine("wrote      o-5 .. o-9 while the reader was down")

                ' Resuming from one past the checkpoint. Not from the
                ' checkpoint: that offset was handled, and starting there would
                ' handle it twice.
                Dim resumed As New List(Of String)
                Using readerTwo = Await mq.Stream(Of Order)(OrderLog) _
                    .FromOffset(checkpoint + 1) _
                    .ConsumeAsync(
                        Function(message As IMessage(Of Order)) As Task
                            SyncLock resumed
                                resumed.Add(message.Payload.OrderId)
                            End SyncLock
                            Return Task.CompletedTask
                        End Function)

                    Await WaitFor(Function() HandledCount(resumed) = 5, token)
                End Using
                Console.WriteLine($"resumed    [{String.Join(", ", Snapshot(resumed))}]")

                ' The property a queue does not have. Two readers have been over
                ' this stream and a third, attached now, still sees all ten:
                ' nothing above consumed anything.
                Dim auditor As New List(Of String)
                Using readerThree = Await mq.Stream(Of Order)(OrderLog) _
                    .FromFirst() _
                    .ConsumeAsync(
                        Function(message As IMessage(Of Order)) As Task
                            SyncLock auditor
                                auditor.Add(message.Payload.OrderId)
                            End SyncLock
                            Return Task.CompletedTask
                        End Function)

                    Await WaitFor(Function() HandledCount(auditor) = 10, token)
                End Using
                Console.WriteLine($"auditor    saw all {HandledCount(auditor)} from the beginning")

                ' And the default, which is the one to be deliberate about.
                ' FromNext() is what a live consumer wants and what a
                ' StreamReader does when it is not told otherwise: history is
                ' skipped entirely.
                Dim live As New List(Of String)
                Using readerFour = Await mq.Stream(Of Order)(OrderLog) _
                    .FromNext() _
                    .ConsumeAsync(
                        Function(message As IMessage(Of Order)) As Task
                            SyncLock live
                                live.Add(message.Payload.OrderId)
                            End SyncLock
                            Return Task.CompletedTask
                        End Function)

                    Await publisher.SendAsync(New Order With {.OrderId = "o-10", .Total = 100})
                    Await WaitFor(Function() HandledCount(live) = 1, token)
                End Using
                Console.WriteLine(
                    $"live       saw [{String.Join(", ", Snapshot(live))}] and none of the history")

                ' Eleven messages written and twenty-one deliveries handled,
                ' which is the arithmetic a queue cannot produce: on a queue
                ' eleven messages are eleven deliveries and then the queue is
                ' empty.
                Dim delivered = HandledCount(first) + HandledCount(resumed) +
                                HandledCount(auditor) + HandledCount(live)
                Console.WriteLine($"totals     11 written, {delivered} handled across four readers")

                ' Worth knowing rather than worth asserting: the broker does not
                ' report a stream's length through queue.declare, so
                ' MessageCountAsync comes back 0 for a stream however much is in
                ' it. Reach for the management API if the depth is what you need.
                Console.WriteLine(
                    $"depth      MessageCountAsync says {Await mq.MessageCountAsync(OrderLog)}")

                Check(Snapshot(first).SequenceEqual(New String() {"o-0", "o-1", "o-2", "o-3", "o-4"}),
                      $"the first reader saw [{String.Join(", ", Snapshot(first))}], not o-0..o-4")
                Check(checkpoint = 4, $"the checkpoint was offset {checkpoint}, not 4")
                Check(Snapshot(resumed).SequenceEqual(New String() {"o-5", "o-6", "o-7", "o-8", "o-9"}),
                      $"resuming saw [{String.Join(", ", Snapshot(resumed))}], " &
                      "so there is a gap or a repeat")
                Check(HandledCount(auditor) = 10,
                      $"the auditor saw {HandledCount(auditor)} of the first ten")
                Check(Snapshot(live).SequenceEqual(New String() {"o-10"}),
                      $"the live reader saw [{String.Join(", ", Snapshot(live))}], not just o-10")
                Check(delivered = 21,
                      $"{delivered} deliveries from eleven messages, " &
                      "so a reader did not see what it should")

                ' When not to use one. A stream is the wrong shape for work
                ' distribution: every reader sees every message, so two workers
                ' on a stream both do the job rather than sharing it. That is a
                ' queue, and a consumer group over a queue is how it is scaled --
                ' see intermediate/06-consumer-groups-vbnet.

                Await mq.DeleteQueueAsync(OrderLog)
                Await mq.DeleteQueueAsync(OrderLog & ".dlq")
                Await mq.DeleteQueueAsync(OrderLog & ".parked")
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Function HandledCount(seen As List(Of String)) As Integer
        SyncLock seen
            Return seen.Count
        End SyncLock
    End Function

    Private Function Snapshot(seen As List(Of String)) As String()
        SyncLock seen
            Return seen.ToArray()
        End SyncLock
    End Function

    Private Async Function WaitFor(held As Func(Of Boolean), token As CancellationToken) As Task
        While Not held()
            token.ThrowIfCancellationRequested()
            Await Task.Delay(25, token)
        End While
    End Function

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
