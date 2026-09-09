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

' Several consumers over one queue, started and stopped together, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/06-consumer-groups-vbnet
'
' Forty jobs handled one at a time, then forty more handled eight at a time,
' with the wall clock and the measured concurrency printed for both. It takes
' about three seconds, because the handler really does sleep.
'
' Two numbers decide how a queue behaves under load and they get confused
' constantly. CONCURRENCY is how many handlers run at once. PREFETCH is how many
' messages the broker may hand one consumer before it acknowledges any of them.
' Getting the second one wrong is what makes a queue with eight consumers behave
' like a queue with one.

Imports System.Collections.Concurrent
Imports System.Diagnostics
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Job
    Public Property JobId As String = ""
End Class

Public Class Batch
    Public Property Elapsed As Long
    Public Property Peak As Integer
    Public Property SizeWhileRunning As Integer
    Public Property SizeAfterDispose As Integer
End Class

Module Program

    ' Distinct from the C# example's queue for the usual reason: both run
    ' against one broker on CI.
    Private Const Work As String = "dotnet-vbnet-group-work"

    ' Long enough that the difference between one consumer and eight is the wall
    ' clock rather than the noise in it.
    Private ReadOnly HandlerTakes As TimeSpan = TimeSpan.FromMilliseconds(50)

    Private ReadOnly Handled As New ConcurrentDictionary(Of String, Integer)
    Private ReadOnly PeakGate As New Object()
    Private Running As Integer
    Private Peak As Integer

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(120))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/06-consumer-groups-vbnet") _
                    .Build())

            Try
                Await mq.DeclareQueueAsync(Work)

                ' Prefetch 1 in both runs, so the only thing that changes
                ' between them is the number of consumers. It is also the
                ' setting that makes the second run work at all: with the
                ' default the broker may hand twenty messages to the first
                ' consumer that asks, and seven of the eight sit idle while one
                ' of them works through a private backlog. A group is only as
                ' parallel as its prefetch lets it be.
                Dim settings = ConsumerOptions.Prefetch(1)

                Dim oneAtATime = Await RunBatch(mq, settings, 1, 0, 40, token)
                Console.WriteLine(
                    $"1 consumer  40 jobs in {oneAtATime.Elapsed} ms, " &
                    $"peak concurrency {oneAtATime.Peak}")

                ' Named "eightAtOnce" rather than "parallel": Parallel is
                ' System.Threading.Tasks.Parallel, and VB.NET would report the
                ' clash as a type it cannot infer rather than as a name in use.
                Dim eightAtOnce = Await RunBatch(mq, settings, 8, 40, 40, token)
                Console.WriteLine(
                    $"8 consumers 40 jobs in {eightAtOnce.Elapsed} ms, " &
                    $"peak concurrency {eightAtOnce.Peak}")

                Console.WriteLine()
                Console.WriteLine(
                    $"handled     {Handled.Count} of 80, each exactly once: {EachOnce()}")
                Console.WriteLine(
                    $"group size  {oneAtATime.SizeWhileRunning} then " &
                    $"{eightAtOnce.SizeWhileRunning}, and " &
                    $"{eightAtOnce.SizeAfterDispose} once disposed")

                ' What to call before shutting down. Disposing a connection
                ' while handlers are mid-flight abandons their work: the
                ' messages were never acknowledged so they come back, but a side
                ' effect already applied has happened twice by the time they do.
                Dim drained = Await mq.DrainConsumersAsync(TimeSpan.FromSeconds(10))
                Console.WriteLine($"drained     {drained}, in flight {mq.InFlight}")

                Check(Handled.Count = 80,
                      $"{Handled.Count} distinct jobs were handled, not eighty")
                Check(EachOnce(),
                      "a job was handled more than once, so the group duplicated work")
                Check(oneAtATime.Peak = 1,
                      $"one consumer reached concurrency {oneAtATime.Peak}, " &
                      "which one consumer cannot do")
                Check(eightAtOnce.Peak >= 2,
                      $"eight consumers never got past concurrency {eightAtOnce.Peak}, " &
                      "so nothing ran in parallel")

                ' Two times rather than eight. The handler sleeps 50 ms, so
                ' forty jobs one at a time cannot take less than two seconds and
                ' an eight-way run should be near a quarter of that -- but
                ' asserting 8x would be asserting that the machine running CI is
                ' idle. 2x still fails loudly if the group quietly regresses to
                ' serial.
                Check(oneAtATime.Elapsed >= 2 * eightAtOnce.Elapsed,
                      $"forty jobs took {oneAtATime.Elapsed} ms on one consumer and " &
                      $"{eightAtOnce.Elapsed} ms on eight, which is not the speed-up " &
                      "a group is for")

                Check(oneAtATime.SizeWhileRunning = 1 AndAlso eightAtOnce.SizeWhileRunning = 8,
                      "the group did not report the size it was started with")
                Check(eightAtOnce.SizeAfterDispose = 0,
                      $"{eightAtOnce.SizeAfterDispose} consumers were still attached after Dispose")
                Check(drained AndAlso mq.InFlight = 0,
                      $"draining left {mq.InFlight} handlers running, so a shutdown here " &
                      "would abandon work")

                ' What a group costs. One consumer on a queue sees messages in
                ' order; eight do not, because the broker round-robins between
                ' them and a slow handler finishes after a fast one that started
                ' later. Where a later message about the same entity must not
                ' overtake an earlier one, keep the group and route by key --
                ' see basic/03-idempotent-consumer-vbnet for the other half of
                ' that story.
                '
                ' A group is not the only way to go faster, either. Raising
                ' prefetch lets one consumer hold more unacknowledged messages,
                ' but the handler still runs them one at a time on that
                ' consumer's channel. Reach for a group when the handler is slow
                ' enough that one channel is the limit, or when a fair share
                ' across processes matters: eight consumers here compete evenly
                ' with eight in another instance.

                Await mq.DeleteQueueAsync(Work)
                Await mq.DeleteQueueAsync(Work & ".dlq")
                Await mq.DeleteQueueAsync(Work & ".parked")
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Async Function RunBatch(
        mq As AceMqConnection, settings As ConsumerOptions,
        size As Integer, from As Integer, count As Integer,
        token As CancellationToken) As Task(Of Batch)

        Dim publisher = mq.Publisher(Of Job)("", Work)
        For i = from To from + count - 1
            Await publisher.SendAsync(New Job With {.JobId = $"job-{i}"})
        Next

        SyncLock PeakGate
            Peak = 0
        End SyncLock
        Dim before = Handled.Count
        Dim clock = Stopwatch.StartNew()

        ' The two things a group saves you from. Starting workers by hand means
        ' remembering to close every one, and a partial shutdown leaves messages
        ' held by a consumer nobody is waiting for. And the size is a number, so
        ' it can come from configuration -- which is the setting most often
        ' changed after a service is already running.
        '
        ' Named "workers" rather than "group": Group is a query keyword in
        ' VB.NET and would have to be written [group] to be a variable at all.
        Dim workers = Await ConsumerGroup.StartAsync(Of Job)(
            mq, Work, size, settings, AddressOf Handle)
        Dim batch As New Batch With {.SizeWhileRunning = workers.Size}
        Try
            Await WaitFor(Function() Handled.Count = before + count, token)
            clock.Stop()
        Finally
            ' Stops every consumer, and closes all of them even if one throws:
            ' leaving the rest running after a failed shutdown is worse than the
            ' failure.
            workers.Dispose()
        End Try

        batch.Elapsed = clock.ElapsedMilliseconds
        SyncLock PeakGate
            batch.Peak = Peak
        End SyncLock
        batch.SizeAfterDispose = workers.Size
        Return batch
    End Function

    ' Concurrency is measured by counting the handlers running at the same
    ' moment, not by counting threads. The group dispatches onto the thread
    ' pool, so even the serial run touches a dozen thread names and a thread
    ' count would prove nothing.
    Private Async Function Handle(message As IMessage(Of Job)) As Task(Of Ack)
        SyncLock PeakGate
            Running += 1
            If Running > Peak Then Peak = Running
        End SyncLock

        Try
            Await Task.Delay(HandlerTakes)
            Handled.AddOrUpdate(message.Payload.JobId, 1, Function(key, times) times + 1)
            Return Ack.Accept()
        Finally
            SyncLock PeakGate
                Running -= 1
            End SyncLock
        End Try
    End Function

    Private Function EachOnce() As Boolean
        Return Handled.Values.All(Function(times) times = 1)
    End Function

    Private Async Function WaitFor(held As Func(Of Boolean), token As CancellationToken) As Task
        While Not held()
            token.ThrowIfCancellationRequested()
            Await Task.Delay(10, token)
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
