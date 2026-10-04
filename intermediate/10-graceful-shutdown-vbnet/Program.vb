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

' What happens to the message being handled when the process is told to stop,
' in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/10-graceful-shutdown-vbnet
'
' Three shutdowns of the same consumer, each arriving while a handler is half way
' through a one-second job with a backlog of orders queued behind it:
'
'   CloseAsync    drains first: the job finishes and its message is settled
'   Dispose       does not drain: the job's message comes back
'   out of time   a drain with too small a budget, and the answer it gives
'
' Every one of them closes in about a second, backlog and all, and the backlog is
' still on the queue afterwards -- nothing lost, nothing dead-lettered.
'
' Kubernetes sends SIGTERM and starts a clock. Whatever is still inside a handler
' when the connection goes away was never acknowledged, so the broker hands it to
' somebody else -- correct, and the reason a deployment shows up as a spike of
' duplicate work when nobody arranged otherwise. It takes about three seconds,
' because the handler really does sleep.
'
' C# writes the first of these as `await using`. VB.NET has no `await using` and
' cannot Await the ValueTask that DisposeAsync returns, so the same drain is
' offered as CloseAsync, which returns a Task -- one method, both languages, the
' same code underneath.

Imports System.Diagnostics
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Order
    Public Property OrderId As String = ""
End Class

' One consumer's worth of state: when its first job started, and how many it has
' finished.
Public Class Worker
    Private _handled As Integer

    Public ReadOnly Property Started As New TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously)

    Public ReadOnly Property Handled As Integer
        Get
            Return Volatile.Read(_handled)
        End Get
    End Property

    ' Not disposed on its own: the connection owns every consumer started on it
    ' and closes them on the way out, which is the order a drain needs -- the
    ' handlers finish first, then the subscriptions go.
    Public Function ConsumeAsync(mq As AceMqConnection, queueName As String) As Task(Of IMessageConsumer)
        Return mq.ConsumeAsync(Of Order)(queueName, ConsumerOptions.Prefetch(1), AddressOf Handle)
    End Function

    Private Async Function Handle(message As IMessage(Of Order)) As Task(Of Ack)
        Started.TrySetResult()
        Await Task.Delay(Program.JobTakes)
        Interlocked.Increment(_handled)
        Return Ack.Accept()
    End Function
End Class

Module Program

    ' Distinct from the C# example's queues for the usual reason: both run
    ' against one broker on CI.
    '
    ' Suffixed "Queue" because VB.NET is case-insensitive. The drain's answer
    ' below is a local called drained, and a local hides a module member of the
    ' same name for the whole of its function -- including the lines above its
    ' Dim, where using the constant is then error BC32000, "cannot be referred to
    ' before it is declared".
    Private Const DrainedQueue As String = "dotnet-vbnet-shutdown-drained"
    Private Const AbandonedQueue As String = "dotnet-vbnet-shutdown-abandoned"
    Private Const OutOfTimeQueue As String = "dotnet-vbnet-shutdown-out-of-time"

    ' A backlog, because a real queue has one. While the first job runs, the drain
    ' pauses consuming; once that job is acknowledged the broker pushes the next
    ' order, which stops at the pause gate. Closing hands it back with requeue, so
    ' it is redelivered later rather than lost or dead-lettered.
    Private Const Orders As Integer = 5

    ' How long a close may take before this example calls it slow. A close with a
    ' backlog should take about one job's length; five seconds is generous for
    ' that and well inside Kubernetes' 30-second grace period.
    Private ReadOnly Promptly As TimeSpan = TimeSpan.FromSeconds(5)

    ' Long enough that a message is genuinely still being handled when the
    ' shutdown arrives, short enough that the example does not drag.
    Friend ReadOnly JobTakes As TimeSpan = TimeSpan.FromSeconds(1)

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            ' A second connection that only looks. The broker is the one to ask
            ' how many messages are left, and asking from the connection being
            ' shut down is not possible once it has been.
            Dim inspector = Await Connect("inspector")
            Try
                Await RunAgainst(inspector, token)
                Await inspector.CloseAsync()
            Finally
                ' VB.NET cannot Await inside a Finally, so CloseAsync is the last
                ' thing the Try does and this is only the backstop for a failure
                ' -- Dispose after CloseAsync does nothing.
                inspector.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Async Function RunAgainst(inspector As AceMqConnection, token As CancellationToken) As Task
        ' 1. CloseAsync. Consuming is paused, the handler already running is
        '    given up to twenty seconds (AceMqConnection.DefaultDrainTimeout) to
        '    finish, and only then is the connection closed. The job in hand
        '    completes and is acknowledged by the handler that had it.
        Dim first As New Worker()
        Dim clock As New Stopwatch()
        Dim closing = Await Connect("drained")
        Try
            Await Fill(inspector, DrainedQueue)
            Await first.ConsumeAsync(closing, DrainedQueue)
            Await first.Started.Task.WaitAsync(token)

            ' SIGTERM arrives here, with a job half done.
            clock.Start()
            Await closing.CloseAsync()
        Finally
            closing.Dispose()
        End Try
        clock.Stop()
        Dim drainedTook = clock.Elapsed
        Dim drainedHandled = first.Handled
        Dim drainedLeft = Await inspector.MessageCountAsync(DrainedQueue)
        Dim drainedDead = Await inspector.MessageCountAsync(DrainedQueue & ".dlq")
        Console.WriteLine(
            $"CloseAsync   closed after {clock.ElapsedMilliseconds,4} ms, " &
            $"handled {drainedHandled} first, {drainedLeft} of {Orders} left, {drainedDead} dead-lettered")

        ' 2. Dispose. It does not drain. Nor does it return at once: on RabbitMQ
        '    the channel closes only after the handler running on it has
        '    returned, so the job does finish. But by then Dispose has closed the
        '    channel, so the acknowledgement never reaches the broker and the
        '    message is redelivered -- the side effect has happened, and it will
        '    happen again for whoever takes it next.
        Dim second As New Worker()
        Dim disposing = Await Connect("abandoned")
        Try
            Await Fill(inspector, AbandonedQueue)
            Await second.ConsumeAsync(disposing, AbandonedQueue)
            Await second.Started.Task.WaitAsync(token)
            clock.Restart()
        Finally
            disposing.Dispose()
        End Try
        clock.Stop()
        Dim abandonedTook = clock.Elapsed
        Dim abandonedHandled = second.Handled
        Dim abandonedLeft = Await inspector.MessageCountAsync(AbandonedQueue)
        Dim abandonedDead = Await inspector.MessageCountAsync(AbandonedQueue & ".dlq")
        Console.WriteLine(
            $"Dispose      closed after {clock.ElapsedMilliseconds,4} ms, " &
            $"handled {abandonedHandled} first, {abandonedLeft} of {Orders} left, {abandonedDead} dead-lettered")

        ' 3. out of time. The shape a service wants when it needs the answer
        '    rather than only the tidy-up: drain with a budget under the grace
        '    period, and say so when it ran out. CloseAsync discards that answer,
        '    because a teardown has nobody to report it to.
        Dim third As New Worker()
        Dim drained As Boolean
        Dim inFlight As Long
        clock.Reset()
        Dim hurried = Await Connect("out-of-time")
        Try
            Await Fill(inspector, OutOfTimeQueue)
            Await third.ConsumeAsync(hurried, OutOfTimeQueue)
            Await third.Started.Task.WaitAsync(token)

            ' A tenth of what the job needs. False is the signal worth logging
            ' and alerting on: either the grace period is shorter than the
            ' slowest handler, or a handler is stuck.
            drained = Await hurried.DrainConsumersAsync(TimeSpan.FromMilliseconds(100), token)
            inFlight = hurried.InFlight
            Console.WriteLine(
                $"out of time  drained {drained}, {inFlight} still in flight, so the grace " &
                "period was shorter than the job")
        Finally
            ' And then the clock runs out, which is what Dispose stands in for
            ' here -- not CloseAsync, which would drain again with the full
            ' twenty seconds, time a process past its grace period does not have.
            clock.Start()
            hurried.Dispose()
            clock.Stop()
        End Try
        Dim outOfTimeTook = clock.Elapsed
        Dim outOfTimeLeft = Await inspector.MessageCountAsync(OutOfTimeQueue)
        Dim outOfTimeDead = Await inspector.MessageCountAsync(OutOfTimeQueue & ".dlq")
        Console.WriteLine(
            $"             closed after {clock.ElapsedMilliseconds,4} ms, " &
            $"{outOfTimeLeft} of {Orders} left, {outOfTimeDead} dead-lettered")

        Check(drainedTook < Promptly,
              $"CloseAsync took {drainedTook.TotalMilliseconds:F0} ms with a backlog; it should " &
              "take about one job")
        Check(drainedHandled = 1,
              $"CloseAsync returned with {drainedHandled} jobs handled; it should wait for the " &
              "one in hand and start no other")
        Check(drainedLeft = Orders - 1,
              $"{drainedLeft} left after a drained close; the finished job's message should be " &
              "gone and the held one back on the queue")
        Check(abandonedTook < Promptly,
              $"Dispose took {abandonedTook.TotalMilliseconds:F0} ms with a backlog; it should " &
              "take about one job")
        Check(abandonedLeft = Orders,
              $"{abandonedLeft} left after Dispose; the unacknowledged message should have come back")
        Check(Not drained AndAlso inFlight = 1,
              $"a 100 ms drain of a one-second job answered {drained} with {inFlight} in flight")
        Check(outOfTimeTook < Promptly,
              $"Dispose after the drain ran out took {outOfTimeTook.TotalMilliseconds:F0} ms")
        Check(outOfTimeLeft = Orders,
              $"{outOfTimeLeft} left after running out of time; the unfinished message should " &
              "have come back")
        Check(drainedDead + abandonedDead + outOfTimeDead = 0,
              $"{drainedDead + abandonedDead + outOfTimeDead} dead-lettered; a shutdown should " &
              "requeue, never reject")

        ' What redelivery costs is nothing if the handler is idempotent, and
        ' everything if it charges a card. A graceful shutdown reduces
        ' duplicates; it does not eliminate them, because a power cut has no
        ' SIGTERM. See basic/03-idempotent-consumer-vbnet for the other half.

        For Each name In {DrainedQueue, AbandonedQueue, OutOfTimeQueue}
            Await inspector.DeleteQueueAsync(name)
            Await inspector.DeleteQueueAsync(name & ".dlq")
            Await inspector.DeleteQueueAsync(name & ".parked")
        Next
    End Function

    Private Async Function Fill(mq As AceMqConnection, queueName As String) As Task
        ' Emptied first, because every number this example checks is a count of
        ' what is left on the queue, and a run that failed half way leaves
        ' messages behind for the next one to count.
        Await mq.DeleteQueueAsync(queueName)
        Await mq.DeclareQueueAsync(queueName)
        Dim publisher = mq.Publisher(Of Order)("", queueName)
        For i = 1 To Orders
            Await publisher.SendAsync(New Order With {.OrderId = $"o-{i}"})
        Next
    End Function

    Private Function Connect(name As String) As Task(Of AceMqConnection)
        Return AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl()) _
                .ClientName($"examples/10-graceful-shutdown-vbnet/{name}") _
                .Build())
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
