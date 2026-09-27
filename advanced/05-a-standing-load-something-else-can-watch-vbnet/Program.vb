' A load that keeps running and says what is happening to it, one line at a time.
'
' Every other example here finishes. This one does not: it publishes and consumes
' at a steady rate and writes one JSON object per second describing what it has
' seen. That makes it the thing a fault drill breaks the cluster underneath -- the
' drill kills a node or raises a memory alarm, reads these lines, and judges what
' the client did about it.
'
' Why a drill needs this rather than a probe of its own
' -----------------------------------------------------
' A probe that connects to the broker can answer "is the cluster usable". It cannot
' answer what an application saw: whether it was told the broker had stopped
' reading from it, whether it stopped publishing, whether it started again on its
' own or sat there. Those are properties of a client library, they differ between
' libraries that are otherwise equivalent, and the only thing that can report them
' is a client.
'
' What a line contains
' --------------------
'   blocked      the broker is refusing to read from this connection now
'   published    sends attempted since the start
'   confirmed    sends the broker has acknowledged
'   consumed     deliveries handled
'   failed       sends that failed for a reason other than back-pressure
'   publishRate  confirms per second over the last interval
'   consumeRate  deliveries per second over the last interval
'
' Running it
'
'   dotnet run --project advanced/05-a-standing-load-something-else-can-watch-vbnet _
'     > readings.jsonl
'
' Then read the last few lines at any point to see what the client is seeing. Under
' a fault drill that file is the client's testimony, and `tail -n 60` on it is how
' the drill asks.
'
' It stops on Ctrl-C, or after ACEMQ_EXAMPLE_SECONDS if that is set -- which CI
' sets, because a load with no reason to stop is not a failing example there, it is
' a job that never ends. Unset, it runs until interrupted, which is what a drill
' campaign wants.
'
' What to watch under a fault: `published` and `confirmed` moving apart, `blocked`
' turning true with the broker's own reason beside it, and both counters moving
' again once it clears. None of that is visible to anything that asks the broker how
' it is doing -- the cluster is healthy, and this application is not publishing.

Imports System.Text.Json
Imports System.Text.Json.Serialization
Imports System.Threading
Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public NotInheritable Class Order
    Public Property Id As String = ""
End Class

' The shape of one line of the timeline.
'
' The property names are a contract with whatever reads them, so they are spelled
' here exactly as a reader expects rather than left to a naming policy. A reader
' looking for `confirmed` and finding `Confirmed` sees a client reporting nothing,
' which is indistinguishable from a well-behaved client on a quiet cluster.
Public NotInheritable Class Reading
    <JsonPropertyName("at")> Public Property At As String = ""
    <JsonPropertyName("elapsedMs")> Public Property ElapsedMs As Long
    <JsonPropertyName("blocked")> Public Property Blocked As Boolean
    <JsonPropertyName("published")> Public Property Published As Long
    <JsonPropertyName("confirmed")> Public Property Confirmed As Long
    <JsonPropertyName("consumed")> Public Property Consumed As Long
    <JsonPropertyName("failed")> Public Property Failed As Long
    ' Sends this library declined because the broker had blocked the connection. Not
    ' part of what a drill judges, and reported anyway: it is the number that
    ' separates a library which refuses back-pressure promptly from one that parks on
    ' it, and counting a prompt refusal as a failure would make the better behaviour
    ' look like the worse one.
    <JsonPropertyName("refused")> Public Property Refused As Long

    <JsonPropertyName("publishRate")> Public Property PublishRate As Double
    <JsonPropertyName("consumeRate")> Public Property ConsumeRate As Double

    <JsonPropertyName("reason")>
    <JsonIgnore(Condition:=JsonIgnoreCondition.WhenWritingNull)>
    Public Property Reason As String
End Class

Public Module Program

    Private Const Queue As String = "dotnet-standing-load-vb.orders"

    ' Counters, read and written from two places. Interlocked rather than a lock
    ' because each one is a single counter incremented in one task and read in
    ' another, and a reading a fraction of a second stale is not one anybody would
    ' act on differently.
    Private _published As Long
    Private _confirmed As Long
    Private _consumed As Long
    Private _failed As Long
    Private _refused As Long

    Public Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        ' Stderr, so stdout carries nothing but readings. A reader skips whatever
        ' is not a JSON object, so mixing them would work -- and it would also mean
        ' every diagnostic line here had to stay un-JSON-like for ever, which is
        ' not a property anybody would remember to preserve.
        Console.Error.WriteLine($"standing load: {BrokerUrl()}, {Queue} at {Rate()}/s")

        ' The AMQP transport, registered before connecting. Without it the only
        ' scheme this library knows is `memory`, and the failure names the missing
        ' call rather than leaving it to be guessed at.
        Transports.Register(New RabbitMqTransport())

        Using stopping As New CancellationTokenSource()
            ' Handled rather than letting the runtime kill the process, so the
            ' connection closes and the final reading is written. A drill reads the
            ' tail of this output, and a load that dies without a last line leaves
            ' its final interval unaccounted for.
            AddHandler Console.CancelKeyPress,
                Sub(sender As Object, e As ConsoleCancelEventArgs)
                    e.Cancel = True
                    stopping.Cancel()
                End Sub

            If RunForSeconds() > 0 Then
                stopping.CancelAfter(TimeSpan.FromSeconds(RunForSeconds()))
            End If

            Dim token = stopping.Token

            Using mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/05-a-standing-load-vbnet") _
                    .ConfirmTimeout(TimeSpan.FromSeconds(5)) _
                    .Build(),
                New JsonCodec(),
                token)

                ' Quorum, because a drill's faults are about losing a node. A
                ' classic queue lives on one node, and when that node is the one
                ' the drill stops, the load stops with it -- which reports a client
                ' that gave up when the truth is that the queue went away.
                Await mq.DeclareQueueAsync(Queue, QueueType.Quorum, Nothing)

                Using consumer = Await mq.ConsumeAsync(Of Order)(
                    Queue,
                    Function(message)
                        Interlocked.Increment(_consumed)
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim publishing = PublishAsync(mq, token)
                    Await SampleAsync(mq, token)
                    Await publishing
                End Using
            End Using
        End Using

        Return 0
    End Function

    Private Async Function PublishAsync(mq As AceMqConnection, token As CancellationToken) As Task
        Dim publisher = mq.Publisher(Of Order)("", Queue)
        Dim interval = TimeSpan.FromSeconds(1.0 / Math.Max(Rate(), 1))
        Dim n As Long = 0

        Do While Not token.IsCancellationRequested
            n += 1
            Interlocked.Increment(_published)
            Try
                Await publisher.SendAsync(New Order With {.Id = $"o-{n}"})
                Interlocked.Increment(_confirmed)
            Catch ex As OperationCanceledException When token.IsCancellationRequested
                Return
            Catch ex As Exception When TypeOf ex Is ConnectionBlockedException OrElse TypeOf ex Is PublishingPausedException
                ' Back-pressure, correctly reported, and not a failure: the broker
                ' said it had stopped reading and this library declined rather than
                ' waiting. Counted separately so that a library which says so promptly
                ' does not read as one losing tens of thousands of messages.
                '
                ' Two exception types, because this library distinguishes two things
                ' Go names alike: ConnectionBlockedException is the broker blocking the
                ' connection, and PublishingPausedException is this application having
                ' paused its own publishing. Either way the send was declined rather
                ' than lost.
                Interlocked.Increment(_refused)
            Catch ex As Exception
                ' Counted rather than hidden, and not fatal: a standing load
                ' reports what happened to it and keeps going. While the broker is
                ' blocking this connection these are the sends that never
                ' completed, and `blocked` on the same line is what says why.
                Interlocked.Increment(_failed)
            End Try

            Try
                Await Task.Delay(interval, token)
            Catch ex As OperationCanceledException
                Return
            End Try
        Loop
    End Function

    Private Async Function SampleAsync(mq As AceMqConnection, token As CancellationToken) As Task
        Dim interval = TimeSpan.FromSeconds(IntervalSeconds())
        Dim started = DateTimeOffset.UtcNow
        Dim last = started
        Dim lastConfirmed As Long = 0
        Dim lastConsumed As Long = 0

        Dim emit =
            Sub()
                Dim now = DateTimeOffset.UtcNow
                Dim elapsed = (now - last).TotalSeconds
                Dim confirmed = Interlocked.Read(_confirmed)
                Dim consumed = Interlocked.Read(_consumed)

                Dim reading As New Reading With {
                    .At = now.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                    .ElapsedMs = CLng((now - started).TotalMilliseconds),
                    .Blocked = mq.IsBlocked,
                    .Reason = mq.BlockedReason,
                    .Published = Interlocked.Read(_published),
                    .Confirmed = confirmed,
                    .Consumed = consumed,
                    .Failed = Interlocked.Read(_failed),
                    .Refused = Interlocked.Read(_refused)
                }
                If elapsed > 0 Then
                    reading.PublishRate = (confirmed - lastConfirmed) / elapsed
                    reading.ConsumeRate = (consumed - lastConsumed) / elapsed
                End If
                last = now
                lastConfirmed = confirmed
                lastConsumed = consumed

                Console.WriteLine(JsonSerializer.Serialize(reading))
                ' Flushed, because stdout to a file is buffered and a drill reads
                ' that file while this process is still running. Without the flush
                ' the last readings sit in a buffer and the drill reports a client
                ' that went quiet.
                Console.Out.Flush()
            End Sub

        Do While Not token.IsCancellationRequested
            Try
                Await Task.Delay(interval, token)
            Catch ex As OperationCanceledException
                Exit Do
            End Try
            emit()
        Loop

        ' One last line on the way out, so the final interval is accounted for
        ' rather than being the one a reader has to guess about.
        emit()
    End Function

    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

    Private Function Rate() As Integer
        Return IntFromEnv("ACEMQ_LOAD_RATE", 200)
    End Function

    Private Function IntervalSeconds() As Double
        Return IntFromEnv("ACEMQ_LOAD_INTERVAL", 1)
    End Function

    ' How long to run when nobody said: a minute, not for ever.
    '
    ' That default is deliberate. CI runs every example in this repository with no
    ' arguments and waits for each to finish, so an unbounded default is not a failing
    ' example -- it is a job that runs to the six-hour ceiling and is then cancelled.
    ' That happened, in three repositories at once, and cost about eighteen hours of
    ' runner time before anybody looked.
    '
    ' So a forgotten setting gives a short run, and "until interrupted" has to be asked
    ' for: ACEMQ_EXAMPLE_SECONDS=0, which is what a drill campaign passes.
    Private Const DefaultRunForSeconds As Integer = 60

    Private Function RunForSeconds() As Integer
        Dim raw = Environment.GetEnvironmentVariable("ACEMQ_EXAMPLE_SECONDS")
        If String.IsNullOrEmpty(raw) Then
            Return DefaultRunForSeconds
        End If

        ' Zero is a real setting, and the only way to ask for an unbounded run.
        Dim value As Integer
        If Integer.TryParse(raw, value) Then
            Return value
        End If
        Return DefaultRunForSeconds
    End Function

    Private Function IntFromEnv(name As String, fallback As Integer) As Integer
        Dim value As Integer
        If Integer.TryParse(Environment.GetEnvironmentVariable(name), value) AndAlso value > 0 Then
            Return value
        End If
        Return fallback
    End Function

End Module
