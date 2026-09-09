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

' Asking a question over a queue and waiting for the answer, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/03-request-reply-vbnet
'
' Three things happen here: a round trip through a Responder, the same round
' trip served by hand so the reply address can be read off the message, and a
' request nobody answers.

Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class QuoteRequest
    Public Property Symbol As String = ""
End Class

Public Class Quote
    Public Property Symbol As String = ""
    Public Property Pence As Long
End Class

Module Program

    ' Queue names of this example's own, and distinct from the C# example's:
    ' both run against one broker on CI, and two examples declaring one name
    ' with different settings is a PRECONDITION_FAILED rather than a
    ' coincidence.
    Private Const Served As String = "quotes-served-vb"
    Private Const ByHand As String = "quotes-by-hand-vb"
    Private Const Unanswered As String = "quotes-unanswered-vb"

    ' What the hand-written responder saw. Read after the round trip, so a
    ' field rather than something threaded through a lambda.
    Private HeaderAddress As String = Nothing
    Private PropertyAddress As String = Nothing

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/03-request-reply-vbnet") _
                    .Build())

            Try
                Await mq.DeclareQueueAsync(Served)
                Await mq.DeclareQueueAsync(ByHand)
                ' Declared and deliberately never consumed. A request sent here
                ' is the one that times out.
                Await mq.DeclareQueueAsync(Unanswered)

                ' One reply queue for the whole requester, not one per request.
                ' A queue per call costs the broker a declare and a delete every
                ' time. Replies are matched by correlation id instead.
                '
                ' Named "asking" rather than "requester": VB.NET is
                ' case-insensitive, so a variable called requester collides with
                ' the Requester type and the compiler reports it as a type it
                ' cannot infer rather than as a name clash.
                Using asking = Await mq.RequesterAsync()
                    Console.WriteLine($"replies come back on {asking.ReplyQueue}")

                    ' ---- 1. the round trip --------------------------------
                    '
                    ' A Responder is a consumer that publishes the handler's
                    ' return value back to whoever asked. Nothing in the handler
                    ' mentions a reply queue.
                    Using answering = Await mq.RespondAsync(Of QuoteRequest, Quote)(
                        Served,
                        Function(request)
                            Return Task.FromResult(
                                New Quote With {.Symbol = request.Symbol, .Pence = 1234})
                        End Function)

                        Dim answer = Await asking.RequestAsync(Of QuoteRequest, Quote)(
                            "", Served, New QuoteRequest With {.Symbol = "ACME"},
                            TimeSpan.FromSeconds(20), token)

                        Console.WriteLine($"asked for ACME and got {answer.Symbol} at {answer.Pence}p")
                        Check(answer.Symbol = "ACME", $"the reply was for {answer.Symbol}, not ACME")
                        Check(answer.Pence = 1234, $"the reply carried {answer.Pence}, not 1234")

                        ' Read straight away, with no wait. The counter is
                        ' incremented before the reply is published, so a caller
                        ' holding its answer can never see a count that has not
                        ' caught up. Writing this example is what found the bug:
                        ' it used to be counted afterwards, and asserting it here
                        ' failed about half the time.
                        Check(answering.Answered = 1,
                              $"the responder answered {answering.Answered} times, not once")

                        ' ---- 2. where the reply address travels -----------
                        '
                        ' This is what 0.5.0 fixed, and it is invisible from
                        ' inside a Responder. .NET and Java put the address in
                        ' AMQP's own reply-to property; Go, Python and Ruby put
                        ' it in an application header, so a .NET requester and a
                        ' Go responder could not talk at all. Every library now
                        ' writes both and reads the header first, so this
                        ' consumer -- standing in for a responder written in
                        ' another language -- can pick up either one and answer.
                        Dim seen As New TaskCompletionSource(Of Boolean)(
                            TaskCreationOptions.RunContinuationsAsynchronously)

                        Using manual = Await mq.ConsumeAsync(Of QuoteRequest)(
                            ByHand,
                            Async Function(message) As Task(Of Ack)
                                ' Requester.ReplyToHeader is "acemq-reply-to".
                                ' It deliberately does not carry the x-acemq-
                                ' prefix: that namespace is the engine's and is
                                ' stripped before a handler sees it, so a
                                ' responder could never read an address there.
                                Dim carried As Object = Nothing
                                message.Headers.TryGetValue(Requester.ReplyToHeader, carried)
                                HeaderAddress = If(carried Is Nothing, Nothing, carried.ToString())
                                PropertyAddress = message.ReplyTo
                                seen.TrySetResult(True)

                                ' Answering by hand is three lines: publish to
                                ' the address on the default exchange, with the
                                ' request's id as the correlation. That is the
                                ' whole contract a Responder implements.
                                '
                                ' Named "stamp" rather than "envelope": VB.NET
                                ' is case-insensitive, so a variable called
                                ' envelope collides with the Envelope type and
                                ' the compiler reports it as a type it cannot
                                ' infer rather than as a name clash.
                                Dim stamp = Envelope.Of(message.Envelope.Type) _
                                    .CorrelationId(message.Envelope.Id) _
                                    .CausationId(message.Envelope.Id) _
                                    .Build()
                                Dim reply As New Quote With {
                                    .Symbol = message.Payload.Symbol, .Pence = 4321}
                                Await mq.Publisher(Of Quote)("", message.ReplyTo).SendAsync(reply, stamp)

                                Return Ack.Accept()
                            End Function)

                            Dim handMade = Await asking.RequestAsync(Of QuoteRequest, Quote)(
                                "", ByHand, New QuoteRequest With {.Symbol = "GLOBEX"},
                                TimeSpan.FromSeconds(20), token)

                            Await seen.Task.WaitAsync(token)
                            Console.WriteLine($"  {Requester.ReplyToHeader} header: {HeaderAddress}")
                            Console.WriteLine($"  AMQP reply-to property:  {PropertyAddress}")
                            Check(HeaderAddress = asking.ReplyQueue,
                                  $"the header said {HeaderAddress}, not {asking.ReplyQueue}")
                            Check(PropertyAddress = asking.ReplyQueue,
                                  $"the property said {PropertyAddress}, not {asking.ReplyQueue}")
                            Check(handMade.Pence = 4321,
                                  $"the hand-written reply carried {handMade.Pence}, not 4321")
                        End Using

                        ' ---- 3. nobody answers ----------------------------
                        '
                        ' The interesting failure. A request that is never
                        ' answered does not hang for ever and does not fail
                        ' silently: it throws once the timeout is up, and the
                        ' requester counts it.
                        Dim timedOut = False
                        Try
                            Await asking.RequestAsync(Of QuoteRequest, Quote)(
                                "", Unanswered, New QuoteRequest With {.Symbol = "NOBODY"},
                                TimeSpan.FromSeconds(2), token)
                        Catch failure As RequestTimedOutException
                            timedOut = True
                            Console.WriteLine($"gave up: {failure.Message}")
                        End Try

                        Check(timedOut, "a request to a queue nobody serves came back anyway")
                        Check(asking.TimedOut = 1, $"{asking.TimedOut} requests timed out, not one")

                        ' A reply that turns up after its caller has given up is
                        ' counted and dropped rather than handed to whoever asks
                        ' next. Handing a late answer to the wrong caller is
                        ' worse than no answer, and it is what happens when a
                        ' shared reply queue is read without matching on the
                        ' correlation id.
                        Console.WriteLine(
                            $"answered {answering.Answered}, timed out {asking.TimedOut}, " &
                            $"unmatched replies {asking.Unmatched}")
                    End Using
                End Using

                ' The example cleans up after itself so a second run reports the
                ' same numbers as the first. A service would leave the queues
                ' alone. The reply queue needs no cleaning: it carries
                ' x-expires, so a process killed without disposing leaves
                ' nothing behind for an afternoon.
                For Each queue In New String() {Served, ByHand, Unanswered}
                    Await mq.DeleteQueueAsync(queue)
                    Await mq.DeleteQueueAsync(queue & ".dlq")
                    Await mq.DeleteQueueAsync(queue & ".parked")
                Next
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
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
