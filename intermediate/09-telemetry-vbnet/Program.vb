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

' One trace across two hops, and the same traffic scraped as Prometheus metrics,
' in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/09-telemetry-vbnet
'
' An order is placed, a handler ships it by publishing again, and a second
' handler hears about the shipment. Four operations, two trips through the
' broker -- and one trace, because the context travels on the message as a
' traceparent header and each consumer continues the trace that sent it.
'
' Then the actuator from AceMq.Amqp.Diagnostics is scraped over HTTP the way
' Prometheus would scrape it, and the counters are checked against the work that
' was actually done.
'
' The library depends on neither OpenTelemetry nor Prometheus. It records to the
' runtime's own ActivitySource and Meter, both named "AceMq.Amqp", and the
' application decides what listens.

Imports System.Diagnostics
Imports System.Globalization
Imports System.Linq
Imports System.Net.Http
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.Diagnostics
Imports AceMq.Amqp.RabbitMq

Imports OpenTelemetry
Imports OpenTelemetry.Trace

Public Class OrderPlaced
    Public Property OrderId As String = ""
End Class

Public Class OrderShipped
    Public Property OrderId As String = ""
    Public Property Tracking As String = ""
End Class

Module Program

    ' Distinct from the C# example's queues for the usual reason: both run
    ' against one broker on CI.
    Private Const Placed As String = "dotnet-vbnet-telemetry-placed"
    Private Const Shipped As String = "dotnet-vbnet-telemetry-shipped"

    ' Not 9464, the OpenTelemetry convention and the actuator's default, because
    ' something on a developer's machine is often already there. One along from
    ' the C# example's, so the two can run side by side.
    Private Const MetricsPort As Integer = 9472

    Private ReadOnly Heard As New TaskCompletionSource(Of Boolean)(
        TaskCreationOptions.RunContinuationsAsynchronously)
    Private ArrivedWith As String = ""

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            ' The whole of the tracing setup, and none of it is AceMQ's API: the
            ' library's spans come from an ActivitySource, and AddSource is how
            ' any OpenTelemetry pipeline subscribes to one. A real service adds an
            ' OTLP exporter here instead of the in-memory one, and nothing else
            ' changes.
            Dim spans As New List(Of Activity)
            Using tracing = Sdk.CreateTracerProviderBuilder() _
                    .AddSource(MetricNames.ActivitySource) _
                    .AddInMemoryExporter(spans) _
                    .Build()

                Dim mq = Await AceMqConnection.ConnectAsync(
                    ConnectionConfig.ForUrl(BrokerUrl()) _
                        .ClientName("examples/09-telemetry-vbnet") _
                        .Build())

                Try
                    ' Started before any traffic. The collector counts from the
                    ' moment it starts listening, so a scrape only ever describes
                    ' what happened after that -- which is also what a restarted
                    ' process looks like to Prometheus, and why its counters are
                    ' allowed to go back to zero.
                    Using actuator = AceMqActuator.Start(mq, New ActuatorOptions With {.Port = MetricsPort})
                        Await RunAgainst(mq, actuator, spans, token)
                    End Using

                    ' Drains the consumers, then closes. VB.NET cannot Await
                    ' inside a Finally, so the drain is the last thing the Try
                    ' does and the Finally below is only the backstop for a
                    ' failure -- Dispose after CloseAsync does nothing.
                    Await mq.CloseAsync()
                Finally
                    mq.Dispose()
                End Try
            End Using

            Return 0
        End Using
    End Function

    Private Async Function RunAgainst(
        mq As AceMqConnection, actuator As AceMqActuator,
        spans As List(Of Activity), token As CancellationToken) As Task

        Await mq.DeclareQueueAsync(Placed)
        Await mq.DeclareQueueAsync(Shipped)

        Using shipping = Await mq.ConsumeAsync(Of OrderPlaced)(Placed,
            Async Function(message As IMessage(Of OrderPlaced)) As Task(Of Ack)
                ArrivedWith = Text(message.WireHeaders, "traceparent")

                ' A publish from inside a handler. It joins the trace of the
                ' message being handled, because the handler's span is
                ' Activity.Current while this runs -- which is what turns two
                ' services into one picture instead of two unrelated ones.
                Await mq.Publisher(Of OrderShipped)("", Shipped).SendAsync(
                    New OrderShipped With {.OrderId = message.Payload.OrderId, .Tracking = "TRK-1"})
                Return Ack.Accept()
            End Function)

            Using notifying = Await mq.ConsumeAsync(Of OrderShipped)(Shipped,
                Function(message As IMessage(Of OrderShipped)) As Task(Of Ack)
                    Heard.TrySetResult(True)
                    Return Task.FromResult(Ack.Accept())
                End Function)

                Await mq.Publisher(Of OrderPlaced)("", Placed).SendAsync(
                    New OrderPlaced With {.OrderId = "o-1"})

                Await Heard.Task.WaitAsync(token)

                ' A span reaches the exporter when it ends, and the last one ends
                ' just after its handler returns -- so hearing about the shipment
                ' is not quite the same moment as having four spans.
                Await WaitFor(Function() spans.Count >= 4, token)
            End Using
        End Using

        ' Copied once, now that nothing else is running that could end a span.
        ' The exporter appends from whichever thread ended one, so the list is
        ' not something to enumerate while traffic is still moving.
        Dim recorded = spans.ToList()

        Dim placedPublish = Find(recorded, Placed & MetricNames.SpanPublishSuffix)
        Dim placedProcess = Find(recorded, Placed & MetricNames.SpanProcessSuffix)
        Dim shippedPublish = Find(recorded, Shipped & MetricNames.SpanPublishSuffix)
        Dim shippedProcess = Find(recorded, Shipped & MetricNames.SpanProcessSuffix)
        Dim chain = {placedPublish, placedProcess, shippedPublish, shippedProcess}

        For Each span In chain
            Dim parent = If(span.ParentSpanId = Nothing, "(root)", span.ParentSpanId.ToHexString())
            Console.WriteLine(
                $"{span.DisplayName,-40} {span.GetTagItem(AceMqTelemetry.AttrOutcome),-9} " &
                $"{span.TraceId.ToHexString()}  parent={parent}")
        Next

        Dim traces = recorded.Select(Function(s) s.TraceId).Distinct().Count()
        Dim roots = recorded.Where(Function(s) s.ParentSpanId = Nothing).Count()
        Console.WriteLine()
        Console.WriteLine($"spans {recorded.Count}, traces {traces}, roots {roots}")
        Console.WriteLine($"o-1 arrived carrying traceparent {ArrivedWith}")

        Check(recorded.Count = 4, $"{recorded.Count} spans were recorded, not four")
        Check(traces = 1,
              $"four operations landed in {traces} traces, so the context was lost on the way")
        Check(roots = 1,
              $"{roots} spans have no parent; only the first publish should be a root")

        ' The join, link by link. One trace id is necessary and not sufficient:
        ' four spans could share a trace and still name the wrong parents.
        Check(placedProcess.ParentSpanId = placedPublish.SpanId,
              "the first consumer's span is not a child of the publish that sent it")
        Check(shippedPublish.ParentSpanId = placedProcess.SpanId,
              "the publish inside the handler did not join the handler's trace")
        Check(shippedProcess.ParentSpanId = shippedPublish.SpanId,
              "the second consumer's span is not a child of the publish that sent it")

        ' And where it came from: the header on the wire names exactly the trace
        ' and the span the publisher reported. W3C's format, so a Java, Go,
        ' Python or Ruby consumer reading this message joins the same trace.
        Dim expected = $"00-{placedPublish.TraceId.ToHexString()}-{placedPublish.SpanId.ToHexString()}-01"
        Check(ArrivedWith = expected,
              $"the message carried traceparent {ArrivedWith}, which is not the span that published it")

        ' The outcome is on the span as well as in the counter, so a dashboard
        ' showing failures and a trace search for them find the same messages.
        Check(chain.All(Function(s)
                            Dim outcome = TryCast(s.GetTagItem(AceMqTelemetry.AttrOutcome), String)
                            Return outcome = "confirmed" OrElse outcome = "acked"
                        End Function),
              "a span is missing its outcome")

        Console.WriteLine()

        ' Scraped over HTTP, the way Prometheus does it, rather than by calling
        ' actuator.Metrics() -- the endpoint is the thing a scrape job depends
        ' on, content type included.
        Using http As New HttpClient()
            Dim scrape = ""
            Await WaitFor(Function()
                              scrape = http.GetStringAsync(actuator.Url & "acemq-metrics", token) _
                                  .GetAwaiter().GetResult()
                              Return Sample(scrape, "acemq_consume_total", Shipped) >= 1
                          End Function, token)

            For Each line In scrape.Split(ControlChars.Lf)
                If (line.StartsWith("acemq_publish_total{") OrElse
                    line.StartsWith("acemq_consume_total{") OrElse
                    line.StartsWith("acemq_consume_duration_seconds_count{")) AndAlso
                   (line.Contains(Placed) OrElse line.Contains(Shipped)) Then
                    Console.WriteLine(line)
                End If
            Next

            Dim metrics = Await http.GetAsync(actuator.Url & "acemq-metrics", token)
            Dim answer = Await http.GetAsync(actuator.Url & "acemq-health", token)
            Dim answerBody = Await answer.Content.ReadAsStringAsync(token)
            Dim answerCode = CInt(answer.StatusCode)
            Console.WriteLine()
            Console.WriteLine($"{answerCode} {answerBody.Trim()}")

            ' Exactly one each, because exactly one of each happened. A counter
            ' that reads two here is counting something twice, and a dashboard
            ' built on it doubles every rate it shows.
            For Each name In {Placed, Shipped}
                Check(Sample(scrape, "acemq_publish_total", name, "confirmed") = 1,
                      $"the scrape does not show one confirmed publish to {name}")
                Check(Sample(scrape, "acemq_consume_total", name, "acked") = 1,
                      $"the scrape does not show one acknowledged delivery on {name}")
                Check(Sample(scrape, "acemq_consume_duration_seconds_count", name) = 1,
                      $"the handler on {name} was not timed exactly once")
            Next

            ' Prometheus refuses a scrape without this, version and all.
            Dim served = If(metrics.Content.Headers.ContentType?.ToString(), "")
            Check(served.StartsWith("text/plain; version=0.0.4"),
                  $"the metrics were served as {served}, which Prometheus will not parse")
            Check(answerCode = 200 AndAlso answerBody.Contains("""status"":""UP"""),
                  $"health answered {answerCode} {answerBody.Trim()} for a connection that is open")
        End Using

        ' The actuator binds to localhost on purpose. Those endpoints are not
        ' authenticated and they name every queue and its traffic, so binding to
        ' 0.0.0.0 publishes that to anything that can reach the port. Let the
        ' scraper reach it through the same host, a sidecar, or a network policy
        ' instead.

        Await mq.DeleteQueueAsync(Placed)
        Await mq.DeleteQueueAsync(Placed & ".dlq")
        Await mq.DeleteQueueAsync(Placed & ".parked")
        Await mq.DeleteQueueAsync(Shipped)
        Await mq.DeleteQueueAsync(Shipped & ".dlq")
        Await mq.DeleteQueueAsync(Shipped & ".parked")
    End Function

    Private Function Find(recorded As List(Of Activity), name As String) As Activity
        Dim found = recorded.SingleOrDefault(Function(s) s.DisplayName = name)
        If found Is Nothing Then
            Throw New InvalidOperationException($"no span called ""{name}"" was recorded")
        End If
        Return found
    End Function

    ' One series from the Prometheus text format: the line for this metric whose
    ' labels mention every one of the given values. Zero when there is no such
    ' line, which is what Prometheus would make of it too.
    Private Function Sample(scrape As String, metric As String, ParamArray labelled As String()) As Double
        For Each line In scrape.Split(ControlChars.Lf)
            If Not line.StartsWith(metric & "{") Then Continue For
            Dim labels = line.Substring(0, line.LastIndexOf("}"c))
            If labelled.All(Function(value) labels.Contains($"""{value}""")) Then
                Return Double.Parse(line.Substring(line.LastIndexOf(" "c) + 1), CultureInfo.InvariantCulture)
            End If
        Next
        Return 0
    End Function

    Private Function Text(headers As IReadOnlyDictionary(Of String, Object), name As String) As String
        Dim value As Object = Nothing
        If Not headers.TryGetValue(name, value) Then Return ""
        Return If(Convert.ToString(value, CultureInfo.InvariantCulture), "")
    End Function

    Private Async Function WaitFor(held As Func(Of Boolean), token As CancellationToken) As Task
        While Not held()
            token.ThrowIfCancellationRequested()
            Await Task.Delay(20, token)
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
