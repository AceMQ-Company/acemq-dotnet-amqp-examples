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

' A message travelling several hops, and picked up where it stopped, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/08-pipelines-vbnet
'
' Three steps, each on its own queue, and a card processor that is down the
' first time. The order dead-letters at `charge`, and is then put back at CHARGE
' rather than at the entrance -- so `validate` runs once, not twice.
'
' That is what the routing slip buys. Every message carries one, naming the
' whole route and how far along it is, so a message dead-lettered at step two
' still says it is at step two. Without one, the only safe place to replay a
' half-finished message is the beginning, and every step it already passed runs
' again -- which a step that charges a card cannot survive.
'
' The same run happens twice, once in each of the two wire forms AceMQ has for a
' slip, because they are read and written by different languages.

Imports System.Collections.Concurrent
Imports System.Collections.Generic
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Order
    Public Property OrderId As String = ""

    ''' <summary>What has been done to it, so a repeated step is visible in the payload.</summary>
    Public Property Trail As String = ""
End Class

Module Program

    ' Named for the slip form rather than called Declared and Itinerary: both of
    ' those are SlipForm members, and VB.NET is case-insensitive, so a constant
    ' of either name shadows the enum where it is used.
    '
    ' Distinct from the C# example's pipelines for the usual reason: both run
    ' against one broker on CI.
    Private Const ByName As String = "dotnet-vbnet-fulfilment"
    Private Const ByAddress As String = "dotnet-vbnet-fulfilment-by-address"

    Private ReadOnly Ran As New ConcurrentQueue(Of String)

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(120))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/08-pipelines-vbnet") _
                    .Build())

            Try
                Await DeclaredForm(mq, token)
                Console.WriteLine()
                Await ItineraryForm(mq, token)
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    ''' <summary>The default: step names in x-acemq-route, which is what Java writes.</summary>
    Private Async Function DeclaredForm(mq As AceMqConnection, token As CancellationToken) As Task
        Dim chargeFails = True
        Dim shipped As New TaskCompletionSource(Of Order)(
            TaskCreationOptions.RunContinuationsAsynchronously)

        ' Each step is a queue, and that is what separates this from calling
        ' three methods in a row: a step that fails retries on its own, a slow
        ' step builds a visible backlog instead of blocking the ones before it,
        ' and each step scales independently.
        '
        ' The two type parameters make the chain check at compile time -- a step
        ' added after one producing Order can only accept an Order -- so a
        ' mismatch is a compile error rather than a decode failure at the third
        ' step in production.
        '
        ' Named "chain" rather than "pipeline": VB.NET is case-insensitive, so a
        ' variable called pipeline collides with the Pipeline(Of T) type.
        Dim chain = Await mq.Pipeline(Of Order)(ByName) _
            .Step(Of Order)("validate",
                Function(item As Order) Task.FromResult(Did("validate", item))) _
            .Step(Of Order)("charge",
                Function(item As Order) As Task(Of Order)
                    Ran.Enqueue("charge")
                    ' Fatal rather than ordinary: a fatal failure dead-letters
                    ' immediately instead of retrying, which is what puts the
                    ' message somewhere an operator can find it.
                    If chargeFails Then Throw New AceFatalException("the card processor is down")
                    Return Task.FromResult(Did("charged", item, counted:=False))
                End Function) _
            .Step(Of Order)("ship",
                Function(item As Order) As Task(Of Order)
                    Dim done = Did("ship", item)
                    shipped.TrySetResult(done)
                    Return Task.FromResult(done)
                End Function) _
            .BuildAsync()

        ' Nothing declares {queue}.dlq here, and that is deliberate. Building the
        ' pipeline started a consumer on every step queue, and a consumer
        ' declares its own {queue}.dlq and {queue}.parked as it starts. Adding
        ' `Await mq.DeclareQueueAsync(chain.QueueFor("charge") & ".dlq")` after
        ' this line is a PRECONDITION_FAILED rather than a no-op: that overload
        ' declares a QUORUM queue, and the dead-letter queues are CLASSIC, which
        ' is what Java and Topology declare them as.

        Try
            Console.WriteLine($"pipeline    {chain.Name}: {String.Join(" -> ", chain.StepNames)}")
            Console.WriteLine($"slip form   {chain.SlipForm}, in {RoutingSlip.RouteHeader}")

            Await chain.SendAsync(New Order With {.OrderId = "order-1"})

            ' What an operator finds in the dead-letter queue: the message
            ' exactly as it was when it failed, headers and all.
            Dim stuck = Await StuckAt(mq, chain.QueueFor("charge"), token)
            Await stuck.AcknowledgeAsync()

            Dim stalled = CType(Route.From(stuck.WireHeaders), RoutingSlip)
            Console.WriteLine(
                $"stalled at  {stalled.Current}, position {stalled.Position} of " &
                $"[{String.Join(", ", stalled.Steps)}]")
            Console.WriteLine(
                $"on the wire {RoutingSlip.RouteHeader}: " &
                HeaderText(stuck.WireHeaders, RoutingSlip.RouteHeader))
            Console.WriteLine($"payload     {stuck.Payload.Trail}")

            ' The card processor comes back.
            chargeFails = False

            ' ResumeAsync, not SendAsync. SendAsync would start the run over,
            ' repeating every step before the failure; the slip among these
            ' headers is what says which step this message had reached, and there
            ' is nothing else on the message that does.
            Await chain.ResumeAsync(stuck.Payload, stuck.WireHeaders)
            Dim delivered = Await shipped.Task.WaitAsync(token)

            Console.WriteLine($"resumed     ran [{String.Join(", ", Ran)}]")
            Console.WriteLine($"delivered   {delivered.OrderId}: {delivered.Trail}")
            Console.WriteLine(
                $"counters    entered {chain.Entered}, completed {chain.Completed}, " &
                $"ended early {chain.EndedEarly}, in flight {chain.InFlight}")

            Check(stalled.Current = "charge",
                  $"the slip said the message was at {stalled.Current}, not at charge")
            Check(stalled.Position = 1, $"the slip said position {stalled.Position}, not 1")
            Check(stalled.Steps.SequenceEqual(New String() {"validate", "charge", "ship"}),
                  $"the slip named [{String.Join(", ", stalled.Steps)}], " &
                  "not the pipeline's own steps")
            Check(HeaderText(stuck.WireHeaders, RoutingSlip.RouteHeader) = "validate,charge,ship",
                  "the route on the wire is not the comma-joined form Java reads")

            ' The claim the whole example exists for. A restart would have re-run
            ' every step before the failure; resuming runs the step that failed
            ' and everything after it, and nothing else.
            Check(Ran.ToArray().SequenceEqual(
                      New String() {"validate", "charge", "charge", "ship"}),
                  $"the steps that ran were [{String.Join(", ", Ran)}], and a resume that " &
                  "repeats validate is a restart wearing a different name")
            Check(delivered.Trail = "validate,charged,ship",
                  $"the delivered order says '{delivered.Trail}', " &
                  "so a step ran that should not have")
            Check(chain.Entered = 1 AndAlso chain.Completed = 1 AndAlso chain.InFlight = 0,
                  $"the pipeline counted {chain.Entered} in and {chain.Completed} out")
        Finally
            chain.Dispose()
        End Try

        Await Clean(mq, chain)
    End Function

    ''' <summary>
    ''' The other form: a JSON itinerary in acemq-routing-slip, carrying each stop's
    ''' address rather than its name.
    ''' </summary>
    ''' <remarks>
    ''' What Go, Python and Ruby write. Worth choosing when a consumer in one of those
    ''' reads one of these queues, or when a stop is somewhere this library has not
    ''' declared: the message carries its own addresses, so nothing has to be resolved
    ''' against anything. Either form is READ whatever this is set to.
    ''' </remarks>
    Private Async Function ItineraryForm(mq As AceMqConnection, token As CancellationToken) As Task
        Dim shipFails = True
        Dim shipped As New TaskCompletionSource(Of Order)(
            TaskCreationOptions.RunContinuationsAsynchronously)

        Dim chain = Await mq.Pipeline(Of Order)(ByAddress) _
            .WritingSlipAs(SlipForm.Itinerary) _
            .Step(Of Order)("validate",
                Function(item As Order) Task.FromResult(Did("validate", item, counted:=False))) _
            .Step(Of Order)("ship",
                Function(item As Order) As Task(Of Order)
                    If shipFails Then Throw New AceFatalException("the label printer is offline")
                    Dim done = Did("ship", item, counted:=False)
                    shipped.TrySetResult(done)
                    Return Task.FromResult(done)
                End Function) _
            .BuildAsync()

        Try
            Console.WriteLine($"pipeline    {chain.Name}: {String.Join(" -> ", chain.StepNames)}")
            Console.WriteLine($"slip form   {chain.SlipForm}, in {Itinerary.Header}")

            Await chain.SendAsync(New Order With {.OrderId = "order-2"})
            Dim stuck = Await StuckAt(mq, chain.QueueFor("ship"), token)
            Await stuck.AcknowledgeAsync()

            Dim stalled = CType(Route.From(stuck.WireHeaders), Itinerary)
            Dim wire = HeaderText(stuck.WireHeaders, Itinerary.Header)
            Console.WriteLine($"on the wire {Itinerary.Header}: {wire}")
            Console.WriteLine(
                $"stalled at  {stalled.Next.Name} on {stalled.Next.RoutingKey}, " &
                $"after {String.Join(", ", stalled.Done.Select(Function(s) s.Name))}")

            shipFails = False
            Await chain.ResumeAsync(stuck.Payload, stuck.WireHeaders)
            Dim delivered = Await shipped.Task.WaitAsync(token)
            Console.WriteLine($"delivered   {delivered.OrderId}: {delivered.Trail}")

            ' The queue rather than the bare step name, because an itinerary is
            ' meant to be followed by something that has never heard of this
            ' pipeline and so cannot turn `ship` into `<pipeline>.ship`.
            Check(stalled.Next.RoutingKey = chain.QueueFor("ship"),
                  $"the itinerary points at {stalled.Next.RoutingKey}, not at a queue " &
                  "anything could publish to without knowing this pipeline")
            Check(stalled.Next.Exchange.Length = 0,
                  $"the next stop names exchange '{stalled.Next.Exchange}'; a pipeline's " &
                  "steps are queues on the default exchange")
            Check(stalled.Done.Count = 1 AndAlso stalled.Done(0).Name = "validate",
                  "the itinerary does not say which steps are already behind it")
            Check(stalled.Done(0).CompletedAt.EndsWith("Z", StringComparison.Ordinal),
                  $"a completed step is stamped '{stalled.Done(0).CompletedAt}', which is not " &
                  "the RFC 3339 UTC every one of the five libraries parses")
            Check(wire.Contains("""routingKey""", StringComparison.Ordinal),
                  $"the slip on the wire is {wire}, " &
                  "which is not the JSON the other libraries read")

            ' And the point worth making twice: resuming does not care which form
            ' the slip is in. Both say where the message was, and that is all
            ' ResumeAsync needs.
            Check(delivered.Trail = "validate,ship",
                  $"the delivered order says '{delivered.Trail}', " &
                  "so resuming an itinerary re-ran a step")
        Finally
            chain.Dispose()
        End Try

        Await Clean(mq, chain)
    End Function

    ''' <summary>Records a step and stamps the payload, so a repeat shows up in both.</summary>
    ''' <remarks>
    ''' The first parameter is called "name" rather than "step": Step is a VB.NET
    ''' keyword, from For ... Step, and cannot be a parameter name.
    ''' </remarks>
    Private Function Did(name As String, item As Order, Optional counted As Boolean = True) As Order
        If counted Then Ran.Enqueue(name)
        Return New Order With {
            .OrderId = item.OrderId,
            .Trail = If(item.Trail.Length = 0, name, item.Trail & "," & name)}
    End Function

    Private Async Function StuckAt(
        mq As AceMqConnection, stepQueue As String,
        token As CancellationToken) As Task(Of PulledMessage(Of Order))

        While True
            token.ThrowIfCancellationRequested()
            Dim pulled = Await mq.PullAsync(Of Order)(
                stepQueue & ".dlq", TimeSpan.FromMilliseconds(100))
            If pulled IsNot Nothing Then Return pulled
        End While

        ' Unreachable, and VB.NET still wants it: the loop condition is a
        ' constant, so the compiler does not treat the End Function as
        ' unreachable the way C# does.
        Return Nothing
    End Function

    ''' <summary>A header as text, whichever of the two the transport hands back.</summary>
    Private Function HeaderText(
        headers As IReadOnlyDictionary(Of String, Object), name As String) As String

        Dim value As Object = Nothing
        If Not headers.TryGetValue(name, value) OrElse value Is Nothing Then Return "(absent)"

        Dim text = TryCast(value, String)
        If text IsNot Nothing Then Return text
        Return System.Text.Encoding.UTF8.GetString(CType(value, Byte()))
    End Function

    Private Async Function Clean(mq As AceMqConnection, chain As Pipeline(Of Order)) As Task
        For Each name In chain.StepNames
            Dim stepQueue = chain.QueueFor(name)
            Await mq.DeleteQueueAsync(stepQueue)
            Await mq.DeleteQueueAsync(stepQueue & ".dlq")
            Await mq.DeleteQueueAsync(stepQueue & ".parked")
        Next
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
