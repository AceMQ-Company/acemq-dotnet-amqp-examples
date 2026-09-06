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

' A topology described once, planned, then applied — in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/02-topology-as-data-vbnet

Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class OrderPlaced
    Public Property OrderId As String = ""
End Class

Module Program

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(30))
            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/02-topology-as-data-vbnet") _
                    .Build())

            Try
                ' Named "wanted" rather than "topology": VB.NET is case-insensitive, so a
                ' variable called topology collides with the Topology type.
                Dim wanted = Topology.Define() _
                    .Exchange("orders-events", "topic") _
                    .QueueWithDeadLetter("shipping-orders") _
                    .Bind("shipping-orders", "orders-events", "order.placed") _
                    .Bind("shipping-orders", "orders-events", "order.cancelled") _
                    .Build()

                ' DryRun changes nothing, so a deployment can be read first.
                Dim plan = Await mq.ApplyAsync(wanted, ApplyMode.DryRun)
                Console.WriteLine("what applying this would do:")
                For Each action In plan.Actions
                    Console.WriteLine($"  {action}")
                Next

                Dim applied = Await mq.ApplyAsync(wanted)
                Console.WriteLine($"applied: {applied.Actions.Count} action(s)")

                Await mq.ApplyAsync(wanted)
                Console.WriteLine("applied again, unchanged, without complaint")

                Dim arrived As New TaskCompletionSource(Of OrderPlaced)(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using consumer = Await mq.ConsumeAsync(Of OrderPlaced)(
                    "shipping-orders",
                    Function(message)
                        arrived.TrySetResult(message.Payload)
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim order As New OrderPlaced With {.OrderId = "o-1"}
                    Await mq.Publisher(Of OrderPlaced)("orders-events", "order.placed").SendAsync(order)

                    Dim routed = Await arrived.Task.WaitAsync(cancellation.Token)
                    Console.WriteLine($"routed end to end: {routed.OrderId}")
                End Using
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
