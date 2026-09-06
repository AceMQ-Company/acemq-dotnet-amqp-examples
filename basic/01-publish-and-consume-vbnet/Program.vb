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

' Publishing a message and consuming it, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/01-publish-and-consume-vbnet
'
' The same example as its C# neighbour, because the library is one assembly and
' both languages call it the same way. That is the point of having this: a VB
' team should not have to translate C# to find out whether the library suits
' them.

' VB.NET's implicit imports are a shorter list than C#'s, so threading and
' tasks are named here. An example that does not compile teaches nothing.
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
        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(30))
            Dim token = cancellation.Token

            ' The transport has to be registered before a broker URL can be
            ' resolved. Referencing the package is not enough: nothing in this
            ' program touches that assembly otherwise, so it is never loaded.
            Transports.Register(New RabbitMqTransport())

            ' ClientName is what RabbitMQ's management interface shows.
            Dim config = ConnectionConfig.ForUrl(BrokerUrl()) _
                .ClientName("examples/01-publish-and-consume-vbnet") _
                .Build()

            Dim mq = Await AceMqConnection.ConnectAsync(config)

            Try
                Await mq.DeclareQueueAsync("orders")

                Dim arrived As New TaskCompletionSource(Of IMessage(Of OrderPlaced))(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using consumer = Await mq.ConsumeAsync(Of OrderPlaced)(
                    "orders",
                    Function(message)
                        arrived.TrySetResult(message)
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim publisher = mq.Publisher(Of OrderPlaced)("", "orders")

                    Dim order As New OrderPlaced With {.OrderId = "o-1", .TotalCents = 4250}
                    Dim result = Await publisher.SendAsync(order)
                    Console.WriteLine(
                        $"published {result.MessageId}, routed by the broker: {result.Routed}, " &
                        $"took {result.Latency.TotalMilliseconds:F1}ms")

                    Dim received = Await arrived.Task.WaitAsync(token)
                    Console.WriteLine(
                        $"consumed  {received.Envelope.Id}: order {received.Payload.OrderId} " &
                        $"for {received.Payload.TotalCents} cents")
                    Console.WriteLine(
                        $"          type=""{received.Envelope.Type}"" attempt={received.Envelope.Attempt} " &
                        $"origin={received.Envelope.Origin}")
                End Using
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    ' The compose broker unless ACEMQ_URL names another.
    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
