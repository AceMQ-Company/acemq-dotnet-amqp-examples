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

' Choosing what goes on the wire, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project basic/06-serialization-vbnet
'
' JSON is the default and is usually right. This is the other case: a queue
' that has to be read while two formats are in flight — because a publisher
' somewhere still sends XML, or because a migration is half done and turning
' both ends off at the same instant was never an option.

Imports System.Collections.Generic
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Order
    Public Property OrderId As String = ""
    Public Property TotalCents As Long
End Class

Module Program

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            ' A codec belongs to a connection: everything published on it is
            ' encoded the same way, and everything consumed is read the same way.
            '
            ' The consumer here reads both. A composite decodes by the content type
            ' on the message rather than by guessing, and encodes with the first
            ' codec it was given — so the order of the arguments is the format this
            ' connection publishes.
            Dim reader = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/06-serialization-vb-reader") _
                    .Build(),
                CompositeCodec.Of(New JsonCodec(), New XmlCodec()),
                token)

            Try
                Await reader.DeclareQueueAsync("serialization-orders-vb")

                Dim arrived As New List(Of String)()
                Dim both As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using consumer = Await reader.ConsumeAsync(Of Order)(
                    "serialization-orders-vb",
                    Function(message)
                        SyncLock arrived
                            arrived.Add(message.Payload.OrderId)
                            If arrived.Count = 2 Then both.TrySetResult(True)
                        End SyncLock
                        Console.WriteLine(
                            $"  read {message.Payload.OrderId} ({message.Payload.TotalCents}) " &
                            $"sent as {message.ContentType}")
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    ' One publisher speaking JSON...
                    Dim json = Await AceMqConnection.ConnectAsync(
                        ConnectionConfig.ForUrl(BrokerUrl()) _
                            .ClientName("examples/06-serialization-vb-json") _
                            .Build())
                    Try
                        Await json.Publisher(Of Order)("", "serialization-orders-vb").SendAsync(
                            New Order With {.OrderId = "o-json", .TotalCents = 1999})
                    Finally
                        json.Dispose()
                    End Try

                    ' ...and one that has not been migrated yet.
                    Dim xml = Await AceMqConnection.ConnectAsync(
                        ConnectionConfig.ForUrl(BrokerUrl()) _
                            .ClientName("examples/06-serialization-vb-xml") _
                            .Build(),
                        New XmlCodec(),
                        token)
                    Try
                        Await xml.Publisher(Of Order)("", "serialization-orders-vb").SendAsync(
                            New Order With {.OrderId = "o-xml", .TotalCents = 4500})
                    Finally
                        xml.Dispose()
                    End Try

                    Await both.Task.WaitAsync(token)
                    Console.WriteLine(
                        $"both formats read off one queue: {String.Join(", ", arrived)}")
                End Using

                ' What else is available. XML and JSON are in the main package;
                ' YAML, TOML, Protobuf and Avro are packages of their own, so a
                ' service pays for the formats it uses and nothing else.
                Console.WriteLine($"codecs registered here: {String.Join(", ", CodecRegistry.Names())}")

                ' The example cleans up after itself so a second run reports the
                ' same numbers as the first. A service would leave the queue alone.
                Await reader.DeleteQueueAsync("serialization-orders-vb")
            Finally
                reader.Dispose()
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
