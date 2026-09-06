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

' Interceptors: cross-cutting work in one place, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/01-interceptors-vbnet

Imports System.Diagnostics
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Invoice
    Public Property InvoiceId As String = ""
End Class

' Inheriting the abstract base rather than implementing the interface: VB.NET
' cannot use default interface members, so this is the only way to override one
' moment without writing the other two empty.
Public Class TenantStamp
    Inherits PublishInterceptor

    Private ReadOnly _tenant As String

    Public Sub New(tenant As String)
        _tenant = tenant
    End Sub

    Public Overrides Function BeforePublish(context As PublishContext) As PublishContext
        ' The envelope is the only part an interceptor may change. .NET's
        ' Envelope has no ToBuilder, so the fields are carried across by hand.
        Dim stamped = Envelope.Of(context.Envelope.Type) _
            .Id(context.Envelope.Id) _
            .Version(context.Envelope.Version) _
            .CorrelationId(context.Envelope.CorrelationId) _
            .CausationId(context.Envelope.CausationId) _
            .Attempt(context.Envelope.Attempt) _
            .FirstSeen(context.Envelope.FirstSeen) _
            .Origin(context.Envelope.Origin) _
            .Header("x-tenant", _tenant)

        For Each header In context.Envelope.Headers
            stamped.Header(header.Key, header.Value)
        Next

        Return context.WithEnvelope(stamped.Build())
    End Function

    Public Overrides Sub AfterConfirm(context As PublishContext, result As PublishResult)
        Console.WriteLine($"  [publish] {result.MessageId} confirmed in {result.Latency.TotalMilliseconds:F1}ms")
    End Sub
End Class

Public Class Timing
    Inherits ConsumeInterceptor

    Private ReadOnly _clock As New Stopwatch()

    Public Overrides Sub BeforeHandle(context As ConsumeContext)
        _clock.Restart()
    End Sub

    Public Overrides Sub AfterHandle(context As ConsumeContext, ack As Ack)
        Console.WriteLine($"  [consume] {context.Envelope.Id} -> {ack.Kind} in {_clock.Elapsed.TotalMilliseconds:F1}ms")
    End Sub
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
                    .ClientName("examples/01-interceptors-vbnet") _
                    .Build())

            Try
                mq.Intercept(New TenantStamp("acme"))
                mq.Intercept(New Timing())

                Await mq.DeclareQueueAsync("invoices")

                Dim arrived As New TaskCompletionSource(Of IMessage(Of Invoice))(
                    TaskCreationOptions.RunContinuationsAsynchronously)

                Using consumer = Await mq.ConsumeAsync(Of Invoice)(
                    "invoices",
                    Function(message)
                        arrived.TrySetResult(message)
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Dim invoice As New Invoice With {.InvoiceId = "inv-1"}
                    Await mq.Publisher(Of Invoice)("", "invoices").SendAsync(invoice)

                    Dim received = Await arrived.Task.WaitAsync(cancellation.Token)
                    Console.WriteLine(
                        $"the handler received {received.Payload.InvoiceId}, " &
                        $"stamped for tenant {received.Headers("x-tenant")}")
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
