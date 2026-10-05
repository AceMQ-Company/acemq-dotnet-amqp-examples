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

' Takes the money.
'
' This is the service where at-least-once delivery stops being a technicality.
' Every other service in this system can handle a message twice and produce the
' same outcome; this one cannot, because the second charge is real money
' belonging to a real customer.
'
' So it claims each order in a shared store before charging, and confirms
' afterwards. The store is shared rather than in-memory because there is more
' than one instance of this service in production, and an in-memory store makes
' each instance individually idempotent while the fleet is not.

Imports System.Runtime.ExceptionServices
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class PaymentsService

    ' Over this, a human has to look at it. Every payment system has one.
    Private Const AutomaticLimit As Double = 1000.0

    Private ReadOnly _mq As AceMqConnection
    Private ReadOnly _charged As DbIdempotencyStore
    Private ReadOnly _capturedEvents As IPublisher(Of PaymentCaptured)
    Private ReadOnly _declinedEvents As IPublisher(Of PaymentDeclined)
    Private _captures As Integer
    Private _declines As Integer
    Private _duplicatesRefused As Integer

    Private Sub New(mq As AceMqConnection, database As ConnectionSupplier)
        _mq = mq

        ' A generous claim timeout: it has to outlast the slowest charge, because
        ' a claim that expires while the payment provider is still thinking is a
        ' claim another instance will take, and then the customer pays twice.
        _charged = New DbIdempotencyStore(
            database, TimeSpan.FromDays(7), "payments_handled", "@", TimeSpan.FromMinutes(2))

        ' Built once. A publisher is meant to live as long as the service does,
        ' and the connection keeps every one it hands out until it closes.
        _capturedEvents = mq.Publisher(Of PaymentCaptured)(Contract.Exchange, Contract.PaymentCapturedKey)
        _declinedEvents = mq.Publisher(Of PaymentDeclined)(Contract.Exchange, Contract.PaymentDeclinedKey)
    End Sub

    Public Shared Async Function StartAsync(url As String, database As ConnectionSupplier) As Task(Of PaymentsService)
        Dim mq = Await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-vbnet/payments").Build())
        Await mq.ApplyAsync(Contract.Everything())

        Dim payments As New PaymentsService(mq, database)
        Using connection = database()
            connection.Open()
            Using command = connection.CreateCommand()
                command.CommandText = payments._charged.CreateTableSql()
                command.ExecuteNonQuery()
            End Using
        End Using

        ' Explicit JSON: these messages come from the gateway's outbox, which
        ' stores already-serialised bytes and republishes them as they were.
        Dim options = ConsumerOptions.Prefetch(20) _
            .As(CodecRegistry.ByName("json")) _
            .WithRetry(RetryPolicy.Exponential(4, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)))

        Await mq.ConsumeAsync(Of OrderPlaced)(Contract.Payments, options, AddressOf payments.ChargeAsync)
        Return payments
    End Function

    Private Async Function ChargeAsync(message As IMessage(Of OrderPlaced)) As Task(Of Ack)
        Dim placed = message.Payload

        ' The claim is the whole safety net. A redelivery -- from a broker
        ' restart, a consumer that died mid-handle, or a relay that published
        ' twice -- loses here.
        If Not Await _charged.ClaimAsync(placed.OrderId) Then
            Interlocked.Increment(_duplicatesRefused)
            Return Ack.Accept()
        End If

        ' Caught and held rather than handled in the Catch, because VB.NET cannot
        ' Await inside one, and releasing the claim is an Await.
        Dim failure As ExceptionDispatchInfo = Nothing
        Try
            ' The correlation id is what makes five services one story in a log
            ' aggregator. Carrying it forward is not optional.
            If placed.Total > AutomaticLimit Then
                Await _declinedEvents.SendAsync(
                    New PaymentDeclined With {
                        .OrderId = placed.OrderId, .Customer = placed.Customer,
                        .Reason = "over the automatic limit"},
                    Envelope.Of("PaymentDeclined").CorrelationId(message.Envelope.CorrelationId).Build())
                Interlocked.Increment(_declines)
            Else
                Await _capturedEvents.SendAsync(
                    New PaymentCaptured With {
                        .OrderId = placed.OrderId, .Customer = placed.Customer, .Sku = placed.Sku,
                        .Quantity = placed.Quantity, .Amount = placed.Total},
                    Envelope.Of("PaymentCaptured").CorrelationId(message.Envelope.CorrelationId).Build())
                Interlocked.Increment(_captures)
            End If
        Catch e As Exception
            failure = ExceptionDispatchInfo.Capture(e)
        End Try

        If failure IsNot Nothing Then
            ' The one place this app departs from the Java one on purpose. There
            ' the claim is kept when the publish fails, and the retry the failure
            ' asks for then finds its own claim, is refused as a duplicate and
            ' acknowledged: the order stops, with the customer neither charged nor
            ' told. Released, the retry can take it again.
            Await _charged.ReleaseAsync(placed.OrderId)
            failure.Throw()
        End If

        ' Confirmed only after the outcome is published. Confirming first would
        ' mean a crash in between leaves the order marked as charged with nothing
        ' downstream ever told -- an order that took the money and stopped.
        Await _charged.ConfirmAsync(placed.OrderId)
        Return Ack.Accept()
    End Function

    Public ReadOnly Property Captured As Integer
        Get
            Return Volatile.Read(_captures)
        End Get
    End Property

    Public ReadOnly Property Declined As Integer
        Get
            Return Volatile.Read(_declines)
        End Get
    End Property

    ''' <summary>How many redeliveries were recognised and refused. Worth graphing.</summary>
    Public ReadOnly Property DuplicatesRefused As Integer
        Get
            Return Volatile.Read(_duplicatesRefused)
        End Get
    End Property

    Public Function CloseAsync() As Task
        Return _mq.CloseAsync()
    End Function

End Class
