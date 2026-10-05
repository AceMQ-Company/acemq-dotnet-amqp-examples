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

' Holds stock for orders that have been paid for.
'
' The service that talks to something unreliable. A warehouse system that times
' out is the ordinary case, not the exception, and two failures have to be told
' apart:
'
'   the warehouse did not answer        retry, it will probably work in a moment
'   three are left and they want ten    retrying changes nothing
'
' The first is a plain exception and goes up the retry ladder. The second is not
' a failure of this service at all: it is an answer, published as
' StockUnavailable so the customer is told at once rather than after four more
' attempts that could not have conjured stock.

Imports System.Collections.Concurrent
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class InventoryService

    Private ReadOnly _mq As AceMqConnection
    Private ReadOnly _reservedEvents As IPublisher(Of StockReserved)
    Private ReadOnly _unavailableEvents As IPublisher(Of StockUnavailable)
    Private ReadOnly _stock As New ConcurrentDictionary(Of String, Integer)()
    Private _reserved As Integer
    Private _rejected As Integer
    Private _retried As Integer
    Private _warehouseCalls As Integer

    ' How many warehouse calls fail before it starts working. Set by the run.
    Private _failuresToSimulate As Integer

    Private Sub New(mq As AceMqConnection)
        _mq = mq
        _reservedEvents = mq.Publisher(Of StockReserved)(Contract.Exchange, Contract.StockReservedKey)
        _unavailableEvents = mq.Publisher(Of StockUnavailable)(Contract.Exchange, Contract.StockUnavailableKey)
    End Sub

    Public Shared Async Function StartAsync(url As String) As Task(Of InventoryService)
        Dim mq = Await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-vbnet/inventory").Build())
        Await mq.ApplyAsync(Contract.Everything())

        Dim inventory As New InventoryService(mq)

        ' Four attempts, 200 ms doubling to at most five seconds -- the Java app's
        ' ladder. Java waits every rung inside the broker. This library waits a
        ' rung shorter than RetryPolicy.DefaultBrokerWaitThreshold (thirty
        ' seconds) on the consumer instead, and only the longer ones in the
        ' broker, so every rung here is a short wait in-process. Either way the
        ' attempt count travels on the message, which is what the handler reads.
        Dim options = ConsumerOptions.Prefetch(20) _
            .WithRetry(RetryPolicy.Exponential(4, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)))

        Await mq.ConsumeAsync(Of PaymentCaptured)(Contract.Inventory, options, AddressOf inventory.ReserveAsync)
        Return inventory
    End Function

    Public Function WithStock(sku As String, quantity As Integer) As InventoryService
        _stock(sku) = quantity
        Return Me
    End Function

    ''' <summary>Makes the next <paramref name="count"/> warehouse calls fail, the way a real one does.</summary>
    Public Function WithFlakyWarehouse(count As Integer) As InventoryService
        Volatile.Write(_failuresToSimulate, count)
        Return Me
    End Function

    Private Async Function ReserveAsync(message As IMessage(Of PaymentCaptured)) As Task(Of Ack)
        ' message.Attempt, not message.Envelope.Attempt: the count the consumer
        ' keeps is the one that moves. Counted here, from what actually arrived,
        ' rather than from what the library says it scheduled -- the .NET
        ' consumer has no Retried counter of the kind Java's has, and a retry that
        ' was scheduled and never came back is exactly what this would miss.
        If message.Attempt > 1 Then Interlocked.Increment(_retried)

        ' The transient failure. Nothing is wrong with the message, so it goes
        ' back on the ladder and arrives again shortly.
        If Interlocked.Increment(_warehouseCalls) <= Volatile.Read(_failuresToSimulate) Then
            Throw New InvalidOperationException("warehouse did not respond")
        End If

        Dim payment = message.Payload
        Dim available As Integer
        _stock.TryGetValue(payment.Sku, available)

        If available < payment.Quantity Then
            ' The permanent one. Retrying will not conjure stock.
            Await _unavailableEvents.SendAsync(
                New StockUnavailable With {
                    .OrderId = payment.OrderId, .Customer = payment.Customer, .Sku = payment.Sku,
                    .Reason = $"only {available} left"},
                Envelope.Of("StockUnavailable").CorrelationId(message.Envelope.CorrelationId).Build())
            Interlocked.Increment(_rejected)
            Return Ack.Accept()
        End If

        _stock.AddOrUpdate(payment.Sku, -payment.Quantity, Function(sku, held) held - payment.Quantity)
        Await _reservedEvents.SendAsync(
            New StockReserved With {
                .OrderId = payment.OrderId, .Customer = payment.Customer, .Sku = payment.Sku,
                .Quantity = payment.Quantity},
            Envelope.Of("StockReserved").CorrelationId(message.Envelope.CorrelationId).Build())
        Interlocked.Increment(_reserved)
        Return Ack.Accept()
    End Function

    Public ReadOnly Property Reserved As Integer
        Get
            Return Volatile.Read(_reserved)
        End Get
    End Property

    Public ReadOnly Property Rejected As Integer
        Get
            Return Volatile.Read(_rejected)
        End Get
    End Property

    ''' <summary>Deliveries that were a second or later attempt.</summary>
    Public ReadOnly Property Retried As Integer
        Get
            Return Volatile.Read(_retried)
        End Get
    End Property

    Public Function StockOf(sku As String) As Integer
        Dim quantity As Integer
        Return If(_stock.TryGetValue(sku, quantity), quantity, 0)
    End Function

    Public Function CloseAsync() As Task
        Return _mq.CloseAsync()
    End Function

End Class
