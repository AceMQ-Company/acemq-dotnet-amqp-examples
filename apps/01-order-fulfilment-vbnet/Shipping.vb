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

' Dispatches what has been paid for and reserved.
'
' The simplest service in the system, and it is worth noticing why: it reacts
' to one event, does one thing, and publishes one event. It knows nothing about
' payments, nothing about stock levels, and nothing about who else cares that an
' order shipped. Adding a service that also reacts to stock.reserved requires no
' change here at all.

Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class ShippingService

    Private ReadOnly _mq As AceMqConnection
    Private ReadOnly _shippedEvents As IPublisher(Of OrderShipped)
    Private _shipped As Integer

    Private Sub New(mq As AceMqConnection)
        _mq = mq
        _shippedEvents = mq.Publisher(Of OrderShipped)(Contract.Exchange, Contract.OrderShippedKey)
    End Sub

    Public Shared Async Function StartAsync(url As String) As Task(Of ShippingService)
        Dim mq = Await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-vbnet/shipping").Build())
        Await mq.ApplyAsync(Contract.Everything())

        Dim shipping As New ShippingService(mq)
        Await mq.ConsumeAsync(Of StockReserved)(
            Contract.Shipping, ConsumerOptions.Prefetch(10), AddressOf shipping.DispatchAsync)
        Return shipping
    End Function

    Private Async Function DispatchAsync(message As IMessage(Of StockReserved)) As Task(Of Ack)
        Dim reservation = message.Payload
        Dim tracking = "TRK-" & reservation.OrderId.Substring(4).ToUpperInvariant()
        Await _shippedEvents.SendAsync(
            New OrderShipped With {
                .OrderId = reservation.OrderId, .Customer = reservation.Customer, .Tracking = tracking},
            Envelope.Of("OrderShipped").CorrelationId(message.Envelope.CorrelationId).Build())
        Interlocked.Increment(_shipped)
        Return Ack.Accept()
    End Function

    Public ReadOnly Property Shipped As Integer
        Get
            Return Volatile.Read(_shipped)
        End Get
    End Property

    Public Function CloseAsync() As Task
        Return _mq.CloseAsync()
    End Function

End Class
