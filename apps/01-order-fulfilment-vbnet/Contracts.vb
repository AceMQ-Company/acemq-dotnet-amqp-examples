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

' What every service in this system agrees on, and nothing else.
'
' The events, the exchange, the queue each service reads, and the routing keys
' that connect them. In a larger estate this is what a schema registry holds.
'
' What is deliberately not here: any service's domain model, any database
' access, any shared "helper". A contracts file that grows those stops being a
' contract and becomes a shared library, which is how five services turn back
' into one deployable that happens to have five entry points.

Imports System.Collections.Generic

Imports AceMq.Amqp

' ---- events -------------------------------------------------------------------
'
' Classes with settable properties, which is what System.Text.Json reads into
' from VB.NET. The library's JSON codec camelCases on the way out, so OrderId is
' "orderId" on the wire -- the same field the Java app's records produce, and
' the same the C# twin's records produce. Each carries the order id, because
' that is the only identifier every service shares.

''' <summary>Someone placed an order. Published by the gateway, from its outbox.</summary>
Public Class OrderPlaced
    Public Property OrderId As String = ""
    Public Property Customer As String = ""
    Public Property Sku As String = ""
    Public Property Quantity As Integer
    Public Property Total As Double
End Class

''' <summary>The money is ours. Published by payments.</summary>
Public Class PaymentCaptured
    Public Property OrderId As String = ""
    Public Property Customer As String = ""
    Public Property Sku As String = ""
    Public Property Quantity As Integer
    Public Property Amount As Double
End Class

''' <summary>It is not, and will not be. Published by payments; nothing downstream proceeds.</summary>
Public Class PaymentDeclined
    Public Property OrderId As String = ""
    Public Property Customer As String = ""
    Public Property Reason As String = ""
End Class

''' <summary>Stock is held for this order. Published by inventory.</summary>
Public Class StockReserved
    Public Property OrderId As String = ""
    Public Property Customer As String = ""
    Public Property Sku As String = ""
    Public Property Quantity As Integer
End Class

''' <summary>There is not enough. Published by inventory; the money must be given back.</summary>
Public Class StockUnavailable
    Public Property OrderId As String = ""
    Public Property Customer As String = ""
    Public Property Sku As String = ""
    Public Property Reason As String = ""
End Class

''' <summary>On its way. Published by shipping.</summary>
Public Class OrderShipped
    Public Property OrderId As String = ""
    Public Property Customer As String = ""
    Public Property Tracking As String = ""
End Class

' A class of shared members rather than a Module. A Module's members are in
' scope everywhere in the project without qualification, so a constant called
' Payments would quietly meet every local, parameter and property called
' payments -- and in VB.NET, which ignores case, that is all of them.
Public NotInheritable Class Contract

    Private Sub New()
    End Sub

    ' Every broker object this app declares starts with this, and nothing else
    ' differs from the Java app. The C# and VB.NET twins run against the same
    ' broker in CI, and the same app in four other languages may be running
    ' against the same cluster in a fault drill; two of them sharing
    ' "fulfilment.payments" would each take half of the other's orders.
    '
    ' Routing keys and envelope types are NOT prefixed: they are the contract,
    ' and they are the Java app's, character for character.
    Private Const Prefix As String = "dotnet-vbnet."

    ''' <summary>One topic exchange. Every event in the system is published here.</summary>
    Public Const Exchange As String = Prefix & "fulfilment"

    ' ---- routing keys ---------------------------------------------------------
    '
    ' "fulfilment.<aggregate>.<past-tense-verb>". The aggregate in the middle is
    ' what lets notifications subscribe to everything at all.

    Public Const OrderPlacedKey As String = "fulfilment.order.placed"
    Public Const PaymentCapturedKey As String = "fulfilment.payment.captured"
    Public Const PaymentDeclinedKey As String = "fulfilment.payment.declined"
    Public Const StockReservedKey As String = "fulfilment.stock.reserved"
    Public Const StockUnavailableKey As String = "fulfilment.stock.unavailable"
    Public Const OrderShippedKey As String = "fulfilment.order.shipped"

    ' ---- queues ---------------------------------------------------------------
    '
    ' A queue per service, named after the service rather than after the event.
    ' Two services wanting the same event each get their own copy, and neither
    ' can starve the other.

    Public Const Payments As String = Prefix & "fulfilment.payments"
    Public Const Inventory As String = Prefix & "fulfilment.inventory"
    Public Const Shipping As String = Prefix & "fulfilment.shipping"
    Public Const Notifications As String = Prefix & "fulfilment.notifications"

    Public Shared ReadOnly Queues As IReadOnlyList(Of String) = {Payments, Inventory, Shipping, Notifications}

    ''' <summary>What must exist for this system to work: the whole topology, as one value.</summary>
    ''' <remarks>
    ''' Every service applies this on start-up. Applying it five times is safe and
    ''' is the point: no service depends on another having started first, so there
    ''' is no deployment order to get wrong.
    '''
    ''' Classic queues, as in the Java app. The builder's <c>Queue(name)</c> alone
    ''' would declare quorum queues, which is the right default for a real
    ''' deployment and a difference from the app being ported.
    ''' </remarks>
    Public Shared Function Everything() As Topology
        ' Payments acts on new orders. Inventory acts once the money is taken, not
        ' before: reserving stock for an order that cannot be paid for is how a
        ' warehouse fills with holds nobody releases. Shipping needs stock held.
        ' Notifications wants everything, which is what a wildcard is for.
        Return Topology.Define() _
            .Exchange(Exchange, "topic") _
            .Queue(Payments, QueueType.Classic) _
            .Bind(Payments, Exchange, OrderPlacedKey) _
            .Queue(Inventory, QueueType.Classic) _
            .Bind(Inventory, Exchange, PaymentCapturedKey) _
            .Queue(Shipping, QueueType.Classic) _
            .Bind(Shipping, Exchange, StockReservedKey) _
            .Queue(Notifications, QueueType.Classic) _
            .Bind(Notifications, Exchange, "fulfilment.#") _
            .Build()
    End Function

End Class
