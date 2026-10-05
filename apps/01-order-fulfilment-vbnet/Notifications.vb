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

' Tells the customer what happened.
'
' Bound to fulfilment.# -- everything. This is the service that shows why a
' topic exchange is worth more than a queue per pair of services: it was added
' without a single change to any publisher, and the next one will be too.
'
' It cannot ask for a typed payload, because it subscribes to six event types on
' one queue and their shapes differ. So it takes the body as text and reads the
' envelope, which carries the type and the correlation id -- everything this
' service actually needs.

Imports System.Collections.Concurrent
Imports System.Collections.Generic
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class NotificationsService

    Private ReadOnly _mq As AceMqConnection
    Private ReadOnly _timeline As New ConcurrentDictionary(Of String, List(Of String))()

    Private Sub New(mq As AceMqConnection)
        _mq = mq
    End Sub

    Public Shared Async Function StartAsync(url As String) As Task(Of NotificationsService)
        Dim mq = Await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-vbnet/notifications").Build())
        Await mq.ApplyAsync(Contract.Everything())

        Dim notifications As New NotificationsService(mq)

        ' The text codec is the part worth copying. Asking for String with the
        ' JSON codec hands it an object and tells it to produce a string, which
        ' fails on every message -- a fan-in consumer's most common mistake.
        '
        ' Registered as "string" here and in Ruby; Java and Python call the same
        ' codec "text". A name taken from one service's configuration is refused
        ' by the other two libraries, so this one is spelled out.
        Dim options = ConsumerOptions.Prefetch(50).As(CodecRegistry.ByName("string"))

        Await mq.ConsumeAsync(Of String)(Contract.Notifications, options, AddressOf notifications.Record)
        Return notifications
    End Function

    Private Function Record(message As IMessage(Of String)) As Task(Of Ack)
        ' The correlation id is the order it belongs to, set by whichever service
        ' published it and carried forward by all of them.
        Dim seen = _timeline.GetOrAdd(message.Envelope.CorrelationId, Function(id) New List(Of String)())
        SyncLock seen
            seen.Add(message.Envelope.Type)
        End SyncLock
        Return Task.FromResult(Ack.Accept())
    End Function

    ''' <summary>What a customer looking at "where is my order" would be shown.</summary>
    Public Function TimelineOf(orderId As String) As IReadOnlyList(Of String)
        Dim seen As List(Of String) = Nothing
        If Not _timeline.TryGetValue(orderId, seen) Then Return Array.Empty(Of String)()
        SyncLock seen
            Return seen.ToArray()
        End SyncLock
    End Function

    Public Function CloseAsync() As Task
        Return _mq.CloseAsync()
    End Function

End Class
