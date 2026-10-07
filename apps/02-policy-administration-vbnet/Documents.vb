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

' Documents: the claim-check pattern.
'
' A medical report scanned at 300 dpi is tens of megabytes. Putting it on a queue
' is possible and is a mistake -- it fills the broker's memory, it is copied to
' every bound queue, it makes a dead-letter queue impossible to inspect, and it
' turns a broker into a filesystem with worse tools. What travels instead is a
' claim check: the document goes to a store, and the message carries the key.
'
' The store is a dictionary because the example must run without infrastructure.
' A real one is S3, Azure Blob Storage or a filesystem, and only two method bodies
' change. The library has IClaimCheckStore and a ClaimCheckCodec that does this
' transparently for any payload over a threshold; this module does it by hand, as
' the Java one does, because its key says what it is -- the policy and the kind of
' document -- where the library's is opaque.
'
' Retention is the part people forget. The store and the queue have different
' lifetimes. A message replayed a month later carries a key, and if the store
' expired it the replay produces a message nobody can read -- worse than a lost
' message, because it looks like a message.

Imports System.Collections.Concurrent
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class DocumentModule

    Private ReadOnly _stored As IPublisher(Of DocumentStored)
    Private ReadOnly _store As New ConcurrentDictionary(Of String, Byte())()

    Public Sub New(mq As AceMqConnection)
        _stored = mq.Publisher(Of DocumentStored)(Policies.Exchange, Policies.DocumentStoredKey)
    End Sub

    ''' <summary>Stores a document and announces that it exists.</summary>
    ''' <returns>The key the event carries. The bytes do not go anywhere near the broker.</returns>
    Public Async Function StoreAsync(policyId As String, kind As String, content As Byte()) As Task(Of String)
        Dim key = $"doc/{policyId}/{kind}/{Guid.NewGuid().ToString("N").Substring(0, 8)}"
        _store(key) = content

        ' The event is a few hundred bytes whatever the document weighs.
        Await _stored.SendAsync(
            New DocumentStored With {
                .PolicyId = policyId, .DocumentKey = key, .Kind = kind, .Bytes = content.Length},
            Envelope.Of("DocumentStored").CorrelationId(policyId).Build())
        Return key
    End Function

    ''' <summary>Redeems a claim check: the document, when the store still has it.</summary>
    Public Function Fetch(key As String) As Byte()
        Dim content As Byte() = Nothing
        Return If(_store.TryGetValue(key, content), content, Nothing)
    End Function

    Public ReadOnly Property Held As Integer
        Get
            Return _store.Count
        End Get
    End Property

End Class
