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

' Where transfers are asked for, and where refusals are noticed.
'
' Deliberately thin. Everything a ledger is careful about happens in the writer;
' this module makes the shape obvious -- a transfer is a command, sent to a queue,
' which may be refused, and a refusal is a normal outcome rather than an error.
'
' Events are named in the past tense and cannot be argued with; commands are
' requests and can be turned down. Systems that blur the two end up publishing
' TransferMade before knowing whether it was, and then need a second event to
' take it back.

Imports System.Collections.Generic
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class TransferGateway

    Private ReadOnly _requests As IPublisher(Of Transfer)
    Private ReadOnly _refused As New List(Of TransferRejected)()

    Private Sub New(mq As AceMqConnection)
        _requests = mq.Publisher(Of Transfer)(Ledger.Exchange, Ledger.TransferRequestedKey)
    End Sub

    Public Shared Async Function StartAsync(mq As AceMqConnection) As Task(Of TransferGateway)
        Dim started As New TransferGateway(mq)
        Await mq.ConsumeAsync(Of TransferRejected)(Ledger.Rejections,
            Function(message)
                SyncLock started._refused
                    started._refused.Add(message.Payload)
                End SyncLock
                Return Task.FromResult(Ack.Accept())
            End Function)
        Return started
    End Function

    ''' <summary>Asks for money to move.</summary>
    ''' <returns>The transfer id, which correlates the command with both entries and any refusal.</returns>
    Public Async Function RequestAsync(debited As String, credited As String, amountMinor As Long,
                                       description As String) As Task(Of String)
        Dim transferId = "T-" & Guid.NewGuid().ToString("N").Substring(0, 8)
        Await _requests.SendAsync(
            New Transfer With {
                .TransferId = transferId, .From = debited, .To = credited,
                .AmountMinor = amountMinor, .Description = description},
            Envelope.Of("Transfer").CorrelationId(transferId).Build())
        Return transferId
    End Function

    ''' <summary>The transfers the ledger refused, and why.</summary>
    Public ReadOnly Property Refused As IReadOnlyList(Of TransferRejected)
        Get
            SyncLock _refused
                Return _refused.ToArray()
            End SyncLock
        End Get
    End Property

End Class
