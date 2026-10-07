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

' The events a ledger is made of.
'
' In apps/01 and apps/02 the events describe what happened to the system of
' record. Here they ARE the system of record. There is no balances table that
' events update; a balance is what you get by adding up entries, and it can be
' deleted and recomputed without losing anything, because nothing was ever stored
' that the log does not contain.
'
' One consequence worth stating before the code: an entry is never changed and
' never deleted. Money moved wrongly is corrected by posting the opposite entry,
' exactly as a paper ledger does, and both entries stay.

Imports AceMq.Amqp

' ---- the log ------------------------------------------------------------------

''' <summary>One side of one movement of money.</summary>
''' <remarks>
''' Signed rather than a debit/credit flag: a sum over a column is then simply a
''' sum. Whole minor units -- pennies, cents -- in a Long, because a ledger in Double
''' disagrees with itself after enough additions.
''' </remarks>
Public Class EntryPosted
    ''' <summary>Unique, and the idempotency key.</summary>
    Public Property EntryId As String = ""
    ''' <summary>The movement this is one half of.</summary>
    Public Property TransferId As String = ""
    ''' <summary>Whose balance changes.</summary>
    Public Property Account As String = ""
    ''' <summary>Positive credits the account, negative debits it.</summary>
    Public Property AmountMinor As Long
    ''' <summary>What a statement will show.</summary>
    Public Property Description As String = ""
End Class

''' <summary>A transfer that was refused, with the reason kept beside the ones that were not.</summary>
''' <remarks>From and To are VB.NET keywords, hence the brackets; on the wire they are "from" and "to".</remarks>
Public Class TransferRejected
    Public Property TransferId As String = ""
    Public Property [From] As String = ""
    Public Property [To] As String = ""
    Public Property AmountMinor As Long
    Public Property Reason As String = ""
End Class

' ---- commands -----------------------------------------------------------------

''' <summary>Move money between two accounts. Not an event: a request, and it may be refused.</summary>
Public Class Transfer
    Public Property TransferId As String = ""
    Public Property [From] As String = ""
    Public Property [To] As String = ""
    Public Property AmountMinor As Long
    Public Property Description As String = ""
End Class

' A class of Shared members rather than a Module, so its names stay qualified:
' VB.NET ignores case, and a module's Journal would meet every journal in the
' project.
Public NotInheritable Class Ledger

    Private Sub New()
    End Sub

    ' Every broker object this app declares starts with this; routing keys and
    ' envelope types are the Java app's, character for character. The C# and
    ' VB.NET twins share a broker in CI, and two ledgers appending to one journal
    ' would each rebuild the other's balances.
    Private Const Prefix As String = "dotnet-vbnet."

    ''' <summary>The stream every entry is appended to.</summary>
    ''' <remarks>
    ''' A stream rather than a queue, and the difference is the point. A queue is
    ''' emptied by being read; a stream is not. Ten readers can each read all of
    ''' history at their own pace, and nothing anybody reads removes anything for
    ''' anybody else.
    ''' </remarks>
    Public Const Journal As String = Prefix & "ledger.journal"

    ''' <summary>Where transfer commands arrive. An ordinary queue: a command is handled once.</summary>
    Public Const Commands As String = Prefix & "ledger.commands"

    ''' <summary>Where refusals are announced, for whoever wants to be told rather than to read.</summary>
    Public Const Rejections As String = Prefix & "ledger.rejections"

    ''' <summary>Where the ledger announces what it decided, for anything that is not a projection.</summary>
    Public Const Exchange As String = Prefix & "ledger"

    Public Const EntryPostedKey As String = "ledger.entry.posted"
    Public Const TransferRequestedKey As String = "ledger.transfer.requested"
    Public Const TransferRejectedKey As String = "ledger.transfer.rejected"

    ''' <summary>The whole application's topology, apart from the journal.</summary>
    ''' <remarks>
    ''' Commands go to an ordinary queue and entries to a stream. Getting that
    ''' backwards is the most common mistake in event-sourced systems. The journal
    ''' is declared by its only writer, with its retention, in LedgerModule.
    ''' </remarks>
    Public Shared Function Everything() As Topology
        ' Commands: an ordinary queue, because a transfer must be applied once.
        ' Rejections are announced so somebody can act on them.
        Return Topology.Define() _
            .Exchange(Exchange, "topic") _
            .Queue(Commands, QueueType.Classic) _
            .Bind(Commands, Exchange, TransferRequestedKey) _
            .Queue(Rejections, QueueType.Classic) _
            .Bind(Rejections, Exchange, TransferRejectedKey) _
            .Build()
    End Function

End Class
