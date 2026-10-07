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

' A statement per account, built by reading the journal.
'
' This module makes one claim checkable: a projection is disposable. It stores
' nothing the log does not contain, it is built by reading from offset zero, and
' throwing it away costs nothing but the time to read the log again.
'
' It also proves the log is genuinely shared. The ledger reads the same stream
' from the same offset for its own purposes, and neither reader affects the other
' -- no competing consumption, no "who got the message".
'
' Adding a projection later is the point. A fraud model, a tax report, a
' daily-balance chart: each is a new reader from offset zero, added without
' touching the writer, with full history from the day it starts.

Imports System.Collections.Concurrent
Imports System.Collections.Generic
Imports System.Linq
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class StatementProjection
    Implements IDisposable

    Private ReadOnly _statements As New ConcurrentDictionary(Of String, List(Of EntryPosted))()
    Private _reader As IStreamConsumer

    Private Sub New()
    End Sub

    ''' <param name="mq">The connection.</param>
    ''' <param name="fromFirst">Whether to read all of history, or only what arrives from now.</param>
    Public Shared Async Function StartAsync(mq As AceMqConnection, fromFirst As Boolean) As Task(Of StatementProjection)
        Dim projection As New StatementProjection()
        Dim stream = mq.Stream(Of EntryPosted)(Ledger.Journal)
        projection._reader = Await If(fromFirst, stream.FromFirst(), stream.FromNext()).ConsumeAsync(
            Function(message)
                Dim entry = message.Payload
                Dim statement = projection._statements.GetOrAdd(entry.Account, Function(account) New List(Of EntryPosted)())
                SyncLock statement
                    statement.Add(entry)
                End SyncLock
                Return Task.CompletedTask
            End Function)
        Return projection
    End Function

    ''' <summary>The entries seen for an account, oldest first.</summary>
    Public Function StatementOf(account As String) As IReadOnlyList(Of EntryPosted)
        Dim statement As List(Of EntryPosted) = Nothing
        If Not _statements.TryGetValue(account, statement) Then Return Array.Empty(Of EntryPosted)()
        SyncLock statement
            Return statement.ToArray()
        End SyncLock
    End Function

    ''' <summary>The sum of the entries, which is what a balance is.</summary>
    Public Function BalanceOf(account As String) As Long
        Return StatementOf(account).Sum(Function(entry) entry.AmountMinor)
    End Function

    ''' <summary>Every account this projection has seen an entry for.</summary>
    Public ReadOnly Property Accounts As IReadOnlyList(Of String)
        Get
            Return _statements.Keys.ToArray()
        End Get
    End Property

    ''' <summary>Entries read.</summary>
    Public ReadOnly Property Entries As Integer
        Get
            Return Accounts.Sum(Function(account) StatementOf(account).Count)
        End Get
    End Property

    Public Sub Dispose() Implements IDisposable.Dispose
        _reader?.Dispose()
    End Sub

End Class
