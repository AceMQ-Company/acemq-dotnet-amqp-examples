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

' The only thing allowed to append to the journal.
'
' One writer, deliberately. A ledger's invariant -- every transfer produces two
' entries that sum to zero -- cannot be enforced by two processes appending
' independently, and a stream will happily accept an unbalanced pair from each.
' Making the writer singular is what makes the invariant checkable at all.
'
' Balances are not stored here. This module decides whether a transfer is allowed
' and appends the entries; its view of the balances is rebuilt from the journal
' every time it starts, for the one decision it has to make.

Imports System.Collections.Concurrent
Imports System.Diagnostics
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class LedgerModule

    ''' <summary>How long the journal keeps entries.</summary>
    ''' <remarks>
    ''' An hour, because this is an example. A real ledger keeps them as long as the
    ''' law says, which is years, and this is the setting people get wrong: if the
    ''' retention is shorter than "forever", the projection is the system of record
    ''' after all and nobody wrote that down.
    ''' </remarks>
    Private Shared ReadOnly Retention As TimeSpan = TimeSpan.FromHours(1)

    Private Const MaxBytes As Long = 50L * 1024 * 1024

    Private ReadOnly _journal As IPublisher(Of EntryPosted)
    Private ReadOnly _rejections As IPublisher(Of TransferRejected)
    Private ReadOnly _balances As Balances

    ' One writer is a claim about processes; this makes it true inside this one too.
    ' A transfer and an opening balance never interleave, so a decision is always
    ' made against every entry before it.
    Private ReadOnly _writing As New SemaphoreSlim(1, 1)

    Private _posted As Long
    Private _rejected As Long

    Private Sub New(mq As AceMqConnection, rebuilt As Balances)
        ' Published straight at the stream by name. A stream is addressed as a queue,
        ' so the default exchange and the stream's name is the whole of it.
        _journal = mq.Publisher(Of EntryPosted)("", Ledger.Journal)
        _rejections = mq.Publisher(Of TransferRejected)(Ledger.Exchange, Ledger.TransferRejectedKey)
        _balances = rebuilt
    End Sub

    Public Shared Async Function StartAsync(mq As AceMqConnection) As Task(Of LedgerModule)
        Await mq.DeclareStreamAsync(Ledger.Journal, Retention, MaxBytes)

        ' The writer's own view of the balances, rebuilt from the journal. Not a
        ' cache of somebody else's state: derived here, from the log.
        Dim started As New LedgerModule(mq, Await Balances.RebuiltFromAsync(mq))

        Await mq.ConsumeAsync(Of Transfer)(Ledger.Commands,
            Async Function(message)
                Await started.ApplyAsync(message.Payload)
                Return Ack.Accept()
            End Function)
        Return started
    End Function

    ' The release is not an Await, so it can live in the Finally.
    Private Async Function ApplyAsync(requested As Transfer) As Task
        Await _writing.WaitAsync()
        Try
            Dim available = _balances.Of(requested.From)
            If requested.AmountMinor <= 0 Then
                Await RejectAsync(requested, "a transfer must be for a positive amount")
                Return
            End If
            If available < requested.AmountMinor Then
                ' Refused, and the refusal is recorded. A ledger that silently drops
                ' what it will not do cannot explain itself later.
                Await RejectAsync(requested, $"insufficient funds: {requested.From} holds {available}")
                Return
            End If

            ' Fully qualified: VB.NET ignores case, so a local called envelope would
            ' otherwise be the Envelope on its own right-hand side.
            Dim envelope = AceMq.Amqp.Envelope.Of("EntryPosted").CorrelationId(requested.TransferId).Build()

            ' Two entries, one transfer, summing to zero. Appended one after the other
            ' by the only writer there is -- a real ledger appends them as one record
            ' so a crash between them is impossible, and that is the honest
            ' limitation of doing it this way.
            Await PostAsync(New EntryPosted With {
                .EntryId = EntryId(), .TransferId = requested.TransferId, .Account = requested.From,
                .AmountMinor = -requested.AmountMinor, .Description = requested.Description}, envelope)
            Await PostAsync(New EntryPosted With {
                .EntryId = EntryId(), .TransferId = requested.TransferId, .Account = requested.To,
                .AmountMinor = requested.AmountMinor, .Description = requested.Description}, envelope)
        Finally
            _writing.Release()
        End Try
    End Function

    ''' <summary>Opens an account with money in it, which every ledger needs a way to do.</summary>
    Public Async Function FundAsync(account As String, amountMinor As Long) As Task
        Await _writing.WaitAsync()
        Try
            Dim opening As New EntryPosted With {
                .EntryId = EntryId(), .TransferId = "OPENING-" & account, .Account = account,
                .AmountMinor = amountMinor, .Description = "opening balance"}
            Await PostAsync(opening, Envelope.Of("EntryPosted").CorrelationId(opening.TransferId).Build())
        Finally
            _writing.Release()
        End Try
    End Function

    ' Appended, then applied: once the stream has it, and not before. Applied here
    ' rather than read back off the stream -- see Balances for why both at once is
    ' the bug.
    Private Async Function PostAsync(entry As EntryPosted, envelope As Envelope) As Task
        Await _journal.SendAsync(entry, envelope)
        _balances.Apply(entry)
        Interlocked.Increment(_posted)
    End Function

    Private Async Function RejectAsync(requested As Transfer, reason As String) As Task
        Await _rejections.SendAsync(
            New TransferRejected With {
                .TransferId = requested.TransferId, .From = requested.From, .To = requested.To,
                .AmountMinor = requested.AmountMinor, .Reason = reason},
            Envelope.Of("TransferRejected").CorrelationId(requested.TransferId).Build())
        Interlocked.Increment(_rejected)
    End Function

    ''' <summary>This module's own view, derived from the log.</summary>
    Public Function BalanceOf(account As String) As Long
        Return _balances.Of(account)
    End Function

    Public ReadOnly Property Posted As Long
        Get
            Return Interlocked.Read(_posted)
        End Get
    End Property

    Public ReadOnly Property Rejected As Long
        Get
            Return Interlocked.Read(_rejected)
        End Get
    End Property

    Private Shared Function EntryId() As String
        Return "E-" & Guid.NewGuid().ToString()
    End Function

End Class

' Balances, computed by reading the journal from the beginning.
'
' This holds no state anybody wrote down. Delete it, restart the process, and it
' comes back identical, because it is a function of the log and nothing else.
' FromFirst() is the whole trick: a queue cannot do this -- reading it consumes it.
'
' Read to the end, then stop. The reader is closed once it has caught up, and the
' writer maintains the balances itself from then on. That is a correctness
' requirement, not an optimisation: the Java app's first version kept following
' the stream AND applied each entry as it was written, so every posting was
' counted twice. Keeping only the stream has the opposite problem -- a transfer
' decided against a balance that does not yet include the one before it.
'
' A rebuild is O(history). At a billion entries the answer is a snapshot -- "the
' balance at offset N, plus everything after N" -- deliberately not here.
Friend NotInheritable Class Balances

    ''' <summary>How long without an entry counts as caught up.</summary>
    ''' <remarks>
    ''' Crude, and honest about it. The precise way is to read the offset of the last
    ''' entry before starting and stop there.
    ''' </remarks>
    Private Shared ReadOnly QuietPeriod As TimeSpan = TimeSpan.FromMilliseconds(400)

    Private Shared ReadOnly RebuildLimit As TimeSpan = TimeSpan.FromSeconds(30)

    Private ReadOnly _accounts As New ConcurrentDictionary(Of String, Long)()

    Private Sub New()
    End Sub

    Public Shared Async Function RebuiltFromAsync(mq As AceMqConnection) As Task(Of Balances)
        Dim rebuilt As New Balances()
        Dim lastSeen = Stopwatch.GetTimestamp()
        Dim started = Stopwatch.StartNew()

        Dim reader = Await mq.Stream(Of EntryPosted)(Ledger.Journal).FromFirst().ConsumeAsync(
            Function(message)
                rebuilt.Apply(message.Payload)
                Interlocked.Exchange(lastSeen, Stopwatch.GetTimestamp())
                Return Task.CompletedTask
            End Function)
        Try
            While Stopwatch.GetElapsedTime(Interlocked.Read(lastSeen)) < QuietPeriod
                If started.Elapsed > RebuildLimit Then
                    Throw New InvalidOperationException(
                        $"the journal did not stop producing entries within {RebuildLimit.TotalSeconds:F0}s; " &
                        "a rebuild cannot finish while somebody is still writing")
                End If
                Await Task.Delay(20)
            End While

            ' A reader that failed on an entry stopped there, and balances built from
            ' part of the log are wrong in a way nothing downstream can see.
            If reader.Failed > 0 Then
                Throw New InvalidOperationException($"could not rebuild balances: {reader.Failed} entries failed")
            End If
        Finally
            reader.Dispose()
        End Try
        Return rebuilt
    End Function

    ''' <summary>Applied by the writer as it appends, which is safe because there is one writer.</summary>
    Public Sub Apply(entry As EntryPosted)
        _accounts.AddOrUpdate(entry.Account, entry.AmountMinor, Function(account, balance) balance + entry.AmountMinor)
    End Sub

    Public Function [Of](account As String) As Long
        Dim balance As Long
        Return If(_accounts.TryGetValue(account, balance), balance, 0L)
    End Function

End Class
