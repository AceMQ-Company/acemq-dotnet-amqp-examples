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

' An event-sourced ledger on a RabbitMQ stream, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project apps/03-ledger-vbnet
'
' The log is the system of record. Balances are not stored anywhere: the writer
' rebuilds them from the journal every time it starts, and a projection rebuilds
' them again from offset zero and must agree.
'
' The Java test's five scenarios, each with a freshly started ledger -- and, as
' there, one journal shared by all of them, so every start after the first
' rebuilds balances from entries an earlier ledger wrote. Then one check of the
' whole journal the Java test does not make. The process exits non-zero if
' anything did not hold.

Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Module Program

    Private ReadOnly Patience As TimeSpan = TimeSpan.FromSeconds(60)

    ' Every entry any ledger in this run appended, so the last check knows how many
    ' the journal must hold.
    Private _posted As Long

    Function Main() As Integer
        Return RunAllAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAllAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())
        Dim url = BrokerUrl()

        ' A journal left by an earlier run would be rebuilt into this one's
        ' balances: alice would start with last time's money.
        Await ForgetEarlierRunsAsync(url)

        ' No Finally around the scenarios: each one catches its own failure, and
        ' VB.NET could not Await the clean-up in a Finally anyway.
        Dim notHeld = 0
        notHeld += Await RunAsync(url, "a transfer posts two entries that sum to zero", AddressOf DoubleEntry)
        notHeld += Await RunAsync(url, "a transfer that would overdraw is refused, and the refusal is recorded",
                                  AddressOf InsufficientFunds)
        notHeld += Await RunAsync(url, "a projection built from offset zero agrees with the writer",
                                  AddressOf ProjectionAgreesWithTheWriter)
        notHeld += Await RunAsync(url, "a projection added later still gets all of history",
                                  AddressOf ALaterProjectionSeesEverything)
        notHeld += Await RunAsync(url, "two readers of the same stream do not compete for entries",
                                  AddressOf ReadersDoNotCompete)
        notHeld += Await RunAsync(url, "the journal holds every entry once, and a restarted writer agrees with it",
                                  AddressOf TheJournalIsTheRecord)

        Await ForgetEarlierRunsAsync(url)

        Console.WriteLine(If(notHeld = 0, "all six held", $"{notHeld} of six did not hold"))
        Return If(notHeld = 0, 0, 1)
    End Function

    ' ---- the Java test's five -----------------------------------------------------

    Private Async Function DoubleEntry(running As TheLedger) As Task
        Await running.Ledger.FundAsync("alice", 10000)
        Await WaitFor(Function() running.Ledger.BalanceOf("alice") = 10000, "alice to be funded")

        Await running.Transfers.RequestAsync("alice", "bob", 2500, "rent")

        Await WaitFor(Function() running.Ledger.BalanceOf("bob") = 2500, "bob to be paid")
        Check(running.Ledger.BalanceOf("alice") = 7500, $"alice holds {running.Ledger.BalanceOf("alice")}, not 7500")

        ' The invariant: money is neither created nor destroyed by a transfer.
        Dim total = running.Ledger.BalanceOf("alice") + running.Ledger.BalanceOf("bob")
        Check(total = 10000, $"alice and bob hold {total} between them, not 10000")
    End Function

    Private Async Function InsufficientFunds(running As TheLedger) As Task
        Await running.Ledger.FundAsync("carol", 1000)
        Await WaitFor(Function() running.Ledger.BalanceOf("carol") = 1000, "carol to be funded")

        Await running.Transfers.RequestAsync("carol", "dave", 5000, "optimistic")

        Await WaitFor(Function() running.Transfers.Refused.Count > 0, "the transfer to be refused")
        Dim reason = running.Transfers.Refused(0).Reason
        Check(reason.Contains("insufficient funds"), $"the refusal says ""{reason}"", not insufficient funds")

        ' Nothing was posted. A ledger that half-applies a refused transfer is worse
        ' than one that refuses loudly.
        Check(running.Ledger.BalanceOf("carol") = 1000, $"carol holds {running.Ledger.BalanceOf("carol")}, not 1000")
        Check(running.Ledger.BalanceOf("dave") = 0, $"dave holds {running.Ledger.BalanceOf("dave")}, not nothing")
    End Function

    Private Async Function ProjectionAgreesWithTheWriter(running As TheLedger) As Task
        Await running.Ledger.FundAsync("erin", 20000)
        Await WaitFor(Function() running.Ledger.BalanceOf("erin") = 20000, "erin to be funded")
        Await running.Transfers.RequestAsync("erin", "frank", 3000, "invoice 1")
        Await running.Transfers.RequestAsync("erin", "frank", 4000, "invoice 2")
        Await WaitFor(Function() running.Ledger.BalanceOf("frank") = 7000, "frank to be paid twice")

        ' A reader that has never seen a message, starting from the beginning of time.
        ' It stores nothing the log does not contain, and it must reach the same
        ' answer.
        Using statements = Await StatementProjection.StartAsync(running.Mq, fromFirst:=True)
            Await WaitFor(Function() statements.BalanceOf("frank") = 7000, "the projection to reach frank's balance")

            Check(statements.BalanceOf("erin") = running.Ledger.BalanceOf("erin"),
                  $"the projection says erin holds {statements.BalanceOf("erin")}, the writer {running.Ledger.BalanceOf("erin")}")
            Check(statements.BalanceOf("frank") = running.Ledger.BalanceOf("frank"),
                  $"the projection says frank holds {statements.BalanceOf("frank")}, the writer {running.Ledger.BalanceOf("frank")}")

            ' And it has the detail the balance does not: three entries against erin
            ' -- the opening balance and two debits.
            Dim erin = statements.StatementOf("erin")
            Check(erin.Count = 3, $"erin's statement has {erin.Count} entries, not 3")
            Dim frank = statements.StatementOf("frank").Select(Function(entry) entry.Description).ToArray()
            Check(frank.SequenceEqual({"invoice 1", "invoice 2"}),
                  $"frank's statement reads [{String.Join(", ", frank)}], not [invoice 1, invoice 2]")
        End Using
    End Function

    Private Async Function ALaterProjectionSeesEverything(running As TheLedger) As Task
        Await running.Ledger.FundAsync("grace", 5000)
        Await running.Transfers.RequestAsync("grace", "heidi", 1000, "before the projection existed")
        Await WaitFor(Function() running.Ledger.BalanceOf("heidi") = 1000, "heidi to be paid")

        ' Started now, after the entries were written. On a queue there would be
        ' nothing left to read. A stream is not emptied by reading, so this gets
        ' everything, and so would one written next year.
        Using late = Await StatementProjection.StartAsync(running.Mq, fromFirst:=True)
            Await WaitFor(Function() late.BalanceOf("heidi") = 1000, "the late projection to reach heidi's balance")

            Dim heidi = late.StatementOf("heidi")
            Check(heidi.Count = 1, $"heidi's statement has {heidi.Count} entries, not 1")
            Check(heidi(0).Description = "before the projection existed",
                  $"heidi's entry reads ""{heidi(0).Description}""")
        End Using
    End Function

    Private Async Function ReadersDoNotCompete(running As TheLedger) As Task
        Await running.Ledger.FundAsync("ivan", 8000)
        Await running.Transfers.RequestAsync("ivan", "judy", 2000, "shared")
        Await WaitFor(Function() running.Ledger.BalanceOf("judy") = 2000, "judy to be paid")

        Using first = Await StatementProjection.StartAsync(running.Mq, fromFirst:=True)
            Using second = Await StatementProjection.StartAsync(running.Mq, fromFirst:=True)
                Await WaitFor(Function() first.BalanceOf("judy") = 2000 AndAlso second.BalanceOf("judy") = 2000,
                              "both projections to reach judy's balance")

                ' Both saw the same entry. On a queue exactly one of them would have,
                ' which is what makes a queue wrong for a ledger and right for a
                ' command.
                Check(first.StatementOf("judy").Count = 1,
                      $"the first projection has {first.StatementOf("judy").Count} entries for judy")
                Check(second.StatementOf("judy").Count = 1,
                      $"the second projection has {second.StatementOf("judy").Count} entries for judy")
            End Using
        End Using
    End Function

    ' ---- and one the Java test does not make --------------------------------------

    ' Every scenario above checks the accounts it touched. This checks the log
    ' itself, after five ledgers have each rebuilt from it and appended to it: it
    ' holds exactly the entries those ledgers posted, no entry twice, every
    ' transfer's entries summing to zero -- and this run's freshly started writer,
    ' which rebuilt every balance from that log, agrees with a projection of it
    ' account by account. A library that dropped or duplicated an entry on the way
    ' in or out of the stream fails here even if every scenario's own accounts
    ' happened to come out right.
    Private Async Function TheJournalIsTheRecord(running As TheLedger) As Task
        Using everything = Await StatementProjection.StartAsync(running.Mq, fromFirst:=True)
            Dim expected = Interlocked.Read(_posted)
            Await WaitFor(Function() everything.Entries >= expected, $"{expected} entries in the journal")
            Await Task.Delay(TimeSpan.FromSeconds(1))
            Check(everything.Entries = expected, $"the journal holds {everything.Entries} entries; {expected} were posted")

            Dim entries = everything.Accounts.SelectMany(Function(account) everything.StatementOf(account)).ToArray()
            Dim distinct = entries.Select(Function(entry) entry.EntryId).Distinct().Count()
            Check(distinct = entries.Length, $"{entries.Length - distinct} entries appear more than once")

            For Each movement In entries.Where(Function(entry) Not entry.TransferId.StartsWith("OPENING-")) _
                                        .GroupBy(Function(entry) entry.TransferId)
                Dim net = movement.Sum(Function(entry) entry.AmountMinor)
                Check(movement.Count() = 2 AndAlso net = 0,
                      $"transfer {movement.Key} has {movement.Count()} entries summing to {net}")
            Next

            For Each account In everything.Accounts
                Check(running.Ledger.BalanceOf(account) = everything.BalanceOf(account),
                      $"the restarted writer says {account} holds {running.Ledger.BalanceOf(account)}, " &
                      $"the journal {everything.BalanceOf(account)}")
            Next
        End Using
    End Function

    ' ---- running one --------------------------------------------------------------

    ' Starts a ledger, runs one scenario, and checks what none may leave behind:
    ' nothing in the .dlq or .parked queue beside the commands, the rejections or
    ' the journal itself.
    Private Async Function RunAsync(url As String, name As String,
                                    scenario As Func(Of TheLedger, Task)) As Task(Of Integer)
        Dim running = Await TheLedger.StartAsync(url)

        ' Caught and kept, then reported after the ledger is stopped: stopping is an
        ' Await, and VB.NET cannot Await in a Catch or a Finally.
        Dim failure As Exception = Nothing
        Try
            Await scenario(running)
            For Each queueName In {Ledger.Commands, Ledger.Rejections, Ledger.Journal}
                For Each setAside In {queueName & ".dlq", queueName & ".parked"}
                    Dim count = Await running.Mq.MessageCountAsync(setAside)
                    Check(count = 0, $"{count} message(s) in {setAside}")
                Next
            Next
        Catch e As Exception
            failure = e
        End Try

        Interlocked.Add(_posted, running.Ledger.Posted)
        Await running.Mq.CloseAsync()

        If failure IsNot Nothing Then
            Console.WriteLine($"DID NOT   {name}: {failure.Message}")
            Return 1
        End If
        Console.WriteLine($"held      {name}")
        Return 0
    End Function

    Friend NotInheritable Class TheLedger
        Public Property Mq As AceMqConnection
        Public Property Ledger As LedgerModule
        Public Property Transfers As TransferGateway

        Public Shared Async Function StartAsync(url As String) As Task(Of TheLedger)
            Dim connection = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url).ClientName("examples/apps/03-ledger-vbnet").Build())

            ' Spelled out in full because, inside this class, Ledger is the property
            ' below rather than the contract.
            Await connection.ApplyAsync(Global.AceMq.Examples.Ledger.Everything())
            Return New TheLedger With {
                .Mq = connection,
                .Ledger = Await LedgerModule.StartAsync(connection),
                .Transfers = Await TransferGateway.StartAsync(connection)
            }
        End Function
    End Class

    Private Async Function ForgetEarlierRunsAsync(url As String) As Task
        Dim mq = Await AceMqConnection.ConnectAsync(url)
        Try
            For Each queueName In {Ledger.Commands, Ledger.Rejections, Ledger.Journal}
                Await mq.DeleteQueueAsync(queueName)
                Await mq.DeleteQueueAsync(queueName & ".dlq")
                Await mq.DeleteQueueAsync(queueName & ".parked")
            Next
            Await mq.DeleteExchangeAsync(Ledger.Exchange)
        Finally
            mq.Dispose()
        End Try
    End Function

    Private Async Function WaitFor(done As Func(Of Boolean), what As String) As Task
        Dim deadline = DateTime.UtcNow + Patience
        While Not done()
            If DateTime.UtcNow > deadline Then
                Throw New TimeoutException($"waited {Patience.TotalSeconds:F0}s for {what}")
            End If
            Await Task.Delay(50)
        End While
    End Function

    Private Sub Check(held As Boolean, wrong As String)
        If Not held Then Throw New InvalidOperationException(wrong)
    End Sub

    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
