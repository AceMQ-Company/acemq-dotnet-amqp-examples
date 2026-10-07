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

' Policy administration: a modular monolith, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project apps/02-policy-administration-vbnet
'
' One deployable, six modules, one database and one connection -- and still no
' module that calls another. This file is the only one that names more than one
' module, which is the same role the Java app's system test plays.
'
' Five scenarios, each with a freshly started application, the way the Java test
' runs them. Each checks its own claims, and the process exits non-zero if any
' did not hold.

Imports System.Collections.Generic
Imports System.IO
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq
Imports Microsoft.Data.Sqlite

Module Program

    ' How long the application gets to reach the state a scenario waits for. The
    ' Java test allows ninety seconds; every scenario gets there in well under one.
    Private ReadOnly Patience As TimeSpan = TimeSpan.FromSeconds(60)

    Private ReadOnly DatabaseFiles As New List(Of String)()

    Function Main() As Integer
        Return RunAllAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAllAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())
        Dim url = BrokerUrl()

        Dim notHeld = 0
        Try
            notHeld += Await RunAsync(url, "an ordinary application becomes a policy, and the premium is taken once",
                                      AddressOf TheHappyPath)
            notHeld += Await RunAsync(url, "an application above the automatic limit is referred, and never becomes a policy",
                                      AddressOf ReferredAboveTheLimit)
            notHeld += Await RunAsync(url, "a claim is assessed against an answer from policies, not against a local copy",
                                      AddressOf ClaimsAskRatherThanRead)
            notHeld += Await RunAsync(url, "a large document travels as a claim check, not as a message",
                                      AddressOf DocumentsTravelByReference)
            notHeld += Await RunAsync(url, "three copies of one event charge once; a genuinely different event still charges",
                                      AddressOf BillingIsIdempotent)

            ' An example that leaves its queues behind changes the next run's
            ' numbers. The last thing the Try does rather than a Finally, because
            ' VB.NET cannot Await in one.
            Await ForgetEarlierRunsAsync(url)
        Finally
            SqliteConnection.ClearAllPools()
            For Each leftover In DatabaseFiles
                For Each suffix In {"", "-wal", "-shm"}
                    File.Delete(leftover & suffix)
                Next
            Next
        End Try

        Console.WriteLine(If(notHeld = 0, "all five held", $"{notHeld} of five did not hold"))
        Return If(notHeld = 0, 0, 1)
    End Function

    ' ---- the five scenarios -------------------------------------------------------
    '
    ' Each returns how many events it published, which the audit trail must hold.

    Private Async Function TheHappyPath(running As TheApplication) As Task(Of Integer)
        Await running.Policies.SubmitAsync("A. Applicant", "TERM-LIFE", 100000, 40)

        ' Submitted -> underwritten -> issued -> charged, with no module calling another.
        Await WaitFor(Function() running.Billing.Charges.Count = 1, "billing to charge")

        Check(running.Underwriting.Accepted = 1, $"underwriting accepted {running.Underwriting.Accepted}, not 1")
        Check(running.Policies.Issued = 1, $"policies issued {running.Policies.Issued}, not 1")
        Check(running.Billing.Charges.Count = 1, $"billing charged {running.Billing.Charges.Count} times, not once")

        ' 100 base + 20 age loading, from the rating table in the pricing stage.
        Dim policyId = running.Billing.Charges(0)
        Dim premium = Await running.Policies.PremiumOfAsync(policyId)
        Check(premium.HasValue AndAlso premium.Value = 120, $"the premium of {policyId} is {premium}, not 120")

        ' Submitted, accepted, issued, charged.
        Return 4
    End Function

    Private Async Function ReferredAboveTheLimit(running As TheApplication) As Task(Of Integer)
        Await running.Policies.SubmitAsync("B. Applicant", "TERM-LIFE", 750000, 35)

        Await WaitFor(Function() running.Underwriting.Declined = 1, "underwriting to decline")

        ' Nothing downstream ran. Billing charging for a referred application would
        ' be the expensive version of this bug, which is why it is bound to
        ' PolicyIssued and not to the application.
        Check(running.Policies.Issued = 0, $"policies issued {running.Policies.Issued}, not none")
        Check(running.Billing.Charges.Count = 0, $"billing charged {running.Billing.Charges.Count} times, not never")

        ' Submitted, declined.
        Return 2
    End Function

    Private Async Function ClaimsAskRatherThanRead(running As TheApplication) As Task(Of Integer)
        Await running.Policies.SubmitAsync("C. Applicant", "TERM-LIFE", 50000, 30)
        Await WaitFor(Function() running.Billing.Charges.Count = 1, "billing to charge")
        Dim policyId = running.Billing.Charges(0)

        Await running.Claims.SubmitAsync(policyId, 5000, "windscreen")
        ' A policy nobody issued. The lookup is what makes this answerable at all.
        Await running.Claims.SubmitAsync("POL-does-not-exist", 5000, "windscreen")

        Await WaitFor(Function() running.Claims.Settled = 1 AndAlso running.Claims.Rejected = 1,
                      "one claim settled and one rejected")

        ' The four of the happy path, then settled and rejected.
        Return 6
    End Function

    Private Async Function DocumentsTravelByReference(running As TheApplication) As Task(Of Integer)
        Await running.Policies.SubmitAsync("D. Applicant", "TERM-LIFE", 60000, 45)
        Await WaitFor(Function() running.Billing.Charges.Count = 1, "billing to charge")
        Dim policyId = running.Billing.Charges(0)

        ' Four megabytes, which is a small scan and a large message.
        Dim scan = New Byte(4 * 1024 * 1024 - 1) {}
        Dim key = Await running.Documents.StoreAsync(policyId, "medical-report", scan)

        Dim fetched = running.Documents.Fetch(key)
        Check(fetched IsNot Nothing, $"the store has nothing under {key}")
        Check(fetched.Length = scan.Length, $"the store gave back {fetched.Length} bytes, not {scan.Length}")

        ' What crossed the broker is the key and the size. The whole event is a few
        ' hundred bytes; the four megabytes never went near a queue.
        Check(key.Contains(policyId) AndAlso key.Contains("medical-report"),
              $"the key {key} does not say which policy and document it is")

        ' The four of the happy path, then the document.
        Return 5
    End Function

    Private Async Function BillingIsIdempotent(running As TheApplication) As Task(Of Integer)
        Await running.Policies.SubmitAsync("E. Applicant", "TERM-LIFE", 80000, 50)
        Await WaitFor(Function() running.Billing.Charges.Count = 1, "billing to charge")
        Dim policyId = running.Billing.Charges(0)
        Dim premium = Await running.Policies.PremiumOfAsync(policyId)
        If Not premium.HasValue Then Throw New InvalidOperationException($"{policyId} is not in force")

        ' Three copies carrying one message id: what a redelivery looks like from
        ' the consumer's side, and what the idempotency store exists to absorb.
        Dim messageId = "redelivery-" & Guid.NewGuid().ToString()
        Dim issued = running.Mq.Publisher(Of PolicyIssued)(Policies.Exchange, Policies.PolicyIssuedKey)
        For copy = 1 To 3
            Await issued.SendAsync(
                New PolicyIssued With {
                    .PolicyId = policyId, .ApplicationId = "APP-x", .Applicant = "E. Applicant",
                    .Product = "TERM-LIFE", .AnnualPremium = premium.Value},
                Envelope.Of("PolicyIssued").Id(messageId).Build())
        Next

        ' Two, not one -- and the difference is the whole point. The store
        ' deduplicates by message id, so the three copies are one charge. They are
        ' not the same message as the original issue, which had an id of its own, so
        ' suppressing that too would mean the store had stopped distinguishing "sent
        ' twice" from "happened twice".
        ' At least two rather than exactly two, so a third charge is reported as one
        ' rather than as a wait that never ended.
        Await WaitFor(Function() running.Billing.Charges.Count >= 2, "billing to charge a second time")
        Await Task.Delay(TimeSpan.FromSeconds(2))
        Check(running.Billing.Charges.Count = 2, $"billing charged {running.Billing.Charges.Count} times, not twice")

        ' The four of the happy path, the three copies, and the one charge they made.
        Return 8
    End Function

    ' ---- running one --------------------------------------------------------------

    ' Starts the application, runs one scenario, and checks what every scenario must
    ' leave behind whatever else it asserts -- none of which the Java test checks.
    ' The audit queue, bound to policy.#, holds exactly the events the scenario
    ' published: one fewer is a lost message, one more is a duplicate. Nothing is in
    ' any module's .dlq or .parked queue, the outbox is empty, and no lookup timed
    ' out.
    '
    ' The application is called running rather than app or application, which are
    ' fine, and rather than system, which is not: VB.NET ignores case, so a local
    ' called system hides the System namespace for the whole function.
    Private Async Function RunAsync(url As String, name As String,
                                    scenario As Func(Of TheApplication, Task(Of Integer))) As Task(Of Integer)
        ' Each scenario starts from an empty broker, so one that left something in a
        ' dead-letter queue is not blamed on the next.
        Await ForgetEarlierRunsAsync(url)

        Dim running = Await TheApplication.StartAsync(url, OneDatabase())

        ' Caught and kept, then reported after the application is stopped: stopping
        ' is an Await, and VB.NET cannot Await in a Catch or a Finally.
        Dim failure As Exception = Nothing
        Try
            Dim events = Await scenario(running)

            Await WaitFor(Async Function() Await running.Mq.MessageCountAsync(Policies.Audit) >= events,
                          $"{events} events in the audit trail")
            Await Task.Delay(TimeSpan.FromMilliseconds(500))
            Dim audited = Await running.Mq.MessageCountAsync(Policies.Audit)
            Check(audited = events, $"the audit trail holds {audited} events, not {events}")

            Dim pending = Await running.Policies.PendingInOutboxAsync()
            Check(pending = 0, $"{pending} record(s) still in the outbox")
            Check(running.Claims.LookupsTimedOut = 0, $"{running.Claims.LookupsTimedOut} lookup(s) timed out")
            For Each queueName In Policies.ConsumedQueues
                For Each setAside In {queueName & ".dlq", queueName & ".parked"}
                    Dim count = Await running.Mq.MessageCountAsync(setAside)
                    Check(count = 0, $"{count} message(s) in {setAside}")
                Next
            Next
        Catch e As Exception
            failure = e
        End Try

        Await running.StopAsync()

        If failure IsNot Nothing Then
            Console.WriteLine($"DID NOT   {name}: {failure.Message}")
            Return 1
        End If
        Console.WriteLine($"held      {name}")
        Return 0
    End Function

    Friend NotInheritable Class TheApplication
        Public Property Mq As AceMqConnection
        Public Property Policies As PolicyModule
        Public Property Underwriting As UnderwritingModule
        Public Property Documents As DocumentModule
        Public Property Billing As BillingModule
        Public Property Claims As ClaimsModule

        Public Shared Async Function StartAsync(url As String, database As ConnectionSupplier) As Task(Of TheApplication)
            ' One connection for the whole application, because it is one
            ' application. In apps/01 each service had its own; here sharing one is
            ' correct, and is the only thing that differs at the transport level.
            Dim connection = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url).ClientName("examples/apps/02-policy-administration-vbnet").Build())

            ' Spelled out in full because, inside this class, Policies is the
            ' property below rather than the contract.
            Await connection.ApplyAsync(Global.AceMq.Examples.Policies.Everything())

            ' One database, several modules -- the monolith's actual advantage. The
            ' outbox still has to exist, because the broker is not in this
            ' database's transaction.
            Return New TheApplication With {
                .Mq = connection,
                .Policies = Await PolicyModule.StartAsync(connection, database),
                .Underwriting = Await UnderwritingModule.StartAsync(connection),
                .Documents = New DocumentModule(connection),
                .Billing = Await BillingModule.StartAsync(connection, database),
                .Claims = Await ClaimsModule.StartAsync(connection)
            }
        End Function

        ' The relay first, so nothing is published into a connection that is going.
        ' Closing the connection drains every handler before it closes.
        Public Function StopAsync() As Task
            Policies.Dispose()
            Return Mq.CloseAsync()
        End Function
    End Class

    ''' <summary>One database for the whole application: a SQLite file nothing else opens.</summary>
    ''' <remarks>
    ''' Write-ahead logging, because three modules write to it at once -- the relay
    ''' marking records published, policies issuing, billing claiming -- and in its
    ''' default mode SQLite makes a reader wait for every writer.
    ''' </remarks>
    Private Function OneDatabase() As ConnectionSupplier
        Dim file = Path.Combine(Path.GetTempPath(), $"policy-vbnet-{Guid.NewGuid():N}.db")
        DatabaseFiles.Add(file)
        Using connection As New SqliteConnection($"Data Source={file}")
            connection.Open()
            Using command = connection.CreateCommand()
                command.CommandText = "PRAGMA journal_mode=WAL;"
                command.ExecuteNonQuery()
            End Using
        End Using
        Return Function() New SqliteConnection($"Data Source={file}")
    End Function

    Private Async Function ForgetEarlierRunsAsync(url As String) As Task
        Dim mq = Await AceMqConnection.ConnectAsync(url)
        Try
            For Each queueName In Policies.ConsumedQueues
                Await mq.DeleteQueueAsync(queueName)
                Await mq.DeleteQueueAsync(queueName & ".dlq")
                Await mq.DeleteQueueAsync(queueName & ".parked")
            Next
            Await mq.DeleteQueueAsync(Policies.Audit)
            Await mq.DeleteExchangeAsync(Policies.Exchange)
        Finally
            mq.Dispose()
        End Try
    End Function

    Private Function WaitFor(done As Func(Of Boolean), what As String) As Task
        Return WaitFor(Function() Task.FromResult(done()), what)
    End Function

    Private Async Function WaitFor(done As Func(Of Task(Of Boolean)), what As String) As Task
        Dim deadline = DateTime.UtcNow + Patience
        While Not Await done()
            If DateTime.UtcNow > deadline Then
                Throw New TimeoutException($"waited {Patience.TotalSeconds:F0}s for {what}")
            End If
            Await Task.Delay(50)
        End While
    End Function

    ' An example that prints the right answer whatever happened is an example that
    ' cannot fail, and CI running it proves nothing.
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
