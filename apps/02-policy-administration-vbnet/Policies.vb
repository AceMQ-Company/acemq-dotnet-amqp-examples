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

' Applications and policies: the module that owns the records everything else
' refers to.
'
' Two things here are worth the reading.
'
' The outbox is still necessary. This is a monolith with one database, so the
' usual argument for an outbox -- two services, two datastores -- does not apply.
' It applies anyway, because the two systems that must agree are this database
' and the broker, and no transaction spans both. A monolith removes the
' distributed transaction between modules; it does not remove the one between a
' module and its broker.
'
' Claims asks this module a question rather than reading its tables. The module
' answers on a queue. In one process a direct call would obviously work, which is
' exactly why the discipline matters: the moment claims calls a method here, the
' two modules are one module.

Imports System.Data.Common
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class PolicyModule
    Implements IDisposable

    Private ReadOnly _mq As AceMqConnection
    Private ReadOnly _database As ConnectionSupplier
    Private ReadOnly _outbox As DbOutboxStore
    Private ReadOnly _relay As OutboxRelay
    Private _issued As Integer

    Private Sub New(mq As AceMqConnection, database As ConnectionSupplier)
        _mq = mq
        _database = database
        _outbox = New DbOutboxStore(database)

        ' The relay opens connections of its own: it runs on its own schedule and
        ' must not be inside anybody's transaction. Every 200 ms, twenty at a time.
        _relay = New OutboxRelay(mq, _outbox, 20, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30))
    End Sub

    ' The instance is called started rather than policies: VB.NET ignores case, and
    ' a local called policies would hide the Policies contract for the whole method.
    Public Shared Async Function StartAsync(mq As AceMqConnection, database As ConnectionSupplier) As Task(Of PolicyModule)
        Dim started As New PolicyModule(mq, database)
        started.CreateSchema()
        started._relay.Start()

        ' Explicit JSON, as for every consumer of what an outbox publishes: the
        ' relay republishes the stored bytes as they were.
        Dim json = ConsumerOptions.Defaults().As(CodecRegistry.ByName("json"))
        Await mq.ConsumeAsync(Of ApplicationAccepted)(Policies.PoliciesQueue, json,
            Async Function(message)
                Await started.IssueAsync(message.Payload)
                Return Ack.Accept()
            End Function)

        Await mq.RespondAsync(Of PolicyQuery, PolicyStatus)(Policies.PolicyLookup, AddressOf started.StatusOfAsync)
        Return started
    End Function

    ''' <summary>Takes an application, and announces it, in one transaction.</summary>
    Public Async Function SubmitAsync(applicant As String, product As String, sumAssured As Integer,
                                      age As Integer) As Task(Of String)
        Dim applicationId = "APP-" & Guid.NewGuid().ToString("N").Substring(0, 8)

        Using connection = _database()
            connection.Open()
            Using transaction = connection.BeginTransaction()
                Execute(connection, transaction,
                    "INSERT INTO applications (id, applicant, product, sum_assured, age) " &
                    "VALUES (@id, @applicant, @product, @sum, @age)",
                    ("@id", applicationId), ("@applicant", applicant), ("@product", product),
                    ("@sum", sumAssured), ("@age", age))

                Await _outbox.AddAsync(
                    OutboxRecord.For(
                        _mq, Policies.Exchange, Policies.ApplicationSubmittedKey,
                        New ApplicationSubmitted With {
                            .ApplicationId = applicationId, .Applicant = applicant, .Product = product,
                            .SumAssured = sumAssured, .AgeOfApplicant = age},
                        Envelope.Of("ApplicationSubmitted").CorrelationId(applicationId).Build()),
                    transaction)

                ' One commit decides both. Either the application exists and the
                ' event is queued, or neither happened: disposing an uncommitted
                ' transaction rolls it back.
                transaction.Commit()
            End Using
        End Using
        Return applicationId
    End Function

    ''' <summary>Underwriting said yes, so the policy exists.</summary>
    Private Async Function IssueAsync(accepted As ApplicationAccepted) As Task
        Dim policyId = "POL-" & Guid.NewGuid().ToString("N").Substring(0, 8)

        Using connection = _database()
            connection.Open()
            Using transaction = connection.BeginTransaction()
                Execute(connection, transaction,
                    "INSERT INTO policies (id, application_id, applicant, product, premium) " &
                    "VALUES (@id, @application, @applicant, @product, @premium)",
                    ("@id", policyId), ("@application", accepted.ApplicationId), ("@applicant", accepted.Applicant),
                    ("@product", accepted.Product), ("@premium", accepted.AnnualPremium))

                Await _outbox.AddAsync(
                    OutboxRecord.For(
                        _mq, Policies.Exchange, Policies.PolicyIssuedKey,
                        New PolicyIssued With {
                            .PolicyId = policyId, .ApplicationId = accepted.ApplicationId,
                            .Applicant = accepted.Applicant, .Product = accepted.Product,
                            .AnnualPremium = accepted.AnnualPremium},
                        Envelope.Of("PolicyIssued").CorrelationId(accepted.ApplicationId).Build()),
                    transaction)

                transaction.Commit()
            End Using
        End Using
        Interlocked.Increment(_issued)
    End Function

    ''' <summary>Answers the question claims asks, without claims touching this module's tables.</summary>
    Private Function StatusOfAsync(query As PolicyQuery) As Task(Of PolicyStatus)
        Using connection = _database()
            connection.Open()
            Using selectPremium = connection.CreateCommand()
                selectPremium.CommandText = "SELECT premium FROM policies WHERE id = @id"
                Add(selectPremium, "@id", query.PolicyId)
                Dim premium = selectPremium.ExecuteScalar()
                Return Task.FromResult(New PolicyStatus With {
                    .PolicyId = query.PolicyId,
                    .InForce = premium IsNot Nothing,
                    .AnnualPremium = If(premium Is Nothing, 0, Convert.ToInt32(premium))})
            End Using
        End Using
    End Function

    Public ReadOnly Property Issued As Integer
        Get
            Return Volatile.Read(_issued)
        End Get
    End Property

    ''' <summary>The premium recorded for a policy, when it exists.</summary>
    Public Async Function PremiumOfAsync(policyId As String) As Task(Of Integer?)
        Dim status = Await StatusOfAsync(New PolicyQuery With {.PolicyId = policyId})
        Return If(status.InForce, status.AnnualPremium, CType(Nothing, Integer?))
    End Function

    ''' <summary>Recorded and not yet published. Zero once the relay has caught up.</summary>
    Public Function PendingInOutboxAsync() As Task(Of Long)
        Return _outbox.PendingCountAsync()
    End Function

    ''' <summary>Stops the relay. The consumers close with the connection.</summary>
    Public Sub Dispose() Implements IDisposable.Dispose
        _relay.Dispose()
    End Sub

    Private Sub CreateSchema()
        Using connection = _database()
            connection.Open()
            Using command = connection.CreateCommand()
                ' The outbox table is the library's own statement, handed over rather
                ' than run, because a library that creates tables unasked has
                ' overstepped.
                command.CommandText =
                    "CREATE TABLE applications (id VARCHAR(64) PRIMARY KEY, applicant VARCHAR(255), " &
                    "product VARCHAR(64), sum_assured INT, age INT);" &
                    "CREATE TABLE policies (id VARCHAR(64) PRIMARY KEY, application_id VARCHAR(64), " &
                    "applicant VARCHAR(255), product VARCHAR(64), premium INT);" &
                    _outbox.CreateTableSql()
                command.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Private Shared Sub Execute(connection As DbConnection, transaction As DbTransaction, sql As String,
                               ParamArray values As (Name As String, Value As Object)())
        Using command = connection.CreateCommand()
            command.Transaction = transaction
            command.CommandText = sql
            For Each value In values
                Add(command, value.Name, value.Value)
            Next
            command.ExecuteNonQuery()
        End Using
    End Sub

    Private Shared Sub Add(command As DbCommand, name As String, value As Object)
        Dim parameter = command.CreateParameter()
        parameter.ParameterName = name
        parameter.Value = value
        command.Parameters.Add(parameter)
    End Sub

End Class
