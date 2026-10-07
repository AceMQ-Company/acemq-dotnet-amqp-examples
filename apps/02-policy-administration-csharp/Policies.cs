// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Data.Common;

using AceMq.Amqp;

namespace PolicyAdministration;

// Applications and policies: the module that owns the records everything else
// refers to.
//
// Two things here are worth the reading.
//
// The outbox is still necessary. This is a monolith with one database, so the
// usual argument for an outbox -- two services, two datastores -- does not apply.
// It applies anyway, because the two systems that must agree are this database
// and the broker, and no transaction spans both. A monolith removes the
// distributed transaction between modules; it does not remove the one between a
// module and its broker.
//
// Claims asks this module a question rather than reading its tables. The module
// answers on a queue. In one process a direct call would obviously work, which is
// exactly why the discipline matters: the moment claims calls a method here, the
// two modules are one module.
public sealed class PolicyModule : IDisposable
{
    private readonly AceMqConnection _mq;
    private readonly ConnectionSupplier _database;
    private readonly DbOutboxStore _outbox;
    private readonly OutboxRelay _relay;
    private int _issued;

    private PolicyModule(AceMqConnection mq, ConnectionSupplier database)
    {
        _mq = mq;
        _database = database;
        _outbox = new DbOutboxStore(database);

        // The relay opens connections of its own: it runs on its own schedule and
        // must not be inside anybody's transaction. Every 200 ms, twenty at a time.
        _relay = new OutboxRelay(mq, _outbox, 20, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30));
    }

    public static async Task<PolicyModule> StartAsync(AceMqConnection mq, ConnectionSupplier database)
    {
        var policies = new PolicyModule(mq, database);
        policies.CreateSchema();
        policies._relay.Start();

        // Explicit JSON, as for every consumer of what an outbox publishes: the
        // relay republishes the stored bytes as they were.
        var json = ConsumerOptions.Defaults().As(CodecRegistry.ByName("json"));
        await mq.ConsumeAsync<ApplicationAccepted>(Policies.PoliciesQueue, json, async message =>
        {
            await policies.IssueAsync(message.Payload);
            return Ack.Accept();
        });

        await mq.RespondAsync<PolicyQuery, PolicyStatus>(Policies.PolicyLookup, policies.StatusOfAsync);
        return policies;
    }

    /// <summary>Takes an application, and announces it, in one transaction.</summary>
    public async Task<string> SubmitAsync(string applicant, string product, int sumAssured, int age)
    {
        var applicationId = "APP-" + Guid.NewGuid().ToString("N")[..8];

        using var connection = _database();
        connection.Open();
        using var transaction = connection.BeginTransaction();

        Execute(connection, transaction,
            "INSERT INTO applications (id, applicant, product, sum_assured, age) " +
            "VALUES (@id, @applicant, @product, @sum, @age)",
            ("@id", applicationId), ("@applicant", applicant), ("@product", product),
            ("@sum", sumAssured), ("@age", age));

        await _outbox.AddAsync(
            OutboxRecord.For(
                _mq, Policies.Exchange, Policies.ApplicationSubmittedKey,
                new ApplicationSubmitted(applicationId, applicant, product, sumAssured, age),
                Envelope.Of("ApplicationSubmitted").CorrelationId(applicationId).Build()),
            transaction);

        // One commit decides both. Either the application exists and the event is
        // queued, or neither happened: disposing an uncommitted transaction rolls
        // it back.
        transaction.Commit();
        return applicationId;
    }

    /// <summary>Underwriting said yes, so the policy exists.</summary>
    private async Task IssueAsync(ApplicationAccepted accepted)
    {
        var policyId = "POL-" + Guid.NewGuid().ToString("N")[..8];

        using var connection = _database();
        connection.Open();
        using var transaction = connection.BeginTransaction();

        Execute(connection, transaction,
            "INSERT INTO policies (id, application_id, applicant, product, premium) " +
            "VALUES (@id, @application, @applicant, @product, @premium)",
            ("@id", policyId), ("@application", accepted.ApplicationId), ("@applicant", accepted.Applicant),
            ("@product", accepted.Product), ("@premium", accepted.AnnualPremium));

        await _outbox.AddAsync(
            OutboxRecord.For(
                _mq, Policies.Exchange, Policies.PolicyIssuedKey,
                new PolicyIssued(
                    policyId, accepted.ApplicationId, accepted.Applicant, accepted.Product, accepted.AnnualPremium),
                Envelope.Of("PolicyIssued").CorrelationId(accepted.ApplicationId).Build()),
            transaction);

        transaction.Commit();
        Interlocked.Increment(ref _issued);
    }

    /// <summary>Answers the question claims asks, without claims touching this module's tables.</summary>
    private Task<PolicyStatus> StatusOfAsync(PolicyQuery query)
    {
        using var connection = _database();
        connection.Open();
        using var select = connection.CreateCommand();
        select.CommandText = "SELECT premium FROM policies WHERE id = @id";
        Add(select, "@id", query.PolicyId);
        var premium = select.ExecuteScalar();
        return Task.FromResult(premium is null
            ? new PolicyStatus(query.PolicyId, false, 0)
            : new PolicyStatus(query.PolicyId, true, Convert.ToInt32(premium)));
    }

    public int Issued => Volatile.Read(ref _issued);

    /// <summary>The premium recorded for a policy, when it exists.</summary>
    public async Task<int?> PremiumOfAsync(string policyId)
    {
        var status = await StatusOfAsync(new PolicyQuery(policyId));
        return status.InForce ? status.AnnualPremium : null;
    }

    /// <summary>Recorded and not yet published. Zero once the relay has caught up.</summary>
    public Task<long> PendingInOutboxAsync() => _outbox.PendingCountAsync();

    /// <summary>Stops the relay. The consumers close with the connection.</summary>
    public void Dispose() => _relay.Dispose();

    private void CreateSchema()
    {
        using var connection = _database();
        connection.Open();
        using var command = connection.CreateCommand();
        // The outbox table is the library's own statement, handed over rather than
        // run, because a library that creates tables unasked has overstepped.
        command.CommandText =
            "CREATE TABLE applications (id VARCHAR(64) PRIMARY KEY, applicant VARCHAR(255), " +
            "product VARCHAR(64), sum_assured INT, age INT);" +
            "CREATE TABLE policies (id VARCHAR(64) PRIMARY KEY, application_id VARCHAR(64), " +
            "applicant VARCHAR(255), product VARCHAR(64), premium INT);" +
            _outbox.CreateTableSql();
        command.ExecuteNonQuery();
    }

    private static void Execute(
        DbConnection connection, DbTransaction transaction, string sql, params (string Name, object Value)[] values)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        foreach (var (name, value) in values) Add(command, name, value);
        command.ExecuteNonQuery();
    }

    private static void Add(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
