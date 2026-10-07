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

// Policy administration: a modular monolith, in C#.
//
//   docker compose up -d
//   dotnet run --project apps/02-policy-administration-csharp
//
// One deployable, six modules, one database and one connection -- and still no
// module that calls another. This file is the only one that names more than one
// module, which is the same role the Java app's system test plays.
//
// Five scenarios, each with a freshly started application, the way the Java test
// runs them. Each checks its own claims, and the process exits non-zero if any
// did not hold.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;
using Microsoft.Data.Sqlite;
using PolicyAdministration;

public static class Program
{
    // How long the application gets to reach the state a scenario waits for. The
    // Java test allows ninety seconds; every scenario gets there in well under one.
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private static readonly List<string> DatabaseFiles = new();

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());
        var url = BrokerUrl();

        var failed = 0;
        try
        {
            failed += await RunAsync(url, "an ordinary application becomes a policy, and the premium is taken once",
                TheHappyPath);
            failed += await RunAsync(url, "an application above the automatic limit is referred, and never becomes a policy",
                ReferredAboveTheLimit);
            failed += await RunAsync(url, "a claim is assessed against an answer from policies, not against a local copy",
                ClaimsAskRatherThanRead);
            failed += await RunAsync(url, "a large document travels as a claim check, not as a message",
                DocumentsTravelByReference);
            failed += await RunAsync(url, "three copies of one event charge once; a genuinely different event still charges",
                BillingIsIdempotent);
        }
        finally
        {
            // An example that leaves its queues behind changes the next run's
            // numbers. A real system would leave them alone.
            await ForgetEarlierRunsAsync(url);
            SqliteConnection.ClearAllPools();
            foreach (var file in DatabaseFiles)
            {
                foreach (var suffix in new[] { "", "-wal", "-shm" }) File.Delete(file + suffix);
            }
        }

        Console.WriteLine(failed == 0 ? "all five held" : $"{failed} of five did not hold");
        return failed == 0 ? 0 : 1;
    }

    // ---- the five scenarios -------------------------------------------------------

    private static async Task<int> TheHappyPath(TheApplication app)
    {
        await app.Policies.SubmitAsync("A. Applicant", "TERM-LIFE", 100_000, 40);

        // Submitted -> underwritten -> issued -> charged, with no module calling another.
        await WaitFor(() => app.Billing.Charges.Count == 1, "billing to charge");

        Check(app.Underwriting.Accepted == 1, $"underwriting accepted {app.Underwriting.Accepted}, not 1");
        Check(app.Policies.Issued == 1, $"policies issued {app.Policies.Issued}, not 1");
        Check(app.Billing.Charges.Count == 1, $"billing charged {app.Billing.Charges.Count} times, not once");

        // 100 base + 20 age loading, from the rating table in the pricing stage.
        var policyId = app.Billing.Charges[0];
        var premium = await app.Policies.PremiumOfAsync(policyId);
        Check(premium == 120, $"the premium of {policyId} is {premium}, not 120");

        // Submitted, accepted, issued, charged.
        return 4;
    }

    private static async Task<int> ReferredAboveTheLimit(TheApplication app)
    {
        await app.Policies.SubmitAsync("B. Applicant", "TERM-LIFE", 750_000, 35);

        await WaitFor(() => app.Underwriting.Declined == 1, "underwriting to decline");

        // Nothing downstream ran. Billing charging for a referred application would
        // be the expensive version of this bug, which is why it is bound to
        // PolicyIssued and not to the application.
        Check(app.Policies.Issued == 0, $"policies issued {app.Policies.Issued}, not none");
        Check(app.Billing.Charges.Count == 0, $"billing charged {app.Billing.Charges.Count} times, not never");

        // Submitted, declined.
        return 2;
    }

    private static async Task<int> ClaimsAskRatherThanRead(TheApplication app)
    {
        await app.Policies.SubmitAsync("C. Applicant", "TERM-LIFE", 50_000, 30);
        await WaitFor(() => app.Billing.Charges.Count == 1, "billing to charge");
        var policyId = app.Billing.Charges[0];

        await app.Claims.SubmitAsync(policyId, 5_000, "windscreen");
        // A policy nobody issued. The lookup is what makes this answerable at all.
        await app.Claims.SubmitAsync("POL-does-not-exist", 5_000, "windscreen");

        await WaitFor(() => app.Claims.Settled == 1 && app.Claims.Rejected == 1, "one claim settled and one rejected");

        // The four of the happy path, then settled and rejected.
        return 6;
    }

    private static async Task<int> DocumentsTravelByReference(TheApplication app)
    {
        await app.Policies.SubmitAsync("D. Applicant", "TERM-LIFE", 60_000, 45);
        await WaitFor(() => app.Billing.Charges.Count == 1, "billing to charge");
        var policyId = app.Billing.Charges[0];

        // Four megabytes, which is a small scan and a large message.
        var scan = new byte[4 * 1024 * 1024];
        var key = await app.Documents.StoreAsync(policyId, "medical-report", scan);

        var fetched = app.Documents.Fetch(key);
        Check(fetched is not null, $"the store has nothing under {key}");
        Check(fetched!.Length == scan.Length, $"the store gave back {fetched.Length} bytes, not {scan.Length}");

        // What crossed the broker is the key and the size. The whole event is a few
        // hundred bytes; the four megabytes never went near a queue.
        Check(key.Contains(policyId) && key.Contains("medical-report"),
            $"the key {key} does not say which policy and document it is");

        // The four of the happy path, then the document.
        return 5;
    }

    private static async Task<int> BillingIsIdempotent(TheApplication app)
    {
        await app.Policies.SubmitAsync("E. Applicant", "TERM-LIFE", 80_000, 50);
        await WaitFor(() => app.Billing.Charges.Count == 1, "billing to charge");
        var policyId = app.Billing.Charges[0];
        var premium = await app.Policies.PremiumOfAsync(policyId)
                      ?? throw new InvalidOperationException($"{policyId} is not in force");

        // Three copies carrying one message id: what a redelivery looks like from
        // the consumer's side, and what the idempotency store exists to absorb.
        var messageId = "redelivery-" + Guid.NewGuid();
        var issued = app.Mq.Publisher<PolicyIssued>(Policies.Exchange, Policies.PolicyIssuedKey);
        for (var i = 0; i < 3; i++)
        {
            await issued.SendAsync(
                new PolicyIssued(policyId, "APP-x", "E. Applicant", "TERM-LIFE", premium),
                Envelope.Of("PolicyIssued").Id(messageId).Build());
        }

        // Two, not one -- and the difference is the whole point. The store
        // deduplicates by message id, so the three copies are one charge. They are
        // not the same message as the original issue, which had an id of its own,
        // so suppressing that too would mean the store had stopped distinguishing
        // "sent twice" from "happened twice".
        // At least two rather than exactly two, so a third charge is reported as one
        // rather than as a wait that never ended.
        await WaitFor(() => app.Billing.Charges.Count >= 2, "billing to charge a second time");
        await Task.Delay(TimeSpan.FromSeconds(2));
        Check(app.Billing.Charges.Count == 2, $"billing charged {app.Billing.Charges.Count} times, not twice");

        // The four of the happy path, the three copies, and the one charge they made.
        return 8;
    }

    // ---- running one --------------------------------------------------------------

    // Starts the application, runs one scenario, and checks what every scenario
    // must leave behind whatever else it asserts -- none of which the Java test
    // checks. The audit queue, bound to policy.#, holds exactly the events the
    // scenario published: one fewer is a lost message, one more is a duplicate.
    // Nothing is in any module's .dlq or .parked queue, the outbox is empty, and no
    // lookup timed out.
    private static async Task<int> RunAsync(string url, string name, Func<TheApplication, Task<int>> scenario)
    {
        // Each scenario starts from an empty broker, so one that left something in a
        // dead-letter queue is not blamed on the next.
        await ForgetEarlierRunsAsync(url);

        var app = await TheApplication.StartAsync(url, OneDatabase());
        try
        {
            var events = await scenario(app);

            await WaitFor(async () => await app.Mq.MessageCountAsync(Policies.Audit) >= events,
                $"{events} events in the audit trail");
            await Task.Delay(TimeSpan.FromMilliseconds(500));
            var audited = await app.Mq.MessageCountAsync(Policies.Audit);
            Check(audited == events, $"the audit trail holds {audited} events, not {events}");

            var pending = await app.Policies.PendingInOutboxAsync();
            Check(pending == 0, $"{pending} record(s) still in the outbox");
            Check(app.Claims.LookupsTimedOut == 0, $"{app.Claims.LookupsTimedOut} lookup(s) timed out");
            foreach (var queue in Policies.ConsumedQueues)
            {
                foreach (var setAside in new[] { queue + ".dlq", queue + ".parked" })
                {
                    var count = await app.Mq.MessageCountAsync(setAside);
                    Check(count == 0, $"{count} message(s) in {setAside}");
                }
            }
            Console.WriteLine($"held      {name}");
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine($"DID NOT   {name}: {e.Message}");
            return 1;
        }
        finally
        {
            await app.StopAsync();
        }
    }

    private sealed class TheApplication
    {
        public required AceMqConnection Mq { get; init; }
        public required PolicyModule Policies { get; init; }
        public required UnderwritingModule Underwriting { get; init; }
        public required DocumentModule Documents { get; init; }
        public required BillingModule Billing { get; init; }
        public required ClaimsModule Claims { get; init; }

        public static async Task<TheApplication> StartAsync(string url, ConnectionSupplier database)
        {
            // One connection for the whole application, because it is one
            // application. In apps/01 each service had its own; here sharing one is
            // correct, and is the only thing that differs at the transport level.
            var mq = await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url).ClientName("examples/apps/02-policy-administration-csharp").Build());
            await mq.ApplyAsync(PolicyAdministration.Policies.Everything());

            // One database, several modules -- the monolith's actual advantage. The
            // outbox still has to exist, because the broker is not in this
            // database's transaction.
            return new TheApplication
            {
                Mq = mq,
                Policies = await PolicyModule.StartAsync(mq, database),
                Underwriting = await UnderwritingModule.StartAsync(mq),
                Documents = new DocumentModule(mq),
                Billing = await BillingModule.StartAsync(mq, database),
                Claims = await ClaimsModule.StartAsync(mq),
            };
        }

        // The relay first, so nothing is published into a connection that is going.
        // Closing the connection drains every handler before it closes.
        public Task StopAsync()
        {
            Policies.Dispose();
            return Mq.CloseAsync();
        }
    }

    /// <summary>One database for the whole application: a SQLite file nothing else opens.</summary>
    /// <remarks>
    /// Write-ahead logging, because three modules write to it at once -- the relay
    /// marking records published, policies issuing, billing claiming -- and in its
    /// default mode SQLite makes a reader wait for every writer.
    /// </remarks>
    private static ConnectionSupplier OneDatabase()
    {
        var file = Path.Combine(Path.GetTempPath(), $"policy-csharp-{Guid.NewGuid():N}.db");
        DatabaseFiles.Add(file);
        using (var connection = new SqliteConnection($"Data Source={file}"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "PRAGMA journal_mode=WAL;";
            command.ExecuteNonQuery();
        }
        return () => new SqliteConnection($"Data Source={file}");
    }

    private static async Task ForgetEarlierRunsAsync(string url)
    {
        using var mq = await AceMqConnection.ConnectAsync(url);
        foreach (var queue in Policies.ConsumedQueues)
        {
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
        }
        await mq.DeleteQueueAsync(Policies.Audit);
        await mq.DeleteExchangeAsync(Policies.Exchange);
    }

    private static Task WaitFor(Func<bool> done, string what) => WaitFor(() => Task.FromResult(done()), what);

    private static async Task WaitFor(Func<Task<bool>> done, string what)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!await done())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"waited {Patience.TotalSeconds:F0}s for {what}");
            }
            await Task.Delay(50);
        }
    }

    // An example that prints the right answer whatever happened is an example that
    // cannot fail, and CI running it proves nothing.
    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
