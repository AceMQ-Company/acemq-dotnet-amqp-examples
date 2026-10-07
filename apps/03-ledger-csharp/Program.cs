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

// An event-sourced ledger on a RabbitMQ stream, in C#.
//
//   docker compose up -d
//   dotnet run --project apps/03-ledger-csharp
//
// The log is the system of record. Balances are not stored anywhere: the writer
// rebuilds them from the journal every time it starts, and a projection rebuilds
// them again from offset zero and must agree.
//
// The Java test's five scenarios, each with a freshly started ledger -- and, as
// there, one journal shared by all of them, so every start after the first
// rebuilds balances from entries an earlier ledger wrote. Then one check of the
// whole journal the Java test does not make. The process exits non-zero if
// anything did not hold.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;
using EventSourcedLedger;

public static class Program
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    // Every entry any ledger in this run appended, so the last check knows how
    // many the journal must hold.
    private static long _posted;

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());
        var url = BrokerUrl();

        var failed = 0;
        try
        {
            // A journal left by an earlier run would be rebuilt into this one's
            // balances: alice would start with last time's money.
            await ForgetEarlierRunsAsync(url);

            failed += await RunAsync(url, "a transfer posts two entries that sum to zero", DoubleEntry);
            failed += await RunAsync(url, "a transfer that would overdraw is refused, and the refusal is recorded",
                InsufficientFunds);
            failed += await RunAsync(url, "a projection built from offset zero agrees with the writer",
                ProjectionAgreesWithTheWriter);
            failed += await RunAsync(url, "a projection added later still gets all of history",
                ALaterProjectionSeesEverything);
            failed += await RunAsync(url, "two readers of the same stream do not compete for entries",
                ReadersDoNotCompete);
            failed += await RunAsync(url, "the journal holds every entry once, and a restarted writer agrees with it",
                TheJournalIsTheRecord);
        }
        finally
        {
            await ForgetEarlierRunsAsync(url);
        }

        Console.WriteLine(failed == 0 ? "all six held" : $"{failed} of six did not hold");
        return failed == 0 ? 0 : 1;
    }

    // ---- the Java test's five -----------------------------------------------------

    private static async Task DoubleEntry(TheLedger app)
    {
        await app.Ledger.FundAsync("alice", 10_000);
        await WaitFor(() => app.Ledger.BalanceOf("alice") == 10_000, "alice to be funded");

        await app.Transfers.RequestAsync("alice", "bob", 2_500, "rent");

        await WaitFor(() => app.Ledger.BalanceOf("bob") == 2_500, "bob to be paid");
        Check(app.Ledger.BalanceOf("alice") == 7_500, $"alice holds {app.Ledger.BalanceOf("alice")}, not 7500");

        // The invariant: money is neither created nor destroyed by a transfer.
        var total = app.Ledger.BalanceOf("alice") + app.Ledger.BalanceOf("bob");
        Check(total == 10_000, $"alice and bob hold {total} between them, not 10000");
    }

    private static async Task InsufficientFunds(TheLedger app)
    {
        await app.Ledger.FundAsync("carol", 1_000);
        await WaitFor(() => app.Ledger.BalanceOf("carol") == 1_000, "carol to be funded");

        await app.Transfers.RequestAsync("carol", "dave", 5_000, "optimistic");

        await WaitFor(() => app.Transfers.Refused.Count > 0, "the transfer to be refused");
        var reason = app.Transfers.Refused[0].Reason;
        Check(reason.Contains("insufficient funds"), $"the refusal says \"{reason}\", not insufficient funds");

        // Nothing was posted. A ledger that half-applies a refused transfer is worse
        // than one that refuses loudly.
        Check(app.Ledger.BalanceOf("carol") == 1_000, $"carol holds {app.Ledger.BalanceOf("carol")}, not 1000");
        Check(app.Ledger.BalanceOf("dave") == 0, $"dave holds {app.Ledger.BalanceOf("dave")}, not nothing");
    }

    private static async Task ProjectionAgreesWithTheWriter(TheLedger app)
    {
        await app.Ledger.FundAsync("erin", 20_000);
        await WaitFor(() => app.Ledger.BalanceOf("erin") == 20_000, "erin to be funded");
        await app.Transfers.RequestAsync("erin", "frank", 3_000, "invoice 1");
        await app.Transfers.RequestAsync("erin", "frank", 4_000, "invoice 2");
        await WaitFor(() => app.Ledger.BalanceOf("frank") == 7_000, "frank to be paid twice");

        // A reader that has never seen a message, starting from the beginning of
        // time. It stores nothing the log does not contain, and it must reach the
        // same answer.
        using var statements = await StatementProjection.StartAsync(app.Mq, fromFirst: true);
        await WaitFor(() => statements.BalanceOf("frank") == 7_000, "the projection to reach frank's balance");

        Check(statements.BalanceOf("erin") == app.Ledger.BalanceOf("erin"),
            $"the projection says erin holds {statements.BalanceOf("erin")}, the writer {app.Ledger.BalanceOf("erin")}");
        Check(statements.BalanceOf("frank") == app.Ledger.BalanceOf("frank"),
            $"the projection says frank holds {statements.BalanceOf("frank")}, the writer {app.Ledger.BalanceOf("frank")}");

        // And it has the detail the balance does not: three entries against erin --
        // the opening balance and two debits.
        var erin = statements.StatementOf("erin");
        Check(erin.Count == 3, $"erin's statement has {erin.Count} entries, not 3");
        var frank = statements.StatementOf("frank").Select(entry => entry.Description).ToArray();
        Check(frank.SequenceEqual(new[] { "invoice 1", "invoice 2" }),
            $"frank's statement reads [{string.Join(", ", frank)}], not [invoice 1, invoice 2]");
    }

    private static async Task ALaterProjectionSeesEverything(TheLedger app)
    {
        await app.Ledger.FundAsync("grace", 5_000);
        await app.Transfers.RequestAsync("grace", "heidi", 1_000, "before the projection existed");
        await WaitFor(() => app.Ledger.BalanceOf("heidi") == 1_000, "heidi to be paid");

        // Started now, after the entries were written. On a queue there would be
        // nothing left to read. A stream is not emptied by reading, so this gets
        // everything, and so would one written next year.
        using var late = await StatementProjection.StartAsync(app.Mq, fromFirst: true);
        await WaitFor(() => late.BalanceOf("heidi") == 1_000, "the late projection to reach heidi's balance");

        var heidi = late.StatementOf("heidi");
        Check(heidi.Count == 1, $"heidi's statement has {heidi.Count} entries, not 1");
        Check(heidi[0].Description == "before the projection existed",
            $"heidi's entry reads \"{heidi[0].Description}\"");
    }

    private static async Task ReadersDoNotCompete(TheLedger app)
    {
        await app.Ledger.FundAsync("ivan", 8_000);
        await app.Transfers.RequestAsync("ivan", "judy", 2_000, "shared");
        await WaitFor(() => app.Ledger.BalanceOf("judy") == 2_000, "judy to be paid");

        using var first = await StatementProjection.StartAsync(app.Mq, fromFirst: true);
        using var second = await StatementProjection.StartAsync(app.Mq, fromFirst: true);

        await WaitFor(() => first.BalanceOf("judy") == 2_000 && second.BalanceOf("judy") == 2_000,
            "both projections to reach judy's balance");

        // Both saw the same entry. On a queue exactly one of them would have, which
        // is what makes a queue wrong for a ledger and right for a command.
        Check(first.StatementOf("judy").Count == 1, $"the first projection has {first.StatementOf("judy").Count} entries for judy");
        Check(second.StatementOf("judy").Count == 1, $"the second projection has {second.StatementOf("judy").Count} entries for judy");
    }

    // ---- and one the Java test does not make --------------------------------------

    // Every scenario above checks the accounts it touched. This checks the log
    // itself, after five ledgers have each rebuilt from it and appended to it: it
    // holds exactly the entries those ledgers posted, no entry twice, every
    // transfer's entries summing to zero -- and this run's freshly started writer,
    // which rebuilt every balance from that log, agrees with a projection of it
    // account by account. A library that dropped or duplicated an entry on the way
    // in or out of the stream fails here even if every scenario's own accounts
    // happened to come out right.
    private static async Task TheJournalIsTheRecord(TheLedger app)
    {
        using var everything = await StatementProjection.StartAsync(app.Mq, fromFirst: true);
        var expected = Interlocked.Read(ref _posted);
        await WaitFor(() => everything.Entries >= expected, $"{expected} entries in the journal");
        await Task.Delay(TimeSpan.FromSeconds(1));
        Check(everything.Entries == expected, $"the journal holds {everything.Entries} entries; {expected} were posted");

        var entries = everything.Accounts.SelectMany(everything.StatementOf).ToArray();
        var distinct = entries.Select(entry => entry.EntryId).Distinct().Count();
        Check(distinct == entries.Length, $"{entries.Length - distinct} entries appear more than once");

        foreach (var transfer in entries.Where(e => !e.TransferId.StartsWith("OPENING-")).GroupBy(e => e.TransferId))
        {
            Check(transfer.Count() == 2 && transfer.Sum(entry => entry.AmountMinor) == 0,
                $"transfer {transfer.Key} has {transfer.Count()} entries summing to {transfer.Sum(entry => entry.AmountMinor)}");
        }

        foreach (var account in everything.Accounts)
        {
            Check(app.Ledger.BalanceOf(account) == everything.BalanceOf(account),
                $"the restarted writer says {account} holds {app.Ledger.BalanceOf(account)}, " +
                $"the journal {everything.BalanceOf(account)}");
        }
    }

    // ---- running one --------------------------------------------------------------

    // Starts a ledger, runs one scenario, and checks what none may leave behind:
    // nothing in the .dlq or .parked queue beside the commands, the rejections or
    // the journal itself.
    private static async Task<int> RunAsync(string url, string name, Func<TheLedger, Task> scenario)
    {
        var app = await TheLedger.StartAsync(url);
        try
        {
            await scenario(app);
            foreach (var queue in new[] { Ledger.Commands, Ledger.Rejections, Ledger.Journal })
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
            Interlocked.Add(ref _posted, app.Ledger.Posted);
            await app.Mq.CloseAsync();
        }
    }

    private sealed class TheLedger
    {
        public required AceMqConnection Mq { get; init; }
        public required LedgerModule Ledger { get; init; }
        public required TransferGateway Transfers { get; init; }

        public static async Task<TheLedger> StartAsync(string url)
        {
            var mq = await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url).ClientName("examples/apps/03-ledger-csharp").Build());
            await mq.ApplyAsync(EventSourcedLedger.Ledger.Everything());
            return new TheLedger
            {
                Mq = mq,
                Ledger = await LedgerModule.StartAsync(mq),
                Transfers = await TransferGateway.StartAsync(mq),
            };
        }
    }

    private static async Task ForgetEarlierRunsAsync(string url)
    {
        using var mq = await AceMqConnection.ConnectAsync(url);
        foreach (var queue in new[] { Ledger.Commands, Ledger.Rejections, Ledger.Journal })
        {
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
        }
        await mq.DeleteExchangeAsync(Ledger.Exchange);
    }

    private static async Task WaitFor(Func<bool> done, string what)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!done())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"waited {Patience.TotalSeconds:F0}s for {what}");
            }
            await Task.Delay(50);
        }
    }

    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
