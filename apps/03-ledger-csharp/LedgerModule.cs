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

using System.Collections.Concurrent;
using System.Diagnostics;

using AceMq.Amqp;

namespace EventSourcedLedger;

// The only thing allowed to append to the journal.
//
// One writer, deliberately. A ledger's invariant -- every transfer produces two
// entries that sum to zero -- cannot be enforced by two processes appending
// independently, and a stream will happily accept an unbalanced pair from each.
// Making the writer singular is what makes the invariant checkable at all.
//
// Balances are not stored here. This module decides whether a transfer is allowed
// and appends the entries; its view of the balances is rebuilt from the journal
// every time it starts, for the one decision it has to make.
public sealed class LedgerModule
{
    /// <summary>How long the journal keeps entries.</summary>
    /// <remarks>
    /// An hour, because this is an example. A real ledger keeps them as long as the
    /// law says, which is years, and this is the setting people get wrong: if the
    /// retention is shorter than "forever", the projection is the system of record
    /// after all and nobody wrote that down.
    /// </remarks>
    private static readonly TimeSpan Retention = TimeSpan.FromHours(1);

    private const long MaxBytes = 50L * 1024 * 1024;

    private readonly IPublisher<EntryPosted> _journal;
    private readonly IPublisher<TransferRejected> _rejections;
    private readonly Balances _balances;

    // One writer is a claim about processes; this makes it true inside this one
    // too. A transfer and an opening balance never interleave, so a decision is
    // always made against every entry before it.
    private readonly SemaphoreSlim _writing = new(1, 1);

    private long _posted;
    private long _rejected;

    private LedgerModule(AceMqConnection mq, Balances balances)
    {
        // Published straight at the stream by name. A stream is addressed as a
        // queue, so the default exchange and the stream's name is the whole of it.
        _journal = mq.Publisher<EntryPosted>("", Ledger.Journal);
        _rejections = mq.Publisher<TransferRejected>(Ledger.Exchange, Ledger.TransferRejectedKey);
        _balances = balances;
    }

    public static async Task<LedgerModule> StartAsync(AceMqConnection mq)
    {
        await mq.DeclareStreamAsync(Ledger.Journal, Retention, MaxBytes);

        // The writer's own view of the balances, rebuilt from the journal. Not a
        // cache of somebody else's state: derived here, from the log.
        var ledger = new LedgerModule(mq, await Balances.RebuiltFromAsync(mq));

        await mq.ConsumeAsync<Transfer>(Ledger.Commands, async message =>
        {
            await ledger.ApplyAsync(message.Payload);
            return Ack.Accept();
        });
        return ledger;
    }

    private async Task ApplyAsync(Transfer transfer)
    {
        await _writing.WaitAsync();
        try
        {
            var available = _balances.Of(transfer.From);
            if (transfer.AmountMinor <= 0)
            {
                await RejectAsync(transfer, "a transfer must be for a positive amount");
                return;
            }
            if (available < transfer.AmountMinor)
            {
                // Refused, and the refusal is recorded. A ledger that silently drops
                // what it will not do cannot explain itself later.
                await RejectAsync(transfer, $"insufficient funds: {transfer.From} holds {available}");
                return;
            }

            var envelope = Envelope.Of("EntryPosted").CorrelationId(transfer.TransferId).Build();

            // Two entries, one transfer, summing to zero. Appended one after the
            // other by the only writer there is -- a real ledger appends them as one
            // record so a crash between them is impossible, and that is the honest
            // limitation of doing it this way.
            await PostAsync(new EntryPosted(EntryId(), transfer.TransferId, transfer.From,
                -transfer.AmountMinor, transfer.Description), envelope);
            await PostAsync(new EntryPosted(EntryId(), transfer.TransferId, transfer.To,
                transfer.AmountMinor, transfer.Description), envelope);
        }
        finally
        {
            _writing.Release();
        }
    }

    /// <summary>Opens an account with money in it, which every ledger needs a way to do.</summary>
    public async Task FundAsync(string account, long amountMinor)
    {
        await _writing.WaitAsync();
        try
        {
            var entry = new EntryPosted(EntryId(), "OPENING-" + account, account, amountMinor, "opening balance");
            await PostAsync(entry, Envelope.Of("EntryPosted").CorrelationId(entry.TransferId).Build());
        }
        finally
        {
            _writing.Release();
        }
    }

    // Appended, then applied: once the stream has it, and not before. Applied here
    // rather than read back off the stream -- see Balances for why both at once is
    // the bug.
    private async Task PostAsync(EntryPosted entry, Envelope envelope)
    {
        await _journal.SendAsync(entry, envelope);
        _balances.Apply(entry);
        Interlocked.Increment(ref _posted);
    }

    private async Task RejectAsync(Transfer transfer, string reason)
    {
        await _rejections.SendAsync(
            new TransferRejected(transfer.TransferId, transfer.From, transfer.To, transfer.AmountMinor, reason),
            Envelope.Of("TransferRejected").CorrelationId(transfer.TransferId).Build());
        Interlocked.Increment(ref _rejected);
    }

    /// <summary>This module's own view, derived from the log.</summary>
    public long BalanceOf(string account) => _balances.Of(account);

    public long Posted => Interlocked.Read(ref _posted);

    public long Rejected => Interlocked.Read(ref _rejected);

    private static string EntryId() => "E-" + Guid.NewGuid();
}

// Balances, computed by reading the journal from the beginning.
//
// This holds no state anybody wrote down. Delete it, restart the process, and it
// comes back identical, because it is a function of the log and nothing else.
// FromFirst() is the whole trick: a queue cannot do this -- reading it consumes it.
//
// Read to the end, then stop. The reader is closed once it has caught up, and the
// writer maintains the balances itself from then on. That is a correctness
// requirement, not an optimisation: the Java app's first version kept following
// the stream AND applied each entry as it was written, so every posting was
// counted twice. Keeping only the stream has the opposite problem -- a transfer
// decided against a balance that does not yet include the one before it.
//
// A rebuild is O(history). At a billion entries the answer is a snapshot -- "the
// balance at offset N, plus everything after N" -- deliberately not here.
internal sealed class Balances
{
    /// <summary>How long without an entry counts as caught up.</summary>
    /// <remarks>
    /// Crude, and honest about it. The precise way is to read the offset of the last
    /// entry before starting and stop there.
    /// </remarks>
    private static readonly TimeSpan QuietPeriod = TimeSpan.FromMilliseconds(400);

    private static readonly TimeSpan RebuildLimit = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<string, long> _accounts = new();

    private Balances() { }

    public static async Task<Balances> RebuiltFromAsync(AceMqConnection mq)
    {
        var balances = new Balances();
        var lastSeen = Stopwatch.GetTimestamp();
        var started = Stopwatch.StartNew();

        using var reader = await mq.Stream<EntryPosted>(Ledger.Journal).FromFirst().ConsumeAsync(message =>
        {
            balances.Apply(message.Payload);
            Interlocked.Exchange(ref lastSeen, Stopwatch.GetTimestamp());
            return Task.CompletedTask;
        });

        while (Stopwatch.GetElapsedTime(Interlocked.Read(ref lastSeen)) < QuietPeriod)
        {
            if (started.Elapsed > RebuildLimit)
            {
                throw new InvalidOperationException(
                    $"the journal did not stop producing entries within {RebuildLimit.TotalSeconds:F0}s; " +
                    "a rebuild cannot finish while somebody is still writing");
            }
            await Task.Delay(20);
        }

        // A reader that failed on an entry stopped there, and balances built from
        // part of the log are wrong in a way nothing downstream can see.
        if (reader.Failed > 0)
        {
            throw new InvalidOperationException($"could not rebuild balances: {reader.Failed} entries failed");
        }
        return balances;
    }

    /// <summary>Applied by the writer as it appends, which is safe because there is one writer.</summary>
    public void Apply(EntryPosted entry) =>
        _accounts.AddOrUpdate(entry.Account, entry.AmountMinor, (_, balance) => balance + entry.AmountMinor);

    public long Of(string account) => _accounts.TryGetValue(account, out var balance) ? balance : 0L;
}
