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

using AceMq.Amqp;

namespace EventSourcedLedger;

// The events a ledger is made of.
//
// In apps/01 and apps/02 the events describe what happened to the system of
// record. Here they ARE the system of record. There is no balances table that
// events update; a balance is what you get by adding up entries, and it can be
// deleted and recomputed without losing anything, because nothing was ever stored
// that the log does not contain.
//
// One consequence worth stating before the code: an entry is never changed and
// never deleted. Money moved wrongly is corrected by posting the opposite entry,
// exactly as a paper ledger does, and both entries stay.

// ---- the log ------------------------------------------------------------------

/// <summary>One side of one movement of money.</summary>
/// <remarks>
/// Signed rather than a debit/credit flag: a sum over a column is then simply a
/// sum. Whole minor units -- pennies, cents -- in a <c>long</c>, because a ledger in
/// <c>double</c> disagrees with itself after enough additions.
/// </remarks>
/// <param name="EntryId">Unique, and the idempotency key.</param>
/// <param name="TransferId">The movement this is one half of.</param>
/// <param name="Account">Whose balance changes.</param>
/// <param name="AmountMinor">Positive credits the account, negative debits it.</param>
/// <param name="Description">What a statement will show.</param>
public sealed record EntryPosted(
    string EntryId, string TransferId, string Account, long AmountMinor, string Description);

/// <summary>A transfer that was refused, with the reason kept beside the ones that were not.</summary>
public sealed record TransferRejected(string TransferId, string From, string To, long AmountMinor, string Reason);

// ---- commands -----------------------------------------------------------------

/// <summary>Move money between two accounts. Not an event: a request, and it may be refused.</summary>
public sealed record Transfer(string TransferId, string From, string To, long AmountMinor, string Description);

public static class Ledger
{
    // Every broker object this app declares starts with this; routing keys and
    // envelope types are the Java app's, character for character. The C# and
    // VB.NET twins share a broker in CI, and two ledgers appending to one journal
    // would each rebuild the other's balances.
    private const string Prefix = "dotnet-csharp.";

    /// <summary>The stream every entry is appended to.</summary>
    /// <remarks>
    /// A stream rather than a queue, and the difference is the point. A queue is
    /// emptied by being read; a stream is not. Ten readers can each read all of
    /// history at their own pace, and nothing anybody reads removes anything for
    /// anybody else.
    /// </remarks>
    public const string Journal = Prefix + "ledger.journal";

    /// <summary>Where transfer commands arrive. An ordinary queue: a command is handled once.</summary>
    public const string Commands = Prefix + "ledger.commands";

    /// <summary>Where refusals are announced, for whoever wants to be told rather than to read.</summary>
    public const string Rejections = Prefix + "ledger.rejections";

    /// <summary>Where the ledger announces what it decided, for anything that is not a projection.</summary>
    public const string Exchange = Prefix + "ledger";

    public const string EntryPostedKey = "ledger.entry.posted";
    public const string TransferRequestedKey = "ledger.transfer.requested";
    public const string TransferRejectedKey = "ledger.transfer.rejected";

    /// <summary>The whole application's topology, apart from the journal.</summary>
    /// <remarks>
    /// Commands go to an ordinary queue and entries to a stream. Getting that
    /// backwards is the most common mistake in event-sourced systems. The journal
    /// is declared by its only writer, with its retention, in LedgerModule.
    /// </remarks>
    public static Topology Everything() =>
        AceMq.Amqp.Topology.Define()
            .Exchange(Exchange, "topic")

            // Commands: an ordinary queue, because a transfer must be applied once.
            .Queue(Commands, QueueType.Classic)
            .Bind(Commands, Exchange, TransferRequestedKey)

            // Rejections are announced so somebody can act on them.
            .Queue(Rejections, QueueType.Classic)
            .Bind(Rejections, Exchange, TransferRejectedKey)

            .Build();
}
