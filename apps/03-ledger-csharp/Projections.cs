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

using AceMq.Amqp;

namespace EventSourcedLedger;

// A statement per account, built by reading the journal.
//
// This module makes one claim checkable: a projection is disposable. It stores
// nothing the log does not contain, it is built by reading from offset zero, and
// throwing it away costs nothing but the time to read the log again.
//
// It also proves the log is genuinely shared. The ledger reads the same stream
// from the same offset for its own purposes, and neither reader affects the other
// -- no competing consumption, no "who got the message".
//
// Adding a projection later is the point. A fraud model, a tax report, a
// daily-balance chart: each is a new reader from offset zero, added without
// touching the writer, with full history from the day it starts.
public sealed class StatementProjection : IDisposable
{
    private readonly ConcurrentDictionary<string, List<EntryPosted>> _statements = new();
    private IStreamConsumer? _reader;

    private StatementProjection() { }

    /// <param name="mq">The connection.</param>
    /// <param name="fromFirst">Whether to read all of history, or only what arrives from now.</param>
    public static async Task<StatementProjection> StartAsync(AceMqConnection mq, bool fromFirst)
    {
        var projection = new StatementProjection();
        var stream = mq.Stream<EntryPosted>(Ledger.Journal);
        projection._reader = await (fromFirst ? stream.FromFirst() : stream.FromNext()).ConsumeAsync(message =>
        {
            var entry = message.Payload;
            var statement = projection._statements.GetOrAdd(entry.Account, _ => new List<EntryPosted>());
            lock (statement) statement.Add(entry);
            return Task.CompletedTask;
        });
        return projection;
    }

    /// <summary>The entries seen for an account, oldest first.</summary>
    public IReadOnlyList<EntryPosted> StatementOf(string account)
    {
        if (!_statements.TryGetValue(account, out var statement)) return Array.Empty<EntryPosted>();
        lock (statement) return statement.ToArray();
    }

    /// <summary>The sum of the entries, which is what a balance is.</summary>
    public long BalanceOf(string account) => StatementOf(account).Sum(entry => entry.AmountMinor);

    /// <summary>Every account this projection has seen an entry for.</summary>
    public IReadOnlyList<string> Accounts => _statements.Keys.ToArray();

    /// <summary>Entries read.</summary>
    public int Entries => Accounts.Sum(account => StatementOf(account).Count);

    public void Dispose() => _reader?.Dispose();
}
