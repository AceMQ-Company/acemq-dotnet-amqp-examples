# apps/03 — event-sourced ledger, in C#

The log **is** the system of record. Balances are not stored; they are what you
get by adding up the log, and can be deleted and rebuilt at any time.

[apps/01](../01-order-fulfilment-csharp) and [apps/02](../02-policy-administration-csharp)
publish events describing what happened to a system of record that lives in a
database. Here there is no such database. Every entry is appended to a RabbitMQ
**stream** and nothing is ever updated or deleted — money moved wrongly is
corrected by posting the opposite entry, exactly as a paper ledger does.

It is a port of the Java examples'
[apps/03-ledger](https://github.com/AceMQ-Company/acemq-java-amqp-examples/tree/main/apps/03-ledger):
the same modules, the same events, the same five scenarios and the same checks,
plus one. The same app exists in VB.NET at [apps/03-ledger-vbnet](../03-ledger-vbnet).

## Why a stream and not a queue

**A queue is emptied by being read. A stream is not.** That single difference is
the reason this application uses one:

- the writer reads the whole journal at start-up to recompute balances;
- a statement projection reads the same journal, from the same offset, at the
  same time, and neither reader affects the other;
- a projection written next year starts at offset zero and gets all of history.

On a queue exactly one of those readers would get each entry — correct for a
*command*, which must be applied once, and wrong for a *fact*. So commands go to
an ordinary queue and entries to a stream; getting that backwards is the most
common mistake in event-sourced systems.

| Module | File | |
|---|---|---|
| **ledger** | [LedgerModule.cs](LedgerModule.cs) | The only writer. Decides whether a transfer is allowed and appends the entries. Its balances are rebuilt from the journal every time it starts |
| **projections** | [Projections.cs](Projections.cs) | A statement per account, built by reading from offset zero. Stores nothing the log does not contain |
| **transfers** | [Transfers.cs](Transfers.cs) | Where transfers are asked for, and refusals noticed |

[Contracts.cs](Contracts.cs) is the events and the topology, and
[Program.cs](Program.cs) is the Java app's system test.

## One writer, deliberately

Every transfer produces two entries that sum to zero. That invariant cannot be
enforced by two processes appending independently — a stream will happily accept
an unbalanced pair from each. Making the writer singular is what makes the
invariant checkable at all, and what makes the writer's own balance tracking
correct: after rebuilding from the journal it maintains its own totals, which is
safe *because* nothing else writes.

**Read to the end, then stop reading.** The Java app's first version of
`Balances` kept following the stream *and* applied each entry as the writer wrote
it, so every entry was counted twice. Keeping only the stream has the opposite
problem: a transfer decided against a balance that does not yet include the one
before it.

## Amounts are integers

```csharp
public sealed record EntryPosted(..., long AmountMinor, ...);
```

Whole minor units, signed. A ledger in `double` disagrees with itself after
enough additions, and a debit/credit flag makes every reader answer "which sign
means debit" for themselves.

## Running it

```bash
docker compose up -d
dotnet run --project apps/03-ledger-csharp
```

RabbitMQ streams need no plugin over AMQP 0-9-1 — `x-queue-type: stream` is core
since 3.9 — so this runs against the same broker as every other example. It
takes about five seconds.

```
held      a transfer posts two entries that sum to zero
held      a transfer that would overdraw is refused, and the refusal is recorded
held      a projection built from offset zero agrees with the writer
held      a projection added later still gets all of history
held      two readers of the same stream do not compete for entries
held      the journal holds every entry once, and a restarted writer agrees with it
all six held
```

Each scenario starts a fresh ledger, and — as in the Java test, where every test
shares one container — all of them share one journal. So every ledger after the
first **rebuilds its balances from entries an earlier one wrote**, which is the
claim event sourcing makes, exercised five times.

**The sixth is not in the Java test.** The other five check the accounts they
touched; this one checks the log itself. It holds exactly as many entries as the
five ledgers posted, no entry appears twice, every transfer's two entries sum to
zero, and a sixth freshly started writer — which rebuilt every balance from that
log — agrees with a projection of it, account by account. Every scenario also
checks that nothing landed in a `.dlq` or `.parked` queue beside the commands,
the rejections or the journal.

## What porting it found in the library

**A stream reader whose handler failed wrote the entry to the stream again.** In
AceMq.Amqp 0.7.9 a failure in a `Stream<T>().ConsumeAsync` handler was answered
with a retry, and a retry republishes to the queue the message came from — which,
on a stream, appends a second copy of the entry to the end of the log. Every
other reader then reads it twice, and so does every rebuild from offset zero.

The sixth check is where that shows. Make one projection throw once on one of
heidi's entries and run this app against 0.7.9:

```
held      a projection added later still gets all of history
...
DID NOT   the journal holds every entry once, and a restarted writer agrees with it: the journal holds 16 entries; 15 were posted
```

The scenario that threw still *held* — the copy arrived at the end of the stream
five seconds later, and the late projection waited long enough to see it. Only
the check of the whole log noticed that the system of record now said heidi was
paid twice. Java's reader stops at the failing entry and moves nothing; the .NET
reader does the same since the release after 0.7.9, and until then a projection
handler that can throw is a ledger that can double-post.

## Where it differs from the Java app, and why

**Broker names carry a prefix.** The journal is `dotnet-csharp.ledger.journal`,
the exchange `dotnet-csharp.ledger`, and the queues
`dotnet-csharp.ledger.commands` and `dotnet-csharp.ledger.rejections` — the Java
app hardcodes the last one, and it is prefixed here like the rest. Routing keys
(`ledger.transfer.requested`, `ledger.transfer.rejected`) and envelope types are
the Java app's exactly. The contracts are identical by construction; a Java
writer and a .NET projection have not been run against one journal here, so this
README does not claim that they interoperate.

**The writer holds a lock.** "One writer" is a claim about processes; a
`SemaphoreSlim` makes it true inside this one too, so an opening balance and a
transfer never interleave.

**A rebuild that read a failing entry refuses to finish.** Balances built from
part of the log are wrong in a way nothing downstream can see, so the rebuild
checks its reader recorded no failures.

**Counted after appending, not before**, so a publish that failed is not a
posting.

## What is honestly not here

The same three things the Java app leaves out, for the same reasons:
**snapshots** (a rebuild is O(history)), **atomic double entry** (the two halves
of a transfer are two appends, so a crash between them leaves the journal
unbalanced), and **retention** beyond an hour — and if retention is shorter than
"forever", the projection is the system of record after all.

## Related

- [basic/07](../../basic/07-streams-csharp) — offsets and replay, one idea at a time
- [apps/02](../02-policy-administration-csharp) — events about a database, rather than instead of one
