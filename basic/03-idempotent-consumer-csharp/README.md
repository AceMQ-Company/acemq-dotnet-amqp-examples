# basic/03 — one charge delivered four times and taken once, in C#

One logical charge published three times under the same message id, its first
delivery failed on purpose so the broker sends it again, and a consumer with an
idempotency store in front of the handler that lets exactly one of them through
to the card — because retries and at-least-once producers mean a message can
arrive more than once, and a handler that changes anything has to be able to
tell.

The same example exists in VB.NET at
[basic/03-idempotent-consumer-vbnet](../03-idempotent-consumer-vbnet).

## What it shows

- **The message id is the idempotency key.** The publisher sets it with
  `Envelope.Of("charge").Id("charge-c-1")`, and all three publishes carry it.
- **A duplicate never reaches the handler.** `.Idempotent(store)` claims the id
  before the handler runs and confirms it after, so the second and third copies
  are settled without the handler seeing them.
- **A failure releases the claim.** The first delivery fails, and its id is
  released rather than confirmed — which is what lets the retry actually run.
  Remembering a message that then failed would mean the retry silently did
  nothing.

## Running it

```bash
docker compose up -d
dotnet run --project basic/03-idempotent-consumer-csharp
```

## What to look for

```
delivery 1: failing on purpose
charging c-1 for 4250 cents
delivered 2 times, charged 1 time(s)
```

**`delivered 2 times`** counts the times the handler ran. The broker delivered
four — three publishes and one retry — and the store kept two of them out.

**`charged 1 time(s)`** is the line the example exists for. Without
`.Idempotent(store)` every copy that gets past the failed first delivery reaches
the handler, and every one of them is charged.

## Which store

```csharp
var store = new InMemoryIdempotencyStore(TimeSpan.FromHours(1));
```

In this process only, which is right for one worker and wrong the moment there
are two: each has its own memory and both believe they were first. A store the
workers share — ideally the same database as the work, in the same transaction —
is what makes this hold across a fleet.

The window has to outlast the longest a message can still be retried. An id
forgotten before its last redelivery is an id charged twice.

## How this stays honest

The program exits 1 unless the charge was taken exactly once, so a store that
let a duplicate through, or a failed delivery that was remembered and never
retried, is a red build. CI runs it against a real broker on every push.

## In the documentation

- [Reliability — duplicates](https://acemq-company.github.io/acemq-dotnet-amqp/reliability.html#duplicates)
- [Reliability — a claim is a lease, not a fact](https://acemq-company.github.io/acemq-dotnet-amqp/reliability.html#a-claim-is-a-lease-not-a-fact)
- [Reliability — what the in-memory store is and is not](https://acemq-company.github.io/acemq-dotnet-amqp/reliability.html#what-the-in-memory-store-is-and-is-not)
