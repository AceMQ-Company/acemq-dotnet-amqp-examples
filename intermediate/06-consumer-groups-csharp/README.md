# intermediate/06 — several consumers over one queue, in C#

Forty jobs handled one at a time, then forty more handled eight at a time, with
the wall clock and the measured concurrency printed for both.

The same example exists in VB.NET at
[intermediate/06-consumer-groups-vbnet](../06-consumer-groups-vbnet).

## What it shows

- **A group is started and stopped as one thing**, and a failed start closes
  whatever had already started.
- **Concurrency and prefetch are different numbers**, and the second is the one
  people get wrong.
- **Scaling did not drop or duplicate anything**: eighty jobs, each handled
  exactly once, across two differently sized groups.
- **Draining before shutdown**, so closing does not abandon a handler mid-flight.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/06-consumer-groups-csharp
```

It takes about three seconds, because the handler really does sleep for 50 ms.

## What to look for

```
1 consumer  40 jobs in 2208 ms, peak concurrency 1
8 consumers 40 jobs in 291 ms, peak concurrency 8

handled     80 of 80, each exactly once: True
group size  1 then 8, and 0 once disposed
drained     True, in flight 0
```

Two thousand milliseconds is forty fifty-millisecond handlers in a row, which is
the number one consumer can produce and no other. Two hundred and ninety is eight
of them overlapping.

## What a group buys over starting consumers by hand

Two things, and neither is speed on its own.

**Lifetime.** Starting workers by hand means remembering to close every one, and
a partial shutdown leaves messages held by a consumer nobody is waiting for.
`ConsumerGroup.StartAsync` closes whatever it had already started if one of them
fails to start, and `Dispose` closes all of them even if one throws — the first
exception is rethrown once the others are shut.

**A number.** The size is an `int`, so it comes from configuration. It is the
setting most often changed after a service is already running, and a group is the
shape that lets it be changed without touching the code that consumes.

## Prefetch is the setting people get wrong

```csharp
var options = ConsumerOptions.Prefetch(1);
```

**Concurrency** is how many handlers run at once. **Prefetch** is how many
messages the broker may hand *one consumer* before it acknowledges any of them.

With the library default the broker may hand twenty messages to the first
consumer that asks. Seven of the eight then sit idle while one of them works
through a private backlog, and a queue with eight consumers behaves like a queue
with one. A group is only as parallel as its prefetch lets it be.

The reverse trade also exists: prefetch 1 is a round trip per message, so it is
the right setting for slow handlers and the wrong one for a handler that takes a
microsecond. Both runs here use 1 so that the only thing changing between them is
the number of consumers.

Raising prefetch is also *not* the same as adding consumers. One consumer with a
high prefetch holds more unacknowledged messages but still runs its handler one
at a time on that consumer's channel. Reach for a group when the handler is slow
enough that one channel is the limit, or when a fair share across processes
matters: the broker round-robins between consumers, so eight here compete evenly
with eight in another instance.

## Concurrency is measured, not counted

The example counts the handlers running at the same moment rather than counting
threads. The group dispatches onto the thread pool, so even the serial run
touches a dozen thread names and a thread count would prove nothing at all.

## Draining is not optional

```csharp
var drained = await mq.DrainConsumersAsync(TimeSpan.FromSeconds(10));
```

Disposing a connection while handlers are mid-flight abandons their work. The
messages were never acknowledged so they come back — but a side effect already
applied has happened twice by the time they do. Draining pauses consuming and
waits for what is running, which turns a rolling deploy into an orderly handover.

## Ordering is what it costs

One consumer on a queue sees messages in order. Eight do not: the broker
round-robins between them, and a slow handler finishes after a fast one that
started later. Where a later message about the same entity must not overtake an
earlier one, keep the group and route by key so each key reaches one consumer —
or make the handler idempotent, which is
[basic/03-idempotent-consumer-csharp](../../basic/03-idempotent-consumer-csharp).

## How this stays honest

The example asserts eighty distinct jobs handled, each exactly once, across both
batches — the claim that matters, since scaling a live group must not drop or
duplicate anything. It asserts peak concurrency of exactly 1 for the group of
one and at least 2 for the group of eight, that the group reports the size it was
started with and zero after `Dispose`, and that draining left nothing in flight.

The timing assertion is `serial >= 2 × parallel` rather than `8 ×`. Eight would
be asserting that the machine running CI is idle; two still fails loudly if the
group quietly regresses to serial. The example exits non-zero if any of them is
wrong.
