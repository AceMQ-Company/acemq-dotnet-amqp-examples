# basic/04 — dead letters put back, one tenant at a time, in C#

Three invoices dead-lettered while a downstream service is down, then — once the
fix is deployed for one tenant only — the two that belong to that tenant
replayed from the dead-letter queue into the work queue, and the third left
exactly where it was: dead-lettering is only half the story, and this is the
other half.

The same example exists in VB.NET at
[basic/04-replay-vbnet](../04-replay-vbnet).

## What it shows

- **`Ack.DeadLetter` is a decision, not a failure.** The handler says the message
  will not succeed by being tried again, and it goes to `{queue}.dlq` with the
  reason attached. `Ack.Retry` would have spun it round the same broken handler.
- **`mq.Replay(from).Into(to)` moves messages back.** `PendingAsync` says how many
  are waiting; `ReplayAsync(max, filter)` moves the ones the filter accepts.
- **What a filter passes over stays.** The globex invoice is not discarded, so a
  selective replay is not a way to lose messages.
- **The filter sees bytes, not a payload.** It is handed the `InboundDelivery`,
  because a message on a dead-letter queue is not guaranteed to be anything a
  codec can read.

## Running it

```bash
docker compose up -d
dotnet run --project basic/04-replay-csharp
```

## What to look for

```
3 message(s) waiting on replay-invoices.dlq
  handled inv-1 for acme
  handled inv-3 for acme
replayed 2 message(s) for acme
1 message(s) still waiting for the rest of the fix
```

**`replayed 2`, then `1 still waiting`** is the selective replay in two lines: the
two acme invoices handled by the fixed consumer, and globex's still on the
dead-letter queue for whenever its fix ships.

**The last line reads 1 every time.** It is a count of the queue read straight
after the replay. Before AceMq.Amqp 0.7.14 it could read 0, because the message
the filter passed over was handed back to the queue asynchronously and the count
could beat it there. Since 0.7.14 `ReplayAsync` returns only once every
passed-over message is back on the queue.

## Stop the broken consumer first

```csharp
broken.Dispose();
var replay = mq.Replay(deadLetters).Into("replay-invoices");
```

Replaying into a queue somebody is still rejecting from would put the messages
straight back where they came from. The order in a real deployment is the same:
ship the fix, and only then replay.

## How this stays honest

The program waits at most sixty seconds each for the broken consumer to reject
all three invoices and for the fixed one to handle both acme invoices, and a
wait that runs out throws — so a replay that moved nothing, or the wrong tenant,
is a non-zero exit. It deletes both queues at the end, so a second run starts
from the same place as the first. CI runs it against a real broker on every
push.

## In the documentation

- [Patterns — replay](https://acemq-company.github.io/acemq-dotnet-amqp/patterns.html#replay)
- [Consuming — where a dead letter goes](https://acemq-company.github.io/acemq-dotnet-amqp/consuming.html#where-a-dead-letter-goes)
