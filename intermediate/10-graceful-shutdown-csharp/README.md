# intermediate/10 — what a shutdown does to the message being handled, in C#

Three shutdowns of the same consumer, each arriving while a handler is half way
through a one-second job: `await using`, plain `using`, and a drain given less
time than the job needs — each with a backlog of orders queued behind the one
in hand.

The same example exists in VB.NET at
[intermediate/10-graceful-shutdown-vbnet](../10-graceful-shutdown-vbnet), where
the first of the three is spelled `CloseAsync`.

## What it shows

- **`await using` drains.** Consuming is paused, the handler already running is
  given up to twenty seconds to finish, and its message is acknowledged before
  the connection closes.
- **`using` does not.** The job's acknowledgement never reaches the broker, so
  the message comes back — the work has been done once and will be done again.
- **A drain tells you whether it finished.** `DrainConsumersAsync` returns
  `false` when the budget runs out, which is the number worth logging.
- **A backlog does not slow any of them down, and is not lost.** Every close
  takes about one job's length, and every order that was not finished is back
  on the queue afterwards, none of them dead-lettered.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/10-graceful-shutdown-csharp
```

It takes about three seconds, because the handler really does sleep for one.

## What to look for

```
await using  closed after 1019 ms, handled 1 first, 4 of 5 left, 0 dead-lettered
using        closed after 1004 ms, handled 1 first, 5 of 5 left, 0 dead-lettered
out of time  drained False, 1 still in flight, so the grace period was shorter than the job
             closed after  900 ms, 5 of 5 left, 0 dead-lettered
```

**The first two lines differ in one number, and it is the one that matters.**
Both closes took about a second and both saw the job finish — `Dispose` does not
return at once either: on RabbitMQ the client closes a channel only after the
handlers running on it are done, so it waits for them. But by then it has
closed the channel, so their acknowledgements never reach the broker. Only the
drained close settled the message: 4 of 5 left. After `using`, all 5 are back —
the job's side effect has happened *and* its message is on the queue for
whoever starts next. That is what a deployment's spike of duplicate work is
made of.

## The two ways to close are not the same

| | What it does |
|---|---|
| `await using` / `CloseAsync()` | Pauses consuming, waits up to `AceMqConnection.DefaultDrainTimeout` (20 s) for the handlers already running, then closes. |
| `using` / `Dispose()` | Cancels the consumers and closes without draining. Returns once the handlers already running have returned, but closes the channel first, so their acknowledgements are lost and the broker redelivers those messages. |
| `DrainConsumersAsync(timeout)` | Pauses and waits, and **returns whether everything finished**. Does not close. |

Twenty seconds because the number it has to fit inside is usually Kubernetes'
default `terminationGracePeriodSeconds` of 30 — and it is the same twenty the
Java and Ruby libraries use, so a polyglot estate has one number to reason about.

`await using` binds to `IAsyncDisposable.DisposeAsync`, which the library
implements explicitly and forwards to `CloseAsync()`. The public method returns a
`Task` rather than a `ValueTask` because Visual Basic can await only the first,
and the library's API is audited for exactly that.

## The answer is the point

`await using` discards the drain's result, because a teardown has nobody to
report it to. When you want it, drain first:

```csharp
var drained = await mq.DrainConsumersAsync(TimeSpan.FromSeconds(25), stopping);
if (!drained)
{
    log.LogWarning("shut down with {Count} still in flight; it will be redelivered", mq.InFlight);
}
```

`false` means the clock ran out with handlers still running, which says one of
two things: the grace period is shorter than the slowest handler, or a handler is
stuck. Set the budget **slightly under** the grace period, so the drain loses the
race to your own log line rather than to `SIGKILL`.

Cancelling the token abandons the wait, not the work: it throws rather than
answering false, so a caller can tell "it was given long enough and did not
finish" from "I changed my mind".

## A backlog closes in about a second

Each queue holds five orders. With prefetch 1, the drain pauses consuming, the
handler finishes and acknowledges, and the broker — prefetch now has room —
sends the next order. That delivery stops at the pause gate. The drain is right
to answer `true`: no handler is running.

Closing then cancels the consumers first, so the broker sends them nothing
more, and hands every delivery held at the gate back **with requeue** — never
acknowledged, never rejected without requeue. The broker redelivers it later,
and nothing reaches the dead-letter queue. `Dispose` does the same.

Every close here is asserted to finish in under five seconds, so a backlog that
slowed shutdown down would fail the example.

## What redelivery costs

Nothing, if the handler is idempotent. Everything, if it charges a card. A
graceful shutdown reduces duplicates; it does not eliminate them, because a
power cut has no `SIGTERM`. The other half of the story is
[basic/03-idempotent-consumer-csharp](../../basic/03-idempotent-consumer-csharp).

## How this stays honest

Every count comes from the broker, read on a second connection after the first
has closed — not from the consumer's own idea of what it did. The example
asserts that every close, backlog and all, took under five seconds; that
`await using` returned only after the job in hand finished, started no other, and left
four of five on the queue — the finished one gone, the held one back; that
`using` left all five; that the 100 ms drain of a one-second job answered `false`
with one handler still in flight, and that its message came back too; and that
nothing was dead-lettered.

It does not assert that `using` returns before the job finishes. It does not —
see the first section — and an example that asserted what the documentation
once said rather than what the code does would be the wrong kind of honest. The
example exits non-zero if any assertion is wrong.
