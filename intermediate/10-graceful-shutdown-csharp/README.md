# intermediate/10 — what a shutdown does to the message being handled, in C#

Three shutdowns of the same consumer, each arriving while a handler is half way
through a one-second job: `await using`, plain `using`, and a drain given less
time than the job needs.

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

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/10-graceful-shutdown-csharp
```

It takes about three seconds, because the handler really does sleep for one.

## What to look for

```
await using  closed after 1056 ms, handled 1 first, 0 left on the queue
using        closed after 1008 ms, handled 1 first, 1 left on the queue
out of time  drained False, 1 still in flight, so the grace period was shorter than the job
             1 left on the queue
```

**The first two lines differ in one number, and it is the one that matters.**
Both closes took about a second and both saw the job finish — `Dispose` does not
return at once either, because closing the channel waits for the callback still
running on it. But only the drained close settled the message. After `using`,
the job's side effect has happened *and* its message is back on the queue for
whoever starts next. That is what a deployment's spike of duplicate work is made
of.

## The two ways to close are not the same

| | What it does |
|---|---|
| `await using` / `CloseAsync()` | Pauses consuming, waits up to `AceMqConnection.DefaultDrainTimeout` (20 s) for the handlers already running, then closes. |
| `using` / `Dispose()` | Closes. Whatever a handler was doing is not acknowledged, and the broker redelivers it. |
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

## What it does not show: a backlog makes closing take thirty seconds longer

Each queue here holds **one** order, deliberately. With a second message behind
it, AceMq.Amqp 0.7.5 behaves like this:

```
drain True after 1029 ms, held 1
dispose took 29994 ms
```

The drain pauses consuming, the handler finishes and acknowledges, and the
broker — prefetch now has room — sends the next message. That delivery stops at
the pause gate (`mq.Held` reads 1) and waits there for a resume, for up to thirty
seconds. The drain is right to answer `true`: no handler is running. But closing
the channel then waits for that held delivery's callback, which is still sitting
out its thirty seconds — so `await using` and `CloseAsync()` take about **31
seconds** on any consumer whose queue has a backlog. That is longer than
Kubernetes' default grace period, and the pod is killed before `Dispose` returns.

Nothing is lost — the held message was never acknowledged and is redelivered —
but the shutdown overruns its own budget, and this example would otherwise spend
thirty seconds proving it. It is a library limitation in 0.7.5, not something
the caller can configure around.

## What redelivery costs

Nothing, if the handler is idempotent. Everything, if it charges a card. A
graceful shutdown reduces duplicates; it does not eliminate them, because a
power cut has no `SIGTERM`. The other half of the story is
[basic/03-idempotent-consumer-csharp](../../basic/03-idempotent-consumer-csharp).

## How this stays honest

Every count comes from the broker, read on a second connection after the first
has closed — not from the consumer's own idea of what it did. The example
asserts that `await using` returned only after the job in hand finished and left
nothing on the queue; that `using` left the message on the queue; and that the
100 ms drain of a one-second job answered `false` with one handler still in
flight, and that its message came back too.

It does not assert that `using` returns before the job finishes. It does not —
see the first section — and an example that asserted what the documentation
once said rather than what the code does would be the wrong kind of honest. The
example exits non-zero if any assertion is wrong.
