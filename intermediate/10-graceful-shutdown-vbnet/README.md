# intermediate/10 — what a shutdown does to the message being handled, in VB.NET

Three shutdowns of the same consumer, each arriving while a handler is half way
through a one-second job: `CloseAsync`, `Dispose`, and a drain given less time
than the job needs — each with a backlog of orders queued behind the one in
hand.

The same example exists in C# at
[intermediate/10-graceful-shutdown-csharp](../10-graceful-shutdown-csharp), where
the first of the three is spelled `await using`.

## What it shows

- **`CloseAsync` drains.** Consuming is paused, the handler already running is
  given up to twenty seconds to finish, and its message is acknowledged before
  the connection closes.
- **`Dispose` does not drain, but it does wait — five seconds at most.** The job
  in hand finishes inside that and its acknowledgement reaches the broker. A
  handler still running after five seconds is abandoned: the connection closes,
  its message comes back, and whatever it answers later is dropped.
- **A drain tells you whether it finished.** `DrainConsumersAsync` returns
  `False` when the budget runs out, which is the number worth logging.
- **A backlog does not slow any of them down, and is not lost.** Every close
  takes about one job's length, and every order that was not finished is back
  on the queue afterwards, none of them dead-lettered.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/10-graceful-shutdown-vbnet
```

It takes about three seconds, because the handler really does sleep for one.

## What to look for

```
CloseAsync   closed after 1038 ms, handled 1 first, 4 of 5 left, 0 dead-lettered
Dispose      closed after 1003 ms, handled 1 first, 4 of 5 left, 0 dead-lettered
out of time  drained False, 1 still in flight, so the grace period was shorter than the job
             closed after  895 ms, 4 of 5 left, 0 dead-lettered
```

**The three lines end the same way, and since `AceMq.Amqp` 0.7.12 that is
right.** Every close took about a second and every one saw the job finish, so
every one settled its message: 4 of 5 left. `Dispose` does not drain — it starts no
wait for the backlog and gives the job in hand no twenty seconds — but on
RabbitMQ the client closes a connection only after the handlers running on it
are done, so it waits for them, up to five seconds, with the channel still open.
A one-second job finishes inside that and its acknowledgement arrives. (Before
0.7.12 the channel was already closing, the acknowledgement was lost, and all 5
were back.)

The difference shows with a handler that outlives the five seconds — one that
ignores its `CancellationToken`. `Dispose` abandons it: the connection and its
socket close, the broker hands the message back at once, and the handler's late
acknowledgement is dropped rather than sent. The side effect has happened *and*
the message is on the queue for whoever starts next. That is what a deployment's
spike of duplicate work is made of, and why the handler should pass the token on.

## Why VB.NET calls `CloseAsync`

C# writes a draining close as `await using`. VB.NET has neither `await using` nor
the ability to `Await` a `ValueTask`, which is what `IAsyncDisposable.DisposeAsync`
returns. So the library implements `DisposeAsync` explicitly and puts the same
work on a public `CloseAsync()` returning a `Task` — the name Java, Go, Python
and Ruby already use. Its public API is audited for exactly this: a member only
C# can call is a member half the supported languages cannot use.

The shape that goes with it:

```vb
Dim mq = Await AceMqConnection.ConnectAsync(url)
Try
    ' ... the service's life ...
    Await mq.CloseAsync()
Finally
    mq.Dispose()
End Try
```

**`CloseAsync` is at the end of the `Try`, not in the `Finally`**, because VB.NET
cannot `Await` inside a `Finally` or a `Catch` at all — the compiler reports
BC36943. The `Finally` calls `Dispose`, which does nothing after `CloseAsync` and
still closes the connection if anything above it threw.

## The two ways to close are not the same

| | What it does |
|---|---|
| `CloseAsync()` | Pauses consuming, waits up to `AceMqConnection.DefaultDrainTimeout` (20 s) for the handlers already running, then closes. |
| `Dispose()` | Cancels the consumers and closes without draining. Waits up to five seconds for the handlers already running, whose acknowledgements still arrive; a handler running longer is abandoned, the connection closes anyway and the broker redelivers its message. |
| `DrainConsumersAsync(timeout)` | Pauses and waits, and **returns whether everything finished**. Does not close. |

Twenty seconds because the number it has to fit inside is usually Kubernetes'
default `terminationGracePeriodSeconds` of 30 — and it is the same twenty the
Java and Ruby libraries use, so a polyglot estate has one number to reason about.

## The answer is the point

`CloseAsync` discards the drain's result, because a teardown has nobody to report
it to. When you want it, drain first:

```vb
Dim finished = Await mq.DrainConsumersAsync(TimeSpan.FromSeconds(25), stopping)
If Not finished Then
    log.LogWarning("shut down with {Count} still in flight; it will be redelivered", mq.InFlight)
End If
```

`False` means the clock ran out with handlers still running, which says one of
two things: the grace period is shorter than the slowest handler, or a handler is
stuck. Set the budget **slightly under** the grace period, so the drain loses the
race to your own log line rather than to `SIGKILL`.

Cancelling the token abandons the wait, not the work: it throws rather than
answering `False`, so a caller can tell "it was given long enough and did not
finish" from "I changed my mind".

## A backlog closes in about a second

Each queue holds five orders. With prefetch 1, the drain pauses consuming, the
handler finishes and acknowledges, and the broker — prefetch now has room —
sends the next order. That delivery stops at the pause gate. The drain is right
to answer `True`: no handler is running.

Closing then cancels the consumers first, so the broker sends them nothing
more, and hands every delivery held at the gate back **with requeue** — never
acknowledged, never rejected without requeue. The broker redelivers it later,
and nothing reaches the dead-letter queue. `Dispose` does the same.

Every close here is asserted to finish in under five seconds, so a backlog that
slowed shutdown down would fail the example.

## One thing VB.NET makes you name differently

**`DrainedQueue`, not `Drained`.** The drain's answer is a local called
`drained`, and in a case-insensitive language a local hides a module member of
the same name for the whole of its function — including the lines above its
`Dim`, where using the constant becomes error BC32000, "Local variable 'drained'
cannot be referred to before it is declared". The C# twin needs no suffix.

## What redelivery costs

Nothing, if the handler is idempotent. Everything, if it charges a card. A
graceful shutdown reduces duplicates; it does not eliminate them, because a
power cut has no `SIGTERM`. The other half of the story is
[basic/03-idempotent-consumer-vbnet](../../basic/03-idempotent-consumer-vbnet).

## How this stays honest

Every count comes from the broker, read on a second connection after the first
has closed — not from the consumer's own idea of what it did. The example
asserts that every close, backlog and all, took under five seconds; that
`CloseAsync` returned only after the job in hand finished, started no other, and left
four of five on the queue — the finished one gone, the held one back; that
`Dispose` waited for that job too and left four of five, its acknowledgement
delivered; that the 100 ms drain of a one-second job answered `False` with one
handler still in flight, and that the close after it let that handler finish and
settle, leaving four of five as well; and that nothing was dead-lettered.

It does not assert that `Dispose` returns before the job finishes. It does not —
see the first section — and an example that asserted what the documentation
once said rather than what the code does would be the wrong kind of honest. The
example exits non-zero if any assertion is wrong.
