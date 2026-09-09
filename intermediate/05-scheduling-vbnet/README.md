# intermediate/05 — delivering a message later, in VB.NET

Three reminders: one already overdue, one in three seconds, one in five. No
scheduler process, no cron, and no broker plugin.

The same example exists in C# at
[intermediate/05-scheduling-csharp](../05-scheduling-csharp).

## What it shows

- **A ladder of queues, each with a uniform time to live.** A message hops
  through them until it is due.
- **The accuracy that buys, and what it costs.**
- **The payload is encoded once and carried as bytes**, with its content type in
  a header and put back on the message finally delivered.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/05-scheduling-vbnet
```

It takes about five seconds, because it is waiting for real delays.

## What to look for

```
rungs: acemq.schedule.1h, acemq.schedule.10m, acemq.schedule.1m, acemq.schedule.10s, acemq.schedule.1s
scheduled 3, delivered 3, hops 6

asked for   arrived at
     past       0.1s   R-0
       3s       2.1s   R-1
       5s       4.1s   R-2

content type on arrival: application/json
```

**Read the two columns together.** A three-second delay lands at about two. A
message is delivered as soon as less than one second is left, because another hop
through the smallest rung would cost more than the accuracy it buys.

That is the trade this design makes, and it is stated rather than hidden:
delivery is accurate to about the smallest rung. Something that must fire at
09:00:00.000 wants a scheduler, not a message broker.

**`hops 6`** is the other number. R-0 was due already and took none; the other
two took three each. A one-day delay costs twenty-four hops and a one-minute
delay costs one — long delays are several broker round trips rather than one,
which is the honest cost of not requiring a plugin.

## Why not a per-message time to live

The obvious implementation is to set `expiration` on the message, drop it in a
queue nobody consumes, and let it dead-letter to its destination. It is what most
articles suggest and it is wrong for anything but a single fixed delay, because
**a classic queue expires messages only from its head**.

Put a four-hour message in, then a one-minute message behind it, and the
one-minute message is delivered in four hours. Nothing reports this: the queue
looks healthy, the message is not lost, it is simply late by a factor nobody
predicted. It is the single most common way a home-made scheduler fails, and it
fails in production under mixed load rather than in testing under uniform load.

The ladder avoids it by giving every message in a rung the same delay, so the
head is always the message due soonest.

The alternative is RabbitMQ's delayed-message-exchange plugin, which does this
properly and is a plugin — so it is not available everywhere, and a library that
silently required it would be a library that works on your laptop.

## The names are the contract

`acemq.schedule.{1h,10m,1m,10s,1s}`, `acemq.schedule.due`, and the four headers a
scheduled message carries are shared with the Java, Go, Python and Ruby
libraries. Two services scheduling on one broker declare the same queues, and a
rung declared with a different time to live is a `PRECONDITION_FAILED` for
whichever declares second. Nothing about it is a local decision.

The headers are `x-schedule-*` and deliberately **not** `x-acemq-schedule-*`: the
latter is the engine's reserved namespace, and a header carrying it is refused on
publish here and silently dropped on consume in Java.

## Two things VB.NET makes you name differently

**`later`, not `scheduler`.** VB.NET is case-insensitive, so a variable called
`scheduler` collides with the `Scheduler` type and the compiler reports it as a
type it cannot infer rather than as a name clash.

**`DueQueue`, not `Queue`**, for the same reason: `System.Collections.Generic`
brings `Queue(Of T)` into scope, and a constant called `Queue` collides with it.

There is also a VB.NET syntax trap worth knowing: **a comment cannot go inside a
`With` initialiser.** The line continuation that holds the initialiser together
ends at the apostrophe, and the errors that follow (`')' expected`, `'}'
expected`) point nowhere useful. Put the comment above the `New`.

## Anything in the past is delivered now

`AtAsync` with a moment that has already gone delivers immediately rather than
refusing, which is what makes a schedule read out of a database after an outage
safe to replay.

## Lifetimes

The scheduler has to still be running when the messages come due — a scheduler
that has been disposed is a ladder nobody is watching. This example holds it in a
`Using` that outlives the wait.

Its queues are shared, so the example does **not** delete them on the way out:
they may be holding somebody else's message. It cleans up only the destination
exchange and queue it declared itself.

## How this stays honest

The arrival times are asserted, in bands wide enough that a busy CI runner is not
what fails: R-0 within 1.5s, R-1 between 1s and 4s, R-2 between 3s and 6.5s. So
are the delivered count, the hop count being non-zero, and the content type on
arrival — a reminder that arrived as `application/octet-stream` would be the
right bytes that nothing can decode. The example exits non-zero if any of them is
wrong.
