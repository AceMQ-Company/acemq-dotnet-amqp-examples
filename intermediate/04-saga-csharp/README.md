# intermediate/04 — a saga, in C#

Reserving stock, taking a payment and booking a courier are three services with
three databases. There is no transaction across them, so "roll it back" is not
something a database can be asked to do — it has to be done by running the
opposite of each step that succeeded, in reverse.

The saga runs three times: once where everything works, once where the courier
refuses and the earlier steps are undone, and once where **undoing itself
fails**. The third is the one worth reading.

The same example exists in VB.NET at [intermediate/04-saga-vbnet](../04-saga-vbnet).

## What it shows

- **Compensations run backwards**, newest first, because the later steps are the
  ones built on the earlier ones.
- **A step with no compensation is legitimate** when nothing after it can fail.
- **A compensation that fails does not stop the rest**, and what comes back names
  what was left in a state nobody intended.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/04-saga-csharp
```

## What to look for

```
complete=True steps=[reserve-stock, take-payment, book-courier]
  the broker saw: [stock.reserved, payment.taken, courier.booked]
complete=False compensated=True failedAt=book-courier because no courier covers that postcode today
  the broker saw: [stock.reserved, payment.taken, payment.refunded, stock.released]
unresolved=[reserve-stock]
  the broker saw: [stock.reserved, payment.taken, payment.refunded]
  SagaResult{place-order failed at book-courier, compensated [reserve-stock, take-payment], UNRESOLVED [reserve-stock]}
```

**Read the second line's broker column right to left.** `payment.refunded` comes
before `stock.released`: the compensations ran newest first. That order is not
cosmetic — a compensation often depends on state a later step has not yet
altered, and running them forwards would undo the foundation before the thing
standing on it.

**The third run is the point of the example.** The warehouse has already picked
the reservation, so releasing it is not something a message can do. The saga does
not stop — stopping would leave *more* undone than carrying on — so the payment
is still refunded, and `Unresolved` names the one thing that was not put right.

`stock.released` never appears in that run's broker column, and the example
asserts that it does not.

## `Unresolved` is the list to alert on

Everything else a saga reports is recoverable by construction. `Unresolved` is
real-world effects that happened, were meant to be undone, and were not. Nothing
else in the system knows about them and no retry will resolve them — a person
has to. It is also reported to `AceMqDiagnostics` under `acemq.saga.unresolved`,
at error level, for the same reason.

## Not a distributed transaction

Nothing here is isolated. After `take-payment` the customer's money really has
moved and anybody looking sees that it has. If `book-courier` then fails, the
refund is a **new fact** rather than an erasure of the old one, and for a few
seconds the world contained a charge that should not have happened.

That is not a defect in `Saga<T>`; it is what compensating a real-world action
means, and a saga is honest about it where a two-phase commit pretends otherwise.

So the steps must be things that can be undone by doing something else. Sending
an email cannot be compensated — the apology is a second email, not an unsend —
and a step that sends one should be **last**, after everything that can still
fail. `book-courier` has no compensation here for exactly that reason.

## Not durable

This runs in one process with its state on the stack. A crash midway leaves the
saga half-applied with nothing to resume it, which is the honest limitation of
the in-process form. Where a saga must survive the process, the steps have to be
messages and the state has to be in a database — a much larger thing, and it is
not this.

## Not a message pattern either

Nothing in `Saga<T>` publishes anything and no header is set, so unlike the
envelope or the scheduler it has no wire contract to hold to; only the behaviour
matches Java's `org.acemq.amqp.patterns.Saga`. The steps in this example publish
because the steps of a real saga almost always do, and it makes the order of
events visible on the broker rather than only in a return value.

## How this stays honest

Every line of output above is asserted — the step lists, the failure point, the
compensation order as the broker saw it, and the unresolved step — and the
example exits non-zero if one is wrong. CI compiles and runs it against a real
broker on every push.
