# basic/05 — a message written with the work and published after it, in VB.NET

Two order messages written into an outbox in the same transaction as the orders
themselves, and a relay that publishes them afterwards and marks them done once
the broker has confirmed — so a process that dies between writing a row and
announcing it can no longer leave the row without the announcement, or the
announcement without the row.

The same example exists in C# at
[basic/05-transactional-outbox-csharp](../05-transactional-outbox-csharp).

## What it shows

- **Writing is not publishing.** `outbox.AddAsync(OutboxRecord.For(...))` puts the
  message beside the business row; nothing reaches the broker until the relay
  runs.
- **The relay is the only publisher.** `OutboxRelay.DrainOnceAsync()` publishes
  what was written and reports how many records it moved. `Start()` does the same
  work on a timer.
- **A record is marked published after the broker confirms it.** A second drain
  moves nothing.
- **The record is encoded with the connection's codec.** `OutboxRecord.For(mq,
  ...)` uses the codec the consumer will read with, rather than JSON written out
  by hand inside a transaction.

## Running it

```bash
docker compose up -d
dotnet run --project basic/05-transactional-outbox-vbnet
```

## What to look for

```
2 record(s) committed, nothing published yet
the relay published 2 record(s)
  consumed o-1
  consumed o-2
0 record(s) left in the outbox
a second drain moved 0 record(s)
```

**`committed, nothing published yet`** is the state the pattern exists to make
safe. With a database store the records are as durable as the row they were
written beside, so a crash here loses nothing: the relay picks them up when the
process comes back.

**`a second drain moved 0`** is the relay not publishing twice in the normal
case. It is not a promise that it never will — see below.

The queue is `outbox-orders-vb`, not the C# twin's `outbox-orders`, so the two
can run against one broker without consuming each other's orders.

## Two systems, made one

A service that writes a row and then publishes a message has two systems and no
transaction across them. Whichever order the two calls go in, a crash between
them leaves one done and the other not. No ordering fixes it.

The outbox makes the message part of the database write. It exists if and only
if the row does, and getting it onto the broker becomes a job that can be
retried until it succeeds.

## At least once, deliberately

The relay marks a record published *after* the broker has confirmed it. A relay
that died between the confirm and the mark would publish the message again, so
this is at-least-once — and a consumer of anything sent this way has to tolerate
duplicates. The envelope's id is the idempotency key for that;
[basic/03-idempotent-consumer-vbnet](../03-idempotent-consumer-vbnet) is the
consumer side.

## Which store

```vb
Dim outbox As New InMemoryOutboxStore()
```

Not an outbox at all, in a real service: it shares the process's lifetime, so it
is lost on exactly the restart the pattern exists to survive. It is here to show
the shape and to exercise the relay. In a service the store is `DbOutboxStore`,
over the same database connection — and the same transaction — as the business
tables.

## How this stays honest

The program waits at most sixty seconds for both orders to be consumed, and a
wait that runs out throws, so a relay that published nothing is a non-zero exit.
It deletes its queue at the end, so a second run reports the same numbers as the
first. CI runs it against a real broker on every push.

## In the documentation

- [Patterns — the outbox](https://acemq-company.github.io/acemq-dotnet-amqp/patterns.html#the-outbox)
- [Patterns — a record the broker will never take](https://acemq-company.github.io/acemq-dotnet-amqp/patterns.html#a-record-the-broker-will-never-take)
- [Reliability — duplicates](https://acemq-company.github.io/acemq-dotnet-amqp/reliability.html#duplicates)
- [VB.NET — publishing and consuming in VB](https://acemq-company.github.io/acemq-dotnet-amqp/vbnet.html#publishing-and-consuming-in-vb)
