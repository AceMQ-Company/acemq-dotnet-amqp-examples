# basic/07 — a log that is read rather than emptied, in C#

Eleven orders written to one stream and twenty-one deliveries handled out of it,
by four readers who each start somewhere different.

The same example exists in VB.NET at
[basic/07-streams-vbnet](../07-streams-vbnet).

## What it shows

- **Retention is the argument that matters.** `DeclareStreamAsync` takes `null`
  for both limits, which is legal and almost always wrong.
- **Reading does not consume.** A third reader attaches after two have been all
  the way through and still sees everything.
- **Every reader holds its own position, and the broker holds none of them.**
  The checkpoint is `LastHandledOffset`, and storing it is the application's job.
- **The default offset is `FromNext()`**, which is right for a live consumer and
  silently wrong for a projection being built for the first time.

## Running it

```bash
docker compose up -d
dotnet run --project basic/07-streams-csharp
```

## What to look for

```
wrote      o-0 .. o-4
read       5, checkpoint offset 4
wrote      o-5 .. o-9 while the reader was down
resumed    [o-5, o-6, o-7, o-8, o-9]
auditor    saw all 10 from the beginning
live       saw [o-10] and none of the history
totals     11 written, 21 handled across four readers
depth      MessageCountAsync says 0
```

**`11 written, 21 handled`** is the whole difference from a queue in one line. On
a queue, eleven messages are eleven deliveries and then the queue is empty.

## Retention, or a full disk

```csharp
await mq.DeclareStreamAsync(Log, TimeSpan.FromHours(1), 20L * 1024 * 1024, 1L * 1024 * 1024);
```

Both limits are nullable and neither has a default, because a default here would
be a guess about your disk. A stream with no limit grows until that disk is full,
and on RabbitMQ a full disk is not a stream's problem: it raises a resource alarm
that blocks **every publisher on the node**.

The fourth argument is the segment size, added in 0.5.0 and opt-in for a reason
of its own: retention happens a whole segment at a time. Nothing is discarded
until an entire segment can be, so a stream bounded at 20 MB with 20 MB segments
keeps rather more than 20 MB. It is left absent unless asked for, which also
keeps a stream declared here redeclarable from Go, Python or Ruby — a mismatched
argument fails a redeclaration rather than being ignored.

## The checkpoint is yours

The broker remembers no reader's position. That is exactly what makes a stream
cheap to put several readers on, and it means resuming is something the
application does:

```csharp
checkpoint = readerOne.LastHandledOffset;      // the offset handled, not the count
// ... later, in another process
mq.Stream<Order>(Log).FromOffset(checkpoint + 1)
```

**One past it**, not at it. That offset was already handled, and starting there
handles it twice. Store the value beside whatever the reader wrote, in the same
transaction if the reader is building something transactional — a checkpoint
saved separately from the work it describes is a checkpoint that can disagree
with it.

## Say where to start

`FromFirst()`, `FromLast()`, `FromNext()`, `FromOffset(n)`, `FromTime(t)`,
`FromLast(TimeSpan)`. A `StreamReader` that is not told reads from `FromNext()`,
so a projection built for the first time by code that forgot to say **comes up
empty and looks perfectly healthy**. That is the failure worth knowing about
here; nothing reports it.

## No plugin needed

Stream queues are part of the broker. The `rabbitmq_stream` plugin serves the
separate binary stream protocol, which this library does not use — everything
here is AMQP 0-9-1 against a stock `rabbitmq:4` image. If that were not true the
example would fail at `DeclareStreamAsync`.

Streams do require a prefetch: the broker refuses a stream consumer without one,
because a stream would otherwise hand over its entire history as fast as the
network allows. The library sets one, and `Prefetch(n)` changes it.

## `MessageCountAsync` reports 0 for a stream

Printed here rather than asserted, because it surprises people. The broker does
not report a stream's length through `queue.declare`, so the count comes back
zero however much is in the stream. Use the management API if the depth is what
you need.

## When not to use one

A stream is the wrong shape for work distribution. Every reader sees every
message, so two workers on a stream both do the job rather than sharing it. That
is a queue, and
[intermediate/06-consumer-groups-csharp](../../intermediate/06-consumer-groups-csharp)
is how a queue is scaled.

## How this stays honest

The example deletes the stream before declaring it, so a second run reads its own
eleven messages rather than this run's as well. It then asserts what each reader
saw, by name and in order: `o-0..o-4` for the first, `o-5..o-9` for the resumed
one, all ten for the auditor, and `o-10` alone for the live reader. It asserts
the checkpoint is offset 4 rather than the count 5, and that twenty-one
deliveries came out of eleven messages. It exits non-zero if any of them is
wrong.
