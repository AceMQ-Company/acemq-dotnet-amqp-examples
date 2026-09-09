# intermediate/03 — request and reply, in C#

A question sent over a queue and the answer waited for. Three round trips: one
served by a `Responder`, one served by hand so the reply address can be read off
the message, and one nobody answers.

The same example exists in VB.NET at
[intermediate/03-request-reply-vbnet](../03-request-reply-vbnet).

## What it shows

- **One reply queue per requester, not one per request.** Replies are matched by
  correlation id.
- **The reply address travels twice**, in AMQP's own `reply-to` property *and* in
  the `acemq-reply-to` header — which is what lets a .NET requester talk to a Go,
  Python or Ruby responder. This is the thing 0.5.0 fixed.
- **A request nobody answers throws**, rather than hanging or failing silently.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/03-request-reply-csharp
```

It takes about two seconds, most of which is the deliberate timeout at the end.

## What to look for

```
replies come back on acemq.reply.060f0de67c064b9693b42654a51a6a4b
asked for ACME and got ACME at 1234p
  acemq-reply-to header: acemq.reply.060f0de67c064b9693b42654a51a6a4b
  AMQP reply-to property:  acemq.reply.060f0de67c064b9693b42654a51a6a4b
gave up: no reply to def7300c-… on /quotes-unanswered within 2s
answered 1, timed out 1, unmatched replies 0
```

**The two addresses are the same value, written twice.** Until 0.5.0 they were
not. This library and Java carried the reply address in the native AMQP
property; Go, Python and Ruby carried it in an application header. A .NET
requester and a Go responder therefore could not talk at all — the request
arrived, the responder found no address, and counted it unanswerable. Every
library now writes both and reads the header first, so both halves of the family
answer each other and a current requester is answered identically either way.

The header is `acemq-reply-to` and deliberately **not** `x-acemq-reply-to`. That
prefix is the engine's: `Envelope.FromWire` drops every header carrying it from
the application's view on the way in, so a responder could never read a reply
address written there.

## Why one reply queue

A queue per request costs the broker a declare and a delete on every call, which
is the difference between request/reply being usable at rate and being a
curiosity. So a `Requester` declares one queue — `acemq.reply.{a fresh guid}` —
and matches replies to callers by correlation id.

That queue is declared **classic**, asked for rather than inherited: it holds
answers nobody will read once the process is gone, so there is nothing worth
replicating, and it carries `x-expires` of ten minutes so a process killed
without disposing leaves nothing behind for an afternoon.

`Unmatched` is the counter to know about. A reply that arrives after its caller
has given up is counted and dropped rather than handed to whoever asks next —
handing a late answer to the wrong caller is worse than no answer, and it is
exactly what happens when a shared reply queue is read without matching.

## Answering by hand

The second round trip does what a `Responder` does, in five lines, so there is
nothing hidden:

```csharp
var envelope = Envelope.Of(message.Envelope.Type)
    .CorrelationId(message.Envelope.Id)
    .CausationId(message.Envelope.Id)
    .Build();
await mq.Publisher<Quote>("", message.ReplyTo!).SendAsync(answer, envelope);
```

The default exchange addresses the reply queue by name, and the request's id
becomes the reply's correlation id. That is the whole contract.

## What the counters promise

`Responder.Answered` is incremented **before** the reply is published, so a
caller holding its answer can never read a count that has not caught up. The
example asserts it straight away, with no wait.

That guarantee exists because writing this example broke without it. The counter
used to be incremented after the publish, and asserting it here failed about
half the time — a library bug that had been living in this README as an
explanation of why the example waited.

## How this stays honest

Every claim above is asserted, and the example exits non-zero if one fails —
including the two reply addresses matching the requester's queue name, and the
timeout being counted. CI compiles and runs it against a real broker on every
push.
