# intermediate/03 — request and reply, in VB.NET

A question sent over a queue and the answer waited for. Three round trips: one
served by a `Responder`, one served by hand so the reply address can be read off
the message, and one nobody answers.

The same example exists in C# at
[intermediate/03-request-reply-csharp](../03-request-reply-csharp).

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
dotnet run --project intermediate/03-request-reply-vbnet
```

It takes about two seconds, most of which is the deliberate timeout at the end.

## What to look for

```
replies come back on acemq.reply.f79d46b7210f4e138286002ab2ccddbe
asked for ACME and got ACME at 1234p
  acemq-reply-to header: acemq.reply.f79d46b7210f4e138286002ab2ccddbe
  AMQP reply-to property:  acemq.reply.f79d46b7210f4e138286002ab2ccddbe
gave up: no reply to b6aa2cc3-… on /quotes-unanswered-vb within 2s
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

```vb
Dim stamp = Envelope.Of(message.Envelope.Type) _
    .CorrelationId(message.Envelope.Id) _
    .CausationId(message.Envelope.Id) _
    .Build()
Await mq.Publisher(Of Quote)("", message.ReplyTo).SendAsync(answer, stamp)
```

The default exchange addresses the reply queue by name, and the request's id
becomes the reply's correlation id. That is the whole contract.

## Two things VB.NET makes you name differently

**`asking`, not `requester`.** VB.NET is case-insensitive, so a variable called
`requester` collides with the `Requester` type — and the compiler reports it as a
type it cannot infer rather than as a name clash, which costs twenty minutes if
nobody has written it down.

**`stamp`, not `envelope`**, for the same reason. `Envelope` is a type here.

Reading a header is also a line longer, since `TryGetValue` needs a declared
`Object` rather than an inline `out var`:

```vb
Dim carried As Object = Nothing
message.Headers.TryGetValue(Requester.ReplyToHeader, carried)
```

## Counters lag the answer

`Responder.Answered` is incremented **after** the reply is published, so a caller
holding its answer can still read `0`. The example waits briefly rather than
asserting it straight away; reading it without waiting fails about half the time.

## How this stays honest

Every claim above is asserted, and the example exits non-zero if one fails —
including the two reply addresses matching the requester's queue name, and the
timeout being counted. CI compiles and runs it against a real broker on every
push.
