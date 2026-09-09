# AceMQ for .NET — examples

[![ci](https://github.com/AceMQ-Company/acemq-dotnet-amqp-examples/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-dotnet-amqp-examples/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-dotnet-amqp-examples/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-dotnet-amqp-examples/actions/workflows/attribution-guard.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4)](#requirements)
[![languages](https://img.shields.io/badge/languages-C%23%20%26%20VB.NET-512BD4)](#both-languages)

Runnable examples for [AceMQ for .NET](https://github.com/AceMQ-Company/acemq-dotnet-amqp).
Each one is a single `Program` file: open a directory and the whole example is
in front of you, with no shared helpers to trace.

They resolve the **released** packages from <https://acemq.org/nuget/>, so they
use exactly what the documentation tells you to depend on — and an example that
stops compiling against a release is a red build here rather than a surprise for
whoever copies it.

## Both languages

Every example exists in **C# and VB.NET**. The library is one assembly and both
languages call it the same way, which is the reason a VB team should not have to
translate C# to find out whether it suits them.

CI builds and runs both, because "should work" and "does work" are different
claims and only one of them belongs in a README.

The VB projects turn `Option Strict On`. VB.NET defaults it off, which lets a
mistyped call compile and fail at run time — not something an example should
teach.

## Running one

```bash
docker compose up -d
dotnet run --project basic/01-publish-and-consume-csharp
dotnet run --project basic/01-publish-and-consume-vbnet
```

Point them somewhere else with `ACEMQ_URL`:

```bash
ACEMQ_URL=amqps://guest:guest@broker:5671/ dotnet run --project basic/01-publish-and-consume-csharp
```

## What is here

### basic

| | C# | VB.NET |
|---|---|---|
| A durable queue, a confirmed publish, and a consumer that says what it did | [01](basic/01-publish-and-consume-csharp) | [01](basic/01-publish-and-consume-vbnet) |
| The attempt counter moving, and a message giving up | [02](basic/02-retries-and-dead-letters-csharp) | [02](basic/02-retries-and-dead-letters-vbnet) |
| One logical message delivered four times and charged once | [03](basic/03-idempotent-consumer-csharp) | [03](basic/03-idempotent-consumer-vbnet) |
| Dead-lettered messages put back, one tenant at a time | [04](basic/04-replay-csharp) | [04](basic/04-replay-vbnet) |
| A message written in the same transaction as the work, and a relay publishing it after | [05](basic/05-transactional-outbox-csharp) | [05](basic/05-transactional-outbox-vbnet) |
| JSON and XML read off one queue, which is what a format migration looks like | [06](basic/06-serialization-csharp) | [06](basic/06-serialization-vbnet) |
| A log four readers go through from four different places, and nothing is consumed | [07](basic/07-streams-csharp) | [07](basic/07-streams-vbnet) |

### intermediate

| | C# | VB.NET |
|---|---|---|
| A tenant stamped on every message and every handler timed, without either appearing in the handler | [01](intermediate/01-interceptors-csharp) | [01](intermediate/01-interceptors-vbnet) |
| A topology described once, dry-run so it can be read, then applied | [02](intermediate/02-topology-as-data-csharp) | [02](intermediate/02-topology-as-data-vbnet) |
| A question asked over a queue, the answer matched to it, and a question nobody answers | [03](intermediate/03-request-reply-csharp) | [03](intermediate/03-request-reply-vbnet) |
| Three steps undone in reverse, and one that could not be undone at all | [04](intermediate/04-saga-csharp) | [04](intermediate/04-saga-vbnet) |
| Messages delivered later, and how close to "later" they actually land | [05](intermediate/05-scheduling-csharp) | [05](intermediate/05-scheduling-vbnet) |
| Eight consumers on one queue, and the setting that makes them behave like one | [06](intermediate/06-consumer-groups-csharp) | [06](intermediate/06-consumer-groups-vbnet) |
| A field added to a schema, read by a consumer that has not heard of it | [07](intermediate/07-schema-evolution-csharp) | [07](intermediate/07-schema-evolution-vbnet) |
| A message stuck three steps in, put back at step three rather than at step one | [08](intermediate/08-pipelines-csharp) | [08](intermediate/08-pipelines-vbnet) |

### advanced

| | C# | VB.NET |
|---|---|---|
| Message bodies the broker cannot read, and a keyring that can rotate | [01](advanced/01-encrypting-payloads-csharp) | [01](advanced/01-encrypting-payloads-vbnet) |
| A payload too large for a broker kept off it, and the boundary where that starts | [02](advanced/02-claim-check-csharp) | [02](advanced/02-claim-check-vbnet) |

From `intermediate/03` onwards each directory carries its own `README.md`, in
both languages, explaining what the example proves and what it costs — and so
does `basic/07`, because a stream has more ways to go quietly wrong than the rest
of `basic` put together.

More are being added. The [Java examples](https://github.com/AceMQ-Company/acemq-java-amqp-examples)
are further along and cover the same library, so the shape of anything missing
here can be read there in the meantime.

## Four things these examples exist to show

**The transport has to be registered.**

```csharp
Transports.Register(new RabbitMqTransport());
```

Referencing the package is not enough: nothing in the program touches that
assembly otherwise, so the runtime never loads it and a broker URL cannot be
resolved. The error says exactly this, but an example that does it is worth more
than an error that explains it.

**`message.Attempt`, not `message.Envelope.Attempt`.**

The envelope carries what the publisher wrote, and a broker redelivers the
original bytes — so that header reads 1 for ever, however many times the message
has come back. The count the consumer keeps is the one that moves. Example 02
prints `[1, 2, 3]`; reading the envelope would print `[1, 1, 1]` and a retry
limit built on it would never trip.

**A dead letter goes to `{queue}.dlq`, not to the queue's `x-dead-letter-exchange`.**

When a handler rejects a message, or a retry policy runs out of attempts, the
library republishes the message to `{queue}.dlq` with the reason on the envelope
and then acknowledges the original — rather than nacking it and leaving the
route to the broker. That is what carries the reason and the attempt count
across; a nack carries neither. A consumer declares `{queue}.dlq` and
`{queue}.parked` when it starts, so nothing in an example has to.

Setting `x-dead-letter-exchange` on the source queue is still worth doing, but it
is the backstop underneath — for a TTL expiry, an `x-max-length` drop, or a
rejection from something that is not this library. Examples 02 and 04 read
`{queue}.dlq`, and they read the wrong queue until 0.5.0 was picked up here.

**VB.NET is case-insensitive.** A variable named `keyring` collides with the
`Keyring` type, and the compiler reports it as a type it cannot infer rather
than as a name clash. The encryption example calls it `ring`, the topology
example calls its variable `wanted`, request/reply calls a `Requester` `asking`
and an `Envelope` `stamp`, scheduling calls a `Scheduler` `later`, the claim
check calls its codec `framing`, consumer groups call a batch `eightAtOnce`
rather than `parallel`, and the pipeline example calls a `Pipeline(Of T)` `chain`
— all for the same reason. The sort of thing that costs twenty minutes if nobody
has written it down.

The same rule turns namespaces into collisions. `Imports AceMq.Amqp` brings
`AceMq.Amqp.Avro` into scope as `Avro`, so `Avro.Schema` in the schema-evolution
example is ambiguous with Apache's `Avro` namespace and has to be imported and
named unqualified instead. C# has the same problem; VB.NET just removes one more
way out of it.

## Requirements

.NET 8 or later, and Docker. The examples target `net8.0` and roll forward, so
they also run on a machine that only has a newer runtime.

## How these stay honest

CI compiles and **runs every one of them, in both languages, against a real
broker**, on every push and once a week. It fails if it finds fewer than two
examples in either language, since a `find` that matched nothing would otherwise
pass having run nothing at all.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
