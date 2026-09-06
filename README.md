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

## Two things these examples exist to show

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
