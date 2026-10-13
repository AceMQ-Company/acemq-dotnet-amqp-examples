# basic/01 — one message, published and consumed, in VB.NET

One order published to a durable queue, confirmed by the broker, and read back
by a consumer that has to say what it did with it — the smallest program that
still uses the library the way a service would, written in VB.NET against the
same assembly C# uses.

The same example exists in C# at
[basic/01-publish-and-consume-csharp](../01-publish-and-consume-csharp).

## What it shows

- **The transport is registered, not discovered.** `Transports.Register(New
  RabbitMqTransport())` comes before the first URL is resolved. Referencing the
  package is not enough: nothing else in the program touches that assembly, so
  it is never loaded.
- **A publish is confirmed.** `SendAsync` returns once the broker has taken the
  message, and the result says whether it was routed anywhere.
- **The handler returns its decision.** `Ack.Accept()` is a return value, so a
  handler that forgets to decide does not compile.
- **The envelope travels with the payload.** Id, type, attempt and origin
  arrive beside the order without the handler asking for them.

## Running it

```bash
docker compose up -d
dotnet run --project basic/01-publish-and-consume-vbnet
```

## What to look for

```
published c30a2f49-ffb5-4cfd-a712-a2b6de4d55b6, routed by the broker: True, took 8.6ms
consumed  c30a2f49-ffb5-4cfd-a712-a2b6de4d55b6: order o-1 for 4250 cents
          type="orders" attempt=1 origin=acemq@Kenshi
```

The id and the latency change on every run, and `origin` names the machine it
ran on.

**The same id on both lines** is the envelope doing its job: the consumer sees
the id the publisher assigned, which is what an idempotency store or a trace
later keys on.

**`routed by the broker: True`** is the part a plain AMQP publish does not tell
you. The broker's default for a message that matches no queue is to confirm it
and drop it; here that publish throws a `PublishFailedException` instead, unless
it was sent with `PublishOptions.AllowUnroutable()`, and then `Routed` is
`False`.

## The name on the connection

```vb
Dim config = ConnectionConfig.ForUrl(BrokerUrl()) _
    .ClientName("examples/01-publish-and-consume-vbnet") _
    .Build()
```

`ClientName` is what RabbitMQ's management interface shows against the
connection. Worth setting before you need it, which will be while working out
which process is holding a message.

## What VB.NET makes you say

**`Imports System.Threading` and `System.Threading.Tasks`.** VB.NET's implicit
imports are a shorter list than C#'s, so the example names them rather than
relying on a project setting a reader cannot see.

**`Main` blocks on an async function.** `Return RunAsync().GetAwaiter().GetResult()`
is how a VB console program waits for async work; there is no `Async Main`.

The project turns `Option Strict On`, as every VB project in this repository
does, so a mistyped call fails the build rather than the run.

## How this stays honest

The consumer waits at most thirty seconds for the message, and a wait that runs
out throws, so a publish that never arrives is a non-zero exit rather than a
program that prints one line and stops. CI runs it against a real broker on
every push.

## In the documentation

- [VB.NET — publishing and consuming in VB](https://acemq-company.github.io/acemq-dotnet-amqp/vbnet.html#publishing-and-consuming-in-vb)
- [Getting started — send and receive a message](https://acemq-company.github.io/acemq-dotnet-amqp/getting-started.html#send-and-receive-a-message)
- [Publishing — what "sent" means](https://acemq-company.github.io/acemq-dotnet-amqp/publishing.html#what-sent-means)
- [Consuming — the disposition is a return value](https://acemq-company.github.io/acemq-dotnet-amqp/consuming.html#the-disposition-is-a-return-value-not-an-exception)
