# basic/01 — one message, published and consumed, in C#

One order published to a durable queue, confirmed by the broker, and read back
by a consumer that has to say what it did with it — the smallest program that
still uses the library the way a service would.

The same example exists in VB.NET at
[basic/01-publish-and-consume-vbnet](../01-publish-and-consume-vbnet).

## What it shows

- **The transport is registered, not discovered.** `Transports.Register(new
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
dotnet run --project basic/01-publish-and-consume-csharp
```

## What to look for

```
published c6a90322-adbb-42f8-9165-b43050712c66, routed by the broker: True, took 12.8ms
consumed  c6a90322-adbb-42f8-9165-b43050712c66: order o-1 for 4250 cents
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

```csharp
ConnectionConfig.ForUrl(BrokerUrl())
    .ClientName("examples/01-publish-and-consume-csharp")
    .Build();
```

`ClientName` is what RabbitMQ's management interface shows against the
connection. Worth setting before you need it, which will be while working out
which process is holding a message.

## The field names are the contract

The codec camel-cases by default, so `OrderId` goes out as `"orderId"` — the same
field a Java, Go, Python or Ruby service in the family reads and writes. Renaming
a property is a change to the wire format, not a refactor.

## How this stays honest

The consumer waits at most thirty seconds for the message, and a wait that runs
out throws, so a publish that never arrives is a non-zero exit rather than a
program that prints one line and stops. CI runs it against a real broker on
every push.

## In the documentation

- [Getting started — send and receive a message](https://acemq-company.github.io/acemq-dotnet-amqp/getting-started.html#send-and-receive-a-message)
- [Publishing — what "sent" means](https://acemq-company.github.io/acemq-dotnet-amqp/publishing.html#what-sent-means)
- [Consuming — the disposition is a return value](https://acemq-company.github.io/acemq-dotnet-amqp/consuming.html#the-disposition-is-a-return-value-not-an-exception)
