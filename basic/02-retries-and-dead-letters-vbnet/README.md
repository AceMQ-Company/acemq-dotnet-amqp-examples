# basic/02 — a message retried three times and then given up on, in VB.NET

One payment whose handler always fails, retried on a fixed policy until the
policy runs out, and then moved to a dead-letter queue with its id intact — the
attempt counter is the thing to watch, because it is the one number a broker on
its own would get wrong.

The same example exists in C# at
[basic/02-retries-and-dead-letters-csharp](../02-retries-and-dead-letters-csharp).

## What it shows

- **The policy decides when to stop, not the handler.** The handler returns
  `Ack.Retry` every time; `RetryPolicy.Fixed(3, ...)` is what turns the third
  failure into a give-up.
- **`message.Attempt` moves; `message.Envelope.Attempt` does not.** A broker
  redelivers the bytes it was given, so the header on the wire says 1 for ever.
  The count the consumer keeps comes from redelivery, which is why the number
  below goes 1, 2, 3.
- **A give-up lands on `{queue}.dlq`.** The library republishes the message there
  and acknowledges the original, rather than nacking it and leaving the route to
  the broker. The consumer declares that queue when it starts.
- **The envelope survives the trip.** The dead-lettered message has the same id
  as the one that was published.

## Running it

```bash
docker compose up -d
dotnet run --project basic/02-retries-and-dead-letters-vbnet
```

## What to look for

```
handling p-1, attempt 1
handling p-1, attempt 2
handling p-1, attempt 3
dead-lettered after attempts [1, 2, 3]
the envelope kept its history: id=0b8a63fe-3c29-4577-a313-c026a7439ce3
```

The C# twin also prints each attempt's age, which steps by the policy's 300 ms
wait; this one keeps to the counter.

**Three attempts, then the dead-letter queue.** The waits between them are even
because the example turns jitter off so the output is readable. Leave it on in
anything real: messages that failed together must not come back together.

## The policy

```vb
Dim options = ConsumerOptions.Defaults() _
    .WithRetry(RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(300)).WithJitter(0))
```

Three attempts in all, not three retries after the first. `Ack.Retry(TimeSpan.Zero,
reason)` from the handler leaves the waiting to the policy; a non-zero delay
there overrides it for that one message.

## The dead-letter queue is not optional reading

A dead-letter queue nobody reads is a place messages go to be forgotten quietly,
which is worse than dropping them: it looks like nothing is wrong.
[basic/04-replay-vbnet](../04-replay-vbnet) is what reading it looks like.

Setting `x-dead-letter-exchange` on the source queue is still worth doing — it is
the backstop for a TTL expiry, an `x-max-length` drop, or a rejection from
something that is not this library — but it is not where a give-up from a retry
policy lands.

## The same queue names as C#

This example declares `payments` and `payments.dlq`, the names its C# twin uses.
Run the two one after the other rather than side by side against one broker, or
each will consume the other's message.

## How this stays honest

The program waits at most sixty seconds for the message to reach `payments.dlq`,
and a wait that runs out throws, so a policy that retried for ever or a give-up
that went nowhere is a non-zero exit. CI runs it against a real broker on every
push.

## In the documentation

- [Reliability — retry policies](https://acemq-company.github.io/acemq-dotnet-amqp/reliability.html#retry-policies)
- [Reliability — why jitter is on by default](https://acemq-company.github.io/acemq-dotnet-amqp/reliability.html#why-jitter-is-on-by-default)
- [Consuming — where a dead letter goes](https://acemq-company.github.io/acemq-dotnet-amqp/consuming.html#where-a-dead-letter-goes)
- [VB.NET — publishing and consuming in VB](https://acemq-company.github.io/acemq-dotnet-amqp/vbnet.html#publishing-and-consuming-in-vb)
