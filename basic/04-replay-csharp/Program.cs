// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Putting dead-lettered messages back, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/04-replay-csharp
//
// Dead-lettering is only half the story. The messages are still there, and the
// point of keeping them is that they get another run once whatever broke has
// been fixed. This is that second half.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Invoice
{
    public string InvoiceId { get; set; } = "";
    public string Tenant { get; set; } = "";
}

public static class Program
{
    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/04-replay-csharp")
                .Build());

        await mq.DeclareQueueAsync("replay-invoices");

        // Where dead letters actually land. A handler that rejects a message
        // does not nack it and leave the route to the broker: the library
        // republishes it to {queue}.dlq and acknowledges the original, so the
        // reason travels with the message and the attempt count survives. The
        // consumer declares that queue when it starts, so nothing here has to.
        const string deadLetters = "replay-invoices.dlq";

        // ---- something breaks --------------------------------------------
        //
        // A downstream service is down, so every invoice is dead-lettered.
        // Ack.DeadLetter is the handler saying this will not succeed by being
        // tried again; Ack.Retry would spin it round the same broken handler.
        var brokenDeadline = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var rejected = 0;

        var broken = await mq.ConsumeAsync<Invoice>("replay-invoices", message =>
        {
            if (Interlocked.Increment(ref rejected) == 3) brokenDeadline.TrySetResult(true);
            return Task.FromResult(Ack.DeadLetter("the ledger service is down"));
        });

        var publisher = mq.Publisher<Invoice>("", "replay-invoices");
        await publisher.SendAsync(new Invoice { InvoiceId = "inv-1", Tenant = "acme" });
        await publisher.SendAsync(new Invoice { InvoiceId = "inv-2", Tenant = "globex" });
        await publisher.SendAsync(new Invoice { InvoiceId = "inv-3", Tenant = "acme" });

        await brokenDeadline.Task.WaitAsync(token);

        // The broken consumer has to go before anything is replayed. Replaying
        // into a queue somebody is still rejecting from would put the messages
        // straight back where they came from.
        broken.Dispose();

        var replay = mq.Replay(deadLetters).Into("replay-invoices");
        Console.WriteLine($"{await replay.PendingAsync()} message(s) waiting on {replay.From}");

        // ---- the fix is deployed, but only for one tenant -----------------
        //
        // A filter is normally about picking out one tenant or one kind of
        // failure. What it passes over is left where it was rather than
        // discarded, so replaying selectively is not a way to lose messages.
        var handled = new List<string>();
        var acme = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var fixedConsumer = await mq.ConsumeAsync<Invoice>("replay-invoices", message =>
        {
            lock (handled)
            {
                handled.Add(message.Payload.InvoiceId);
                if (handled.Count == 2) acme.TrySetResult(true);
            }
            Console.WriteLine($"  handled {message.Payload.InvoiceId} for {message.Payload.Tenant}");
            return Task.FromResult(Ack.Accept());
        });

        var replayed = await replay.ReplayAsync(
            10, delivery => Tenant(delivery) == "acme");
        Console.WriteLine($"replayed {replayed} message(s) for acme");

        await acme.Task.WaitAsync(token);

        // globex was passed over, and is still on the dead-letter queue rather
        // than gone.
        Console.WriteLine($"{await replay.PendingAsync()} message(s) still waiting for the rest of the fix");

        // The example cleans up after itself so a second run reports the same
        // numbers as the first. A service would leave the queues alone.
        fixedConsumer.Dispose();
        await mq.DeleteQueueAsync("replay-invoices");
        await mq.DeleteQueueAsync(deadLetters);

        return 0;
    }

    // The tenant is on the message body, and a filter sees the delivery rather
    // than a decoded payload -- the messages on a dead-letter queue are not
    // guaranteed to be anything a codec can read, which is why the filter is
    // handed the bytes.
    private static string Tenant(InboundDelivery delivery) =>
        System.Text.Encoding.UTF8.GetString(delivery.Body).Contains("\"acme\"") ? "acme" : "other";

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
