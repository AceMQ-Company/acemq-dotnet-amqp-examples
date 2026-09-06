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

// The transactional outbox, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/05-transactional-outbox-csharp
//
// The problem it solves is the one nobody notices until it happens: a service
// writes a row and publishes a message, and the process dies between the two.
// Either the row exists and nothing was announced, or the announcement went out
// about something that was rolled back. No ordering of the two calls fixes it,
// because they are two systems.
//
// The outbox makes it one system. The message is written in the same
// transaction as the row, and a relay publishes it afterwards -- so the message
// exists if and only if the row does.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
    public long TotalCents { get; set; }
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
                .ClientName("examples/05-transactional-outbox-csharp")
                .Build());

        await mq.DeclareQueueAsync("outbox-orders");

        var arrived = new List<string>();
        var both = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await mq.ConsumeAsync<OrderPlaced>("outbox-orders", message =>
        {
            lock (arrived)
            {
                arrived.Add(message.Payload.OrderId);
                if (arrived.Count == 2) both.TrySetResult(true);
            }
            Console.WriteLine($"  consumed {message.Payload.OrderId}");
            return Task.FromResult(Ack.Accept());
        });

        // In a real service this is DbOutboxStore, over the same database
        // connection -- and the same transaction -- as the business tables. In
        // memory it shares the process's lifetime, so it is not an outbox at
        // all: it is lost on exactly the restart the pattern exists to survive.
        // It is here to show the shape and to exercise the relay.
        var outbox = new InMemoryOutboxStore();

        // ---- the transaction ---------------------------------------------
        //
        // Pretend a BEGIN here, the order row written, these records added, and
        // a COMMIT. Nothing has been published and nothing needs to be: the
        // records are as durable as the row they were written beside.
        //
        // For encodes with the connection's codec, which is the codec the
        // consumer will read with. The alternative is writing the encoded
        // payload out by hand, and hand-written JSON inside a transaction is a
        // typo waiting to become a message nothing can decode.
        await outbox.AddAsync(OutboxRecord.For(
            mq, "", "outbox-orders", new OrderPlaced { OrderId = "o-1", TotalCents = 1999 }));
        await outbox.AddAsync(OutboxRecord.For(
            mq, "", "outbox-orders", new OrderPlaced { OrderId = "o-2", TotalCents = 4500 }));

        Console.WriteLine($"{await outbox.PendingCountAsync()} record(s) committed, nothing published yet");

        // ---- the relay ----------------------------------------------------
        //
        // A background job that publishes what was written down. Start() polls;
        // DrainOnceAsync is that same work done once, which is what makes the
        // pattern testable without waiting on a timer.
        using var relay = new OutboxRelay(mq, outbox);
        var moved = await relay.DrainOnceAsync();
        Console.WriteLine($"the relay published {moved} record(s)");

        await both.Task.WaitAsync(token);
        Console.WriteLine($"{await outbox.PendingCountAsync()} record(s) left in the outbox");

        // Draining again publishes nothing: a record is marked published once the
        // broker has confirmed it.
        //
        // Marked *after*, deliberately. A relay that died in between would
        // publish the message twice, which is why this is at-least-once and why
        // consumers of anything sent this way have to tolerate duplicates -- the
        // envelope's id is the idempotency key for that.
        Console.WriteLine($"a second drain moved {await relay.DrainOnceAsync()} record(s)");

        // The example cleans up after itself so a second run reports the same
        // numbers as the first. A service would leave the queue alone.
        consumer.Dispose();
        await mq.DeleteQueueAsync("outbox-orders");

        return 0;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
