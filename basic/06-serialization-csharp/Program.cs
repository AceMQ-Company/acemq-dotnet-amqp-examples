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

// Choosing what goes on the wire, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/06-serialization-csharp
//
// JSON is the default and is usually right. This is the other case: a queue
// that has to be read while two formats are in flight -- because a publisher
// somewhere still sends XML, or because a migration is half done and turning
// both ends off at the same instant was never an option.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Order
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

        // A codec belongs to a connection: everything published on it is encoded
        // the same way, and everything consumed is read the same way.
        //
        // The consumer here reads both. A composite decodes by the content type
        // on the message rather than by guessing, and encodes with the first
        // codec it was given -- so the order of the arguments is the format this
        // connection publishes.
        using var reader = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl()).ClientName("examples/06-serialization-reader").Build(),
            CompositeCodec.Of(new JsonCodec(), new XmlCodec()),
            token);

        await reader.DeclareQueueAsync("serialization-orders");

        var arrived = new List<string>();
        var both = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await reader.ConsumeAsync<Order>("serialization-orders", message =>
        {
            lock (arrived)
            {
                arrived.Add(message.Payload.OrderId);
                if (arrived.Count == 2) both.TrySetResult(true);
            }
            Console.WriteLine(
                $"  read {message.Payload.OrderId} ({message.Payload.TotalCents}) " +
                $"sent as {message.ContentType}");
            return Task.FromResult(Ack.Accept());
        });

        // One publisher speaking JSON...
        using var json = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl()).ClientName("examples/06-serialization-json").Build());
        await json.Publisher<Order>("", "serialization-orders")
            .SendAsync(new Order { OrderId = "o-json", TotalCents = 1999 });

        // ...and one that has not been migrated yet.
        using var xml = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl()).ClientName("examples/06-serialization-xml").Build(),
            new XmlCodec(),
            token);
        await xml.Publisher<Order>("", "serialization-orders")
            .SendAsync(new Order { OrderId = "o-xml", TotalCents = 4500 });

        await both.Task.WaitAsync(token);
        Console.WriteLine($"both formats read off one queue: {string.Join(", ", arrived)}");

        // What else is available. XML and JSON are in the main package; YAML,
        // TOML, Protobuf and Avro are packages of their own, so a service pays
        // for the formats it uses and nothing else.
        Console.WriteLine($"codecs registered here: {string.Join(", ", CodecRegistry.Names())}");

        // The example cleans up after itself so a second run reports the same
        // numbers as the first. A service would leave the queue alone.
        consumer.Dispose();
        await reader.DeleteQueueAsync("serialization-orders");

        return 0;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
