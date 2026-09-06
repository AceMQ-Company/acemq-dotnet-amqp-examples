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

// A topology described once, planned, then applied — in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/02-topology-as-data-csharp
//
// Declaring a queue at a time works until somebody needs to know what a service
// will do to a broker before it does it.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
}

public static class Program
{
    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/02-topology-as-data-csharp")
                .Build());

        var topology = Topology.Define()
            .Exchange("orders-events", "topic")
            .QueueWithDeadLetter("shipping-orders")
            .Bind("shipping-orders", "orders-events", "order.placed")
            .Bind("shipping-orders", "orders-events", "order.cancelled")
            .Build();

        // DryRun asks the broker what is already there and changes nothing, so
        // a deployment can be read before it happens.
        var plan = await mq.ApplyAsync(topology, ApplyMode.DryRun);
        Console.WriteLine("what applying this would do:");
        foreach (var action in plan.Actions)
        {
            Console.WriteLine($"  {action}");
        }

        var applied = await mq.ApplyAsync(topology);
        Console.WriteLine($"applied: {applied.Actions.Count} action(s)");

        // Applying an unchanged topology again is not an error.
        await mq.ApplyAsync(topology);
        Console.WriteLine("applied again, unchanged, without complaint");

        // And it routes, which is the only proof that matters.
        var arrived = new TaskCompletionSource<OrderPlaced>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await mq.ConsumeAsync<OrderPlaced>("shipping-orders", message =>
        {
            arrived.TrySetResult(message.Payload);
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<OrderPlaced>("orders-events", "order.placed")
            .SendAsync(new OrderPlaced { OrderId = "o-1" });

        var order = await arrived.Task.WaitAsync(cancellation.Token);
        Console.WriteLine($"routed end to end: {order.OrderId}");

        return 0;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
