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

// Order fulfilment: five services, one broker, in C#.
//
//   docker compose up -d
//   dotnet run --project apps/01-order-fulfilment-csharp
//
// In production these are five deployments. Here they run in one process
// against one real RabbitMQ, which exercises every queue, every hop and every
// failure path -- and fails if any service stopped agreeing with the contracts.
//
// Four orders go through, each with a freshly started system, the way the Java
// app's system test runs them: one that succeeds, one where the warehouse is
// flaky, one over the payment limit, and one where stock runs out. Each of them
// checks its own claims, and the process exits non-zero if any did not hold.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;
using Fulfilment;
using Microsoft.Data.Sqlite;

public static class Program
{
    // How long the system gets to reach the state a scenario waits for. Every
    // one of them gets there in well under a second; this is the bound on a
    // run that has gone wrong, so CI fails rather than hangs.
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private static readonly List<string> DatabaseFiles = new();

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());
        var url = BrokerUrl();

        var failed = 0;
        try
        {
            failed += await RunAsync(url, "an order travels through every service", AnOrderTravelsThroughEveryService);
            failed += await RunAsync(url, "a flaky warehouse is retried rather than failed", AFlakyWarehouseIsRetried);
            failed += await RunAsync(url, "an order over the limit stops at payments", AnOrderOverTheLimitStopsAtPayments);
            failed += await RunAsync(url, "there is not enough stock, and retrying would not help", ThereIsNotEnoughStock);
        }
        finally
        {
            // An example that leaves its queues behind changes the next run's
            // numbers. A real system would leave them alone.
            await ForgetEarlierRunsAsync(url);
            SqliteConnection.ClearAllPools();
            foreach (var file in DatabaseFiles) File.Delete(file);
        }

        Console.WriteLine(failed == 0 ? "all four held" : $"{failed} of four did not hold");
        return failed == 0 ? 0 : 1;
    }

    // ---- the four orders --------------------------------------------------------

    private static async Task AnOrderTravelsThroughEveryService(TheSystem system)
    {
        var orderId = await system.Gateway.PlaceOrderAsync("ada", "WIDGET", 2, 42.00);

        await WaitFor(() => system.Shipping.Shipped == 1, "the order to ship");

        // One order in at the gateway, and every service downstream acted
        // exactly once.
        Check(system.Payments.Captured == 1, $"payments captured {system.Payments.Captured} times, not once");
        Check(system.Inventory.Reserved == 1, $"inventory reserved {system.Inventory.Reserved} times, not once");
        Check(system.Shipping.Shipped == 1, $"shipping shipped {system.Shipping.Shipped} times, not once");

        // Stock actually moved. Without this the reservation is a log line.
        Check(system.Inventory.StockOf("WIDGET") == 8, $"{system.Inventory.StockOf("WIDGET")} widgets left, not 8");

        // And the customer's view is the whole story, assembled from events
        // published by four services that never spoke to each other. The wait is
        // for the final count rather than an intermediate one: polling for 3 can
        // miss the moment the third arrives and the fourth follows.
        await WaitFor(() => system.Notifications.TimelineOf(orderId).Count >= 4, "four events in the timeline");
        CheckTimeline(system, orderId, "OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped");

        // The outbox is empty, so nothing is waiting to be published.
        var pending = await system.Gateway.PendingInOutboxAsync();
        Check(pending == 0, $"{pending} record(s) still in the outbox");
    }

    private static async Task AFlakyWarehouseIsRetried(TheSystem system)
    {
        system.Inventory.WithFlakyWarehouse(2);

        var orderId = await system.Gateway.PlaceOrderAsync("grace", "WIDGET", 1, 10.00);

        await WaitFor(() => system.Shipping.Shipped == 1, "the order to ship");

        // Two failures, then success. The order was never lost and no human was
        // involved.
        Check(system.Inventory.Retried >= 2, $"inventory saw {system.Inventory.Retried} retries, not at least 2");
        Check(system.Inventory.Reserved == 1, $"inventory reserved {system.Inventory.Reserved} times, not once");

        // The Java app asserts only that OrderShipped is in the timeline, read
        // straight after shipping counted it -- which can race the notification.
        // Waiting for all four and checking the whole sequence is stricter, and
        // is where a retry that duplicated StockReserved would show.
        await WaitFor(() => system.Notifications.TimelineOf(orderId).Count >= 4, "four events in the timeline");
        CheckTimeline(system, orderId, "OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped");
    }

    private static async Task AnOrderOverTheLimitStopsAtPayments(TheSystem system)
    {
        var orderId = await system.Gateway.PlaceOrderAsync("charles", "WIDGET", 1, 5_000.00);

        await WaitFor(() => system.Payments.Declined == 1, "payments to decline");

        // Nothing downstream ran, which is the point of declining before
        // reserving: stock held for an order that cannot be paid for is stock
        // nobody releases.
        Check(system.Inventory.Reserved == 0, $"inventory reserved {system.Inventory.Reserved} times, not never");
        Check(system.Shipping.Shipped == 0, $"shipping shipped {system.Shipping.Shipped} times, not never");
        Check(system.Inventory.StockOf("WIDGET") == 10, $"{system.Inventory.StockOf("WIDGET")} widgets left, not 10");

        await WaitFor(() => system.Notifications.TimelineOf(orderId).Count == 2, "two events in the timeline");
        CheckTimeline(system, orderId, "OrderPlaced", "PaymentDeclined");
    }

    private static async Task ThereIsNotEnoughStock(TheSystem system)
    {
        var orderId = await system.Gateway.PlaceOrderAsync("alan", "WIDGET", 99, 99.00);

        await WaitFor(() => system.Inventory.Rejected == 1, "inventory to reject");

        // The money was taken and the stock was not there. In a real system this
        // is where a refund is triggered. It is deliberately visible rather than
        // swallowed.
        Check(system.Payments.Captured == 1, $"payments captured {system.Payments.Captured} times, not once");
        Check(system.Shipping.Shipped == 0, $"shipping shipped {system.Shipping.Shipped} times, not never");

        await WaitFor(() => system.Notifications.TimelineOf(orderId).Count == 3, "three events in the timeline");
        CheckTimeline(system, orderId, "OrderPlaced", "PaymentCaptured", "StockUnavailable");
    }

    // ---- running one --------------------------------------------------------------

    // Starts the system, runs one order through it, and checks what every order
    // must leave behind whatever else it asserts: nothing dead-lettered, nothing
    // parked, and no order handed to payments twice. A library that lost or
    // duplicated a message on any of these paths shows up here even when the
    // scenario's own numbers happen to come out right.
    private static async Task<int> RunAsync(string url, string name, Func<TheSystem, Task> scenario)
    {
        // Each order starts from an empty broker, so one that left something in
        // a dead-letter queue is not blamed on the next. And a run that died half
        // way leaves nothing to be counted as this one's.
        await ForgetEarlierRunsAsync(url);

        var system = await TheSystem.StartAsync(url, FreshDatabase);
        try
        {
            await scenario(system);
            Check(system.Payments.DuplicatesRefused == 0,
                $"payments refused {system.Payments.DuplicatesRefused} duplicate(s) of an order published once");
            foreach (var queue in Contract.Queues)
            {
                foreach (var setAside in new[] { queue + ".dlq", queue + ".parked" })
                {
                    var count = await system.Inspector.MessageCountAsync(setAside);
                    Check(count == 0, $"{count} message(s) in {setAside}");
                }
            }
            Console.WriteLine($"held      {name}");
            return 0;
        }
        catch (Exception e)
        {
            Console.WriteLine($"DID NOT   {name}: {e.Message}");
            return 1;
        }
        finally
        {
            await system.StopAsync();
        }
    }

    private sealed class TheSystem
    {
        public required AceMqConnection Inspector { get; init; }
        public required GatewayService Gateway { get; init; }
        public required PaymentsService Payments { get; init; }
        public required InventoryService Inventory { get; init; }
        public required ShippingService Shipping { get; init; }
        public required NotificationsService Notifications { get; init; }

        public static async Task<TheSystem> StartAsync(string url, Func<string, ConnectionSupplier> database) =>
            new TheSystem
            {
                // A sixth connection that only looks, for the dead-letter counts.
                Inspector = await AceMqConnection.ConnectAsync(url),

                // A database per service, because services do not share one. The
                // moment two services read the same table, the deployment
                // boundary is a fiction.
                Gateway = await GatewayService.StartAsync(url, database("gateway")),
                Payments = await PaymentsService.StartAsync(url, database("payments")),
                Inventory = (await InventoryService.StartAsync(url)).WithStock("WIDGET", 10),
                Shipping = await ShippingService.StartAsync(url),
                Notifications = await NotificationsService.StartAsync(url),
            };

        // Downstream first, so nothing is still being published into a service
        // that has gone. Each close drains its handlers before the connection
        // goes, so a message in hand is settled rather than redelivered.
        public async Task StopAsync()
        {
            await Notifications.CloseAsync();
            await Shipping.CloseAsync();
            await Inventory.CloseAsync();
            await Payments.CloseAsync();
            await Gateway.CloseAsync();
            await Inspector.CloseAsync();
        }
    }

    /// <summary>A fresh database per service, per order: a SQLite file nothing else opens.</summary>
    /// <remarks>
    /// A file rather than SQLite's shared in-memory mode, which locks per table
    /// and fails at once rather than waiting -- and the relay polls the outbox
    /// while an order is being written into it.
    /// </remarks>
    private static ConnectionSupplier FreshDatabase(string service)
    {
        var file = Path.Combine(Path.GetTempPath(), $"fulfilment-csharp-{service}-{Guid.NewGuid():N}.db");
        DatabaseFiles.Add(file);
        return () => new SqliteConnection($"Data Source={file}");
    }

    private static async Task ForgetEarlierRunsAsync(string url)
    {
        using var mq = await AceMqConnection.ConnectAsync(url);
        foreach (var queue in Contract.Queues)
        {
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
        }
        await mq.DeleteExchangeAsync(Contract.Exchange);
    }

    private static void CheckTimeline(TheSystem system, string orderId, params string[] expected)
    {
        var timeline = system.Notifications.TimelineOf(orderId);
        Check(timeline.SequenceEqual(expected),
            $"the timeline of {orderId} was [{string.Join(", ", timeline)}], not [{string.Join(", ", expected)}]");
        Console.WriteLine($"          {orderId}: {string.Join(" -> ", timeline)}");
    }

    private static async Task WaitFor(Func<bool> done, string what)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (!done())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"waited {Patience.TotalSeconds:F0}s for {what}");
            }
            await Task.Delay(50);
        }
    }

    // An example that prints the right answer whatever happened is an example
    // that cannot fail, and CI running it proves nothing.
    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
