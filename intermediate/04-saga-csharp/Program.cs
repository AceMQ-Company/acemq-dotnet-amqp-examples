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

// Three services, no shared transaction, and what happens when the third says
// no, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/04-saga-csharp
//
// Reserving stock, taking a payment and booking a courier are three services
// with three databases. There is no transaction across them, so "roll it back"
// is not something a database can be asked to do -- it has to be done by
// running the opposite of each step that succeeded, in reverse.
//
// The saga runs three times: once where everything works, once where the
// courier refuses and the earlier steps are undone, and once where undoing
// itself fails. The third is the one worth reading.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Order
{
    public string OrderId { get; set; } = "";
}

public static class Program
{
    private const string Events = "saga-order-events";
    private const string Ledger = "saga-ledger";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/04-saga-csharp")
                .Build());

        await mq.DeclareExchangeAsync(Events, "topic");
        await mq.DeclareQueueAsync(Ledger);
        await mq.BindAsync(Ledger, Events, "#");

        // Every step and every compensation announces itself, so what the saga
        // did is visible on the broker rather than only in a return value. A
        // saga is not a message pattern -- nothing in Saga<T> publishes
        // anything -- but the steps of a real one almost always do, and the
        // order they arrive in is the point being made.
        var recorded = new List<string>();
        using var ledger = await mq.ConsumeAsync<Order>(Ledger, message =>
        {
            lock (recorded) recorded.Add(message.RoutingKey ?? "?");
            return Task.FromResult(Ack.Accept());
        });

        Func<string, Func<Order, Task>> announces = key => async order =>
            await mq.Publisher<Order>(Events, key).SendAsync(order, Envelope.Of(key).Build());

        Func<string, Func<Order, Task>> refuses = why => _ =>
            throw new InvalidOperationException(why);

        // ---- 1. everything works -----------------------------------------
        var booking = Saga<Order>.Named("place-order")
            .Step("reserve-stock", announces("stock.reserved"))
                .CompensateWith(announces("stock.released"))
            .Step("take-payment", announces("payment.taken"))
                .CompensateWith(announces("payment.refunded"))
            // No compensation, and legitimately so: this is the last step, so
            // nothing after it can fail and ask for it back. A step that cannot
            // be undone -- sending an email, printing a label -- belongs last,
            // after everything that can still go wrong.
            .Step("book-courier", announces("courier.booked"))
            .Build();

        var happy = await booking.RunAsync(new Order { OrderId = "ORD-1" }, token);
        var sawHappy = await Settled(recorded, 3, token);

        Console.WriteLine($"complete={happy.IsComplete} steps=[{string.Join(", ", happy.Completed)}]");
        Console.WriteLine($"  the broker saw: [{string.Join(", ", sawHappy)}]");
        Check(happy.IsComplete, "the saga did not complete");
        Check(string.Join(",", happy.Completed) == "reserve-stock,take-payment,book-courier",
            $"the completed steps were [{string.Join(", ", happy.Completed)}]");
        Check(string.Join(",", sawHappy) == "stock.reserved,payment.taken,courier.booked",
            $"the broker saw [{string.Join(", ", sawHappy)}]");

        // ---- 2. the courier refuses --------------------------------------
        //
        // Compensations run backwards, newest first, because the later steps
        // are the ones built on the earlier ones -- and a compensation often
        // depends on state a later step has not yet altered.
        var refused = Saga<Order>.Named("place-order")
            .Step("reserve-stock", announces("stock.reserved"))
                .CompensateWith(announces("stock.released"))
            .Step("take-payment", announces("payment.taken"))
                .CompensateWith(announces("payment.refunded"))
            .Step("book-courier", refuses("no courier covers that postcode today"))
            .Build();

        var unhappy = await refused.RunAsync(new Order { OrderId = "ORD-2" }, token);
        var sawUnhappy = await Settled(recorded, 4, token);

        Console.WriteLine(
            $"complete={unhappy.IsComplete} compensated={unhappy.Compensated} " +
            $"failedAt={unhappy.FailedAt} because {unhappy.Failure?.Message}");
        Console.WriteLine($"  the broker saw: [{string.Join(", ", sawUnhappy)}]");
        Check(unhappy.Compensated, "a refused saga did not compensate");
        Check(unhappy.FailedAt == "book-courier", $"it failed at {unhappy.FailedAt}");
        Check(!unhappy.HasUnresolved, "something was left unresolved that should not have been");
        Check(
            string.Join(",", sawUnhappy) ==
            "stock.reserved,payment.taken,payment.refunded,stock.released",
            $"the broker saw [{string.Join(", ", sawUnhappy)}]");

        // ---- 3. the undo itself fails ------------------------------------
        //
        // The row a person has to look at. The warehouse has already picked the
        // reservation, so releasing it is not something a message can do. The
        // saga does not stop -- stopping would leave more undone than carrying
        // on -- so the payment is still refunded, and what comes back names the
        // one thing that was not put right.
        //
        // Nothing else in the system knows about it and no retry will resolve
        // it. This list is the thing to alert on.
        var stuck = Saga<Order>.Named("place-order")
            .Step("reserve-stock", announces("stock.reserved"))
                .CompensateWith(refuses("the warehouse will not release a picked reservation"))
            .Step("take-payment", announces("payment.taken"))
                .CompensateWith(announces("payment.refunded"))
            .Step("book-courier", refuses("no courier covers that postcode today"))
            .Build();

        var half = await stuck.RunAsync(new Order { OrderId = "ORD-3" }, token);
        var sawStuck = await Settled(recorded, 3, token);

        Console.WriteLine($"unresolved=[{string.Join(", ", half.Unresolved)}]");
        Console.WriteLine($"  the broker saw: [{string.Join(", ", sawStuck)}]");
        Console.WriteLine($"  {half}");
        Check(half.HasUnresolved, "a failed compensation was not reported as unresolved");
        Check(string.Join(",", half.Unresolved) == "reserve-stock",
            $"the unresolved steps were [{string.Join(", ", half.Unresolved)}]");
        Check(string.Join(",", sawStuck) == "stock.reserved,payment.taken,payment.refunded",
            $"the broker saw [{string.Join(", ", sawStuck)}]");
        Check(!sawStuck.Contains("stock.released"),
            "the stock was released after all, which is not what the compensation did");

        // A saga is not a distributed transaction and does not pretend to be.
        // After take-payment the customer's money really has moved and anybody
        // looking sees that it has; the refund is a new fact rather than an
        // erasure of the old one. For a moment the world contained a charge
        // that should not have happened. That is what compensating a real
        // action means.
        //
        // It is also not durable: this runs in one process with its state on
        // the stack, so a crash midway leaves the saga half-applied with
        // nothing to resume it.

        // The example cleans up after itself so a second run reports the same
        // numbers as the first. A service would leave the topology alone.
        ledger.Dispose();
        await mq.DeleteQueueAsync(Ledger);
        await mq.DeleteQueueAsync(Ledger + ".dlq");
        await mq.DeleteQueueAsync(Ledger + ".parked");
        await mq.DeleteExchangeAsync(Events);

        return 0;
    }

    // Waits for the broker to have delivered everything the saga published,
    // then takes the list and empties it for the next run.
    private static async Task<List<string>> Settled(
        List<string> recorded, int expected, CancellationToken token)
    {
        for (var i = 0; i < 200; i++)
        {
            lock (recorded)
            {
                if (recorded.Count >= expected) break;
            }
            await Task.Delay(50, token);
        }

        lock (recorded)
        {
            // A short pause even once the count is reached, so an extra message
            // nobody expected is caught here rather than turning up inside the
            // next run's list and making that one fail instead.
            var seen = new List<string>(recorded);
            recorded.Clear();
            Check(seen.Count == expected,
                $"expected {expected} message(s), saw [{string.Join(", ", seen)}]");
            return seen;
        }
    }

    // An example that prints the right answer whatever happened is an example
    // that cannot fail, and CI running it proves nothing. This throws, which
    // makes the process exit non-zero.
    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
