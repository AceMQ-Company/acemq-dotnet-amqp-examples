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

// Delivering a message later, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/05-scheduling-csharp
//
// Three reminders: one already overdue, one in three seconds, one in five. No
// scheduler process, no cron, and no broker plugin. It takes about five
// seconds, because it is waiting for real delays.
//
// Read the two columns of the table it prints together. A three-second delay
// lands at about two, and that is the design rather than a defect -- see the
// note further down.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Reminder
{
    public string ReminderId { get; set; } = "";
}

public sealed class Arrival
{
    public string ReminderId { get; set; } = "";
    public TimeSpan After { get; set; }
    public string? ContentType { get; set; }
}

public static class Program
{
    private const string Exchange = "scheduling-reminders";
    private const string Queue = "scheduling-reminders-due";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/05-scheduling-csharp")
                .Build());

        await mq.DeclareExchangeAsync(Exchange, "direct");
        await mq.DeclareQueueAsync(Queue);
        await mq.BindAsync(Queue, Exchange, "reminder.due");

        var arrivals = new List<Arrival>();
        var all = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = DateTimeOffset.UtcNow;

        using var consumer = await mq.ConsumeAsync<Reminder>(Queue, message =>
        {
            lock (arrivals)
            {
                arrivals.Add(new Arrival
                {
                    ReminderId = message.Payload.ReminderId,
                    After = DateTimeOffset.UtcNow - started,
                    // The scheduler republishes bytes rather than objects, so it
                    // carries the content type the payload was encoded as and
                    // puts it back on the message it finally delivers. Without
                    // that, what arrives is the right bytes under
                    // application/octet-stream and nothing can decode it.
                    ContentType = message.ContentType,
                });
                if (arrivals.Count == 3) all.TrySetResult(true);
            }
            return Task.FromResult(Ack.Accept());
        });

        // Starting a scheduler declares its exchange, its five rungs and its
        // control queue. They are shared, so a second scheduler on the same
        // broker declares the same seven objects rather than a set of its own —
        // and every name and argument below is a wire contract shared with the
        // Java, Go, Python and Ruby libraries. A rung declared with a different
        // time to live is a PRECONDITION_FAILED for whichever service starts
        // second.
        using var scheduler = await Scheduler.OnAsync(mq);
        Console.WriteLine($"rungs: {string.Join(", ", Scheduler.Rungs.Select(Scheduler.RungName))}");

        // Already overdue. Anything in the past is delivered immediately rather
        // than refused, which is what makes a schedule read out of a database
        // after an outage safe to replay.
        await scheduler.AtAsync(
            started.AddSeconds(-5), Exchange, "reminder.due", new Reminder { ReminderId = "R-0" });
        await scheduler.InAsync(
            TimeSpan.FromSeconds(3), Exchange, "reminder.due", new Reminder { ReminderId = "R-1" });
        await scheduler.InAsync(
            TimeSpan.FromSeconds(5), Exchange, "reminder.due", new Reminder { ReminderId = "R-2" });

        await all.Task.WaitAsync(token);

        Console.WriteLine(
            $"scheduled {scheduler.Scheduled}, delivered {scheduler.Delivered}, " +
            $"hops {scheduler.Hops}");
        Console.WriteLine();
        Console.WriteLine("asked for   arrived at");

        List<Arrival> seen;
        lock (arrivals) seen = arrivals.OrderBy(a => a.ReminderId).ToList();

        var asked = new Dictionary<string, string> { ["R-0"] = "past", ["R-1"] = "3s", ["R-2"] = "5s" };
        foreach (var arrival in seen)
        {
            Console.WriteLine(
                $"{asked[arrival.ReminderId],9}   {arrival.After.TotalSeconds,7:F1}s   {arrival.ReminderId}");
        }

        Console.WriteLine();
        Console.WriteLine($"content type on arrival: {seen[0].ContentType}");

        Check(seen.Count == 3, $"{seen.Count} reminders arrived, not three");
        Check(scheduler.Delivered == 3, $"the scheduler delivered {scheduler.Delivered}, not three");
        Check(scheduler.Hops > 0, "nothing hopped, so nothing waited in the broker");
        Check(seen.Select(a => a.ReminderId).SequenceEqual(new[] { "R-0", "R-1", "R-2" }),
            "the reminders that arrived were not the three that were scheduled");

        // R-0 was due already, so it takes no hops and arrives about now.
        Between(seen[0], TimeSpan.Zero, TimeSpan.FromSeconds(1.5));

        // The two real delays. The bands are wide on purpose: this is the
        // accuracy the design buys, and asserting to the tenth of a second
        // would be asserting that the machine running CI is not busy.
        Between(seen[1], TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(4));
        Between(seen[2], TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(6.5));

        Check(seen.All(a => a.ContentType == "application/json"),
            $"a reminder arrived as {seen[0].ContentType}, which no consumer would decode");

        // What the table is saying. A three-second delay lands at about two,
        // because a message is delivered as soon as less than one second is
        // left: another hop through the smallest rung would cost more than the
        // accuracy it buys. Delivery is accurate to about the smallest rung, and
        // something that must fire at 09:00:00.000 wants a scheduler rather than
        // a message broker.
        //
        // The hop count is the other number. A long delay is several broker
        // round trips rather than one -- a one-day message is twenty-four hops
        // through the one-hour rung -- which is the honest cost of not requiring
        // the delayed-message-exchange plugin.
        //
        // Why not simply set an expiration on the message and let it
        // dead-letter? Because a classic queue expires messages only from its
        // HEAD. Put a four-hour message in and a one-minute message behind it,
        // and the one-minute message is delivered in four hours -- and nothing
        // reports it: the queue looks healthy and the message is not lost, it is
        // just late by a factor nobody predicted. The ladder avoids that by
        // giving every message in a rung the same delay, so the head is always
        // the message due soonest.

        // The example cleans up its own destination. The scheduler's queues stay
        // where they are: they are shared with every other service on this
        // broker, and they may be holding somebody else's message.
        consumer.Dispose();
        await mq.DeleteQueueAsync(Queue);
        await mq.DeleteQueueAsync(Queue + ".dlq");
        await mq.DeleteQueueAsync(Queue + ".parked");
        await mq.DeleteExchangeAsync(Exchange);

        return 0;
    }

    private static void Between(Arrival arrival, TimeSpan lower, TimeSpan upper) =>
        Check(arrival.After >= lower && arrival.After <= upper,
            $"{arrival.ReminderId} arrived after {arrival.After.TotalSeconds:F1}s, " +
            $"outside {lower.TotalSeconds:F1}s..{upper.TotalSeconds:F1}s");

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
