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

// A log that is read rather than emptied, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/07-streams-csharp
//
// A queue is destructive: one consumer takes a message and it is gone. A stream
// keeps everything until retention removes it, every reader holds its own
// position in it, and reading changes nothing.
//
// Four readers run over one stream here. Three of them read messages that an
// earlier reader had already read, which is the whole point and is impossible
// with a queue.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Order
{
    public string OrderId { get; set; } = "";
    public int Total { get; set; }
}

public static class Program
{
    // Streams are shared with the Go, Python, Ruby and Java examples on a
    // development broker, and a stream redeclared with different arguments is a
    // PRECONDITION_FAILED rather than an adjustment. A name of its own avoids
    // borrowing somebody else's retention policy.
    private const string Log = "dotnet-csharp-orders-log";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/07-streams-csharp")
                .Build());

        // A stream is append-only and nothing consumes it away, so a second run
        // would read this run's messages as well as its own. Deleting first is
        // what makes the example re-runnable; a real log is not deleted.
        await mq.DeleteQueueAsync(Log);

        // Retention is the argument that matters. Both limits accept null, which
        // is legal and almost always wrong: a stream with neither grows until
        // the disk is full, and on RabbitMQ a full disk is not a stream problem
        // but a broker-wide alarm that blocks every publisher on the node.
        //
        // The fourth argument is the segment size, and it is opt-in because
        // retention happens a whole segment at a time. A stream bounded at 20 MB
        // with 20 MB segments keeps rather more than 20 MB, since nothing can be
        // discarded until an entire segment can be.
        await mq.DeclareStreamAsync(
            Log, TimeSpan.FromHours(1), 20L * 1024 * 1024, 1L * 1024 * 1024);

        var publisher = mq.Publisher<Order>("", Log);
        for (var i = 0; i < 5; i++)
        {
            await publisher.SendAsync(new Order { OrderId = $"o-{i}", Total = i * 10 });
        }
        Console.WriteLine("wrote      o-0 .. o-4");

        // A projection being built for the first time has to see history, so it
        // says FromFirst(). A reader that is not told where to start reads from
        // FromNext() -- right for a live consumer, and silently wrong for this:
        // it would come up empty and look perfectly healthy.
        var first = new List<string>();
        long checkpoint;
        var readerOne = await mq.Stream<Order>(Log)
            .FromFirst()
            .Prefetch(10)
            .ConsumeAsync(message =>
            {
                lock (first) first.Add(message.Payload.OrderId);
                return Task.CompletedTask;
            });
        try
        {
            await WaitFor(() => Count(first) == 5, token);

            // The offset of the last message handled, not the count of them --
            // and nothing stores it for you. The broker remembers no reader's
            // position, which is exactly what makes a stream cheap to have
            // several readers on. Saving this next to whatever the projection
            // wrote is the application's job.
            checkpoint = readerOne.LastHandledOffset
                ?? throw new InvalidOperationException("the reader handled nothing");
            Console.WriteLine($"read       {readerOne.Handled}, checkpoint offset {checkpoint}");
        }
        finally
        {
            readerOne.Dispose();
        }

        // Five more while that reader is not running, which is what a deploy or
        // a crash looks like from the stream's side.
        for (var i = 5; i < 10; i++)
        {
            await publisher.SendAsync(new Order { OrderId = $"o-{i}", Total = i * 10 });
        }
        Console.WriteLine("wrote      o-5 .. o-9 while the reader was down");

        // Resuming from one past the checkpoint. Not from the checkpoint: that
        // offset was handled, and starting there would handle it twice.
        var resumed = new List<string>();
        using (var readerTwo = await mq.Stream<Order>(Log)
                   .FromOffset(checkpoint + 1)
                   .ConsumeAsync(message =>
                   {
                       lock (resumed) resumed.Add(message.Payload.OrderId);
                       return Task.CompletedTask;
                   }))
        {
            await WaitFor(() => Count(resumed) == 5, token);
        }
        Console.WriteLine($"resumed    [{string.Join(", ", Snapshot(resumed))}]");

        // The property a queue does not have. Two readers have been over this
        // stream and a third, attached now, still sees all ten: nothing above
        // consumed anything.
        var auditor = new List<string>();
        using (var readerThree = await mq.Stream<Order>(Log)
                   .FromFirst()
                   .ConsumeAsync(message =>
                   {
                       lock (auditor) auditor.Add(message.Payload.OrderId);
                       return Task.CompletedTask;
                   }))
        {
            await WaitFor(() => Count(auditor) == 10, token);
        }
        Console.WriteLine($"auditor    saw all {Count(auditor)} from the beginning");

        // And the default, which is the one to be deliberate about. FromNext()
        // is what a live consumer wants and what a StreamReader does when it is
        // not told otherwise: history is skipped entirely.
        var live = new List<string>();
        using (var readerFour = await mq.Stream<Order>(Log)
                   .FromNext()
                   .ConsumeAsync(message =>
                   {
                       lock (live) live.Add(message.Payload.OrderId);
                       return Task.CompletedTask;
                   }))
        {
            await publisher.SendAsync(new Order { OrderId = "o-10", Total = 100 });
            await WaitFor(() => Count(live) == 1, token);
        }
        Console.WriteLine($"live       saw [{string.Join(", ", Snapshot(live))}] and none of the history");

        // Eleven messages written and twenty-one deliveries handled, which is
        // the arithmetic a queue cannot produce: on a queue eleven messages are
        // eleven deliveries and then the queue is empty.
        var delivered = Count(first) + Count(resumed) + Count(auditor) + Count(live);
        Console.WriteLine($"totals     11 written, {delivered} handled across four readers");

        // Worth knowing rather than worth asserting: the broker does not report
        // a stream's length through queue.declare, so MessageCountAsync comes
        // back 0 for a stream however much is in it. Reach for the management
        // API if the depth is what you need.
        Console.WriteLine($"depth      MessageCountAsync says {await mq.MessageCountAsync(Log)}");

        Check(Snapshot(first).SequenceEqual(new[] { "o-0", "o-1", "o-2", "o-3", "o-4" }),
            $"the first reader saw [{string.Join(", ", Snapshot(first))}], not o-0..o-4");
        Check(checkpoint == 4, $"the checkpoint was offset {checkpoint}, not 4");
        Check(Snapshot(resumed).SequenceEqual(new[] { "o-5", "o-6", "o-7", "o-8", "o-9" }),
            $"resuming saw [{string.Join(", ", Snapshot(resumed))}], so there is a gap or a repeat");
        Check(Count(auditor) == 10, $"the auditor saw {Count(auditor)} of the first ten");
        Check(Snapshot(live).SequenceEqual(new[] { "o-10" }),
            $"the live reader saw [{string.Join(", ", Snapshot(live))}], not just o-10");
        Check(delivered == 21,
            $"{delivered} deliveries from eleven messages, so a reader did not see what it should");

        // When not to use one. A stream is the wrong shape for work
        // distribution: every reader sees every message, so two workers on a
        // stream both do the job rather than sharing it. That is a queue, and a
        // consumer group over a queue is how it is scaled -- see
        // intermediate/06-consumer-groups-csharp.

        await mq.DeleteQueueAsync(Log);
        await mq.DeleteQueueAsync(Log + ".dlq");
        await mq.DeleteQueueAsync(Log + ".parked");

        return 0;
    }

    private static int Count(List<string> seen)
    {
        lock (seen) return seen.Count;
    }

    private static string[] Snapshot(List<string> seen)
    {
        lock (seen) return seen.ToArray();
    }

    private static async Task WaitFor(Func<bool> held, CancellationToken token)
    {
        while (!held())
        {
            token.ThrowIfCancellationRequested();
            await Task.Delay(25, token);
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
