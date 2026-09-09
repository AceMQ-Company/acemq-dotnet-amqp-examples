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

// Several consumers over one queue, started and stopped together, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/06-consumer-groups-csharp
//
// Forty jobs handled one at a time, then forty more handled eight at a time,
// with the wall clock and the measured concurrency printed for both. It takes
// about three seconds, because the handler really does sleep.
//
// Two numbers decide how a queue behaves under load and they get confused
// constantly. CONCURRENCY is how many handlers run at once. PREFETCH is how
// many messages the broker may hand one consumer before it acknowledges any of
// them. Getting the second one wrong is what makes a queue with eight consumers
// behave like a queue with one.

using System.Collections.Concurrent;
using System.Diagnostics;

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Job
{
    public string JobId { get; set; } = "";
}

public static class Program
{
    private const string Work = "dotnet-csharp-group-work";

    // Long enough that the difference between one consumer and eight is the
    // wall clock rather than the noise in it.
    private static readonly TimeSpan HandlerTakes = TimeSpan.FromMilliseconds(50);

    private static readonly ConcurrentDictionary<string, int> Handled = new();
    private static readonly object PeakGate = new();
    private static int _running;
    private static int _peak;

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/06-consumer-groups-csharp")
                .Build());

        await mq.DeclareQueueAsync(Work);

        // Prefetch 1 in both runs, so the only thing that changes between them
        // is the number of consumers. It is also the setting that makes the
        // second run work at all: with the default the broker may hand twenty
        // messages to the first consumer that asks, and seven of the eight sit
        // idle while one of them works through a private backlog. A group is
        // only as parallel as its prefetch lets it be.
        var options = ConsumerOptions.Prefetch(1);

        var serial = await RunBatch(mq, options, size: 1, from: 0, count: 40, token);
        Console.WriteLine(
            $"1 consumer  40 jobs in {serial.Elapsed} ms, peak concurrency {serial.Peak}");

        var parallel = await RunBatch(mq, options, size: 8, from: 40, count: 40, token);
        Console.WriteLine(
            $"8 consumers 40 jobs in {parallel.Elapsed} ms, peak concurrency {parallel.Peak}");

        Console.WriteLine();
        Console.WriteLine($"handled     {Handled.Count} of 80, each exactly once: {EachOnce()}");
        Console.WriteLine($"group size  {serial.SizeWhileRunning} then {parallel.SizeWhileRunning}, " +
                          $"and {parallel.SizeAfterDispose} once disposed");

        // What to call before shutting down. Disposing a connection while
        // handlers are mid-flight abandons their work: the messages were never
        // acknowledged so they come back, but a side effect already applied has
        // happened twice by the time they do.
        var drained = await mq.DrainConsumersAsync(TimeSpan.FromSeconds(10));
        Console.WriteLine($"drained     {drained}, in flight {mq.InFlight}");

        Check(Handled.Count == 80, $"{Handled.Count} distinct jobs were handled, not eighty");
        Check(EachOnce(), "a job was handled more than once, so the group duplicated work");
        Check(serial.Peak == 1,
            $"one consumer reached concurrency {serial.Peak}, which one consumer cannot do");
        Check(parallel.Peak >= 2,
            $"eight consumers never got past concurrency {parallel.Peak}, so nothing ran in parallel");

        // Two times rather than eight. The handler sleeps 50 ms, so forty jobs
        // one at a time cannot take less than two seconds and an eight-way run
        // should be near a quarter of that -- but asserting 8x would be
        // asserting that the machine running CI is idle. 2x still fails loudly
        // if the group quietly regresses to serial.
        Check(serial.Elapsed >= 2 * parallel.Elapsed,
            $"forty jobs took {serial.Elapsed} ms on one consumer and {parallel.Elapsed} ms on " +
            "eight, which is not the speed-up a group is for");

        Check(serial.SizeWhileRunning == 1 && parallel.SizeWhileRunning == 8,
            "the group did not report the size it was started with");
        Check(parallel.SizeAfterDispose == 0,
            $"{parallel.SizeAfterDispose} consumers were still attached after Dispose");
        Check(drained && mq.InFlight == 0,
            $"draining left {mq.InFlight} handlers running, so a shutdown here would abandon work");

        // What a group costs. One consumer on a queue sees messages in order;
        // eight do not, because the broker round-robins between them and a slow
        // handler finishes after a fast one that started later. Where a later
        // message about the same entity must not overtake an earlier one, keep
        // the group and route by key -- see basic/03-idempotent-consumer-csharp
        // for the other half of that story.
        //
        // A group is not the only way to go faster, either. Raising prefetch
        // lets one consumer hold more unacknowledged messages, but the handler
        // still runs them one at a time on that consumer's channel. Reach for a
        // group when the handler is slow enough that one channel is the limit,
        // or when a fair share across processes matters: eight consumers here
        // compete evenly with eight in another instance.

        await mq.DeleteQueueAsync(Work);
        await mq.DeleteQueueAsync(Work + ".dlq");
        await mq.DeleteQueueAsync(Work + ".parked");

        return 0;
    }

    private sealed class Batch
    {
        public long Elapsed { get; set; }
        public int Peak { get; set; }
        public int SizeWhileRunning { get; set; }
        public int SizeAfterDispose { get; set; }
    }

    private static async Task<Batch> RunBatch(
        AceMqConnection mq, ConsumerOptions options,
        int size, int from, int count, CancellationToken token)
    {
        var publisher = mq.Publisher<Job>("", Work);
        for (var i = from; i < from + count; i++)
        {
            await publisher.SendAsync(new Job { JobId = $"job-{i}" });
        }

        lock (PeakGate) _peak = 0;
        var before = Handled.Count;
        var clock = Stopwatch.StartNew();

        // The two things a group saves you from. Starting workers by hand means
        // remembering to close every one, and a partial shutdown leaves
        // messages held by a consumer nobody is waiting for. And the size is a
        // number, so it can come from configuration -- which is the setting
        // most often changed after a service is already running.
        var group = await ConsumerGroup.StartAsync<Job>(mq, Work, size, options, Handle);
        var batch = new Batch { SizeWhileRunning = group.Size };
        try
        {
            await WaitFor(() => Handled.Count == before + count, token);
            clock.Stop();
        }
        finally
        {
            // Stops every consumer, and closes all of them even if one throws:
            // leaving the rest running after a failed shutdown is worse than
            // the failure.
            group.Dispose();
        }

        batch.Elapsed = clock.ElapsedMilliseconds;
        lock (PeakGate) batch.Peak = _peak;
        batch.SizeAfterDispose = group.Size;
        return batch;
    }

    // Concurrency is measured by counting the handlers running at the same
    // moment, not by counting threads. The group dispatches onto the thread
    // pool, so even the serial run touches a dozen thread names and a thread
    // count would prove nothing.
    private static async Task<Ack> Handle(IMessage<Job> message)
    {
        lock (PeakGate)
        {
            _running++;
            if (_running > _peak) _peak = _running;
        }

        try
        {
            await Task.Delay(HandlerTakes);
            Handled.AddOrUpdate(message.Payload.JobId, 1, (_, times) => times + 1);
            return Ack.Accept();
        }
        finally
        {
            lock (PeakGate) _running--;
        }
    }

    private static bool EachOnce() => Handled.Values.All(times => times == 1);

    private static async Task WaitFor(Func<bool> held, CancellationToken token)
    {
        while (!held())
        {
            token.ThrowIfCancellationRequested();
            await Task.Delay(10, token);
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
