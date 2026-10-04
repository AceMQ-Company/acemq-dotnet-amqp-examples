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

// What happens to the message being handled when the process is told to stop,
// in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/10-graceful-shutdown-csharp
//
// Three shutdowns of the same consumer, each arriving while a handler is half
// way through a one-second job:
//
//   await using   drains first: the job finishes and its message is settled
//   using         does not drain: the job's message comes back
//   out of time   a drain with too small a budget, and the answer it gives
//
// Kubernetes sends SIGTERM and starts a clock. Whatever is still inside a
// handler when the connection goes away was never acknowledged, so the broker
// hands it to somebody else -- correct, and the reason a deployment shows up as
// a spike of duplicate work when nobody arranged otherwise. It takes about three
// seconds, because the handler really does sleep.

using System.Diagnostics;

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Order
{
    public string OrderId { get; set; } = "";
}

public static class Program
{
    private const string Drained = "dotnet-csharp-shutdown-drained";
    private const string Abandoned = "dotnet-csharp-shutdown-abandoned";
    private const string OutOfTime = "dotnet-csharp-shutdown-out-of-time";

    // One order per queue, and deliberately one. With a second message behind
    // it, the drain pauses consuming and the broker's next delivery waits at
    // the pause gate -- and in 0.7.5 closing then blocks for a further thirty
    // seconds while that delivery waits out its hold. The README has the
    // numbers. One order keeps this example about what a drain does rather
    // than about that.
    private const int Orders = 1;

    // Long enough that a message is genuinely still being handled when the
    // shutdown arrives, short enough that the example does not drag.
    private static readonly TimeSpan JobTakes = TimeSpan.FromSeconds(1);

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        // A second connection that only looks. The broker is the one to ask how
        // many messages are left, and asking from the connection being shut
        // down is not possible once it has been.
        await using var inspector = await Connect("inspector");

        // 1. await using. Leaving the block drains: consuming is paused, the
        //    handler already running is given up to twenty seconds
        //    (AceMqConnection.DefaultDrainTimeout) to finish, and only then is
        //    the connection closed. The job in hand completes and is
        //    acknowledged by the handler that had it.
        var first = new Worker();
        var clock = new Stopwatch();
        await using (var mq = await Connect("drained"))
        {
            await Fill(inspector, Drained);
            await first.ConsumeAsync(mq, Drained);
            await first.Started.Task.WaitAsync(token);

            // SIGTERM arrives here, with a job half done.
            clock.Start();
        }
        clock.Stop();
        var drainedHandled = first.Handled;
        var drainedLeft = await inspector.MessageCountAsync(Drained);
        Console.WriteLine(
            $"await using  closed after {clock.ElapsedMilliseconds,4} ms, " +
            $"handled {drainedHandled} first, {drainedLeft} left on the queue");

        // 2. using. Dispose does not drain. Nor does it return at once: closing
        //    the channel waits for the callback still running on it, so the job
        //    does finish. But its acknowledgement never reaches the broker, and
        //    the message goes back on the queue -- the side effect has happened,
        //    and it will happen again for whoever takes the message next.
        var second = new Worker();
        clock.Restart();
        using (var mq = await Connect("abandoned"))
        {
            await Fill(inspector, Abandoned);
            await second.ConsumeAsync(mq, Abandoned);
            await second.Started.Task.WaitAsync(token);
            clock.Restart();
        }
        clock.Stop();
        var abandonedHandled = second.Handled;
        var abandonedLeft = await inspector.MessageCountAsync(Abandoned);
        Console.WriteLine(
            $"using        closed after {clock.ElapsedMilliseconds,4} ms, " +
            $"handled {abandonedHandled} first, {abandonedLeft} left on the queue");

        // 3. out of time. The shape a service wants when it needs the answer
        //    rather than only the tidy-up: drain with a budget under the grace
        //    period, and say so when it ran out. await using discards that
        //    answer, because a teardown has nobody to report it to.
        var third = new Worker();
        bool drained;
        long inFlight;
        await using (var mq = await Connect("out-of-time"))
        {
            await Fill(inspector, OutOfTime);
            await third.ConsumeAsync(mq, OutOfTime);
            await third.Started.Task.WaitAsync(token);

            // A tenth of what the job needs. False is the signal worth logging
            // and alerting on: either the grace period is shorter than the
            // slowest handler, or a handler is stuck.
            drained = await mq.DrainConsumersAsync(TimeSpan.FromMilliseconds(100), token);
            inFlight = mq.InFlight;
            Console.WriteLine(
                $"out of time  drained {drained}, {inFlight} still in flight, so the grace period " +
                "was shorter than the job");

            // And then the clock runs out, which is what Dispose stands in for
            // here. Disposed explicitly, before the block ends: leaving the
            // block would drain again with the full twenty seconds, which is
            // time a process past its grace period does not have.
            mq.Dispose();
        }
        var outOfTimeLeft = await inspector.MessageCountAsync(OutOfTime);
        Console.WriteLine($"             {outOfTimeLeft} left on the queue");

        Check(drainedHandled == 1,
            $"await using closed with {drainedHandled} jobs handled, so it did not wait for the one in hand");
        Check(drainedLeft == 0,
            $"{drainedLeft} left after a drained close; the finished job's message should be gone");
        Check(abandonedLeft == Orders,
            $"{abandonedLeft} left after Dispose; the unacknowledged message should have come back");
        Check(!drained && inFlight == 1,
            $"a 100 ms drain of a one-second job answered {drained} with {inFlight} in flight");
        Check(outOfTimeLeft == Orders,
            $"{outOfTimeLeft} left after running out of time; the unfinished message should have come back");

        // What redelivery costs is nothing if the handler is idempotent, and
        // everything if it charges a card. A graceful shutdown reduces
        // duplicates; it does not eliminate them, because a power cut has no
        // SIGTERM. See basic/03-idempotent-consumer-csharp for the other half.

        foreach (var queue in new[] { Drained, Abandoned, OutOfTime })
        {
            await inspector.DeleteQueueAsync(queue);
            await inspector.DeleteQueueAsync(queue + ".dlq");
            await inspector.DeleteQueueAsync(queue + ".parked");
        }

        return 0;
    }

    // One consumer's worth of state: when its first job started, and how many
    // it has finished.
    private sealed class Worker
    {
        private int _handled;

        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Handled => Volatile.Read(ref _handled);

        // Not in a using of its own: the connection owns every consumer started
        // on it and closes them on the way out, which is the order a drain
        // needs -- the handlers finish first, then the subscriptions go.
        public Task<IMessageConsumer> ConsumeAsync(AceMqConnection mq, string queue) =>
            mq.ConsumeAsync<Order>(queue, ConsumerOptions.Prefetch(1), async message =>
            {
                Started.TrySetResult();
                await Task.Delay(JobTakes);
                Interlocked.Increment(ref _handled);
                return Ack.Accept();
            });
    }

    private static async Task Fill(AceMqConnection mq, string queue)
    {
        // Emptied first, because every number this example checks is a count of
        // what is left on the queue, and a run that failed half way leaves
        // messages behind for the next one to count.
        await mq.DeleteQueueAsync(queue);
        await mq.DeclareQueueAsync(queue);
        var publisher = mq.Publisher<Order>("", queue);
        for (var i = 1; i <= Orders; i++)
        {
            await publisher.SendAsync(new Order { OrderId = $"o-{i}" });
        }
    }

    private static Task<AceMqConnection> Connect(string name) =>
        AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName($"examples/10-graceful-shutdown-csharp/{name}")
                .Build());

    private static async Task WaitFor(Func<bool> held, CancellationToken token)
    {
        while (!held())
        {
            token.ThrowIfCancellationRequested();
            await Task.Delay(20, token);
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
