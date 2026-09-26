// A load that keeps running and says what is happening to it, one line at a time.
//
// Every other example here finishes. This one does not: it publishes and consumes
// at a steady rate and writes one JSON object per second describing what it has
// seen. That makes it the thing a fault drill breaks the cluster underneath -- the
// drill kills a node or raises a memory alarm, reads these lines, and judges what
// the client did about it.
//
// Why a drill needs this rather than a probe of its own
// -----------------------------------------------------
// A probe that connects to the broker can answer "is the cluster usable". It cannot
// answer what an application saw: whether it was told the broker had stopped
// reading from it, whether it stopped publishing, whether it started again on its
// own or sat there. Those are properties of a client library, they differ between
// libraries that are otherwise equivalent, and the only thing that can report them
// is a client.
//
// What a line contains
// --------------------
//   blocked      the broker is refusing to read from this connection now
//   published    sends attempted since the start
//   confirmed    sends the broker has acknowledged
//   consumed     deliveries handled
//   failed       sends that failed for a reason other than back-pressure
//   publishRate  confirms per second over the last interval
//   consumeRate  deliveries per second over the last interval
//
// Running it
//
//   dotnet run --project advanced/05-a-standing-load-something-else-can-watch-csharp \
//     > readings.jsonl
//
// Then read the last few lines at any point to see what the client is seeing. Under
// a fault drill that file is the client's testimony, and `tail -n 60` on it is how
// the drill asks.
//
// It stops on Ctrl-C, or after ACEMQ_EXAMPLE_SECONDS if that is set -- which CI
// sets, because a load with no reason to stop is not a failing example there, it is
// a job that never ends. Unset, it runs until interrupted, which is what a drill
// campaign wants.
//
// What to watch under a fault: `published` and `confirmed` moving apart, `blocked`
// turning true with the broker's own reason beside it, and both counters moving
// again once it clears. None of that is visible to anything that asks the broker how
// it is doing -- the cluster is healthy, and this application is not publishing.

using System.Text.Json;
using System.Text.Json.Serialization;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

namespace AceMq.Examples;

public sealed class Order
{
    public string Id { get; set; } = "";
}

// The shape of one line of the timeline.
//
// The property names are a contract with whatever reads them, so they are spelled
// here exactly as a reader expects rather than left to a naming policy. A reader
// looking for `confirmed` and finding `Confirmed` sees a client reporting nothing,
// which is indistinguishable from a well-behaved client on a quiet cluster.
public sealed class Reading
{
    [JsonPropertyName("at")] public string At { get; set; } = "";
    [JsonPropertyName("elapsedMs")] public long ElapsedMs { get; set; }
    [JsonPropertyName("blocked")] public bool Blocked { get; set; }
    [JsonPropertyName("published")] public long Published { get; set; }
    [JsonPropertyName("confirmed")] public long Confirmed { get; set; }
    [JsonPropertyName("consumed")] public long Consumed { get; set; }
    [JsonPropertyName("failed")] public long Failed { get; set; }
    // Sends this library declined because the broker had blocked the connection.
    // Not part of what a drill judges, and reported anyway: it is the number that
    // separates a library which refuses back-pressure promptly from one that parks
    // on it, and counting a prompt refusal as a failure would make the better
    // behaviour look like the worse one.
    [JsonPropertyName("refused")] public long Refused { get; set; }

    [JsonPropertyName("publishRate")] public double PublishRate { get; set; }
    [JsonPropertyName("consumeRate")] public double ConsumeRate { get; set; }

    [JsonPropertyName("reason")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Reason { get; set; }
}

public static class Program
{
    private const string Queue = "dotnet-standing-load.orders";

    // Counters, as longs read and written from two places. Interlocked rather than a
    // lock because each one is a single counter incremented in one task and read in
    // another, and a reading a fraction of a second stale is not one anybody would
    // act on differently.
    private static long _published;
    private static long _confirmed;
    private static long _consumed;
    private static long _failed;
    private static long _refused;

    public static async Task<int> Main()
    {
        // Stderr, so stdout carries nothing but readings. A reader skips whatever is
        // not a JSON object, so mixing them would work -- and it would also mean
        // every diagnostic line here had to stay un-JSON-like for ever, which is not
        // a property anybody would remember to preserve.
        Console.Error.WriteLine($"standing load: {BrokerUrl()}, {Queue} at {Rate()}/s");

        // The AMQP transport, registered before connecting. Without it the only
        // scheme this library knows is `memory`, and the failure names the missing
        // call rather than leaving it to be guessed at.
        Transports.Register(new RabbitMqTransport());

        using var stopping = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            // Handled rather than letting the runtime kill the process, so the
            // connection closes and the final reading is written. A drill reads the
            // tail of this output, and a load that dies without a last line leaves
            // its final interval unaccounted for.
            e.Cancel = true;
            stopping.Cancel();
        };
        if (RunForSeconds() > 0)
        {
            stopping.CancelAfter(TimeSpan.FromSeconds(RunForSeconds()));
        }

        var token = stopping.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/05-a-standing-load-csharp")
                // Short, because a publish that will never be confirmed is the
                // normal state under the faults this load exists to be watched
                // through. A generous timeout would take the sampler's next line
                // with it, and a client that went quiet looks exactly like a client
                // that was never running.
                .ConfirmTimeout(TimeSpan.FromSeconds(5))
                .Build(),
            new JsonCodec(),
            token);

        // Quorum, because a drill's faults are about losing a node. A classic queue
        // lives on one node, and when that node is the one the drill stops, the load
        // stops with it -- which reports a client that gave up when the truth is
        // that the queue went away.
        await mq.DeclareQueueAsync(Queue, QueueType.Quorum, null);

        using var consumer = await mq.ConsumeAsync<Order>(Queue, _ =>
        {
            Interlocked.Increment(ref _consumed);
            return Task.FromResult(Ack.Accept());
        });

        var publishing = PublishAsync(mq, token);
        await SampleAsync(mq, token);
        await publishing;
        return 0;
    }

    private static async Task PublishAsync(AceMqConnection mq, CancellationToken token)
    {
        var publisher = mq.Publisher<Order>("", Queue);
        var interval = TimeSpan.FromSeconds(1.0 / Math.Max(Rate(), 1));

        for (var n = 0L; !token.IsCancellationRequested; n++)
        {
            Interlocked.Increment(ref _published);
            try
            {
                await publisher.SendAsync(new Order { Id = $"o-{n}" });
                Interlocked.Increment(ref _confirmed);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is ConnectionBlockedException or PublishingPausedException)
            {
                // Back-pressure, correctly reported, and not a failure: the broker
                // said it had stopped reading and this library declined rather than
                // waiting. Counted separately so that a library which says so
                // promptly does not read as one losing tens of thousands of messages.
                //
                // Two exception types, because this library distinguishes two things
                // Go names alike: ConnectionBlockedException is the broker blocking
                // the connection, and PublishingPausedException is this application
                // having paused its own publishing. Either way the send was declined
                // rather than lost.
                Interlocked.Increment(ref _refused);
            }
            catch (Exception)
            {
                // Counted rather than hidden, and not fatal: a standing load reports
                // what happened to it and keeps going. While the broker is blocking
                // this connection these are the sends that never completed, and
                // `blocked` on the same line is what says why.
                Interlocked.Increment(ref _failed);
            }

            try
            {
                await Task.Delay(interval, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private static async Task SampleAsync(AceMqConnection mq, CancellationToken token)
    {
        var interval = TimeSpan.FromSeconds(IntervalSeconds());
        var started = DateTimeOffset.UtcNow;
        var last = started;
        long lastConfirmed = 0, lastConsumed = 0;

        void Emit()
        {
            var now = DateTimeOffset.UtcNow;
            var elapsed = (now - last).TotalSeconds;
            var confirmed = Interlocked.Read(ref _confirmed);
            var consumed = Interlocked.Read(ref _consumed);

            var reading = new Reading
            {
                At = now.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                ElapsedMs = (long)(now - started).TotalMilliseconds,
                Blocked = mq.IsBlocked,
                Reason = mq.BlockedReason,
                Published = Interlocked.Read(ref _published),
                Confirmed = confirmed,
                Consumed = consumed,
                Failed = Interlocked.Read(ref _failed),
                Refused = Interlocked.Read(ref _refused),
            };
            if (elapsed > 0)
            {
                reading.PublishRate = (confirmed - lastConfirmed) / elapsed;
                reading.ConsumeRate = (consumed - lastConsumed) / elapsed;
            }
            last = now;
            lastConfirmed = confirmed;
            lastConsumed = consumed;

            Console.WriteLine(JsonSerializer.Serialize(reading));
            // Flushed, because stdout to a file is buffered and a drill reads that
            // file while this process is still running. Without the flush the last
            // readings sit in a buffer and the drill reports a client that went
            // quiet.
            Console.Out.Flush();
        }

        while (!token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            Emit();
        }

        // One last line on the way out, so the final interval is accounted for
        // rather than being the one a reader has to guess about.
        Emit();
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") ?? "amqp://guest:guest@localhost:5672/";

    private static int Rate() => IntFromEnv("ACEMQ_LOAD_RATE", 200);

    private static double IntervalSeconds() => IntFromEnv("ACEMQ_LOAD_INTERVAL", 1);

    // Bounded in CI, where every example is run with no arguments and nothing is
    // standing by to interrupt one.
    private static int RunForSeconds() => IntFromEnv("ACEMQ_EXAMPLE_SECONDS", 0);

    private static int IntFromEnv(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value > 0
            ? value
            : fallback;
}
