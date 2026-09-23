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

// What Health() says when the broker has stopped accepting publishes, in C#.
//
//   docker compose up -d
//   dotnet run --project advanced/03-health-and-back-pressure-csharp
//
// A broker under memory or disk pressure raises an alarm and stops reading from
// the connections that are publishing. The connection is open, the process is
// fine, and nothing is lost -- the broker is protecting itself and will start
// reading again when the pressure passes.
//
// The question this example answers is what a health endpoint should say about
// that, because the obvious answer is wrong. If the service reports unhealthy,
// an orchestrator restarts it into the same blocked broker, having thrown away
// whatever it was holding, and does that to every replica at once. So Health()
// reports UP with the reason attached, and leaves "should this count against
// us" to the application -- which is a policy, and belongs where policies live.
//
// Up to 0.6.0 the library reported Degraded here. Because the aggregate takes
// the worst report, that reading overruled any more careful one a caller had
// composed alongside it. 0.7.0 changed it, and this example is the check that
// it stayed changed.
//
// THIS EXAMPLE NEEDS A BROKER OF ITS OWN, and the reason is worth reading
// before copying any of it. A memory alarm is raised on the node, not on the
// connection: every publisher on that broker blocks, not just this one. Run
// this against the broker the other examples share and it blocks them too, so
// compose gives it `broker-health` and nothing else goes near that one. The
// alternative -- running it last and hoping -- is a test that fails on a
// Tuesday for reasons nobody can reconstruct.

using System.Diagnostics;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Reading
{
    public string Id { get; set; } = "";
}

// Something of the application's own in the health report, which is the reason
// the report is a list rather than a single status. Its state is settable here
// only so one example can show it both ways; in a service it would be reading
// a queue depth, a lag, or the age of the last successful run.
public sealed class Projection : IHealthContributor
{
    public string Name => "orders-projection";

    public HealthStatus Status { get; set; } = HealthStatus.Up;

    public HealthReport Report() =>
        new HealthReport(Name, Status, new Dictionary<string, string>
        {
            ["behindBy"] = Status == HealthStatus.Up ? "0" : "4200",
        });
}

// The 0.6.0 reading, reconstructed as what it always should have been: this
// application's policy rather than the library's. A service that genuinely
// wants back pressure to count against it writes these eight lines and
// registers them, and a service that does not simply does not.
public sealed class BlockedIsDegraded : IHealthContributor
{
    private readonly AceMqConnection _mq;

    public BlockedIsDegraded(AceMqConnection mq) => _mq = mq;

    public string Name => "back-pressure-policy";

    public HealthReport Report() =>
        new HealthReport(
            Name,
            _mq.IsBlocked ? HealthStatus.Degraded : HealthStatus.Up,
            new Dictionary<string, string>
            {
                ["reason"] = _mq.BlockedReason ?? "(not blocked)",
            });
}

// A contributor that throws is itself a health problem, and must not take the
// whole report down with it -- an endpoint that returns 500 because one probe
// had a bad afternoon tells an operator nothing about the other five.
public sealed class Broken : IHealthContributor
{
    public string Name => "a-contributor-that-throws";

    public HealthReport Report() => throw new InvalidOperationException("no route to the metrics store");
}

public static class Program
{
    private const string Queue = "health-back-pressure-csharp";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        var container = Environment.GetEnvironmentVariable("ACEMQ_HEALTH_CONTAINER") is { Length: > 0 } c
            ? c
            : "acemq-examples-broker-health";

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(180));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/03-health-and-back-pressure-csharp")
                // A publish that will never be confirmed is the normal state
                // here, so the wait for a confirmation has to be short enough
                // that the example can notice rather than hang.
                .ConfirmTimeout(TimeSpan.FromSeconds(2))
                .Build(),
            new JsonCodec(),
            token);

        await mq.DeclareQueueAsync(Queue);

        // ---- nothing wrong ------------------------------------------------
        var healthy = mq.Health();
        Show("a broker that is fine", healthy);
        Check(healthy.Status == HealthStatus.Up, $"a working connection reported {healthy.Status}");
        Check(healthy.Reports.Count == 1, "something other than the connection is already reporting");

        var connection = Report(healthy, "connection");
        Check(connection.Details["open"] == "true", "the connection is not open");
        Check(connection.Details["blocked"] == "false", "the connection is blocked before anything blocked it");
        Check(!connection.Details.ContainsKey("blockedReason"),
            "an unblocked connection named a reason for being blocked");

        // ---- the worst report wins ----------------------------------------
        //
        // Which is why a lagging projection is visible at all. Averaging health,
        // or letting the connection speak for the whole service, hides exactly
        // the component that has stopped.
        var projection = new Projection();
        mq.RegisterHealth(projection);
        Check(mq.Health().Status == HealthStatus.Up, "an Up contributor moved the aggregate");

        projection.Status = HealthStatus.Degraded;
        var lagging = mq.Health();
        Show("a projection falling behind", lagging);
        Check(lagging.Status == HealthStatus.Degraded,
            $"a Degraded contributor left the aggregate at {lagging.Status}");
        Check(Report(lagging, "connection").Status == HealthStatus.Up,
            "the connection was dragged down by a contributor that is not the connection");
        projection.Status = HealthStatus.Up;

        // ---- now actually block the broker --------------------------------
        //
        // The alarm alone is not enough: RabbitMQ blocks a connection when it
        // next tries to publish, so something has to publish. The watermark is
        // put back in the finally, and putting it back is the whole reason
        // there is a finally -- a broker left with a 0.0001 watermark is a
        // broker that blocks every later run of every other example, and the
        // symptom is a publish that hangs with nothing in any log to say why.
        Console.WriteLine($"\ndropping the memory high watermark on {container}");
        Rabbitmqctl(container, "set_vm_memory_high_watermark", "0.0001");
        try
        {
            var publisher = mq.Publisher<Reading>("", Queue);
            for (var i = 0; i < 40 && !mq.IsBlocked; i++)
            {
                try
                {
                    await publisher.SendAsync(new Reading { Id = "r-" + i })
                        .WaitAsync(TimeSpan.FromMilliseconds(500), token);
                }
                catch (TimeoutException)
                {
                    // A publish that never confirms because the broker has
                    // stopped reading is the state being waited for, not a
                    // failure.
                }

                await Task.Delay(250, token);
            }

            Check(mq.IsBlocked, "the broker never blocked, so this example proved nothing");
            Console.WriteLine($"IsBlocked={mq.IsBlocked}, BlockedReason={mq.BlockedReason}");
            Check(mq.BlockedReason != null, "the connection is blocked but will not say why");
            Check(mq.BlockedReason!.Contains("memory", StringComparison.OrdinalIgnoreCase),
                $"the broker blocked for some other reason: {mq.BlockedReason}");

            // ---- THE READING THIS EXAMPLE EXISTS FOR ----------------------
            var blocked = mq.Health();
            Show("a broker applying back pressure", blocked);

            Check(blocked.Status == HealthStatus.Up,
                $"a blocked connection reported {blocked.Status}; since 0.7.0 it reports Up with the reason");
            var blockedConnection = Report(blocked, "connection");
            Check(blockedConnection.Status == HealthStatus.Up,
                $"the connection report was {blockedConnection.Status}");
            Check(blockedConnection.Details["open"] == "true",
                "a blocked connection was reported as closed; it is open, and that is the point");
            Check(blockedConnection.Details["blocked"] == "true",
                "the health report does not mention that the connection is blocked");
            Check(blockedConnection.Details.TryGetValue("blockedReason", out var why) && why == mq.BlockedReason,
                "the health report does not carry the reason the broker gave");

            // ---- and it answers without asking the broker ------------------
            //
            // The property that makes it usable as a liveness probe. Health()
            // reads state the connection already has; it sends nothing and
            // waits for nothing, so a broker that has stopped reading cannot
            // make the probe hang -- which would get the process killed by the
            // very back pressure it was reporting on.
            var clock = Stopwatch.StartNew();
            for (var i = 0; i < 1000; i++) mq.Health();
            clock.Stop();
            var each = clock.Elapsed.TotalMilliseconds / 1000;
            Console.WriteLine($"1000 Health() calls on a blocked connection took " +
                $"{clock.Elapsed.TotalMilliseconds:F1}ms, {each * 1000:F1}us each");
            Check(each < 1.0,
                $"Health() took {each:F3}ms per call on a blocked connection, which is too slow to be free");

            // ---- wanting the old reading back ------------------------------
            mq.RegisterHealth(new BlockedIsDegraded(mq));
            var policy = mq.Health();
            Show("the same broker, with a policy that counts back pressure", policy);
            Check(policy.Status == HealthStatus.Degraded,
                "a contributor that calls a blocked connection Degraded did not move the aggregate");
            Check(Report(policy, "connection").Status == HealthStatus.Up,
                "the library's own reading changed, and it should not have");
        }
        finally
        {
            // 0.6 is RabbitMQ 4's default. Leaving the broker usable is part of
            // the example: the next thing to run against it is somebody's real
            // work, and an alarm nobody raised on purpose is a bad afternoon.
            Console.WriteLine("\nputting the memory high watermark back");
            Rabbitmqctl(container, "set_vm_memory_high_watermark", "0.6");
        }

        // ---- the alarm clears ---------------------------------------------
        for (var i = 0; i < 40 && mq.IsBlocked; i++) await Task.Delay(250, token);

        var recovered = mq.Health();
        Show("the alarm cleared", recovered);
        Check(!mq.IsBlocked, "the broker is still blocked after the watermark was restored");
        Check(mq.BlockedReason == null, $"the connection still names a reason: {mq.BlockedReason}");
        Check(recovered.Status == HealthStatus.Up, $"the aggregate came back as {recovered.Status}");
        Check(!Report(recovered, "connection").Details.ContainsKey("blockedReason"),
            "the reason is still in the report after the alarm cleared");

        // ---- a contributor that throws -------------------------------------
        mq.RegisterHealth(new Broken());
        var broken = mq.Health();
        Show("one probe having a bad afternoon", broken);
        Check(broken.Status == HealthStatus.Down, $"a throwing contributor reported {broken.Status}");
        Check(Report(broken, "a-contributor-that-throws").Details["error"] == "no route to the metrics store",
            "the exception the contributor threw was not carried into the report");
        Check(Report(broken, "connection").Status == HealthStatus.Up,
            "one contributor throwing took the connection's own report down with it");

        await mq.DeleteQueueAsync(Queue);
        await mq.DeleteQueueAsync(Queue + ".dlq");
        await mq.DeleteQueueAsync(Queue + ".parked");

        return 0;
    }

    private static void Show(string label, AggregateHealth health)
    {
        Console.WriteLine($"\n{label}: {health.Status}");
        foreach (var report in health.Reports)
        {
            var details = string.Join(", ", report.Details.Select(d => $"{d.Key}={d.Value}"));
            Console.WriteLine($"  {report.Name}: {report.Status}  [{details}]");
        }
    }

    private static HealthReport Report(AggregateHealth health, string name)
    {
        var report = health.Reports.FirstOrDefault(r => r.Name == name);
        Check(report != null, $"nothing called {name} was in the health report");
        return report!;
    }

    // Reaching for the broker's own control tool, because nothing in AMQP can
    // raise a memory alarm and an example that mocked one would be proving
    // something about the mock. HOME is set because rabbitmqctl reads the
    // Erlang cookie from it, and the broker runs with HOME=/tmp.
    private static void Rabbitmqctl(string container, params string[] arguments)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var argument in new[]
                 {
                     "exec", "-u", "rabbitmq", "-e", "HOME=/var/lib/rabbitmq",
                     container, "rabbitmqctl",
                 }.Concat(arguments))
        {
            start.ArgumentList.Add(argument);
        }

        using var process = Process.Start(start)
            ?? throw new InvalidOperationException("docker could not be started");
        var output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
        process.WaitForExit();
        Check(process.ExitCode == 0,
            $"`rabbitmqctl {string.Join(" ", arguments)}` on {container} failed: {output.Trim()}");
    }

    // An example that prints the right answer whatever happened is an example
    // that cannot fail, and CI running it proves nothing. This throws, which
    // makes the process exit non-zero.
    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    // Its own broker, not the one the other examples share -- see the note at
    // the top. ACEMQ_URL is deliberately not consulted: picking it up would
    // point a memory alarm at whatever that variable happens to name.
    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_HEALTH_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5673/";
}
