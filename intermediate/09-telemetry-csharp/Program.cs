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

// One trace across two hops, and the same traffic scraped as Prometheus
// metrics, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/09-telemetry-csharp
//
// An order is placed, a handler ships it by publishing again, and a second
// handler hears about the shipment. Four operations, two trips through the
// broker -- and one trace, because the context travels on the message as a
// traceparent header and each consumer continues the trace that sent it.
//
// Then the actuator from AceMq.Amqp.Diagnostics is scraped over HTTP the way
// Prometheus would scrape it, and the counters are checked against the work
// that was actually done.
//
// The library depends on neither OpenTelemetry nor Prometheus. It records to
// the runtime's own ActivitySource and Meter, both named "AceMq.Amqp", and the
// application decides what listens.

using System.Diagnostics;
using System.Globalization;

using AceMq.Amqp;
using AceMq.Amqp.Diagnostics;
using AceMq.Amqp.RabbitMq;

using OpenTelemetry;
using OpenTelemetry.Trace;

public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
}

public sealed class OrderShipped
{
    public string OrderId { get; set; } = "";
    public string Tracking { get; set; } = "";
}

public static class Program
{
    private const string Placed = "dotnet-csharp-telemetry-placed";
    private const string Shipped = "dotnet-csharp-telemetry-shipped";

    // Not 9464, the OpenTelemetry convention and the actuator's default,
    // because something on a developer's machine is often already there. The
    // VB.NET example uses the next one along so the two can run side by side.
    private const int MetricsPort = 9471;

    private static readonly TaskCompletionSource<bool> Heard =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private static string _arrivedWith = "";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        // The whole of the tracing setup, and none of it is AceMQ's API: the
        // library's spans come from an ActivitySource, and AddSource is how any
        // OpenTelemetry pipeline subscribes to one. A real service adds an OTLP
        // exporter here instead of the in-memory one, and nothing else changes.
        var spans = new List<Activity>();
        using var tracing = Sdk.CreateTracerProviderBuilder()
            .AddSource(MetricNames.ActivitySource)
            .AddInMemoryExporter(spans)
            .Build();

        // await using, so leaving Main drains the consumers before closing.
        await using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/09-telemetry-csharp")
                .Build());

        // Started before any traffic. The collector counts from the moment it
        // starts listening, so a scrape only ever describes what happened after
        // that -- which is also what a restarted process looks like to
        // Prometheus, and why its counters are allowed to go back to zero.
        using var actuator = AceMqActuator.Start(mq, new ActuatorOptions { Port = MetricsPort });

        await mq.DeclareQueueAsync(Placed);
        await mq.DeclareQueueAsync(Shipped);

        using var shipping = await mq.ConsumeAsync<OrderPlaced>(Placed, async message =>
        {
            _arrivedWith = Text(message.WireHeaders, "traceparent");

            // A publish from inside a handler. It joins the trace of the message
            // being handled, because the handler's span is Activity.Current
            // while this runs -- which is what turns two services into one
            // picture instead of two unrelated ones.
            await mq.Publisher<OrderShipped>("", Shipped)
                .SendAsync(new OrderShipped { OrderId = message.Payload.OrderId, Tracking = "TRK-1" });
            return Ack.Accept();
        });

        using var notifying = await mq.ConsumeAsync<OrderShipped>(Shipped, message =>
        {
            Heard.TrySetResult(true);
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<OrderPlaced>("", Placed).SendAsync(new OrderPlaced { OrderId = "o-1" });

        await Heard.Task.WaitAsync(token);

        // A span reaches the exporter when it ends, and the last one ends just
        // after its handler returns -- so hearing about the shipment is not
        // quite the same moment as having four spans.
        await WaitFor(() => spans.Count >= 4, token);

        // Copied once, now that nothing else is running that could end a span.
        // The exporter appends from whichever thread ended one, so the list is
        // not something to enumerate while traffic is still moving.
        var recorded = spans.ToList();

        var placedPublish = Find(recorded, Placed + MetricNames.SpanPublishSuffix);
        var placedProcess = Find(recorded, Placed + MetricNames.SpanProcessSuffix);
        var shippedPublish = Find(recorded, Shipped + MetricNames.SpanPublishSuffix);
        var shippedProcess = Find(recorded, Shipped + MetricNames.SpanProcessSuffix);
        var chain = new[] { placedPublish, placedProcess, shippedPublish, shippedProcess };

        foreach (var span in chain)
        {
            var parent = span.ParentSpanId == default ? "(root)" : span.ParentSpanId.ToHexString();
            Console.WriteLine(
                $"{span.DisplayName,-40} {span.GetTagItem(AceMqTelemetry.AttrOutcome),-9} " +
                $"{span.TraceId.ToHexString()}  parent={parent}");
        }

        var traces = recorded.Select(s => s.TraceId).Distinct().Count();
        var roots = recorded.Count(s => s.ParentSpanId == default);
        Console.WriteLine();
        Console.WriteLine($"spans {recorded.Count}, traces {traces}, roots {roots}");
        Console.WriteLine($"o-1 arrived carrying traceparent {_arrivedWith}");

        Check(recorded.Count == 4, $"{recorded.Count} spans were recorded, not four");
        Check(traces == 1, $"four operations landed in {traces} traces, so the context was lost on the way");
        Check(roots == 1, $"{roots} spans have no parent; only the first publish should be a root");

        // The join, link by link. One trace id is necessary and not sufficient:
        // four spans could share a trace and still name the wrong parents.
        Check(placedProcess.ParentSpanId == placedPublish.SpanId,
            "the first consumer's span is not a child of the publish that sent it");
        Check(shippedPublish.ParentSpanId == placedProcess.SpanId,
            "the publish inside the handler did not join the handler's trace");
        Check(shippedProcess.ParentSpanId == shippedPublish.SpanId,
            "the second consumer's span is not a child of the publish that sent it");

        // And where it came from: the header on the wire names exactly the
        // trace and the span the publisher reported. W3C's format, so a Java,
        // Go, Python or Ruby consumer reading this message joins the same trace.
        Check(_arrivedWith == $"00-{placedPublish.TraceId.ToHexString()}-{placedPublish.SpanId.ToHexString()}-01",
            $"the message carried traceparent {_arrivedWith}, which is not the span that published it");

        // The outcome is on the span as well as in the counter, so a dashboard
        // showing failures and a trace search for them find the same messages.
        Check(chain.All(s => s.GetTagItem(AceMqTelemetry.AttrOutcome) is "confirmed" or "acked"),
            "a span is missing its outcome");

        Console.WriteLine();

        // Scraped over HTTP, the way Prometheus does it, rather than by calling
        // actuator.Metrics() -- the endpoint is the thing a scrape job depends
        // on, content type included.
        using var http = new HttpClient();
        var scrape = "";
        await WaitFor(() =>
        {
            scrape = http.GetStringAsync(actuator.Url + "acemq-metrics", token).GetAwaiter().GetResult();
            return Sample(scrape, "acemq_consume_total", Shipped) >= 1;
        }, token);

        foreach (var line in scrape.Split('\n'))
        {
            if ((line.StartsWith("acemq_publish_total{") || line.StartsWith("acemq_consume_total{")
                 || line.StartsWith("acemq_consume_duration_seconds_count{"))
                && (line.Contains(Placed) || line.Contains(Shipped)))
            {
                Console.WriteLine(line);
            }
        }

        var metrics = await http.GetAsync(actuator.Url + "acemq-metrics", token);
        var health = await http.GetAsync(actuator.Url + "acemq-health", token);
        var healthBody = await health.Content.ReadAsStringAsync(token);
        Console.WriteLine();
        Console.WriteLine($"{(int)health.StatusCode} {healthBody.Trim()}");

        // Exactly one each, because exactly one of each happened. A counter
        // that reads two here is counting something twice, and a dashboard
        // built on it doubles every rate it shows.
        foreach (var queue in new[] { Placed, Shipped })
        {
            Check(Sample(scrape, "acemq_publish_total", queue, "confirmed") == 1,
                $"the scrape does not show one confirmed publish to {queue}");
            Check(Sample(scrape, "acemq_consume_total", queue, "acked") == 1,
                $"the scrape does not show one acknowledged delivery on {queue}");
            Check(Sample(scrape, "acemq_consume_duration_seconds_count", queue) == 1,
                $"the handler on {queue} was not timed exactly once");
        }

        // Prometheus refuses a scrape without this, version and all.
        Check(metrics.Content.Headers.ContentType?.ToString().StartsWith("text/plain; version=0.0.4") == true,
            $"the metrics were served as {metrics.Content.Headers.ContentType}, which Prometheus will not parse");
        Check((int)health.StatusCode == 200 && healthBody.Contains("\"status\":\"UP\""),
            $"health answered {(int)health.StatusCode} {healthBody.Trim()} for a connection that is open");

        // The actuator binds to localhost on purpose. Those endpoints are not
        // authenticated and they name every queue and its traffic, so binding
        // to 0.0.0.0 publishes that to anything that can reach the port. Let
        // the scraper reach it through the same host, a sidecar, or a network
        // policy instead.

        await mq.DeleteQueueAsync(Placed);
        await mq.DeleteQueueAsync(Placed + ".dlq");
        await mq.DeleteQueueAsync(Placed + ".parked");
        await mq.DeleteQueueAsync(Shipped);
        await mq.DeleteQueueAsync(Shipped + ".dlq");
        await mq.DeleteQueueAsync(Shipped + ".parked");

        return 0;
    }

    private static Activity Find(List<Activity> recorded, string name) =>
        recorded.SingleOrDefault(s => s.DisplayName == name)
        ?? throw new InvalidOperationException($"no span called \"{name}\" was recorded");

    // One series from the Prometheus text format: the line for this metric
    // whose labels mention every one of the given values. Zero when there is
    // no such line, which is what Prometheus would make of it too.
    private static double Sample(string scrape, string metric, params string[] labelled)
    {
        foreach (var line in scrape.Split('\n'))
        {
            if (!line.StartsWith(metric + "{")) continue;
            var labels = line[..line.LastIndexOf('}')];
            if (labelled.All(value => labels.Contains($"\"{value}\"")))
            {
                return double.Parse(line[(line.LastIndexOf(' ') + 1)..], CultureInfo.InvariantCulture);
            }
        }
        return 0;
    }

    private static string Text(IReadOnlyDictionary<string, object> headers, string name) =>
        headers.TryGetValue(name, out var value) ? Convert.ToString(value, CultureInfo.InvariantCulture) ?? "" : "";

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
