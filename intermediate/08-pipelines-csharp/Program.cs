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

// A message travelling several hops, and picked up where it stopped, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/08-pipelines-csharp
//
// Three steps, each on its own queue, and a card processor that is down the
// first time. The order dead-letters at `charge`, and is then put back at
// CHARGE rather than at the entrance -- so `validate` runs once, not twice.
//
// That is what the routing slip buys. Every message carries one, naming the
// whole route and how far along it is, so a message dead-lettered at step two
// still says it is at step two. Without one, the only safe place to replay a
// half-finished message is the beginning, and every step it already passed runs
// again -- which a step that charges a card cannot survive.
//
// The same run happens twice, once in each of the two wire forms AceMQ has for
// a slip, because they are read and written by different languages.

using System.Collections.Concurrent;

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Order
{
    public string OrderId { get; set; } = "";

    /// <summary>What has been done to it, so a repeated step is visible in the payload.</summary>
    public string Trail { get; set; } = "";
}

public static class Program
{
    private const string Declared = "dotnet-csharp-fulfilment";
    private const string Itinerised = "dotnet-csharp-fulfilment-by-address";

    private static readonly ConcurrentQueue<string> Ran = new();

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/08-pipelines-csharp")
                .Build());

        await DeclaredForm(mq, token);
        Console.WriteLine();
        await ItineraryForm(mq, token);

        return 0;
    }

    /// <summary>The default: step names in <c>x-acemq-route</c>, which is what Java writes.</summary>
    private static async Task DeclaredForm(AceMqConnection mq, CancellationToken token)
    {
        var chargeFails = true;
        var shipped = new TaskCompletionSource<Order>(TaskCreationOptions.RunContinuationsAsynchronously);

        // Each step is a queue, and that is what separates this from calling
        // three methods in a row: a step that fails retries on its own, a slow
        // step builds a visible backlog instead of blocking the ones before it,
        // and each step scales independently.
        //
        // The two type parameters make the chain check at compile time -- a step
        // added after one producing Order can only accept an Order -- so a
        // mismatch is a compile error rather than a decode failure at the third
        // step in production.
        using var pipeline = await mq.Pipeline<Order>(Declared)
            .Step<Order>("validate", order => Task.FromResult<Order?>(Did("validate", order)))
            .Step<Order>("charge", order =>
            {
                Ran.Enqueue("charge");
                // Fatal rather than ordinary: a fatal failure dead-letters
                // immediately instead of retrying, which is what puts the
                // message somewhere an operator can find it.
                if (chargeFails) throw new AceFatalException("the card processor is down");
                return Task.FromResult<Order?>(Did("charged", order, counted: false));
            })
            .Step<Order>("ship", order =>
            {
                var done = Did("ship", order);
                shipped.TrySetResult(done);
                return Task.FromResult<Order?>(done);
            })
            .BuildAsync();

        // Nothing declares {queue}.dlq here, and that is deliberate. Building
        // the pipeline started a consumer on every step queue, and a consumer
        // declares its own {queue}.dlq and {queue}.parked as it starts. Adding
        // `await mq.DeclareQueueAsync(pipeline.QueueFor("charge") + ".dlq")`
        // after this line is a PRECONDITION_FAILED rather than a no-op: that
        // overload declares a QUORUM queue, and the dead-letter queues are
        // CLASSIC, which is what Java and Topology declare them as.

        Console.WriteLine($"pipeline    {pipeline.Name}: {string.Join(" -> ", pipeline.StepNames)}");
        Console.WriteLine($"slip form   {pipeline.SlipForm}, in {RoutingSlip.RouteHeader}");

        await pipeline.SendAsync(new Order { OrderId = "order-1" });

        // What an operator finds in the dead-letter queue: the message exactly
        // as it was when it failed, headers and all.
        var stuck = await StuckAt(mq, pipeline.QueueFor("charge"), token);
        await stuck.AcknowledgeAsync();

        var stalled = (RoutingSlip)Route.From(stuck.WireHeaders)!;
        Console.WriteLine(
            $"stalled at  {stalled.Current}, position {stalled.Position} of " +
            $"[{string.Join(", ", stalled.Steps)}]");
        Console.WriteLine($"on the wire {RoutingSlip.RouteHeader}: " +
                          $"{Text(stuck.WireHeaders, RoutingSlip.RouteHeader)}");
        Console.WriteLine($"payload     {stuck.Payload.Trail}");

        // The card processor comes back.
        chargeFails = false;

        // ResumeAsync, not SendAsync. SendAsync would start the run over,
        // repeating every step before the failure; the slip among these headers
        // is what says which step this message had reached, and there is nothing
        // else on the message that does.
        await pipeline.ResumeAsync(stuck.Payload, stuck.WireHeaders);
        var delivered = await shipped.Task.WaitAsync(token);

        Console.WriteLine($"resumed     ran [{string.Join(", ", Ran)}]");
        Console.WriteLine($"delivered   {delivered.OrderId}: {delivered.Trail}");
        Console.WriteLine(
            $"counters    entered {pipeline.Entered}, completed {pipeline.Completed}, " +
            $"ended early {pipeline.EndedEarly}, in flight {pipeline.InFlight}");

        Check(stalled.Current == "charge",
            $"the slip said the message was at {stalled.Current}, not at charge");
        Check(stalled.Position == 1, $"the slip said position {stalled.Position}, not 1");
        Check(stalled.Steps.SequenceEqual(new[] { "validate", "charge", "ship" }),
            $"the slip named [{string.Join(", ", stalled.Steps)}], not the pipeline's own steps");
        Check(Text(stuck.WireHeaders, RoutingSlip.RouteHeader) == "validate,charge,ship",
            "the route on the wire is not the comma-joined form Java reads");

        // The claim the whole example exists for. A restart would have re-run
        // every step before the failure; resuming runs the step that failed and
        // everything after it, and nothing else.
        Check(Ran.ToArray().SequenceEqual(new[] { "validate", "charge", "charge", "ship" }),
            $"the steps that ran were [{string.Join(", ", Ran)}], and a resume that repeats "
            + "validate is a restart wearing a different name");
        Check(delivered.Trail == "validate,charged,ship",
            $"the delivered order says '{delivered.Trail}', so a step ran that should not have");
        Check(pipeline.Entered == 1 && pipeline.Completed == 1 && pipeline.InFlight == 0,
            $"the pipeline counted {pipeline.Entered} in and {pipeline.Completed} out");

        pipeline.Dispose();
        await Clean(mq, pipeline);
    }

    /// <summary>
    /// The other form: a JSON itinerary in <c>acemq-routing-slip</c>, carrying each
    /// stop's address rather than its name.
    /// </summary>
    /// <remarks>
    /// What Go, Python and Ruby write. Worth choosing when a consumer in one of
    /// those reads one of these queues, or when a stop is somewhere this library
    /// has not declared: the message carries its own addresses, so nothing has to
    /// be resolved against anything. Either form is <em>read</em> whatever this is
    /// set to.
    /// </remarks>
    private static async Task ItineraryForm(AceMqConnection mq, CancellationToken token)
    {
        var shipFails = true;
        var shipped = new TaskCompletionSource<Order>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var pipeline = await mq.Pipeline<Order>(Itinerised)
            .WritingSlipAs(SlipForm.Itinerary)
            .Step<Order>("validate", order => Task.FromResult<Order?>(Did("validate", order, counted: false)))
            .Step<Order>("ship", order =>
            {
                if (shipFails) throw new AceFatalException("the label printer is offline");
                var done = Did("ship", order, counted: false);
                shipped.TrySetResult(done);
                return Task.FromResult<Order?>(done);
            })
            .BuildAsync();

        Console.WriteLine($"pipeline    {pipeline.Name}: {string.Join(" -> ", pipeline.StepNames)}");
        Console.WriteLine($"slip form   {pipeline.SlipForm}, in {Itinerary.Header}");

        await pipeline.SendAsync(new Order { OrderId = "order-2" });
        var stuck = await StuckAt(mq, pipeline.QueueFor("ship"), token);
        await stuck.AcknowledgeAsync();

        var stalled = (Itinerary)Route.From(stuck.WireHeaders)!;
        Console.WriteLine($"on the wire {Itinerary.Header}: {Text(stuck.WireHeaders, Itinerary.Header)}");
        Console.WriteLine(
            $"stalled at  {stalled.Next!.Name} on {stalled.Next.RoutingKey}, " +
            $"after {string.Join(", ", stalled.Done.Select(step => step.Name))}");

        shipFails = false;
        await pipeline.ResumeAsync(stuck.Payload, stuck.WireHeaders);
        var delivered = await shipped.Task.WaitAsync(token);
        Console.WriteLine($"delivered   {delivered.OrderId}: {delivered.Trail}");

        // The queue rather than the bare step name, because an itinerary is
        // meant to be followed by something that has never heard of this
        // pipeline and so cannot turn `ship` into `<pipeline>.ship`.
        Check(stalled.Next.RoutingKey == pipeline.QueueFor("ship"),
            $"the itinerary points at {stalled.Next.RoutingKey}, not at a queue anything could "
            + "publish to without knowing this pipeline");
        Check(stalled.Next.Exchange.Length == 0,
            $"the next stop names exchange '{stalled.Next.Exchange}'; a pipeline's steps are "
            + "queues on the default exchange");
        Check(stalled.Done.Count == 1 && stalled.Done[0].Name == "validate",
            "the itinerary does not say which steps are already behind it");
        Check(stalled.Done[0].CompletedAt.EndsWith("Z", StringComparison.Ordinal),
            $"a completed step is stamped '{stalled.Done[0].CompletedAt}', which is not the "
            + "RFC 3339 UTC every one of the five libraries parses");

        var wire = Text(stuck.WireHeaders, Itinerary.Header);
        Check(wire.Contains("\"routingKey\"", StringComparison.Ordinal),
            $"the slip on the wire is {wire}, which is not the JSON the other libraries read");

        // And the point worth making twice: resuming does not care which form
        // the slip is in. Both say where the message was, and that is all
        // ResumeAsync needs.
        Check(delivered.Trail == "validate,ship",
            $"the delivered order says '{delivered.Trail}', so resuming an itinerary re-ran a step");

        pipeline.Dispose();
        await Clean(mq, pipeline);
    }

    /// <summary>Records a step and stamps the payload, so a repeat shows up in both.</summary>
    private static Order Did(string step, Order order, bool counted = true)
    {
        if (counted) Ran.Enqueue(step);
        return new Order
        {
            OrderId = order.OrderId,
            Trail = order.Trail.Length == 0 ? step : order.Trail + "," + step,
        };
    }

    private static async Task<PulledMessage<Order>> StuckAt(
        AceMqConnection mq, string stepQueue, CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();
            var pulled = await mq.PullAsync<Order>(stepQueue + ".dlq", TimeSpan.FromMilliseconds(100));
            if (pulled != null) return pulled;
        }
    }

    /// <summary>A header as text, whichever of the two the transport hands back.</summary>
    private static string Text(IReadOnlyDictionary<string, object> headers, string name) =>
        headers.TryGetValue(name, out var value) && value != null
            ? value as string ?? System.Text.Encoding.UTF8.GetString((byte[])value)
            : "(absent)";

    private static async Task Clean(AceMqConnection mq, Pipeline<Order> pipeline)
    {
        foreach (var step in pipeline.StepNames)
        {
            var queue = pipeline.QueueFor(step);
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
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
