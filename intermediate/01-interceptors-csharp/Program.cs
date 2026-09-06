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

// Interceptors: cross-cutting work in one place, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/01-interceptors-csharp
//
// A tenant stamped on every message, and every handled message timed, without
// either concern appearing in the handler.

using System.Diagnostics;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Invoice
{
    public string InvoiceId { get; set; } = "";
}

// Stamps a header on the way out. Inheriting from the abstract base rather than
// implementing the interface: netstandard2.0 has no default interface members,
// so an interceptor that cares about one moment would otherwise write two empty
// methods — and VB cannot use them either way.
public sealed class TenantStamp : PublishInterceptor
{
    private readonly string _tenant;

    public TenantStamp(string tenant) => _tenant = tenant;

    public override PublishContext BeforePublish(PublishContext context)
    {
        // The envelope is the only part an interceptor may change — an
        // interceptor able to rewrite the payload or the destination could send
        // a message somewhere the caller never asked for.
        //
        // .NET's Envelope has no ToBuilder, so the fields are carried across by
        // hand. Miss one and the message loses it, which is why this is written
        // out rather than hidden in a helper.
        var stamped = Envelope.Of(context.Envelope.Type)
            .Id(context.Envelope.Id)
            .Version(context.Envelope.Version)
            .CorrelationId(context.Envelope.CorrelationId)
            .CausationId(context.Envelope.CausationId)
            .Attempt(context.Envelope.Attempt)
            .FirstSeen(context.Envelope.FirstSeen)
            .Origin(context.Envelope.Origin)
            // x-acemq- is the engine's namespace and would be dropped on
            // consume, so an application header uses its own prefix.
            .Header("x-tenant", _tenant);

        foreach (var header in context.Envelope.Headers)
        {
            stamped.Header(header.Key, header.Value);
        }

        return context.WithEnvelope(stamped.Build());
    }

    public override void AfterConfirm(PublishContext context, PublishResult result) =>
        Console.WriteLine($"  [publish] {result.MessageId} confirmed in {result.Latency.TotalMilliseconds:F1}ms");
}

// Times every handled message and reports what the handler decided.
public sealed class Timing : ConsumeInterceptor
{
    private readonly Stopwatch _clock = new Stopwatch();

    public override void BeforeHandle(ConsumeContext context) => _clock.Restart();

    public override void AfterHandle(ConsumeContext context, Ack ack) =>
        Console.WriteLine(
            $"  [consume] {context.Envelope.Id} -> {ack.Kind} in {_clock.Elapsed.TotalMilliseconds:F1}ms");

    public override void OnError(ConsumeContext context, Exception failure) =>
        Console.WriteLine($"  [consume] {context.Envelope.Id} threw: {failure.Message}");
}

public static class Program
{
    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/01-interceptors-csharp")
                .Build());

        mq.Intercept(new TenantStamp("acme"));
        mq.Intercept(new Timing());

        await mq.DeclareQueueAsync("invoices");

        var arrived = new TaskCompletionSource<IMessage<Invoice>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await mq.ConsumeAsync<Invoice>("invoices", message =>
        {
            // The handler knows nothing about tenants or timing.
            arrived.TrySetResult(message);
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Invoice>("", "invoices").SendAsync(new Invoice { InvoiceId = "inv-1" });

        var received = await arrived.Task.WaitAsync(cancellation.Token);
        Console.WriteLine(
            $"the handler received {received.Payload.InvoiceId}, " +
            $"stamped for tenant {received.Headers["x-tenant"]}");

        return 0;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
