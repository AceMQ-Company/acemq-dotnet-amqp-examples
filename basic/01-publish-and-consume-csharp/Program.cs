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

// Publishing a message and consuming it, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/01-publish-and-consume-csharp
//
// The smallest thing that is still honest: a durable queue, a confirmed
// publish, and a consumer that says what it did with the message.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

// The json property names are what make a C# field and a Java field the same
// field. The codec camel-cases by default, so OrderId goes out as "orderId".
public sealed class OrderPlaced
{
    public string OrderId { get; set; } = "";
    public long TotalCents { get; set; }
}

public static class Program
{
    public static async Task<int> Main()
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = cancellation.Token;

        // The transport has to be registered before a broker URL can be
        // resolved. Referencing the package is not enough: nothing in this
        // program touches that assembly otherwise, so it is never loaded.
        // Without this line the connection fails with a message that says
        // exactly what to do about it.
        Transports.Register(new RabbitMqTransport());

        // ClientName is what RabbitMQ's management interface shows. Worth
        // setting before you need it, which will be while working out which
        // process is holding a message.
        var config = ConnectionConfig.ForUrl(BrokerUrl())
            .ClientName("examples/01-publish-and-consume-csharp")
            .Build();

        using var mq = await AceMqConnection.ConnectAsync(config);

        await mq.DeclareQueueAsync("orders");

        var arrived = new TaskCompletionSource<IMessage<OrderPlaced>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await mq.ConsumeAsync<OrderPlaced>("orders", message =>
        {
            arrived.TrySetResult(message);
            // Returning the disposition rather than calling a method means a
            // handler that forgets to decide does not compile.
            return Task.FromResult(Ack.Accept());
        });

        var publisher = mq.Publisher<OrderPlaced>("", "orders");

        var result = await publisher.SendAsync(new OrderPlaced { OrderId = "o-1", TotalCents = 4250 });
        Console.WriteLine(
            $"published {result.MessageId}, routed by the broker: {result.Routed}, " +
            $"took {result.Latency.TotalMilliseconds:F1}ms");

        var received = await arrived.Task.WaitAsync(token);
        Console.WriteLine(
            $"consumed  {received.Envelope.Id}: order {received.Payload.OrderId} " +
            $"for {received.Payload.TotalCents} cents");
        Console.WriteLine(
            $"          type=\"{received.Envelope.Type}\" attempt={received.Envelope.Attempt} " +
            $"origin={received.Envelope.Origin}");

        return 0;
    }

    // The compose broker unless ACEMQ_URL names another.
    //
    // Repeated in every example rather than shared: each directory is meant to
    // be readable on its own, and a helper somewhere else is one more thing to
    // go and find.
    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
