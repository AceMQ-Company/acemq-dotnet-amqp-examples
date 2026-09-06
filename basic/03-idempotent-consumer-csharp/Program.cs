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

// Handling a message once, even when it arrives twice, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/03-idempotent-consumer-csharp
//
// Retries and redeliveries mean a message can arrive more than once, so a
// handler that changes anything needs to be able to tell.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Charge
{
    public string ChargeId { get; set; } = "";
    public long Cents { get; set; }
}

public static class Program
{
    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/03-idempotent-consumer-csharp")
                .Build());

        await mq.DeclareQueueAsync("charges");

        // In this process only, which is right for one worker and wrong the
        // moment there are two: each would have its own memory and both would
        // believe they were first. A store the workers share — ideally the same
        // database as the work, in the same transaction — is what makes this
        // hold.
        //
        // The window has to outlast the longest a message can still be retried.
        var store = new InMemoryIdempotencyStore(TimeSpan.FromHours(1));

        var charged = 0;
        var deliveries = 0;

        var options = ConsumerOptions.Defaults()
            .WithRetry(RetryPolicy.Fixed(5, TimeSpan.FromMilliseconds(200)).WithJitter(0))
            // The consumer claims the message id before the handler runs and
            // confirms it after, so a duplicate never reaches the handler.
            .Idempotent(store);

        using var consumer = await mq.ConsumeAsync<Charge>("charges", options, message =>
        {
            Interlocked.Increment(ref deliveries);

            if (Volatile.Read(ref deliveries) == 1)
            {
                // Fail the first delivery so the broker sends it again. A
                // failed message is released rather than confirmed, which is
                // what lets the retry actually run — remembering one that then
                // failed would mean the retry silently does nothing.
                Console.WriteLine("delivery 1: failing on purpose");
                return Task.FromResult(Ack.Retry(TimeSpan.Zero, "the card processor timed out"));
            }

            Interlocked.Increment(ref charged);
            Console.WriteLine($"charging {message.Payload.ChargeId} for {message.Payload.Cents} cents");
            return Task.FromResult(Ack.Accept());
        });

        var publisher = mq.Publisher<Charge>("", "charges");
        var charge = new Charge { ChargeId = "c-1", Cents = 4250 };

        // The same message id three times: one logical message published more
        // than once, which is what an at-least-once producer looks like.
        for (var i = 0; i < 3; i++)
        {
            await publisher.SendAsync(charge, Envelope.Of("charge").Id("charge-c-1").Build());
        }

        await Task.Delay(TimeSpan.FromSeconds(3), cancellation.Token);

        Console.WriteLine(
            $"delivered {Volatile.Read(ref deliveries)} times, " +
            $"charged {Volatile.Read(ref charged)} time(s)");

        return Volatile.Read(ref charged) == 1 ? 0 : 1;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
