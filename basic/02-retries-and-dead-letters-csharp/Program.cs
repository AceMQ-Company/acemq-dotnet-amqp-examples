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

// Retrying a message, and giving up on it, in C#.
//
//   docker compose up -d
//   dotnet run --project basic/02-retries-and-dead-letters-csharp
//
// The attempt counter is the thing to watch. A broker requeues the bytes it was
// given, so the header on the wire still says 1 however many times the message
// has come back — the count comes from the redelivery flag instead, which is
// why the number below actually moves.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Payment
{
    public string PaymentId { get; set; } = "";
}

public static class Program
{
    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/02-retries-and-dead-letters-csharp")
                .Build());

        // Rejected messages go somewhere somebody can read. A dead-letter
        // queue nobody reads is a place messages go to be forgotten quietly,
        // which is worse than dropping them: it looks like nothing is wrong.
        //
        // The name is not a choice: when a retry policy runs out of attempts
        // the library republishes the message to {queue}.dlq and acknowledges
        // the original, rather than nacking it and leaving the route to the
        // broker. A consumer declares that queue when it starts, so nothing
        // here has to. Setting x-dead-letter-exchange on the source queue is
        // still worth doing — it is the backstop for a TTL expiry, an
        // x-max-length drop, or a rejection from something that is not this
        // library — but it is not where a give-up lands.
        await mq.DeclareQueueAsync("payments");
        const string deadLetters = "payments.dlq";

        var attempts = new List<int>();
        var dead = new TaskCompletionSource<IMessage<Payment>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // Three attempts in all, a short wait between them, and no jitter so
        // the output is readable. Use jitter in anything real: messages that
        // failed together must not come back together.
        var options = ConsumerOptions.Defaults()
            .WithRetry(RetryPolicy.Fixed(3, TimeSpan.FromMilliseconds(300)).WithJitter(0));

        using var consumer = await mq.ConsumeAsync<Payment>("payments", options, message =>
        {
            lock (attempts) attempts.Add(message.Attempt);
            // message.Attempt, not message.Envelope.Attempt. The envelope
            // carries what the publisher wrote and a broker redelivers the
            // original bytes, so that header says 1 for ever; the count this
            // consumer keeps is the one that moves.
            var age = DateTimeOffset.UtcNow - message.Envelope.FirstSeen;
            Console.WriteLine(
                $"handling {message.Payload.PaymentId}, attempt {message.Attempt}, " +
                $"age {age.TotalMilliseconds:F0}ms");

            // Always fails. The policy decides when to stop. TimeSpan.Zero
            // leaves the waiting to the policy rather than overriding it here.
            return Task.FromResult(Ack.Retry(TimeSpan.Zero, "the payment gateway is not answering"));
        });

        using var deadConsumer = await mq.ConsumeAsync<Payment>(deadLetters, message =>
        {
            dead.TrySetResult(message);
            return Task.FromResult(Ack.Accept());
        });

        await mq.Publisher<Payment>("", "payments").SendAsync(new Payment { PaymentId = "p-1" });

        var deadLettered = await dead.Task.WaitAsync(token);
        lock (attempts)
        {
            Console.WriteLine($"dead-lettered after attempts [{string.Join(", ", attempts)}]");
        }
        var lifetime = DateTimeOffset.UtcNow - deadLettered.Envelope.FirstSeen;
        Console.WriteLine(
            $"the envelope kept its history: id={deadLettered.Envelope.Id}, " +
            $"first seen {lifetime.TotalMilliseconds:F0}ms ago");

        return 0;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
