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

// Encrypting the message body, in C#.
//
//   docker compose up -d
//   dotnet run --project advanced/01-encrypting-payloads-csharp
//
// The broker, its disk, its backups and its management interface see
// ciphertext. Headers do not: the envelope is how the library routes and
// retries, so it cannot be encrypted without the broker losing the ability to
// do its job. Do not put anything secret in a header.

using System.Text;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class CardPayment
{
    public string PaymentId { get; set; } = "";
    public string Pan { get; set; } = "";
}

public static class Program
{
    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // A keyring holds more than one key because rotation needs an overlap:
        // the new key is added everywhere first so every consumer can read it,
        // and only then made current. A keyring with one key cannot rotate
        // without an outage.
        var lastYear = EncryptionKey.Generate("2025-06");
        var current = EncryptionKey.Generate("2026-01");
        var keyring = Keyring.Builder()
            .Add(lastYear)
            .Current(current)
            .Build();

        // The encrypting codec wraps another: the payload is encoded as usual
        // and the bytes are then encrypted.
        var codec = EncryptedCodec.Wrapping(new JsonCodec(), keyring);

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/01-encrypting-payloads-csharp")
                .Build(),
            codec,
            cancellation.Token);

        await mq.DeclareQueueAsync("card-payments");

        var arrived = new TaskCompletionSource<IMessage<CardPayment>>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var consumer = await mq.ConsumeAsync<CardPayment>("card-payments", message =>
        {
            arrived.TrySetResult(message);
            return Task.FromResult(Ack.Accept());
        });

        var payment = new CardPayment { PaymentId = "p-1", Pan = "4111111111111111" };
        await mq.Publisher<CardPayment>("", "card-payments").SendAsync(payment);

        var received = await arrived.Task.WaitAsync(cancellation.Token);
        Console.WriteLine($"the consumer read {received.Payload.PaymentId} / {received.Payload.Pan}");

        // What actually went on the wire.
        var onTheWire = codec.Encode(payment);
        var asText = Encoding.UTF8.GetString(onTheWire);
        Console.WriteLine($"the body is {onTheWire.Length} bytes of ciphertext");
        Console.WriteLine($"  contains the card number: {asText.Contains("4111111111111111")}");
        Console.WriteLine($"  names the key that opens it: {EncryptedCodec.KeyIdOf(onTheWire)}");

        // The key id travels in the clear, and has to: a consumer must know
        // which key to try before it can decrypt anything. It names a key
        // rather than revealing one.
        return 0;
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
