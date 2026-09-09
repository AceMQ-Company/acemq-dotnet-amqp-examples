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

// Keeping a large payload off the broker, in C#.
//
//   docker compose up -d
//   dotnet run --project advanced/02-claim-check-csharp
//
// A scanned report is tens of megabytes. Putting it on a queue is possible and
// is a mistake: it fills the broker's memory, it is copied to every bound
// queue, it makes a dead-letter queue impossible to inspect, and it turns a
// broker into a filesystem with worse tools. What travels instead is a claim
// check -- the payload goes to a store, and the message carries the key.
//
// Two documents are published here, one either side of the threshold, because
// the threshold is the whole point and it is invisible if every message is
// large. Both are read by one consumer that is not told which is which.

using System.Text;
using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class Document
{
    public string DocumentId { get; set; } = "";
    public string Body { get; set; } = "";
}

public static class Program
{
    private const string Exchange = "claim-check-documents";
    private const string Readers = "claim-check-readers";
    private const string Wire = "claim-check-wire";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        // A filesystem store rather than the in-memory one, because a claim
        // check that does not outlive the process that wrote it is a message
        // nobody else can read. InMemoryClaimCheckStore holds the payloads in
        // the publisher's heap -- which is where they were going to be anyway,
        // so it takes them off the broker and does nothing else. It is genuinely
        // useful in a test, where publisher and consumer are one process and the
        // framing is what is being proved.
        //
        // This one is useful where the filesystem is shared and durable: an NFS
        // mount, a persistent volume. On a container's local disk it is the
        // in-memory store with extra steps, since the consumer is on another
        // host and finds nothing. Object storage is the usual right answer, and
        // a store in front of S3 or Azure Blob Storage is three short methods.
        var directory = Path.Combine(Path.GetTempPath(), "acemq-claim-check-csharp");
        var store = new FilesystemClaimCheckStore(directory);
        Console.WriteLine($"payloads go to {store.Location}");

        // The codec wraps another: the payload is encoded as usual, and what
        // happens to those bytes then depends on how many there are.
        var codec = ClaimCheckCodec.Wrapping(new JsonCodec(), store);
        Check(codec.Threshold == ClaimCheckCodec.DefaultThreshold,
            $"the threshold was {codec.Threshold}");
        Check(ClaimCheckCodec.DefaultThreshold == 65536,
            $"the default threshold was {ClaimCheckCodec.DefaultThreshold}, not 65536");

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/02-claim-check-csharp")
                .Build(),
            codec,
            token);

        // One fanout, two queues, one publish. The first queue is read the
        // ordinary way and yields Documents; the second is read as raw bytes, so
        // what actually went on the wire can be printed rather than described.
        await mq.DeclareExchangeAsync(Exchange, "fanout");
        await mq.DeclareQueueAsync(Readers);
        await mq.DeclareQueueAsync(Wire);
        await mq.BindAsync(Readers, Exchange, "");
        await mq.BindAsync(Wire, Exchange, "");

        var read = new List<Document>();
        var bodies = new List<byte[]>();
        var readAll = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wireAll = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        string? contentTypeOfChecked = null;

        // The consumer is not told which document was offloaded. The framing
        // says which of the two it is, and that is what allows the threshold to
        // be changed, or this codec to be introduced, without a flag day.
        using var readers = await mq.ConsumeAsync<Document>(Readers, message =>
        {
            lock (read)
            {
                read.Add(message.Payload);
                if (read.Count == 2) readAll.TrySetResult(true);
            }
            return Task.FromResult(Ack.Accept());
        });

        using var wire = await mq.ConsumeAsync<byte[]>(
            Wire, ConsumerOptions.Defaults().As(new BytesCodec()), message =>
            {
                lock (bodies)
                {
                    bodies.Add(message.Payload);
                    if (ClaimCheckCodec.IsClaimCheck(message.Payload))
                    {
                        contentTypeOfChecked = message.ContentType;
                    }
                    if (bodies.Count == 2) wireAll.TrySetResult(true);
                }
                return Task.FromResult(Ack.Accept());
            });

        // A small document, which travels inline. Offloading a two-hundred-byte
        // message turns one broker round trip into a store round trip AND a
        // broker round trip, so an unconditional claim check makes the common
        // case slower in order to fix the rare one.
        var small = new Document { DocumentId = "doc-small", Body = "a note" };

        // And one of exactly 65536 encoded bytes, which is the boundary itself.
        // The comparison is strictly-less-than -- `encoded.Length < threshold`
        // travels inline -- so a payload of exactly the threshold is offloaded.
        // The padding is measured rather than guessed: encode the document with
        // an empty body, and make up the difference in characters that are one
        // byte each in both UTF-8 and JSON.
        var probe = codec.Delegate.Encode(new Document { DocumentId = "doc-large", Body = "" });
        var large = new Document
        {
            DocumentId = "doc-large",
            Body = new string('x', ClaimCheckCodec.DefaultThreshold - probe.Length),
        };
        Check(codec.Delegate.Encode(large).Length == ClaimCheckCodec.DefaultThreshold,
            "the large document is not exactly the threshold, so it proves nothing about it");

        var publisher = mq.Publisher<Document>(Exchange, "");
        await publisher.SendAsync(small);
        await publisher.SendAsync(large);

        await Task.WhenAll(readAll.Task.WaitAsync(token), wireAll.Task.WaitAsync(token));

        // ---- what went on the wire ---------------------------------------
        //
        //   0xAC 0x01 0x00  payload    inline, identical to what JSON wrote
        //   0xAC 0x01 0x01  key        a claim check, the key as UTF-8
        //
        // Byte for byte what the Java, Python and Ruby libraries write.
        byte[] inlineBody, checkedBody;
        lock (bodies)
        {
            inlineBody = bodies.Single(b => !ClaimCheckCodec.IsClaimCheck(b));
            checkedBody = bodies.Single(ClaimCheckCodec.IsClaimCheck);
        }

        Console.WriteLine(
            $"inline:  {Hex(inlineBody)} + {inlineBody.Length - 3} bytes of payload");
        Console.WriteLine(
            $"checked: {Hex(checkedBody)} + {checkedBody.Length - 3} bytes naming the payload");

        Check(inlineBody[0] == 0xAC && inlineBody[1] == 0x01 && inlineBody[2] == 0x00,
            "the inline message is not framed as inline");
        Check(checkedBody[0] == 0xAC && checkedBody[1] == 0x01 && checkedBody[2] == 0x01,
            "the offloaded message is not framed as a claim check");
        Check(inlineBody.Length == codec.Delegate.Encode(small).Length + 3,
            "the inline body is not the JSON plus a three-byte header");
        Check(ClaimCheckCodec.KeyOf(inlineBody) == null,
            "an inline message named a key, which it has no business doing");

        // The content type is the delegate's, unchanged. Unlike encryption,
        // where the bytes really are something else, a claim-checked message is
        // still a document -- it is a document that is somewhere else, and a
        // consumer without the store gets a clear failure rather than a parser
        // error.
        Console.WriteLine($"content type of the claim check: {contentTypeOfChecked}");
        Check(contentTypeOfChecked == "application/json",
            $"the claim check arrived as {contentTypeOfChecked}");

        // ---- the key on the wire is the file on disk ----------------------
        var key = ClaimCheckCodec.KeyOf(checkedBody);
        Check(key != null, "the offloaded message carried no key");

        var held = Directory.GetFiles(store.Location);
        Console.WriteLine($"the store holds {held.Length} payload(s): {Path.GetFileName(held[0])}");
        Check(held.Length == 1,
            $"the store holds {held.Length} payloads: the small one was offloaded too");
        Check(Path.GetFileName(held[0]) == key,
            "the file on disk is not the key that was on the wire");
        Check(new FileInfo(held[0]).Length == ClaimCheckCodec.DefaultThreshold,
            "the stored payload is not the document that was offloaded");

        // Redeemed through a store built from nothing but the directory, which
        // is the point of a filesystem store and the thing the in-memory one
        // cannot do: this object never saw the publish. In a real deployment it
        // is in another process, on another host.
        var elsewhere = new FilesystemClaimCheckStore(directory);
        var redeemed = elsewhere.Get(key!);
        Check(redeemed != null && redeemed.Length == ClaimCheckCodec.DefaultThreshold,
            "a store that did not write the payload could not read it back");

        // ---- and what the consumer saw ------------------------------------
        List<Document> documents;
        lock (read) documents = read.OrderBy(d => d.DocumentId).ToList();

        foreach (var document in documents)
        {
            Console.WriteLine($"read {document.DocumentId}: {document.Body.Length} characters");
        }

        Check(documents.Count == 2, $"{documents.Count} documents arrived, not two");
        Check(documents[0].DocumentId == "doc-large" && documents[0].Body == large.Body,
            "the offloaded document did not come back whole");
        Check(documents[1].DocumentId == "doc-small" && documents[1].Body == small.Body,
            "the inline document did not come back whole");

        // RETENTION IS THE PART THAT GOES WRONG, and this example is about to
        // do the exact thing a deployment must not. The store and the queue have
        // different lifetimes and nothing enforces a relationship between them:
        // a message replayed a month later carries a key, and if the store
        // expired that key the replay produces a message nobody can read --
        // worse than a lost message, because it looks like a message and fails
        // deep inside a consumer rather than visibly.
        //
        // So a store's retention has to outlast every retention that could bring
        // a message back: queue TTLs, dead-letter queues, and however long
        // somebody might sit on a message before replaying it by hand. When in
        // doubt, longer. The directory is removed here only so a second run
        // reports the same numbers as the first.
        readers.Dispose();
        wire.Dispose();
        foreach (var queue in new[] { Readers, Wire })
        {
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
        }
        await mq.DeleteExchangeAsync(Exchange);
        Directory.Delete(directory, true);

        return 0;
    }

    private static string Hex(byte[] body) =>
        string.Join(" ", body.Take(3).Select(b => "0x" + b.ToString("X2")));

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
