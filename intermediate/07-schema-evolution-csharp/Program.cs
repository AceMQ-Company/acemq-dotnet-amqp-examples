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

// A producer on a new schema and a consumer on an old one, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/07-schema-evolution-csharp
//
// Avro messages are not self-describing: a reader must already hold the schema
// the writer used, or the bytes cannot be read. That is the whole design
// difference from JSON, and it is why a registry exists -- the message carries
// a four-byte identifier and the reader looks the schema up.
//
// Adding a field is the change every service makes eventually, and it is only
// safe in one direction at a time unless the readers can resolve the writer's
// schema against their own. Both directions run here, over a real broker: a
// producer deployed ahead of its consumers, and a consumer deployed ahead of
// its producers.

using AceMq.Amqp;
using AceMq.Amqp.Avro;
using AceMq.Amqp.RabbitMq;

// Unqualified, and deliberately. Writing `Avro.Schema` would be ambiguous:
// `using AceMq.Amqp;` brings the nested namespace AceMq.Amqp.Avro into scope
// under the simple name `Avro`, so the compiler cannot tell which one is meant.
using Avro;
using Avro.Generic;

public static class Program
{
    private const string Subject = "acemq.examples.OrderPlaced";

    // What the producers were writing last month.
    private const string V1 = """
        {"type":"record","name":"OrderPlaced","namespace":"acemq.examples","fields":[
          {"name":"id","type":"string"},
          {"name":"total","type":"int"}
        ]}
        """;

    // The same record with a currency added, and a default for it. The default
    // is the whole of the compatibility: it is what a reader on this schema puts
    // in the field when the writer did not send one.
    private const string V2 = """
        {"type":"record","name":"OrderPlaced","namespace":"acemq.examples","fields":[
          {"name":"id","type":"string"},
          {"name":"total","type":"int"},
          {"name":"currency","type":"string","default":"GBP"}
        ]}
        """;

    // The same addition done wrong: a new field with nothing to fall back on.
    private const string V2NoDefault = """
        {"type":"record","name":"OrderPlaced","namespace":"acemq.examples","fields":[
          {"name":"id","type":"string"},
          {"name":"total","type":"int"},
          {"name":"currency","type":"string"}
        ]}
        """;

    private const string Exchange = "dotnet-csharp-avro-orders";
    private const string OldReader = "dotnet-csharp-avro-v1-reader";
    private const string NewReader = "dotnet-csharp-avro-v2-reader";
    private const string RawReader = "dotnet-csharp-avro-bytes";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var token = cancellation.Token;

        // One registry, shared by every producer and consumer in this process.
        // That sharing is the part an in-memory registry cannot give you across
        // processes: it hands out ids in the order schemas are registered, so a
        // restart renumbers everything and two services never agree. It is right
        // for a test and for one process reading its own messages, and wrong for
        // anything where a message outlives the process that wrote it --
        // implement ISchemaRegistry against your database or a
        // Confluent-compatible registry for that.
        var registry = new InMemorySchemaRegistry();

        var url = BrokerUrl();

        // A codec belongs to a connection: the publisher overload that takes one
        // is internal, so a producer that writes a different schema is a
        // different connection. Two producers, two connections, and neither is
        // told anything about the readers.
        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/07-schema-evolution-csharp").Build());
        using var lastMonth = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/07-schema-evolution-csharp/v1").Build(),
            AvroCodec.Registered(registry, V1), token);
        using var deployedToday = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/07-schema-evolution-csharp/v2").Build(),
            AvroCodec.Registered(registry, V2), token);

        await mq.DeclareExchangeAsync(Exchange, "fanout");
        foreach (var queue in new[] { OldReader, NewReader, RawReader })
        {
            await mq.DeclareQueueAsync(queue);
            await mq.BindAsync(queue, Exchange, "");
        }

        var seenByOld = new Dictionary<string, GenericRecord>();
        var seenByNew = new Dictionary<string, GenericRecord>();
        // In arrival order, which one queue with one consumer preserves: the
        // first is the message the v1 producer wrote.
        var raw = new List<byte[]>();
        var contentTypes = new List<string>();

        // Each consumer says which schema it was written against. That is the
        // reader schema, and it is what makes resolution happen: the codec looks
        // the writer's schema up by the id on the message and asks Avro to read
        // one into the other.
        using var oldConsumer = await mq.ConsumeAsync<GenericRecord>(
            OldReader,
            ConsumerOptions.Prefetch(1).As(AvroCodec.Registered(registry, V1)),
            message => Collect(seenByOld, message.Payload));

        using var newConsumer = await mq.ConsumeAsync<GenericRecord>(
            NewReader,
            ConsumerOptions.Prefetch(1).As(AvroCodec.Registered(registry, V2)),
            message =>
            {
                lock (contentTypes) contentTypes.Add(message.ContentType ?? "(none)");
                return Collect(seenByNew, message.Payload);
            });

        // A third reader that does not decode at all, so the framing can be
        // shown rather than described.
        using var rawConsumer = await mq.ConsumeAsync<byte[]>(
            RawReader,
            ConsumerOptions.Prefetch(1).As(new BytesCodec()),
            message =>
            {
                lock (raw) raw.Add(message.Payload);
                return Task.FromResult(Ack.Accept());
            });

        // The producers publish whatever version they are on.
        await lastMonth.Publisher<GenericRecord>(Exchange, "")
            .SendAsync(Order(V1, "o-1", 100, null));
        await deployedToday.Publisher<GenericRecord>(Exchange, "")
            .SendAsync(Order(V2, "o-2", 200, "EUR"));

        await WaitFor(() => Count(seenByOld) == 2 && Count(seenByNew) == 2 && Bodies(raw).Length == 2,
            token);

        var bodies = Bodies(raw);
        var (magic, v1Id) = Frame(bodies[0]);
        var v2Id = Frame(bodies[1]).Id;

        Console.WriteLine($"registry    {registry.Count} schemas, ids issued as they were first written");
        Console.WriteLine(
            $"wire format {contentTypes.First()}, 0x{magic:x2} then a schema id, " +
            $"then the Avro body");
        Console.WriteLine(
            $"ids         v1 producer wrote id {v1Id}, v2 producer wrote id {v2Id}");
        Console.WriteLine();
        Console.WriteLine("reader   message  id     total  currency");
        Console.WriteLine($"v1       from v1  {Show(seenByOld, "o-1")}");
        Console.WriteLine($"v1       from v2  {Show(seenByOld, "o-2")}");
        Console.WriteLine($"v2       from v1  {Show(seenByNew, "o-1")}");
        Console.WriteLine($"v2       from v2  {Show(seenByNew, "o-2")}");

        // Producer deployed first. The old reader has never heard of `currency`,
        // and the field is SKIPPED rather than shifting every byte after it --
        // which is what would happen without the writer's schema, and why a
        // total of 200 arriving intact is the thing to look at here.
        Check(Field(seenByOld, "o-2", "currency") == null,
            "the v1 reader saw a currency, which is not in the schema it was written against");
        Check(Equals(Field(seenByOld, "o-2", "total"), 200),
            $"the v1 reader read total {Field(seenByOld, "o-2", "total")} out of a v2 message, "
            + "so the unknown field shifted the bytes after it");

        // Consumer deployed first. The producer is not sending `currency` yet
        // and the reader's default fills it in, so the new code can be written
        // as though the field were always there.
        Check(Equals(Field(seenByNew, "o-1", "currency"), "GBP"),
            $"the v2 reader saw currency {Field(seenByNew, "o-1", "currency")} on a v1 message, "
            + "not the default its own schema declares");
        Check(Equals(Field(seenByNew, "o-2", "currency"), "EUR"),
            "the v2 reader did not see the currency a v2 producer actually sent");

        Check(Equals(Field(seenByOld, "o-1", "total"), 100)
              && Equals(Field(seenByNew, "o-1", "total"), 100),
            "a reader disagreed with the other about what a v1 message says");

        // The framing, which is Confluent's and the same bytes the Java library
        // writes: one zero byte, then four bytes of identifier, big-endian, then
        // the Avro body. A message written here can be read by either.
        Check(magic == 0, $"the message began with 0x{magic:x2}, not the magic zero byte");
        Check(v1Id != v2Id,
            $"both producers wrote schema id {v1Id}, so the id does not identify the schema");
        Check(registry.SchemaFor(v1Id).Subject == Subject
              && registry.SchemaFor(v2Id).Subject == Subject,
            $"an id resolved to something other than {Subject}");
        Check(registry.SchemaFor(v1Id).Format == "avro",
            $"schema id {v1Id} is registered as {registry.SchemaFor(v1Id).Format}, not avro");
        Check(!registry.SchemaFor(v1Id).Definition.Contains("currency")
              && registry.SchemaFor(v2Id).Definition.Contains("currency"),
            "the ids on the wire do not point at the schemas the producers were on");
        Check(contentTypes.All(type => type == AvroCodec.RegisteredContentType),
            $"a message arrived as {contentTypes.First()}, not {AvroCodec.RegisteredContentType}");

        // And the rule all of this rests on. Add the same field without a
        // default and there is nothing Avro can put in it when an old producer
        // omits it, so the read fails -- which in a deployment means every
        // consumer breaking the moment it is rolled out ahead of the producers.
        // It fails identically every time, so the library raises it as fatal and
        // the message is dead-lettered rather than retried round a loop.
        var refused = Refuses(registry, bodies[0]);
        Console.WriteLine();
        Console.WriteLine($"no default  refused: {refused}");
        Check(refused,
            "a reader on a schema whose new field has no default read an old message anyway, "
            + "which would make the incompatible change look safe");

        Check(registry.Count == 2,
            $"{registry.Count} schemas were registered, not the two the producers write");

        oldConsumer.Dispose();
        newConsumer.Dispose();
        rawConsumer.Dispose();

        foreach (var queue in new[] { OldReader, NewReader, RawReader })
        {
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
        }
        await mq.DeleteExchangeAsync(Exchange);

        return 0;
    }

    private static GenericRecord Order(string schemaJson, string id, int total, string? currency)
    {
        var record = new GenericRecord((RecordSchema)Schema.Parse(schemaJson));
        record.Add("id", id);
        record.Add("total", total);
        if (currency != null) record.Add("currency", currency);
        return record;
    }

    private static Task<Ack> Collect(Dictionary<string, GenericRecord> into, GenericRecord order)
    {
        lock (into) into[(string)order["id"]] = order;
        return Task.FromResult(Ack.Accept());
    }

    /// <summary>What a reader ended up with, or null when the field is not in its schema.</summary>
    private static object? Field(Dictionary<string, GenericRecord> seen, string id, string field)
    {
        lock (seen)
        {
            return seen.TryGetValue(id, out var order) && order.TryGetValue(field, out var value)
                ? value
                : null;
        }
    }

    private static bool Refuses(ISchemaRegistry registry, byte[] writtenByV1)
    {
        try
        {
            AvroCodec.Registered(registry, V2NoDefault).Decode(writtenByV1, typeof(GenericRecord));
            return false;
        }
        catch (AceFatalException)
        {
            return true;
        }
    }

    private static (byte Magic, int Id) Frame(byte[] body) =>
        (body[0], (body[1] << 24) | (body[2] << 16) | (body[3] << 8) | body[4]);

    private static byte[][] Bodies(List<byte[]> raw)
    {
        lock (raw) return raw.ToArray();
    }

    private static string Show(Dictionary<string, GenericRecord> seen, string id)
    {
        var currency = Field(seen, id, "currency");
        return $" {id,-5}  {Field(seen, id, "total"),5}  "
               + (currency == null ? "(not in my schema)" : currency);
    }

    private static int Count<TValue>(Dictionary<string, TValue> seen)
    {
        lock (seen) return seen.Count;
    }

    private static async Task WaitFor(Func<bool> held, CancellationToken token)
    {
        while (!held())
        {
            token.ThrowIfCancellationRequested();
            await Task.Delay(25, token);
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
