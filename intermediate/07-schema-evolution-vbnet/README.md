# intermediate/07 — a producer on a new schema and a consumer on an old one, in VB.NET

Two producers on two versions of the same Avro record, three readers on the same
fanout, and the same field added twice — once correctly and once not.

The same example exists in C# at
[intermediate/07-schema-evolution-csharp](../07-schema-evolution-csharp).

## What it shows

- **Producer deployed first.** A reader that has never heard of `currency` reads
  a message containing it, and the field is *skipped* rather than shifting every
  byte after it.
- **Consumer deployed first.** A reader already on the new schema reads a message
  from a producer that is not, and its own default fills the gap — so the new
  code can be written as though the field were always there.
- **The identifier on the wire**, in the framing Confluent's clients and the Java
  library both use.
- **The same addition without a default is refused**, which is what makes the
  rule a rule.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/07-schema-evolution-vbnet
```

## What to look for

```
registry    2 schemas, ids issued as they were first written
wire format application/vnd.acemq.avro, 0x00 then a schema id, then the Avro body
ids         v1 producer wrote id 1, v2 producer wrote id 2

reader   message  id     total  currency
v1       from v1   o-1      100  (not in my schema)
v1       from v2   o-2      200  (not in my schema)
v2       from v1   o-1      100  GBP
v2       from v2   o-2      200  EUR

no default  refused: True
```

**`v1 from v2 … 200`** is the line that matters most. A reader without the
writer's schema would not skip the field it does not know; it would read the
bytes of `currency` as whatever it expected next and be wrong about everything
after that. Avro will not always notice.

## Why a registry at all

Avro messages are not self-describing. A reader must already hold the schema the
writer used, or the bytes cannot be read — that is the whole design difference
from JSON, and it is why the codec is constructed *with* a schema rather than
created empty.

`AvroCodec.Of(schema)` fixes one schema for the codec's life. Small, fast and
nothing extra to run, and the writer's schema is whatever the reader happens to
have compiled in — sound only where producer and consumer are released together.

`AvroCodec.Registered(registry, schema)` writes the schema's identifier into the
front of every message, so a reader can look up exactly what the writer used and
let Avro resolve it against its own. The schema is kilobytes and every message
would pay for it; the id is four bytes and the consumer resolves it once.

**The schema you pass is the reader's.** That is what makes resolution happen: on
the way out it is the writer's schema, and on the way back it is what the
writer's schema is resolved *into*.

## Always give a new field a default

```json
{"name":"currency","type":"string","default":"GBP"}
```

The default is the whole of the compatibility. It is what a reader on the new
schema puts in the field when the writer did not send one. Add the same field
without one and there is nothing Avro can put there, so the read fails — which in
a deployment means every consumer breaking the moment it is rolled out ahead of
the producers.

The library raises that as fatal rather than retryable, so the message is
dead-lettered instead of going round a loop: a schema mismatch fails identically
every time, and five retries only delay whoever has to look at it.

## The framing

One zero byte, then four bytes of identifier, big-endian, then the Avro body —
the layout Confluent's clients use and the same bytes the Java library writes.
Messages written here can be read by either. The third reader in this example
takes the body as `Byte()` so the frame can be shown rather than described.

The content type is `application/vnd.acemq.avro` when the schema is registered
and `avro/binary` when it is fixed, and the two are not interchangeable: a codec
built with `Registered(...)` reads messages that carry a frame, and a message
written with `Of(...)` has none.

## Two things about the registry in this example

**A codec belongs to a connection.** The `Publisher(Of T)` overload that takes an
`ICodec` is internal, so a producer writing a different schema is a different
connection — which is why there are three here. Consumers are easier:
`ConsumerOptions.As(codec)` is public and per-consumer.

**`InMemorySchemaRegistry` is per process.** Ids are handed out in the order
schemas are registered, so two processes disagree about what an id means and a
restart renumbers everything. That makes it right for a test and for one process
reading its own messages — which is exactly what this example is — and wrong for
anything where a message outlives the process that wrote it. Implement
`ISchemaRegistry` against your database or a Confluent-compatible registry for
that; nothing else in the example changes.

## Four things VB.NET makes you write differently

**Never write `Avro.Schema`.** `Imports AceMq.Amqp` brings the nested namespace
`AceMq.Amqp.Avro` into scope under the simple name `Avro`, and VB.NET is
case-insensitive on top of that, so the compiler cannot tell which `Avro` is
meant. `Imports Avro` and `Imports Avro.Generic` at the top, then `Schema`,
`RecordSchema` and `GenericRecord` unqualified, and the ambiguity never arises.
The same trap exists in C#; it is worse here only because the case rule removes
one more way out.

**`FieldValue`, not `Field`.** `Avro.Field` is a type, and a function called
`Field` shadows it.

**`wire`, not `bodies`.** A local called `bodies` hides the module's `Bodies`
function — and the error names the local rather than the clash, which costs
twenty minutes if nobody has written it down.

**No raw string literals.** An Avro schema in VB.NET is doubled quotes and joined
lines, which is a good argument for keeping schemas in `.avsc` resource files and
reading them at startup rather than writing them out the way this example does.

## How this stays honest

Every message goes over a real broker through a fanout, so the resolution being
demonstrated is the one that happens on the wire rather than one arranged in
memory. The example asserts that the v1 reader sees no `currency` **and still
reads `total` as 200**, that the v2 reader sees `GBP` from a v1 producer and
`EUR` from a v2 one, that the two producers wrote different schema ids and that
each id resolves to the schema its producer was on, that the frame starts with
the magic zero byte, that the content type is the registered one, and that a
reader on the no-default schema refuses an old message rather than reading it. It
exits non-zero if any of them is wrong.
