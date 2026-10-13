# basic/06 — JSON and XML read off one queue, in VB.NET

One consumer reading a queue that two publishers write to in two formats — one
already speaking JSON, one not yet migrated off XML — and decoding each message
by the content type it carries rather than by guessing, which is what a format
migration looks like while both ends are still in flight.

The same example exists in C# at
[basic/06-serialization-csharp](../06-serialization-csharp).

## What it shows

- **A codec belongs to a connection.** Everything published on it is encoded
  the same way, and everything consumed is read the same way.
- **`CompositeCodec.Of(...)` reads several formats.** It decodes by the message's
  content type, and encodes with the first codec it was given — so the order of
  the arguments is the format that connection publishes.
- **The content type travels with the message.** The handler sees
  `message.ContentType`, and nothing in it branches on the format.
- **JSON is the default.** A connection opened without a codec speaks JSON; XML
  is asked for with `New XmlCodec()`.

## Running it

```bash
docker compose up -d
dotnet run --project basic/06-serialization-vbnet
```

## What to look for

```
  read o-json (1999) sent as application/json
both formats read off one queue: o-json, o-xml
  read o-xml (4500) sent as application/xml
codecs registered here: bytes, json, string, text, xml
```

**Two content types, one handler, the same `Order`.** Neither line needed the
handler to know which format it was reading.

The summary line came out before the second `read` line on this run, and on
another it will not: the handler records the order and signals before it prints,
so the two writes race. The order of the list in the summary is the order the
messages were handled.

The queue is `serialization-orders-vb`, not the C# twin's
`serialization-orders`, so the two can run against one broker without reading
each other's orders.

## The order of the arguments is a decision

```vb
CompositeCodec.Of(New JsonCodec(), New XmlCodec())
```

Decoding goes by content type, so either order reads both. Encoding goes to the
first, so anything this connection publishes goes out as JSON. Swapping them
would change what it writes without changing what it can read.

## What else is available

`codecs registered here` lists what this process can encode and decode. JSON and
XML are in the main package; YAML, TOML, Protocol Buffers and Avro are packages
of their own, so a service pays for the formats it uses and nothing else.

## How this stays honest

The program waits at most sixty seconds for both orders, and a wait that runs out
throws, so an XML message the composite could not decode is a non-zero exit
rather than one line missing from the output. It deletes its queue at the end,
so a second run reports the same thing as the first. CI runs it against a real
broker on every push.

## In the documentation

- [Serialization — changing format without stopping](https://acemq-company.github.io/acemq-dotnet-amqp/serialization.html#changing-format-without-stopping)
- [Serialization — XML](https://acemq-company.github.io/acemq-dotnet-amqp/serialization.html#xml)
- [Serialization — what ships](https://acemq-company.github.io/acemq-dotnet-amqp/serialization.html#what-ships)
- [VB.NET — publishing and consuming in VB](https://acemq-company.github.io/acemq-dotnet-amqp/vbnet.html#publishing-and-consuming-in-vb)
