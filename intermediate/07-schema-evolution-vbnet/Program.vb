' Copyright 2026 AceMQ.
'
' Licensed under the Apache License, Version 2.0 (the "License");
' you may not use this file except in compliance with the License.
' You may obtain a copy of the License at
'
'     https://www.apache.org/licenses/LICENSE-2.0
'
' Unless required by applicable law or agreed to in writing, software
' distributed under the License is distributed on an "AS IS" BASIS,
' WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
' See the License for the specific language governing permissions and
' limitations under the License.

' A producer on a new schema and a consumer on an old one, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/07-schema-evolution-vbnet
'
' Avro messages are not self-describing: a reader must already hold the schema
' the writer used, or the bytes cannot be read. That is the whole design
' difference from JSON, and it is why a registry exists -- the message carries a
' four-byte identifier and the reader looks the schema up.
'
' Adding a field is the change every service makes eventually, and it is only
' safe in one direction at a time unless the readers can resolve the writer's
' schema against their own. Both directions run here, over a real broker: a
' producer deployed ahead of its consumers, and a consumer deployed ahead of its
' producers.

Imports System.Collections.Generic
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.Avro
Imports AceMq.Amqp.RabbitMq

' Imported so the types can be named unqualified. Writing Avro.Schema would be
' ambiguous: Imports AceMq.Amqp brings the nested namespace AceMq.Amqp.Avro into
' scope under the simple name Avro, and VB.NET is case-insensitive on top of
' that, so the compiler cannot tell which one is meant.
Imports Avro
Imports Avro.Generic

Module Program

    Private Const Subject As String = "acemq.examples.OrderPlaced"

    ' What the producers were writing last month. VB.NET has no raw string
    ' literal, so the quotes are doubled and the lines are joined -- which is
    ' also why an Avro schema in VB is usually read from a resource file rather
    ' than written out like this.
    Private Const V1 As String =
        "{""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.examples""," &
        """fields"":[" &
        "{""name"":""id"",""type"":""string""}," &
        "{""name"":""total"",""type"":""int""}]}"

    ' The same record with a currency added, and a default for it. The default is
    ' the whole of the compatibility: it is what a reader on this schema puts in
    ' the field when the writer did not send one.
    Private Const V2 As String =
        "{""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.examples""," &
        """fields"":[" &
        "{""name"":""id"",""type"":""string""}," &
        "{""name"":""total"",""type"":""int""}," &
        "{""name"":""currency"",""type"":""string"",""default"":""GBP""}]}"

    ' The same addition done wrong: a new field with nothing to fall back on.
    Private Const V2NoDefault As String =
        "{""type"":""record"",""name"":""OrderPlaced"",""namespace"":""acemq.examples""," &
        """fields"":[" &
        "{""name"":""id"",""type"":""string""}," &
        "{""name"":""total"",""type"":""int""}," &
        "{""name"":""currency"",""type"":""string""}]}"

    ' Distinct from the C# example's names for the usual reason: both run against
    ' one broker on CI.
    Private Const Exchange As String = "dotnet-vbnet-avro-orders"
    Private Const OldReader As String = "dotnet-vbnet-avro-v1-reader"
    Private Const NewReader As String = "dotnet-vbnet-avro-v2-reader"
    Private Const RawReader As String = "dotnet-vbnet-avro-bytes"

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(120))
            Dim token = cancellation.Token

            ' One registry, shared by every producer and consumer in this
            ' process. That sharing is the part an in-memory registry cannot give
            ' you across processes: it hands out ids in the order schemas are
            ' registered, so a restart renumbers everything and two services
            ' never agree. It is right for a test and for one process reading its
            ' own messages, and wrong for anything where a message outlives the
            ' process that wrote it -- implement ISchemaRegistry against your
            ' database or a Confluent-compatible registry for that.
            Dim registry As New InMemorySchemaRegistry()

            Dim url = BrokerUrl()

            ' A codec belongs to a connection: the publisher overload that takes
            ' one is internal, so a producer that writes a different schema is a
            ' different connection. Two producers, two connections, and neither
            ' is told anything about the readers.
            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url) _
                    .ClientName("examples/07-schema-evolution-vbnet").Build())
            Dim lastMonth = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url) _
                    .ClientName("examples/07-schema-evolution-vbnet/v1").Build(),
                AvroCodec.Registered(registry, V1), token)
            Dim deployedToday = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(url) _
                    .ClientName("examples/07-schema-evolution-vbnet/v2").Build(),
                AvroCodec.Registered(registry, V2), token)

            Try
                Await mq.DeclareExchangeAsync(Exchange, "fanout")
                For Each name In New String() {OldReader, NewReader, RawReader}
                    Await mq.DeclareQueueAsync(name)
                    Await mq.BindAsync(name, Exchange, "")
                Next

                Dim seenByOld As New Dictionary(Of String, GenericRecord)
                Dim seenByNew As New Dictionary(Of String, GenericRecord)
                ' In arrival order, which one queue with one consumer preserves:
                ' the first is the message the v1 producer wrote.
                Dim raw As New List(Of Byte())
                Dim contentTypes As New List(Of String)

                ' Each consumer says which schema it was written against. That is
                ' the reader schema, and it is what makes resolution happen: the
                ' codec looks the writer's schema up by the id on the message and
                ' asks Avro to read one into the other.
                Dim oldConsumer = Await mq.ConsumeAsync(Of GenericRecord)(
                    OldReader,
                    ConsumerOptions.Prefetch(1).As(AvroCodec.Registered(registry, V1)),
                    Function(message) Collect(seenByOld, message.Payload))

                Dim newConsumer = Await mq.ConsumeAsync(Of GenericRecord)(
                    NewReader,
                    ConsumerOptions.Prefetch(1).As(AvroCodec.Registered(registry, V2)),
                    Function(message)
                        SyncLock contentTypes
                            contentTypes.Add(If(message.ContentType, "(none)"))
                        End SyncLock
                        Return Collect(seenByNew, message.Payload)
                    End Function)

                ' A third reader that does not decode at all, so the framing can
                ' be shown rather than described.
                Dim rawConsumer = Await mq.ConsumeAsync(Of Byte())(
                    RawReader,
                    ConsumerOptions.Prefetch(1).As(New BytesCodec()),
                    Function(message)
                        SyncLock raw
                            raw.Add(message.Payload)
                        End SyncLock
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                ' The producers publish whatever version they are on.
                Await lastMonth.Publisher(Of GenericRecord)(Exchange, "") _
                    .SendAsync(NewOrder(V1, "o-1", 100, Nothing))
                Await deployedToday.Publisher(Of GenericRecord)(Exchange, "") _
                    .SendAsync(NewOrder(V2, "o-2", 200, "EUR"))

                Await WaitFor(
                    Function() Seen(seenByOld) = 2 AndAlso Seen(seenByNew) = 2 _
                               AndAlso Bodies(raw).Length = 2,
                    token)

                ' Named "wire" rather than "bodies": VB.NET is case-insensitive,
                ' so a local called bodies hides the Bodies function used a few
                ' lines above -- and the error it produces names the local rather
                ' than the clash.
                Dim wire = Bodies(raw)
                Dim magic = MagicOf(wire(0))
                Dim v1Id = IdOf(wire(0))
                Dim v2Id = IdOf(wire(1))

                Console.WriteLine(
                    $"registry    {registry.Count} schemas, ids issued as they were first written")
                Console.WriteLine(
                    $"wire format {contentTypes.First()}, 0x{magic:x2} then a schema id, " &
                    "then the Avro body")
                Console.WriteLine(
                    $"ids         v1 producer wrote id {v1Id}, v2 producer wrote id {v2Id}")
                Console.WriteLine()
                Console.WriteLine("reader   message  id     total  currency")
                Console.WriteLine($"v1       from v1  {Show(seenByOld, "o-1")}")
                Console.WriteLine($"v1       from v2  {Show(seenByOld, "o-2")}")
                Console.WriteLine($"v2       from v1  {Show(seenByNew, "o-1")}")
                Console.WriteLine($"v2       from v2  {Show(seenByNew, "o-2")}")

                ' Producer deployed first. The old reader has never heard of
                ' `currency`, and the field is SKIPPED rather than shifting every
                ' byte after it -- which is what would happen without the
                ' writer's schema, and why a total of 200 arriving intact is the
                ' thing to look at here.
                Check(FieldValue(seenByOld, "o-2", "currency") Is Nothing,
                      "the v1 reader saw a currency, which is not in the schema " &
                      "it was written against")
                Check(Object.Equals(FieldValue(seenByOld, "o-2", "total"), 200),
                      $"the v1 reader read total {FieldValue(seenByOld, "o-2", "total")} " &
                      "out of a v2 message, so the unknown field shifted the bytes after it")

                ' Consumer deployed first. The producer is not sending `currency`
                ' yet and the reader's default fills it in, so the new code can
                ' be written as though the field were always there.
                Check(Object.Equals(FieldValue(seenByNew, "o-1", "currency"), "GBP"),
                      $"the v2 reader saw currency {FieldValue(seenByNew, "o-1", "currency")} " &
                      "on a v1 message, not the default its own schema declares")
                Check(Object.Equals(FieldValue(seenByNew, "o-2", "currency"), "EUR"),
                      "the v2 reader did not see the currency a v2 producer actually sent")

                Check(Object.Equals(FieldValue(seenByOld, "o-1", "total"), 100) AndAlso
                      Object.Equals(FieldValue(seenByNew, "o-1", "total"), 100),
                      "a reader disagreed with the other about what a v1 message says")

                ' The framing, which is Confluent's and the same bytes the Java
                ' library writes: one zero byte, then four bytes of identifier,
                ' big-endian, then the Avro body. A message written here can be
                ' read by either.
                Check(magic = 0, $"the message began with 0x{magic:x2}, not the magic zero byte")
                Check(v1Id <> v2Id,
                      $"both producers wrote schema id {v1Id}, " &
                      "so the id does not identify the schema")
                Check(registry.SchemaFor(v1Id).Subject = Subject AndAlso
                      registry.SchemaFor(v2Id).Subject = Subject,
                      $"an id resolved to something other than {Subject}")
                Check(registry.SchemaFor(v1Id).Format = "avro",
                      $"schema id {v1Id} is registered as {registry.SchemaFor(v1Id).Format}, not avro")
                Check(Not registry.SchemaFor(v1Id).Definition.Contains("currency") AndAlso
                      registry.SchemaFor(v2Id).Definition.Contains("currency"),
                      "the ids on the wire do not point at the schemas the producers were on")
                Check(contentTypes.All(Function(type) type = AvroCodec.RegisteredContentType),
                      $"a message arrived as {contentTypes.First()}, " &
                      $"not {AvroCodec.RegisteredContentType}")

                ' And the rule all of this rests on. Add the same field without a
                ' default and there is nothing Avro can put in it when an old
                ' producer omits it, so the read fails -- which in a deployment
                ' means every consumer breaking the moment it is rolled out ahead
                ' of the producers. It fails identically every time, so the
                ' library raises it as fatal and the message is dead-lettered
                ' rather than retried round a loop.
                Dim refused = Refuses(registry, wire(0))
                Console.WriteLine()
                Console.WriteLine($"no default  refused: {refused}")
                Check(refused,
                      "a reader on a schema whose new field has no default read an old " &
                      "message anyway, which would make the incompatible change look safe")

                Check(registry.Count = 2,
                      $"{registry.Count} schemas were registered, " &
                      "not the two the producers write")

                oldConsumer.Dispose()
                newConsumer.Dispose()
                rawConsumer.Dispose()

                For Each name In New String() {OldReader, NewReader, RawReader}
                    Await mq.DeleteQueueAsync(name)
                    Await mq.DeleteQueueAsync(name & ".dlq")
                    Await mq.DeleteQueueAsync(name & ".parked")
                Next
                Await mq.DeleteExchangeAsync(Exchange)
            Finally
                deployedToday.Dispose()
                lastMonth.Dispose()
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    ' Named NewOrder rather than Order: Order is a query keyword in VB.NET.
    Private Function NewOrder(
        schemaJson As String, id As String, total As Integer, currency As String) As GenericRecord

        Dim record As New GenericRecord(CType(Schema.Parse(schemaJson), RecordSchema))
        record.Add("id", id)
        record.Add("total", total)
        If currency IsNot Nothing Then record.Add("currency", currency)
        Return record
    End Function

    Private Function Collect(
        into As Dictionary(Of String, GenericRecord), order As GenericRecord) As Task(Of Ack)

        SyncLock into
            into(CStr(order("id"))) = order
        End SyncLock
        Return Task.FromResult(Ack.Accept())
    End Function

    ''' <summary>What a reader ended up with, or Nothing when the field is not in its schema.</summary>
    ''' <remarks>
    ''' Named FieldValue rather than Field: Avro.Field is a type, and VB.NET is
    ''' case-insensitive, so a function called Field shadows it.
    ''' </remarks>
    Private Function FieldValue(
        seen As Dictionary(Of String, GenericRecord), id As String, name As String) As Object

        SyncLock seen
            Dim order As GenericRecord = Nothing
            If Not seen.TryGetValue(id, order) Then Return Nothing

            Dim value As Object = Nothing
            If Not order.TryGetValue(name, value) Then Return Nothing
            Return value
        End SyncLock
    End Function

    Private Function Refuses(registry As ISchemaRegistry, writtenByV1 As Byte()) As Boolean
        Try
            AvroCodec.Registered(registry, V2NoDefault).Decode(writtenByV1, GetType(GenericRecord))
            Return False
        Catch e As AceFatalException
            Return True
        End Try
    End Function

    Private Function MagicOf(body As Byte()) As Byte
        Return body(0)
    End Function

    Private Function IdOf(body As Byte()) As Integer
        Return (CInt(body(1)) << 24) Or (CInt(body(2)) << 16) Or
               (CInt(body(3)) << 8) Or CInt(body(4))
    End Function

    Private Function Bodies(raw As List(Of Byte())) As Byte()()
        SyncLock raw
            Return raw.ToArray()
        End SyncLock
    End Function

    Private Function Show(seen As Dictionary(Of String, GenericRecord), id As String) As String
        Dim currency = FieldValue(seen, id, "currency")
        Return " " & id.PadRight(5) & "  " &
               CStr(FieldValue(seen, id, "total")).PadLeft(5) & "  " &
               If(currency Is Nothing, "(not in my schema)", CStr(currency))
    End Function

    Private Function Seen(Of TValue)(records As Dictionary(Of String, TValue)) As Integer
        SyncLock records
            Return records.Count
        End SyncLock
    End Function

    Private Async Function WaitFor(held As Func(Of Boolean), token As CancellationToken) As Task
        While Not held()
            token.ThrowIfCancellationRequested()
            Await Task.Delay(25, token)
        End While
    End Function

    ' An example that prints the right answer whatever happened is an example
    ' that cannot fail, and CI running it proves nothing. This throws, which
    ' makes the process exit non-zero.
    Private Sub Check(held As Boolean, wrong As String)
        If Not held Then Throw New InvalidOperationException(wrong)
    End Sub

    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5672/"
        End If
        Return url
    End Function

End Module
