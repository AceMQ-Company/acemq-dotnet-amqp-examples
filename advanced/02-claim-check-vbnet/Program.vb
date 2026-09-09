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

' Keeping a large payload off the broker, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project advanced/02-claim-check-vbnet
'
' A scanned report is tens of megabytes. Putting it on a queue is possible and
' is a mistake: it fills the broker's memory, it is copied to every bound queue,
' it makes a dead-letter queue impossible to inspect, and it turns a broker into
' a filesystem with worse tools. What travels instead is a claim check -- the
' payload goes to a store, and the message carries the key.
'
' Two documents are published here, one either side of the threshold, because
' the threshold is the whole point and it is invisible if every message is
' large. Both are read by one consumer that is not told which is which.

Imports System.Collections.Generic
Imports System.IO
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Document
    Public Property DocumentId As String = ""
    Public Property Body As String = ""
End Class

Module Program

    ' Distinct from the C# example's names: both run against one broker on CI.
    Private Const Destination As String = "claim-check-documents-vb"
    Private Const Readers As String = "claim-check-readers-vb"
    Private Const Wire As String = "claim-check-wire-vb"

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            ' A filesystem store rather than the in-memory one, because a claim
            ' check that does not outlive the process that wrote it is a message
            ' nobody else can read. InMemoryClaimCheckStore holds the payloads in
            ' the publisher's heap -- which is where they were going to be
            ' anyway, so it takes them off the broker and does nothing else. It
            ' is genuinely useful in a test, where publisher and consumer are one
            ' process and the framing is what is being proved.
            '
            ' This one is useful where the filesystem is shared and durable: an
            ' NFS mount, a persistent volume. On a container's local disk it is
            ' the in-memory store with extra steps, since the consumer is on
            ' another host and finds nothing. Object storage is the usual right
            ' answer, and a store in front of S3 or Azure Blob Storage is three
            ' short methods.
            Dim directory = Path.Combine(Path.GetTempPath(), "acemq-claim-check-vbnet")
            Dim store As New FilesystemClaimCheckStore(directory)
            Console.WriteLine($"payloads go to {store.Location}")

            ' The codec wraps another: the payload is encoded as usual, and what
            ' happens to those bytes then depends on how many there are.
            '
            ' Named "framing" rather than "codec" out of the same caution that
            ' names a keyring "ring" elsewhere in this repository: VB.NET is
            ' case-insensitive, and a variable sharing a type's name is reported
            ' as a type that cannot be inferred rather than as a name clash.
            Dim framing = ClaimCheckCodec.Wrapping(New JsonCodec(), store)
            Check(framing.Threshold = ClaimCheckCodec.DefaultThreshold,
                  $"the threshold was {framing.Threshold}")
            Check(ClaimCheckCodec.DefaultThreshold = 65536,
                  $"the default threshold was {ClaimCheckCodec.DefaultThreshold}, not 65536")

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/02-claim-check-vbnet") _
                    .Build(),
                framing,
                token)

            Try
                ' One fanout, two queues, one publish. The first queue is read
                ' the ordinary way and yields Documents; the second is read as
                ' raw bytes, so what actually went on the wire can be printed
                ' rather than described.
                Await mq.DeclareExchangeAsync(Destination, "fanout")
                Await mq.DeclareQueueAsync(Readers)
                Await mq.DeclareQueueAsync(Wire)
                Await mq.BindAsync(Readers, Destination, "")
                Await mq.BindAsync(Wire, Destination, "")

                Dim decoded As New List(Of Document)
                Dim bodies As New List(Of Byte())
                Dim decodedAll As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)
                Dim wireAll As New TaskCompletionSource(Of Boolean)(
                    TaskCreationOptions.RunContinuationsAsynchronously)
                Dim checkedContentType As String = Nothing

                ' The consumer is not told which document was offloaded. The
                ' framing says which of the two it is, and that is what allows
                ' the threshold to be changed, or this codec to be introduced,
                ' without a flag day.
                Using typed = Await mq.ConsumeAsync(Of Document)(
                    Readers,
                    Function(message)
                        SyncLock decoded
                            decoded.Add(message.Payload)
                            If decoded.Count = 2 Then decodedAll.TrySetResult(True)
                        End SyncLock
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    Using raw = Await mq.ConsumeAsync(Of Byte())(
                        Wire, ConsumerOptions.Defaults().As(New BytesCodec()),
                        Function(message)
                            SyncLock bodies
                                bodies.Add(message.Payload)
                                If ClaimCheckCodec.IsClaimCheck(message.Payload) Then
                                    checkedContentType = message.ContentType
                                End If
                                If bodies.Count = 2 Then wireAll.TrySetResult(True)
                            End SyncLock
                            Return Task.FromResult(Ack.Accept())
                        End Function)

                        ' A small document, which travels inline. Offloading a
                        ' two-hundred-byte message turns one broker round trip
                        ' into a store round trip AND a broker round trip, so an
                        ' unconditional claim check makes the common case slower
                        ' in order to fix the rare one.
                        Dim small As New Document With {
                            .DocumentId = "doc-small", .Body = "a note"}

                        ' And one of exactly 65536 encoded bytes, which is the
                        ' boundary itself. The comparison is strictly-less-than
                        ' -- encoded.Length < threshold travels inline -- so a
                        ' payload of exactly the threshold is offloaded. The
                        ' padding is measured rather than guessed: encode the
                        ' document with an empty body, and make up the difference
                        ' in characters that are one byte each in both UTF-8 and
                        ' JSON.
                        Dim probe = framing.Delegate.Encode(
                            New Document With {.DocumentId = "doc-large", .Body = ""})
                        Dim large As New Document With {
                            .DocumentId = "doc-large",
                            .Body = New String("x"c, ClaimCheckCodec.DefaultThreshold - probe.Length)}
                        Check(framing.Delegate.Encode(large).Length = ClaimCheckCodec.DefaultThreshold,
                              "the large document is not exactly the threshold, " &
                              "so it proves nothing about it")

                        Dim publisher = mq.Publisher(Of Document)(Destination, "")
                        Await publisher.SendAsync(small)
                        Await publisher.SendAsync(large)

                        Await Task.WhenAll(decodedAll.Task.WaitAsync(token),
                                           wireAll.Task.WaitAsync(token))

                        ' ---- what went on the wire ------------------------
                        '
                        '   0xAC 0x01 0x00  payload   inline, identical to JSON
                        '   0xAC 0x01 0x01  key       a claim check, UTF-8
                        '
                        ' Byte for byte what the Java, Python and Ruby libraries
                        ' write.
                        Dim inlineBody As Byte()
                        Dim checkedBody As Byte()
                        SyncLock bodies
                            inlineBody = bodies.Single(
                                Function(b) Not ClaimCheckCodec.IsClaimCheck(b))
                            checkedBody = bodies.Single(
                                Function(b) ClaimCheckCodec.IsClaimCheck(b))
                        End SyncLock

                        Console.WriteLine(
                            $"inline:  {Hex(inlineBody)} + {inlineBody.Length - 3} " &
                            "bytes of payload")
                        Console.WriteLine(
                            $"checked: {Hex(checkedBody)} + {checkedBody.Length - 3} " &
                            "bytes naming the payload")

                        Check(inlineBody(0) = &HAC AndAlso inlineBody(1) = &H1 AndAlso
                              inlineBody(2) = &H0,
                              "the inline message is not framed as inline")
                        Check(checkedBody(0) = &HAC AndAlso checkedBody(1) = &H1 AndAlso
                              checkedBody(2) = &H1,
                              "the offloaded message is not framed as a claim check")
                        Check(inlineBody.Length = framing.Delegate.Encode(small).Length + 3,
                              "the inline body is not the JSON plus a three-byte header")
                        Check(ClaimCheckCodec.KeyOf(inlineBody) Is Nothing,
                              "an inline message named a key, which it has no business doing")

                        ' The content type is the delegate's, unchanged. Unlike
                        ' encryption, where the bytes really are something else,
                        ' a claim-checked message is still a document -- it is a
                        ' document that is somewhere else, and a consumer without
                        ' the store gets a clear failure rather than a parser
                        ' error.
                        Console.WriteLine($"content type of the claim check: {checkedContentType}")
                        Check(checkedContentType = "application/json",
                              $"the claim check arrived as {checkedContentType}")

                        ' ---- the key on the wire is the file on disk -------
                        Dim key = ClaimCheckCodec.KeyOf(checkedBody)
                        Check(key IsNot Nothing, "the offloaded message carried no key")

                        Dim onDisk = System.IO.Directory.GetFiles(store.Location)
                        Console.WriteLine(
                            $"the store holds {onDisk.Length} payload(s): " &
                            Path.GetFileName(onDisk(0)))
                        Check(onDisk.Length = 1,
                              $"the store holds {onDisk.Length} payloads: " &
                              "the small one was offloaded too")
                        Check(Path.GetFileName(onDisk(0)) = key,
                              "the file on disk is not the key that was on the wire")
                        Check(New FileInfo(onDisk(0)).Length = ClaimCheckCodec.DefaultThreshold,
                              "the stored payload is not the document that was offloaded")

                        ' Redeemed through a store built from nothing but the
                        ' directory, which is the point of a filesystem store and
                        ' the thing the in-memory one cannot do: this object never
                        ' saw the publish. In a real deployment it is in another
                        ' process, on another host.
                        Dim elsewhere As New FilesystemClaimCheckStore(directory)
                        Dim redeemed = elsewhere.Get(key)
                        Check(redeemed IsNot Nothing AndAlso
                              redeemed.Length = ClaimCheckCodec.DefaultThreshold,
                              "a store that did not write the payload could not read it back")

                        ' ---- and what the consumer saw --------------------
                        Dim documents As List(Of Document)
                        SyncLock decoded
                            documents = decoded.OrderBy(Function(d) d.DocumentId).ToList()
                        End SyncLock

                        For Each item In documents
                            Console.WriteLine(
                                $"read {item.DocumentId}: {item.Body.Length} characters")
                        Next

                        Check(documents.Count = 2, $"{documents.Count} documents arrived, not two")
                        Check(documents(0).DocumentId = "doc-large" AndAlso
                              documents(0).Body = large.Body,
                              "the offloaded document did not come back whole")
                        Check(documents(1).DocumentId = "doc-small" AndAlso
                              documents(1).Body = small.Body,
                              "the inline document did not come back whole")
                    End Using
                End Using

                ' RETENTION IS THE PART THAT GOES WRONG, and this example is
                ' about to do the exact thing a deployment must not. The store
                ' and the queue have different lifetimes and nothing enforces a
                ' relationship between them: a message replayed a month later
                ' carries a key, and if the store expired that key the replay
                ' produces a message nobody can read -- worse than a lost
                ' message, because it looks like a message and fails deep inside
                ' a consumer rather than visibly.
                '
                ' So a store's retention has to outlast every retention that
                ' could bring a message back: queue TTLs, dead-letter queues, and
                ' however long somebody might sit on a message before replaying
                ' it by hand. When in doubt, longer. The directory is removed
                ' here only so a second run reports the same numbers as the
                ' first.
                For Each queue In New String() {Readers, Wire}
                    Await mq.DeleteQueueAsync(queue)
                    Await mq.DeleteQueueAsync(queue & ".dlq")
                    Await mq.DeleteQueueAsync(queue & ".parked")
                Next
                Await mq.DeleteExchangeAsync(Destination)
                System.IO.Directory.Delete(directory, True)
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Function Hex(body As Byte()) As String
        Return String.Join(" ", body.Take(3).Select(Function(b) "0x" & b.ToString("X2")))
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
