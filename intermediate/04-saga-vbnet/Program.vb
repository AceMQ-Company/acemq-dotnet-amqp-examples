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

' Three services, no shared transaction, and what happens when the third says
' no, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project intermediate/04-saga-vbnet
'
' Reserving stock, taking a payment and booking a courier are three services
' with three databases. There is no transaction across them, so "roll it back"
' is not something a database can be asked to do -- it has to be done by
' running the opposite of each step that succeeded, in reverse.
'
' The saga runs three times: once where everything works, once where the
' courier refuses and the earlier steps are undone, and once where undoing
' itself fails. The third is the one worth reading.

Imports System.Collections.Generic
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Order
    Public Property OrderId As String = ""
End Class

Module Program

    ' Distinct from the C# example's names: both run against one broker on CI,
    ' and two examples declaring one name with different settings is a
    ' PRECONDITION_FAILED rather than a coincidence.
    Private Const Events As String = "saga-order-events-vb"
    Private Const Ledger As String = "saga-ledger-vb"

    ' The steps below publish, and a lambda handed to Saga takes only the
    ' subject -- so the connection is reached as a field rather than captured.
    ' Named "broker" rather than "mq" only for readability; the case-sensitivity
    ' traps in this file are Order and Envelope, not this one.
    Private Broker As AceMqConnection = Nothing

    Private ReadOnly Recorded As New List(Of String)

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(60))
            Dim token = cancellation.Token

            Broker = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/04-saga-vbnet") _
                    .Build())

            Try
                Await Broker.DeclareExchangeAsync(Events, "topic")
                Await Broker.DeclareQueueAsync(Ledger)
                Await Broker.BindAsync(Ledger, Events, "#")

                ' Every step and every compensation announces itself, so what
                ' the saga did is visible on the broker rather than only in a
                ' return value. A saga is not a message pattern -- nothing in
                ' Saga(Of T) publishes anything -- but the steps of a real one
                ' almost always do, and the order they arrive in is the point.
                Using ledger = Await Broker.ConsumeAsync(Of Order)(
                    Program.Ledger,
                    Function(message)
                        SyncLock Recorded
                            Recorded.Add(If(message.RoutingKey, "?"))
                        End SyncLock
                        Return Task.FromResult(Ack.Accept())
                    End Function)

                    ' ---- 1. everything works --------------------------------
                    Dim booking = Saga(Of Order).Named("place-order") _
                        .Step("reserve-stock", Announces("stock.reserved")) _
                            .CompensateWith(Announces("stock.released")) _
                        .Step("take-payment", Announces("payment.taken")) _
                            .CompensateWith(Announces("payment.refunded")) _
                        .Step("book-courier", Announces("courier.booked")) _
                        .Build()
                    ' book-courier has no compensation, and legitimately so: it
                    ' is the last step, so nothing after it can fail and ask for
                    ' it back. A step that cannot be undone -- sending an email,
                    ' printing a label -- belongs last, after everything that can
                    ' still go wrong.

                    Dim happy = Await booking.RunAsync(New Order With {.OrderId = "ORD-1"}, token)
                    Dim sawHappy = Await Settled(3, token)

                    Console.WriteLine(
                        $"complete={happy.IsComplete} steps=[{String.Join(", ", happy.Completed)}]")
                    Console.WriteLine($"  the broker saw: [{String.Join(", ", sawHappy)}]")
                    Check(happy.IsComplete, "the saga did not complete")
                    Check(String.Join(",", happy.Completed) = "reserve-stock,take-payment,book-courier",
                          $"the completed steps were [{String.Join(", ", happy.Completed)}]")
                    Check(String.Join(",", sawHappy) = "stock.reserved,payment.taken,courier.booked",
                          $"the broker saw [{String.Join(", ", sawHappy)}]")

                    ' ---- 2. the courier refuses -----------------------------
                    '
                    ' Compensations run backwards, newest first, because the
                    ' later steps are the ones built on the earlier ones -- and a
                    ' compensation often depends on state a later step has not
                    ' yet altered.
                    Dim refused = Saga(Of Order).Named("place-order") _
                        .Step("reserve-stock", Announces("stock.reserved")) _
                            .CompensateWith(Announces("stock.released")) _
                        .Step("take-payment", Announces("payment.taken")) _
                            .CompensateWith(Announces("payment.refunded")) _
                        .Step("book-courier", Refuses("no courier covers that postcode today")) _
                        .Build()

                    Dim unhappy = Await refused.RunAsync(New Order With {.OrderId = "ORD-2"}, token)
                    Dim sawUnhappy = Await Settled(4, token)

                    Console.WriteLine(
                        $"complete={unhappy.IsComplete} compensated={unhappy.Compensated} " &
                        $"failedAt={unhappy.FailedAt} because {unhappy.Failure?.Message}")
                    Console.WriteLine($"  the broker saw: [{String.Join(", ", sawUnhappy)}]")
                    Check(unhappy.Compensated, "a refused saga did not compensate")
                    Check(unhappy.FailedAt = "book-courier", $"it failed at {unhappy.FailedAt}")
                    Check(Not unhappy.HasUnresolved,
                          "something was left unresolved that should not have been")
                    Check(String.Join(",", sawUnhappy) =
                              "stock.reserved,payment.taken,payment.refunded,stock.released",
                          $"the broker saw [{String.Join(", ", sawUnhappy)}]")

                    ' ---- 3. the undo itself fails ---------------------------
                    '
                    ' The row a person has to look at. The warehouse has already
                    ' picked the reservation, so releasing it is not something a
                    ' message can do. The saga does not stop -- stopping would
                    ' leave more undone than carrying on -- so the payment is
                    ' still refunded, and what comes back names the one thing
                    ' that was not put right.
                    '
                    ' Nothing else in the system knows about it and no retry will
                    ' resolve it. This list is the thing to alert on.
                    Dim stuck = Saga(Of Order).Named("place-order") _
                        .Step("reserve-stock", Announces("stock.reserved")) _
                            .CompensateWith(
                                Refuses("the warehouse will not release a picked reservation")) _
                        .Step("take-payment", Announces("payment.taken")) _
                            .CompensateWith(Announces("payment.refunded")) _
                        .Step("book-courier", Refuses("no courier covers that postcode today")) _
                        .Build()

                    Dim half = Await stuck.RunAsync(New Order With {.OrderId = "ORD-3"}, token)
                    Dim sawStuck = Await Settled(3, token)

                    Console.WriteLine($"unresolved=[{String.Join(", ", half.Unresolved)}]")
                    Console.WriteLine($"  the broker saw: [{String.Join(", ", sawStuck)}]")
                    Console.WriteLine($"  {half}")
                    Check(half.HasUnresolved,
                          "a failed compensation was not reported as unresolved")
                    Check(String.Join(",", half.Unresolved) = "reserve-stock",
                          $"the unresolved steps were [{String.Join(", ", half.Unresolved)}]")
                    Check(String.Join(",", sawStuck) = "stock.reserved,payment.taken,payment.refunded",
                          $"the broker saw [{String.Join(", ", sawStuck)}]")
                    Check(Not sawStuck.Contains("stock.released"),
                          "the stock was released after all, which is not what the compensation did")
                End Using

                ' A saga is not a distributed transaction and does not pretend to
                ' be. After take-payment the customer's money really has moved
                ' and anybody looking sees that it has; the refund is a new fact
                ' rather than an erasure of the old one. For a moment the world
                ' contained a charge that should not have happened. That is what
                ' compensating a real action means.
                '
                ' It is also not durable: this runs in one process with its state
                ' on the stack, so a crash midway leaves the saga half-applied
                ' with nothing to resume it.

                ' The example cleans up after itself so a second run reports the
                ' same numbers as the first. A service would leave the topology
                ' alone.
                Await Broker.DeleteQueueAsync(Ledger)
                Await Broker.DeleteQueueAsync(Ledger & ".dlq")
                Await Broker.DeleteQueueAsync(Ledger & ".parked")
                Await Broker.DeleteExchangeAsync(Events)
            Finally
                Broker.Dispose()
            End Try

            Return 0
        End Using
    End Function

    ' A step that publishes an event and succeeds.
    Private Function Announces(key As String) As Func(Of Order, Task)
        Return Function(placed As Order) Publish(key, placed)
    End Function

    ' A step that fails. Task.FromException rather than a lambda that throws:
    ' a VB lambda declared As Task must produce one, and an Async lambda with no
    ' Await in it is a warning -- which this repository builds as an error.
    Private Function Refuses(why As String) As Func(Of Order, Task)
        Return Function(placed As Order) Task.FromException(New InvalidOperationException(why))
    End Function

    Private Async Function Publish(key As String, placed As Order) As Task
        ' Named "stamp" rather than "envelope": VB.NET is case-insensitive, so a
        ' variable called envelope collides with the Envelope type and the
        ' compiler reports it as a type it cannot infer rather than a name clash.
        Dim stamp = Envelope.Of(key).Build()
        Await Broker.Publisher(Of Order)(Events, key).SendAsync(placed, stamp)
    End Function

    ' Waits for the broker to have delivered everything the saga published, then
    ' takes the list and empties it for the next run.
    Private Async Function Settled(expected As Integer, token As CancellationToken) _
            As Task(Of List(Of String))
        For i = 0 To 199
            SyncLock Recorded
                If Recorded.Count >= expected Then Exit For
            End SyncLock
            Await Task.Delay(50, token)
        Next

        SyncLock Recorded
            Dim seen As New List(Of String)(Recorded)
            Recorded.Clear()
            Check(seen.Count = expected,
                  $"expected {expected} message(s), saw [{String.Join(", ", seen)}]")
            Return seen
        End SyncLock
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
