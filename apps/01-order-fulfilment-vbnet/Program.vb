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

' Order fulfilment: five services, one broker, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project apps/01-order-fulfilment-vbnet
'
' In production these are five deployments. Here they run in one process
' against one real RabbitMQ, which exercises every queue, every hop and every
' failure path -- and fails if any service stopped agreeing with the contracts.
'
' Four orders go through, each with a freshly started system, the way the Java
' app's system test runs them: one that succeeds, one where the warehouse is
' flaky, one over the payment limit, and one where stock runs out. Each of them
' checks its own claims, and the process exits non-zero if any did not hold.

Imports System.Collections.Generic
Imports System.IO
Imports System.Linq
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq
Imports Microsoft.Data.Sqlite

Module Program

    ' How long the system gets to reach the state a scenario waits for. Every one
    ' of them gets there in well under a second; this is the bound on a run that
    ' has gone wrong, so CI fails rather than hangs.
    Private ReadOnly Patience As TimeSpan = TimeSpan.FromSeconds(60)

    Private ReadOnly DatabaseFiles As New List(Of String)()

    Function Main() As Integer
        Return RunAllAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAllAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())
        Dim url = BrokerUrl()

        Dim notHeld = 0
        Try
            notHeld += Await RunAsync(url, "an order travels through every service",
                                      AddressOf AnOrderTravelsThroughEveryService)
            notHeld += Await RunAsync(url, "a flaky warehouse is retried rather than failed",
                                      AddressOf AFlakyWarehouseIsRetried)
            notHeld += Await RunAsync(url, "an order over the limit stops at payments",
                                      AddressOf AnOrderOverTheLimitStopsAtPayments)
            notHeld += Await RunAsync(url, "there is not enough stock, and retrying would not help",
                                      AddressOf ThereIsNotEnoughStock)

            ' An example that leaves its queues behind changes the next run's
            ' numbers. A real system would leave them alone. The last thing the Try
            ' does rather than a Finally, because VB.NET cannot Await in one.
            Await ForgetEarlierRunsAsync(url)
        Finally
            SqliteConnection.ClearAllPools()
            For Each leftover In DatabaseFiles
                File.Delete(leftover)
            Next
        End Try

        Console.WriteLine(If(notHeld = 0, "all four held", $"{notHeld} of four did not hold"))
        Return If(notHeld = 0, 0, 1)
    End Function

    ' ---- the four orders --------------------------------------------------------

    Private Async Function AnOrderTravelsThroughEveryService(running As TheSystem) As Task
        Dim orderId = Await running.Gateway.PlaceOrderAsync("ada", "WIDGET", 2, 42.0)

        Await WaitFor(Function() running.Shipping.Shipped = 1, "the order to ship")

        ' One order in at the gateway, and every service downstream acted exactly
        ' once.
        Check(running.Payments.Captured = 1, $"payments captured {running.Payments.Captured} times, not once")
        Check(running.Inventory.Reserved = 1, $"inventory reserved {running.Inventory.Reserved} times, not once")
        Check(running.Shipping.Shipped = 1, $"shipping shipped {running.Shipping.Shipped} times, not once")

        ' Stock actually moved. Without this the reservation is a log line.
        Check(running.Inventory.StockOf("WIDGET") = 8, $"{running.Inventory.StockOf("WIDGET")} widgets left, not 8")

        ' And the customer's view is the whole story, assembled from events
        ' published by four services that never spoke to each other. The wait is
        ' for the final count rather than an intermediate one: polling for 3 can
        ' miss the moment the third arrives and the fourth follows.
        Await WaitFor(Function() running.Notifications.TimelineOf(orderId).Count >= 4, "four events in the timeline")
        CheckTimeline(running, orderId, "OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped")

        ' The outbox is empty, so nothing is waiting to be published.
        Dim pending = Await running.Gateway.PendingInOutboxAsync()
        Check(pending = 0, $"{pending} record(s) still in the outbox")
    End Function

    Private Async Function AFlakyWarehouseIsRetried(running As TheSystem) As Task
        running.Inventory.WithFlakyWarehouse(2)

        Dim orderId = Await running.Gateway.PlaceOrderAsync("grace", "WIDGET", 1, 10.0)

        Await WaitFor(Function() running.Shipping.Shipped = 1, "the order to ship")

        ' Two failures, then success. The order was never lost and no human was
        ' involved.
        Check(running.Inventory.Retried >= 2, $"inventory saw {running.Inventory.Retried} retries, not at least 2")
        Check(running.Inventory.Reserved = 1, $"inventory reserved {running.Inventory.Reserved} times, not once")

        ' The Java app asserts only that OrderShipped is in the timeline, read
        ' straight after shipping counted it -- which can race the notification.
        ' Waiting for all four and checking the whole sequence is stricter, and is
        ' where a retry that duplicated StockReserved would show.
        Await WaitFor(Function() running.Notifications.TimelineOf(orderId).Count >= 4, "four events in the timeline")
        CheckTimeline(running, orderId, "OrderPlaced", "PaymentCaptured", "StockReserved", "OrderShipped")
    End Function

    Private Async Function AnOrderOverTheLimitStopsAtPayments(running As TheSystem) As Task
        Dim orderId = Await running.Gateway.PlaceOrderAsync("charles", "WIDGET", 1, 5000.0)

        Await WaitFor(Function() running.Payments.Declined = 1, "payments to decline")

        ' Nothing downstream ran, which is the point of declining before
        ' reserving: stock held for an order that cannot be paid for is stock
        ' nobody releases.
        Check(running.Inventory.Reserved = 0, $"inventory reserved {running.Inventory.Reserved} times, not never")
        Check(running.Shipping.Shipped = 0, $"shipping shipped {running.Shipping.Shipped} times, not never")
        Check(running.Inventory.StockOf("WIDGET") = 10, $"{running.Inventory.StockOf("WIDGET")} widgets left, not 10")

        Await WaitFor(Function() running.Notifications.TimelineOf(orderId).Count = 2, "two events in the timeline")
        CheckTimeline(running, orderId, "OrderPlaced", "PaymentDeclined")
    End Function

    Private Async Function ThereIsNotEnoughStock(running As TheSystem) As Task
        Dim orderId = Await running.Gateway.PlaceOrderAsync("alan", "WIDGET", 99, 99.0)

        Await WaitFor(Function() running.Inventory.Rejected = 1, "inventory to reject")

        ' The money was taken and the stock was not there. In a real system this
        ' is where a refund is triggered. It is deliberately visible rather than
        ' swallowed.
        Check(running.Payments.Captured = 1, $"payments captured {running.Payments.Captured} times, not once")
        Check(running.Shipping.Shipped = 0, $"shipping shipped {running.Shipping.Shipped} times, not never")

        Await WaitFor(Function() running.Notifications.TimelineOf(orderId).Count = 3, "three events in the timeline")
        CheckTimeline(running, orderId, "OrderPlaced", "PaymentCaptured", "StockUnavailable")
    End Function

    ' ---- running one --------------------------------------------------------------

    ' Starts the system, runs one order through it, and checks what every order
    ' must leave behind whatever else it asserts: nothing dead-lettered, nothing
    ' parked, and no order handed to payments twice. A library that lost or
    ' duplicated a message on any of these paths shows up here even when the
    ' scenario's own numbers happen to come out right.
    '
    ' The system is called running rather than system: VB.NET ignores case, so a
    ' local called system hides the System namespace for the whole function.
    Private Async Function RunAsync(url As String, name As String,
                                    scenario As Func(Of TheSystem, Task)) As Task(Of Integer)
        ' Each order starts from an empty broker, so one that left something in a
        ' dead-letter queue is not blamed on the next. And a run that died half way
        ' leaves nothing to be counted as this one's.
        Await ForgetEarlierRunsAsync(url)

        Dim running = Await TheSystem.StartAsync(url, AddressOf FreshDatabase)

        ' Caught and kept, then reported after the system is stopped: stopping is
        ' an Await, and VB.NET cannot Await in a Catch or a Finally.
        Dim failure As Exception = Nothing
        Try
            Await scenario(running)
            Check(running.Payments.DuplicatesRefused = 0,
                  $"payments refused {running.Payments.DuplicatesRefused} duplicate(s) of an order published once")
            For Each queueName In Contract.Queues
                For Each setAside In {queueName & ".dlq", queueName & ".parked"}
                    Dim count = Await running.Inspector.MessageCountAsync(setAside)
                    Check(count = 0, $"{count} message(s) in {setAside}")
                Next
            Next
        Catch e As Exception
            failure = e
        End Try

        Await running.StopAsync()

        If failure IsNot Nothing Then
            Console.WriteLine($"DID NOT   {name}: {failure.Message}")
            Return 1
        End If
        Console.WriteLine($"held      {name}")
        Return 0
    End Function

    Friend NotInheritable Class TheSystem
        Public Property Inspector As AceMqConnection
        Public Property Gateway As GatewayService
        Public Property Payments As PaymentsService
        Public Property Inventory As InventoryService
        Public Property Shipping As ShippingService
        Public Property Notifications As NotificationsService

        Public Shared Async Function StartAsync(url As String,
                                                database As Func(Of String, ConnectionSupplier)) As Task(Of TheSystem)
            ' A sixth connection only looks, for the dead-letter counts. And a
            ' database per service, because services do not share one: the moment
            ' two services read the same table, the deployment boundary is a
            ' fiction.
            Return New TheSystem With {
                .Inspector = Await AceMqConnection.ConnectAsync(url),
                .Gateway = Await GatewayService.StartAsync(url, database("gateway")),
                .Payments = Await PaymentsService.StartAsync(url, database("payments")),
                .Inventory = (Await InventoryService.StartAsync(url)).WithStock("WIDGET", 10),
                .Shipping = Await ShippingService.StartAsync(url),
                .Notifications = Await NotificationsService.StartAsync(url)
            }
        End Function

        ' Downstream first, so nothing is still being published into a service
        ' that has gone. Each close drains its handlers before the connection
        ' goes, so a message in hand is settled rather than redelivered.
        Public Async Function StopAsync() As Task
            Await Notifications.CloseAsync()
            Await Shipping.CloseAsync()
            Await Inventory.CloseAsync()
            Await Payments.CloseAsync()
            Await Gateway.CloseAsync()
            Await Inspector.CloseAsync()
        End Function
    End Class

    ''' <summary>A fresh database per service, per order: a SQLite file nothing else opens.</summary>
    ''' <remarks>
    ''' A file rather than SQLite's shared in-memory mode, which locks per table
    ''' and fails at once rather than waiting -- and the relay polls the outbox
    ''' while an order is being written into it.
    ''' </remarks>
    Private Function FreshDatabase(service As String) As ConnectionSupplier
        Dim file = Path.Combine(Path.GetTempPath(), $"fulfilment-vbnet-{service}-{Guid.NewGuid():N}.db")
        DatabaseFiles.Add(file)
        Return Function() New SqliteConnection($"Data Source={file}")
    End Function

    Private Async Function ForgetEarlierRunsAsync(url As String) As Task
        Dim mq = Await AceMqConnection.ConnectAsync(url)
        Try
            For Each queueName In Contract.Queues
                Await mq.DeleteQueueAsync(queueName)
                Await mq.DeleteQueueAsync(queueName & ".dlq")
                Await mq.DeleteQueueAsync(queueName & ".parked")
            Next
            Await mq.DeleteExchangeAsync(Contract.Exchange)
        Finally
            mq.Dispose()
        End Try
    End Function

    Private Sub CheckTimeline(running As TheSystem, orderId As String, ParamArray expected As String())
        Dim timeline = running.Notifications.TimelineOf(orderId)
        Check(timeline.SequenceEqual(expected),
              $"the timeline of {orderId} was [{String.Join(", ", timeline)}], not [{String.Join(", ", expected)}]")
        Console.WriteLine($"          {orderId}: {String.Join(" -> ", timeline)}")
    End Sub

    Private Async Function WaitFor(done As Func(Of Boolean), what As String) As Task
        Dim deadline = DateTime.UtcNow + Patience
        While Not done()
            If DateTime.UtcNow > deadline Then
                Throw New TimeoutException($"waited {Patience.TotalSeconds:F0}s for {what}")
            End If
            Await Task.Delay(50)
        End While
    End Function

    ' An example that prints the right answer whatever happened is an example
    ' that cannot fail, and CI running it proves nothing.
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
