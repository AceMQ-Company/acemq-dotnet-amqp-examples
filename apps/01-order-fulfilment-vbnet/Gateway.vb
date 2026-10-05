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

' Where orders enter the system.
'
' The edge of a system is where the dual-write problem lives: an order has to be
' saved *and* announced, and doing those as two writes means a crash between
' them either loses the announcement or announces something that was never
' saved. Neither is recoverable by retrying, because the process that would
' retry is the one that died.
'
' So the gateway does one write. The event is inserted in the same transaction
' as the order, and a relay publishes it afterwards.

Imports System.Data.Common
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class GatewayService

    Private ReadOnly _mq As AceMqConnection
    Private ReadOnly _database As ConnectionSupplier
    Private ReadOnly _outbox As DbOutboxStore
    Private ReadOnly _relay As OutboxRelay

    Private Sub New(mq As AceMqConnection, database As ConnectionSupplier)
        _mq = mq
        _database = database
        _outbox = New DbOutboxStore(database)

        ' Polls every 200 ms, twenty at a time, and a claimed record is leased for
        ' thirty seconds -- the Java app's figures. The relay opens connections of
        ' its own: it runs on its own schedule and must not be inside anybody's
        ' request transaction.
        _relay = New OutboxRelay(mq, _outbox, 20, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30))
    End Sub

    Public Shared Async Function StartAsync(url As String, database As ConnectionSupplier) As Task(Of GatewayService)
        Dim mq = Await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-vbnet/gateway").Build())

        ' Every service applies the whole topology. Applying it five times is safe
        ' and means no service depends on another having started first.
        Await mq.ApplyAsync(Contract.Everything())

        Dim gateway As New GatewayService(mq, database)
        gateway.CreateSchema()
        gateway._relay.Start()
        Return gateway
    End Function

    ''' <summary>Takes an order, and returns the id the customer is given.</summary>
    ''' <remarks>
    ''' In a real gateway this is the body of an HTTP handler. The two writes look
    ''' exactly like this.
    ''' </remarks>
    Public Async Function PlaceOrderAsync(customer As String, sku As String, quantity As Integer,
                                          total As Double) As Task(Of String)
        Dim orderId = "ord-" & Guid.NewGuid().ToString("N").Substring(0, 8)

        Using connection = _database()
            connection.Open()
            Using transaction = connection.BeginTransaction()
                Using insert = connection.CreateCommand()
                    insert.Transaction = transaction
                    insert.CommandText =
                        "INSERT INTO orders (id, customer, sku, quantity, total, status) " &
                        "VALUES (@id, @customer, @sku, @quantity, @total, 'PLACED')"
                    Add(insert, "@id", orderId)
                    Add(insert, "@customer", customer)
                    Add(insert, "@sku", sku)
                    Add(insert, "@quantity", quantity)
                    Add(insert, "@total", total)
                    insert.ExecuteNonQuery()
                End Using

                ' The outbox writes through the caller's transaction. That is the
                ' whole trick: there is no second commit that can fail on its own.
                '
                ' OutboxRecord.For encodes with the connection's codec, so the
                ' stored bytes are exactly what a typed consumer reads -- the Java
                ' app builds that JSON by hand. The order id is the envelope id,
                ' which makes it the key a duplicate is recognised by, and the
                ' correlation id every later event carries forward.
                Await _outbox.AddAsync(
                    OutboxRecord.For(
                        _mq, Contract.Exchange, Contract.OrderPlacedKey,
                        New OrderPlaced With {
                            .OrderId = orderId, .Customer = customer, .Sku = sku,
                            .Quantity = quantity, .Total = total},
                        Envelope.Of("OrderPlaced").Id(orderId).CorrelationId(orderId).Build()),
                    transaction)

                ' Disposing an uncommitted transaction rolls it back, so an
                ' exception anywhere above leaves neither the order nor its
                ' announcement.
                transaction.Commit()
            End Using
        End Using
        Return orderId
    End Function

    ''' <summary>Recorded and not yet published. Zero once the relay has caught up.</summary>
    Public Function PendingInOutboxAsync() As Task(Of Long)
        Return _outbox.PendingCountAsync()
    End Function

    Public Async Function CloseAsync() As Task
        _relay.Dispose()
        Await _mq.CloseAsync()
    End Function

    Private Sub CreateSchema()
        Using connection = _database()
            connection.Open()
            Using command = connection.CreateCommand()
                ' The outbox table is the library's own statement. It hands the
                ' DDL over rather than running it, because a library that silently
                ' creates tables in an application's database has done something
                ' nobody asked for.
                command.CommandText =
                    "CREATE TABLE orders (id VARCHAR(64) PRIMARY KEY, customer VARCHAR(128), " &
                    "sku VARCHAR(64), quantity INT, total DECIMAL(10,2), status VARCHAR(32));" &
                    _outbox.CreateTableSql()
                command.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Private Shared Sub Add(command As DbCommand, name As String, value As Object)
        Dim parameter = command.CreateParameter()
        parameter.ParameterName = name
        parameter.Value = value
        command.Parameters.Add(parameter)
    End Sub

End Class
