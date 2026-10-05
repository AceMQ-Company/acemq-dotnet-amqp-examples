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

using System.Data.Common;

using AceMq.Amqp;

namespace Fulfilment;

// Where orders enter the system.
//
// The edge of a system is where the dual-write problem lives: an order has to be
// saved *and* announced, and doing those as two writes means a crash between
// them either loses the announcement or announces something that was never
// saved. Neither is recoverable by retrying, because the process that would
// retry is the one that died.
//
// So the gateway does one write. The event is inserted in the same transaction
// as the order, and a relay publishes it afterwards.
public sealed class GatewayService
{
    private readonly AceMqConnection _mq;
    private readonly ConnectionSupplier _database;
    private readonly DbOutboxStore _outbox;
    private readonly OutboxRelay _relay;

    private GatewayService(AceMqConnection mq, ConnectionSupplier database)
    {
        _mq = mq;
        _database = database;
        _outbox = new DbOutboxStore(database);

        // Polls every 200 ms, twenty at a time, and a claimed record is leased for
        // thirty seconds -- the Java app's figures. The relay opens connections of
        // its own: it runs on its own schedule and must not be inside anybody's
        // request transaction.
        _relay = new OutboxRelay(mq, _outbox, 20, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(30));
    }

    public static async Task<GatewayService> StartAsync(string url, ConnectionSupplier database)
    {
        var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-csharp/gateway").Build());

        // Every service applies the whole topology. Applying it five times is safe
        // and means no service depends on another having started first.
        await mq.ApplyAsync(Contract.Everything());

        var gateway = new GatewayService(mq, database);
        gateway.CreateSchema();
        gateway._relay.Start();
        return gateway;
    }

    /// <summary>Takes an order, and returns the id the customer is given.</summary>
    /// <remarks>
    /// In a real gateway this is the body of an HTTP handler. The two writes look
    /// exactly like this.
    /// </remarks>
    public async Task<string> PlaceOrderAsync(string customer, string sku, int quantity, double total)
    {
        var orderId = "ord-" + Guid.NewGuid().ToString("N")[..8];

        using var connection = _database();
        connection.Open();
        using var transaction = connection.BeginTransaction();

        using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText =
                "INSERT INTO orders (id, customer, sku, quantity, total, status) " +
                "VALUES (@id, @customer, @sku, @quantity, @total, 'PLACED')";
            Add(insert, "@id", orderId);
            Add(insert, "@customer", customer);
            Add(insert, "@sku", sku);
            Add(insert, "@quantity", quantity);
            Add(insert, "@total", total);
            insert.ExecuteNonQuery();
        }

        // The outbox writes through the caller's transaction. That is the whole
        // trick: there is no second commit that can fail on its own.
        //
        // OutboxRecord.For encodes with the connection's codec, so the stored
        // bytes are exactly what a typed consumer reads -- the Java app builds
        // that JSON by hand. The order id is the envelope id, which makes it the
        // key a duplicate is recognised by, and the correlation id every later
        // event carries forward.
        await _outbox.AddAsync(
            OutboxRecord.For(
                _mq, Contract.Exchange, Contract.OrderPlacedKey,
                new OrderPlaced(orderId, customer, sku, quantity, total),
                Envelope.Of("OrderPlaced").Id(orderId).CorrelationId(orderId).Build()),
            transaction);

        // Disposing an uncommitted transaction rolls it back, so an exception
        // anywhere above leaves neither the order nor its announcement.
        transaction.Commit();
        return orderId;
    }

    /// <summary>Recorded and not yet published. Zero once the relay has caught up.</summary>
    public Task<long> PendingInOutboxAsync() => _outbox.PendingCountAsync();

    public async Task CloseAsync()
    {
        _relay.Dispose();
        await _mq.CloseAsync();
    }

    private void CreateSchema()
    {
        using var connection = _database();
        connection.Open();
        using var command = connection.CreateCommand();
        // The outbox table is the library's own statement. It hands the DDL over
        // rather than running it, because a library that silently creates tables
        // in an application's database has done something nobody asked for.
        command.CommandText =
            "CREATE TABLE orders (id VARCHAR(64) PRIMARY KEY, customer VARCHAR(128), sku VARCHAR(64), " +
            "quantity INT, total DECIMAL(10,2), status VARCHAR(32));" + _outbox.CreateTableSql();
        command.ExecuteNonQuery();
    }

    private static void Add(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
