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

using AceMq.Amqp;

namespace Fulfilment;

// Takes the money.
//
// This is the service where at-least-once delivery stops being a technicality.
// Every other service in this system can handle a message twice and produce the
// same outcome; this one cannot, because the second charge is real money
// belonging to a real customer.
//
// So it claims each order in a shared store before charging, and confirms
// afterwards. The store is shared rather than in-memory because there is more
// than one instance of this service in production, and an in-memory store makes
// each instance individually idempotent while the fleet is not.
public sealed class PaymentsService
{
    // Over this, a human has to look at it. Every payment system has one.
    private const double AutomaticLimit = 1_000.00;

    private readonly AceMqConnection _mq;
    private readonly DbIdempotencyStore _charged;
    private readonly IPublisher<PaymentCaptured> _capturedEvents;
    private readonly IPublisher<PaymentDeclined> _declinedEvents;
    private int _captures;
    private int _declines;
    private int _duplicatesRefused;

    private PaymentsService(AceMqConnection mq, ConnectionSupplier database)
    {
        _mq = mq;

        // A generous claim timeout: it has to outlast the slowest charge, because
        // a claim that expires while the payment provider is still thinking is a
        // claim another instance will take, and then the customer pays twice.
        _charged = new DbIdempotencyStore(
            database, TimeSpan.FromDays(7), "payments_handled", "@", TimeSpan.FromMinutes(2));

        // Built once. A publisher is meant to live as long as the service does,
        // and the connection keeps every one it hands out until it closes.
        _capturedEvents = mq.Publisher<PaymentCaptured>(Contract.Exchange, Contract.PaymentCapturedKey);
        _declinedEvents = mq.Publisher<PaymentDeclined>(Contract.Exchange, Contract.PaymentDeclinedKey);
    }

    public static async Task<PaymentsService> StartAsync(string url, ConnectionSupplier database)
    {
        var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-csharp/payments").Build());
        await mq.ApplyAsync(Contract.Everything());

        var payments = new PaymentsService(mq, database);
        using (var connection = database())
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = payments._charged.CreateTableSql();
            command.ExecuteNonQuery();
        }

        // Explicit JSON: these messages come from the gateway's outbox, which
        // stores already-serialised bytes and republishes them as they were.
        var options = ConsumerOptions.Prefetch(20)
            .As(CodecRegistry.ByName("json"))
            .WithRetry(RetryPolicy.Exponential(4, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)));

        await mq.ConsumeAsync<OrderPlaced>(Contract.Payments, options, payments.ChargeAsync);
        return payments;
    }

    private async Task<Ack> ChargeAsync(IMessage<OrderPlaced> message)
    {
        var order = message.Payload;

        // The claim is the whole safety net. A redelivery -- from a broker
        // restart, a consumer that died mid-handle, or a relay that published
        // twice -- loses here.
        if (!await _charged.ClaimAsync(order.OrderId))
        {
            Interlocked.Increment(ref _duplicatesRefused);
            return Ack.Accept();
        }

        try
        {
            // The correlation id is what makes five services one story in a log
            // aggregator. Carrying it forward is not optional.
            if (order.Total > AutomaticLimit)
            {
                await _declinedEvents.SendAsync(
                    new PaymentDeclined(order.OrderId, order.Customer, "over the automatic limit"),
                    Envelope.Of("PaymentDeclined").CorrelationId(message.Envelope.CorrelationId).Build());
                Interlocked.Increment(ref _declines);
            }
            else
            {
                await _capturedEvents.SendAsync(
                    new PaymentCaptured(order.OrderId, order.Customer, order.Sku, order.Quantity, order.Total),
                    Envelope.Of("PaymentCaptured").CorrelationId(message.Envelope.CorrelationId).Build());
                Interlocked.Increment(ref _captures);
            }
        }
        catch
        {
            // The one place this app departs from the Java one on purpose. There
            // the claim is kept when the publish fails, and the retry the failure
            // asks for then finds its own claim, is refused as a duplicate and
            // acknowledged: the order stops, with the customer neither charged nor
            // told. Released, the retry can take it again.
            await _charged.ReleaseAsync(order.OrderId);
            throw;
        }

        // Confirmed only after the outcome is published. Confirming first would
        // mean a crash in between leaves the order marked as charged with nothing
        // downstream ever told -- an order that took the money and stopped.
        await _charged.ConfirmAsync(order.OrderId);
        return Ack.Accept();
    }

    public int Captured => Volatile.Read(ref _captures);

    public int Declined => Volatile.Read(ref _declines);

    /// <summary>How many redeliveries were recognised and refused. Worth graphing.</summary>
    public int DuplicatesRefused => Volatile.Read(ref _duplicatesRefused);

    public Task CloseAsync() => _mq.CloseAsync();
}
