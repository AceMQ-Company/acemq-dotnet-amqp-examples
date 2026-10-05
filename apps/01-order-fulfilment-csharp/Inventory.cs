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

using System.Collections.Concurrent;

using AceMq.Amqp;

namespace Fulfilment;

// Holds stock for orders that have been paid for.
//
// The service that talks to something unreliable. A warehouse system that times
// out is the ordinary case, not the exception, and two failures have to be told
// apart:
//
//   the warehouse did not answer        retry, it will probably work in a moment
//   three are left and they want ten    retrying changes nothing
//
// The first is a plain exception and goes up the retry ladder. The second is not
// a failure of this service at all: it is an answer, published as
// StockUnavailable so the customer is told at once rather than after four more
// attempts that could not have conjured stock.
public sealed class InventoryService
{
    private readonly AceMqConnection _mq;
    private readonly IPublisher<StockReserved> _reservedEvents;
    private readonly IPublisher<StockUnavailable> _unavailableEvents;
    private readonly ConcurrentDictionary<string, int> _stock = new();
    private int _reserved;
    private int _rejected;
    private int _retried;
    private int _warehouseCalls;

    // How many warehouse calls fail before it starts working. Set by the run.
    private int _failuresToSimulate;

    private InventoryService(AceMqConnection mq)
    {
        _mq = mq;
        _reservedEvents = mq.Publisher<StockReserved>(Contract.Exchange, Contract.StockReservedKey);
        _unavailableEvents = mq.Publisher<StockUnavailable>(Contract.Exchange, Contract.StockUnavailableKey);
    }

    public static async Task<InventoryService> StartAsync(string url)
    {
        var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-csharp/inventory").Build());
        await mq.ApplyAsync(Contract.Everything());

        var inventory = new InventoryService(mq);

        // Four attempts, 200 ms doubling to at most five seconds -- the Java app's
        // ladder. Java waits every rung inside the broker. This library waits a
        // rung shorter than RetryPolicy.DefaultBrokerWaitThreshold (thirty
        // seconds) on the consumer instead, and only the longer ones in the
        // broker, so every rung here is a short wait in-process. Either way the
        // attempt count travels on the message, which is what the handler reads.
        var options = ConsumerOptions.Prefetch(20)
            .WithRetry(RetryPolicy.Exponential(4, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)));

        await mq.ConsumeAsync<PaymentCaptured>(Contract.Inventory, options, inventory.ReserveAsync);
        return inventory;
    }

    public InventoryService WithStock(string sku, int quantity)
    {
        _stock[sku] = quantity;
        return this;
    }

    /// <summary>Makes the next <paramref name="count"/> warehouse calls fail, the way a real one does.</summary>
    public InventoryService WithFlakyWarehouse(int count)
    {
        Volatile.Write(ref _failuresToSimulate, count);
        return this;
    }

    private async Task<Ack> ReserveAsync(IMessage<PaymentCaptured> message)
    {
        // message.Attempt, not message.Envelope.Attempt: the count the consumer
        // keeps is the one that moves. Counted here, from what actually arrived,
        // rather than from what the library says it scheduled -- the .NET
        // consumer has no Retried counter of the kind Java's has, and a retry that
        // was scheduled and never came back is exactly what this would miss.
        if (message.Attempt > 1) Interlocked.Increment(ref _retried);

        // The transient failure. Nothing is wrong with the message, so it goes
        // back on the ladder and arrives again shortly.
        if (Interlocked.Increment(ref _warehouseCalls) <= Volatile.Read(ref _failuresToSimulate))
        {
            throw new InvalidOperationException("warehouse did not respond");
        }

        var payment = message.Payload;

        _stock.TryGetValue(payment.Sku, out var available);
        if (available < payment.Quantity)
        {
            // The permanent one. Retrying will not conjure stock.
            await _unavailableEvents.SendAsync(
                new StockUnavailable(payment.OrderId, payment.Customer, payment.Sku, $"only {available} left"),
                Envelope.Of("StockUnavailable").CorrelationId(message.Envelope.CorrelationId).Build());
            Interlocked.Increment(ref _rejected);
            return Ack.Accept();
        }

        _stock.AddOrUpdate(payment.Sku, -payment.Quantity, (_, now) => now - payment.Quantity);
        await _reservedEvents.SendAsync(
            new StockReserved(payment.OrderId, payment.Customer, payment.Sku, payment.Quantity),
            Envelope.Of("StockReserved").CorrelationId(message.Envelope.CorrelationId).Build());
        Interlocked.Increment(ref _reserved);
        return Ack.Accept();
    }

    public int Reserved => Volatile.Read(ref _reserved);

    public int Rejected => Volatile.Read(ref _rejected);

    /// <summary>Deliveries that were a second or later attempt.</summary>
    public int Retried => Volatile.Read(ref _retried);

    public int StockOf(string sku) => _stock.TryGetValue(sku, out var quantity) ? quantity : 0;

    public Task CloseAsync() => _mq.CloseAsync();
}
