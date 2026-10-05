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

// Dispatches what has been paid for and reserved.
//
// The simplest service in the system, and it is worth noticing why: it reacts
// to one event, does one thing, and publishes one event. It knows nothing about
// payments, nothing about stock levels, and nothing about who else cares that an
// order shipped. Adding a service that also reacts to stock.reserved requires no
// change here at all.
public sealed class ShippingService
{
    private readonly AceMqConnection _mq;
    private readonly IPublisher<OrderShipped> _shippedEvents;
    private int _shipped;

    private ShippingService(AceMqConnection mq)
    {
        _mq = mq;
        _shippedEvents = mq.Publisher<OrderShipped>(Contract.Exchange, Contract.OrderShippedKey);
    }

    public static async Task<ShippingService> StartAsync(string url)
    {
        var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-csharp/shipping").Build());
        await mq.ApplyAsync(Contract.Everything());

        var shipping = new ShippingService(mq);
        await mq.ConsumeAsync<StockReserved>(Contract.Shipping, ConsumerOptions.Prefetch(10), shipping.DispatchAsync);
        return shipping;
    }

    private async Task<Ack> DispatchAsync(IMessage<StockReserved> message)
    {
        var reservation = message.Payload;
        var tracking = "TRK-" + reservation.OrderId[4..].ToUpperInvariant();
        await _shippedEvents.SendAsync(
            new OrderShipped(reservation.OrderId, reservation.Customer, tracking),
            Envelope.Of("OrderShipped").CorrelationId(message.Envelope.CorrelationId).Build());
        Interlocked.Increment(ref _shipped);
        return Ack.Accept();
    }

    public int Shipped => Volatile.Read(ref _shipped);

    public Task CloseAsync() => _mq.CloseAsync();
}
