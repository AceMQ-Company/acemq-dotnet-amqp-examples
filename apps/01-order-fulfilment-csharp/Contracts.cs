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

// What every service in this system agrees on, and nothing else.
//
// The events, the exchange, the queue each service reads, and the routing keys
// that connect them. In a larger estate this is what a schema registry holds.
//
// What is deliberately not here: any service's domain model, any database
// access, any shared "helper". A contracts file that grows those stops being a
// contract and becomes a shared library, which is how five services turn back
// into one deployable that happens to have five entry points.

// ---- events -------------------------------------------------------------------
//
// Positional records, so they serialise to JSON without ceremony and cannot be
// half-built. The library's JSON codec camelCases on the way out, so OrderId is
// "orderId" on the wire -- the same field name the Java app's records produce.
// Each carries the order id, because that is the only identifier every service
// shares.

/// <summary>Someone placed an order. Published by the gateway, from its outbox.</summary>
public sealed record OrderPlaced(string OrderId, string Customer, string Sku, int Quantity, double Total);

/// <summary>The money is ours. Published by payments.</summary>
public sealed record PaymentCaptured(string OrderId, string Customer, string Sku, int Quantity, double Amount);

/// <summary>It is not, and will not be. Published by payments; nothing downstream proceeds.</summary>
public sealed record PaymentDeclined(string OrderId, string Customer, string Reason);

/// <summary>Stock is held for this order. Published by inventory.</summary>
public sealed record StockReserved(string OrderId, string Customer, string Sku, int Quantity);

/// <summary>There is not enough. Published by inventory; the money must be given back.</summary>
public sealed record StockUnavailable(string OrderId, string Customer, string Sku, string Reason);

/// <summary>On its way. Published by shipping.</summary>
public sealed record OrderShipped(string OrderId, string Customer, string Tracking);

public static class Contract
{
    // Every broker object this app declares starts with this, and nothing else
    // differs from the Java app. The C# and VB.NET twins run against the same
    // broker in CI, and the same app in four other languages may be running
    // against the same cluster in a fault drill; two of them sharing
    // "fulfilment.payments" would each take half of the other's orders.
    //
    // Routing keys and envelope types are NOT prefixed: they are the contract,
    // and they are the Java app's, character for character.
    private const string Prefix = "dotnet-csharp.";

    /// <summary>One topic exchange. Every event in the system is published here.</summary>
    public const string Exchange = Prefix + "fulfilment";

    // ---- routing keys ---------------------------------------------------------
    //
    // "fulfilment.<aggregate>.<past-tense-verb>". The aggregate in the middle is
    // what lets notifications subscribe to everything at all.

    public const string OrderPlacedKey = "fulfilment.order.placed";
    public const string PaymentCapturedKey = "fulfilment.payment.captured";
    public const string PaymentDeclinedKey = "fulfilment.payment.declined";
    public const string StockReservedKey = "fulfilment.stock.reserved";
    public const string StockUnavailableKey = "fulfilment.stock.unavailable";
    public const string OrderShippedKey = "fulfilment.order.shipped";

    // ---- queues ---------------------------------------------------------------
    //
    // A queue per service, named after the service rather than after the event.
    // Two services wanting the same event each get their own copy, and neither
    // can starve the other.

    public const string Payments = Prefix + "fulfilment.payments";
    public const string Inventory = Prefix + "fulfilment.inventory";
    public const string Shipping = Prefix + "fulfilment.shipping";
    public const string Notifications = Prefix + "fulfilment.notifications";

    public static readonly IReadOnlyList<string> Queues = new[] { Payments, Inventory, Shipping, Notifications };

    /// <summary>What must exist for this system to work: the whole topology, as one value.</summary>
    /// <remarks>
    /// Every service applies this on start-up. Applying it five times is safe and
    /// is the point: no service depends on another having started first, so there
    /// is no deployment order to get wrong.
    ///
    /// Classic queues, as in the Java app. The .NET builder's <c>Queue(name)</c>
    /// alone would declare quorum queues, which is the right default for a real
    /// deployment and a difference from the app being ported.
    /// </remarks>
    public static Topology Everything() =>
        AceMq.Amqp.Topology.Define()
            .Exchange(Exchange, "topic")

            // Payments acts on new orders.
            .Queue(Payments, QueueType.Classic)
            .Bind(Payments, Exchange, OrderPlacedKey)

            // Inventory acts once the money is taken, not before. Reserving stock
            // for an order that cannot be paid for is how a warehouse fills with
            // holds nobody releases.
            .Queue(Inventory, QueueType.Classic)
            .Bind(Inventory, Exchange, PaymentCapturedKey)

            // Shipping needs stock held.
            .Queue(Shipping, QueueType.Classic)
            .Bind(Shipping, Exchange, StockReservedKey)

            // Notifications wants everything, which is what a wildcard is for.
            .Queue(Notifications, QueueType.Classic)
            .Bind(Notifications, Exchange, "fulfilment.#")

            .Build();
}
