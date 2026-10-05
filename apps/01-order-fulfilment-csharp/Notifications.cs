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

// Tells the customer what happened.
//
// Bound to fulfilment.# -- everything. This is the service that shows why a
// topic exchange is worth more than a queue per pair of services: it was added
// without a single change to any publisher, and the next one will be too.
//
// It cannot ask for a typed payload, because it subscribes to six event types on
// one queue and their shapes differ. So it takes the body as text and reads the
// envelope, which carries the type and the correlation id -- everything this
// service actually needs.
public sealed class NotificationsService
{
    private readonly AceMqConnection _mq;
    private readonly ConcurrentDictionary<string, List<string>> _timeline = new();

    private NotificationsService(AceMqConnection mq) => _mq = mq;

    public static async Task<NotificationsService> StartAsync(string url)
    {
        var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(url).ClientName("examples/apps/01-order-fulfilment-csharp/notifications").Build());
        await mq.ApplyAsync(Contract.Everything());

        var notifications = new NotificationsService(mq);

        // The text codec is the part worth copying. Asking for string with the
        // JSON codec hands it an object and tells it to produce a string, which
        // fails on every message -- a fan-in consumer's most common mistake.
        //
        // Registered as "string" here and in Ruby; Java and Python call the same
        // codec "text". A name taken from one service's configuration is refused
        // by the other two libraries, so this one is spelled out.
        var options = ConsumerOptions.Prefetch(50).As(CodecRegistry.ByName("string"));

        await mq.ConsumeAsync<string>(Contract.Notifications, options, message =>
        {
            // The correlation id is the order it belongs to, set by whichever
            // service published it and carried forward by all of them.
            var events = notifications._timeline.GetOrAdd(message.Envelope.CorrelationId, _ => new List<string>());
            lock (events) events.Add(message.Envelope.Type);
            return Task.FromResult(Ack.Accept());
        });
        return notifications;
    }

    /// <summary>What a customer looking at "where is my order" would be shown.</summary>
    public IReadOnlyList<string> TimelineOf(string orderId)
    {
        if (!_timeline.TryGetValue(orderId, out var events)) return Array.Empty<string>();
        lock (events) return events.ToArray();
    }

    public Task CloseAsync() => _mq.CloseAsync();
}
