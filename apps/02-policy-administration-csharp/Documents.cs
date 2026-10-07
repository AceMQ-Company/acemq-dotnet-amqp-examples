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

namespace PolicyAdministration;

// Documents: the claim-check pattern.
//
// A medical report scanned at 300 dpi is tens of megabytes. Putting it on a queue
// is possible and is a mistake -- it fills the broker's memory, it is copied to
// every bound queue, it makes a dead-letter queue impossible to inspect, and it
// turns a broker into a filesystem with worse tools. What travels instead is a
// claim check: the document goes to a store, and the message carries the key.
//
// The store is a dictionary because the example must run without infrastructure.
// A real one is S3, Azure Blob Storage or a filesystem, and only two method bodies
// change. The library has IClaimCheckStore and a ClaimCheckCodec that does this
// transparently for any payload over a threshold; this module does it by hand, as
// the Java one does, because its key says what it is -- the policy and the kind of
// document -- where the library's is opaque.
//
// Retention is the part people forget. The store and the queue have different
// lifetimes. A message replayed a month later carries a key, and if the store
// expired it the replay produces a message nobody can read -- worse than a lost
// message, because it looks like a message.
public sealed class DocumentModule
{
    private readonly IPublisher<DocumentStored> _stored;
    private readonly ConcurrentDictionary<string, byte[]> _store = new();

    public DocumentModule(AceMqConnection mq) =>
        _stored = mq.Publisher<DocumentStored>(Policies.Exchange, Policies.DocumentStoredKey);

    /// <summary>Stores a document and announces that it exists.</summary>
    /// <returns>The key the event carries. The bytes do not go anywhere near the broker.</returns>
    public async Task<string> StoreAsync(string policyId, string kind, byte[] content)
    {
        var key = $"doc/{policyId}/{kind}/{Guid.NewGuid().ToString("N")[..8]}";
        _store[key] = content;

        // The event is a few hundred bytes whatever the document weighs.
        await _stored.SendAsync(
            new DocumentStored(policyId, key, kind, content.Length),
            Envelope.Of("DocumentStored").CorrelationId(policyId).Build());
        return key;
    }

    /// <summary>Redeems a claim check: the document, when the store still has it.</summary>
    public byte[]? Fetch(string key) => _store.TryGetValue(key, out var content) ? content : null;

    public int Held => _store.Count;
}
