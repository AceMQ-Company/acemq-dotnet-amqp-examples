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

namespace PolicyAdministration;

// Taking the first premium: the one module where handling a message twice is real
// money.
//
// The same reasoning as payments in apps/01, and worth repeating because the
// monolith makes it easy to assume the problem went away. It did not. A redeploy
// mid-handler still leaves a message unacknowledged, and the redelivery still
// arrives at a module that already took the money.
//
// The idempotency store is in the database rather than in memory even though this
// is one process, because "one process" is a fact about today. The moment this
// module is lifted out -- the whole point of the arrangement -- an in-memory store
// becomes two stores that each think they are the only one, and each charges once.
public sealed class BillingModule
{
    private readonly IPublisher<PremiumCharged> _charged;
    private readonly List<string> _charges = new();

    private BillingModule(AceMqConnection mq) =>
        _charged = mq.Publisher<PremiumCharged>(Policies.Exchange, Policies.PremiumChargedKey);

    public static async Task<BillingModule> StartAsync(AceMqConnection mq, ConnectionSupplier database)
    {
        var billing = new BillingModule(mq);

        var seen = new DbIdempotencyStore(database, TimeSpan.FromDays(7));
        using (var connection = database())
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = seen.CreateTableSql();
            command.ExecuteNonQuery();
        }

        // The store is handed to the consumer rather than used by hand: the claim is
        // taken before the handler and confirmed after it returns, which is the order
        // that closes the window a manual "mark it afterwards" leaves open. Keyed by
        // the envelope's id, so "sent twice" is one charge and "happened twice" is
        // two.
        var options = ConsumerOptions.Prefetch(10).Idempotent(seen).As(CodecRegistry.ByName("json"));
        await mq.ConsumeAsync<PolicyIssued>(Policies.Billing, options, async message =>
        {
            var policy = message.Payload;
            await billing._charged.SendAsync(
                new PremiumCharged(policy.PolicyId, policy.Applicant, policy.AnnualPremium),
                Envelope.Of("PremiumCharged").CorrelationId(policy.ApplicationId).Build());

            // Counted once the charge is announced, so a publish that failed and was
            // retried is not counted twice.
            lock (billing._charges) billing._charges.Add(policy.PolicyId);
            return Ack.Accept();
        });
        return billing;
    }

    /// <summary>One entry per charge actually taken; duplicates here would be the bug.</summary>
    public IReadOnlyList<string> Charges
    {
        get
        {
            lock (_charges) return _charges.ToArray();
        }
    }
}
