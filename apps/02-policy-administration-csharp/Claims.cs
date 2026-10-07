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

// Claims: the module that has to ask another module a question.
//
// Everything else in this application reacts to events, which is the right
// default. Claims cannot: before settling a claim it must know whether the policy
// is in force, and it needs the answer now, in the middle of a decision. An event
// cannot answer a question.
//
// So it asks, over the broker. The callee is in the same process, and a method call
// would work today and would be the wrong choice: the moment claims calls into
// policies directly, the two are one module. Asking costs a millisecond and keeps
// the seam.
//
// The timeout is the part not to skip. A request that waits for ever is how one
// slow module stops the whole application, monolith or not.
public sealed class ClaimsModule
{
    /// <summary>Generous for an in-process hop, and still bounded.</summary>
    private static readonly TimeSpan LookupTimeout = TimeSpan.FromSeconds(5);

    private readonly Requester _requester;
    private readonly IPublisher<ClaimSettled> _settlements;
    private readonly IPublisher<ClaimRejected> _rejections;
    private int _settled;
    private int _rejected;

    private ClaimsModule(AceMqConnection mq, Requester requester)
    {
        _requester = requester;
        _settlements = mq.Publisher<ClaimSettled>(Policies.Exchange, Policies.ClaimSettledKey);
        _rejections = mq.Publisher<ClaimRejected>(Policies.Exchange, Policies.ClaimRejectedKey);
    }

    public static async Task<ClaimsModule> StartAsync(AceMqConnection mq)
    {
        var claims = new ClaimsModule(mq, await mq.RequesterAsync());

        // Claims listens for issued policies only to know they exist at all; the
        // authoritative answer still comes from the lookup, because a policy can be
        // cancelled after issue and this module keeps no copy of another's state.
        var json = ConsumerOptions.Defaults().As(CodecRegistry.ByName("json"));
        await mq.ConsumeAsync<PolicyIssued>(Policies.Claims, json, _ => Task.FromResult(Ack.Accept()));
        return claims;
    }

    /// <summary>Assesses a claim against a policy, and returns the claim id.</summary>
    public async Task<string> SubmitAsync(string policyId, int amount, string description)
    {
        var claimId = "CLM-" + Guid.NewGuid().ToString("N")[..8];
        var envelope = Envelope.Of("Claim").CorrelationId(policyId).Build();

        PolicyStatus status;
        try
        {
            status = await _requester.RequestAsync<PolicyQuery, PolicyStatus>(
                "", Policies.PolicyLookup, new PolicyQuery(policyId), LookupTimeout, CancellationToken.None);
        }
        catch (RequestTimedOutException e)
        {
            // Not an answer, and must not be treated as "no". Refusing a valid claim
            // because a lookup was slow is the failure mode worth being explicit
            // about.
            throw new InvalidOperationException(
                $"could not establish whether {policyId} is in force, so claim {claimId} " +
                "was neither settled nor rejected; it must be retried", e);
        }

        if (!status.InForce)
        {
            await _rejections.SendAsync(new ClaimRejected(claimId, policyId, "no policy in force"), envelope);
            Interlocked.Increment(ref _rejected);
            return claimId;
        }

        // A real assessment is a great deal more than this. What matters is that it
        // happened after an authoritative answer rather than after a guess.
        await _settlements.SendAsync(new ClaimSettled(claimId, policyId, amount), envelope);
        Interlocked.Increment(ref _settled);
        return claimId;
    }

    public int Settled => Volatile.Read(ref _settled);

    /// <summary>Claims rejected because no policy was in force.</summary>
    public int Rejected => Volatile.Read(ref _rejected);

    /// <summary>Lookups that gave up waiting. Every scenario expects none.</summary>
    public long LookupsTimedOut => _requester.TimedOut;
}
