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

// Deciding whether to accept an application, and at what price.
//
// Underwriting is the one part of this application that is genuinely a sequence:
// check the applicant against the register, price the risk, then decide. Each
// stage fails for its own reasons and is slow for its own reasons, which is what a
// pipeline is for -- a queue per stage, so a slow stage shows up as a deep queue
// you can point at, and can be retried and scaled without touching the others.
//
// Written as one consumer doing three things in order, all of that disappears:
// one queue, one failure mode, and one number that says "underwriting is slow".
public sealed class UnderwritingModule
{
    /// <summary>Applications above this are a human's decision, not a rule's.</summary>
    private const int ReferralThreshold = 500_000;

    private readonly IPublisher<ApplicationAccepted> _acceptances;
    private readonly IPublisher<ApplicationDeclined> _declines;
    private int _accepted;
    private int _declined;

    private UnderwritingModule(AceMqConnection mq)
    {
        _acceptances = mq.Publisher<ApplicationAccepted>(Policies.Exchange, Policies.ApplicationAcceptedKey);
        _declines = mq.Publisher<ApplicationDeclined>(Policies.Exchange, Policies.ApplicationDeclinedKey);
    }

    /// <summary>What the register stage produces.</summary>
    public sealed record Checked(ApplicationSubmitted Application, bool KnownToRegister);

    /// <summary>What the pricing stage produces.</summary>
    public sealed record Priced(ApplicationSubmitted Application, int AnnualPremium, bool Refer);

    public static async Task<UnderwritingModule> StartAsync(AceMqConnection mq)
    {
        var underwriting = new UnderwritingModule(mq);

        // Step names are the queue suffixes, so they stay short and stable:
        // renaming one strands whatever is in flight against the old name.
        //
        // The retry policy is the register's in the Java app -- the register is
        // somebody else's service and is periodically unavailable, which is a wait
        // rather than a decline. The .NET builder takes one policy for the whole
        // pipeline, so the other two stages have it too; neither of them fails.
        var pipeline = await mq.Pipeline<ApplicationSubmitted>(Policies.UnderwritingPipeline)
            .WithRetry(RetryPolicy.Exponential(4, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5)))
            .Step<Checked>("register", application => Task.FromResult<Checked?>(CheckRegister(application)))
            .Step<Priced>("price", checkedOne => Task.FromResult<Priced?>(Price(checkedOne)))
            .Step<object>("decide", underwriting.DecideAsync)
            .BuildAsync();

        // Fed from the module's own queue rather than bound to the exchange itself:
        // the pipeline owns its stages' queues, and what enters it is this module's
        // decision. The correlation id goes in with it, so the decision at the end
        // can carry it on.
        var json = ConsumerOptions.Defaults().As(CodecRegistry.ByName("json"));
        await mq.ConsumeAsync<ApplicationSubmitted>(Policies.Underwriting, json, async message =>
        {
            await pipeline.SendAsync(
                message.Payload,
                Envelope.Of("ApplicationSubmitted").CorrelationId(message.Payload.ApplicationId).Build());
            return Ack.Accept();
        });
        return underwriting;
    }

    private static Checked CheckRegister(ApplicationSubmitted application) =>
        // A real one calls an industry service. What matters here is that it is
        // the stage most likely to be slow, and it has its own queue to prove it.
        new(application, application.Applicant.Contains("known", StringComparison.OrdinalIgnoreCase));

    private static Priced Price(Checked checkedOne)
    {
        var application = checkedOne.Application;

        // A rating table, compressed to one line. Older applicants and larger sums
        // cost more.
        var baseRate = application.SumAssured / 1000;
        var ageLoading = Math.Max(0, application.AgeOfApplicant - 30) * 2;
        var registerLoading = checkedOne.KnownToRegister ? baseRate / 2 : 0;
        var premium = baseRate + ageLoading + registerLoading;

        return new Priced(application, premium, application.SumAssured > ReferralThreshold);
    }

    private async Task<object?> DecideAsync(Priced priced)
    {
        var application = priced.Application;
        var envelope = Envelope.Of("UnderwritingDecision").CorrelationId(application.ApplicationId).Build();

        if (priced.Refer)
        {
            // Declined here rather than parked, because "a human must look at this"
            // is a real outcome of underwriting and not a failure of it. Modelling
            // it as an error would put it in a dead-letter queue, where it would
            // look like something broke.
            await _declines.SendAsync(
                new ApplicationDeclined(
                    application.ApplicationId, application.Applicant,
                    $"sum assured of {application.SumAssured} is above the automatic limit"),
                envelope);
            Interlocked.Increment(ref _declined);
            return null;
        }

        await _acceptances.SendAsync(
            new ApplicationAccepted(
                application.ApplicationId, application.Applicant, application.Product,
                application.SumAssured, priced.AnnualPremium),
            envelope);
        Interlocked.Increment(ref _accepted);

        // The last step: nothing goes on, and the pipeline counts it completed.
        return null;
    }

    public int Accepted => Volatile.Read(ref _accepted);

    /// <summary>How many it referred or refused.</summary>
    public int Declined => Volatile.Read(ref _declined);
}
