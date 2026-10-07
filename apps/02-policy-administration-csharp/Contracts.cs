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

// What every module in this monolith agrees on, and nothing else.
//
// These modules run in one process, so nothing but discipline stops one from
// reaching into another's classes. In the Java app the boundary is a Maven
// dependency graph: every module depends on the contracts jar and on none of its
// siblings. Here it is a convention -- each module's file uses this one and no
// other module's types -- and Program.cs is the only file that names more than
// one module.
//
// That is what makes the monolith modular rather than merely large. A module that
// only ever received events can be lifted into its own process by changing where
// it connects: no call sites to find, because there are none.
//
// What is deliberately not here: any module's domain model, any persistence, any
// shared helper. Those turn a contracts file into a common-utils file, and a
// common-utils file is how modular monoliths become ordinary ones.

// ---- events -------------------------------------------------------------------
//
// Positional records. The JSON codec camelCases, so ApplicationId is
// "applicationId" on the wire -- the field name the Java records produce.

/// <summary>A broker submitted an application. Published by policies, from its outbox.</summary>
public sealed record ApplicationSubmitted(
    string ApplicationId, string Applicant, string Product, int SumAssured, int AgeOfApplicant);

/// <summary>Underwriting reached a decision and priced it.</summary>
public sealed record ApplicationAccepted(
    string ApplicationId, string Applicant, string Product, int SumAssured, int AnnualPremium);

/// <summary>Underwriting refused it, with a reason a human can act on.</summary>
public sealed record ApplicationDeclined(string ApplicationId, string Applicant, string Reason);

/// <summary>A policy exists. Published by policies once underwriting accepted.</summary>
public sealed record PolicyIssued(
    string PolicyId, string ApplicationId, string Applicant, string Product, int AnnualPremium);

/// <summary>A document belongs to a policy.</summary>
/// <remarks>
/// The document itself is <strong>not</strong> here. This carries a claim check --
/// the key it was stored under and how big it is -- because a medical report
/// scanned at 300 dpi is tens of megabytes and a broker is not a filesystem.
/// </remarks>
public sealed record DocumentStored(string PolicyId, string DocumentKey, string Kind, int Bytes);

/// <summary>The first premium was taken. Published by billing.</summary>
public sealed record PremiumCharged(string PolicyId, string Applicant, int Amount);

/// <summary>Somebody claimed against a policy.</summary>
public sealed record ClaimSubmitted(string ClaimId, string PolicyId, int Amount, string Description);

/// <summary>The claim was assessed.</summary>
public sealed record ClaimSettled(string ClaimId, string PolicyId, int Paid);

/// <summary>It was not, and why.</summary>
public sealed record ClaimRejected(string ClaimId, string PolicyId, string Reason);

/// <summary>What claims asks.</summary>
public sealed record PolicyQuery(string PolicyId);

/// <summary>What policies answers. A record rather than a boolean, so it can grow a reason.</summary>
public sealed record PolicyStatus(string PolicyId, bool InForce, int AnnualPremium);

public static class Policies
{
    // Every broker object this app declares starts with this, and nothing else
    // differs from the Java app. The C# and VB.NET twins run against one broker in
    // CI, and the same app in other languages may share a drill cluster with them;
    // two apps reading one "policy.billing" would each take half of the other's
    // policies. Routing keys and envelope types are NOT prefixed: they are the
    // contract, and they are the Java app's, character for character.
    private const string Prefix = "dotnet-csharp.";

    /// <summary>One topic exchange for the whole application.</summary>
    public const string Exchange = Prefix + "policy";

    // ---- routing keys -------------------------------------------------------------

    public const string ApplicationSubmittedKey = "policy.application.submitted";
    public const string ApplicationAcceptedKey = "policy.application.accepted";
    public const string ApplicationDeclinedKey = "policy.application.declined";
    public const string PolicyIssuedKey = "policy.policy.issued";
    public const string DocumentStoredKey = "policy.document.stored";
    public const string PremiumChargedKey = "policy.premium.charged";
    public const string ClaimSubmittedKey = "policy.claim.submitted";
    public const string ClaimSettledKey = "policy.claim.settled";
    public const string ClaimRejectedKey = "policy.claim.rejected";

    // ---- queues -------------------------------------------------------------------
    //
    // A queue per module, named for the module, as in apps/01: two modules wanting
    // the same event each get their own copy. That these queues are served by one
    // process is an operational detail, not an architectural one.

    public const string Underwriting = Prefix + "policy.underwriting";
    public const string PoliciesQueue = Prefix + "policy.policies";
    public const string Billing = Prefix + "policy.billing";
    public const string Claims = Prefix + "policy.claims";

    /// <summary>Everything, for the audit trail.</summary>
    /// <remarks>
    /// Not decoration. Writing the Java app without it produced a real failure:
    /// claims and documents published events nothing was bound to, and the library
    /// refused the publish rather than discarding it. A regulated insurer has this
    /// queue whatever else it has.
    /// </remarks>
    public const string Audit = Prefix + "policy.audit";

    /// <summary>Where claims asks policies whether a policy is in force. Request/reply, not an event.</summary>
    public const string PolicyLookup = Prefix + "policy.lookup";

    /// <summary>
    /// The name underwriting's pipeline declares its stage queues under:
    /// <c>{this}.register</c>, <c>{this}.price</c>, <c>{this}.decide</c>.
    /// </summary>
    public const string UnderwritingPipeline = Prefix + "underwriting";

    public static readonly IReadOnlyList<string> PipelineSteps = new[] { "register", "price", "decide" };

    /// <summary>Every queue a consumer reads, and so every queue with a .dlq and .parked beside it.</summary>
    public static IReadOnlyList<string> ConsumedQueues =>
        new[] { Underwriting, PoliciesQueue, Billing, Claims, PolicyLookup }
            .Concat(PipelineSteps.Select(step => $"{UnderwritingPipeline}.{step}"))
            .ToArray();

    /// <summary>The whole application's topology, as one value.</summary>
    /// <remarks>
    /// Applied once at start-up: one process, so once is enough -- and still
    /// declared here rather than assembled from each module's fragment, because a
    /// module that declares its own queue is a module that can be started against
    /// an exchange nobody created. Classic queues, as in the Java app.
    /// </remarks>
    public static Topology Everything() =>
        AceMq.Amqp.Topology.Define()
            .Exchange(Exchange, "topic")

            // Underwriting acts on submissions.
            .Queue(Underwriting, QueueType.Classic)
            .Bind(Underwriting, Exchange, ApplicationSubmittedKey)

            // Policies issues once underwriting has accepted, and answers lookups.
            .Queue(PoliciesQueue, QueueType.Classic)
            .Bind(PoliciesQueue, Exchange, ApplicationAcceptedKey)

            // Billing charges once a policy exists, never before: charging for a
            // policy that was never issued is a refund and an apology.
            .Queue(Billing, QueueType.Classic)
            .Bind(Billing, Exchange, PolicyIssuedKey)

            // Claims needs to know which policies exist.
            .Queue(Claims, QueueType.Classic)
            .Bind(Claims, Exchange, PolicyIssuedKey)

            // Everything, in order, for as long as the regulator asks. A wildcard
            // also means a new event type is audited the day it is introduced.
            .Queue(Audit, QueueType.Classic)
            .Bind(Audit, Exchange, "policy.#")

            // The request/reply queue. Not bound to the exchange: a request is
            // addressed to a queue, not routed to whoever happens to be listening.
            .Queue(PolicyLookup, QueueType.Classic)

            .Build();
}
