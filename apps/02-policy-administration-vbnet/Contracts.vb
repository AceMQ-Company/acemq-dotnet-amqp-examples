' Copyright 2026 AceMQ.
'
' Licensed under the Apache License, Version 2.0 (the "License");
' you may not use this file except in compliance with the License.
' You may obtain a copy of the License at
'
'     https://www.apache.org/licenses/LICENSE-2.0
'
' Unless required by applicable law or agreed to in writing, software
' distributed under the License is distributed on an "AS IS" BASIS,
' WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
' See the License for the specific language governing permissions and
' limitations under the License.

' What every module in this monolith agrees on, and nothing else.
'
' These modules run in one process, so nothing but discipline stops one from
' reaching into another's classes. In the Java app the boundary is a Maven
' dependency graph: every module depends on the contracts jar and on none of its
' siblings. Here it is a convention -- each module's file uses this one and no
' other module's types -- and Program.vb is the only file that names more than
' one module.
'
' That is what makes the monolith modular rather than merely large. A module that
' only ever received events can be lifted into its own process by changing where
' it connects: no call sites to find, because there are none.
'
' What is deliberately not here: any module's domain model, any persistence, any
' shared helper. Those turn a contracts file into a common-utils file, and a
' common-utils file is how modular monoliths become ordinary ones.

Imports System.Collections.Generic
Imports System.Linq

Imports AceMq.Amqp

' ---- events -------------------------------------------------------------------
'
' Classes with settable properties, which is what System.Text.Json reads into from
' VB.NET. The JSON codec camelCases, so ApplicationId is "applicationId" on the
' wire -- the field the Java records and the C# twin's records produce.

''' <summary>A broker submitted an application. Published by policies, from its outbox.</summary>
Public Class ApplicationSubmitted
    Public Property ApplicationId As String = ""
    Public Property Applicant As String = ""
    Public Property Product As String = ""
    Public Property SumAssured As Integer
    Public Property AgeOfApplicant As Integer
End Class

''' <summary>Underwriting reached a decision and priced it.</summary>
Public Class ApplicationAccepted
    Public Property ApplicationId As String = ""
    Public Property Applicant As String = ""
    Public Property Product As String = ""
    Public Property SumAssured As Integer
    Public Property AnnualPremium As Integer
End Class

''' <summary>Underwriting refused it, with a reason a human can act on.</summary>
Public Class ApplicationDeclined
    Public Property ApplicationId As String = ""
    Public Property Applicant As String = ""
    Public Property Reason As String = ""
End Class

''' <summary>A policy exists. Published by policies once underwriting accepted.</summary>
Public Class PolicyIssued
    Public Property PolicyId As String = ""
    Public Property ApplicationId As String = ""
    Public Property Applicant As String = ""
    Public Property Product As String = ""
    Public Property AnnualPremium As Integer
End Class

''' <summary>A document belongs to a policy.</summary>
''' <remarks>
''' The document itself is <b>not</b> here. This carries a claim check -- the key
''' it was stored under and how big it is -- because a medical report scanned at
''' 300 dpi is tens of megabytes and a broker is not a filesystem.
''' </remarks>
Public Class DocumentStored
    Public Property PolicyId As String = ""
    Public Property DocumentKey As String = ""
    Public Property Kind As String = ""
    Public Property Bytes As Integer
End Class

''' <summary>The first premium was taken. Published by billing.</summary>
Public Class PremiumCharged
    Public Property PolicyId As String = ""
    Public Property Applicant As String = ""
    Public Property Amount As Integer
End Class

''' <summary>Somebody claimed against a policy.</summary>
Public Class ClaimSubmitted
    Public Property ClaimId As String = ""
    Public Property PolicyId As String = ""
    Public Property Amount As Integer
    Public Property Description As String = ""
End Class

''' <summary>The claim was assessed.</summary>
Public Class ClaimSettled
    Public Property ClaimId As String = ""
    Public Property PolicyId As String = ""
    Public Property Paid As Integer
End Class

''' <summary>It was not, and why.</summary>
Public Class ClaimRejected
    Public Property ClaimId As String = ""
    Public Property PolicyId As String = ""
    Public Property Reason As String = ""
End Class

''' <summary>What claims asks.</summary>
Public Class PolicyQuery
    Public Property PolicyId As String = ""
End Class

''' <summary>What policies answers. A class rather than a Boolean, so it can grow a reason.</summary>
Public Class PolicyStatus
    Public Property PolicyId As String = ""
    Public Property InForce As Boolean
    Public Property AnnualPremium As Integer
End Class

' A class of Shared members rather than a Module: a module's members are in scope
' everywhere without qualification, and VB.NET ignores case, so a constant called
' Billing would meet every billing in the project.
Public NotInheritable Class Policies

    Private Sub New()
    End Sub

    ' Every broker object this app declares starts with this, and nothing else
    ' differs from the Java app. The C# and VB.NET twins run against one broker in
    ' CI; two apps reading one "policy.billing" would each take half of the other's
    ' policies. Routing keys and envelope types are NOT prefixed: they are the
    ' contract, and they are the Java app's, character for character.
    Private Const Prefix As String = "dotnet-vbnet."

    ''' <summary>One topic exchange for the whole application.</summary>
    Public Const Exchange As String = Prefix & "policy"

    ' ---- routing keys -------------------------------------------------------------

    Public Const ApplicationSubmittedKey As String = "policy.application.submitted"
    Public Const ApplicationAcceptedKey As String = "policy.application.accepted"
    Public Const ApplicationDeclinedKey As String = "policy.application.declined"
    Public Const PolicyIssuedKey As String = "policy.policy.issued"
    Public Const DocumentStoredKey As String = "policy.document.stored"
    Public Const PremiumChargedKey As String = "policy.premium.charged"
    Public Const ClaimSubmittedKey As String = "policy.claim.submitted"
    Public Const ClaimSettledKey As String = "policy.claim.settled"
    Public Const ClaimRejectedKey As String = "policy.claim.rejected"

    ' ---- queues -------------------------------------------------------------------
    '
    ' A queue per module, named for the module, as in apps/01: two modules wanting
    ' the same event each get their own copy.

    Public Const Underwriting As String = Prefix & "policy.underwriting"
    Public Const PoliciesQueue As String = Prefix & "policy.policies"
    Public Const Billing As String = Prefix & "policy.billing"
    Public Const Claims As String = Prefix & "policy.claims"

    ''' <summary>Everything, for the audit trail.</summary>
    ''' <remarks>
    ''' Not decoration. Writing the Java app without it produced a real failure:
    ''' claims and documents published events nothing was bound to, and the library
    ''' refused the publish rather than discarding it.
    ''' </remarks>
    Public Const Audit As String = Prefix & "policy.audit"

    ''' <summary>Where claims asks policies whether a policy is in force. Request/reply, not an event.</summary>
    Public Const PolicyLookup As String = Prefix & "policy.lookup"

    ''' <summary>
    ''' The name underwriting's pipeline declares its stage queues under:
    ''' <c>{this}.register</c>, <c>{this}.price</c>, <c>{this}.decide</c>.
    ''' </summary>
    Public Const UnderwritingPipeline As String = Prefix & "underwriting"

    Public Shared ReadOnly PipelineSteps As IReadOnlyList(Of String) = {"register", "price", "decide"}

    ''' <summary>Every queue a consumer reads, and so every queue with a .dlq and .parked beside it.</summary>
    Public Shared ReadOnly Property ConsumedQueues As IReadOnlyList(Of String)
        Get
            Return {Underwriting, PoliciesQueue, Billing, Claims, PolicyLookup} _
                .Concat(PipelineSteps.Select(Function(stepName) $"{UnderwritingPipeline}.{stepName}")) _
                .ToArray()
        End Get
    End Property

    ''' <summary>The whole application's topology, as one value.</summary>
    ''' <remarks>
    ''' Applied once at start-up: one process, so once is enough -- and still
    ''' declared here rather than assembled from each module's fragment, because a
    ''' module that declares its own queue is a module that can be started against
    ''' an exchange nobody created. Classic queues, as in the Java app.
    ''' </remarks>
    Public Shared Function Everything() As Topology
        ' Underwriting acts on submissions; policies issues once underwriting has
        ' accepted; billing charges once a policy exists and never before; claims
        ' needs to know which policies exist; audit takes everything, so a new event
        ' type is audited the day it is introduced. The lookup queue is not bound to
        ' the exchange: a request is addressed to a queue, not routed to whoever
        ' happens to be listening.
        Return Topology.Define() _
            .Exchange(Exchange, "topic") _
            .Queue(Underwriting, QueueType.Classic) _
            .Bind(Underwriting, Exchange, ApplicationSubmittedKey) _
            .Queue(PoliciesQueue, QueueType.Classic) _
            .Bind(PoliciesQueue, Exchange, ApplicationAcceptedKey) _
            .Queue(Billing, QueueType.Classic) _
            .Bind(Billing, Exchange, PolicyIssuedKey) _
            .Queue(Claims, QueueType.Classic) _
            .Bind(Claims, Exchange, PolicyIssuedKey) _
            .Queue(Audit, QueueType.Classic) _
            .Bind(Audit, Exchange, "policy.#") _
            .Queue(PolicyLookup, QueueType.Classic) _
            .Build()
    End Function

End Class
