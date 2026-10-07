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

' Claims: the module that has to ask another module a question.
'
' Everything else in this application reacts to events, which is the right
' default. Claims cannot: before settling a claim it must know whether the policy
' is in force, and it needs the answer now, in the middle of a decision. An event
' cannot answer a question.
'
' So it asks, over the broker. The callee is in the same process, and a method call
' would work today and would be the wrong choice: the moment claims calls into
' policies directly, the two are one module. Asking costs a millisecond and keeps
' the seam.
'
' The timeout is the part not to skip. A request that waits for ever is how one
' slow module stops the whole application, monolith or not.

Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class ClaimsModule

    ''' <summary>Generous for an in-process hop, and still bounded.</summary>
    Private Shared ReadOnly LookupTimeout As TimeSpan = TimeSpan.FromSeconds(5)

    Private ReadOnly _requester As Requester
    Private ReadOnly _settlements As IPublisher(Of ClaimSettled)
    Private ReadOnly _rejections As IPublisher(Of ClaimRejected)
    Private _settled As Integer
    Private _rejected As Integer

    Private Sub New(mq As AceMqConnection, requester As Requester)
        _requester = requester
        _settlements = mq.Publisher(Of ClaimSettled)(Policies.Exchange, Policies.ClaimSettledKey)
        _rejections = mq.Publisher(Of ClaimRejected)(Policies.Exchange, Policies.ClaimRejectedKey)
    End Sub

    Public Shared Async Function StartAsync(mq As AceMqConnection) As Task(Of ClaimsModule)
        Dim started As New ClaimsModule(mq, Await mq.RequesterAsync())

        ' Claims listens for issued policies only to know they exist at all; the
        ' authoritative answer still comes from the lookup, because a policy can be
        ' cancelled after issue and this module keeps no copy of another's state.
        Dim json = ConsumerOptions.Defaults().As(CodecRegistry.ByName("json"))
        Await mq.ConsumeAsync(Of PolicyIssued)(Policies.Claims, json,
            Function(message) Task.FromResult(Ack.Accept()))
        Return started
    End Function

    ''' <summary>Assesses a claim against a policy, and returns the claim id.</summary>
    Public Async Function SubmitAsync(policyId As String, amount As Integer, description As String) As Task(Of String)
        Dim claimId = "CLM-" & Guid.NewGuid().ToString("N").Substring(0, 8)
        Dim envelope = AceMq.Amqp.Envelope.Of("Claim").CorrelationId(policyId).Build()

        Dim status As PolicyStatus
        Try
            status = Await _requester.RequestAsync(Of PolicyQuery, PolicyStatus)(
                "", Policies.PolicyLookup, New PolicyQuery With {.PolicyId = policyId},
                LookupTimeout, CancellationToken.None)
        Catch e As RequestTimedOutException
            ' Not an answer, and must not be treated as "no". Refusing a valid claim
            ' because a lookup was slow is the failure mode worth being explicit
            ' about.
            Throw New InvalidOperationException(
                $"could not establish whether {policyId} is in force, so claim {claimId} " &
                "was neither settled nor rejected; it must be retried", e)
        End Try

        If Not status.InForce Then
            Await _rejections.SendAsync(
                New ClaimRejected With {.ClaimId = claimId, .PolicyId = policyId, .Reason = "no policy in force"},
                envelope)
            Interlocked.Increment(_rejected)
            Return claimId
        End If

        ' A real assessment is a great deal more than this. What matters is that it
        ' happened after an authoritative answer rather than after a guess.
        Await _settlements.SendAsync(
            New ClaimSettled With {.ClaimId = claimId, .PolicyId = policyId, .Paid = amount}, envelope)
        Interlocked.Increment(_settled)
        Return claimId
    End Function

    Public ReadOnly Property Settled As Integer
        Get
            Return Volatile.Read(_settled)
        End Get
    End Property

    ''' <summary>Claims rejected because no policy was in force.</summary>
    Public ReadOnly Property Rejected As Integer
        Get
            Return Volatile.Read(_rejected)
        End Get
    End Property

    ''' <summary>Lookups that gave up waiting. Every scenario expects none.</summary>
    Public ReadOnly Property LookupsTimedOut As Long
        Get
            Return _requester.TimedOut
        End Get
    End Property

End Class
