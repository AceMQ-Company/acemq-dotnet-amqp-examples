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

' Deciding whether to accept an application, and at what price.
'
' Underwriting is the one part of this application that is genuinely a sequence:
' check the applicant against the register, price the risk, then decide. Each
' stage fails for its own reasons and is slow for its own reasons, which is what a
' pipeline is for -- a queue per stage, so a slow stage shows up as a deep queue
' you can point at, and can be retried and scaled without touching the others.
'
' Written as one consumer doing three things in order, all of that disappears:
' one queue, one failure mode, and one number that says "underwriting is slow".

Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class UnderwritingModule

    ''' <summary>Applications above this are a human's decision, not a rule's.</summary>
    Private Const ReferralThreshold As Integer = 500000

    Private ReadOnly _acceptances As IPublisher(Of ApplicationAccepted)
    Private ReadOnly _declines As IPublisher(Of ApplicationDeclined)
    Private _accepted As Integer
    Private _declined As Integer

    Private Sub New(mq As AceMqConnection)
        _acceptances = mq.Publisher(Of ApplicationAccepted)(Policies.Exchange, Policies.ApplicationAcceptedKey)
        _declines = mq.Publisher(Of ApplicationDeclined)(Policies.Exchange, Policies.ApplicationDeclinedKey)
    End Sub

    ''' <summary>What the register stage produces.</summary>
    Public Class Checked
        Public Property Application As ApplicationSubmitted = New ApplicationSubmitted()
        Public Property KnownToRegister As Boolean
    End Class

    ''' <summary>What the pricing stage produces.</summary>
    Public Class Priced
        Public Property Application As ApplicationSubmitted = New ApplicationSubmitted()
        Public Property AnnualPremium As Integer
        Public Property Refer As Boolean
    End Class

    Public Shared Async Function StartAsync(mq As AceMqConnection) As Task(Of UnderwritingModule)
        Dim started As New UnderwritingModule(mq)

        ' Step names are the queue suffixes, so they stay short and stable:
        ' renaming one strands whatever is in flight against the old name.
        '
        ' The retry policy is the register's in the Java app -- the register is
        ' somebody else's service and is periodically unavailable, which is a wait
        ' rather than a decline. The .NET builder takes one policy for the whole
        ' pipeline, so the other two stages have it too; neither of them fails.
        Dim pipeline = Await mq.Pipeline(Of ApplicationSubmitted)(Policies.UnderwritingPipeline) _
            .WithRetry(RetryPolicy.Exponential(4, TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5))) _
            .Step(Of Checked)("register", Function(application) Task.FromResult(CheckRegister(application))) _
            .Step(Of Priced)("price", Function(registered) Task.FromResult(Price(registered))) _
            .Step(Of Object)("decide", AddressOf started.DecideAsync) _
            .BuildAsync()

        ' Fed from the module's own queue rather than bound to the exchange itself:
        ' the pipeline owns its stages' queues, and what enters it is this module's
        ' decision. The correlation id goes in with it, so the decision at the end
        ' can carry it on.
        Dim json = ConsumerOptions.Defaults().As(CodecRegistry.ByName("json"))
        Await mq.ConsumeAsync(Of ApplicationSubmitted)(Policies.Underwriting, json,
            Async Function(message)
                Await pipeline.SendAsync(
                    message.Payload,
                    Envelope.Of("ApplicationSubmitted").CorrelationId(message.Payload.ApplicationId).Build())
                Return Ack.Accept()
            End Function)
        Return started
    End Function

    Private Shared Function CheckRegister(application As ApplicationSubmitted) As Checked
        ' A real one calls an industry service. What matters here is that it is the
        ' stage most likely to be slow, and it has its own queue to prove it.
        Return New Checked With {
            .Application = application,
            .KnownToRegister = application.Applicant.IndexOf("known", StringComparison.OrdinalIgnoreCase) >= 0}
    End Function

    Private Shared Function Price(registered As Checked) As Priced
        Dim application = registered.Application

        ' A rating table, compressed to one line. Older applicants and larger sums
        ' cost more.
        Dim baseRate = application.SumAssured \ 1000
        Dim ageLoading = Math.Max(0, application.AgeOfApplicant - 30) * 2
        Dim registerLoading = If(registered.KnownToRegister, baseRate \ 2, 0)

        Return New Priced With {
            .Application = application,
            .AnnualPremium = baseRate + ageLoading + registerLoading,
            .Refer = application.SumAssured > ReferralThreshold}
    End Function

    Private Async Function DecideAsync(priced As Priced) As Task(Of Object)
        Dim application = priced.Application
        Dim envelope = AceMq.Amqp.Envelope.Of("UnderwritingDecision").CorrelationId(application.ApplicationId).Build()

        If priced.Refer Then
            ' Declined here rather than parked, because "a human must look at this"
            ' is a real outcome of underwriting and not a failure of it. Modelling it
            ' as an error would put it in a dead-letter queue, where it would look
            ' like something broke.
            Await _declines.SendAsync(
                New ApplicationDeclined With {
                    .ApplicationId = application.ApplicationId, .Applicant = application.Applicant,
                    .Reason = $"sum assured of {application.SumAssured} is above the automatic limit"},
                envelope)
            Interlocked.Increment(_declined)
            Return Nothing
        End If

        Await _acceptances.SendAsync(
            New ApplicationAccepted With {
                .ApplicationId = application.ApplicationId, .Applicant = application.Applicant,
                .Product = application.Product, .SumAssured = application.SumAssured,
                .AnnualPremium = priced.AnnualPremium},
            envelope)
        Interlocked.Increment(_accepted)

        ' The last step: nothing goes on, and the pipeline counts it completed.
        Return Nothing
    End Function

    Public ReadOnly Property Accepted As Integer
        Get
            Return Volatile.Read(_accepted)
        End Get
    End Property

    ''' <summary>How many it referred or refused.</summary>
    Public ReadOnly Property Declined As Integer
        Get
            Return Volatile.Read(_declined)
        End Get
    End Property

End Class
