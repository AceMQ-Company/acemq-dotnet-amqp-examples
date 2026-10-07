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

' Taking the first premium: the one module where handling a message twice is real
' money.
'
' The same reasoning as payments in apps/01, and worth repeating because the
' monolith makes it easy to assume the problem went away. It did not. A redeploy
' mid-handler still leaves a message unacknowledged, and the redelivery still
' arrives at a module that already took the money.
'
' The idempotency store is in the database rather than in memory even though this
' is one process, because "one process" is a fact about today. The moment this
' module is lifted out -- the whole point of the arrangement -- an in-memory store
' becomes two stores that each think they are the only one, and each charges once.

Imports System.Collections.Generic
Imports System.Threading.Tasks

Imports AceMq.Amqp

Public NotInheritable Class BillingModule

    Private ReadOnly _charged As IPublisher(Of PremiumCharged)
    Private ReadOnly _charges As New List(Of String)()

    Private Sub New(mq As AceMqConnection)
        _charged = mq.Publisher(Of PremiumCharged)(Policies.Exchange, Policies.PremiumChargedKey)
    End Sub

    Public Shared Async Function StartAsync(mq As AceMqConnection, database As ConnectionSupplier) As Task(Of BillingModule)
        Dim started As New BillingModule(mq)

        Dim seen As New DbIdempotencyStore(database, TimeSpan.FromDays(7))
        Using connection = database()
            connection.Open()
            Using command = connection.CreateCommand()
                command.CommandText = seen.CreateTableSql()
                command.ExecuteNonQuery()
            End Using
        End Using

        ' The store is handed to the consumer rather than used by hand: the claim is
        ' taken before the handler and confirmed after it returns, which is the order
        ' that closes the window a manual "mark it afterwards" leaves open. Keyed by
        ' the envelope's id, so "sent twice" is one charge and "happened twice" is two.
        Dim options = ConsumerOptions.Prefetch(10).Idempotent(seen).As(CodecRegistry.ByName("json"))
        Await mq.ConsumeAsync(Of PolicyIssued)(Policies.Billing, options,
            Async Function(message)
                Dim issued = message.Payload
                Await started._charged.SendAsync(
                    New PremiumCharged With {
                        .PolicyId = issued.PolicyId, .Applicant = issued.Applicant,
                        .Amount = issued.AnnualPremium},
                    Envelope.Of("PremiumCharged").CorrelationId(issued.ApplicationId).Build())

                ' Counted once the charge is announced, so a publish that failed and
                ' was retried is not counted twice.
                SyncLock started._charges
                    started._charges.Add(issued.PolicyId)
                End SyncLock
                Return Ack.Accept()
            End Function)
        Return started
    End Function

    ''' <summary>One entry per charge actually taken; duplicates here would be the bug.</summary>
    Public ReadOnly Property Charges As IReadOnlyList(Of String)
        Get
            SyncLock _charges
                Return _charges.ToArray()
            End SyncLock
        End Get
    End Property

End Class
