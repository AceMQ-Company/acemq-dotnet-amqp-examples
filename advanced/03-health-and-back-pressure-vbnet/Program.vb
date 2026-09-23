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

' What Health() says when the broker has stopped accepting publishes, in VB.NET.
'
'   docker compose up -d
'   dotnet run --project advanced/03-health-and-back-pressure-vbnet
'
' A broker under memory or disk pressure raises an alarm and stops reading from
' the connections that are publishing. The connection is open, the process is
' fine, and nothing is lost -- the broker is protecting itself and will start
' reading again when the pressure passes.
'
' The question this example answers is what a health endpoint should say about
' that, because the obvious answer is wrong. If the service reports unhealthy,
' an orchestrator restarts it into the same blocked broker, having thrown away
' whatever it was holding, and does that to every replica at once. So Health()
' reports UP with the reason attached, and leaves "should this count against us"
' to the application -- which is a policy, and belongs where policies live.
'
' Up to 0.6.0 the library reported Degraded here. Because the aggregate takes
' the worst report, that reading overruled any more careful one a caller had
' composed alongside it. 0.7.0 changed it, and this example is the check that it
' stayed changed.
'
' THIS EXAMPLE NEEDS A BROKER OF ITS OWN, and the reason is worth reading before
' copying any of it. A memory alarm is raised on the node, not on the
' connection: every publisher on that broker blocks, not just this one. Run this
' against the broker the other examples share and it blocks them too, so compose
' gives it `broker-health` and nothing else goes near that one. The alternative
' -- running it last and hoping -- is a test that fails on a Tuesday for reasons
' nobody can reconstruct.

Imports System.Collections.Generic
Imports System.Diagnostics
Imports System.Linq
Imports System.Threading
Imports System.Threading.Tasks

Imports AceMq.Amqp
Imports AceMq.Amqp.RabbitMq

Public Class Reading
    Public Property Id As String = ""
End Class

' Something of the application's own in the health report, which is the reason
' the report is a list rather than a single status. Its state is settable here
' only so one example can show it both ways; in a service it would be reading a
' queue depth, a lag, or the age of the last successful run.
Public Class LaggingProjection
    Implements IHealthContributor

    Public Property Lag As HealthStatus = HealthStatus.Up

    Public ReadOnly Property Name As String Implements IHealthContributor.Name
        Get
            Return "orders-projection"
        End Get
    End Property

    Public Function Report() As HealthReport Implements IHealthContributor.Report
        Return New HealthReport(Name, Lag,
            New Dictionary(Of String, String) From {
                {"behindBy", If(Lag = HealthStatus.Up, "0", "4200")}})
    End Function
End Class

' The 0.6.0 reading, reconstructed as what it always should have been: this
' application's policy rather than the library's. A service that genuinely wants
' back pressure to count against it writes these few lines and registers them,
' and a service that does not simply does not.
Public Class BlockedIsDegraded
    Implements IHealthContributor

    Private ReadOnly _mq As AceMqConnection

    Public Sub New(connection As AceMqConnection)
        _mq = connection
    End Sub

    Public ReadOnly Property Name As String Implements IHealthContributor.Name
        Get
            Return "back-pressure-policy"
        End Get
    End Property

    Public Function Report() As HealthReport Implements IHealthContributor.Report
        Return New HealthReport(
            Name,
            If(_mq.IsBlocked, HealthStatus.Degraded, HealthStatus.Up),
            New Dictionary(Of String, String) From {
                {"reason", If(_mq.BlockedReason, "(not blocked)")}})
    End Function
End Class

' A contributor that throws is itself a health problem, and must not take the
' whole report down with it -- an endpoint that returns 500 because one probe had
' a bad afternoon tells an operator nothing about the other five.
Public Class Broken
    Implements IHealthContributor

    Public ReadOnly Property Name As String Implements IHealthContributor.Name
        Get
            Return "a-contributor-that-throws"
        End Get
    End Property

    Public Function Report() As HealthReport Implements IHealthContributor.Report
        Throw New InvalidOperationException("no route to the metrics store")
    End Function
End Class

Module Program

    ' Distinct from the C# example's name: both run against one broker.
    Private Const QueueName As String = "health-back-pressure-vbnet"

    Function Main() As Integer
        Return RunAsync().GetAwaiter().GetResult()
    End Function

    Private Async Function RunAsync() As Task(Of Integer)
        Transports.Register(New RabbitMqTransport())

        Dim container = Environment.GetEnvironmentVariable("ACEMQ_HEALTH_CONTAINER")
        If String.IsNullOrEmpty(container) Then container = "acemq-examples-broker-health"

        Using cancellation As New CancellationTokenSource(TimeSpan.FromSeconds(180))
            Dim token = cancellation.Token

            Dim mq = Await AceMqConnection.ConnectAsync(
                ConnectionConfig.ForUrl(BrokerUrl()) _
                    .ClientName("examples/03-health-and-back-pressure-vbnet") _
                    .ConfirmTimeout(TimeSpan.FromSeconds(2)) _
                    .Build(),
                New JsonCodec(),
                token)

            Try
                Await mq.DeclareQueueAsync(QueueName)

                ' ---- nothing wrong ----------------------------------------
                Dim healthy = mq.Health()
                Show("a broker that is fine", healthy)
                Check(healthy.Status = HealthStatus.Up,
                      $"a working connection reported {healthy.Status}")
                Check(healthy.Reports.Count = 1,
                      "something other than the connection is already reporting")

                Dim connection = Named(healthy, "connection")
                Check(connection.Details("open") = "true", "the connection is not open")
                Check(connection.Details("blocked") = "false",
                      "the connection is blocked before anything blocked it")
                Check(Not connection.Details.ContainsKey("blockedReason"),
                      "an unblocked connection named a reason for being blocked")

                ' ---- the worst report wins --------------------------------
                '
                ' Which is why a lagging projection is visible at all.
                ' Averaging health, or letting the connection speak for the
                ' whole service, hides exactly the component that has stopped.
                Dim projection As New LaggingProjection()
                mq.RegisterHealth(projection)
                Check(mq.Health().Status = HealthStatus.Up,
                      "an Up contributor moved the aggregate")

                projection.Lag = HealthStatus.Degraded
                Dim behind = mq.Health()
                Show("a projection falling behind", behind)
                Check(behind.Status = HealthStatus.Degraded,
                      $"a Degraded contributor left the aggregate at {behind.Status}")
                Check(Named(behind, "connection").Status = HealthStatus.Up,
                      "the connection was dragged down by a contributor that is not the connection")
                projection.Lag = HealthStatus.Up

                ' ---- now actually block the broker ------------------------
                '
                ' The alarm alone is not enough: RabbitMQ blocks a connection
                ' when it next tries to publish, so something has to publish.
                ' The watermark is put back in the Finally, and putting it back
                ' is the whole reason there is a Finally -- a broker left with a
                ' 0.0001 watermark is a broker that blocks every later run of
                ' every other example, and the symptom is a publish that hangs
                ' with nothing in any log to say why.
                Console.WriteLine($"{vbLf}dropping the memory high watermark on {container}")
                Rabbitmqctl(container, "set_vm_memory_high_watermark", "0.0001")
                Try
                    Dim publisher = mq.Publisher(Of Reading)("", QueueName)
                    Dim attempt = 0
                    Do While attempt < 40 AndAlso Not mq.IsBlocked
                        Try
                            Await publisher.SendAsync(New Reading With {.Id = "r-" & attempt}) _
                                .WaitAsync(TimeSpan.FromMilliseconds(500), token)
                        Catch ex As TimeoutException
                            ' A publish that never confirms because the broker
                            ' has stopped reading is the state being waited for,
                            ' not a failure.
                        End Try

                        Await Task.Delay(250, token)
                        attempt += 1
                    Loop

                    Check(mq.IsBlocked, "the broker never blocked, so this example proved nothing")
                    Console.WriteLine($"IsBlocked={mq.IsBlocked}, BlockedReason={mq.BlockedReason}")
                    Check(mq.BlockedReason IsNot Nothing,
                          "the connection is blocked but will not say why")
                    Check(mq.BlockedReason.IndexOf("memory", StringComparison.OrdinalIgnoreCase) >= 0,
                          $"the broker blocked for some other reason: {mq.BlockedReason}")

                    ' ---- THE READING THIS EXAMPLE EXISTS FOR --------------
                    Dim blocked = mq.Health()
                    Show("a broker applying back pressure", blocked)

                    Check(blocked.Status = HealthStatus.Up,
                          $"a blocked connection reported {blocked.Status}; " &
                          "since 0.7.0 it reports Up with the reason")
                    Dim blockedConnection = Named(blocked, "connection")
                    Check(blockedConnection.Status = HealthStatus.Up,
                          $"the connection report was {blockedConnection.Status}")
                    Check(blockedConnection.Details("open") = "true",
                          "a blocked connection was reported as closed; it is open, and that is the point")
                    Check(blockedConnection.Details("blocked") = "true",
                          "the health report does not mention that the connection is blocked")

                    Dim why As String = Nothing
                    Check(blockedConnection.Details.TryGetValue("blockedReason", why) AndAlso
                          why = mq.BlockedReason,
                          "the health report does not carry the reason the broker gave")

                    ' ---- and it answers without asking the broker ----------
                    '
                    ' The property that makes it usable as a liveness probe.
                    ' Health() reads state the connection already has; it sends
                    ' nothing and waits for nothing, so a broker that has
                    ' stopped reading cannot make the probe hang -- which would
                    ' get the process killed by the very back pressure it was
                    ' reporting on.
                    Dim clock = Stopwatch.StartNew()
                    For i = 1 To 1000
                        mq.Health()
                    Next
                    clock.Stop()
                    ' Not named "each": Each is a VB.NET keyword, from For Each,
                    ' and the compiler reports it as "keyword is not valid as an
                    ' identifier" several lines further down than the Dim.
                    Dim perCall = clock.Elapsed.TotalMilliseconds / 1000
                    Console.WriteLine(
                        $"1000 Health() calls on a blocked connection took " &
                        $"{clock.Elapsed.TotalMilliseconds:F1}ms, {perCall * 1000:F1}us each")
                    Check(perCall < 1.0,
                          $"Health() took {perCall:F3}ms per call on a blocked connection, " &
                          "which is too slow to be free")

                    ' ---- wanting the old reading back ----------------------
                    mq.RegisterHealth(New BlockedIsDegraded(mq))
                    Dim policy = mq.Health()
                    Show("the same broker, with a policy that counts back pressure", policy)
                    Check(policy.Status = HealthStatus.Degraded,
                          "a contributor that calls a blocked connection Degraded " &
                          "did not move the aggregate")
                    Check(Named(policy, "connection").Status = HealthStatus.Up,
                          "the library's own reading changed, and it should not have")
                Finally
                    ' 0.6 is RabbitMQ 4's default. Leaving the broker usable is
                    ' part of the example: the next thing to run against it is
                    ' somebody's real work, and an alarm nobody raised on
                    ' purpose is a bad afternoon.
                    Console.WriteLine($"{vbLf}putting the memory high watermark back")
                    Rabbitmqctl(container, "set_vm_memory_high_watermark", "0.6")
                End Try

                ' ---- the alarm clears -------------------------------------
                Dim waited = 0
                Do While waited < 40 AndAlso mq.IsBlocked
                    Await Task.Delay(250, token)
                    waited += 1
                Loop

                Dim recovered = mq.Health()
                Show("the alarm cleared", recovered)
                Check(Not mq.IsBlocked,
                      "the broker is still blocked after the watermark was restored")
                Check(mq.BlockedReason Is Nothing,
                      $"the connection still names a reason: {mq.BlockedReason}")
                Check(recovered.Status = HealthStatus.Up,
                      $"the aggregate came back as {recovered.Status}")
                Check(Not Named(recovered, "connection").Details.ContainsKey("blockedReason"),
                      "the reason is still in the report after the alarm cleared")

                ' ---- a contributor that throws -----------------------------
                mq.RegisterHealth(New Broken())
                Dim broke = mq.Health()
                Show("one probe having a bad afternoon", broke)
                Check(broke.Status = HealthStatus.Down,
                      $"a throwing contributor reported {broke.Status}")
                Check(Named(broke, "a-contributor-that-throws").Details("error") =
                          "no route to the metrics store",
                      "the exception the contributor threw was not carried into the report")
                Check(Named(broke, "connection").Status = HealthStatus.Up,
                      "one contributor throwing took the connection's own report down with it")

                Await mq.DeleteQueueAsync(QueueName)
                Await mq.DeleteQueueAsync(QueueName & ".dlq")
                Await mq.DeleteQueueAsync(QueueName & ".parked")
            Finally
                mq.Dispose()
            End Try

            Return 0
        End Using
    End Function

    Private Sub Show(label As String, snapshot As AggregateHealth)
        Console.WriteLine($"{vbLf}{label}: {snapshot.Status}")
        For Each entry In snapshot.Reports
            Dim details = String.Join(", ",
                entry.Details.Select(Function(d) $"{d.Key}={d.Value}"))
            Console.WriteLine($"  {entry.Name}: {entry.Status}  [{details}]")
        Next
    End Sub

    Private Function Named(snapshot As AggregateHealth, wanted As String) As HealthReport
        Dim found = snapshot.Reports.FirstOrDefault(Function(r) r.Name = wanted)
        Check(found IsNot Nothing, $"nothing called {wanted} was in the health report")
        Return found
    End Function

    ' Reaching for the broker's own control tool, because nothing in AMQP can
    ' raise a memory alarm and an example that mocked one would be proving
    ' something about the mock. HOME is set because rabbitmqctl reads the Erlang
    ' cookie from it, and the broker runs with HOME=/tmp.
    Private Sub Rabbitmqctl(container As String, ParamArray arguments As String())
        Dim start As New ProcessStartInfo("docker") With {
            .RedirectStandardOutput = True,
            .RedirectStandardError = True}

        Dim fixedPart = New String() {
            "exec", "-u", "rabbitmq", "-e", "HOME=/var/lib/rabbitmq",
            container, "rabbitmqctl"}
        For Each argument In fixedPart.Concat(arguments)
            start.ArgumentList.Add(argument)
        Next

        ' Named "runner" rather than "process": VB.NET is case-insensitive, so a
        ' variable called process makes Process.Start a reference to the variable
        ' being declared, and the compiler says only that the type cannot be
        ' inferred from an expression containing itself.
        Dim runner = Process.Start(start)
        Check(runner IsNot Nothing, "docker could not be started")
        Using runner
            Dim output = runner.StandardOutput.ReadToEnd() & runner.StandardError.ReadToEnd()
            runner.WaitForExit()
            Check(runner.ExitCode = 0,
                  $"`rabbitmqctl {String.Join(" ", arguments)}` on {container} failed: " &
                  output.Trim())
        End Using
    End Sub

    ' An example that prints the right answer whatever happened is an example
    ' that cannot fail, and CI running it proves nothing. This throws, which
    ' makes the process exit non-zero.
    Private Sub Check(held As Boolean, wrong As String)
        If Not held Then Throw New InvalidOperationException(wrong)
    End Sub

    ' Its own broker, not the one the other examples share -- see the note at the
    ' top. ACEMQ_URL is deliberately not consulted: picking it up would point a
    ' memory alarm at whatever that variable happens to name.
    Private Function BrokerUrl() As String
        Dim url = Environment.GetEnvironmentVariable("ACEMQ_HEALTH_URL")
        If String.IsNullOrEmpty(url) Then
            Return "amqp://guest:guest@localhost:5673/"
        End If
        Return url
    End Function

End Module
