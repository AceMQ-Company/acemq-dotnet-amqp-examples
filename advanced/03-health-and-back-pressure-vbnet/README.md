# advanced/03 — health and back pressure, in VB.NET

A broker under memory or disk pressure raises an alarm and stops reading from the
connections that are publishing. The connection is open, the process is fine, and
nothing is lost — the broker is protecting itself and will start reading again
when the pressure passes.

This example blocks a real broker and asks `Health()` what it thinks.

The same example exists in C# at
[advanced/03-health-and-back-pressure-csharp](../03-health-and-back-pressure-csharp),
and the reasoning is written out in full there. This page covers the same ground
with the VB.NET specifics.

## What it shows

- **`Health()` on a genuinely blocked broker** — blocked by dropping the memory
  high watermark with `rabbitmqctl` and publishing until the alarm lands, not by
  mocking anything.
- **It reports `Up`, with the reason**, which is a behaviour change in 0.7.0.
- **It answers in microseconds**, because it asks the broker nothing.
- **Health contributors of your own**, including one that reconstructs the old
  reading as application policy, and one that throws.

## Running it

```bash
docker compose up -d
dotnet run --project advanced/03-health-and-back-pressure-vbnet
```

## What to look for

```
a broker that is fine: Up
  connection: Up  [open=true, blocked=false, transport=rabbitmq, inFlight=0, held=0]

dropping the memory high watermark on acemq-examples-broker-health
IsBlocked=True, BlockedReason=low on memory

a broker applying back pressure: Up
  connection: Up  [open=true, blocked=true, transport=rabbitmq, inFlight=0, held=0, blockedReason=low on memory]
1000 Health() calls on a blocked connection took 1.9ms, 1.9us each

the same broker, with a policy that counts back pressure: Degraded
  ...
  back-pressure-policy: Degraded  [reason=low on memory]

putting the memory high watermark back

the alarm cleared: Up
```

## A blocked connection is `Up`, and that is the point

The obvious reading is that a broker refusing to accept publishes is a problem
and the service should say so. Follow it through: the service reports unhealthy,
an orchestrator restarts it into the same blocked broker having thrown away
whatever it was holding, and does that to every replica at once.

So the library reports `Up` and attaches the reason. Everything needed to decide
otherwise is in the report; what is not there is the decision, because that is a
policy and not a messaging library's to make.

**Up to 0.6.0 this reported `Degraded`**, and because `AggregateHealth` takes the
worst report, that reading overruled any more careful one a caller had composed
alongside it.

## A contributor in VB.NET

```vb
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
```

Registered with `mq.RegisterHealth(...)`, the aggregate goes `Degraded` again —
and the connection's own report stays `Up`, so an operator can still see which of
the two opinions is which.

## Two names this example could not use

VB.NET is case-insensitive and has a larger keyword list than C#, and both bit
here:

- **`each`** is a keyword, from `For Each`. `Dim each = ...` is rejected as
  "keyword is not valid as an identifier", reported several lines below the
  `Dim`. The variable is `perCall`.
- **`process`** collides with `System.Diagnostics.Process`, so
  `Dim process = Process.Start(...)` becomes a variable whose type is inferred
  from an expression containing itself — and that is exactly what the compiler
  says, which is a long way from "rename it". The variable is `runner`.

Both belong to the same family as the renames listed in the repository
[README](../../README.md): a VB name that shadows a type it is initialised from
produces an error about inference rather than about naming.

## Why it has a broker to itself

A memory alarm is raised **on the node, not on the connection**, so every
publisher on that broker blocks. `compose.yaml` gives this example
`broker-health` on port 5673 and nothing else goes near it; CI starts a second
container for the same reason.

The watermark is restored in a `Finally` and the example asserts the alarm
actually cleared, because leaving a broker alarmed makes it useless for whatever
runs next.

`ACEMQ_HEALTH_URL` and `ACEMQ_HEALTH_CONTAINER` point it elsewhere. It
deliberately does **not** read `ACEMQ_URL`: picking that up would aim a memory
alarm at whatever that variable happens to name.

## How this stays honest

Every reading is asserted: that the broker really blocked, that the reason names
memory, that the aggregate is `Up` rather than `Degraded`, that the details carry
the reason the broker gave, that a call costs less than a millisecond, that a
policy contributor moves the aggregate without moving the connection's report,
that the alarm cleared and the reason went away, and that a throwing contributor
is `Down` without taking the rest with it. The example exits non-zero if any of
them is wrong. CI compiles and runs it against a real broker on every push.
