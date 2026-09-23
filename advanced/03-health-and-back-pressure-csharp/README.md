# advanced/03 — health and back pressure, in C#

A broker under memory or disk pressure raises an alarm and stops reading from the
connections that are publishing. The connection is open, the process is fine, and
nothing is lost — the broker is protecting itself and will start reading again
when the pressure passes.

This example blocks a real broker and asks `Health()` what it thinks.

The same example exists in VB.NET at
[advanced/03-health-and-back-pressure-vbnet](../03-health-and-back-pressure-vbnet).

## What it shows

- **`Health()` on a genuinely blocked broker** — blocked by dropping the memory
  high watermark with `rabbitmqctl` and publishing until the alarm lands, not by
  mocking anything.
- **It reports `Up`, with the reason**, which is a behaviour change in 0.7.0.
- **It answers in microseconds**, because it asks the broker nothing — the
  property that makes it usable as a liveness probe on a blocked broker.
- **Health contributors of your own**, including one that reconstructs the old
  reading as application policy, and one that throws.

## Running it

```bash
docker compose up -d
dotnet run --project advanced/03-health-and-back-pressure-csharp
```

## What to look for

```
a broker that is fine: Up
  connection: Up  [open=true, blocked=false, transport=rabbitmq, inFlight=0, held=0]

a projection falling behind: Degraded
  connection: Up  [open=true, blocked=false, transport=rabbitmq, inFlight=0, held=0]
  orders-projection: Degraded  [behindBy=4200]

dropping the memory high watermark on acemq-examples-broker-health
IsBlocked=True, BlockedReason=low on memory

a broker applying back pressure: Up
  connection: Up  [open=true, blocked=true, transport=rabbitmq, inFlight=0, held=0, blockedReason=low on memory]
  orders-projection: Up  [behindBy=0]
1000 Health() calls on a blocked connection took 0.9ms, 0.9us each

the same broker, with a policy that counts back pressure: Degraded
  connection: Up  [open=true, blocked=true, transport=rabbitmq, inFlight=0, held=0, blockedReason=low on memory]
  orders-projection: Up  [behindBy=0]
  back-pressure-policy: Degraded  [reason=low on memory]

putting the memory high watermark back

the alarm cleared: Up
  connection: Up  [open=true, blocked=false, transport=rabbitmq, inFlight=0, held=0]

one probe having a bad afternoon: Down
  ...
  a-contributor-that-throws: Down  [error=no route to the metrics store]
```

## A blocked connection is `Up`, and that is the point

The obvious reading is that a broker refusing to accept publishes is a problem
and the service should say so. Follow it through: the service reports unhealthy,
an orchestrator restarts it into the same blocked broker having thrown away
whatever it was holding, and does that to every replica at once. The broker is
now under the same pressure plus a reconnection storm.

So the library reports `Up` and attaches the reason:

```
connection: Up  [open=true, blocked=true, ..., blockedReason=low on memory]
```

Everything needed to decide otherwise is in the report. What is *not* there is
the decision, because that is a policy — how long back pressure has to last
before it counts, whether this service can wait it out, whether the queue it
feeds has somewhere else to go — and none of those are a messaging library's to
make.

**Up to 0.6.0 this reported `Degraded`.** That reading was not merely
debatable, it was contagious: `AggregateHealth` takes the worst report, so the
library's opinion overruled any more careful one a caller had composed alongside
it. The Java library's Spring Boot health indicator reports it the same way, for
the same reason.

## Wanting the old reading back

Write it down as your own:

```csharp
public sealed class BlockedIsDegraded : IHealthContributor
{
    private readonly AceMqConnection _mq;
    public BlockedIsDegraded(AceMqConnection mq) => _mq = mq;

    public string Name => "back-pressure-policy";

    public HealthReport Report() =>
        new HealthReport(
            Name,
            _mq.IsBlocked ? HealthStatus.Degraded : HealthStatus.Up,
            new Dictionary<string, string> { ["reason"] = _mq.BlockedReason ?? "(not blocked)" });
}
```

Registered, the aggregate goes `Degraded` again — and the connection's own report
stays `Up`, so an operator can still see which of the two opinions is which. The
difference from 0.6.0 is not the reading, it is whose reading it is.

## It answers without asking the broker

```
1000 Health() calls on a blocked connection took 0.9ms, 0.9us each
```

`Health()` reads state the connection already holds. It sends no frame and waits
for nothing, which is why a broker that has stopped reading cannot make it hang.
A health endpoint that did a round trip here would time out, and the process
would be killed by the very back pressure it was trying to report on.

The example asserts the average is under a millisecond. The real figure is about
a thousand times better than that; the assertion is there to fail loudly if this
ever grows an I/O path, not to measure the machine.

## The worst report wins

`AggregateHealth.Status` is the worst of everything reporting. Averaging health,
or letting the connection speak for the whole service, hides exactly the
component that has stopped — which is the one worth knowing about. Ordered queues
register themselves, so a halted partition shows up here without anything being
wired: a halted partition is a consumer that has stopped without the connection
or the process noticing.

A contributor that throws is reported `Down` with its message in the details,
rather than taking the whole report with it. An endpoint that returns a 500
because one probe had a bad afternoon tells an operator nothing about the other
five.

## Why it has a broker to itself

A memory alarm is raised **on the node, not on the connection**. Every publisher
on that broker blocks, not just this one.

So `compose.yaml` gives this example `broker-health` on port 5673 and nothing
else goes near it, and CI starts a second container for the same reason. The
alternative — running this one last and hoping the watermark is restored before
anything else publishes — is a suite that fails on a Tuesday for reasons nobody
can reconstruct afterwards.

The example puts the watermark back in a `finally`, because leaving a broker
alarmed is leaving it useless: the next thing to run against it is somebody's
real work, and a publish that hangs with an alarm nobody raised on purpose is a
long afternoon. It also asserts the alarm actually cleared, since a `finally`
that silently failed would be worse than none.

`ACEMQ_HEALTH_URL` and `ACEMQ_HEALTH_CONTAINER` point it elsewhere. It
deliberately does **not** read `ACEMQ_URL`: picking that up would aim a memory
alarm at whatever that variable happens to name.

## `rabbitmqctl`, not AMQP

Nothing in the protocol can raise a memory alarm, so the example reaches the
broker's own control tool through `docker exec`. That is unusual for an example
here and it is the honest option: a mocked alarm would prove something about the
mock, and this is the one claim in the repository where the broker's behaviour
*is* the subject.

## How this stays honest

Every reading is asserted: that the broker really blocked, that the reason names
memory, that the aggregate is `Up` rather than `Degraded`, that the details carry
the reason the broker gave, that a call costs less than a millisecond, that a
policy contributor moves the aggregate without moving the connection's report,
that the alarm cleared and the reason went away, and that a throwing contributor
is `Down` without taking the rest with it. The example exits non-zero if any of
them is wrong. CI compiles and runs it against a real broker on every push.
