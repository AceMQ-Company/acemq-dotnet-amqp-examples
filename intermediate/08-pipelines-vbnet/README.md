# intermediate/08 — a pipeline, and resuming one mid-run, in VB.NET

Three steps, each on its own queue, and a card processor that is down the first
time. The order dead-letters at `charge` and is then put back **at `charge`** —
so `validate` runs once, not twice.

The same example exists in C# at
[intermediate/08-pipelines-csharp](../08-pipelines-csharp).

## What it shows

- **A chain of steps, each a queue**, with the type of each step checked against
  the one before it at compile time.
- **A routing slip on every message**, naming the whole route and how far along
  it is.
- **`ResumeAsync`, which is the point**: a half-finished message goes back to the
  step it reached rather than to the entrance.
- **Both wire forms**, because the five AceMQ libraries write two and this one
  reads and writes both.

## Running it

```bash
docker compose up -d
dotnet run --project intermediate/08-pipelines-vbnet
```

## What to look for

```
pipeline    dotnet-vbnet-fulfilment: validate -> charge -> ship
slip form   Declared, in x-acemq-route
stalled at  charge, position 1 of [validate, charge, ship]
on the wire x-acemq-route: validate,charge,ship
payload     validate
resumed     ran [validate, charge, charge, ship]
delivered   order-1: validate,charged,ship

pipeline    dotnet-vbnet-fulfilment-by-address: validate -> ship
slip form   Itinerary, in acemq-routing-slip
on the wire acemq-routing-slip: {"steps":[{"exchange":"","routingKey":"dotnet-vbnet-fulfilment-by-address.ship","name":"ship"}],"done":[{"exchange":"","routingKey":"dotnet-vbnet-fulfilment-by-address.validate","name":"validate","completedAt":"..."}]}
stalled at  ship on dotnet-vbnet-fulfilment-by-address.ship, after validate
delivered   order-2: validate,ship
```

**`ran [validate, charge, charge, ship]`** is the whole example in one line.
`charge` twice, because it failed and then succeeded. `validate` once, because
resuming did not send the message back to the beginning.

## Why not just call three methods

Each step is a queue. That is what separates a pipeline from three method calls
in a row: a step that fails retries on its own, a slow step builds a visible
backlog instead of blocking the ones before it, and each step scales
independently. The queues are quorum, because a pipeline queue holds work that
has already passed earlier steps — losing the node holding it loses partly
finished runs, and the steps that already succeeded would have to be repeated.

A step returning `Nothing` ends the message's journey there, which is how a
filter is expressed: being rejected by a validation step is a normal outcome and
a failure is not, so they are counted apart (`EndedEarly` against `Completed`).

## The slip is what makes a replay a resume

Every message carries a routing slip. Nothing coordinates: each step reads the
slip and publishes to whatever it says is next. That costs two or three headers
and buys the thing a positional pipeline cannot do — **a message dead-lettered at
step two still says it is at step two**.

```vb
Await chain.ResumeAsync(stuck.Payload, stuck.WireHeaders)
```

The headers are the ones the failed delivery carried, and the slip among them
says where the message was. `SendAsync` is the wrong call for a message that has
already been partway through: it would start the run over, repeating every step
before the one that failed, which a step that charges a card cannot survive.

Resume with headers carrying no slip and the library refuses rather than
guessing:

> this message carries no routing slip, so nothing says which step it had
> reached. Resuming it would be a guess; send it in at the entrance with
> SendAsync if starting the run again is safe.

The payload passed to `ResumeAsync` is what the *failing step received* — the
output of the step before it, not what entered the pipeline — which is why it has
a type parameter of its own.

## The two wire forms

| | header | carries | written by |
|---|---|---|---|
| `SlipForm.Declared` | `x-acemq-route` | step names, comma-joined, plus a position and a run id | Java, and this library by default |
| `SlipForm.Itinerary` | `acemq-routing-slip` | JSON with each stop's exchange and routing key, and the ones already done | Go, Python, Ruby |

Declared is the default because a pipeline has already declared its steps and
their queues: an itinerary would repeat that declaration on every message to say
nothing new, and `validate,charge,ship` at position 1 is readable in a management
console without anybody decoding anything.

Choose `WritingSlipAs(SlipForm.Itinerary)` when a consumer in Go, Python or Ruby
reads one of these queues, or when a stop is somewhere this library has not
declared — the message carries its own addresses, so nothing has to be resolved
against anything. Note in the output above that the itinerary names
`<pipeline>.ship` rather than `ship`: it is meant to be followed by something
that has never heard of this pipeline.

**Either form is read whatever this is set to**, and either form resumes. That is
why the example runs the same failure twice.

## Do not declare `{queue}.dlq` yourself

Building the pipeline starts a consumer on every step queue, and a consumer
declares its own `{queue}.dlq` and `{queue}.parked` as it starts. Adding

```vb
Await mq.DeclareQueueAsync(chain.QueueFor("charge") & ".dlq")   ' don't
```

after `BuildAsync` is a `PRECONDITION_FAILED` rather than a no-op: that overload
declares a **quorum** queue, and the dead-letter queues are **classic** — which
is what Java and `Topology` declare them as, so a queue of that name declared
quorum by one of them is a precondition failure for the next.

## Four things VB.NET makes you name differently

**`chain`, not `pipeline`.** VB.NET is case-insensitive, so a variable called
`pipeline` collides with the `Pipeline(Of T)` type and the compiler reports it as
a type it cannot infer.

**`ByName` and `ByAddress`, not `Declared` and `Itinerary`.** Both of those are
`SlipForm` members, and a module constant of either name shadows the enum member
at the point it is used.

**`Did(name, …)`, not `Did(step, …)`.** `Step` is a keyword, from `For … Step`,
and cannot be a parameter name.

**`While True … End While` needs an unreachable `Return`.** VB.NET does not treat
a constant loop condition as making the end of the function unreachable the way
C# does, so a helper that only ever exits by returning still has to say what it
returns at the bottom.

Each step's lambda also names its parameter and return types —
`Function(item As Order) As Task(Of Order)` — because with `Option Strict On` the
lambda has to be typed before the generic `Step(Of TOut)` it is passed to can be
resolved.

## How this stays honest

The example asserts the slip read off the dead-lettered message: `charge`,
position 1, the three step names in order, and the exact `validate,charge,ship`
text on the wire that Java reads. It asserts the steps that ran after resuming
are `[validate, charge, charge, ship]` and that the delivered payload is stamped
`validate,charged,ship` — a resume that repeats `validate` shows up in both.

For the itinerary run it asserts the next stop is the pipeline's own queue on the
default exchange, that `done` holds `validate` with an RFC 3339 UTC timestamp all
five libraries parse, and that the header really is the JSON the other three
read. It exits non-zero if any of them is wrong.
