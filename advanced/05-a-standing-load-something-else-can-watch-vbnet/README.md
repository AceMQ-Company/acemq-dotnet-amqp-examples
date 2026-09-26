# advanced/05 — a standing load something else can watch

A load that does not finish. It publishes and consumes at a steady rate and writes one
JSON object per second saying what has happened to it, so that something outside the
process can read what the client saw rather than what the broker did.

Every other example here runs, proves a point and exits. This one is the thing a fault
drill breaks the cluster underneath.

## What it shows

- **A client reporting its own state**, in a shape another program can read: one JSON
  object per line on stdout, oldest first.
- **Back-pressure separated from failure.** A send declined because the broker blocked
  the connection is counted as `refused`, not `failed`. Counting a prompt refusal as a
  failure made this library look like one losing eighteen thousand messages under an
  alarm, when it had declined eighteen thousand sends and lost one.
- **Two exception types, kept apart.** `ConnectionBlockedException` is the broker
  blocking the connection; `PublishingPausedException` is this application having paused
  its own publishing. Both mean the send was declined rather than lost, and the
  distinction is this library's — Go names the same situation once.
- **`IsBlocked` and `BlockedReason` on the same line**, so a reading cannot claim to be
  blocked without saying why.
- **Readings flushed as they are taken**, because a drill reads this output while the
  process is still running.

## Running it

```bash
docker compose up -d

dotnet run --project advanced/05-a-standing-load-something-else-can-watch-vbnet \
    > readings.jsonl
```

Environment: `ACEMQ_URL`, `ACEMQ_LOAD_RATE` (per second, default 200),
`ACEMQ_LOAD_INTERVAL` (seconds between readings), `ACEMQ_EXAMPLE_SECONDS` (stop after
this long; unset runs until interrupted, which is what a drill campaign wants).

## What it prints

```json
{"at":"2026-09-26T19:32:14Z","elapsedMs":5011,"blocked":false,"published":676,"confirmed":676,"consumed":676,"failed":0,"refused":0,"publishRate":142.0,"consumeRate":142.0}
```

Under a memory alarm the same line reads:

```json
{"at":"...","elapsedMs":122089,"blocked":true,"published":20110,"confirmed":1883,"consumed":1883,"failed":1,"refused":18225,"publishRate":0,"consumeRate":0,"reason":"low on memory"}
```

`confirmed` has stopped moving, `refused` is climbing because every further send is
declined immediately, and `reason` is the broker's own words. None of that is visible to
anything that asks the broker how it is doing — the cluster is healthy, and this
application is not publishing.

## The fields

| Field | Is |
|---|---|
| `blocked` | the broker is refusing to read from this connection now |
| `published` | sends attempted since the start |
| `confirmed` | sends the broker has acknowledged |
| `consumed` | deliveries handled |
| `failed` | sends that failed for a reason other than back-pressure |
| `publishRate` / `consumeRate` | confirms and deliveries per second over the last interval |
| `refused` | sends declined because the connection was blocked or publishing was paused |
| `reason` | what the broker said when it blocked the connection |

The first seven names are a contract with whatever reads them, which is why they are
spelled with `[JsonPropertyName]` rather than left to a naming policy: a reader looking
for `confirmed` and finding `Confirmed` sees a client reporting nothing, which is
indistinguishable from a well-behaved client on a quiet cluster.

## Why a drill cannot use a probe instead

A probe that connects to the broker answers "is the cluster usable". It cannot answer
whether the application was told the broker had stopped reading from it, whether it
stopped publishing, or whether it recovered on its own — and those differ between client
libraries that are otherwise equivalent. The only thing that can report them is a
client.

## Also see

- `advanced/03-health-and-back-pressure-vbnet` — the same condition asserted rather
  than reported, including what health says about it.
