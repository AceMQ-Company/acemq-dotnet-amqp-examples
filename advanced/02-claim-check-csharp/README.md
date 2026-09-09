# advanced/02 — a claim check, in C#

A scanned medical report is tens of megabytes. Putting it on a queue is possible
and is a mistake: it fills the broker's memory, it is copied to every bound
queue, it makes a dead-letter queue impossible to inspect, and it turns a broker
into a filesystem with worse tools.

What travels instead is a **claim check** — the payload goes to a store, and the
message carries the key.

The same example exists in VB.NET at
[advanced/02-claim-check-vbnet](../02-claim-check-vbnet).

## What it shows

- **Both sides of the threshold in one run.** A small document travels inline; a
  document of exactly 65536 bytes is offloaded.
- **The framing**, read off the wire rather than described.
- **The key on the message is the file in the store**, and a store that never saw
  the publish can redeem it.

## Running it

```bash
docker compose up -d
dotnet run --project advanced/02-claim-check-csharp
```

## What to look for

```
payloads go to /tmp/acemq-claim-check-csharp
inline:  0xAC 0x01 0x00 + 42 bytes of payload
checked: 0xAC 0x01 0x01 + 36 bytes naming the payload
content type of the claim check: application/json
the store holds 1 payload(s): 1e306add-1d7f-4198-8bfd-f7871a7acadb
read doc-large: 65500 characters
read doc-small: 6 characters
```

**One publish, two views.** A fanout exchange feeds two queues: one is read the
ordinary way and yields `Document`s, the other is read as raw bytes so what
actually went on the wire can be printed rather than asserted about in prose.

## The threshold is the whole point

```
0xAC  0x01  0x00  payload      inline, and identical to what JSON wrote
0xAC  0x01  0x01  key          a claim check, the key as UTF-8
```

Byte for byte what the Java, Python and Ruby libraries write.

Below `ClaimCheckCodec.DefaultThreshold` — 64 KiB — the payload travels inline,
exactly as it would without this codec. That matters more than it sounds:
offloading a two-hundred-byte message turns one broker round trip into a store
round trip **and** a broker round trip, so an unconditional claim check makes the
common case slower in order to fix the rare one.

The comparison is **strictly less than**: `encoded.Length < threshold` travels
inline, so a payload of *exactly* 65536 bytes is offloaded. The large document
here is padded to exactly that, measured rather than guessed — encode it with an
empty body, then make up the difference in one-byte characters — and the example
asserts the size before publishing, because a document that missed the boundary
would prove nothing about it.

The framing says which of the two a message is, so a consumer handles both
without being told. That is what allows the threshold to be changed, or this
codec to be introduced, without a flag day: messages written before the change
are still readable after it, and a body with no framing at all is read as the
delegate would read it.

## The content type does not change

It is the delegate's, unchanged. Unlike encryption, where the bytes really are
something else, a claim-checked message is still a document — it is a document
that is somewhere else — and a consumer that lacks the store gets a clear failure
naming the missing key rather than a parser error.

## Which store

**`FilesystemClaimCheckStore`**, used here, is the honest middle ground: useful
where the filesystem is shared and durable — an NFS mount, a persistent volume.
On a container's local disk it is the in-memory store with extra steps, because
the consumer is on another host and finds nothing. Its writes are atomic: the
payload goes to a temporary file and is moved into place, because a consumer fast
enough to read the key before the writer finished would otherwise get a truncated
payload and a parse error somewhere unhelpful — and messaging is exactly the
arrangement that makes a consumer that fast normal rather than unlikely.

It also refuses a key it did not issue. A key becomes a path segment, and
`../../etc/passwd` is a key too.

**`InMemoryClaimCheckStore`** holds the payloads in the publisher's heap — which
is where they were going to be anyway, so it takes them off the broker and does
nothing else. A claim check that does not outlive the process that wrote it is a
message nobody else can read. It is genuinely useful in a test, where publisher
and consumer are one process and the framing is what is being proved.

**Object storage** is the usual right answer, and a store in front of S3 or Azure
Blob Storage is three short methods.

To show the difference, the example redeems the key through a *second*
`FilesystemClaimCheckStore` built from nothing but the directory — an object that
never saw the publish, standing in for the consumer that in a real deployment is
in another process on another host.

## Retention is the part that goes wrong

The store and the queue have different lifetimes and nothing enforces a
relationship between them. A message replayed a month later carries a key, and if
the store expired that key the replay produces a message nobody can read — worse
than a lost message, because it looks like a message and fails deep inside a
consumer rather than visibly.

So the store's retention must exceed every retention that could bring a message
back: queue TTLs, dead-letter queues, and however long somebody might sit on a
message before replaying it by hand. When in doubt, longer.

`IClaimCheckStore.Delete` exists and the codec never calls it. Deleting on read
would break the second consumer of the same message, and deleting on
acknowledgement would break a replay — so *when* a payload may be removed is a
retention decision, and retention decisions belong to whoever owns the data.

**This example deletes its own directory on the way out**, which is precisely the
thing a deployment must not do while a message referring to it is still
deliverable. It does it so that a second run reports the same numbers as the
first.

## For the operator

`ClaimCheckCodec.KeyOf(body)` reads the key a message refers to **without**
fetching it, and `IsClaimCheck(body)` says whether there is one. For somebody
looking at a dead-letter queue, answering *which object does this need, and is it
still in the store?* from the message alone is the difference between a
five-minute check and restoring a backup.

## How this stays honest

The framing bytes, the inline body being the JSON plus three bytes, the key on
the wire matching the filename in the store, the store holding exactly one
payload of exactly 65536 bytes, and both documents coming back whole are all
asserted. The example exits non-zero if any of them is wrong. CI compiles and
runs it against a real broker on every push.
