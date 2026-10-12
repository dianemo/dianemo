# Design notes

Implementation reasoning that spans more than one file — the things a new
maintainer needs once, and that are too long to sit usefully in a comment.

This is not the manual. [Concepts](concepts.md) explains the same system from a
consumer's point of view; if you are trying to *use* dianemo, read that instead.
Everything here is about why the internals are shaped the way they are, and each
section exists because getting it wrong has already cost someone a day.

## Clocks

Three different clocks appear in this codebase, and mixing them is the single
easiest way to introduce a bug that only shows up under load on someone else's
infrastructure.

**The backend's clock owns everything the fleet compares.** Freeze deadlines,
token-bucket `lastUpdate`, concurrency-slot ages — anything two replicas reason
about together comes from `backend.now()` (Redis `TIME`, or `Date.now()` in the
memory backend, which is the same clock by definition because there is only one
process). Queue arrival timestamps are the one exception, below.

The rule that follows: **when you compute a delay from a fleet-wide deadline,
both ends of the subtraction must come from the same clock.**
`scheduleDrainForFreeze` reads `frozenUntil` and `backend.now()` together for
exactly this reason. Using a local `Date.now()` against a backend-written
deadline makes the delay wrong by the host-to-Redis skew — and a backend running
ahead fires the timer *early*, whereupon the pass finds the client still frozen,
books nothing, and the queue sleeps until the health tick.

**Lua scripts that decide fleet-wide state take `TIME` and never an argument.**
`cleanupStaleFrozenGrants` decides which grants stop being marked frozen. A
caller-supplied timestamp there would let one skewed replica unmark a grant the
rest of the fleet still considers frozen. If such a script needs another `ARGV`,
add it after the existing ones and leave `TIME` alone.

**Process wall-clock is correct for one thing: a stored credential's expiry.**
`issuedAt` is the writing replica's `Date.now()`, and `expiresAt` is on the same
wall clock — computed from it on a refresh, or supplied by the host seeding a
grant — so the lifetime `expiresAt − issuedAt` is in one frame, and the renewal
margin is derived from it. Expiry itself is compared against the *reading*
replica's `Date.now()` in `authenticate.ts` — local wall clock against another
host's — and the skew between the two is absorbed by the renewal margin. Taking
one end of the lifetime from `backend.now()` would inject skew into it instead,
and a host lagging by more than the token's lifetime makes it negative.

**Arrival timestamps are approximate by construction**, and that is documented
for consumers in [Concepts](concepts.md#cost-and-priority): the timestamp comes
from whichever replica received the request, so drift between replicas reorders
requests *within* a priority band. Priority ordering is unaffected, because the
bands are far wider than any plausible drift — see below.

## Queue score bands

Priority, retry count and arrival are packed into one sorted-set score, lowest
served first, so that a single `ZRANGE` yields the next request. The layout lives
in `packages/core/src/backend/queueScore.ts` and is shared by both backends.

```
score = (MAX_PRIORITY - priority) * PRIORITY_BAND    PRIORITY_BAND = 1.01e14
      + (MAX_RETRIES  - retries)  * RETRY_BAND       RETRY_BAND    = 1e12
      + (timestamp - EPOCH_MS)                       EPOCH_MS      = 2026-01-01
```

Two arithmetic constraints, and both are tight enough that changing a constant
casually will break ordering rather than error:

- **Bands must not overlap.** Epoch-ms alone is ~1.76e12 without the offset,
  which is why the clock is offset at all: it keeps the arrival term below
  `RETRY_BAND` so a timestamp cannot bleed into the retry band.
- **The total must stay under 2^53**, where doubles stop representing
  consecutive integers. The current maximum is ~1.11e15 against a ceiling of
  ~9.007e15. A `1e15` priority band would exceed it and quantize the millisecond
  ordering away.

`calculateQueueScore` throws a `RangeError` on both violations rather than
silently mis-ordering, which is the only reason a mistake here is survivable.

**`QUEUE_RETRY_BAND` is an on-disk format.** Changing it reinterprets every
score already persisted in Redis. It must stay stable until a versioned queue
migration exists.

`cost` is deliberately not in the score. It decides what a request spends, not
where it sits.

## Numeric field parsing

Queue metadata is stored as strings — Redis hash fields on one side, a `Map` on
the other — and parsed back on both. **The Lua is the reference implementation
and the TypeScript matches it**, not the other way round.

That direction matters because `tonumber` and `Number` disagree on the empty
string: `tonumber("")` is `nil`, so `tonumber(x) or 1` yields the default, while
`Number("")` is `0`. On a `priority` field that difference is a silent demotion
to the bottom band in TypeScript and a default of 1 in Lua, on the same stored
byte. `parseStoredNumber` exists to make emptiness explicit and remove the whole
class rather than the one instance.

The rule applies to **every** read of a stored numeric field in queue, budget
and freeze state, not to an enumerated list of them. It is stated as a rule
rather than a list of sites because a list that undercounts is worse than no
list: walking it feels like coverage. Credential fields are outside it:
`expiresAt`, `issuedAt` and `refreshTokenExpiresAt` are read with `Number` in
`authenticate.ts`, where no Lua reads the same field and an unusable value is
handled at the read. So:

- **In Lua**, a read is either `tonumber(x) or <default>` where a missing field
  means the default, or a bare `tonumber(x)` followed by an explicit `nil` test
  where absence means something else — no freeze state at all, or a bucket that
  has never been written and so counts as full. Both are correct; what is wrong is
  a bare `tonumber` whose `nil` then flows into arithmetic. Grep `tonumber(` in
  `packages/backend-redis/src/lua/` for the current set; there is no registry to
  keep in step.
- **A `nil` test is not enough where the value reaches `cjson.encode` or
  `string.format`.** `tonumber` uses `strtod`, so `"nan"` and `"inf"` both parse
  to numbers that pass a `nil` test, survive `math.min` and every comparison, and
  then fail at the point of serialisation — which failed the operation for a whole
  client rather than defaulting the one corrupt field. Use the `finiteNumber`
  fragment there. `parseStoredNumber` already reads a `NaN` as the default, so
  this is what makes the two sides agree.
- **A computed number goes back to the caller as a string.** `cjson.encode`
  writes numbers at 14 significant digits and raises on a non-finite one, and an
  integer reply truncates — so a fractional balance would read differently from
  the memory backend, and a wait that overflows to `inf` would raise after the
  script's writes had landed. The `exact` fragment formats with `'%.17g'`, and
  `parseLuaNumber` in `luaNumber.ts` reads it back, `inf` included.
- **In TypeScript**, the only implementation is `parseStoredNumber` /
  `parsePriority` / `parseFiniteStored` in
  `packages/core/src/backend/parseStored.ts`. Both backends go through it. A new
  read must use it rather than `Number`, `parseInt` or `parseFloat`, all three of
  which disagree with `tonumber` on some input. `parseFiniteStored` is the twin of
  the `finiteNumber` fragment, for a field that is compared and then written back
  — a freeze deadline, a probe budget.

`retries` and `timestamp` in `decodeRequest` and `toQueued` are the remaining
`parseInt` reads. Both backends read them identically, so the two agree with each
other; what they do not match is the Lua *score*, which uses `tonumber` — a stored
`"2.9"` reads as 2 and scores as 2.9. Harmless while nothing writes a fraction
there, and worth routing through `parseStoredNumber` if either ever becomes
non-integer.

The one exception, deliberate: `cost` is read with `parseFloat(...) || 1` in
`decodeRequest` and `toQueued`, which maps a stored `"0"` to 1. A request may not
legitimately cost 0 — `packages/core/src/request/index.ts` rejects anything below
`Number.EPSILON` — so 0 there is corruption and the default is the safer answer.
Both backends do this identically. `getQueueStats` does *not* share that reading:
it counts a stored `"0"` as 0 on both backends.

Two further rules inside that:

- **A NaN must never reach a score.** The memory backend's `sorted()` compares
  `a[1] - b[1] || lexical`, so a NaN score falls through to ordering by id: the
  priority is silently ignored and the comparator stops being a total order.
- **A read and a score treat `Infinity` differently, deliberately.** A score must
  accept whatever `tonumber` accepts, including `inf`, which both sides then
  clamp into the top band. A *read* is serialised into the `requestReady` payload,
  and `JSON.stringify(Infinity)` is `null`, so a corrupt `"Infinity"` would arrive
  at the owning replica as a null priority in a field typed `number`. Reads floor
  the non-finite to the default; scores keep it. `tonumber` reads every
  infinity `strtod` does — `inf` and `infinity` in any case, signed or not —
  where `Number` reads only `Infinity`, so `parseStoredNumber` matches those
  spellings itself.

The priority default is 1, never 0, because 0 is the documented bottom of the
range and a request may legitimately ask for it. Mapping corruption onto 0 makes
it indistinguishable from an operator's deliberate choice.

## Credential keying and sub-clients

A sub-client inherits its parent's endpoint and authentication — that is what
makes "one credential, several budgets" work, and it is what
[Concepts](concepts.md#sub-clients) promises consumers. So **credentials are
filed under the root of the sub-client path, not under the full
`parent:child` name.**

Keying on the full name gives each sub-client its own credential entry, which
breaks the inheritance in both directions: a grant seeded on the parent is
invisible to the child, and seeding both stores two copies of one refresh token
that then replay each other's rotations.

Two mechanisms implement this, and they are easy to confuse:

- **`authOwnerName`** is set when a sub-client is merged onto its parent and
  carries the parent's own auth owner, so a nested path (`a:b:c`) resolves to the
  client that actually holds the credentials rather than to the template. A
  sub-client declaring its own `authentication` gets none: it owns its
  credentials, and its own sub-clients inherit it as their owner.
- **`sharedLimit` is the deliberate exception.** It is constructed with its
  *parent's* `name`, so its queue, bucket and freeze state resolve to the
  parent's keys — that is the whole point of the type. But it is a peer with its
  own credentials, so it keys those on its own registered name. Two clients
  filing tokens under one key means whichever refreshes first wins and the other
  attaches that token to its own requests, which the vendor will cheerfully
  accept.

The general rule: **budget state follows the parent; credentials follow whoever
owns them.** `usesGrantIsolation` reads from the budget owner for the same
reason, and it turns on whether the owner *accessor exists* rather than whether
it answered — `?? this.authData` cannot distinguish "this client owns its budget"
from "the owner has no auth data", and falling back to the borrower's own config
is exactly the disagreement the indirection removes.

## Template builds and removals are serialized per entry

One template entry — a `(templateName, instanceId)` — can be built and removed
from several directions at once: a local `addTemplateClient` races its own
`templateClientAdded` broadcast, a start reloads every stored entry, and a
removal can arrive while a rebuild sits between reading the stored record and
registering its clients. `entryLocks` in `templateClients.ts` serializes them: a
`WeakMap` keyed by handler, so two handlers in one process stay independent,
holding one promise chain per entry, which a failed predecessor does not break.

The lock orders the work but cannot tell a build that its record is stale. So
every removal bumps the entry's generation (`entryGenerations`), **before** it
awaits the lock, and a build reads the generation before it reads the record.
A build that finds the generation moved — on taking the lock, or after its last
await — registers nothing. Bumped inside the lock instead, a build already
holding it would see no change and register clients for a removed entry.

Credentials need the same guarantee across a rebuild. A replacement client does
not stop the refreshes its predecessor has in flight, so retirement is tracked
per line of rebuilds rather than per client: `credentialLines` in
`authenticate.ts` gives a client and every client it replaced one shared flag
(`inheritCredentialLine`), and a removal's `retireCredentialWrites` stops all of
them. A token refresh checks the flag after its last await before writing.
`setGrantTokens` checks it before its write and again after, since a removal
that purged while the write was in flight never saw that grant: the write
deletes itself and throws `client_removed`.

## Builder output is copied before it is written into

A template builder's return value is deep-copied (`deepCloneWithFunctions`)
before the plan and overrides are written into it, and the chosen plan is
`structuredClone`d per build. Builders may return objects they share between
tenants or keep from one build to the next, and a plan is shared by every tenant
on it, so writing into either in place would carry one tenant's limits into
another's build — past the validation registration gave them.

## Freeze duration and retry wait are separate numbers

They used to be one, and code that assumes they still are will look correct.

- **`waitTime`** is how long *this request* sleeps before its next attempt.
- **The freeze duration** is how long *the whole fleet* stops spending.

They can legitimately disagree. `retryBackoffBaseTime: 0` is legal and means
"retry this request immediately" — it says nothing about whether the fleet should
keep hammering an upstream that just rejected us. Such a client has a zero
`waitTime` while its freeze after a 429 may run the budget owner's entire refill
interval.

Only a 429 takes that floor. `getFreezeBaseTime(isRateLimited)` raises the
freeze to the shortest refill interval when the response was a rate limit, and
a 5xx or dropped connection freezes for the configured back-off: it says nothing
about the budget, and flooring it would stop a per-day client for a day over one
bad response. `MeteredClient` floors its retry wait on the same condition; a
`sharedLimit` child's retry wait takes its own back-off either way.

Consequences worth knowing before touching either:

- **Gate freeze arming on the freeze duration, never on `waitTime`.** Keying it
  on the retry wait means a `retryBackoffBaseTime: 0` client can be rate limited
  repeatedly and never pause anyone sharing its budget.
- **`getFreezeBaseTime` is where the configured back-off becomes a duration**, so
  it is where zero, `NaN` and negatives are floored. A negative is the dangerous
  one: it is truthy, so the arming gate fires with `frozenUntil` already in the
  past, leaving the client in permanent single-probe thaw. `retryOptions` is never
  validated at construction, and `Number(process.env.BACKOFF)` with the variable
  unset is a realistic `NaN` source.
- **Subclasses that override must fold `super`'s result in**, never re-read
  `retryOptions`. `Math.max(interval, NaN)` is `NaN`, which serialises as `null`
  and reads as falsy at the arming gate — so a raw read arms no freeze at all.
- **`armFreeze` is monotone**: it keeps the longer of the two deadlines and never
  shortens one already standing. Everything that books a wake-up depends on this.
  A wake-up computed from a monotone deadline is either exact or early, and early
  self-corrects because the pass declines and re-books. A shrinking deadline would
  make it *late* with nothing behind it.

  It follows that nothing ends a freeze before its window lapses. A successful
  [thaw probe](#thaw-probes) deletes the freeze only once `frozenUntil` has
  passed, so a window re-armed while the probe was in flight stands.
  `clearFreezeState`, `resetTokenBucket` and `clearConcurrency` have no call
  sites in `packages/core/src` outside the backends themselves — they are on the
  interface for an operator, and are documented as such in
  [Concepts](concepts.md#clearing-a-freeze-by-hand). Keep them that way: a caller
  inside the library would be the shortening path this bullet exists to prevent.

## Thaw probes

A freeze whose window has lapsed still owes `thawRequestCount` probes, and they
go out one at a time. When `canProcessNextRequest` reports a thaw, the drain
loop nominates the head through `tryStartThawRequest`, which checks for a
competing in-progress probe and marks this entry `isThawRequest` in one
operation. Without grant isolation every grant shares the one freeze, so the
claim is client-wide (`anyGrant`) and a probe of any grant holds it; an isolated
grant probes only its own freeze.

A nominated probe that does not go out — its turn declined, or the claim threw
— gives the mark back through `returnToQueue` in `processRequests.ts`. Left set,
it reads as a competing probe and holds off every other, and its eventual
completion is counted against whichever freeze stands by then.

A successful probe decrements the count, and `updateThawProgress` (`memory.ts`,
`lua/freeze.ts`) deletes the freeze only once the count is spent **and** the
window has lapsed. Another freeze can be armed while the probe is in flight;
deleting on the count alone would let the probe's success end that new window
early, which is the shortening that
[`armFreeze`'s monotonicity](#freeze-duration-and-retry-wait-are-separate-numbers)
rules out. Each completion id is recorded before the state is read, so a
redelivered completion is not counted twice, and the set of them lives
`FREEZE_PROBE_TTL_SECONDS`, as long as the freeze it guards outlives its window.

What counts as success is decided in `handleRequestDone`: a probe fails only
if its own completion arms a freeze (`freezesClient`, the same test that arms
one), so `classifyError` decides probes as it decides freezes. A failure leaves
the freeze key alone, TTL included. Re-arming there would be redundant, since
the failure has just armed a freeze through `armFreeze`, whose merge keeps the
probes owed and sizes the key for them.

## Claiming and then verifying leaves a residue

Admission excludes a request's own id from the occupancy sum, so that a resubmit
does not compete with itself. The consequence is that a claim for an id which has
just released always succeeds and silently re-creates its entry — and nothing is
left to release it, because the request is done, its queue entry is gone and its
`requestDone` is already consumed. The capacity sits claimed until its TTL,
shrinking the effective cap, and on a small cap taking it to zero.

Closing it means claiming and confirming in one atomic operation. A client whose
one budget is a concurrency limit does exactly that: `acquireQueuedConcurrency`
claims only while the request's metadata still reads `inProgress`, in the same
script or synchronous pass. The multi-limit claim has no such twin, and the
method is optional on `DianemoBackend`, so `processRequests` also re-reads the
entry after any claim that holds a slot. For those cases **that narrows the
window; it does not close it.** A completion landing between the read and the
notification still strands the claim until the slot TTL, and orphan cleanup
cannot help because there is no entry left for it to see. A status-checked
`acquireMultiLimit` is the actual prerequisite for removing the re-read.

The related asymmetry: **a read that failed is not evidence the request is
gone.** Treating an unreadable entry as still queued keeps the claim, which is
the recoverable error — the request either runs, or times out and releases
through the ordinary abandonment path. Treating it as absent releases capacity
for a request that may still be live.

## A queue claim is a lease

`getNextRequest` flips an entry to `inProgress` so that no other pass is handed
it, and the request stays `inProgress` until it completes — which for a stream
can be half an hour. The flip alone cannot tell those two apart, so a claimer
that never acted on its claim stranded the entry: a reply lost after the script
ran, or a controller that died between the claim and `returnToQueue`. Nothing
selected it again, and orphan cleanup spares it while its owner lives.

So the claim carries a timestamp. Both backends stamp `claimedAt` from their own
clock (Redis `TIME`, `Date.now()` in memory) when they hand an entry out, and
`getNextRequest` treats an `inProgress` entry whose stamp is
`QUEUE_CLAIM_LEASE_MS` old as eligible again, clearing any thaw-probe mark the
lapsed claim made. Before dispatching, `processRequests` calls
`confirmRequest`, which deletes the stamp only if it is still the one this pass
was handed; an entry without a stamp is admitted and is never reclaimed. Any
`status` written through `updateRequest` also deletes it, so a returned entry is
plainly pending. A stamp that does not parse never lapses, on both sides.

The lease bounds only the gap between claim and confirmation, a few round
trips, so it can be short. Too short costs little. Within one client object the
passes are serialised by `processingLock`, but the lock is that object's own:
`resetClient` demotes and destroys the old client without waiting out a pass it
is already running, so after a rebuild that pass can drain alongside the
replacement's. That, or two controllers draining at once, is when a lease
lapses under a live claimer.

Then the stale claimer's confirmation fails and it does not dispatch. It does
not release what it claimed either, because a concurrency slot is keyed by
request id and is the slot the new holder is using. The exception is an entry
that a re-read finds gone: it has no holder, so the slot is this pass's alone
and is released. A token-bucket claim is spent twice instead, and the stale
claimer's claim mark, `<bucket>:claim:<slotId>`, is keyed by the same request
and overwrites the new holder's. Settling the new holder's response against
that mark can credit back at most `cost − actual`, against a full extra spend
already taken, so both under-admit rather than over-admit.

Every write-back of a claim in the loop — declined by a freeze, by a probe
already running, by the budget, or failed in admission — goes through
`returnToQueue`, which confirms before it writes `pending`. A stale claimer
that is declined anywhere therefore cannot reset an entry the new holder has
already confirmed.

A thaw-probe mark made under a claim whose lease has lapsed does not count as a
competing probe in `tryStartThawRequest` or `hasThawRequestInProgress`, on the
backend's clock as `getNextRequest` reads it. `getNextRequest` clears such a
mark when it hands the entry out again, but an entry sorted behind a head that
keeps being handed out is never reached, and its mark would otherwise hold off
every probe.

A confirmation whose reply is lost throws into the loop's catch, which returns
the entry to the queue undispatched, as for any other admission failure. That
write does not confirm again: the lost confirmation may have landed and removed
the stamp, and a second one would then refuse and leave the entry `inProgress`
with nothing to move it on.

## A removed request cannot be re-added

`removeRequest` marks the id for as long as that request's own add could still be
outstanding, and `addRequest` refuses a marked id. That is what makes the ordering
contract on `addRequest` hold without trusting the transport to preserve it.

The contract is that the add commits before any `publish` issued after it is
delivered. Command order on one connection used to be enough, and it is not:
**ioredis retries a `NOSCRIPT` as a brand-new command**, attached in a promise
callback (`Script.js`), so it is queued a microtask later than anything the
application wrote in between. A plain `PUBLISH` issued behind an `addRequest`
therefore reaches Redis first whenever the script needs reloading — after an
operator `SCRIPT FLUSH`, or against a node that never loaded it. A full restart is
safe: the socket drops and ioredis re-`EVAL`s.

Without the mark, the abandonment's removal finds nothing to remove and the add
then creates an entry that is `pending`, carries a live `ownerId`, and has no
waiter. `cleanupOrphanedRequests` spares it precisely because its owner is alive,
and it still counts in `ZCARD` — so `queueEmptyGate` refuses the fast path for the
whole client, turning every request from two round-trips into nine, until the 24h
key TTL.

Three properties make the mark safe rather than a second hazard:

- **Ids are `crypto.randomUUID()` and never recycled**, so the only add a mark can
  refuse is one for the very request that was removed.
- **A retry keeps its entry** rather than removing and re-adding —
  `handleRequestDone` removes only on success or terminal failure — so a removal
  and a later add of one id never both happen in the ordinary flow.
- **It expires**, and it is sized from the consumer's `cleanupTimeout` rather than
  fixed. Raising `cleanupTimeout` lengthens the very window the mark exists to
  cover — how long a request's own enqueue can still be outstanding when another
  process removes it — so the mark is
  `max(REQUEST_TOMBSTONE_TTL_SECONDS, cleanupTimeout)`. The constant is the floor
  at 60s, not the value. Both backends then read the argument through
  `normalizeTombstoneTtl` rather than refusing it, because the removal runs on
  every completion and has already deleted the entry by the time a `SET ... EX`
  would reject the TTL: a fraction rounds up, a value with no lifetime (`NaN`,
  zero, negative) takes the constant, and an unbounded `cleanupTimeout` takes
  the longest TTL a key can hold. The two used to drift apart, a fixed minute against a
  window the consumer configured. A permanent marker would also be correct, and
  would grow one key per request forever.

The race is narrow and the fix is not: the mark holds whichever of the two
actually arrives first, which is why the conformance tests assert the invariant
directly rather than trying to win the race.

## TTL arguments are validated before the write, not after

`normalizeTtlSeconds` in `packages/core/src/backend/ttl.ts` is the one reading of a
TTL, shared by both backends — with `normalizeStateTtlSeconds` beside it as
the stricter reading for rate-limit state — and both are called **before**
anything is written.

That ordering is the point. A Redis pipeline is not a transaction, so an `HSET`
followed by an out-of-range `EXPIRE` leaves the hash written with no expiry at all
— for a credential hash, an encrypted refresh token persisted forever. The memory
backend had the mirror-image bug: a `NaN` deadline made its `now >= expiresAt` test
false forever, so the key never expired either. Validating first removes both.

Consequences worth knowing:

- **A fraction rounds up**, because a TTL here is garbage collection rather than
  expiry enforcement — `expiresAt` decides whether a token may be used. Arriving a
  second late costs memory; arriving early discards a live credential.
- **Non-finite and out-of-range raise** rather than clamping, because both mean the
  caller computed something it did not intend. A caller with no bound passes no
  TTL.
- **Zero or negative still means "expire now"** for the store — `set`, `hset`,
  `batch` — on both backends, matching Redis `EXPIRE`, and is passed on as `0`
  because `EXPIRE` refuses a fraction such as `-0.5`. That is a real delete, so
  it is the one TTL that can lose data silently; `credentialTtlSeconds` floors
  well above it.
- **A key holding rate-limit state refuses zero or negative.** A bucket, a slot
  ledger, a queue and a claim mark take `normalizeStateTtlSeconds`, which
  throws a `RangeError` instead. Those keys are shared by every caller of a
  client, so one caller's "expire now" would delete what its peers hold — a
  slot ledger with every other holder's slot in it.
- **`credentialTtlSeconds` clamps its upper end too**, because
  `refresh_token_expires_in` is untrusted provider input checked only for being
  finite and positive, and a "never expires" sentinel exceeds what Redis accepts.

## The Redis backend requires one logical keyspace

Several scripts build metadata keys inside Lua from a prefix — they discover which
keys they need from a `ZRANGE` or `SMEMBERS` at runtime, so no `numberOfKeys` could
declare them — every slot script derives a ledger's `:costs` hash from the ledger's
own key, and two (`cleanupStaleFrozenGrants`, `getInstances`) take a key
prefix through `ARGV`. That breaks the `EVAL` key-declaration contract, which
matters for anything routing by key: Redis Cluster, a proxy, an active-active
deployment. **Dianemo supports a single logical keyspace only**, which is why no
Cluster adapter exists and `redisBackend` types its parameter as `Redis`.

## One config shape, one client class

`rateLimit` is always a list, and its names are resolved **once**, in
`createClient`. Nothing past that point deals in an unnamed limit:
`BaseClient.rateLimit` is always `NamedRateLimitData[]`,
`getClientStats().rateLimit` is always a list, and a limit written without a name
is named `default`. A bare limit object is refused there with a
`ConfigurationError` that shows the wrapped form.

That is what collapses the client types. There is no class per limit type —
the only thing that would vary between them is which backend call claims the
budget, which is a branch, not a class. `MeteredClient` covers every client that
meters against budgets of its own, one of them or several, and keeps the
single-key backend calls as a fast path for the one-budget case: same decision,
one round trip instead of an encoded spec list.

`SharedLimitClient` survives as the one genuine exception. It owns no budget and
no queue — it is constructed with the owner's name — so it is a different thing
rather than a different budget, and it may not be combined with other limits.

Four things follow that are easy to get wrong:

- **Derive, do not cache, anything read off `rateLimit`.** `sole` and `metered`
  are getters. A cached copy is a second reading that a write can leave behind,
  and the two disagreeing would have the claim spend against one budget while the
  ceiling was checked against another.
- **Within the drain loop, nothing claims capacity before `tryAcquireTurn`.**
  The `tryAdmitImmediately` fast path claims outside it, only while the queue is
  empty and before the request is ever enqueued. The drain loop's check
  that the entry is still queued sits after the claim, gated on
  `claimsReleasableCapacity()`, and a claim it finds stale is handed back
  through `refundUnusedAdmission`. The solo-concurrency path uses
  `acquireQueuedConcurrency`, so its status check and claim are one operation.
- **A request releases what it claimed, not what the client now declares.** Each
  claim records its keys as `claimedKeys` on the request's metadata, and
  completion, refund and abandonment act on those. An override or
  `rateLimitChange` landing mid-flight can rename or drop a limit; releasing by
  the current names would leave the old slot held until its TTL, or refund a
  bucket that never gave out the tokens. A slot ledger the current limits no
  longer name is still released; a bucket they no longer name keeps the spend,
  since losing a token is the safe direction.
- **Admission is not the last point a request can be abandoned.** The
  interceptor and a token refresh run after it, so an abort landing there goes
  through `releaseAbandonedRequest` like any other pre-dispatch failure: the
  claim is refunded and the caller gets `RequestAbortedError`. Left to axios, it
  would reject with its own `CanceledError` and keep the spend.

## Several budgets are claimed in one operation, or not at all

A client can declare an array of rate limits, and a request must claim every one
of them. Claiming them in turn would leave the first spent whenever the second
declined — and no decline path hands a budget back, so a per-day cap sitting on
top of a per-second one would leak a token on every refusal, permanently, at
exactly the rate the tighter limit refuses.

So the claim is one backend operation. Both implementations evaluate every budget
before committing to any: the memory backend in a single synchronous pass, Redis
in a single Lua script that runs `planMultiLimit` to completion before
`commitMultiLimit` writes anything.

The release is one operation too. The memory backend's `releaseMultiLimit`
hands back every budget in one synchronous pass, through `creditTokens` and
`releaseSlot` — the bodies of `refundTokens` and `releaseConcurrency`, kept
synchronous for this — rather than awaiting each public method in turn, which
would let another spender land between two budgets of one refund.

Four consequences worth knowing before touching that code:

- **`acquireMultiLimit`, `releaseMultiLimit` and `tryAdmitMultiLimit` are
  optional on `DianemoBackend`**, so a backend written against an earlier version
  still satisfies the interface — but a client declaring several limits refuses
  to construct without the first two, rather than degrading to one budget at a
  time, and a `rateLimitChange` or broadcast update that would need them is
  ignored with an error log. Without `tryAdmitMultiLimit` only the fast path is
  lost.
- **They are the only scripts registered without a fixed key count**, because the
  key list comes from a client's configuration. `ctx.runVariadic` passes the count
  as the call's first argument, which shifts every other position by one; the
  static guards in `test/luaScripts.test.ts` know about this and hold those three
  to the ARGV contract and to addressing `KEYS` dynamically. Every budget key
  is declared in `KEYS`, but not every key the scripts touch: a slot ledger's
  `:costs` hash is built inside the script as `key .. ':costs'`, exactly as the
  single-budget slot scripts build it. That is harmless under
  [the single-keyspace rule](#the-redis-backend-requires-one-logical-keyspace),
  and it is one more reason a Cluster adapter would need more than a key-count
  change.
- **The spec list travels as JSON, so a non-finite number goes as a string.**
  `JSON.stringify` writes `NaN` and `Infinity` as `null`, which `cjson` decodes
  to a value every guard lets past; as a string, `tonumber` reads it back and
  the Lua refuses what the memory backend refuses.
- **Refill credit is persisted even on refusal.** The intervals really did elapse,
  and the single-budget paths write it back too, so a plan that is thrown away
  still commits what it read.

## A multi-limit freeze does not empty the buckets

A client declaring exactly one `requestLimit` zeroes its bucket on a 429: the
response is proof that the bucket's picture of the vendor was wrong, and with one
bucket that says which one. `noLimit` and `concurrencyLimit` entries beside it do
not change that, since neither is a bucket.

A client with several `requestLimit`s does not, and the difference is
deliberate. The response does not say which limit was breached, so zeroing every
bucket would destroy a per-day quota over a per-second refusal. Zeroing only the
shortest adds nothing the freeze does not already do, since the freeze is
floored at that same interval. So the freeze window is the whole stand-down:
floored at the **shortest** refill interval in the array, lengthened by each
further 429 through the same retry multiplier, and probed one request at a time.

Flooring at the shortest rather than the longest is the other half of the same
argument — a per-day interval would pause the fleet until tomorrow.

The release path has to agree. A refund is refused while a freeze that emptied
the bucket stands, and only a 429 on a client with exactly one `requestLimit`
empties one: a 5xx or a dropped connection freezes without touching a bucket,
and so does a 429 on a client with several. A refund refused under any of those
is a token lost for nothing. The freeze therefore records which it is, as an
`emptiesBucket` field that `armFreeze` writes from its `emptiesBucket` option —
`freezeEmptiesBucket` sets it only in the lone-`requestLimit` case — and both
backends' refund gates refuse only while a freeze with that field set stands.
So a multi-limit claim handed back because a freeze landed mid-claim is credited
to every bucket. `releaseClaim` always sends the freeze key: whether to refuse
is the freeze's answer rather than the caller's, since the limits in force at
the release may not be the ones in force when it was armed. An absent field
reads as set, which is what every freeze meant before the field existed.

The probe budget looks like it already says this — a 429 arms with
`thawRequestCount > 0` and a 5xx with 0 — and was rejected as the signal for two
reasons. `retryOptions.thawRequestCount` may be configured to 0, giving a 429
freeze that emptied the bucket and owes no probes. And the merge carries the
field only from a freeze that still stands: a 5xx arm landing on a 429 freeze
whose window has lapsed but whose probes are still owed would otherwise inherit
"emptied" for a bucket that has been refilling since the window ended.

## Completion hand-off

A request's `requestDone` is announced by the replica that owns it, and only the
controller applies it — removes the entry, releases slots, arms freezes, counts
thaw progress. Pub/sub is at-most-once and controllers die, so the owner keeps
each completion it announced until a controller says it has applied it:

- The controller, after applying a completion owned by another replica, sends
  `requestDoneApplied:<ownerId>` back to that owner, and sends it again for any
  re-announcement of something it has already applied.
- The owner re-announces an unacknowledged completion on its health tick, no
  sooner than one full interval after it first stored it, so in the ordinary
  case the acknowledgement lands first and the public channel carries the
  completion once. It gives up on a completion after `MAX_COMPLETION_FORWARDS`
  unacknowledged announcements, whatever the cause — no controller, a lost
  acknowledgement, or a controller that keeps failing to apply it.
- An acknowledged completion is remembered in `completedRequestIds`, on workers
  as on the controller, so a late echo of a re-announcement is not stored again.
  Without that the owner's own echo re-armed it, and `requestDone` — which
  consumers meter — carried one completion on every tick, forever.
- A non-owner never applies its copy: it drops it on its next tick, and on
  promotion before applying anything else. Applied on promotion, it would
  replay a completion the previous controller had already applied — re-arming
  a freeze whose probe had lifted it. The owner's re-announcement is what
  reaches a new controller, and it is also what carries a completion past a
  controller that died with it unapplied.
- **A `sharedLimit` child forwards its completions to its budget owner**, which
  holds the queue and the freeze they apply to. While the owner is mid-rebuild
  there is no owner to reach, so the child keeps the completion and
  `scheduleCompletionForward` retries on a short timer until one is registered.
- **A rebuild hands the state to the replacement.** `adoptWaitingRequests` moves
  `pendingRequestDone` and `completedRequestIds` across, and the old client
  forwards anything that still reaches it. Left behind, an unacknowledged
  completion would never be re-announced, and a remembered one could be stored
  again from a late echo.

A [streamed response](rate-limits/streaming.md) announces twice, `phase:
"headers"` and then `phase: "end"`, and the phase is part of the completion id,
so each is applied and acknowledged on its own. The controller applies only its
own half of each: the queue entry and thaw progress at the headers, the slot
release and any freeze at the end. Split rather than delayed whole, because a
thaw probe's claim lives in its queue entry, and a probe streaming for half an
hour would hold the fleet's recovery for as long.

## One queue per budget, so one arbiter orders it

A `sharedLimit` client is constructed with the owner's name. Its queue, bucket,
freeze state, metadata prefix and channels are all the owner's, which is the
entire mechanism: the owner's controller drains one queue, and `priority` and
arrival order therefore mean something across every client on that budget.

That is why a `sharedLimit` may not be combined with other limits. A client with
budgets of its own cannot adopt the owner's namespace — its own bucket would land
in the owner's keyspace, metering the owner against a limit it never declared — so
it would need a second queue. Two queues on one balance stays *correct*, since
`acquireMultiLimit` is atomic and the budget is never oversold, but nothing
arbitrates between them: priority stops carrying across, and a queue of cheap work
takes each token as it refills while a costly head next door never accumulates
enough. Restoring fair ordering would mean one controller draining several queues
and choosing between their heads by score — a budget-group concept in election,
plus a peek-and-claim across queues. That is a real design, and it was rejected
for now in favour of not offering the combination at all.

The reverse causes no trouble and is allowed: a `sharedLimit` client may point at
an owner that declares several limits. It claims nothing itself, so the owner's
controller does the claiming, of all the owner's budgets at once.

## Absent state reads as permissive

Both backends read a missing key as the permissive answer. A token bucket with
no `tokens` field is full — `finiteNumber(..., maxTokens)` in
`lua/tokenBucket.ts`, `parseFiniteStored(..., config.maxTokens)` in the memory
backend — and absent freeze state is no freeze. That is the only sane reading
for a client's first request: nothing has been written yet, and the alternative
is that a new client stalls until something seeds its keys.

The same reading decides how long a bucket's key must live. An expired bucket
reads as full, so a key that lapses before the bucket could have refilled from
empty hands back budget that was spent — a weekly cap restored after a day of
quiet. Both backends size the key's TTL as the longer of the usual day and
`ceil(maxTokens / tokensToAdd) * interval`: `bucketTtl` in the memory backend,
and its twin fragment in `lua/fragments.ts`. Both clamp the result to the longest
TTL Redis accepts, `REDIS_MAX_TTL_SECONDS`. The clamp is not cosmetic: the
scripts `HSET` the spent balance before they `EXPIRE`, so a refill longer than
that — a valid configuration, if an odd one — would otherwise fail every request
on the bucket and spend a token each time. A key that outlives its refill by
~290 million years instead of forever loses nothing.

The cost is that **a key that was lost is indistinguishable from one that never
existed**. A bucket evicted mid-interval reads as a fresh full budget and the
fleet overspends; an evicted freeze resumes sending into a vendor that just
returned 429. Both are silent — there is no error to raise, because from the
script's point of view nothing went wrong.

The mitigation is operational rather than in code: Redis must run
`noeviction`, which is stated for users in
[the Redis backend's operational notes](backends/redis.md#operational-notes).
The alternative — reading absence as *unknown* and refusing admission until
state is proven — was rejected because it inverts the failure without removing
it. Every genuinely-first request and every new client would wedge alongside
every eviction, and nothing available at the point of the read distinguishes
the three. A limiter that fails closed on its own cold start is a worse trade
than one that depends on a documented Redis setting.

## An alive-instance set that omits the sweeper is not evidence

Orphan cleanup acts on the alive-instance set, and a set that has been lost is
indistinguishable from a fleet that died. Eviction under an `allkeys-*` policy, a
neighbour's `FLUSHDB`, and the window after a recovery before every replica has
re-registered all read the same way: "everyone is gone". Sweeping on that reads
every queued request as orphaned and deletes work whose owners are alive and
heartbeating — and the caller is told nothing, because it goes on waiting out its
admission budget and fails with `request_timeout` like any other slow request.

What makes the read checkable is that **every instance re-adds its own id on each
heartbeat**. A set that does not contain the sweeper's own id therefore cannot be
authoritative, whatever else it holds. Both backends refuse to sweep at all in
that case, and the Lua also refuses when its alive-id argument does not decode.
Skipping leaves a genuine orphan for one more health tick; sweeping on bad
evidence loses live work. The asymmetry decides it.

That is the opposite resolution to the section above, deliberately. An absent
token bucket is read permissively because a first request is the ordinary case
and failing closed would wedge every cold start. An absent alive set has no such
benign reading — nothing legitimately produces one that omits a live sweeper — so
the only thing it can mean is that the read is wrong.

One consequence, and it is the trade rather than a regression: **election
promotes a client to controller before its own registration lands**, so the
promotion sweep skips. A previously-crashed fleet's entries are reaped by the
health tick instead, up to `healthCheckIntervalMs` later — 10s by default. An
orphan lingering one tick is the price of never deleting live work, which is also
why the skip logs at debug rather than warn: every boot reaches it once with
nothing wrong.

## Two backends, one contract

`memoryBackend()` and `redisBackend()` implement the same interface and must be
semantically identical. `test/conformance.test.ts` is the cross-backend guard,
running one suite against both; `test/clientTypes/backendParity.test.ts` holds
the two queue-selection implementations to one answer. A change to one backend's
semantics is expected to touch the other.

Where they legitimately differ is *how* atomicity is achieved, not whether it
holds:

- Redis uses Lua scripts, because between a read and a write from process A,
  process B can and will interleave.
- The memory backend is synchronous. A function body runs to completion before
  anything else does, so atomicity is a property of the language rather than
  something to arrange. That is why there is no locking in it, and why it is
  faster.

Two things follow that are easy to get wrong:

- **Clamps must match on both sides.** A stats read that reports a token balance
  the acquire scripts would not actually hand out is a disagreement, even though
  neither side is "wrong" on its own. `maxTokens` can shrink underneath a stocked
  bucket — that is what `rateLimitChange` is for — so the clamp is unconditional
  rather than applied only after a refill.
- **`addRequest` must commit before any `publish` issued after it is delivered.**
  An abandonment announces itself on `requestDone` while the add may still be
  outstanding, and that announcement is what removes the entry. A removal
  delivered first finds nothing, the add then creates the entry, and it survives
  with a live `ownerId` that nobody awaits — spared by `cleanupOrphanedRequests`
  because its owner is alive, and pinning the queue non-empty, which disables the
  fast path for the whole client. Command order on one connection does not
  meet this — a `NOSCRIPT` resend can put the add behind a later `PUBLISH` — so
  what holds it is the removal tombstone both backends keep: see
  [a removed request cannot be re-added](#a-removed-request-cannot-be-re-added).
  A backend that batches, proxies or resolves the call across a yield has to
  arrange the same explicitly.

`close()` releases what the backend itself opened and keeps the data, on both.
The memory backend stops its sweeper and drops its subscriptions but keeps its
store; the Redis backend quits only the pub/sub duplicate it created and leaves
the caller's connection and every key untouched. Both stay usable afterwards —
`stop()` closes the backend and `start()` re-subscribes, so a handler restart is
a supported pattern rather than a leak, and neither backend refuses a
`subscribe` after a `close`. `doStop` clears `templateClientMap` along with the
clients, so the restart rebuilds every stored template client rather than
skipping it as already built.
