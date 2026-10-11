# Proposal: usage-based limits

**Status:** accepted; sections 1–4 and 6 are implemented. Header sync, section 5, is for a later release. Targets the same major version as
the multi-limit work. Once built, the user-facing parts move into
`docs/rate-limits/` and the invariants into `docs/design-notes.md`, and this
file is deleted.

Some vendors meter what a request *consumed*, not that it was made. AI APIs are
the common case: Anthropic and OpenAI publish requests per minute alongside
input and output tokens per minute, and the token count is not known until the
response — sometimes not until the last event of a stream. Dianemo today
charges one number, fixed before dispatch, against every limit a client
declares. This proposal makes cost per limit, settles it against what the
response reports, and carries both through streamed responses.

Nothing here is AI-specific. Any API whose cost is reported after the fact uses
the same machinery.

## What the vendors do

Anthropic's documented behaviour, which OpenAI's matches in shape:

- Requests, input tokens and output tokens are separate limits, per model class.
  Each is a token bucket that replenishes continuously rather than resetting at
  fixed intervals — the model `requestLimit` already implements.
- Input tokens are estimated when a request starts and corrected to the actual
  count. Cache reads do not count on most models; cache writes do.
- Output tokens are counted as they are generated. `max_tokens` does not factor
  in, so reserving it is far stricter than the vendor's own accounting.
- A 429 carries `retry-after` in seconds, and retrying sooner fails. A spend-cap
  429 carries no `retry-after` and an `error.details.error_code` of
  `enforced_spend_limit_reached`, and does not clear until the next month.
- Responses carry `anthropic-ratelimit-{requests,input-tokens,output-tokens}-remaining`,
  token counts rounded to the nearest thousand.
- A streamed response can report an error such as `overloaded_error` as an event
  after an HTTP 200.

## Delivery

Two branches, stacked on the multi-limit work, because streaming builds on the
claim marks and the settle API:

1. **Settlement** — per-limit cost, claim marks, settlement with `maxDebt`,
   `retry-after` and `classifyError`.
2. **Streaming** — the two-moment lifecycle, settlement from a stream, and the
   heartbeat that keeps long streams' claims alive.

Header sync (section 5) follows in a later release. Wrapping vendor SDKs is out
of scope for both; see [the roadmap](../roadmap.md#wrapping-existing-sdks).

## 1. Per-limit cost

```ts
cost?: number | Record<string, number>;
```

- **A number** charges every limit the same amount, as today.
- **An object** must name every `requestLimit` and `concurrencyLimit` the client
  declares — no more, no fewer. `noLimit` entries are never named. A single
  unnamed limit is `"default"`.
- **Zero is allowed per entry**, so an embeddings call can charge `otpm: 0`. At
  least one entry must be positive, which keeps the guard against a request that
  spends nothing by accident.
- **Each entry is held to its own ceiling**, not the smallest in the list.
  `RequestCostExceedsBudgetError` names the limit that refused.
- **Validated twice** — where the call is made, and again on the controller in
  `tryAcquireTurn`. A `rateLimitChange` can rename or add a limit while a request
  waits, and a stale object must fail with a typed error rather than skip a
  budget silently.
- **A `sharedLimit` client names the owner's limits**, since it spends the
  owner's budget. Origination resolves them through `getBudgetOwnerClient`.

Backend: cost moves onto each `MultiLimitSpec` as `spec.cost`, replacing the one
`cost` argument to `acquireMultiLimit`, `tryAdmitMultiLimit` and
`releaseMultiLimit`. `QueueStats.totalCost` becomes a per-limit map.

## 2. Settlement

*Implemented; the user-facing account is
[settling a request](../rate-limits/settlement.md).*

```ts
handler.handleRequest({
  ...,
  cost: { rpm: 1, itpm: estimate, otpm: 1_000 },
  settle: (response) => ({
    rpm: 1,
    itpm: response.data.usage.input_tokens + response.data.usage.cache_creation_input_tokens,
    otpm: response.data.usage.output_tokens,
  }),
});
```

- `settle` runs once per attempt, on the response or the error, and returns a
  `Cost` — or `undefined` to let the claim stand, as after a network error where
  the vendor's charge is unknown.
- The object form must name every `requestLimit` and must not name a
  `concurrencyLimit`: a slot is returned at completion regardless, so there is
  nothing to settle.
- It runs on the replica that sent the request and writes to the backend
  directly, not through `RequestDoneData`. The function cannot be serialised
  into the queue, and keeping it off the completion path keeps it independent of
  slot release and controller failover.
- Request definitions in a template may declare a default `settle`, so a plugin
  can settle for its callers.
- A settlement that credits budget back pokes the controller through the
  existing request-added notification, so queued work does not sleep through a
  wait computed before the credit.

### Settling late, against a bucket that has moved on

A bucket stores a balance and a timestamp, not a log of requests, so there is no
earlier entry to correct. The danger is a settlement landing on a bucket that has
already forgotten the claim: refill has since covered it, and applying the full
difference now would charge the current window for usage the vendor has already
aged out.

What the bucket can tell us is how much refill it has *discarded* since the
claim. Two counters on each bucket hash, maintained by every script that refills
— `lazyRefill` and its memory twin, including the clamp when `maxTokens` shrinks:

- **`overflow`** — total refill discarded at the `maxTokens` cap.
- **`fills`** — how many times refill reached the cap at all, exactly or over.

At claim, a **claim mark** records `{ claimed, overflow, fills, generation }`
under `<bucketKey>:claim:<requestId>:<retries>`, atomically with the claim. It
is written only for a request that can be settled: one that declares `settle` or
`settleStream`, or is streamed. At settlement, with `delta = actual − claimed`
and `slack = overflow now − overflow at claim`:

| Case | Applied now |
|---|---|
| Debit (`delta > 0`) | `max(0, delta − slack)` |
| Credit (`delta < 0`), `fills` unchanged since the claim, not frozen | the full credit, capped at `maxTokens` |
| Credit, the bucket has filled since the claim | nothing |
| Credit while frozen | nothing — as `refundTokens` already does |
| `generation` changed since the claim | nothing |
| No mark: expired, already settled, or claimed before this shipped | nothing, reported `settled: false` |

**Debits are exact.** Had the debit been charged at claim time, every refill
the bucket discarded since would have gone to paying it down instead. Only what
that discarded refill has not covered still counts.

**Credits are close, not exact.** A credit charged at claim time would have
reached the cap sooner and been discarded. If the bucket has filled since, that
certainly happened and the credit is dropped. If it has not, the credit applies
in full — over-generous by however close the bucket came to full, which is
bounded and errs toward admitting slightly early. Tracking that closeness would
need per-claim history, which is the cost this design avoids.

**`generation`** increments wherever the balance is overwritten rather than
spent — a single-limit freeze emptying its bucket, `resetTokenBucket`. An
overwrite erases every earlier adjustment, so none is applied across one.

Settlement reads and deletes the mark in the same operation, so it applies
exactly once however many times it is retried. That matters because the
completion path is already retried on failure, and settlement is arithmetic
where slot release is idempotent. The backend computes `delta` from the stored
`claimed`, so a caller cannot present a claim that mints budget.

The mark's TTL bounds how late a settlement may arrive. It is set per client as
`settleWindow`, defaulting to the longer of 15 minutes and the time a full bucket
takes to refill (`maxTokens / tokensToAdd × interval`). The mark is written at
admission, so the window has to cover a whole buffered response: a
non-streamed generation can run for ten minutes, which a per-minute bucket's
refill time would not. A stream's mark is refreshed while it runs (section 6). In Redis Cluster the
mark must hash to the same slot as its bucket.

### Debt: `maxDebt`

A debit larger than the balance either drops the excess or takes the balance
below zero. Dropping it under-counts, so the next request may meet a 429 the
freeze then absorbs; going negative holds the client until the vendor's budget
has genuinely recovered, at the risk that a wrong `settle` stalls it.

Per-limit and opt-in:

```ts
{ name: "otpm", type: "requestLimit", interval: 1_000, tokensToAdd: 6_667, maxTokens: 400_000,
  maxDebt: 400_000 } // the balance may reach -400,000; default 0
```

- Default `0`: the balance never goes below zero, which is today's behaviour.
- Only settlement can create debt. Admission still needs the balance to cover
  the whole cost, and the wait arithmetic (`needed = cost − tokens`) already
  handles a negative balance.
- A debit cut short by the floor reports the unapplied amount in its result and
  in a log line. A steady stream of them means `cost` is set too low.
- A single-limit freeze writes `min(tokens, 0)`, not `0`, so it never forgives
  debt.

Debt is the expected configuration for an output-token limit: reserve an
estimate rather than `max_tokens`, and let settlement charge the rest.

New backend methods: optional `settleTokens(specs)`, one atomic operation
across every bucket, and `emptyTokenBucket(key, config)`, which a freeze uses so
the generation moves on. A request that settles is refused when it is made on a
backend without `settleTokens`.

## 3. `retry-after`

*Implemented; see
[`retry-after` sets the minimum](../rate-limits/README.md#retry-after-sets-the-minimum).*

Nothing reads `retry-after` today. A 429's wait comes from `handleBackoff` —
the retry multiplier times the shortest refill interval — and with several
limits that freeze is the only stand-down, since the buckets are not emptied.
Against a one-second request limit, the fleet retries after about a second
while the vendor said twenty.

When the header is present, it becomes the minimum for both the retry wait and
the freeze. It is standard HTTP — seconds or an HTTP date — so every client
benefits, not just AI ones.

## 4. `classifyError`

*Implemented as `retryOptions.classifyError`; see
[classifying failures](../rate-limits/README.md#classifying-failures).*

Every 429 is retried and freezes the fleet. `retryHandler` cannot intervene,
because it only runs for statuses dianemo does not classify, and
`retry429s: false` gives up on rate limits entirely. A spend-cap 429, or
OpenAI's `insufficient_quota`, therefore burns through `maxRetries` and
refreezes the fleet on every request until next month.

A retry option that runs before the built-in rules:

```ts
classifyError?: (error: AxiosError) => ErrorDecision | undefined | Promise<…>; // undefined: built-in rules

type ErrorDecision =
  | { action: "fail" } // terminal: no retry, no freeze
  | { action: "retry"; freeze?: false } // retry this request only
  | { action: "retry"; freeze: true; freezeMs?: number }; // and freeze the fleet
```

`freezeMs` is for a vendor whose error body states how long to stand down. It
is a floor like `retry-after`, never a ceiling: the longer of the two wins, and
the freeze is still lengthened by repeated failures.

## 5. Header sync

*Later release.* Recorded here because sections 2 and 6 fix the order it must
run in.

Settlement corrects our own estimates. It cannot see anything else spending the
same key — another service, a script, a process outside dianemo — and the
balance drifts above the vendor's.

A `syncFromResponse` hook returns `{ [limitName]: remaining }` from the headers,
and the backend sets each balance to the lower of its own and the vendor's.

- **Lower only.** The header excludes requests still in flight, which our
  balance already includes, so raising it would count them twice.
- **After settlement** for a buffered response: the header already includes
  this response's usage. Settle first, then clamp, and it is counted once.
- **At headers** for a streamed response, before its output exists, so the
  settlement at the end of the stream does not double it either.

The existing `rateLimitChange` hook replaces configuration, not balance, and
stays as it is.

## 6. Streaming

*Implemented; see [streamed responses](../rate-limits/streaming.md). Two
departures from the sketch below: the headers settle the queue entry as well as
the thaw probe, by a first `requestDone` announcement carrying
`phase: "headers"`, and `settle` on a stream runs at the end rather than at the
headers. The heartbeat refreshes claim marks through a second backend method,
`touchClaimMark`.*

`responseType: "stream"` already reaches axios, but dianemo publishes
`requestDone` when the headers arrive. The slot is freed mid-generation,
`countInFlight` drops so a shutdown drain can close under a live stream,
settlement has no usage to read, and a long stream outlives both
`concurrencySlotTtl` and its claim mark.

### Two moments

| Moment | What happens |
|---|---|
| **Headers** | Status classification as today: a 429 or 5xx retries or freezes before any body reaches the caller. Header sync runs. A thaw probe reports success here — the vendor accepted it, which is all thawing asks. |
| **Stream end** | `requestDone` is published, the slot released, settlement run, in-flight decremented. The end is the first of: the stream finishing, erroring, being closed by the caller or an abort, or `streamTimeout`. |

A request is streamed when it sets `responseType: "stream"`; there is no
separate flag. Dianemo wraps `response.data` in a `PassThrough`, which keeps
backpressure and observes the end without the caller's help. The caller
receives the same stream type it asked for.

### Settling a stream

Either of two ways, both through the claim mark, so the first wins and the
other is a no-op:

```ts
// Explicit: the caller parsed usage itself
const response = await handler.handleRequest({ ..., responseType: "stream", cost });
await response.dianemo.settle({ rpm: 1, itpm: inputTokens, otpm: outputTokens });

// An observer, usually declared by a plugin's request definition
settleStream: () => {
  let usage: Usage | undefined;
  return {
    onChunk(chunk: Buffer) { /* read usage from message_start / message_delta */ },
    result: () => usage && { rpm: 1, itpm: usage.input, otpm: usage.output },
  };
},
```

- The observer sees each chunk as it passes through, so it reads exactly what
  the caller reads without consuming the stream. Vendor SSE parsing lives in the
  plugin; core never parses a body.
- `result()` is called however the stream ends, so a stream cut short settles
  at what was generated — which is what the vendor bills.
- If neither settles within 5 seconds of the stream ending, the claim stands
  and a warning is logged. The grace exists because an explicit settle often
  runs just after the last chunk.

### Keeping a long stream's claim alive

- A new backend method, `touchConcurrency(key, slotId, ttlMs)`, refreshes a
  streaming request's slot every `requestTtl / 3`. A crashed process stops
  refreshing, so crash recovery stays as fast as today, and
  `concurrencySlotTtl` no longer has to exceed the longest generation.
- The same tick refreshes the claim mark, so a ten-minute stream still finds it.
- `streamTimeout`, default 60 minutes and set per client, closes the stream and
  ends the attempt, so a forgotten stream cannot hold a slot indefinitely. A
  128k-token generation runs for half an hour or more, and with the heartbeat a
  long timeout no longer delays crash recovery.

### Errors inside a stream

A stream is never retried once its body has been handed over — the caller has
already consumed part of it. An observer's `result()` may also return
`{ error: "overloaded" }`, which is treated as a 5xx for freezing only. Without
an observer, core cannot see a mid-stream error and records success.

### Elsewhere

- **Shutdown** counts a stream as in flight until it ends. A drain that times
  out logs what is still streaming.
- **Metrics** measure duration to the end of the stream, plus time to headers.

## Rollout

Breaking, and part of the same major version as the multi-limit work; the
version plan already declares the bump. The upgrade note gains one hazard: a
replica built before per-limit costs reads an object `cost` from the shared
queue as `parseFloat` does, falls back to 1, and charges 1 against every limit —
under-charging a token limit. The major version already requires stopping the
fleet, which avoids it.

## Open questions

- **Header sync** — its hook shape, settled when it is picked up.
