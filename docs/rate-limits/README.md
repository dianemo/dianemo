# Choosing a rate limit

`rateLimit` is a list of limits — usually one, sometimes several. The choice
decides how the client behaves when requests compete, so match it to the limit
the vendor actually publishes rather than to whichever is easiest to configure.

| Type                                       | Use when the vendor limits            | Config                                 |
| ------------------------------------------ | ------------------------------------- | -------------------------------------- |
| [`noLimit`](no-limit.md)                   | nothing you need to enforce           | —                                      |
| [`requestLimit`](request-limit.md)         | requests per unit of time             | `interval`, `tokensToAdd`, `maxTokens` |
| [`concurrencyLimit`](concurrency-limit.md) | requests in flight at once            | `maxConcurrency`                       |
| [`sharedLimit`](shared-limit.md)           | one budget across several credentials | `clientName`                           |
| [several at once](multiple-limits.md)      | more than one of the above at once    | a `name` per limit                     |

## The quick decision

- The vendor says **"600 requests per minute"** → `requestLimit`. This is the
  common case by a wide margin.
- The vendor says **"no more than 5 concurrent connections"** → `concurrencyLimit`.
- You have several credentials that the vendor counts against **one** budget —
  sub-accounts, regional endpoints, a sandbox sharing production's quota →
  `sharedLimit` on the client that owns the budget.
- Internal service, or a vendor whose limit you genuinely do not need to
  respect → `noLimit`.
- The vendor says **"20 per second and 50,000 per day"**, or **"10 per second
  and at most 3 concurrent"** → [several](multiple-limits.md) of the above in one
  list. A request is sent only when every one of them admits it.

Rate limits are not either/or across a deployment: each client picks its own, so
one handler routinely runs all four at once.

## Cost and priority apply to all of them

Independent of type, each request may declare:

```ts
await handler.handleRequest({
  clientName: "carrier:_:production",
  requestName: "carrier.rates",
  method: "POST",
  url: "/rates",
  cost: 2, // how much budget this spends (default 1)
  priority: 1, // 0–10, higher is served first (default 1)
});
```

`cost` exists because vendors rarely price every endpoint the same — a bulk
export can legitimately count as ten calls. `priority` exists so a backfill can
be admitted behind interactive traffic instead of starving it.

A number is charged to every limit the client declares. When the limits meter
different things — an AI API limiting requests and tokens per minute — give an
amount per limit instead, keyed by name:

```ts
cost: { rpm: 1, itpm: 1_200, otpm: 4_096 },
```

The object must name every `requestLimit` and `concurrencyLimit` the client
declares and nothing else, or the request fails with
`RequestCostMismatchError`. An entry may be `0` — an embeddings call spends no
output tokens — but at least one must be positive. A single limit declared
without a name is called `default`, and a `sharedLimit` client names its
owner's limits, since those are the budgets it spends.

When the true cost is only known from the response — the tokens an AI model
generated — reserve an estimate and [settle](settlement.md) it afterwards.

`priority` feeds one packed sorted-set score: priority first, then retry count,
then arrival time; `cost` is not part of it. A retried request sorts ahead of a
fresh one at equal priority, so work already part-done finishes rather than
being crowded out.

Priority is strict: it does not age waiting work upward. Sustained
higher-priority traffic can starve lower-priority traffic, and a costly head on
one shared client-level budget is not bypassed by cheaper work. Grant-isolated
budgets are scanned independently so one tenant cannot cause that head-of-line
blocking for another.

## What every type shares

Whatever the strategy, all clients get the same surrounding machinery:

- **The uncontended fast path.** When nothing is queued and the budget covers
  the request, admission is decided by a single atomic operation and the queue
  is skipped entirely. See [backends](../backends/README.md).
- **Freeze and thaw.** A 429 freezes the client fleet-wide, and so does a 5xx
  or a dropped connection. After a 429, recovery is probed by one request at a
  time across the whole fleet, rather than every replica retrying into a closed
  door, and the freeze clears after `thawRequestCount` (default 3) successful
  probes. A freeze armed by a 5xx or a dropped connection has no probes and
  simply lapses.
- **Leader election.** One process drains each client's queue; the rest enqueue
  and await.
- **Per-grant isolation.** An `oauth2` client with
  `grantRateLimitBehavior: "isolated"` meters each grant separately, so one
  tenant hitting their ceiling does not stall the others. See
  [grants](../concepts.md#grants).

### `retry429s: false` disarms the freeze

The freeze is derived from the retry decision, so switching the retry off
switches the freeze off with it. `retry429s: false` hands a 429 straight to the
caller and **arms no freeze**, which means the rest of the fleet keeps sending
into the closed door. It is legal and it does not warn.

That is the only setting that disarms it. If you want the fleet-wide back-off,
leave `retry429s` on.

### A zero back-off does not disarm it

`retryBackoffBaseTime: 0` means "retry _this request_ with no local delay". It
says nothing about whether the rest of the fleet should keep hammering an
upstream that has just refused us, so it is not permitted to produce a
zero-length freeze — which the arming gate would read as no freeze at all.

For the freeze, a zero back-off is replaced by **1000ms**; a positive one is
used as it is. That is the whole rule for a freeze armed by a 5xx or a dropped
connection, on every type, and for every freeze on `concurrencyLimit` and
`noLimit`. Only a 429's freeze on a client with a refill interval is floored
further: [`requestLimit`](request-limit.md#freezing) floors it at the refill
interval, since the freeze is not over until the bucket can refill; a
`sharedLimit` child floors it at its parent's shortest refill interval, and a
client with [several limits](multiple-limits.md) at the shortest of its own.
The same replacement catches a `retryBackoffBaseTime` that is negative or
`NaN` — an unset `Number(process.env.…)` is the usual source, and a negative
would otherwise put `frozenUntil` in the past and leave the client in permanent
single-probe thaw.

So `retryBackoffBaseTime: 0` on a `noLimit` client gives you a request that
retries immediately and a fleet that still stands down for a second. The
retry wait and the freeze are separate numbers and are allowed to disagree.

### `retry-after` sets the minimum

When a failed response carries `retry-after` — seconds, or an HTTP date — it
becomes the minimum for both the retry wait and the freeze. The back-off above
still applies when it asks for longer, so repeated failures lengthen a freeze
past what the header said. A wait is capped at 2^31-1 ms, about 24.8 days,
the longest a Node timer holds.

### Classifying failures

Every 429 is retried and freezes the fleet; so is every 5xx. Some failures
deserve neither. Anthropic's spend-cap 429 does not clear until the next month,
and retrying it only refreezes the fleet on every request until then.
`retryOptions.classifyError` decides before the built-in rules do:

```ts
retryOptions: {
  classifyError: (error) => {
    const body = error.response?.data as
      | { error?: { details?: { error_code?: string } } }
      | undefined;
    return body?.error?.details?.error_code === "enforced_spend_limit_reached"
      ? { action: "fail" }
      : undefined;
  },
},
```

It returns one of:

- **`{ action: "fail" }`**: the caller gets the error now, with no retry and no
  freeze.
- **`{ action: "retry" }`**: this request is retried within `maxRetries`, and
  the fleet carries on.
- **`{ action: "retry", freeze: true, freezeMs? }`**: retried, and the fleet
  freezes. `freezeMs` is a minimum like `retry-after`: the longer of the two
  wins. The freeze counts as a rate limit — emptying the bucket and owing thaw
  probes — only when the status was 429.
- **`undefined`**: the built-in rules decide.

It runs on every failed attempt, the last included, because a freeze is about
the upstream rather than the request. A `classifyError` that throws, or returns
anything else, is logged and the built-in rules decide. `retryHandler` still
runs for what the built-in rules leave to it.
