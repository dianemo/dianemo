# `requestLimit`

A token bucket. The common case by a wide margin, and the right answer whenever
a vendor expresses its limit as _requests per unit of time_.

```ts
rateLimit: [
  {
    type: "requestLimit",
    interval: 1000,      // ms between refills
    tokensToAdd: 100,    // tokens added each interval
    maxTokens: 100,      // ceiling the bucket can hold
  },
]
```

A request spends `cost` tokens (default 1). If the bucket cannot cover it, the
request queues until it can.

## Translating a published limit

| Vendor says             | Configuration                                      |
| ----------------------- | -------------------------------------------------- |
| 100 requests per second | `interval: 1000, tokensToAdd: 100, maxTokens: 100` |
| 600 per minute          | `interval: 100, tokensToAdd: 1, maxTokens: 60`     |
| 5,000 per hour          | `interval: 720, tokensToAdd: 1, maxTokens: 100`    |

Note that the second and third rows are **not** the literal transcription. That
is deliberate, and it is the single most consequential thing on this page.

## Refill granularity dominates tail latency

`interval`/`tokensToAdd` and its scaled-down equivalent grant the same average
rate. They do not produce the same latency. Each row is configured for the same
140 req/s average:

| Configuration                        | Throughput | p99        |
| ------------------------------------ | ---------- | ---------- |
| `interval: 10000, tokensToAdd: 1400` | 177/s      | 4,790 ms   |
| `interval: 1000, tokensToAdd: 140`   | 142/s      | 602 ms     |
| `interval: 100, tokensToAdd: 14`     | 142/s      | **199 ms** |

**About 3× better p99 at the same throughput** going from a one-second interval
to a 100 ms one, purely from granularity. The ten-second lump's p99 is 24× the
trickle's, but that run also measured a different throughput, so it is not a
like-for-like comparison. With a lump, the budget is spent early in the interval
and everything that arrives afterwards waits for the next refill. With a
trickle, waits are short and even.

So a vendor limit quoted as "1400 per 10 seconds" invites exactly the wrong
configuration. Prefer the smallest interval that still expresses the limit
honestly. The one reason not to: if the vendor enforces a _hard window_ rather
than a rolling rate, a trickle can under-use the quota near window boundaries.
Most vendors do not.

## `maxTokens` is your burst allowance

Setting `maxTokens` above `tokensToAdd` lets an idle client accumulate and then
spend a burst:

```ts
rateLimit: [{ type: "requestLimit", interval: 1000, tokensToAdd: 4, maxTokens: 8 }]
```

This client sustains 4/s but can spend 8 immediately after being idle. Use it
when the vendor tolerates bursts; keep `maxTokens === tokensToAdd` when it does
not. Note that a full bucket at startup is real spendable burst — a short
measurement window can legitimately show more than 100% of the nominal budget.

## Refill is lazy

Tokens are not added by a timer. The bucket records `tokens` and `lastUpdate`,
and refill is computed from elapsed time when a request arrives. This is what
makes coordination across processes cheap — there is no ticker to synchronise,
and a bucket nobody touches costs nothing.

`lastUpdate` advances in **whole intervals**, never to "now", so partial
progress toward the next refill is never silently discarded.

That detail matters to the caller more than it looks. When a request cannot be
admitted, the backend returns a `waitTime` that the client **sleeps on
directly** — so an over-estimate is not a rounding error, it is a stall of
exactly that length. A bucket 9 seconds into a 10-second interval must report
about a 1-second wait, not a 10-second one, and the conformance suite asserts it.

A bucket's key is kept until the bucket could have refilled from empty, because
a key that expired reads as a full bucket. See
[limits slower than a day](multiple-limits.md#limits-slower-than-a-day).

## Cost larger than the budget

A request whose `cost` exceeds `maxTokens` can never be satisfied, so it is
rejected up front with `RequestCostExceedsBudgetError` rather than queued
forever.

## Per-grant isolation

When a client carries several credentials, each grant can meter its own bucket,
so one tenant exhausting their quota does not stall the others. That takes an
`oauth2` client with `grantRateLimitBehavior: "isolated"`; under the default,
`"shared"`, every grant spends the client's one bucket. Grant-scoped
requests deliberately skip the uncontended fast path: that path checks a single
client-level bucket and freeze state, which would be the wrong pair of keys.

## Freezing

If the upstream returns a hard rate-limit response, the client's bucket is reset
to zero and the client freezes. Recovery is probed one request at a time across
the fleet. See [choosing a rate limit](README.md) for the shared machinery. A
client declaring [several `requestLimit`s](multiple-limits.md) does not empty its
buckets.

A 5xx or a dropped connection freezes the client too, but leaves the bucket as
it is: that is upstream trouble, not proof the budget was wrong.

**The floor depends on the response**, and this is the type where the difference
shows. A 429 empties the bucket, so both its freeze and the failing request's
retry wait are floored at `interval` — lapsing sooner would reopen admission onto
zero tokens, which buys nothing. A 5xx or a dropped socket leaves the bucket
untouched, so its freeze and its retry wait both take the configured back-off,
`retryBackoffBaseTime`, with no floor at `interval`: flooring them would stop
the fleet for a full window over one bad response. For the freeze, a zero,
negative or `NaN` back-off becomes 1000 ms, as described in
[choosing a rate limit](README.md#a-zero-back-off-does-not-disarm-it).

That split exists because of the advice at the top of this page. Expressing a
limit with the smallest honest `interval` is good for latency, but it also makes
`interval` a poor back-off for upstream trouble. So `retryBackoffBaseTime`
decides how long the fleet stands down over a 5xx, and the refill interval is
the least it stands down over a 429.
