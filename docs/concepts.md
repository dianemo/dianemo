# Concepts

The vocabulary, and the pieces that do not belong to any single rate limit or
backend.

| Term           | Meaning                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------ |
| **Template**   | The _shape_ of an integration: rate limit, auth flow, sub-clients. Carries no credentials. |
| **Client**     | A template plus one set of credentials, named `<template>:<orgId \| _>:<alias>`.           |
| **Sub-client** | A child of a client that shares its endpoint and auth but meters its own budget.           |
| **Grant**      | A credential _within_ one client, optionally rate-limited on its own.                      |
| **Plugin**     | A template plus the request functions that use it, packaged together.                      |
| **Instance**   | One process running a handler.                                                             |
| **Controller** | The instance currently draining a given client's queue.                                    |

## Client names

```
<templateName>:<organizationId | "_">:<alias>[:<subClientPath>]
```

Position-based, so anything downstream can parse a name without knowing the
template. `_` in the second position means the integration is global rather than
scoped to an organisation.

```ts
import { buildClientName, parseClientName } from "@dianemo/core";

buildClientName("fedex", { instanceId: "production" });
// "fedex:_:production"

parseClientName("fedex:acme-co:production:rest");
// { templateName: "fedex", organizationId: "acme-co",
//   alias: "production", subClientPath: "rest" }
```

`parseClientName` returns `null` for anything with fewer than three segments —
`default`, the handler's no-template client, is the usual case.

Use `parseClientName` in anything that receives a client name from elsewhere: a
metrics listener, a dynamic rate-limit controller, an admin UI. Splitting on `:`
by hand works right up until a sub-client path appears.

## Sub-clients

One vendor, one set of credentials, several endpoint families that the vendor
meters **separately**. A REST API and a bulk-feed API under the same account
often have entirely different quotas.

```ts
await handler.registerClientTemplate("acme", (creds) => [
  {
    name: buildClientName("acme", creds),
    rateLimit: [
      {
        type: "requestLimit",
        interval: 1000,
        tokensToAdd: 100,
        maxTokens: 100,
      },
    ],
    requestOptions: { defaults: { baseURL: creds.baseUrl } },
    authentication: {/* ... */},
    subClients: [
      {
        name: "bulk",
        rateLimit: [
          {
            type: "requestLimit",
            interval: 60_000,
            tokensToAdd: 10,
            maxTokens: 10,
          },
        ],
      },
      {
        name: "search",
        rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 4 }],
      },
    ],
  },
]);
```

A sub-client **inherits** every field of its parent that it does not declare
itself — endpoint configuration, authentication, retry options,
`rateLimitChange` and `rateLimit` among them — and **overrides** whatever it
declares, most usefully its own `rateLimit`. An inherited `rateLimit` is a copy
of the parent's limits, not a share of them: the sub-client meters it as a
separate budget under its own name. (An inherited `sharedLimit` still names the
same owner, so it draws on that owner's budget.) An inherited `rateLimitChange`
runs on the sub-client's own responses and retunes the sub-client's own budget,
so a callback written for the parent's limit names has to cope with the
child's. Objects that are not plain — an `httpAgent`, a `Buffer` — are shared
with the parent as the same object rather than copied.

A sub-client that declares its own `authentication` authenticates as itself: its
tokens are filed under its own name, not its parent's. One that declares none
uses its parent's credential entry and so its parent's tokens.

Address a sub-client by appending its name:

```ts
await handler.handleRequest({
  clientName: "acme:_:production:bulk",
  requestName: "acme.bulk.export",
  method: "POST",
  url: "/bulk/export",
});
```

Sub-clients nest. `acme:_:production:a:b` is `subClients[a].subClients[b]`.

### Sub-client or separate template?

Use a **sub-client** when the credentials and base configuration are genuinely
the same and only the budget differs. Use a **separate template** when the auth
flow or base URL differs — inheritance you have to override entirely is worse
than no inheritance.

Use [`sharedLimit`](rate-limits/shared-limit.md) for the opposite case: separate
credentials that must draw on _one_ budget. Sub-clients split one credential
into several budgets; `sharedLimit` merges several credentials into one.

## Grants

A grant is an OAuth2 credential inside a single client — typically one per
tenant, where the vendor meters each tenant separately but you want one client
and one template.

Isolation is opt-in: set `grantRateLimitBehavior: "isolated"` on the client's
`oauth2` authentication. Each grant then gets its own budgets and its own freeze
state, so one tenant exhausting their quota does not stall the others, and a 429
for one does not pause the rest. The default, `"shared"`, meters every grant
against the client's own budgets and freezes them together.

On a client with a budget — `requestLimit`, `concurrencyLimit` — grant-scoped
requests deliberately skip the uncontended fast path: that path checks a single
client-level bucket and freeze key, which would be the wrong pair for an
isolated grant.
They take the queued path, which reads the right ones.

`noLimit` is the exception, and it is one worth knowing about. It has no bucket,
so its fast path reads the right freeze key for a grant and keeps it — grant
traffic is admitted uncontended, which is the whole point of not configuring a
limit. What it cannot do is answer "is anything waiting?" per grant: there is one
queue per client, and the queue-depth check that stops a new arrival overtaking
work already waiting is necessarily client-wide. So while one tenant has a
backlog, another tenant's arrival takes the queued path behind it.

Isolation is of **budgets and freezes, not of arrival order**. One tenant's 429
never pauses another and one tenant's exhausted quota never spends another's, but
a tenant with a deep backlog does add queueing latency for its neighbours on a
`noLimit` client. Conceding the fast path for all grant traffic would remove that
coupling and cost every grant-scoped request the queued path forever — five extra
backend round trips each, measured — which is the opposite of what a client with
no limit is for.

```ts
await handler.handleRequest({
  clientName: "acme:_:production",
  requestName: "acme.orders.list",
  method: "GET",
  url: "/orders",
  grantId: "tenant-42",
});
```

## Rate-limit overrides

Templates ship in code; the right limit is sometimes discovered in production.
Overrides let a specific `(template, instance)` pair deviate without a redeploy,
keyed by sub-client path:

```ts
await handler.addTemplateClient("acme", credentials, {
  rateLimitOverrides: {
    "": [{ type: "requestLimit", interval: 1000, tokensToAdd: 50, maxTokens: 50 }],
    bulk: [
      {
        type: "requestLimit",
        interval: 60_000,
        tokensToAdd: 5,
        maxTokens: 5,
      },
    ],
  },
});
```

`""` targets the parent; a named path targets that sub-client. A bare record of
paths is still accepted in that argument, as it was before this grew a wrapper.

An override may retune anything about the budgets themselves — a different type,
a different count, [several where there was one](rate-limits/multiple-limits.md).
**The one thing it may not do is swap a client's own budget for another
client's**: a `sharedLimit` client owns no queue, so that is a different kind of
client rather than a different limit. Such an override is skipped with a warning
rather than applied.

A limit given without a `name` keeps the name of the one limit of its type it
replaces, so retuning a client's only `requestLimit` keeps the budget it has
already spent rather than starting a full bucket under `default`.

Overrides are checked before anything is stored. A path given a bare limit
rather than a list, a limit that cannot be built, or path keys mixed in beside
`rateLimitOption` or `rateLimitOverrides` is a `ConfigurationError` from
`addTemplateClient`, and nothing is written. Overrides stored by 1.x, which held
single limits rather than lists, are read back as lists — and one of a different
`type` from the template default's, which 1.x skipped, now applies. See the
[upgrade note](rate-limits/multiple-limits.md#names).

A tenant whose stored settings cannot be built — at startup, or when its template
is registered late — is logged and skipped, and the tenants after it still load.
Re-adding it with `addTemplateClient` replaces what is stored.

This is the operator-side escape hatch and needs no permission from the template.
For a choice the plugin itself sanctions — a subscription plan, picked from a
list the plugin wrote — see
[letting callers pick a plan](writing-plugins.md#letting-callers-pick-a-plan).
When a tenant is on a plan, its overrides apply on top of the plan's limits, so
the override wins, and an unnamed limit in an override takes the name of the
plan's limit of that type.

## Instances, controllers and leader election

Every process running a handler registers itself as an **instance** and
heartbeats. For each client, one instance is elected **controller** and drains
that client's queue; the rest enqueue and await a `requestReady` broadcast.

This is why recovery is not a thundering herd, and why adding replicas does not
multiply the work done on the shared queue. When an instance stops or stops
heartbeating, its requests are reclaimed and roles are recomputed.

Under the [memory backend](backends/memory.md) there is exactly one instance by
definition, and it is always the controller.

## Freeze and thaw

When an upstream returns a 429, the client **freezes** — admission stops until
the freeze lapses, and a client with exactly one `requestLimit` also has its
bucket emptied, whatever `noLimit` or `concurrencyLimit` entries sit beside it.
A client with
[several `requestLimit`s](rate-limits/multiple-limits.md#freeze-does-not-empty-the-buckets)
keeps its buckets and relies on the freeze window alone. A 5xx, or a connection
reset or timeout, freezes the client as well, but leaves every bucket as it is
and needs no thaw probes: the upstream was unwell, which says nothing about the
budget, and that freeze ends when its deadline lapses.

The two kinds of freeze are timed differently, for the same reason. A 5xx's
freeze, like the failing request's retry wait, takes the configured back-off,
`retryBackoffBaseTime`, with a zero, negative or `NaN` back-off replaced by
1000 ms for the freeze. Only a 429's freeze is floored at the client's shortest
refill interval, and on a client that owns its budgets so is the 429's retry
wait. See
[a zero back-off does not disarm it](rate-limits/README.md#a-zero-back-off-does-not-disarm-it).

A claim handed back unsent — an aborted or abandoned request — is refunded even
while a freeze stands, unless that freeze emptied the bucket, which only a 429
on a client with exactly one `requestLimit` does.

Recovery from a 429 is not "everyone retries at once". Once the deadline
passes, one request at a time across the whole fleet is nominated as the **thaw
probe**. Each probe that succeeds counts toward `thawRequestCount` (default 3),
and the freeze clears when that many have. A failed probe does not consume thaw
progress, and the next request becomes the next probe.

With isolated grants, probing is per grant: one probe per grant rather than one
per client, so recovering tenants do not wait behind each other. Without
isolation every grant shares the client's one freeze, so grant-scoped requests
probe one at a time like everything else.

### Clearing a freeze by hand

A freeze ends once its deadline has lapsed and its probe budget is spent — by
successful probes after a 429, at once for a freeze that has none. Probes only
finish a freeze whose window has lapsed: if a failure re-arms the window while a
probe is in flight, that probe succeeding leaves the new deadline standing.
Nothing in the library ends a freeze before its deadline, deliberately: arming
a freeze keeps the **longer** of the two deadlines, so that failures
arming concurrently cannot cut each other short. The same rule keeps a deadline
computed from a skewed clock or from an extreme `retryBackoffBaseTime`. Such a
deadline is bounded — both backends clamp it to 2^53-1 — but bounded far past
any horizon you care about. Waiting the key out is not an escape either: the
freeze key's TTL is sized from the deadline plus a minute, so it outlives the
freeze rather than expiring under it.

The escape hatch is the backend, not the client. Three methods on
`DianemoBackend` exist for this, and nothing in the library calls them:

- `clearFreezeState(key)` discards the freeze and its probe budget.
- `resetTokenBucket(key, maxTokens)` refills a bucket the freeze emptied.
- `clearConcurrency(key)` drops a slot ledger.

The keys they take are built from the handler's `keyPrefix` and the client's
name:

```
<keyPrefix>:requestHandler:<clientName>:freezeState
<keyPrefix>:requestHandler:<clientName>:rateLimit:<limitName>
<keyPrefix>:requestHandler:<clientName>:concurrency:<limitName>
```

A budget carries the name of the limit that owns it, so a client declaring
[several](rate-limits/multiple-limits.md) keeps them apart. A limit declared
without a name is named `default`.

`<keyPrefix>:` is absent if you did not configure one, and spaces in a client
name become underscores. A grant-isolated client freezes each grant separately,
so its keys carry `grant:<grantId>` straight after the client name —
`<keyPrefix>:requestHandler:<clientName>:grant:<grantId>:freezeState`, and
likewise `…:grant:<grantId>:rateLimit:<limitName>` and
`…:grant:<grantId>:concurrency:<limitName>`.

## Cost and priority

Both apply to every rate-limit type.

- **`cost`** — how much budget a request spends. Vendors rarely price every
  endpoint the same; a bulk export can legitimately count as ten calls. Under
  `concurrencyLimit`, cost occupies that many slots. Must be a finite number of
  at least `Number.EPSILON`; fractional costs are supported and spend
  fractionally.
- **`priority`** — an integer 0–10, higher served first. A backfill at priority 0
  is admitted behind interactive traffic at priority 5 instead of starving it.

Both are validated per request: a cost that is zero, negative, `NaN` or infinite
is rejected, as is a priority outside 0–10. A negative cost would otherwise mint
budget rather than spend it.

The queue orders by one packed sorted-set score: priority first, then retry
count, then arrival time. A retried request sorts ahead of a fresh one at equal
priority, so work already part-done finishes rather than being crowded out.

This ordering is strict, not aging-based. A continuous supply of higher-priority
work can therefore starve lower-priority work. Within one client-level budget,
an expensive head that does not currently fit also waits ahead of cheaper work;
the scheduler preserves order rather than using spare capacity out of turn.
Grant-isolated clients are the exception: a blocked grant is skipped so it does
not stall unrelated tenants. A grant-scoped request on a shared client is not
skipped — its grant has no budget of its own to wait on.

`cost` is not part of the score — it decides what a request spends, not where it
sits. Ordering within a priority band is by arrival to the millisecond, which
makes it approximate rather than strict FIFO: requests enqueued in the same
millisecond are ordered arbitrarily, and across replicas the timestamp comes from
whichever one received the request, so clock drift between them reorders the
queue. Priority ordering is unaffected — the bands are far wider than any
plausible drift.

## Cancellation

Pass an `AbortSignal` as `signal` and the request is cancellable at every stage:

```ts
const controller = new AbortController();
const res = handler.handleRequest({
  clientName: "acme:_:production",
  requestName: "acme.orders.list",
  method: "GET",
  url: "/orders",
  signal: controller.signal,
});
controller.abort();
```

Aborting before the request is sent spends **nothing** — no token, no concurrency
slot, no queue place — and a request already queued gives up its position rather
than holding it until the budget arrives. This matters most for the case it was
added for: a caller who has given up used to keep its place in the queue, be
admitted in turn, spend the budget, and only then fail inside axios, with real
traffic waiting behind it for a refill it did not need.

**Which error you get depends on when the abort lands.** Before dispatch it is
`RequestAbortedError` (499), this library's own. After dispatch the call belongs
to axios, which cancels it and throws its `CanceledError`. So a consumer branching
on `err.code === "request_aborted"` catches the give-up cases but not a signal
that fires while the response is in flight — check for both, or check the
`signal.aborted` you already hold.

`signal` must be a real `AbortSignal`. Axios types its own as `GenericAbortSignal`,
whose `addEventListener` is optional, but its Node adapter calls that method
unguarded — so a looser shape throws from inside axios _after_ this library has
admitted the request and spent its budget. That is rejected up front with a
`ConfigurationError` instead, on the same principle as a malformed `cost`: a bad
per-request value is refused where it enters, so it cannot resurface later as a
different and wrong diagnosis.

## Errors

Retries with backoff are automatic; what surfaces is what remained after them.
`httpStatusCodesToMute` downgrades expected-but-noisy statuses to debug logs
rather than errors — useful for a vendor that returns 500 for "not found".

A failed upstream call reaches a `handleRequest` caller as axios's own error,
from the last attempt. Everything dianemo raises itself extends
`RequestHandlerError` — `RequestError` aside, which plugins throw and which has
the same fields — and carries `statusCode` and `code` alongside `message`, so a
host that never imports dianemo's error classes can still map them:

```ts
if (typeof err.statusCode === "number" && typeof err.code === "string") {
  res
    .status(err.statusCode)
    .json({ error: err.code, error_description: err.message });
}
```

| Class                           | Status | When                                                                                       |
| ------------------------------- | ------ | ------------------------------------------------------------------------------------------ |
| `ClientNotFoundError`           | 404    | A request named a client that isn't registered — usually a typo.                           |
| `ClientUnavailableError`        | 503    | A client didn't arrive in time, or shutdown cancelled the wait.                            |
| `ClientConflictError`           | 409    | Duplicate client registration.                                                             |
| `RequestTimeoutError`           | 408    | Queued past `cleanupTimeout` without being admitted.                                       |
| `RequestAbortedError`           | 499    | The caller's `signal` fired before the request was sent.                                   |
| `RequestCostExceedsBudgetError` | 400    | A request's `cost` exceeds a limit's `maxTokens` or `maxConcurrency`.                      |
| `RequestCostMismatchError`      | 400    | A per-limit `cost` does not name exactly the limits the client meters.                     |
| `NotOAuth2ClientError`          | 400    | Grant-token operations attempted on a client that is not OAuth2.                           |
| `GrantRefreshTokenMissingError` | 400    | An OAuth2 grant was asked to refresh but has no stored refresh token.                      |
| `NoResponseError`               | 500    | Defensive: the retry loop ended with no response and nothing to rethrow.                   |
| `ConfigurationError`            | 500    | Bad configuration — a duplicate plugin, an unusable rate limit, a malformed `cost`.        |
| `RequestError`                  | 500    | A plugin call failed; `tryHandleRequest` puts the original under `metadata.originalError`. |

`getOriginalStatus(err)` and `getOriginalResponseData(err)` read the underlying
vendor response off a `RequestError`, for callers that want to turn a 404 into
`null`.

Constructing an error does not log. `tryHandleRequest` logs, once, where the
failure is first observed — so a rethrow never produces a duplicate line.

**A hook that runs after a successful response cannot fail the request.** Once the
upstream has answered 2xx, the call has already had its effect at the vendor —
money moved, a shipment was booked — so a throw from `responseInterceptor` or from
`rateLimitChange` is logged and the response is still returned. Reporting a
success as a failure would invite the caller to retry something already done. Put
validation that must be able to reject a response in your own code around
`handleRequest`, not in these hooks. `requestInterceptor` is the opposite case: it
runs before the request is sent, so a throw there does prevent the call.

## Health checks and probes

Each client runs a periodic health check (`healthCheckIntervalMs`, default 10s)
that reclaims orphaned requests and nudges the queue.

`probeRequest` is separate: it describes a request an _external_ scheduler can
fire to test whether a sustained-downtime integration has recovered. Dianemo
stores the configuration and does not fire it itself.
