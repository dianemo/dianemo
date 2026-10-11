---
'@dianemo/core': major
'@dianemo/backend-redis': major
---

A request's `cost` may name an amount per limit — `{ rpm: 1, itpm: 1_200, otpm: 4_096 }` — for a client whose limits meter different things, as an AI API's requests and tokens per minute do. The object must name every limit the client meters, or the request fails with the new `RequestCostMismatchError`; a number is still charged to every limit.

A request may `settle` its cost against its response: it reserves an estimate when admitted, and `settle` reports what the response says was used, which the bucket is corrected to. A settlement arriving after the bucket has refilled to its ceiling, or after a freeze or reset, does not charge or credit the current window for an earlier one. A `requestLimit`'s new `maxDebt` lets a settlement take its balance below zero, and `requestOptions.settleWindow` bounds how late a settlement may arrive. Streamed responses are not settled yet.

A failed response's `retry-after` header is now the minimum for both the retry wait and the freeze. `retryOptions.classifyError` decides, ahead of the built-in rules, whether a failure is retried and whether it freezes the fleet, and may set a minimum freeze — so a spend-cap 429 can fail at once instead of freezing the fleet until the cap resets.

BREAKING: `getClientStats()` reports each request bucket's `cost` as a record keyed by limit name rather than as one number. A queued request's per-limit cost is stored as JSON, which a replica built before per-limit costs reads as a cost of 1 against every limit, under-charging a token limit; stop the fleet and restart it, as for the rest of 2.0. For a custom `DianemoBackend`: `acquireMultiLimit`, `releaseMultiLimit` and `tryAdmitMultiLimit` take the cost on each `MultiLimitSpec` rather than as an argument; `QueuedRequest.cost` is `number | Record<string, number>`; `getQueueStats` returns `costByLimit`; a token-bucket claim may carry a `ClaimMark`; and the new optional `settleTokens` and `emptyTokenBucket` back settlement — a request with `settle` is refused on a backend without `settleTokens`. `RequestCostExceedsBudgetError` names the limit whose ceiling was exceeded when the client has several.
