---
'@dianemo/core': major
'@dianemo/backend-redis': major
---

A request with `responseType: "stream"` now runs until its stream ends rather than until its headers arrive: its concurrency slot stays held, it counts as in flight for shutdown, and its span stays open. While the stream is open its slot and claim marks are refreshed every third of `concurrencySlotTtl`, so a half-hour generation keeps its slot without a half-hour TTL delaying crash recovery. The new `requestOptions.streamTimeout`, an hour by default, closes a stream left open and releases its slot.

A stream settles when it ends, at what a `settleStream` observer read from the chunks as they passed, or at what the caller reports through `response.dianemo.settle`; the first wins. An observer that reports an error inside the stream freezes the client as a 5xx would. A thaw probe that streams counts as a success at its headers, so a long generation does not hold the fleet's recovery.

BREAKING: a streamed request announces `requestDone` twice — with `phase: "headers"` and then `phase: "end"` — so a host counting completions off that channel should count only `phase !== "headers"`. The caller's `response.data` is a `PassThrough` relaying axios's stream rather than axios's stream itself, and is destroyed with `StreamTimeoutError` or `StreamClosedError` when the timeout or a shutdown closes it. For a custom `DianemoBackend`, the new optional `touchConcurrency` and `touchClaimMark` keep a stream's claims alive; without them a stream outliving `concurrencySlotTtl` loses its slot to the reaper.
