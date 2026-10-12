# Streamed responses

A request that sets `responseType: "stream"` gets its response body as a
stream. Dianemo treats that request as running until the stream ends, not until
the headers arrive:

- its concurrency slot stays held, and is kept alive while the stream runs;
- it counts as in flight, so shutdown waits for it;
- it can be [settled](settlement.md) at what the stream reported, such as the
  output tokens an AI model generated.

```ts
const response = await handler.handleRequest({
  clientName: "anthropic:_:production",
  requestName: "anthropic.messages.stream",
  method: "POST",
  url: "/v1/messages",
  data: { ...body, stream: true },
  responseType: "stream",
  cost: { rpm: 1, itpm: estimatedInputTokens, otpm: 1_000 },
  settleStream: anthropicUsage,
});

for await (const chunk of response.data) {
  // relay it on
}
```

`response.data` is a `PassThrough` relaying axios's stream. It keeps
backpressure, so the vendor's stream is read only as fast as the caller reads.

## The two moments

| Moment | What happens |
| --- | --- |
| **Headers** | The status decides as for any request: a 429 or a 5xx retries or freezes before the caller sees a body. The queue entry is settled, and a [thaw probe](../concepts.md#freeze-and-thaw) counts as a success — the vendor accepted it, which is all thawing asks, and a probe that held its claim for a half-hour generation would hold the whole fleet's recovery with it. `responseInterceptor` and `rateLimitChange` run. |
| **End** | The attempt is settled, its slot released and its count in flight dropped. |

The end is the first of:

- the upstream finishing, reported as `"completed"`;
- the caller destroying the stream, or the request's `signal` aborting,
  reported as `"closed"`. An abort destroys the caller's stream with
  `RequestAbortedError`;
- the upstream connection failing, reported as `"error"`;
- `streamTimeout`, reported as `"timeout"`;
- the handler stopping, reported as `"shutdown"`.

Anything but `"completed"` also closes the upstream, which stops a generation
nobody will read. `response.dianemo.finished` resolves with the reason once the
slot is released.

A streamed request announces `requestDone` twice, with `phase: "headers"` and
then `phase: "end"`; anything else announces once, without a `phase`. A host
that counts completions off that channel should count only `phase !== "headers"`.
A request admitted on the fast path has no queue entry and cannot be a thaw
probe, so it announces only its end.

## Settling a stream

Two ways, and the first to settle wins. Both go through the attempt's
[claim mark](settlement.md#what-settle-returns), so the second applies
nothing.

**An observer**, usually declared by a plugin's request definition, where the
vendor's event format is known. It sees each chunk as it passes through,
without consuming it:

```ts
const anthropicUsage = (): StreamObserver => {
  let buffered = "";
  let input = 0;
  let output = 0;
  let error: string | undefined;
  return {
    onChunk(chunk) {
      buffered += chunk.toString();
      const events = buffered.split("\n\n");
      buffered = events.pop() ?? "";
      for (const event of events) {
        const data = event.split("\n").find((line) => line.startsWith("data: "));
        if (!data) continue;
        const message = JSON.parse(data.slice(6));
        if (message.type === "message_start") {
          const usage = message.message.usage;
          input = usage.input_tokens + (usage.cache_creation_input_tokens ?? 0);
        } else if (message.type === "message_delta") {
          output = message.usage.output_tokens;
        } else if (message.type === "error") {
          error = message.error.type;
        }
      }
    },
    result: () => {
      const cost = { rpm: 1, itpm: input, otpm: output };
      return error ? { error, cost } : cost;
    },
  };
};
```

`result()` is called once however the stream ends, so a stream cut short
settles at what was generated — which is what the vendor bills. It may return:

- a cost, in the shape `settle` returns;
- `undefined`, to leave it to the caller;
- `{ error, cost? }`, for an error the vendor reported inside the stream. That
  freezes the client as a 5xx would, and settles at `cost` if given. Anthropic
  reports `overloaded_error` this way, after a 200.

`onChunk` and `result` must not throw. A throw is logged, the stream carries
on, and the claim stands.

**The caller**, when it parsed the usage itself:

```ts
await response.dianemo.settle({ rpm: 1, itpm: inputTokens, otpm: outputTokens });
```

It may run before the stream ends or after it. If a request declares
`settleStream` or `settle` and nothing has settled it five seconds after the
stream ended, a warning is logged and the claim stands. On a stream, `settle`
runs at the end rather than at the headers, after an observer that settled
nothing.

## A stream is never retried

The status is decided at the headers, so a 429 or a 5xx retries as usual. Once
the body has been handed over the caller may have read part of it, so nothing
that happens inside the stream retries it. An error reported inside the stream
freezes the client for the rest of the fleet; without an observer, dianemo
cannot see one and records a success.

## Keeping a long stream alive

A large generation streams for half an hour or more, far past
`concurrencySlotTtl`. While a stream is open its replica refreshes the
request's slots every third of `concurrencySlotTtl`, and its claim marks with
them, so the reaper never takes a slot that is still in use. A crashed replica
stops refreshing, so crash recovery is as quick as it has always been, and
`concurrencySlotTtl` no longer has to exceed the longest generation.

`requestOptions.streamTimeout`, default 60 minutes, closes a stream still open
after that long and releases its slot, so a stream the caller forgot cannot hold
one for ever. The caller's stream is destroyed with `StreamTimeoutError`.

The backend must implement `touchConcurrency` and `touchClaimMark` for this.
Both built-in backends do; on one that does not, a stream outliving
`concurrencySlotTtl` loses its slot to the reaper.

## Shutdown

A stream counts as in flight until it ends, so `stop()` waits for it within the
drain budget. A stream still open after that is closed with
`StreamClosedError`, its replica logs which requests were still streaming, and
shutdown waits up to five seconds more for each to release its slot.

## Tracing

The request's span stays open until the stream ends. It records
`dianemo.time_to_headers_ms` and `dianemo.stream_end_reason`, and it is marked as
an error when the stream timed out, failed, or reported an error.
