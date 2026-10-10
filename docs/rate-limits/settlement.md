# Settling a request against its response

Some vendors meter what a request _used_, and only the response says how much.
AI APIs are the common case: Anthropic and OpenAI limit input and output tokens
per minute alongside requests, and the output count is not known until the
model has finished. A request reserves an estimate when it is admitted, and
`settle` corrects that reservation once the response arrives.

```ts
rateLimit: [
  { name: "rpm",  type: "requestLimit", interval: 60_000, tokensToAdd: 1_000,   maxTokens: 1_000 },
  { name: "itpm", type: "requestLimit", interval: 1_000,  tokensToAdd: 33_333,  maxTokens: 2_000_000 },
  { name: "otpm", type: "requestLimit", interval: 1_000,  tokensToAdd: 6_667,   maxTokens: 400_000, maxDebt: 400_000 },
]
```

```ts
await handler.handleRequest({
  clientName: "anthropic:_:production",
  requestName: "anthropic.messages",
  method: "POST",
  url: "/v1/messages",
  data: body,
  cost: { rpm: 1, itpm: estimatedInputTokens, otpm: 1_000 },
  settle: ({ response }) =>
    response && {
      rpm: 1,
      itpm: response.data.usage.input_tokens + response.data.usage.cache_creation_input_tokens,
      otpm: response.data.usage.output_tokens,
    },
});
```

The reservation is what admission spends. The settlement moves each bucket to
where the actual usage would have left it: a debit when the response used more,
a credit when it used less.

## What `settle` returns

`settle` runs once per attempt — after a success, and after a failure before
any retry — on the replica that sent the request. It receives
`{ response }` or `{ response, error }`, and returns:

- **A number**, charged to every `requestLimit`.
- **An object keyed by limit name**, which must name every `requestLimit` the
  client declares and no `concurrencyLimit`. A slot is returned when the request
  completes whatever the response said, so there is nothing to settle there.
  Entries may be `0`: a response can report that nothing was spent.
- **`undefined`**, to let the reservation stand — after a network error, say,
  where the vendor's charge is unknown.

A result that names the wrong limits, a `settle` that throws, or a backend that
cannot be reached is logged and the reservation stands. The request itself
never fails because settlement did: the vendor has already answered.

Settling twice applies once. Each claim leaves a mark beside its bucket, and
settling consumes it.

## Reserve an estimate, not the worst case

Anthropic counts output tokens as they are generated; `max_tokens` does not
count against the limit. Reserving `max_tokens` would hold back budget that is
almost never used. Reserve a typical figure instead and let settlement charge
the rest — which is what `maxDebt` is for.

## `maxDebt`

A debit larger than the balance has two possible endings:

- **`maxDebt: 0`, the default.** The balance stops at zero and the rest goes
  uncharged, with a warning logged. Dianemo's picture of the budget is then more
  generous than the vendor's, so the next request may meet a 429, which the
  [freeze](../concepts.md) absorbs.
- **`maxDebt` above zero.** The balance may go that far below zero, and nothing
  is admitted until refill has paid the debt off — the client waits for the
  vendor's budget to recover rather than finding out with a 429.

Only settlement creates debt. Admission still needs a balance that covers the
whole reservation, and a 429 freeze never forgives a debt.

## A response that arrives late

A bucket stores a balance, not a history, so a settlement corrects the bucket as
it is now. If the bucket has refilled to its ceiling since the claim, some or
all of the claim has already aged out, and settlement accounts for it:

- **A debit** is reduced by the refill the bucket has discarded at its ceiling
  since the claim. Had the extra been charged at the start, that refill would
  have paid it off.
- **A credit** is dropped once the bucket has reached its ceiling since the
  claim, because it would have been discarded there.
- **Neither applies** across a freeze that emptied the bucket, or a reset.

The arithmetic is in the
[proposal](../proposals/usage-based-limits.md#settling-late-against-a-bucket-that-has-moved-on).

## `settleWindow`

How long after admission a settlement is still accepted, set on the client's
`requestOptions` in milliseconds. The default is 15 minutes, which covers a
non-streamed response that takes ten minutes to generate. A bucket that takes
longer than that to refill from empty is given that long instead. A settlement
arriving after the window applies nothing.

## Requirements and limits

- The backend must implement `settleTokens`. Both built-in backends do; a
  request with `settle` on one that does not is refused when it is made.
- A `sharedLimit` client settles against its owner's limits, by their names.
- Streamed responses are not settled yet: `settle` runs when the headers
  arrive, before the usage is known.
