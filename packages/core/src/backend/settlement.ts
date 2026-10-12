/**
 * Settlement arithmetic shared by every backend. The memory backend calls it;
 * the Redis backend's `settleTokens` Lua is its twin and must agree with it line
 * for line. The reasoning:
 * docs/proposals/usage-based-limits.md#settling-late-against-a-bucket-that-has-moved-on.
 */

/** Where a claim's mark lives, beside the bucket it was claimed from. */
export function claimMarkKey(bucketKey: string, markId: string): string {
  return `${bucketKey}:claim:${markId}`;
}

export interface SettleInput {
  /** The balance after refilling to now. */
  tokens: number;
  maxTokens: number;
  maxDebt: number;
  actual: number;
  claimed: number;
  overflowNow: number;
  overflowAtClaim: number;
  /**
   * The total discarded refill that has paid down a debit on this bucket, and
   * that total when the claim was made. Forgiveness since the claim comes off
   * what the claim may be forgiven, so each stretch of refill forgives once,
   * however many claims were outstanding across it.
   */
  forgivenNow: number;
  forgivenAtClaim: number;
  fillsNow: number;
  fillsAtClaim: number;
  generationNow: number;
  generationAtClaim: number;
  /** Which incarnation of the bucket this is, and which the claim was made on. */
  epochNow: string;
  epochAtClaim: string;
}

export interface SettleOutcome {
  applied: number;
  unapplied: number;
  /** `SettleInput.forgivenNow` plus the discarded refill this used. */
  forgiven: number;
}

/**
 * How much to take from the balance now: positive debits, negative credits.
 * `unapplied` is the part of a debit `maxDebt` would not let the balance absorb.
 */
export function settleAmount(input: SettleInput): SettleOutcome {
  const none = { applied: 0, unapplied: 0, forgiven: input.forgivenNow };
  // A changed or missing epoch, or a counter gone backwards (they only grow),
  // means the bucket was recreated since the claim — as gone as a reset.
  if (
    input.epochAtClaim === "" ||
    input.epochNow !== input.epochAtClaim ||
    input.generationNow !== input.generationAtClaim ||
    input.overflowNow < input.overflowAtClaim ||
    input.fillsNow < input.fillsAtClaim
  ) {
    return none;
  }

  const delta = input.actual - input.claimed;
  if (delta > 0) {
    const slack = Math.max(
      0,
      input.overflowNow -
        input.overflowAtClaim -
        (input.forgivenNow - input.forgivenAtClaim)
    );
    const used = Math.min(delta, slack);
    const owed = delta - used;
    const maxDebt =
      Number.isFinite(input.maxDebt) && input.maxDebt > 0 ? input.maxDebt : 0;
    const room = Math.max(0, input.tokens + maxDebt);
    const applied = Math.min(owed, room);
    return {
      applied,
      unapplied: owed - applied,
      forgiven: input.forgivenNow + used,
    };
  }
  if (delta < 0 && input.fillsNow === input.fillsAtClaim) {
    const credit = Math.min(
      -delta,
      Math.max(0, input.maxTokens - input.tokens)
    );
    return { applied: -credit, unapplied: 0, forgiven: input.forgivenNow };
  }
  return none;
}
