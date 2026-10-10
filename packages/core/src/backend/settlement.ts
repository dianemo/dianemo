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
  fillsNow: number;
  fillsAtClaim: number;
  generationNow: number;
  generationAtClaim: number;
}

/**
 * How much to take from the balance now: positive debits, negative credits.
 * `unapplied` is the part of a debit `maxDebt` would not let the balance absorb.
 */
export function settleAmount(input: SettleInput): {
  applied: number;
  unapplied: number;
} {
  const none = { applied: 0, unapplied: 0 };
  // Counters only grow, so one that went backwards means the bucket expired and
  // was recreated since the claim — as gone as a reset.
  if (
    input.generationNow !== input.generationAtClaim ||
    input.overflowNow < input.overflowAtClaim ||
    input.fillsNow < input.fillsAtClaim
  ) {
    return none;
  }

  const delta = input.actual - input.claimed;
  if (delta > 0) {
    const owed = Math.max(
      0,
      delta - (input.overflowNow - input.overflowAtClaim)
    );
    const maxDebt =
      Number.isFinite(input.maxDebt) && input.maxDebt > 0 ? input.maxDebt : 0;
    const room = Math.max(0, input.tokens + maxDebt);
    const applied = Math.min(owed, room);
    return { applied, unapplied: owed - applied };
  }
  if (delta < 0 && input.fillsNow === input.fillsAtClaim) {
    const credit = Math.min(
      -delta,
      Math.max(0, input.maxTokens - input.tokens)
    );
    return { applied: -credit, unapplied: 0 };
  }
  return none;
}
