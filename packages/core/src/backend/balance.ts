/**
 * How far below `cost` a balance may fall and still cover it, as a fraction of
 * the smaller of `cost` and `maxTokens`. Fractional costs are not exact in
 * binary, so a bucket of 1 spent in claims of 0.01 drifts a few ulps short of
 * the last claim. Scaled by the claim rather than the ceiling alone, so a spent
 * bucket with a large ceiling cannot admit a claim it holds almost none of. The
 * Redis backend inlines the same literal into its scripts.
 */
export const BALANCE_TOLERANCE = 1e-9;

/**
 * Whether `tokens` covers `cost`. Twin of the `tokens >= cost - ...` test in
 * the Redis backend's token-bucket and multi-limit scripts.
 */
export function coversCost(
  tokens: number,
  cost: number,
  maxTokens: number
): boolean {
  return tokens >= cost - Math.min(maxTokens, cost) * BALANCE_TOLERANCE;
}
