/**
 * How far below `cost` a balance may fall and still cover it, as a fraction of
 * `maxTokens`. Fractional costs are not exact in binary, so a bucket of 1 spent
 * in claims of 0.01 drifts a few ulps short of the last claim. Relative to the
 * ceiling because drift grows with the balance's magnitude. The Redis backend
 * inlines the same literal into its scripts.
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
  return tokens >= cost - maxTokens * BALANCE_TOLERANCE;
}
