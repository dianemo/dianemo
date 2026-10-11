/**
 * Reads a number a script sent as an `exact` string — see `exactFn` in
 * `lua/fragments.ts`. C's `printf` spells the non-finite values `inf`, `-inf`
 * and `nan`, and `Number` reads the first two as `NaN`. A number, as an error
 * reply carries, passes through.
 */
export function parseLuaNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (value === "inf") return Infinity;
  if (value === "-inf") return -Infinity;
  return Number(value);
}
