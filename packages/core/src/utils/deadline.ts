/** What `settleWithin` returns when the time ran out first. */
export const TIMED_OUT: unique symbol = Symbol("timed out");

/**
 * Waits for `work` up to `ms`, then gives up on it without cancelling it. A
 * rejection still propagates if it lands in time; one that lands later is
 * observed by the race and so is not reported as unhandled.
 */
export async function settleWithin<T>(
  work: Promise<T>,
  ms: number
): Promise<T | typeof TIMED_OUT> {
  let timer: NodeJS.Timeout | undefined;
  const expired = new Promise<typeof TIMED_OUT>((resolve) => {
    timer = setTimeout(() => resolve(TIMED_OUT), Math.max(0, ms));
  });
  try {
    return await Promise.race([work, expired]);
  } finally {
    clearTimeout(timer);
  }
}
