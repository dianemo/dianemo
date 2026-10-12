/**
 * The longest delay `setTimeout` honours. Above 2^31-1 ms Node fires the timer
 * after 1 ms instead, which would turn a long stand-down into none.
 */
export const MAX_TIMER_MS = 2_147_483_647;

/**
 * How long a response's `retry-after` header asks the caller to wait, in
 * milliseconds — or `undefined` when it carries none that parses.
 *
 * RFC 9110 §10.2.3 allows either delay-seconds or an HTTP date. A fractional
 * number of seconds is accepted too, since some vendors send one; a date
 * already past reads as no wait.
 */
export function retryAfterMs(
  headers: unknown,
  now = Date.now()
): number | undefined {
  const raw = headerValue(headers, "retry-after");
  if (raw === undefined) return undefined;
  const text = raw.trim();
  if (/^\d+(\.\d+)?$/.test(text)) {
    return clampDelay(Number(text) * 1000);
  }
  // Every HTTP-date form names its month; without one, `Date.parse` would read
  // "-5" as a year.
  if (!/[a-z]/i.test(text)) return undefined;
  const date = Date.parse(text);
  if (Number.isNaN(date)) return undefined;
  return clampDelay(date - now);
}

/** A delay usable as a timer: finite, not negative, and within `MAX_TIMER_MS`. */
export function clampDelay(ms: number): number {
  if (!Number.isFinite(ms) || ms <= 0) return 0;
  return Math.min(Math.ceil(ms), MAX_TIMER_MS);
}

/**
 * Reads a header from axios's `AxiosHeaders`, whose lookup ignores case, or from
 * a plain object, which the adapter fills with lower-cased names but a test or
 * interceptor may not.
 */
function headerValue(headers: unknown, name: string): string | undefined {
  if (typeof headers !== "object" || headers === null) return undefined;
  const getter = (headers as { get?: unknown }).get;
  let value: unknown;
  if (typeof getter === "function") {
    value = (getter as (header: string) => unknown).call(headers, name);
  } else {
    const key = Object.keys(headers).find((k) => k.toLowerCase() === name);
    value =
      key === undefined ? undefined : (headers as Record<string, unknown>)[key];
  }
  if (Array.isArray(value)) value = value[0];
  return typeof value === "string" || typeof value === "number"
    ? String(value)
    : undefined;
}
