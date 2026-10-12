import type { RequestCost } from "../request/types.js";

/**
 * Shared reads of numeric fields stored as strings, used by every backend.
 *
 * The Lua in the Redis backend is the reference implementation and this matches
 * it. The rule, and how to find its sites:
 * docs/design-notes.md#numeric-field-parsing
 */

/**
 * The infinities `tonumber` reads through C's `strtod` — `inf` or `infinity` in
 * any case, optionally signed — of which `Number` accepts only `Infinity`.
 */
const STRTOD_INFINITY = /^\s*([+-]?)inf(?:inity)?\s*$/i;

/**
 * Matches Lua's `tonumber` — absent, empty and unparseable all mean the default —
 * because `Number("")` is 0 and the two backends must read one stored field the same
 * way. The rule, and how to find its sites:
 * docs/design-notes.md#numeric-field-parsing
 */
export function parseStoredNumber(
  raw: string | undefined,
  fallback: number
): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const infinity = STRTOD_INFINITY.exec(raw);
  if (infinity) return infinity[1] === "-" ? -Infinity : Infinity;
  const parsed = Number(raw);
  // NaN, not "not finite". `tonumber("Infinity")` is `inf` — a number Lua then
  // clamps into the top band exactly as `Math.min` does here — so rejecting it as
  // unparseable would make the two backends order the same stored field
  // differently, which is the disagreement this helper exists to remove.
  return Number.isNaN(parsed) ? fallback : parsed;
}

/**
 * Finite on top of the shared parse — the one place a read and a score differ. A
 * score keeps `inf` so both backends agree; a read is serialised, and
 * `JSON.stringify(Infinity)` is `null`, which would arrive as a null priority.
 */
export function parsePriority(raw: string | undefined): number {
  const parsed = parseStoredNumber(raw, 1);
  return Number.isFinite(parsed) ? parsed : 1;
}

/**
 * The shared parse, with a non-finite value taken as the default too.
 *
 * For a field that is compared and then written back — a freeze deadline, a probe
 * budget — where an `inf` survives every comparison and only fails at the point of
 * serialisation. Twin of `finiteNumber` in the Redis backend's Lua fragments.
 */
export function parseFiniteStored(
  raw: string | undefined,
  fallback: number
): number {
  const parsed = parseStoredNumber(raw, fallback);
  return Number.isFinite(parsed) ? parsed : fallback;
}

/**
 * A queued request's cost as the string both backends store: a number as
 * itself, a per-limit cost as JSON. The two cannot be confused on the way back,
 * since a stored number never begins with `{`.
 */
export function formatStoredCost(cost: RequestCost): string {
  return typeof cost === "number" ? String(cost) : JSON.stringify(cost);
}

/**
 * Reads back what {@link formatStoredCost} wrote.
 *
 * A number keeps the long-standing lenient read — absent, empty, unparseable and
 * zero all mean 1 — so entries queued before per-limit costs existed decode as
 * they always did. A per-limit cost that does not parse decodes to `NaN` rather
 * than to that default: charging 1 against every limit would meter a token
 * budget as though it counted requests, where `NaN` is refused when the request
 * is admitted and says what went wrong.
 */
export function parseStoredCost(raw: string | undefined): RequestCost {
  if (raw === undefined || !raw.trimStart().startsWith("{")) {
    return parseFloat(raw ?? "") || 1;
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      !Array.isArray(parsed)
    ) {
      return parsed as Record<string, number>;
    }
  } catch {
    // Reported as NaN below.
  }
  return NaN;
}
