/**
 * The one reading of a TTL argument, shared by every backend so that a value one
 * accepts is never silently ignored by the other.
 */

/**
 * Largest TTL Redis accepts for `EXPIRE`. Beyond this it rejects the command, and
 * measurement puts the boundary between 9.2e15 and 9.3e15 seconds.
 *
 * Also the ceiling a derived lifetime is clamped to — a bucket's refill time, a
 * tombstone sized from an unbounded `cleanupTimeout` — so that a valid
 * configuration never computes a TTL the write would then reject. The Redis
 * backend inlines the same literal; `test/conformance.test.ts` holds them equal.
 */
export const REDIS_MAX_TTL_SECONDS = 9_200_000_000_000_000;

/**
 * Floor for how long a removed request id stays refused by `addRequest`.
 *
 * Sized against the longest an outstanding add can trail the removal that overtook
 * it. That is not one command's round trip: the add belongs to worker W and can sit
 * on a wedged ioredis link for the whole of W's admission budget, while controller C
 * — a different, healthy process — commits the removal. Callers pass their own
 * `cleanupTimeout` and take this as the lower bound.
 *
 * Why a bound at all — a permanent marker would be correct and would grow one key
 * per request forever. Ids are `crypto.randomUUID()` and never recycled, so the only
 * add a tombstone can refuse is one for the very request that was removed.
 *
 * Contract and failure mode: docs/design-notes.md#a-removed-request-cannot-be-re-added.
 */
export const REQUEST_TOMBSTONE_TTL_SECONDS = 60;

/**
 * Normalises a caller's TTL into whole seconds a backend can store, or throws.
 *
 * Rounds a fraction **up**: a TTL is garbage collection rather than expiry
 * enforcement, so arriving a second late costs memory while arriving early can
 * discard a live credential.
 *
 * Throws rather than clamping a non-finite or out-of-range value, because both mean
 * the caller computed something it did not intend and the alternative is a key whose
 * lifetime nobody chose. Callers that legitimately have no bound should pass no TTL.
 *
 * A value at or below zero is returned unchanged and means "expire immediately" —
 * Redis `EXPIRE` semantics, which the memory backend matches.
 */
export function normalizeTtlSeconds(ttlSeconds: number): number {
  if (!Number.isFinite(ttlSeconds)) {
    throw new RangeError(
      `ttlSeconds must be a finite number, received ${String(ttlSeconds)}`
    );
  }
  if (ttlSeconds > REDIS_MAX_TTL_SECONDS) {
    throw new RangeError(
      `ttlSeconds ${ttlSeconds} exceeds the maximum a backend can store (${REDIS_MAX_TTL_SECONDS})`
    );
  }
  if (ttlSeconds <= 0) return ttlSeconds;
  return Math.ceil(ttlSeconds);
}

/**
 * The lifetime `removeRequest` gives its tombstone, in whole seconds, for any
 * `requested` value — never a throw, because the removal runs on every completion
 * and a caller's `cleanupTimeout` is what sizes it.
 *
 * A fraction rounds up. A `NaN`, a zero or a negative — no lifetime at all —
 * takes {@link REQUEST_TOMBSTONE_TTL_SECONDS}, since a mark that expires at once
 * refuses nothing. Clamped to {@link REDIS_MAX_TTL_SECONDS} above, which is what
 * an unbounded admission budget (`Infinity`) asks for: the window the mark
 * covers has no end, so the mark is as close to permanent as a key can be. The
 * Redis backend's `ops/queue.ts` carries the twin.
 */
export function normalizeTombstoneTtl(requested: number): number {
  if (!(requested > 0)) return REQUEST_TOMBSTONE_TTL_SECONDS;
  return Math.min(Math.ceil(requested), REDIS_MAX_TTL_SECONDS);
}
