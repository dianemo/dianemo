/**
 * How long a queue entry handed out by `getNextRequest` stays claimed before
 * either backend hands it out again, unless its claimer confirms it first.
 *
 * Bounds only the gap between the claim and `confirmRequest` — a handful of
 * backend round trips — not how long an admitted request runs: a confirmed entry
 * is never reclaimed. Why a lease, and what a lapsed one costs:
 * docs/design-notes.md#a-queue-claim-is-a-lease
 *
 * Sized for the three or four sequential round trips between claim and confirm
 * to fit under a Redis latency spike of about 2.5 s each; at 2 s, a 600 ms
 * spike refused every confirmation and stalled the queue. Longer costs only
 * how late a lost claim is recovered, still well inside `cleanupTimeout`.
 */
export const QUEUE_CLAIM_LEASE_MS = 10_000;
