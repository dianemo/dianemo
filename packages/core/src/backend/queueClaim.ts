/**
 * How long a queue entry handed out by `getNextRequest` stays claimed before
 * either backend hands it out again, unless its claimer confirms it first.
 *
 * Bounds only the gap between the claim and `confirmRequest` — a handful of
 * backend round trips — not how long an admitted request runs: a confirmed entry
 * is never reclaimed. Why a lease, and what a lapsed one costs:
 * docs/design-notes.md#a-queue-claim-is-a-lease
 */
export const QUEUE_CLAIM_LEASE_MS = 2_000;
