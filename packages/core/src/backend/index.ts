/**
 * `@dianemo/core/backend` — the toolkit for implementing `DianemoBackend`.
 *
 * The contract types, re-exported from the root entry so an implementation
 * imports from one place, and the shared arithmetic both built-in backends
 * use: the queue-score layout, the claim lease, the TTL normalisers, the
 * stored-number parsers, the claim-mark key and the balance tolerance. A
 * backend that reads these rather than reimplementing them agrees with the
 * built-in ones by construction. Nothing here is needed to use the handler.
 * See docs/backends/README.md#implementing-a-backend.
 */

export {
  calculateQueueScore,
  MAX_QUEUE_PRIORITY,
  MAX_QUEUE_RETRIES,
  QUEUE_PRIORITY_BAND,
  QUEUE_RETRY_BAND,
  QUEUE_SCORE_EPOCH_MS,
} from "./queueScore.js";
export { QUEUE_CLAIM_LEASE_MS } from "./queueClaim.js";
export {
  formatStoredCost,
  parseFiniteStored,
  parsePriority,
  parseStoredCost,
  parseStoredNumber,
} from "./parseStored.js";
export {
  FREEZE_PROBE_TTL_SECONDS,
  normalizeStateTtlSeconds,
  normalizeTombstoneTtl,
  normalizeTtlSeconds,
  REDIS_MAX_TTL_SECONDS,
  REQUEST_TOMBSTONE_TTL_SECONDS,
} from "./ttl.js";
export { claimMarkKey } from "./settlement.js";
export { BALANCE_TOLERANCE } from "./balance.js";
export type {
  AcquireConcurrencyResult,
  AcquireMultiLimitResult,
  AcquireTokensResult,
  BatchOp,
  BatchOptions,
  ClaimMark,
  ConcurrencyConfig,
  DianemoBackend,
  FreezeState,
  MessageHandler,
  MultiLimitSpec,
  QueueStats,
  QueuedRequest,
  SettleResult,
  SettleSpec,
  TokenBucketConfig,
} from "./types.js";
