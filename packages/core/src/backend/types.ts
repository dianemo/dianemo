import type { RequestCost } from "../request/types.js";

/** A token bucket as the backend meters it: a `requestLimit` without its name. */
export interface TokenBucketConfig {
  /** Bucket ceiling; a bucket with no stored state reads as this full. */
  maxTokens: number;
  /** Tokens credited per whole `interval` elapsed. */
  tokensToAdd: number;
  /** ms between token additions */
  interval: number;
}

/**
 * Asks a token-bucket claim to leave a mark a later settlement can find: what
 * was claimed, and how far the bucket had got when it was. See
 * docs/proposals/usage-based-limits.md#2-settlement.
 */
export interface ClaimMark {
  /** Identifies the attempt; the mark lives at `claimMarkKey(bucketKey, id)`. */
  id: string;
  /**
   * How long a settlement may arrive after the claim, in seconds. Read by
   * `normalizeStateTtlSeconds`, so zero or below throws a `RangeError` before
   * the claim spends anything.
   */
  ttlSeconds: number;
}

/** One bucket's share of a settlement. */
export interface SettleSpec {
  /** The bucket the attempt claimed from. */
  key: string;
  /** The `ClaimMark.id` the claim was made under. */
  markId: string;
  /** What the response says the attempt actually cost against this bucket. */
  actual: number;
  config: TokenBucketConfig;
  /** How far below zero a debit may take the balance. */
  maxDebt: number;
}

export interface SettleResult {
  key: string;
  /** False when no mark was found: expired, already settled, or never written. */
  settled: boolean;
  /**
   * Change applied to the balance: positive for a debit, negative for a credit.
   * Less than the difference from the claim when the bucket had already
   * forgotten some of it — see the proposal.
   */
  applied: number;
  /** Debit `maxDebt` would not let the balance absorb. */
  unapplied: number;
}

/** A slot ledger as the backend meters it. */
export interface ConcurrencyConfig {
  /** Total cost that may be in flight at once. */
  maxConcurrency: number;
  /**
   * ms before a slot expires — reclaims slots held by a crashed process. Must
   * be a finite number above 0; anything else is refused through
   * {@link AcquireConcurrencyResult.error}, or `false` on the fast path, since
   * a slot claimed against it would never be reaped.
   */
  requestTtl: number;
}

/** What {@link DianemoBackend.acquireTokens} reports. */
export interface AcquireTokensResult {
  acquired: boolean;
  remainingTokens?: number;
  /**
   * ms to wait before retrying, when not acquired: the whole intervals the
   * shortfall needs, less the time already served toward the next one.
   *
   * A cost above `maxTokens` still gets a finite wait, and the retry is
   * declined again, since the bucket never holds more than `maxTokens`. The
   * backend does not refuse such a cost; callers check that ceiling
   * themselves. `Infinity` arises only from overflow — a shortfall so large,
   * or a `tokensToAdd` so small, that the arithmetic leaves the number range.
   */
  waitTime?: number;
  /**
   * Set when the configuration itself is unusable. A cost that is negative or
   * not finite is reported here too.
   */
  error?: string;
}

/** What {@link DianemoBackend.acquireConcurrency} reports. */
export interface AcquireConcurrencyResult {
  acquired: boolean;
  currentConcurrency?: number;
  /**
   * Set when the configuration itself is unusable, as opposed to merely full,
   * so a `maxConcurrency` or `requestTtl` of `NaN` or `0` is not mistaken for a
   * busy client. Mirrors `AcquireTokensResult.error`.
   */
  error?: string;
}

/**
 * One budget a request must claim, named by the key it lives under: a token
 * bucket at `<client namespace>:rateLimit:<name>` or a slot ledger at
 * `<client namespace>:concurrency:<name>`, each with `:grant:<grantId>` after
 * the namespace for an isolated grant.
 *
 * A client with several limits presents the whole list to the backend at once,
 * because claiming them one at a time is not the same operation: a peer can
 * take the second budget between two calls, leaving the first spent on a
 * request that is then declined. See docs/rate-limits/multiple-limits.md.
 */
export type MultiLimitSpec = {
  /**
   * The freeze governing this budget — the claiming client's, or its grant's.
   * `releaseMultiLimit` withholds a refund while it stands.
   */
  freezeKey?: string;
  /**
   * What the request spends against this budget. Per budget, not per claim,
   * because a request can charge each limit differently — one request against
   * a per-minute request limit, a thousand tokens against a token limit. May be
   * 0, which this budget then admits without spending.
   */
  cost: number;
} & (
  | {
      kind: "tokenBucket";
      key: string;
      config: TokenBucketConfig;
      /** Leave a mark for settlement, written only if the whole claim admits. */
      mark?: ClaimMark;
    }
  | {
      kind: "concurrency";
      key: string;
      config: ConcurrencyConfig;
      /** Member id the slot is recorded under; released by the same id. */
      slotId: string;
    }
);

/** What {@link DianemoBackend.acquireMultiLimit} reports. */
export interface AcquireMultiLimitResult {
  acquired: boolean;
  /**
   * ms to wait before retrying — the longest any declining token bucket asked
   * for. A full concurrency ledger has no deadline of its own, so a decline
   * caused only by one carries `0`. A cost above a bucket's `maxTokens` gets a
   * finite wait that never succeeds, and `Infinity` comes only from overflow,
   * as in {@link AcquireTokensResult.waitTime}. Absent when acquired or on
   * `error`.
   */
  waitTime?: number;
  /** Set when one of the configurations is unusable, as opposed to merely full. */
  error?: string;
  /**
   * Key of a budget that declined, for logs: the bucket with the longest wait,
   * or the first full slot ledger when no bucket declined. Unset when every one
   * admitted.
   */
  blockedBy?: string;
}

/** A queue entry as the backend stores it. */
export interface QueuedRequest {
  /** Unique per `handleRequest` call; the queue member and metadata key. */
  requestId: string;
  clientName: string;
  /** The caller's label for the request, for logs and metrics. */
  requestName: string;
  /**
   * `pending` while it waits; `inProgress` once
   * {@link DianemoBackend.getNextRequest} has handed it to the admission loop.
   */
  status: "pending" | "inProgress";
  /** Higher served first. Part of the queue score, which clamps it to 0-10. */
  priority: number;
  /**
   * Budget spent on admission: a number, or one amount per limit name. Stored
   * through `formatStoredCost` and read back through `parseStoredCost`.
   */
  cost: RequestCost;
  /** Attempts already failed; more retries sort ahead. Part of the score. */
  retries: number;
  /** Arrival in epoch ms; earlier sorts ahead at equal priority and retries. */
  timestamp: number;
  /** The grant whose budget and freeze it uses; absent for the client's own. */
  grantId?: string;
  /** Whether this entry holds its grant's single thaw probe. */
  isThawRequest: boolean;
  /** Instance id of the replica that queued it, for orphan cleanup. */
  ownerId: string;
  /**
   * The request will settle its cost against its response, so whoever claims
   * for it leaves a claim mark. Stored only when true.
   */
  settles?: boolean;
  /**
   * Backend-clock epoch ms at which {@link DianemoBackend.getNextRequest}
   * handed the entry out, while that claim is unconfirmed. Absent on a pending
   * entry and on one whose admission {@link DianemoBackend.confirmRequest}
   * recorded; an entry still carrying it after `QUEUE_CLAIM_LEASE_MS` is handed
   * out again.
   */
  claimedAt?: number;
}

/** A client's or grant's freeze, as stored under its freeze key. */
export interface FreezeState {
  /** Epoch ms on the backend's clock — see {@link DianemoBackend.now}. */
  frozenUntil: number;
  /** Successful probes still owed before the client runs at full rate again. */
  thawRequestCount: number;
}

/** Queue depth by status, and the total cost queued. */
export interface QueueStats {
  pending: number;
  inProgress: number;
  /** Summed over requests whose cost is a single number. */
  totalCost: number;
  /**
   * Summed per limit name over requests whose cost names its limits. Kept apart
   * from `totalCost` because the backend does not know which limits a number
   * applies to — the client does, and combines the two.
   */
  costByLimit: Record<string, number>;
}

/**
 * A single write in a {@link DianemoBackend.batch} group.
 *
 * `ttlSeconds` is the key's lifetime in seconds, rounded up, as
 * {@link DianemoBackend.set} reads it: zero or below deletes the key, and a
 * non-finite value or one above `REDIS_MAX_TTL_SECONDS` throws a `RangeError`
 * before any op in the group is applied.
 */
export type BatchOp =
  | { op: "set"; key: string; value: string; ttlSeconds?: number }
  | { op: "del"; key: string }
  | { op: "expire"; key: string; ttlSeconds: number }
  | { op: "sadd"; key: string; member: string }
  | { op: "srem"; key: string; member: string }
  | {
      op: "hset";
      key: string;
      fields: Record<string, string | number>;
      ttlSeconds?: number;
    }
  | { op: "publish"; channel: string; message: string };

export interface BatchOptions {
  /**
   * Apply the group without another client interleaving, so a reader observes it
   * either before or after but never mid-way. Costs a little more than a plain
   * batch, so it is opt-in — use it where a torn read would actually mislead a peer.
   *
   * Isolation, not rollback: Redis `MULTI`/`EXEC` runs the remaining commands after
   * one of them fails and keeps the effects of those that succeeded. Backends
   * therefore validate what they can before the first write, but an op that fails on
   * the server — a `WRONGTYPE` from a key another service owns — still leaves its
   * siblings applied. Group ops that must not tear behind one key you control.
   */
  atomic?: boolean;
}

/** Receives messages for channels passed to {@link DianemoBackend.subscribe}. */
export type MessageHandler = (channel: string, message: string) => void;

/**
 * The storage and coordination contract every dianemo backend implements.
 *
 * Domain operations rather than storage primitives, because each one must be atomic
 * with respect to other spenders and every backend arranges that differently. Keys
 * are built by the caller, so key layout stays the handler's business.
 *
 * Implementing one: docs/design-notes.md#two-backends-one-contract.
 */
export interface DianemoBackend {
  /** Names the implementation for a host to log or branch on, e.g. `"redis"`. */
  readonly kind: string;

  /**
   * Whether this backend shares state between processes. `false` means the budget it
   * enforces covers **this process only**, so N processes send N times the agreed rate.
   *
   * Declared for a host to inspect — nothing in dianemo reads it, so this is not a
   * guard against running a single-process backend across replicas.
   */
  readonly distributed: boolean;

  // ---------------------------------------------------------------- store

  get(key: string): Promise<string | null>;
  /**
   * Writes `value` under `key`, replacing whatever was there.
   *
   * @param ttlSeconds The key's lifetime in seconds, rounded up; omitted, the
   * key does not expire. Zero or below deletes the key instead of writing it.
   * A non-finite value, or one above `REDIS_MAX_TTL_SECONDS`, throws a
   * `RangeError` before anything is written.
   */
  set(key: string, value: string, ttlSeconds?: number): Promise<void>;
  del(...keys: string[]): Promise<void>;
  hgetall(key: string): Promise<Record<string, string>>;
  /**
   * Sets `fields` on the hash at `key`, leaving its other fields alone. An
   * empty `fields` writes nothing but still applies `ttlSeconds`.
   *
   * @param ttlSeconds As {@link DianemoBackend.set}'s: seconds, rounded up,
   * and zero or below deletes the hash. A non-finite value, or one above
   * `REDIS_MAX_TTL_SECONDS`, throws a `RangeError` before any field is set.
   */
  hset(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ): Promise<void>;
  sadd(key: string, member: string): Promise<void>;
  srem(key: string, member: string): Promise<void>;
  smembers(key: string): Promise<string[]>;

  /**
   * Applies several writes together — one round-trip where that is a concept.
   *
   * Every `ttlSeconds` in the group is read as {@link DianemoBackend.set}
   * reads it, and all of them are checked before the first write: one that is
   * non-finite or above `REDIS_MAX_TTL_SECONDS` throws a `RangeError` with
   * nothing applied.
   */
  batch(ops: BatchOp[], options?: BatchOptions): Promise<void>;

  // ---------------------------------------------------------------- locks

  /**
   * Takes `key` for `ttlMs` if it is free. `token` identifies the holder so
   * that only the holder can release it.
   *
   * @throws {RangeError} for a `ttlMs` that is not a finite number above 0,
   * which would otherwise take the lock with no lifetime.
   */
  acquireLock(key: string, token: string, ttlMs: number): Promise<boolean>;
  /** Releases `key` only if `token` still holds it. */
  releaseLock(key: string, token: string): Promise<boolean>;

  // --------------------------------------------------------------- pub/sub

  publish(channel: string, message: string): Promise<void>;
  /**
   * Starts delivering messages on `channels` to `handler`. Called once during
   * handler startup; the backend owns whatever connection that needs.
   */
  subscribe(channels: string[], handler: MessageHandler): Promise<void>;

  // ---------------------------------------------------- admission (fast path)

  /**
   * Admits a request immediately, or declines — never queues, never blocks.
   *
   * Declining is not a failure; it means the caller should take the queued
   * path. Must refuse whenever anything is already queued, so a new arrival
   * can never overtake work that has been waiting. Also refuses inside
   * `freezeKey`'s freeze window and, after it, while thaw probes are still
   * owed, since only the queued path can send one probe at a time. Returns
   * `false`, rather than throwing or reporting, for an unusable `config` or
   * `cost`.
   *
   * @param ttl The bucket key's lifetime in seconds, default 86400, raised to
   * cover a refill from empty. Normalised by `normalizeStateTtlSeconds`, so a
   * non-finite one, or zero or below, throws a `RangeError` before anything
   * is read.
   */
  tryAdmitImmediately(
    queueKey: string,
    bucketKey: string,
    freezeKey: string,
    cost: number,
    config: TokenBucketConfig,
    ttl?: number,
    mark?: ClaimMark
  ): Promise<boolean>;

  /**
   * The same decision for a client with no budget: admit unless something is queued
   * or a freeze or probe stands. One call, because answering the two separately lets
   * a peer enqueue in between and be overtaken.
   */
  tryAdmitNoLimit(queueKey: string, freezeKey: string): Promise<boolean>;

  /**
   * The fast path for a slot ledger: claims a slot of `cost` under
   * `requestId` and returns `true`, or declines with `false` and claims
   * nothing.
   *
   * Refuses on the same terms as {@link DianemoBackend.tryAdmitImmediately}:
   * anything queued, a standing freeze window, or thaw probes still owed. Slots
   * older than `config.requestTtl` are reaped first, and a slot `requestId`
   * already holds is not counted against it. Returns `false` for an unusable
   * `config` or `cost`.
   *
   * @param ttl The ledger key's lifetime in seconds, default 86400. Normalised
   * by `normalizeStateTtlSeconds`, so a non-finite one, or zero or below,
   * throws a `RangeError` before anything is read.
   */
  tryAdmitConcurrency(
    queueKey: string,
    concurrencyKey: string,
    freezeKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig,
    ttl?: number
  ): Promise<boolean>;

  // ------------------------------------------------------------ token bucket

  /**
   * Spends `cost` from the bucket at `key` if its balance covers it, and leaves
   * `mark` beside the bucket when it does.
   *
   * Refills lazily first, crediting `tokensToAdd` per whole `interval` elapsed
   * up to `maxTokens`; a bucket with no stored state reads as full. The refill
   * is written back whether or not the spend succeeds. Never queues, and
   * ignores freezes: those are the caller's to check.
   *
   * An unusable `config` or `cost` is reported through `error` rather than
   * thrown.
   */
  acquireTokens(
    key: string,
    cost: number,
    config: TokenBucketConfig,
    mark?: ClaimMark
  ): Promise<AcquireTokensResult>;
  /**
   * The balance as {@link DianemoBackend.acquireTokens} would see it now,
   * refilled and clamped to `maxTokens`, without writing anything. `lastUpdate`
   * is the backend's clock for an empty key.
   */
  getTokenBucketState(
    key: string,
    config: TokenBucketConfig
  ): Promise<{ tokens: number; lastUpdate: number }>;
  /**
   * Writes the bucket full at `maxTokens`, its refill clock restarted now. This
   * ends every outstanding settlement.
   */
  resetTokenBucket(key: string, maxTokens: number): Promise<void>;
  /**
   * Returns budget claimed for a request that was never dispatched, up to
   * `maxTokens`, which must be a finite number above 0. A bucket with no stored
   * balance is left alone, since it already reads as full.
   *
   * Refused while `freezeKey` holds a standing freeze that emptied the bucket —
   * one armed without `emptiesBucket: false`, see
   * {@link DianemoBackend.armFreeze}. A freeze that left the bucket alone
   * refuses nothing, since the token was never taken from the budget the
   * freeze is protecting.
   *
   * A cost that is negative or not finite credits nothing, as
   * {@link DianemoBackend.acquireTokens} refuses to spend one.
   */
  refundTokens?(
    key: string,
    cost: number,
    maxTokens: number,
    freezeKey?: string
  ): Promise<void>;

  // -------------------------------------------------------------- settlement

  /**
   * Empties a bucket for a freeze: to zero, or left where it is if already in
   * debt, so a freeze never forgives what settlement charged. Ends every
   * outstanding settlement on the bucket, as a reset does.
   *
   * Optional so an earlier backend still satisfies the interface; without it a
   * client writes `tokens: 0` directly, and settlement is unavailable.
   */
  emptyTokenBucket?(key: string, config: TokenBucketConfig): Promise<void>;
  /**
   * Corrects each claim to what its response reported, in one operation.
   *
   * Reads and deletes each claim's mark, so a retried settlement applies once.
   * A debit is reduced by the refill the bucket has discarded at its ceiling
   * since the claim; a credit is dropped once the bucket has filled since, and
   * both are dropped across an `emptyTokenBucket` or `resetTokenBucket`. The
   * arithmetic:
   * docs/proposals/usage-based-limits.md#settling-late-against-a-bucket-that-has-moved-on.
   */
  settleTokens?(specs: SettleSpec[]): Promise<SettleResult[]>;

  // --------------------------------------------------------------- streaming

  /**
   * Restamps a held slot as claimed now, so a request still streaming its
   * response is not reaped by `requestTtl` while it is alive. A slot that is
   * gone stays gone. `ttlSeconds` re-arms the ledger key's expiry, and is
   * normalised by `normalizeStateTtlSeconds`: one that is not finite and above
   * 0 throws a `RangeError` rather than deleting every holder's slot.
   *
   * Optional so an earlier backend still satisfies the interface; without it a
   * stream that outlives `concurrencySlotTtl` loses its slot to the reaper.
   *
   * @returns Whether the slot was still held.
   */
  touchConcurrency?(
    key: string,
    slotId: string,
    ttlSeconds: number
  ): Promise<boolean>;
  /**
   * Extends a claim mark's lifetime, so a stream that outlives its
   * `settleWindow` can still settle. A mark that is gone stays gone. A
   * `ttlSeconds` that is not finite and above 0 throws a `RangeError`.
   *
   * @returns Whether the mark still existed.
   */
  touchClaimMark?(
    bucketKey: string,
    markId: string,
    ttlSeconds: number
  ): Promise<boolean>;

  // ------------------------------------------------------------ several budgets

  /**
   * Claims every listed budget, or none of them.
   *
   * Optional so a backend written against an earlier version still satisfies the
   * interface. A client declaring more than one limit is refused at
   * construction — `ConfigurationError` `backend_missing_multi_limit` — by a
   * backend lacking this or `releaseMultiLimit`, rather than silently metering
   * against one budget. All-or-nothing is the whole contract: a partial claim
   * leaks budget that no completion path hands back.
   *
   * An unusable configuration is reported through `error` rather than thrown,
   * as {@link acquireTokens} does.
   */
  acquireMultiLimit?(specs: MultiLimitSpec[]): Promise<AcquireMultiLimitResult>;
  /**
   * Hands back what `acquireMultiLimit` claimed for a request that was never
   * dispatched. Token buckets are credited, up to each spec's
   * `config.maxTokens`, which must be a finite number above 0; concurrency
   * slots are released.
   *
   * A spec's own `freezeKey` suppresses its refund while a freeze that emptied
   * the bucket stands, matching {@link DianemoBackend.refundTokens}, as does a
   * cost that is negative or not finite. Slots are released regardless.
   */
  releaseMultiLimit?(specs: MultiLimitSpec[]): Promise<void>;
  /**
   * The no-queue fast path for several budgets: admit only if nothing is queued,
   * none of `freezeKeys` stands or owes thaw probes, and every budget can be
   * claimed at once. `ttl` is the key lifetime in seconds, default 86400,
   * normalised by `normalizeStateTtlSeconds`: one that is not finite and above
   * 0 throws a `RangeError` before anything is claimed.
   *
   * Optional; without it a client with several limits always queues.
   */
  tryAdmitMultiLimit?(
    queueKey: string,
    freezeKeys: string[],
    specs: MultiLimitSpec[],
    ttl?: number
  ): Promise<boolean>;

  // ------------------------------------------------------------- concurrency

  /**
   * Claims a slot of `cost` under `requestId` if the cost in flight leaves
   * room for it under `maxConcurrency`.
   *
   * Slots older than `config.requestTtl` are reaped first. A slot `requestId`
   * already holds is not counted against it, and is replaced by the new claim.
   * Ignores the queue and freezes; those are the caller's to check. An
   * unusable `config` or `cost` is reported through `error` rather than thrown.
   */
  acquireConcurrency(
    key: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult>;
  /**
   * Claims capacity only while the selected queue entry still exists in the
   * in-progress state. The check and claim must be atomic.
   */
  acquireQueuedConcurrency?(
    key: string,
    metadataKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult>;
  /** Frees `requestId`'s slot. Idempotent: an absent slot is a no-op. */
  releaseConcurrency(key: string, requestId: string): Promise<void>;
  /**
   * The slots claimed within the last `requestTtl` ms, oldest first, and their
   * total cost, without reaping anything.
   *
   * @throws {RangeError} for a `requestTtl` that is not finite, rather than
   * reporting a busy ledger as idle.
   */
  getConcurrencyState(
    key: string,
    requestTtl: number
  ): Promise<{ currentConcurrency: number; activeRequests: string[] }>;
  /**
   * Drops every slot in the ledger, peers' included, so the cap stops holding
   * until their requests finish. Not for use while other replicas are running.
   */
  clearConcurrency(key: string): Promise<void>;

  // ------------------------------------------------------------------- queue

  /**
   * Returns false when `requestId` is already queued, so retries are safe, and false
   * when it has been removed recently — see {@link removeRequest}.
   *
   * That second refusal is what makes the ordering safe without trusting the
   * transport. An abandonment broadcast can be delivered before an outstanding add
   * commits, and the removal it triggers then finds nothing to remove; without the
   * refusal this call would go on to create an entry nobody awaits, which orphan
   * cleanup spares (its owner is alive) and which pins the queue non-empty, disabling
   * the fast path for the whole client.
   *
   * A retry is unaffected: it keeps its entry rather than removing and re-adding, so
   * a removal and a later add of one id never both happen.
   *
   * @param ttl Lifetime in seconds of the queue key and the entry's metadata,
   * default 86400. Normalised by `normalizeStateTtlSeconds`, so zero or below
   * throws rather than expiring a queue other callers share.
   * @throws {RangeError} before anything is written, for an unusable `ttl` or
   * a request `calculateQueueScore` cannot score.
   */
  addRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    request: QueuedRequest,
    ttl?: number
  ): Promise<boolean>;
  /**
   * Claims the lowest-scored `pending` entry and returns it, flipping its
   * status to `inProgress` and stamping `claimedAt` in the same step so no
   * other caller is handed it. `null` when nothing eligible is queued.
   *
   * A claim is a lease: an `inProgress` entry whose `claimedAt` is
   * `QUEUE_CLAIM_LEASE_MS` old is eligible again, and is handed out with its
   * thaw-probe mark cleared, so a claimer that never answers — its reply lost,
   * its process gone — does not strand the entry. An entry missing `requestId`,
   * `clientName` or `status`, or with no `requestName` field at all, is passed
   * over rather than claimed, since it could not be returned.
   *
   * @param skipGrantIds Grants whose entries to pass over, such as frozen
   * ones. An entry with no grant is never skipped by this.
   */
  getNextRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    skipGrantIds?: string[],
    /**
     * Request ids to pass over. Lets the admission loop step past a head it has
     * already judged unsatisfiable and reach the work behind it, instead of
     * being handed the same entry every iteration.
     */
    skipRequestIds?: string[]
  ): Promise<QueuedRequest | null>;
  /**
   * Records that the claim {@link getNextRequest} stamped with `claimedAt` led
   * to admission, so the entry is never handed out again. True only while the
   * entry is still `inProgress` under that same claim; false when it is gone or
   * its lease lapsed and another caller claimed it, in which case the caller
   * must not dispatch it.
   */
  confirmRequest(
    metadataKeyPrefix: string,
    requestId: string,
    claimedAt: number
  ): Promise<boolean>;
  /**
   * The entry's stored metadata, or `null` when it is gone or lacks a required
   * field. Reads only; the queue order is not consulted.
   */
  getRequest(
    metadataKeyPrefix: string,
    requestId: string
  ): Promise<QueuedRequest | null>;
  /**
   * Updates `status`, `retries`, `priority` and `isThawRequest` on an existing
   * entry; other fields in `updates` are ignored, as is a non-finite number. A
   * `status` written here ends any claim, clearing `claimedAt`. A
   * change to `retries` or `priority` re-scores the entry in the same step,
   * clamping rather than throwing where `calculateQueueScore` would. A no-op
   * for an entry that is gone.
   */
  updateRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    updates: Partial<QueuedRequest>
  ): Promise<void>;
  /**
   * Removes the entry and marks the id as removed for `tombstoneTtlSeconds`, so a
   * later {@link addRequest} for it is refused.
   *
   * The mark is written even when there was no entry to remove — that is the case it
   * exists for.
   *
   * @param tombstoneTtlSeconds How long the mark stands, defaulting to
   * `REQUEST_TOMBSTONE_TTL_SECONDS`. It has to outlast the window in which an add
   * can still be outstanding, which is the caller's admission budget rather than a
   * constant, so a caller that lets a request wait longer must pass a longer value.
   * Never refused: a fraction rounds up, a value with no lifetime (`NaN`, zero,
   * negative) takes the default, and `Infinity` the longest TTL a key can hold.
   */
  removeRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    tombstoneTtlSeconds?: number
  ): Promise<{ wasThawRequest: boolean }>;
  /**
   * Counts the queued entries by status and sums their cost, reading each
   * one's metadata; an entry with no stored status is not counted, and an
   * unusable stored cost counts as 1.
   */
  getQueueStats(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueueStats>;
  /**
   * Depth without reading any metadata, unlike `getQueueStats`. For host
   * observability: the fast paths ask the same question inside their own scripts.
   */
  getQueueLength(queueKey: string): Promise<number>;
  /**
   * Every queued entry in score order, as {@link DianemoBackend.getRequest}
   * reads each; one whose metadata is gone or incomplete is left out.
   */
  getAllRequests(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueuedRequest[]>;
  /**
   * Drops requests owned by instances that are no longer alive, along with
   * queue members whose metadata has already expired, and returns how many
   * entries it removed.
   *
   * Refused outright unless `aliveInstanceIds` contains `currentInstanceId`: an
   * instance registers itself on every heartbeat, so a set lacking the caller's
   * own id proves the read is not authoritative rather than proving everyone
   * died, and sweeping on it deletes live replicas' queued work. Why this
   * resolves the opposite way to absent state elsewhere:
   * docs/design-notes.md#an-alive-instance-set-that-omits-the-sweeper-is-not-evidence.
   */
  cleanupOrphanedRequests(
    queueKey: string,
    metadataKeyPrefix: string,
    aliveInstanceIds: Set<string>,
    currentInstanceId: string
  ): Promise<number>;

  // ------------------------------------------------------------ freeze / thaw

  /**
   * Writes the freeze state to exactly these values.
   *
   * The wrong operation for arming a freeze — see {@link armFreeze}. It exists
   * for callers that mean "the state is now this", such as a fixture moving a
   * freeze from live to lapsed. `options.emptiesBucket` is written as given,
   * defaulting to `true`, with no merge.
   */
  setFreezeState(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    options?: { emptiesBucket?: boolean }
  ): Promise<void>;
  /**
   * Arms a freeze, keeping the longer deadline and larger probe budget, and returns
   * whichever is now in force — schedule wake-ups against that, not the requested
   * value.
   *
   * Must be atomic: a read-then-write lets two concurrent arms read the same old
   * value and the loser still win. Aliasing this to `setFreezeState` is wrong in a
   * way no behaviour test will show; run the conformance suite.
   *
   * `options.emptiesBucket` records whether this freeze emptied the budget it
   * governs, which is what makes {@link refundTokens} refuse under it. Pass
   * `false` for a freeze that leaves the bucket alone — a 5xx or a dropped
   * connection — so a refund under it is not lost. Defaults to `true`, the
   * reading every freeze had before the option existed. The merge keeps `true`
   * if either side has it, but only from a stored freeze that still stands: a
   * lapsed one's bucket has been refilling since.
   *
   * A non-finite `frozenUntil` or `thawRequestCount` is read as 0 before the
   * merge, so `Infinity` arms no window and owes no probes. After the merge
   * both values are truncated toward zero and clamped — `frozenUntil` to
   * ±(2^53-1), `thawRequestCount` to 0 through 2^53-1 — so the state returned
   * is exactly the state stored.
   */
  armFreeze(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    options?: { emptiesBucket?: boolean }
  ): Promise<FreezeState>;
  /**
   * The freeze in force: inside its window, or after it while probes are
   * still owed. `null` when there is no usable stored deadline, or when the
   * window has lapsed with no probes owed. Reads only; a lapsed freeze is
   * left for {@link DianemoBackend.updateThawProgress} to delete.
   */
  getFreezeState(key: string): Promise<FreezeState | null>;
  /**
   * Records a probe's outcome against the freeze at `key` and returns the
   * state now in force, or `null` once none is.
   *
   * A success takes one off the probes owed. The freeze is deleted only once
   * no probes are owed and its window has lapsed, so a window re-armed while
   * the probe was in flight still stands. A failure changes nothing; the
   * caller re-arms. A freeze found lapsed with nothing owed is deleted either
   * way.
   *
   * @param completionId Makes the call idempotent: a repeat of an id already
   * recorded returns the current state and changes nothing. Recorded even when
   * no freeze stands, so a redelivery cannot count against one armed later.
   */
  updateThawProgress(
    key: string,
    success: boolean,
    completionId?: string
  ): Promise<FreezeState | null>;
  /** Deletes the freeze at `key`, window and probes owed alike. */
  clearFreezeState(key: string): Promise<void>;
  /**
   * Whether `key` holds a freeze in force, as
   * {@link DianemoBackend.getFreezeState} reads it: inside its window, or
   * owing probes.
   */
  isFrozen(key: string): Promise<boolean>;
  /**
   * Whether the next request may go, and whether it would be the thaw probe.
   *
   * `frozenUntil` is reported whenever freeze state exists, on the BACKEND's
   * clock, so a caller declining a request can book its retry against the
   * deadline the fleet honours instead of subtracting a local clock from a
   * remote timestamp. Absent only when there is no freeze state at all.
   */
  canProcessRequest(key: string): Promise<{
    canProcess: boolean;
    isThawRequest: boolean;
    frozenUntil?: number;
  }>;
  /**
   * Whether an in-progress thaw probe exists for `grantId` — or, with
   * `options.anyGrant`, for any grant at all.
   */
  hasThawRequestInProgress(
    queueKey: string,
    metadataKeyPrefix: string,
    grantId: string,
    options?: { anyGrant?: boolean }
  ): Promise<boolean>;
  /**
   * Nominates exactly one probe request per grant while frozen.
   *
   * Returns `"started"` to the single winner and `"exists"` to everyone else.
   * A backend that lets two callers win turns a recovering vendor back into a
   * thundering herd, so this has to be genuinely exclusive.
   *
   * With `options.anyGrant`, any other in-progress probe refuses the claim
   * whatever its `grantId`: one probe client-wide, for a client whose grants
   * all share one freeze key. `grantId` is still recorded in the frozen-grants
   * set as without it.
   */
  tryStartThawRequest(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    grantId: string,
    options?: { anyGrant?: boolean }
  ): Promise<"started" | "exists">;
  /**
   * Removes from the frozen-grants set every grant with no in-progress probe
   * whose freeze window has lapsed or whose freeze state is gone, and returns
   * how many it removed. The freeze state itself is not touched.
   *
   * @param freezeStateKeyPrefix A grant's freeze key is this, then the grant
   * id, then `:freezeState`.
   */
  cleanupStaleFrozenGrants(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    freezeStateKeyPrefix: string
  ): Promise<number>;

  // --------------------------------------------------------------- instances

  /**
   * The live instances registered in the set at `instanceSetKey`, each with
   * the data stored under `instanceKeyPrefix` followed by its id. Excludes
   * `currentInstanceId`, and prunes from the set any id whose data is gone or
   * empty — an instance that stopped heartbeating.
   */
  getInstances(
    instanceSetKey: string,
    instanceKeyPrefix: string,
    currentInstanceId: string
  ): Promise<Array<{ id: string; data: string }>>;

  /**
   * The clock the backend's own atomic operations are measured against.
   *
   * Anything written into shared state as a timestamp — `lastUpdate`,
   * `frozenUntil`, a concurrency slot's score — has to come from here, because
   * every replica compares against this clock and not its own.
   */
  now(): Promise<number>;

  // --------------------------------------------------------------- lifecycle

  /**
   * Releases whatever the backend itself opened — timers, a pub/sub
   * connection — and drops every subscriber. Stored data is kept: a handler
   * started again on the same backend, or the same store, finds it. It must
   * not close a connection handed in by the caller — that belongs to them.
   */
  close(): Promise<void>;
}
