/** A token bucket as the backend meters it: a `requestLimit` without its name. */
export interface TokenBucketConfig {
  /** Bucket ceiling; a bucket with no stored state reads as this full. */
  maxTokens: number;
  /** Tokens credited per whole `interval` elapsed. */
  tokensToAdd: number;
  /** ms between token additions */
  interval: number;
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
   * ms to wait before retrying, when not acquired. `Infinity` when no number of
   * refills can cover the cost, such as a `tokensToAdd` too small to add up.
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
} & (
  | { kind: "tokenBucket"; key: string; config: TokenBucketConfig }
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
   * caused only by one carries `0`. `Infinity` when a bucket can never cover
   * the cost, as in {@link AcquireTokensResult.waitTime}. Absent when acquired
   * or on `error`.
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
  requestId: string;
  clientName: string;
  requestName: string;
  status: "pending" | "inProgress";
  priority: number;
  cost: number;
  retries: number;
  timestamp: number;
  grantId?: string;
  isThawRequest: boolean;
  ownerId: string;
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
  totalCost: number;
}

/** A single write in a {@link DianemoBackend.batch} group. */
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
  set(key: string, value: string, ttlSeconds?: number): Promise<void>;
  del(...keys: string[]): Promise<void>;
  hgetall(key: string): Promise<Record<string, string>>;
  hset(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ): Promise<void>;
  sadd(key: string, member: string): Promise<void>;
  srem(key: string, member: string): Promise<void>;
  smembers(key: string): Promise<string[]>;

  /** Applies several writes together — one round-trip where that is a concept. */
  batch(ops: BatchOp[], options?: BatchOptions): Promise<void>;

  // ---------------------------------------------------------------- locks

  /**
   * Takes `key` for `ttlMs` if it is free. `token` identifies the holder so
   * that only the holder can release it.
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
   * can never overtake work that has been waiting.
   */
  tryAdmitImmediately(
    queueKey: string,
    bucketKey: string,
    freezeKey: string,
    cost: number,
    config: TokenBucketConfig,
    ttl?: number
  ): Promise<boolean>;

  /**
   * The same decision for a client with no budget: admit unless something is queued
   * or a freeze or probe stands. One call, because answering the two separately lets
   * a peer enqueue in between and be overtaken.
   */
  tryAdmitNoLimit(queueKey: string, freezeKey: string): Promise<boolean>;

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

  acquireTokens(
    key: string,
    cost: number,
    config: TokenBucketConfig
  ): Promise<AcquireTokensResult>;
  getTokenBucketState(
    key: string,
    config: TokenBucketConfig
  ): Promise<{ tokens: number; lastUpdate: number }>;
  resetTokenBucket(key: string, maxTokens: number): Promise<void>;
  /**
   * Returns budget claimed for a request that was never dispatched.
   *
   * Refused while `freezeKey` holds a standing freeze that emptied the bucket —
   * one armed without `emptiesBucket: false`, see {@link armFreeze}. A freeze
   * that left the bucket alone refuses nothing, since the token was never taken
   * from the budget the freeze is protecting.
   *
   * A cost that is negative or not finite credits nothing, as
   * {@link acquireTokens} refuses to spend one.
   */
  refundTokens?(
    key: string,
    cost: number,
    maxTokens: number,
    freezeKey?: string
  ): Promise<void>;

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
  acquireMultiLimit?(
    specs: MultiLimitSpec[],
    cost: number
  ): Promise<AcquireMultiLimitResult>;
  /**
   * Hands back what `acquireMultiLimit` claimed for a request that was never
   * dispatched. Token buckets are credited and concurrency slots released.
   *
   * A spec's own `freezeKey` suppresses its refund while a freeze that emptied
   * the bucket stands, matching {@link refundTokens}, as does a cost that is
   * negative or not finite. Slots are released regardless.
   */
  releaseMultiLimit?(specs: MultiLimitSpec[], cost: number): Promise<void>;
  /**
   * The no-queue fast path for several budgets: admit only if nothing is queued,
   * none of `freezeKeys` stands or owes thaw probes, and every budget can be
   * claimed at once. `ttl` is the key lifetime in seconds, default 86400.
   *
   * Optional; without it a client with several limits always queues.
   */
  tryAdmitMultiLimit?(
    queueKey: string,
    freezeKeys: string[],
    specs: MultiLimitSpec[],
    cost: number,
    ttl?: number
  ): Promise<boolean>;

  // ------------------------------------------------------------- concurrency

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
  releaseConcurrency(key: string, requestId: string): Promise<void>;
  getConcurrencyState(
    key: string,
    requestTtl: number
  ): Promise<{ currentConcurrency: number; activeRequests: string[] }>;
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
   */
  addRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    request: QueuedRequest,
    ttl?: number
  ): Promise<boolean>;
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
  getRequest(
    metadataKeyPrefix: string,
    requestId: string
  ): Promise<QueuedRequest | null>;
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
  getQueueStats(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueueStats>;
  /**
   * Depth without reading any metadata, unlike `getQueueStats`. For host
   * observability: the fast paths ask the same question inside their own scripts.
   */
  getQueueLength(queueKey: string): Promise<number>;
  getAllRequests(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueuedRequest[]>;
  /**
   * Drops requests owned by instances that are no longer alive.
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
   * After the merge both values are truncated toward zero and clamped —
   * `frozenUntil` to ±(2^53-1), `thawRequestCount` to 0 through 2^53-1 — so
   * the state returned is exactly the state stored.
   */
  armFreeze(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    options?: { emptiesBucket?: boolean }
  ): Promise<FreezeState>;
  getFreezeState(key: string): Promise<FreezeState | null>;
  updateThawProgress(
    key: string,
    success: boolean,
    completionId?: string
  ): Promise<FreezeState | null>;
  clearFreezeState(key: string): Promise<void>;
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
  cleanupStaleFrozenGrants(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    freezeStateKeyPrefix: string
  ): Promise<number>;

  // --------------------------------------------------------------- instances

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
   * Releases whatever the backend itself opened. It must not close a
   * connection handed in by the caller — that belongs to them.
   */
  close(): Promise<void>;
}
