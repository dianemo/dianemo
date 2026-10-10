import { claimMarkKey, settleAmount } from "./settlement.js";
import { EventEmitter } from "node:events";
import {
  formatStoredCost,
  parseFiniteStored,
  parsePriority,
  parseStoredCost,
  parseStoredNumber,
} from "./parseStored.js";
import {
  calculateQueueScore,
  MAX_QUEUE_PRIORITY,
  MAX_QUEUE_RETRIES,
  QUEUE_PRIORITY_BAND,
  QUEUE_RETRY_BAND,
  QUEUE_SCORE_EPOCH_MS,
} from "./queueScore.js";
import type {
  AcquireConcurrencyResult,
  AcquireMultiLimitResult,
  AcquireTokensResult,
  BatchOp,
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
import {
  normalizeTombstoneTtl,
  normalizeTtlSeconds,
  REDIS_MAX_TTL_SECONDS,
  REQUEST_TOMBSTONE_TTL_SECONDS,
} from "./ttl.js";

/**
 * A bucket as `refill` leaves it. `overflow` and `fills` only ever grow: they
 * are what settlement compares against a claim mark to tell how much of a claim
 * the bucket has already forgotten. Twin of the Lua `lazyRefill`.
 */
interface BucketState {
  tokens: number;
  lastUpdate: number;
  /** Refill discarded at the ceiling, in total. */
  overflow: number;
  /** How many times the balance has reached the ceiling. */
  fills: number;
}

/** A bucket's refilled balance, held until every budget in the claim admits. */
interface PlannedBucket extends BucketState {
  key: string;
  cost: number;
  config: TokenBucketConfig;
  mark?: ClaimMark;
}

interface PlannedSlot {
  key: string;
  slotId: string;
  cost: number;
}

interface Entry {
  expiresAt?: number;
  hash?: Map<string, string>;
  set?: Set<string>;
  zset?: Map<string, number>;
  str?: string;
}

/**
 * Whether a concurrency ceiling can admit anything. `NaN` compares false against
 * everything, so an unguarded `used + cost > max` reads as "under the cap".
 */
function usableConcurrency(maxConcurrency: number): boolean {
  return Number.isFinite(maxConcurrency) && maxConcurrency > 0;
}

/**
 * Whether a cost can be measured against a ceiling.
 *
 * `NaN` slips through the same comparison `usableConcurrency` guards. A negative
 * one is worse than unenforced: it lowers the occupancy every later request is
 * measured against, so one parked slot raises the ceiling for everyone. Twin of
 * the concurrency Lua's `usableCost`.
 */
function usableCost(cost: number): boolean {
  return Number.isFinite(cost) && cost >= 0;
}

/**
 * Whether a slot's lease can be measured. A `NaN` cutoff loses every comparison,
 * so a slot claimed under one is never reaped. Twin of the concurrency Lua's
 * `usableRequestTtl`.
 */
function usableRequestTtl(requestTtl: number): boolean {
  return Number.isFinite(requestTtl) && requestTtl > 0;
}

/**
 * Marks an id as removed, so a later add for it is refused rather than creating an
 * entry nobody awaits. Same layout as the Redis backend's.
 */
function tombstoneKey(metadataKeyPrefix: string, requestId: string): string {
  return `${metadataKeyPrefix}:${requestId}:removed`;
}

/**
 * The score `updateRequest` re-writes, computed as the Lua re-score computes it:
 * the same clamps and the same float arithmetic, and no integer check.
 *
 * Not `calculateQueueScore`, which raises for a score that is not a safe
 * integer or an arrival past the retry band. That is right for an add and
 * wrong here, where the fields come back from shared state and the raise would
 * fail the retry being re-scored. Clamping the arrival keeps the entry inside
 * its own band, which is the property ordering depends on.
 */
function rescore(priority: number, retries: number, timestamp: number) {
  const p = Math.min(Math.max(priority, 0), MAX_QUEUE_PRIORITY);
  const r = Math.min(Math.max(retries, 0), MAX_QUEUE_RETRIES);
  const arrival = Math.min(
    Math.max(timestamp - QUEUE_SCORE_EPOCH_MS, 0),
    QUEUE_RETRY_BAND - 1
  );
  return (
    (MAX_QUEUE_PRIORITY - p) * QUEUE_PRIORITY_BAND +
    (MAX_QUEUE_RETRIES - r) * QUEUE_RETRY_BAND +
    arrival
  );
}

/**
 * A stored freeze deadline, or `undefined` when the field cannot serve as one.
 *
 * An unparseable or non-finite value counts as no freeze state at all, matching the
 * Lua's `nil` test: it otherwise survived every comparison and reported a client
 * frozen with no deadline a wake-up could be booked against.
 */
function usableDeadline(raw: string | undefined): number | undefined {
  // Through the shared parse, so an empty field reads as unusable rather than as 0.
  const parsed = parseStoredNumber(raw, NaN);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * A freeze field as both backends store and return it exactly: truncated toward
 * zero, as Lua 5.1's `string.format('%d')` truncates, and clamped to
 * `[min, 2^53-1]`. Twin of the Lua `storable` in `lua/freeze.ts`.
 */
function storable(value: number, min: number): number {
  return Math.min(Math.max(Math.trunc(value), min), Number.MAX_SAFE_INTEGER);
}

/**
 * A bucket key's lifetime in seconds: `ttlSeconds`, or longer for a bucket that
 * takes longer than that to refill from empty. An expired bucket reads as full,
 * so a key outliving only part of the refill hands a spent per-week budget back
 * after a day of quiet. Twin of the Lua `bucketTtl` in `fragments.ts`.
 *
 * Clamped to the longest TTL a backend can store, so a valid configuration whose
 * refill takes longer than that still admits. `!(refill > ttlSeconds)` rather
 * than `Math.max`, which returns `NaN` for the `Infinity * 0` of a zero interval
 * where Lua's `math.max` returns the other argument.
 */
function bucketTtl(ttlSeconds: number, config: TokenBucketConfig): number {
  const refill = Math.ceil(
    (Math.ceil(config.maxTokens / config.tokensToAdd) * config.interval) / 1000
  );
  if (!(refill > ttlSeconds)) return ttlSeconds;
  return Math.min(refill, REDIS_MAX_TTL_SECONDS);
}

/**
 * Why this budget cannot admit anything, or `undefined` if it can.
 *
 * Reported through `AcquireTokensResult.error` rather than thrown, so an unusable
 * configuration stays distinguishable from a merely empty bucket. Mirrors the guards
 * at the top of the `tokenBucket` Lua.
 */
function unusableBudget(
  cost: number,
  config: TokenBucketConfig
): string | undefined {
  if (!(config.tokensToAdd > 0)) return "tokensToAdd must be greater than 0";
  if (!Number.isFinite(config.maxTokens) || config.maxTokens <= 0) {
    return "maxTokens must be a finite number greater than 0";
  }
  if (!usableCost(cost)) {
    return "cost must be a finite number that is not negative";
  }
  // Guarded even though `refill` tolerates it — its `interval > 0` test is false
  // for a NaN, so the bucket simply never refills. The waiting caller is what
  // breaks: `waitTime` is derived from `interval` and clears the `>= 0` clamp.
  if (!Number.isFinite(config.interval)) {
    return "interval must be a finite number";
  }
  return undefined;
}

/**
 * In-process coordination, for a deployment that is exactly one process.
 *
 * **Every process running this gets its own private copy of every limit**, so two
 * processes send twice the agreed rate and nothing reports it. See
 * docs/backends/memory.md before choosing it.
 *
 * Every operation here is synchronous, which is what makes it atomic — a function
 * body runs to completion before anything else does — so there is no locking below.
 */
class MemoryBackend implements DianemoBackend {
  readonly kind = "memory";
  readonly distributed = false;

  private store = new Map<string, Entry>();
  private bus = new EventEmitter();
  /**
   * A Set, not one handler per channel: these channels are a documented extension
   * point, so a host observer must not displace the client's own subscription.
   */
  private subscribed = new Map<string, Set<MessageHandler>>();
  private busListening = false;
  private sweeper: NodeJS.Timeout | null = null;

  constructor() {
    this.bus.setMaxListeners(0);
    this.armSweeper();
  }

  // ------------------------------------------------------------- primitives

  /**
   * Starts the periodic sweep if nothing is sweeping yet.
   *
   * Reads expire lazily, which is enough for correctness. This only stops keys
   * that are never read again from pinning memory forever — so a backend still
   * written to after {@link close} needs it back rather than a permanent stop.
   */
  private armSweeper() {
    if (this.sweeper) return;
    this.sweeper = setInterval(() => this.sweep(), 60_000);
    this.sweeper.unref?.();
  }

  /** Every key creation funnels through here, so any write re-arms the sweep. */
  private put(key: string, entry: Entry) {
    this.armSweeper();
    this.store.set(key, entry);
  }

  private sweep() {
    const now = Date.now();
    for (const [key, entry] of this.store) {
      if (entry.expiresAt !== undefined && now >= entry.expiresAt) {
        this.store.delete(key);
      }
    }
  }

  /**
   * Returns the live entry for `key`, dropping it first if it has expired.
   * `now` is for a decision that has already sampled the clock once.
   */
  private live(key: string, now = Date.now()): Entry | undefined {
    const entry = this.store.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== undefined && now >= entry.expiresAt) {
      this.store.delete(key);
      return undefined;
    }
    return entry;
  }

  private entry(key: string): Entry {
    const existing = this.live(key);
    if (existing) return existing;
    const created: Entry = {};
    this.put(key, created);
    return created;
  }

  private hash(key: string): Map<string, string> {
    const entry = this.entry(key);
    entry.hash ??= new Map();
    return entry.hash;
  }

  private zset(key: string): Map<string, number> {
    const entry = this.entry(key);
    entry.zset ??= new Map();
    return entry.zset;
  }

  private setOf(key: string): Set<string> {
    const entry = this.entry(key);
    entry.set ??= new Set();
    return entry.set;
  }

  private expire(key: string, ttlSeconds: number) {
    // Normalised through the same helper the Redis backend uses, so a TTL
    // either backend refuses throws here too.
    const ttl = normalizeTtlSeconds(ttlSeconds);
    const entry = this.live(key);
    if (!entry) return;
    if (ttl <= 0) {
      this.store.delete(key);
      return;
    }
    entry.expiresAt = Date.now() + ttl * 1000;
  }

  /** Members of a sorted set, ordered by score then lexically, as Redis does. */
  private sorted(key: string): string[] {
    const z = this.live(key)?.zset;
    if (!z) return [];
    return [...z.entries()]
      .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([member]) => member);
  }

  private hget(
    key: string,
    field: string,
    now = Date.now()
  ): string | undefined {
    return this.live(key, now)?.hash?.get(field);
  }

  /**
   * Drops `key` once its set, sorted set or hash has no members left, as Redis
   * does — so an emptied key neither answers an existence check nor keeps its
   * old TTL for whatever is added to it next.
   */
  private dropIfEmpty(key: string) {
    const entry = this.store.get(key);
    if (!entry) return;
    const size = entry.set?.size ?? entry.zset?.size ?? entry.hash?.size;
    if (size === 0) this.store.delete(key);
  }

  private removeMember(key: string, member: string) {
    const set = this.live(key)?.set;
    if (!set) return;
    set.delete(member);
    this.dropIfEmpty(key);
  }

  /**
   * Two states block the fast path: inside the freeze window, and afterwards while
   * probes are still owed. Only the queue path can arrange one-at-a-time probing,
   * so a fast-path admission would resume at full rate and never decrement the count.
   */
  private freezeBlocksAdmission(freezeKey: string, now: number): boolean {
    const raw = this.hget(freezeKey, "frozenUntil");
    if (raw === undefined) return false;
    const frozenUntil = usableDeadline(raw);
    if (frozenUntil === undefined) return false;
    if (now < frozenUntil) return true;
    return parseFiniteStored(this.hget(freezeKey, "thawRequestCount"), 0) > 0;
  }

  /**
   * Whether a refund must be refused: a freeze stands and emptied the bucket.
   * An absent `emptiesBucket` field reads as emptying, as every freeze did
   * before the field existed. Twin of the refund gate in `lua/tokenBucket.ts`.
   */
  private freezeEmptiedBucket(freezeKey: string, now: number): boolean {
    const frozenUntil = parseFiniteStored(
      this.hget(freezeKey, "frozenUntil"),
      0
    );
    return frozenUntil > now && this.hget(freezeKey, "emptiesBucket") !== "0";
  }

  // ------------------------------------------------------------------ store

  async get(key: string): Promise<string | null> {
    return this.live(key)?.str ?? null;
  }

  /**
   * The body of {@link set}, synchronous so that {@link batch} can apply a group
   * without suspending. Every write op has one of these: `await` yields to the
   * microtask queue, so awaiting a sibling method mid-group is what let a reader
   * resume between two ops and observe the group half-applied.
   */
  private writeString(key: string, value: string, ttlSeconds?: number) {
    // Read before the write, so a TTL this backend cannot store leaves the key
    // untouched rather than set with a lifetime nobody chose.
    const ttl =
      ttlSeconds === undefined ? undefined : normalizeTtlSeconds(ttlSeconds);
    if (ttl !== undefined && ttl <= 0) {
      this.store.delete(key);
      return;
    }
    // A plain SET replaces whatever was there, type included.
    const entry: Entry = { str: value };
    if (ttl !== undefined) entry.expiresAt = Date.now() + ttl * 1000;
    this.put(key, entry);
  }

  /** The body of {@link hset}, synchronous for the reason {@link writeString} carries. */
  private writeHash(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ) {
    // Validated first: `expire` would otherwise raise with the fields already set.
    const ttl =
      ttlSeconds === undefined ? undefined : normalizeTtlSeconds(ttlSeconds);
    // No fields creates no key, as the `HSET` the Redis twin skips does not; the
    // expiry still moves an existing hash's TTL there, and so here.
    const entries = Object.entries(fields);
    if (entries.length > 0) {
      const hash = this.hash(key);
      for (const [field, value] of entries) hash.set(field, String(value));
    }
    if (ttl !== undefined) this.expire(key, ttl);
  }

  async set(key: string, value: string, ttlSeconds?: number): Promise<void> {
    this.writeString(key, value, ttlSeconds);
  }

  async del(...keys: string[]): Promise<void> {
    for (const key of keys) this.store.delete(key);
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    const hash = this.live(key)?.hash;
    if (!hash) return {};
    return Object.fromEntries(hash);
  }

  async hset(
    key: string,
    fields: Record<string, string | number>,
    ttlSeconds?: number
  ): Promise<void> {
    this.writeHash(key, fields, ttlSeconds);
  }

  async sadd(key: string, member: string): Promise<void> {
    this.setOf(key).add(member);
  }

  async srem(key: string, member: string): Promise<void> {
    this.removeMember(key, member);
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.live(key)?.set ?? [])];
  }

  async batch(ops: BatchOp[]): Promise<void> {
    // Every TTL read before the first write, matching the Redis backend: otherwise
    // an unusable one raises partway and leaves the earlier ops applied.
    for (const op of ops) {
      if ("ttlSeconds" in op && op.ttlSeconds !== undefined) {
        normalizeTtlSeconds(op.ttlSeconds);
      }
    }
    // Every op body below is synchronous, and must stay that way: nothing can
    // interleave with a synchronous loop, which is the whole of this backend's
    // atomicity and so the whole of what `atomic` asks for. Calling the `async`
    // siblings here instead — `await this.set(...)` — reintroduces the tear, because
    // `await` yields to the microtask queue between two ops even when the promise is
    // already settled.
    for (const op of ops) {
      switch (op.op) {
        case "set":
          this.writeString(op.key, op.value, op.ttlSeconds);
          break;
        case "del":
          this.store.delete(op.key);
          break;
        case "expire":
          this.expire(op.key, op.ttlSeconds);
          break;
        case "sadd":
          this.setOf(op.key).add(op.member);
          break;
        case "srem":
          this.removeMember(op.key, op.member);
          break;
        case "hset":
          this.writeHash(op.key, op.fields, op.ttlSeconds);
          break;
        case "publish":
          this.emit(op.channel, op.message);
          break;
      }
    }
  }

  // ------------------------------------------------------------------ locks

  async acquireLock(
    key: string,
    token: string,
    ttlMs: number
  ): Promise<boolean> {
    // The one TTL argument that does not reach `normalizeTtlSeconds`, so it is
    // checked here. Redis refuses the same values from `SET ... PX`: "invalid
    // expire time in 'set' command" for a non-positive one, "value is not an
    // integer or out of range" for a non-finite one.
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new RangeError(
        `ttlMs must be a finite number greater than 0, received ${String(ttlMs)}`
      );
    }
    if (this.live(key)) return false;
    this.put(key, { str: token, expiresAt: Date.now() + ttlMs });
    return true;
  }

  async releaseLock(key: string, token: string): Promise<boolean> {
    if (this.live(key)?.str !== token) return false;
    this.store.delete(key);
    return true;
  }

  // ---------------------------------------------------------------- pub/sub

  private emit(channel: string, message: string) {
    // Redis delivers to subscribers asynchronously, and callers here rely on
    // publish() returning before the handler runs — a synchronous emit would
    // re-enter the publisher mid-operation.
    queueMicrotask(() => this.bus.emit("message", channel, message));
  }

  async publish(channel: string, message: string): Promise<void> {
    this.emit(channel, message);
  }

  async subscribe(channels: string[], handler: MessageHandler): Promise<void> {
    for (const channel of channels) {
      let handlers = this.subscribed.get(channel);
      if (!handlers) {
        handlers = new Set();
        this.subscribed.set(channel, handlers);
      }
      handlers.add(handler);
    }
    // One bus listener for the lifetime of the backend, rather than one per
    // subscribe call — repeated subscription would otherwise grow the emitter
    // without bound and trip Node's max-listeners warning.
    if (this.busListening) return;
    this.busListening = true;
    this.bus.on("message", (channel: string, message: string) => {
      const handlers = this.subscribed.get(channel);
      if (!handlers) return;
      // Copied, so a handler that subscribes or unsubscribes while dispatching
      // cannot mutate the set being iterated.
      for (const h of [...handlers]) h(channel, message);
    });
  }

  // ------------------------------------------------------------ token bucket

  /**
   * Refills lazily, exactly as the Lua does: whole intervals only, so
   * `lastUpdate` advances in interval-sized steps rather than to `now`.
   */
  private refill(
    key: string,
    config: TokenBucketConfig,
    now: number
  ): BucketState {
    // Through the shared parse, not `Number`: an empty, unparseable or
    // non-finite field reads as absent, as the Lua's `finiteNumber` reads it.
    let tokens = parseFiniteStored(this.hget(key, "tokens"), config.maxTokens);
    let lastUpdate = parseFiniteStored(this.hget(key, "lastUpdate"), now);
    let overflow = parseFiniteStored(this.hget(key, "overflow"), 0);
    let fills = parseFiniteStored(this.hget(key, "fills"), 0);
    const belowCap = tokens < config.maxTokens;

    const elapsed = now - lastUpdate;
    if (elapsed > 0 && config.interval > 0) {
      const intervalsElapsed = Math.floor(elapsed / config.interval);
      if (intervalsElapsed > 0) {
        tokens += intervalsElapsed * config.tokensToAdd;
        lastUpdate += intervalsElapsed * config.interval;
      }
    }
    // Clamped unconditionally, not only after a refill: `maxTokens` can shrink
    // underneath a stocked bucket, which is what `rateLimitChange` is for, and a
    // balance banked under the old ceiling would stay spendable for a whole interval
    // — a cut from 1000 to 10 releasing 1000 requests immediately.
    if (tokens > config.maxTokens) {
      overflow += tokens - config.maxTokens;
      tokens = config.maxTokens;
      fills++;
    } else if (belowCap && tokens >= config.maxTokens) {
      fills++;
    }
    return { tokens, lastUpdate, overflow, fills };
  }

  /**
   * `config` sizes the key to outlive a refill from empty, as the Lua's
   * `bucketTtl` does; omitted only for a bucket written full.
   */
  private writeBucket(
    key: string,
    state: BucketState,
    ttlSeconds: number,
    config?: TokenBucketConfig
  ) {
    // Validated before the fields are set, so a TTL `expire` would refuse cannot
    // leave the balance spent.
    const ttl = normalizeTtlSeconds(
      config ? bucketTtl(ttlSeconds, config) : ttlSeconds
    );
    const hash = this.hash(key);
    hash.set("tokens", String(state.tokens));
    hash.set("lastUpdate", String(state.lastUpdate));
    hash.set("overflow", String(state.overflow));
    hash.set("fills", String(state.fills));
    this.expire(key, ttl);
  }

  /**
   * Records what a claim took and how far the bucket had got, for settlement.
   * Written after the spend, so `overflow` and `fills` are the post-claim values
   * the settlement compares against.
   */
  private writeMark(
    key: string,
    mark: ClaimMark,
    cost: number,
    state: BucketState
  ) {
    const markKey = claimMarkKey(key, mark.id);
    const hash = this.hash(markKey);
    hash.set("claimed", String(cost));
    hash.set("overflow", String(state.overflow));
    hash.set("fills", String(state.fills));
    hash.set(
      "generation",
      String(parseFiniteStored(this.hget(key, "generation"), 0))
    );
    this.expire(markKey, mark.ttlSeconds);
  }

  async acquireTokens(
    key: string,
    cost: number,
    config: TokenBucketConfig,
    mark?: ClaimMark
  ): Promise<AcquireTokensResult> {
    const configError = unusableBudget(cost, config);
    if (configError) {
      return {
        acquired: false,
        waitTime: 0,
        remainingTokens: 0,
        error: configError,
      };
    }
    const now = Date.now();
    const state = this.refill(key, config, now);
    const { tokens, lastUpdate } = state;

    if (tokens >= cost) {
      this.writeBucket(key, { ...state, tokens: tokens - cost }, 86400, config);
      if (mark) this.writeMark(key, mark, cost, state);
      return { acquired: true, remainingTokens: tokens - cost };
    }

    // lastUpdate only moves in whole intervals, so the time already served
    // toward the next refill has to come off the wait — the caller sleeps on
    // this number directly.
    const needed = cost - tokens;
    const intervalsNeeded = Math.ceil(needed / config.tokensToAdd);
    const waitTime = Math.max(
      0,
      intervalsNeeded * config.interval - (now - lastUpdate)
    );
    this.writeBucket(key, state, 86400, config);
    return { acquired: false, waitTime, remainingTokens: tokens };
  }

  async getTokenBucketState(
    key: string,
    config: TokenBucketConfig
  ): Promise<{ tokens: number; lastUpdate: number }> {
    const { tokens, lastUpdate } = this.refill(key, config, Date.now());
    return { tokens, lastUpdate };
  }

  async resetTokenBucket(key: string, maxTokens: number): Promise<void> {
    // The counters carry over — they only ever grow — and the generation moves
    // on, which is what tells a settlement the balance it was owed against is
    // gone.
    this.writeBucket(
      key,
      {
        tokens: maxTokens,
        lastUpdate: Date.now(),
        overflow: parseFiniteStored(this.hget(key, "overflow"), 0),
        fills: parseFiniteStored(this.hget(key, "fills"), 0),
      },
      86400
    );
    this.bumpGeneration(key);
  }

  async emptyTokenBucket(
    key: string,
    config: TokenBucketConfig
  ): Promise<void> {
    // Not refilled first: a freeze empties what is there.
    const tokens = parseFiniteStored(this.hget(key, "tokens"), 0);
    this.writeBucket(
      key,
      {
        tokens: Math.min(tokens, 0),
        lastUpdate: Date.now(),
        overflow: parseFiniteStored(this.hget(key, "overflow"), 0),
        fills: parseFiniteStored(this.hget(key, "fills"), 0),
      },
      86400,
      config
    );
    this.bumpGeneration(key);
  }

  private bumpGeneration(key: string) {
    const hash = this.hash(key);
    hash.set(
      "generation",
      String(parseFiniteStored(hash.get("generation"), 0) + 1)
    );
  }

  async settleTokens(specs: SettleSpec[]): Promise<SettleResult[]> {
    const now = Date.now();
    const results: SettleResult[] = [];
    for (const spec of specs) {
      const unsettled = {
        key: spec.key,
        settled: false,
        applied: 0,
        unapplied: 0,
      };
      const markKey = claimMarkKey(spec.key, spec.markId);
      const mark = this.live(markKey)?.hash;
      // Checked before the mark is consumed, so a settlement the backend cannot
      // act on leaves it for one that can. The Lua twin refuses the same.
      if (
        !mark ||
        !Number.isFinite(spec.actual) ||
        spec.actual < 0 ||
        unusableBudget(0, spec.config)
      ) {
        results.push(unsettled);
        continue;
      }
      this.store.delete(markKey);

      // An expired bucket reads as full, which means it refilled entirely since
      // the claim: nothing of the claim is left to correct.
      if (this.hget(spec.key, "tokens") === undefined) {
        results.push({ ...unsettled, settled: true });
        continue;
      }

      const state = this.refill(spec.key, spec.config, now);
      const { applied, unapplied } = settleAmount({
        tokens: state.tokens,
        maxTokens: spec.config.maxTokens,
        maxDebt: spec.maxDebt,
        actual: spec.actual,
        claimed: parseFiniteStored(mark.get("claimed"), 0),
        overflowNow: state.overflow,
        overflowAtClaim: parseFiniteStored(mark.get("overflow"), 0),
        fillsNow: state.fills,
        fillsAtClaim: parseFiniteStored(mark.get("fills"), 0),
        generationNow: parseFiniteStored(this.hget(spec.key, "generation"), 0),
        generationAtClaim: parseFiniteStored(mark.get("generation"), 0),
      });
      this.writeBucket(
        spec.key,
        { ...state, tokens: state.tokens - applied },
        86400,
        spec.config
      );
      results.push({ key: spec.key, settled: true, applied, unapplied });
    }
    return results;
  }

  async refundTokens(
    key: string,
    cost: number,
    maxTokens: number,
    freezeKey?: string
  ): Promise<void> {
    this.creditTokens(key, cost, maxTokens, freezeKey);
  }

  /**
   * The body of {@link refundTokens}, synchronous so that
   * {@link releaseMultiLimit} can hand back a whole claim without suspending,
   * for the reason {@link writeString} carries.
   */
  private creditTokens(
    key: string,
    cost: number,
    maxTokens: number,
    freezeKey?: string
  ) {
    if (!usableCost(cost)) return;
    if (freezeKey && this.freezeEmptiedBucket(freezeKey, Date.now())) return;
    const hash = this.live(key)?.hash;
    if (!hash) return;
    // An absent field leaves the bucket alone: the Lua returns early rather than
    // creating a balance, because a bucket with no `tokens` already reads as full.
    if (hash.get("tokens") === undefined) return;
    const tokens = parseFiniteStored(hash.get("tokens"), 0);
    hash.set("tokens", String(Math.min(maxTokens, tokens + cost)));
  }

  async tryAdmitImmediately(
    queueKey: string,
    bucketKey: string,
    freezeKey: string,
    cost: number,
    config: TokenBucketConfig,
    ttl = 86400,
    mark?: ClaimMark
  ): Promise<boolean> {
    // Before anything else, as the Redis twin reads it before its script runs.
    const keyTtl = normalizeTtlSeconds(ttl);
    if (unusableBudget(cost, config)) return false;
    // Anything already waiting outranks a new arrival.
    if (this.sorted(queueKey).length > 0) return false;

    const now = Date.now();
    if (this.freezeBlocksAdmission(freezeKey, now)) return false;

    const state = this.refill(bucketKey, config, now);
    if (state.tokens < cost) {
      this.writeBucket(bucketKey, state, keyTtl, config);
      return false;
    }
    this.writeBucket(
      bucketKey,
      { ...state, tokens: state.tokens - cost },
      keyTtl,
      config
    );
    if (mark) this.writeMark(bucketKey, mark, cost, state);
    return true;
  }

  async tryAdmitNoLimit(queueKey: string, freezeKey: string): Promise<boolean> {
    // One call, no suspension: the queue check and the freeze check cannot be
    // separated by another spender's write.
    if (this.sorted(queueKey).length > 0) return false;
    return !this.freezeBlocksAdmission(freezeKey, Date.now());
  }

  // ------------------------------------------------------------ several budgets

  /**
   * Evaluates every budget before committing to any of them, so a request that
   * cannot have all of them spends none. Twin of the Redis backend's
   * `acquireMultiLimit` Lua.
   *
   * Every branch below runs without suspending, which is what makes the two
   * phases one operation here — an `await` between the checks and the writes
   * would let another spender land in between.
   */
  async acquireMultiLimit(
    specs: MultiLimitSpec[]
  ): Promise<AcquireMultiLimitResult> {
    const now = Date.now();
    const planned = this.planMultiLimit(specs, now);
    if (planned.error) return { acquired: false, error: planned.error };
    if (!planned.acquired) {
      // Refill progress is persisted even on refusal, as the single-budget path
      // does: the credit is real whether or not this request gets to spend it.
      for (const bucket of planned.buckets) {
        this.writeBucket(bucket.key, bucket, 86400, bucket.config);
      }
      return {
        acquired: false,
        waitTime: planned.waitTime ?? 0,
        blockedBy: planned.blockedBy,
      };
    }
    for (const bucket of planned.buckets) {
      this.commitBucket(bucket, 86400);
    }
    for (const slot of planned.slots) {
      this.claimSlot(slot.key, slot.slotId, slot.cost, now, 86400);
    }
    return { acquired: true };
  }

  async releaseMultiLimit(specs: MultiLimitSpec[]): Promise<void> {
    for (const spec of specs) {
      if (spec.kind === "concurrency") {
        this.releaseSlot(spec.key, spec.slotId);
        continue;
      }
      this.creditTokens(
        spec.key,
        spec.cost,
        spec.config.maxTokens,
        spec.freezeKey
      );
    }
  }

  async tryAdmitMultiLimit(
    queueKey: string,
    freezeKeys: string[],
    specs: MultiLimitSpec[],
    ttl = 86400
  ): Promise<boolean> {
    // Before anything else, as the Redis twin reads it before its script runs.
    const keyTtl = normalizeTtlSeconds(ttl);
    if (this.sorted(queueKey).length > 0) return false;
    const now = Date.now();
    for (const freezeKey of freezeKeys) {
      if (this.freezeBlocksAdmission(freezeKey, now)) return false;
    }

    const planned = this.planMultiLimit(specs, now);
    if (planned.error) return false;
    if (!planned.acquired) {
      for (const bucket of planned.buckets) {
        this.writeBucket(bucket.key, bucket, keyTtl, bucket.config);
      }
      return false;
    }
    for (const bucket of planned.buckets) {
      this.commitBucket(bucket, keyTtl);
    }
    for (const slot of planned.slots) {
      this.claimSlot(slot.key, slot.slotId, slot.cost, now, keyTtl);
    }
    return true;
  }

  private commitBucket(bucket: PlannedBucket, ttl: number) {
    this.writeBucket(
      bucket.key,
      { ...bucket, tokens: bucket.tokens - bucket.cost },
      ttl,
      bucket.config
    );
    if (bucket.mark)
      this.writeMark(bucket.key, bucket.mark, bucket.cost, bucket);
  }

  /**
   * The check phase both multi-budget paths share: refills every bucket, reaps
   * every slot ledger, and reports whether each could take its spec's `cost`.
   *
   * Returns the refilled balances rather than writing them, so the caller
   * commits once it knows the whole set admitted. Reaping is a write and happens
   * here regardless — an expired slot is expired whoever asked.
   */
  private planMultiLimit(
    specs: MultiLimitSpec[],
    now: number
  ): {
    acquired: boolean;
    error?: string;
    waitTime?: number;
    blockedBy?: string;
    buckets: PlannedBucket[];
    slots: PlannedSlot[];
  } {
    const buckets: PlannedBucket[] = [];
    const slots: PlannedSlot[] = [];
    let acquired = true;
    let waitTime: number | undefined;
    let blockedBy: string | undefined;

    for (const spec of specs) {
      const { cost } = spec;
      if (spec.kind === "tokenBucket") {
        const error = unusableBudget(cost, spec.config);
        if (error) return { acquired: false, error, buckets, slots };
        const state = this.refill(spec.key, spec.config, now);
        const { tokens, lastUpdate } = state;
        buckets.push({
          ...state,
          key: spec.key,
          cost,
          config: spec.config,
          mark: spec.mark,
        });
        if (tokens >= cost) continue;
        const intervalsNeeded = Math.ceil(
          (cost - tokens) / spec.config.tokensToAdd
        );
        const wait = Math.max(
          0,
          intervalsNeeded * spec.config.interval - (now - lastUpdate)
        );
        // The longest wait any budget asks for: coming back sooner only to be
        // refused by this same budget is a wake-up that cannot succeed.
        if (waitTime === undefined || wait > waitTime) {
          waitTime = wait;
          blockedBy = spec.key;
        }
        acquired = false;
        continue;
      }

      if (!usableConcurrency(spec.config.maxConcurrency)) {
        return {
          acquired: false,
          error: "maxConcurrency must be a finite number greater than 0",
          buckets,
          slots,
        };
      }
      if (!usableCost(cost)) {
        return {
          acquired: false,
          error: "cost must be a finite number that is not negative",
          buckets,
          slots,
        };
      }
      if (!usableRequestTtl(spec.config.requestTtl)) {
        return {
          acquired: false,
          error: "requestTtl must be a finite number greater than 0",
          buckets,
          slots,
        };
      }
      this.reapConcurrency(spec.key, now - spec.config.requestTtl);
      const currentCost = this.concurrencyCost(spec.key, spec.slotId);
      if (currentCost + cost > spec.config.maxConcurrency) {
        acquired = false;
        // No deadline of its own: a slot is freed by a completion, and the
        // caller decides what to fall back on. Recorded only if nothing with a
        // real wait already claimed the report.
        if (blockedBy === undefined) blockedBy = spec.key;
        continue;
      }
      slots.push({ key: spec.key, slotId: spec.slotId, cost });
    }

    return { acquired, waitTime, blockedBy, buckets, slots };
  }

  // ------------------------------------------------------------- concurrency

  /** Drops slots older than the TTL — the equivalent of a crashed holder. */
  private reapConcurrency(key: string, expireTime: number) {
    const z = this.live(key)?.zset;
    if (!z) return;
    const costs = this.live(`${key}:costs`)?.hash;
    for (const [member, score] of [...z.entries()]) {
      if (score <= expireTime) {
        z.delete(member);
        costs?.delete(member);
      }
    }
    this.dropIfEmpty(key);
    this.dropIfEmpty(`${key}:costs`);
  }

  /** Total cost in flight, ignoring `exceptId` so a resubmit is not counted twice. */
  private concurrencyCost(key: string, exceptId?: string): number {
    const z = this.live(key)?.zset;
    if (!z) return 0;
    const costs = this.live(`${key}:costs`)?.hash;
    let total = 0;
    for (const member of z.keys()) {
      if (member === exceptId) continue;
      // This total feeds admission, not just stats: a NaN makes `total + cost > max`
      // false and the ceiling stops enforcing entirely. The Lua defaults to 1 here.
      total += parseFiniteStored(costs?.get(member), 1);
    }
    return total;
  }

  private claimSlot(
    key: string,
    requestId: string,
    cost: number,
    now: number,
    keyTtl: number
  ) {
    const ttl = normalizeTtlSeconds(keyTtl);
    this.zset(key).set(requestId, now);
    this.hash(`${key}:costs`).set(requestId, String(cost));
    this.expire(key, ttl);
    this.expire(`${key}:costs`, ttl);
  }

  async acquireConcurrency(
    key: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult> {
    const now = Date.now();
    if (!usableConcurrency(config.maxConcurrency)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "maxConcurrency must be a finite number greater than 0",
      };
    }
    if (!usableCost(cost)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "cost must be a finite number that is not negative",
      };
    }
    if (!usableRequestTtl(config.requestTtl)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "requestTtl must be a finite number greater than 0",
      };
    }
    this.reapConcurrency(key, now - config.requestTtl);
    const currentCost = this.concurrencyCost(key, requestId);

    if (currentCost + cost > config.maxConcurrency) {
      return { acquired: false, currentConcurrency: currentCost };
    }
    this.claimSlot(key, requestId, cost, now, 86400);
    return { acquired: true, currentConcurrency: currentCost + cost };
  }

  async acquireQueuedConcurrency(
    key: string,
    metadataKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig
  ): Promise<AcquireConcurrencyResult> {
    if (!usableConcurrency(config.maxConcurrency)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "maxConcurrency must be a finite number greater than 0",
      };
    }
    // Ahead of the status check, as the Lua twin orders it: an unusable cost is
    // the caller's bug either way, and reporting "not in progress" for it would
    // send them looking at the queue entry instead.
    if (!usableCost(cost)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "cost must be a finite number that is not negative",
      };
    }
    if (!usableRequestTtl(config.requestTtl)) {
      return {
        acquired: false,
        currentConcurrency: 0,
        error: "requestTtl must be a finite number greater than 0",
      };
    }
    this.reapConcurrency(key, Date.now() - config.requestTtl);
    if (this.hget(metadataKey, "status") !== "inProgress") {
      // Excluding `requestId`, as the Lua's shared slot accounting does and as
      // `acquireConcurrency` does below: a resubmission of a request that still
      // holds a slot must not be counted against itself.
      return {
        acquired: false,
        currentConcurrency: this.concurrencyCost(key, requestId),
      };
    }
    return this.acquireConcurrency(key, cost, requestId, config);
  }

  async tryAdmitConcurrency(
    queueKey: string,
    concurrencyKey: string,
    freezeKey: string,
    cost: number,
    requestId: string,
    config: ConcurrencyConfig,
    ttl = 86400
  ): Promise<boolean> {
    // Before anything else, as the Redis twin reads it before its script runs.
    const keyTtl = normalizeTtlSeconds(ttl);
    if (this.sorted(queueKey).length > 0) return false;
    if (!usableConcurrency(config.maxConcurrency)) return false;
    if (!usableCost(cost)) return false;
    if (!usableRequestTtl(config.requestTtl)) return false;

    const now = Date.now();
    if (this.freezeBlocksAdmission(freezeKey, now)) return false;

    this.reapConcurrency(concurrencyKey, now - config.requestTtl);
    const currentCost = this.concurrencyCost(concurrencyKey, requestId);
    if (currentCost + cost > config.maxConcurrency) return false;

    this.claimSlot(concurrencyKey, requestId, cost, now, keyTtl);
    return true;
  }

  async releaseConcurrency(key: string, requestId: string): Promise<void> {
    this.releaseSlot(key, requestId);
  }

  /** The body of {@link releaseConcurrency}; see {@link creditTokens}. */
  private releaseSlot(key: string, requestId: string) {
    this.live(key)?.zset?.delete(requestId);
    this.live(`${key}:costs`)?.hash?.delete(requestId);
    this.dropIfEmpty(key);
    this.dropIfEmpty(`${key}:costs`);
  }

  async getConcurrencyState(
    key: string,
    requestTtl: number
  ): Promise<{ currentConcurrency: number; activeRequests: string[] }> {
    // Raises rather than reporting an empty client: a NaN cutoff loses every
    // `timestamp > cutoff`, so a monitor would read a full client as idle. The
    // Redis twin already fails here, from `ZRANGEBYSCORE '(nan'`.
    if (!Number.isFinite(requestTtl)) {
      throw new RangeError(
        `requestTtl must be a finite number, received ${String(requestTtl)}`
      );
    }
    const cutoff = Date.now() - requestTtl;
    const activeRequests = [...(this.live(key)?.zset?.entries() ?? [])]
      .filter(([, timestamp]) => timestamp > cutoff)
      // Score then member, as `sorted()` and Redis both order: slots claimed in the
      // same millisecond are the common case under load, and ZRANGEBYSCORE breaks
      // that tie lexically rather than by insertion.
      .sort((a, b) => a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
      .map(([requestId]) => requestId);
    const costs = this.live(`${key}:costs`)?.hash;
    return {
      currentConcurrency: activeRequests.reduce(
        (total, requestId) =>
          total + parseFiniteStored(costs?.get(requestId), 1),
        0
      ),
      activeRequests,
    };
  }

  async clearConcurrency(key: string): Promise<void> {
    this.store.delete(key);
    this.store.delete(`${key}:costs`);
  }

  // ------------------------------------------------------------------- queue

  private toQueued(metadata: Record<string, string>): QueuedRequest | null {
    if (
      !metadata.requestId ||
      !metadata.clientName ||
      !metadata.requestName ||
      !metadata.status
    ) {
      return null;
    }
    return {
      requestId: metadata.requestId,
      clientName: metadata.clientName,
      requestName: metadata.requestName,
      status: metadata.status as "pending" | "inProgress",
      priority: parsePriority(metadata.priority),
      cost: parseStoredCost(metadata.cost),
      retries: parseInt(metadata.retries, 10) || 0,
      timestamp: parseInt(metadata.timestamp, 10) || 0,
      grantId: metadata.grantId || undefined,
      isThawRequest: metadata.isThawRequest === "true",
      ownerId: metadata.ownerId || "",
    };
  }

  async addRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    request: QueuedRequest,
    ttl = 86400
  ): Promise<boolean> {
    const metadataKey = `${metadataKeyPrefix}:${request.requestId}`;
    // Refused after removal, so an abandonment that overtook its own add cannot be
    // undone by the add landing afterwards. See the Lua twin.
    if (this.live(tombstoneKey(metadataKeyPrefix, request.requestId))) {
      return false;
    }
    // Already queued — re-adding would reset its position, so a retry of the
    // enqueue call must be a no-op rather than a queue-jump.
    if (this.live(metadataKey)?.hash) return false;

    // Both the score and the TTL are read before either write, so an unusable one
    // leaves nothing behind. A half-added entry is the orphan
    // docs/design-notes.md#two-backends-one-contract warns about: it has a live
    // owner nobody awaits, so cleanup spares it and it pins the queue non-empty.
    const score = calculateQueueScore(
      request.priority,
      request.retries,
      request.timestamp
    );
    const normalizedTtl = normalizeTtlSeconds(ttl);

    this.zset(queueKey).set(request.requestId, score);
    const hash = this.hash(metadataKey);
    hash.set("requestId", request.requestId);
    hash.set("clientName", request.clientName);
    hash.set("requestName", request.requestName);
    hash.set("status", request.status);
    hash.set("priority", String(request.priority));
    hash.set("cost", formatStoredCost(request.cost));
    hash.set("retries", String(request.retries));
    hash.set("timestamp", String(request.timestamp));
    hash.set("grantId", request.grantId || "");
    hash.set("isThawRequest", request.isThawRequest ? "true" : "false");
    hash.set("ownerId", request.ownerId);
    this.expire(queueKey, normalizedTtl);
    this.expire(metadataKey, normalizedTtl);
    return true;
  }

  async getQueueLength(queueKey: string): Promise<number> {
    // `sorted()` would materialise and sort every member to produce a number the
    // map already knows. The fast path asks this on every request, so at depth it
    // was the sort — not the count — that cost the time.
    return this.live(queueKey)?.zset?.size ?? 0;
  }

  async getNextRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    skipGrantIds: string[] = [],
    skipRequestIds: string[] = []
  ): Promise<QueuedRequest | null> {
    const skip = new Set(skipGrantIds);
    const skipIds = new Set(skipRequestIds);
    for (const requestId of this.sorted(queueKey)) {
      if (skipIds.has(requestId)) continue;
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      if (this.hget(metadataKey, "status") !== "pending") continue;
      const grantId = this.hget(metadataKey, "grantId") ?? "";
      if (grantId !== "" && skip.has(grantId)) continue;
      this.hash(metadataKey).set("status", "inProgress");
      return this.toQueued(await this.hgetall(metadataKey));
    }
    return null;
  }

  async getRequest(
    metadataKeyPrefix: string,
    requestId: string
  ): Promise<QueuedRequest | null> {
    return this.toQueued(
      await this.hgetall(`${metadataKeyPrefix}:${requestId}`)
    );
  }

  async updateRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    updates: Partial<QueuedRequest>
  ): Promise<void> {
    const metadataKey = `${metadataKeyPrefix}:${requestId}`;
    const hash = this.live(metadataKey)?.hash;
    if (!hash) return;

    // Skipped rather than stored, matching the Lua's `type(x) == 'number'`:
    // `JSON.stringify` writes null for both NaN and Infinity, so a non-finite
    // field never reaches that script at all.
    const retries =
      updates.retries !== undefined && Number.isFinite(updates.retries)
        ? updates.retries
        : undefined;
    const priority =
      updates.priority !== undefined && Number.isFinite(updates.priority)
        ? updates.priority
        : undefined;
    const needsScoreUpdate = retries !== undefined || priority !== undefined;
    // The member the re-score writes, resolved before any field is: the Lua
    // refuses the whole update without it, so the metadata and the order it
    // carries change together or not at all.
    const member = hash.get("requestId");
    if (needsScoreUpdate && member === undefined) return;

    if (updates.status !== undefined) hash.set("status", updates.status);
    if (retries !== undefined) hash.set("retries", String(retries));
    if (priority !== undefined) hash.set("priority", String(priority));
    if (updates.isThawRequest !== undefined) {
      hash.set("isThawRequest", updates.isThawRequest ? "true" : "false");
    }

    if (!needsScoreUpdate || member === undefined) return;
    this.zset(queueKey).set(
      member,
      // Parsed exactly as the Lua re-score parses the same fields, which is the
      // only way the two agree about a corrupt one: `Number` alone reads an empty
      // field as 0 and an unparseable one as NaN, where `tonumber(...) or <d>`
      // reads both as missing.
      rescore(
        parseStoredNumber(hash.get("priority"), 1),
        parseStoredNumber(hash.get("retries"), 0),
        parseStoredNumber(hash.get("timestamp"), 0)
      )
    );
  }

  async removeRequest(
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    tombstoneTtlSeconds: number = REQUEST_TOMBSTONE_TTL_SECONDS
  ): Promise<{ wasThawRequest: boolean }> {
    const metadataKey = `${metadataKeyPrefix}:${requestId}`;
    const tombstoneTtl = normalizeTombstoneTtl(tombstoneTtlSeconds);
    const wasThawRequest = this.hget(metadataKey, "isThawRequest") === "true";
    this.live(queueKey)?.zset?.delete(requestId);
    this.dropIfEmpty(queueKey);
    this.store.delete(metadataKey);
    // Written even when there was nothing to remove, which is the case the marker
    // exists for. See the Lua twin.
    const tombstone = tombstoneKey(metadataKeyPrefix, requestId);
    this.put(tombstone, {
      str: "1",
      expiresAt: Date.now() + tombstoneTtl * 1000,
    });
    return { wasThawRequest };
  }

  async getQueueStats(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueueStats> {
    let pending = 0;
    let inProgress = 0;
    let totalCost = 0;
    const costByLimit: Record<string, number> = {};
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const status = this.hget(metadataKey, "status");
      if (!status) continue;
      const raw = this.hget(metadataKey, "cost");
      const perLimit = raw?.trimStart().startsWith("{")
        ? parseStoredCost(raw)
        : undefined;
      if (perLimit !== undefined) {
        // Finite numbers only, as the Lua twin keeps them: one corrupt entry
        // must not poison the totals for every limit.
        if (typeof perLimit !== "number") {
          for (const [name, amount] of Object.entries(perLimit)) {
            if (typeof amount !== "number" || !Number.isFinite(amount))
              continue;
            costByLimit[name] = (costByLimit[name] ?? 0) + amount;
          }
        }
      } else {
        // The shared parse, not Number: `Number("")` is 0 and `Number("abc")`
        // is NaN, and a single NaN here poisons totalCost for the whole queue
        // while the Redis twin's `finiteNumber(..., 1)` reports 1 for both.
        totalCost += parseFiniteStored(raw, 1);
      }
      if (status === "pending") pending++;
      else if (status === "inProgress") inProgress++;
    }
    return { pending, inProgress, totalCost, costByLimit };
  }

  async getAllRequests(
    queueKey: string,
    metadataKeyPrefix: string
  ): Promise<QueuedRequest[]> {
    const requests: QueuedRequest[] = [];
    for (const requestId of this.sorted(queueKey)) {
      // Read synchronously, as `getQueueStats` reads the same entries: `await`
      // yields to the microtask queue between two entries even for an already
      // settled promise, and `BatchOptions.atomic` promises a reader never sees
      // a group half-applied.
      const hash = this.live(`${metadataKeyPrefix}:${requestId}`)?.hash;
      if (!hash?.size) continue;
      const parsed = this.toQueued(Object.fromEntries(hash));
      if (parsed) requests.push(parsed);
    }
    return requests;
  }

  async cleanupOrphanedRequests(
    queueKey: string,
    metadataKeyPrefix: string,
    aliveInstanceIds: Set<string>,
    currentInstanceId: string
  ): Promise<number> {
    // A set the sweeper cannot find itself in was not read authoritatively, and
    // acting on it reaps live owners. See the Lua twin.
    if (!aliveInstanceIds.has(currentInstanceId)) return 0;

    let cleanedUp = 0;
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const ownerId = this.hget(metadataKey, "ownerId");
      if (ownerId === undefined) {
        // Metadata already gone; drop the dangling queue entry.
        this.live(queueKey)?.zset?.delete(requestId);
        cleanedUp++;
      } else if (ownerId === "" || !aliveInstanceIds.has(ownerId)) {
        this.live(queueKey)?.zset?.delete(requestId);
        this.store.delete(metadataKey);
        cleanedUp++;
      }
    }
    this.dropIfEmpty(queueKey);
    return cleanedUp;
  }

  // ------------------------------------------------------------ freeze / thaw

  async setFreezeState(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    options: { emptiesBucket?: boolean } = {}
  ): Promise<void> {
    this.writeFreezeState(
      key,
      frozenUntil,
      thawRequestCount,
      "set",
      options.emptiesBucket !== false
    );
  }

  /** See the Redis twin: never shortens a standing freeze, and returns what won. */
  async armFreeze(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    options: { emptiesBucket?: boolean } = {}
  ): Promise<FreezeState> {
    return this.writeFreezeState(
      key,
      frozenUntil,
      thawRequestCount,
      "max",
      options.emptiesBucket !== false
    );
  }

  /**
   * No `await` between the read and the write: this backend's atomicity is a
   * property of the language, but only for code that does not suspend.
   */
  private writeFreezeState(
    key: string,
    frozenUntil: number,
    thawRequestCount: number,
    mode: "set" | "max",
    emptiesBucket: boolean
  ): FreezeState {
    const now = Date.now();
    // Sanitised before the merge: a NaN compares false against the standing value,
    // so the merge kept it and cancelled the probe budget a 429 was owed.
    if (!Number.isFinite(frozenUntil)) frozenUntil = 0;
    if (!Number.isFinite(thawRequestCount)) thawRequestCount = 0;
    if (mode === "max") {
      // An unusable stored deadline is no freeze state to merge with, so the
      // probe budget beside it is not consulted either — the reading every
      // freeze gate gives it, and the Lua twin's.
      const existingUntil = usableDeadline(this.hget(key, "frozenUntil"));
      if (existingUntil !== undefined) {
        if (existingUntil > now && this.hget(key, "emptiesBucket") !== "0") {
          emptiesBucket = true;
        }
        if (existingUntil > frozenUntil) frozenUntil = existingUntil;
        const existingThaw = parseFiniteStored(
          this.hget(key, "thawRequestCount"),
          0
        );
        if (existingThaw > thawRequestCount) thawRequestCount = existingThaw;
      }
    }
    // After the merge, so a stored value that won it is written back in range
    // rather than carried forward by "keep the larger", and what is returned is
    // what was stored. The 2^53-1 ceiling also holds the TTL below inside the
    // range `normalizeTtlSeconds` accepts.
    frozenUntil = storable(frozenUntil, -Number.MAX_SAFE_INTEGER);
    thawRequestCount = storable(thawRequestCount, 0);
    // Derived and validated before the fields are set, as `writeHash` validates
    // its own: `expire` raises, and would otherwise leave the freeze half-written.
    const ttl = normalizeTtlSeconds(
      Math.max(Math.ceil((frozenUntil - now) / 1000) + 60, 60)
    );
    const hash = this.hash(key);
    hash.set("frozenUntil", String(frozenUntil));
    hash.set("thawRequestCount", String(thawRequestCount));
    hash.set("emptiesBucket", emptiesBucket ? "1" : "0");
    this.expire(key, ttl);
    return { frozenUntil, thawRequestCount };
  }

  /**
   * The body of {@link getFreezeState}, synchronous so that the two methods that
   * decide on it do not sample the clock a suspension later than the state. See
   * the Redis twin's one-script `readFreezeState`.
   */
  private readFreezeState(key: string, now: number): FreezeState | null {
    const frozenUntil = usableDeadline(this.hget(key, "frozenUntil", now));
    if (frozenUntil === undefined) return null;
    const thawRequestCount = parseFiniteStored(
      this.hget(key, "thawRequestCount", now),
      0
    );
    // Reports expiry without deleting, matching Redis. See the comment there.
    if (now >= frozenUntil && thawRequestCount <= 0) return null;
    return { frozenUntil, thawRequestCount };
  }

  async getFreezeState(key: string): Promise<FreezeState | null> {
    return this.readFreezeState(key, Date.now());
  }

  async updateThawProgress(
    key: string,
    success: boolean,
    completionId?: string
  ): Promise<FreezeState | null> {
    // Read and write with no `await` between them: this backend's atomicity is
    // a property of the language, but only for code that does not suspend.
    const completionKey = `${key}:thawCompletions`;
    // Recorded before the freeze state is read, as the Lua twin records it: a
    // completion delivered while no freeze stands is still remembered, so a
    // redelivery of it cannot decrement a freeze armed afterwards. Checked through
    // `live`, not `setOf`, so the read does not create the set it is asking about.
    const alreadySeen =
      completionId !== undefined &&
      completionId !== "" &&
      this.live(completionKey)?.set?.has(completionId) === true;
    if (completionId && !alreadySeen) {
      this.setOf(completionKey).add(completionId);
      this.expire(completionKey, 86400);
    }

    const raw = this.hget(key, "frozenUntil");
    if (raw === undefined) return null;
    const frozenUntil = usableDeadline(raw);
    if (frozenUntil === undefined) return null;
    const thawRequestCount = parseFiniteStored(
      this.hget(key, "thawRequestCount"),
      0
    );
    if (alreadySeen) return { frozenUntil, thawRequestCount };

    if (Date.now() >= frozenUntil && thawRequestCount <= 0) {
      // The one place expiry deletes the key; a read must not.
      this.store.delete(key);
      return null;
    }
    if (!success) return { frozenUntil, thawRequestCount };

    const newCount = Math.max(0, thawRequestCount - 1);
    // A window re-armed while this probe was in flight still stands.
    if (newCount === 0 && Date.now() >= frozenUntil) {
      this.store.delete(key);
      return null;
    }
    this.hash(key).set("thawRequestCount", String(newCount));
    return { frozenUntil, thawRequestCount: newCount };
  }

  async clearFreezeState(key: string): Promise<void> {
    this.store.delete(key);
  }

  async isFrozen(key: string): Promise<boolean> {
    // One clock sample for the read and the decision, as the Redis twin decides
    // on one TIME.
    const now = Date.now();
    const state = this.readFreezeState(key, now);
    if (!state) return false;
    if (now < state.frozenUntil) return true;
    return state.thawRequestCount > 0;
  }

  async canProcessRequest(key: string): Promise<{
    canProcess: boolean;
    isThawRequest: boolean;
    frozenUntil?: number;
  }> {
    const now = Date.now();
    const state = this.readFreezeState(key, now);
    if (!state) return { canProcess: true, isThawRequest: false };
    // `frozenUntil` accompanies both outcomes so a caller can book a wake-up
    // against the deadline rather than a locally computed delay. Here the two
    // clocks are the same one; the Redis twin is where it matters.
    if (now < state.frozenUntil) {
      return {
        canProcess: false,
        isThawRequest: false,
        frozenUntil: state.frozenUntil,
      };
    }
    return {
      canProcess: true,
      isThawRequest: true,
      frozenUntil: state.frozenUntil,
    };
  }

  /**
   * Whether the entry at `metadataKey` is an in-progress probe that a claim for
   * `grantId` must yield to — any grant's, under `anyGrant`.
   */
  private isCompetingProbe(
    metadataKey: string,
    grantId: string,
    anyGrant: boolean
  ): boolean {
    // Normalised as the Lua twin's `fields[2] or ''` does: a client-level probe
    // carries no `grantId` field, so an absent one has to match its own kind.
    const otherGrantId = this.hget(metadataKey, "grantId") ?? "";
    return (
      this.hget(metadataKey, "status") === "inProgress" &&
      (anyGrant || otherGrantId === grantId) &&
      this.hget(metadataKey, "isThawRequest") === "true"
    );
  }

  async hasThawRequestInProgress(
    queueKey: string,
    metadataKeyPrefix: string,
    grantId: string,
    options: { anyGrant?: boolean } = {}
  ): Promise<boolean> {
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      if (this.isCompetingProbe(metadataKey, grantId, !!options.anyGrant)) {
        return true;
      }
    }
    return false;
  }

  async tryStartThawRequest(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    requestId: string,
    grantId: string,
    options: { anyGrant?: boolean } = {}
  ): Promise<"started" | "exists"> {
    // A client-level thaw has no grant to track; the frozen-grants set exists
    // so getNextRequest can skip individual grants.
    // Expired as the Lua twin expires it: membership is normally dropped by
    // `cleanupStaleFrozenGrants`, which only a running controller calls.
    if (grantId !== "") {
      this.setOf(frozenGrantsKey).add(grantId);
      this.expire(frozenGrantsKey, 86400);
    }

    for (const otherId of this.sorted(queueKey)) {
      if (otherId === requestId) continue;
      const metadataKey = `${metadataKeyPrefix}:${otherId}`;
      if (this.isCompetingProbe(metadataKey, grantId, !!options.anyGrant)) {
        return "exists";
      }
    }

    // Only marks an entry that still exists — `hash()` would otherwise create one
    // that no cleanup path can reach. See the Lua twin's EXISTS guard.
    const metadataKey = `${metadataKeyPrefix}:${requestId}`;
    if (!this.live(metadataKey)?.hash) return "exists";
    this.hash(metadataKey).set("isThawRequest", "true");
    return "started";
  }

  async cleanupStaleFrozenGrants(
    frozenGrantsKey: string,
    queueKey: string,
    metadataKeyPrefix: string,
    freezeStateKeyPrefix: string
  ): Promise<number> {
    const now = Date.now();
    const probing = new Set<string>();
    for (const requestId of this.sorted(queueKey)) {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const grantId = this.hget(metadataKey, "grantId");
      if (
        this.hget(metadataKey, "status") === "inProgress" &&
        this.hget(metadataKey, "isThawRequest") === "true" &&
        grantId !== undefined
      ) {
        probing.add(grantId);
      }
    }

    let cleaned = 0;
    for (const grantId of [...(this.live(frozenGrantsKey)?.set ?? [])]) {
      if (probing.has(grantId)) continue;
      const raw = this.hget(
        `${freezeStateKeyPrefix}${grantId}:freezeState`,
        "frozenUntil"
      );
      if (raw === undefined || now >= parseFiniteStored(raw, 0)) {
        this.removeMember(frozenGrantsKey, grantId);
        cleaned++;
      }
    }
    return cleaned;
  }

  // --------------------------------------------------------------- instances

  async getInstances(
    instanceSetKey: string,
    instanceKeyPrefix: string,
    currentInstanceId: string
  ): Promise<Array<{ id: string; data: string }>> {
    const instances: Array<{ id: string; data: string }> = [];
    const set = this.live(instanceSetKey)?.set;
    if (!set) return instances;
    for (const id of [...set]) {
      if (id === currentInstanceId) continue;
      const data = this.live(`${instanceKeyPrefix}${id}`)?.str;
      if (data) instances.push({ id, data });
      else this.removeMember(instanceSetKey, id);
    }
    return instances;
  }

  // --------------------------------------------------------------- lifecycle

  async now(): Promise<number> {
    return Date.now();
  }

  /**
   * Stops the expiry sweep and drops every subscriber. The store is kept, as
   * the Redis backend keeps its keys: a handler started again on this backend
   * finds what it stored, and a later write re-arms the sweep.
   */
  async close(): Promise<void> {
    if (this.sweeper) clearInterval(this.sweeper);
    this.sweeper = null;
    this.bus.removeAllListeners();
    this.busListening = false;
    this.subscribed.clear();
  }
}

/**
 * Coordinates in local memory — **single process only**.
 *
 * Each process gets its own copy of every rate limit, so running two of them
 * doubles what the vendor sees. Use `redisBackend` from
 * `@dianemo/backend-redis` for anything that scales past one process. See
 * {@link MemoryBackend} for the full trade-offs.
 *
 * ```ts
 * import RequestHandler, { memoryBackend } from "@dianemo/core";
 *
 * const handler = new RequestHandler({ key, backend: memoryBackend() });
 * ```
 */
export function memoryBackend(): DianemoBackend {
  return new MemoryBackend();
}

export default MemoryBackend;
