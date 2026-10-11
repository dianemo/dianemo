import { ClientFrozenError, ConfigurationError } from "../../errors.js";
import BaseClient from "../index.js";
import {
  assertUsableRateLimits,
  hasMeteredBudget,
  normalizeRateLimit,
  shortestRefillInterval,
  soleRateLimit,
} from "../../utils/rateLimit.js";
import type {
  ClaimMark,
  MultiLimitSpec,
  SettleSpec,
  TokenBucketConfig,
} from "../../backend/types.js";
import type {
  RequestCost,
  RequestDoneData,
  RequestMetadata,
} from "../../request/types.js";
import {
  budgetLimitsFor,
  claimedAmount,
  costFor,
  settledCostProblem,
  unusableCostReason,
} from "../../utils/cost.js";
import type {
  AcquireTurnResult,
  ClientConstructorData,
  NamedRateLimitData,
  NamedRateLimitStats,
  RateLimitUpdatedData,
} from "../types.js";

/**
 * Every client that meters against budgets of its own — one of them or several.
 *
 * One class rather than one per limit type, because the only thing that varies
 * between them is which backend call claims the budget. A request is admitted
 * when every declared limit can take it, which for a single limit is the
 * ordinary token bucket or concurrency ceiling and for several is all of them at
 * once. See docs/rate-limits/multiple-limits.md.
 *
 * The exception is `sharedLimit`, which owns no budget and no queue and is
 * therefore its own class. It may not be combined with these.
 */
class MeteredClient extends BaseClient {
  protected rateLimit: NamedRateLimitData[];
  /**
   * The one limit this client declares, when it declares one.
   *
   * Present only so the single-budget backend calls can be used where they
   * apply: they are one round trip against one key, where the general path
   * encodes a spec list and runs a variadic script. The decision each makes is
   * identical — this is a shortcut, never a different rule.
   *
   * Derived rather than cached, here and below. A cached copy is a second
   * reading of `rateLimit` that a write can leave behind, and the two disagreeing
   * would have the claim spend against one budget while the ceiling was checked
   * against another.
   */
  private get sole(): NamedRateLimitData | undefined {
    return soleRateLimit(this.rateLimit);
  }

  /** False when nothing here meters anything, so admission needs no claim at all. */
  private get metered(): boolean {
    return hasMeteredBudget(this.rateLimit);
  }

  constructor(data: ClientConstructorData, rateLimit: NamedRateLimitData[]) {
    super(data, data.client.name);
    assertUsableRateLimits(rateLimit, data.client.name);
    this.rateLimit = rateLimit;
    assertBackendCanClaim(data.backend, data.client.name, rateLimit);
    const slotTtl = data.client.requestOptions?.concurrencySlotTtl;
    if (slotTtl && Number.isFinite(slotTtl) && slotTtl > 0) {
      this.requestTtl = slotTtl;
    }
  }

  /**
   * Crash recovery only, for concurrency entries, and deliberately not
   * `cleanupTimeout` — the two want opposite values. Too short reaps slots from
   * requests still in flight and the cap stops holding, so tune
   * `concurrencySlotTtl` against the slowest legitimate response.
   */
  private requestTtl = 120000;

  public handleRateLimitUpdated(data: RateLimitUpdatedData) {
    // A config this class does not build would mean a different client class,
    // which a broadcast cannot change; the rebuild path swaps one for another.
    // Guarded because this arrives off pub/sub: an operator, or a replica running
    // a version that predates the list, can publish something that is not one,
    // and an unhandled throw here takes the subscription with it.
    if (!Array.isArray(data.rateLimit)) {
      this.logger.error(
        { rateLimit: data.rateLimit },
        `Client ${this.name} | ignoring a rate limit update that is not a list of limits`
      );
      return;
    }
    // Names resolved on receipt, not just before publishing: an operator's update
    // may omit them exactly as a caller may, and keeps a budget the same way.
    const limits = normalizeRateLimit(data.rateLimit, this.rateLimit);
    if (limits.some((limit) => limit.type === "sharedLimit")) return;
    try {
      assertUsableRateLimits(limits, this.name);
      // Checked here as in the constructor: one limit growing to several would
      // otherwise reach a backend without the multi-limit methods only at claim
      // time, where every pass throws and the queue waits out its timeouts.
      assertBackendCanClaim(this.backend, this.name, limits);
    } catch (error) {
      this.logger.error(
        { error, rateLimit: data.rateLimit },
        `Client ${this.name} | ignoring an unusable rate limit update`
      );
      return;
    }
    this.rateLimit = limits;

    // Poked on any change, in either direction. A reduction can make an
    // already-queued cost impossible, which the drain loop discovers and reports.
    // An increase creates capacity that no existing booking covers, because a
    // declined request's only wake-up was computed from the token maths at decline
    // time. Unconditional, since `processRequests` coalesces through
    // `processingLock` and returns immediately with nothing to admit.
    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed after a budget change`
      );
    });
  }

  // ------------------------------------------------------------ budget specs

  /**
   * Every budget a request must claim, in the order declared, each keyed by its
   * own name under this client's namespace and charged its own share of `cost`.
   * A `noLimit` entry contributes nothing, which is what makes it legal here.
   *
   * `forRelease` reads amounts through `claimedAmount`, so a limit added since
   * the claim was made yields a spec rather than throwing; `claimedKeys` then
   * keeps it out of the release.
   */
  public getLimitSpecs(
    grantId: string | undefined,
    slotId: string,
    cost: RequestCost,
    forRelease = false
  ): MultiLimitSpec[] {
    const amountFor = (name: string) =>
      forRelease ? claimedAmount(cost, name) : costFor(cost, name);
    const specs: MultiLimitSpec[] = [];
    for (const limit of this.rateLimit) {
      if (limit.type === "requestLimit") {
        specs.push({
          kind: "tokenBucket",
          key: this.getBucketKey(limit.name, grantId),
          freezeKey: this.getFreezeStateKey(grantId),
          cost: amountFor(limit.name),
          config: this.bucketConfig(limit),
        });
      } else if (limit.type === "concurrencyLimit") {
        specs.push({
          kind: "concurrency",
          key: this.getSlotKey(limit.name, grantId),
          freezeKey: this.getFreezeStateKey(grantId),
          cost: amountFor(limit.name),
          config: {
            maxConcurrency: limit.maxConcurrency,
            requestTtl: this.requestTtl,
          },
          slotId,
        });
      }
    }
    return specs;
  }

  private getBucketKey(name: string, grantId?: string): string {
    return `${this.getRateLimitKey(grantId)}:${name}`;
  }

  private getSlotKey(name: string, grantId?: string): string {
    return `${this.getConcurrencyKey(grantId)}:${name}`;
  }

  private bucketConfig(limit: NamedRateLimitData & { type: "requestLimit" }) {
    return {
      maxTokens: limit.maxTokens,
      tokensToAdd: limit.tokensToAdd,
      interval: limit.interval,
    };
  }

  // --------------------------------------------------------------- admission

  /**
   * Grant-scoped requests on a metered client are excluded: the queue this
   * path checks is client-level, and a grant-isolated tenant's turn is decided
   * by the scan in `processRequests`.
   */
  protected async tryAdmitImmediately(
    request: RequestMetadata
  ): Promise<boolean> {
    if (!this.metered) {
      // One atomic operation, not two reads: a peer enqueueing between a separate
      // "queue empty?" and "frozen?" would be overtaken by the arrival that read
      // first, which the backend contract forbids. A client with no budget does
      // still queue — a 5xx freezes it, and that backlog is still draining when
      // this path reopens. Grant traffic included: with no budget to scope there
      // is no turn for the grant scan to decide, only the grant's own freeze.
      return this.backend.tryAdmitNoLimit(
        this.getQueueKey(),
        this.getFreezeStateKey(request.grantId)
      );
    }
    if (request.grantId) return false;
    const cost = request.cost ?? 1;
    const sole = this.sole;

    if (sole?.type === "requestLimit") {
      const key = this.getBucketKey(sole.name);
      request.claimedKeys = [key];
      const config = this.bucketConfig(sole);
      return this.backend.tryAdmitImmediately(
        this.getQueueKey(),
        key,
        this.getFreezeStateKey(),
        costFor(cost, sole.name),
        config,
        undefined,
        this.markFor(request, config)
      );
    }
    if (sole?.type === "concurrencyLimit") {
      const key = this.getSlotKey(sole.name);
      request.claimedKeys = [key];
      return this.backend.tryAdmitConcurrency(
        this.getQueueKey(),
        key,
        this.getFreezeStateKey(),
        costFor(cost, sole.name),
        this.getSlotId(request),
        { maxConcurrency: sole.maxConcurrency, requestTtl: this.requestTtl }
      );
    }

    const tryAdmit = this.backend.tryAdmitMultiLimit;
    if (!tryAdmit) return false;
    const specs = this.withMarks(
      request,
      this.getLimitSpecs(undefined, this.getSlotId(request), cost)
    );
    request.claimedKeys = specs.map((spec) => spec.key);
    return tryAdmit.call(
      this.backend,
      this.getQueueKey(),
      [this.getFreezeStateKey()],
      specs
    );
  }

  /**
   * Claims every budget, or none of them.
   *
   * All-or-nothing is the backend's, in one operation: claiming them in turn from
   * here would leave the first spent when the second declines, and nothing on the
   * decline path hands back a budget the request never used.
   */
  protected async tryAcquireTurn(
    request: RequestMetadata
  ): Promise<AcquireTurnResult> {
    const { grantId } = request;
    // Nothing to claim, and nothing to check that `canProcessNextRequest` has not
    // already checked. Returning here is what keeps an unmetered client off the
    // backend entirely on its hot path.
    if (!this.metered) return { acquired: true };

    const cost = request.cost ?? 1;

    // Re-checked on the controller, not just at origination: `Request` validates
    // cost where the call is made, but the controller reads it back out of the
    // shared queue, where a replica running older code or anything else with
    // keyspace write access can present a negative cost that `tokens - cost`
    // would add to the bucket. The origination check is policy; this one guards
    // the arithmetic.
    const unusable = unusableCostReason(cost);
    if (unusable) {
      throw new ConfigurationError(
        "invalid_request_cost",
        `Client ${this.name} refused a queued request whose cost is not spendable: ${unusable}`
      );
    }

    // Re-asked rather than cached: see getCostProblem. A budget shrunk below a
    // queued cost would otherwise report "not acquired, wait 0ms" forever.
    const problem = this.getCostProblemForQueue(cost);
    if (problem) throw problem;

    // A 429 can land on another request between admission and here, and sending
    // this one would walk into a door that just closed.
    if (await this.isFrozen(grantId)) {
      throw new ClientFrozenError(this.name);
    }

    const claimed = await this.claim(request, cost);
    if (claimed.error) {
      throw new ConfigurationError(
        "unusable_rate_limit",
        `Client ${this.name} cannot acquire its budgets: ${claimed.error}`
      );
    }
    if (!claimed.acquired) {
      return { acquired: false, waitTime: this.declineWait(claimed.waitTime) };
    }

    // The freeze may have landed during the claim, budgets already spent. Handing
    // them back is what stops a concurrency entry leaking a slot no completion
    // will ever release.
    if (await this.isFrozen(grantId)) {
      await this.releaseClaim(request, cost).catch((error: unknown) => {
        this.logger.error(
          { error },
          `Client ${this.name} | could not release budgets claimed just before a freeze`
        );
      });
      throw new ClientFrozenError(this.name);
    }

    return { acquired: true };
  }

  /** The claim itself, by whichever backend call fits this client's budgets. */
  private async claim(
    request: RequestMetadata,
    cost: RequestCost
  ): Promise<{ acquired: boolean; waitTime?: number; error?: string }> {
    const sole = this.sole;
    const { grantId } = request;

    if (sole?.type === "requestLimit") {
      const key = this.getBucketKey(sole.name, grantId);
      request.claimedKeys = [key];
      const config = this.bucketConfig(sole);
      return this.backend.acquireTokens(
        key,
        costFor(cost, sole.name),
        config,
        this.markFor(request, config)
      );
    }
    if (sole?.type === "concurrencyLimit") {
      const config = {
        maxConcurrency: sole.maxConcurrency,
        requestTtl: this.requestTtl,
      };
      const key = this.getSlotKey(sole.name, grantId);
      request.claimedKeys = [key];
      const slotId = this.getSlotId(request);
      const amount = costFor(cost, sole.name);
      // Claims only while the entry is still `inProgress`, in one operation. The
      // request can finish between the drain loop selecting it and this claim, and
      // admission excludes a request's own id from the occupancy sum — so a claim
      // for an id that just released would succeed, re-create its entry, and leave
      // nothing to release it until the slot TTL.
      const acquireQueued = this.backend.acquireQueuedConcurrency;
      return acquireQueued
        ? acquireQueued.call(
            this.backend,
            key,
            this.getRequestMetadataKey(request.requestId),
            amount,
            slotId,
            config
          )
        : this.backend.acquireConcurrency(key, amount, slotId, config);
    }

    const acquire = this.backend.acquireMultiLimit;
    if (!acquire) {
      throw new ConfigurationError(
        "backend_missing_multi_limit",
        `Client "${this.name}" declares several rate limits, which needs a backend implementing acquireMultiLimit.`
      );
    }
    const specs = this.withMarks(
      request,
      this.getLimitSpecs(grantId, this.getSlotId(request), cost)
    );
    request.claimedKeys = specs.map((spec) => spec.key);
    return acquire.call(this.backend, specs);
  }

  /**
   * The mark a token-bucket claim leaves for a request that will settle, sized
   * to `settleWindow` but never shorter than the bucket's refill from empty —
   * until then the claim still shapes the balance a settlement corrects.
   */
  private markFor(
    request: RequestMetadata,
    config: TokenBucketConfig
  ): ClaimMark | undefined {
    if (!request.settles) return undefined;
    const window = this.requestOptions.settleWindow;
    const windowMs =
      window !== undefined && Number.isFinite(window) && window > 0
        ? window
        : DEFAULT_SETTLE_WINDOW_MS;
    const refillMs =
      Math.ceil(config.maxTokens / config.tokensToAdd) * config.interval;
    return {
      id: this.getSlotId(request),
      ttlSeconds: Math.ceil(Math.max(windowMs, refillMs) / 1000),
    };
  }

  private withMarks(
    request: RequestMetadata,
    specs: MultiLimitSpec[]
  ): MultiLimitSpec[] {
    if (!request.settles) return specs;
    return specs.map((spec) =>
      spec.kind === "tokenBucket"
        ? { ...spec, mark: this.markFor(request, spec.config) }
        : spec
    );
  }

  /**
   * Corrects what an attempt claimed to what its response reported. Runs on the
   * replica that sent the request, against the buckets the attempt claimed from;
   * the backend reads each claim's mark, so a repeat applies nothing.
   *
   * A result that names the wrong limits is logged and dropped rather than
   * thrown: the vendor has already answered, and the caller must not see a
   * failure for a request that succeeded.
   */
  public async settleAttempt(
    request: RequestMetadata,
    settled: RequestCost
  ): Promise<void> {
    const settleTokens = this.backend.settleTokens;
    if (!settleTokens) return;
    const problem = settledCostProblem(
      settled,
      budgetLimitsFor(this.rateLimit) ?? []
    );
    if (problem) {
      this.logger.error(
        `Client ${this.name} | request ${request.requestId} was not settled: ${problem}`
      );
      return;
    }

    const markId = this.getSlotId(request);
    const specs: SettleSpec[] = [];
    for (const limit of this.rateLimit) {
      if (limit.type !== "requestLimit") continue;
      const key = this.getBucketKey(limit.name, request.grantId);
      if (!wasClaimed(request, key)) continue;
      specs.push({
        key,
        markId,
        actual: costFor(settled, limit.name),
        config: this.bucketConfig(limit),
        maxDebt: limit.maxDebt ?? 0,
      });
    }
    if (!specs.length) return;

    const results = await settleTokens.call(this.backend, specs);
    for (const result of results) {
      if (result.unapplied > 0) {
        this.logger.warn(
          `Client ${this.name} | request ${request.requestId} under-reserved ${result.key} by ${result.unapplied} more than maxDebt lets the balance absorb; raise its cost or maxDebt`
        );
      }
    }
    // A credit is capacity no booked wake-up knows about.
    if (results.some((result) => result.applied < 0)) {
      await this.notifyRequestAdded();
    }
  }

  public async keepAttemptAlive(request: RequestMetadata): Promise<void> {
    const slotId = this.getSlotId(request);
    const specs = this.getLimitSpecs(
      request.grantId,
      slotId,
      request.cost,
      true
    );
    for (const spec of specs) {
      if (!wasClaimed(request, spec.key)) continue;
      if (spec.kind === "concurrency") {
        await this.backend.touchConcurrency?.(spec.key, slotId, 86400);
        continue;
      }
      const mark = this.markFor(request, spec.config);
      if (mark) {
        await this.backend.touchClaimMark?.(spec.key, mark.id, mark.ttlSeconds);
      }
    }
  }

  /**
   * A third of the slot TTL, so two ticks can be lost before the reaper takes
   * the slot.
   */
  public streamKeepAliveMs(): number {
    return this.requestTtl / 3;
  }

  /**
   * Hands back a claim, by the call matching the one that made it.
   *
   * Only to the budgets the attempt claimed from: a limit renamed or added since
   * would otherwise be refunded tokens it never gave out. A slot ledger the
   * current limits no longer name is still released, and a bucket they no longer
   * name keeps the spend — losing a token is the safe direction.
   */
  private async releaseClaim(
    request: RequestMetadata,
    cost: RequestCost
  ): Promise<void> {
    const slotId = this.getSlotId(request);
    const specs = this.getLimitSpecs(request.grantId, slotId, cost, true);
    await this.releaseStaleSlots(request, specs, slotId);
    const claimed = specs.filter((spec) => wasClaimed(request, spec.key));
    if (claimed.length === 0) return;

    const sole = this.sole;
    if (sole?.type === "requestLimit") {
      if (!this.backend.refundTokens) return;
      await this.backend.refundTokens(
        claimed[0].key,
        claimed[0].cost,
        sole.maxTokens,
        this.getFreezeStateKey(request.grantId)
      );
      return;
    }
    if (sole?.type === "concurrencyLimit") {
      await this.backend.releaseConcurrency(claimed[0].key, slotId);
      return;
    }
    // The freeze key always goes along: whether the standing freeze emptied
    // the buckets is recorded on the freeze itself (`emptiesBucket`), and the
    // limits in force now may not be the ones in force when it was armed.
    await this.backend.releaseMultiLimit?.(claimed);
  }

  /** Whether a freeze empties a bucket here; see `handleFreezeOwnTypeRequests`. */
  private emptiedOnFreeze(): boolean {
    return (
      this.rateLimit.filter((limit) => limit.type === "requestLimit").length ===
      1
    );
  }

  /**
   * Releases the slots this attempt holds in ledgers the client's current limits
   * no longer name — what a rebuild or `rateLimitChange` that renamed or dropped
   * a concurrency limit mid-flight leaves behind.
   */
  private async releaseStaleSlots(
    request: RequestMetadata,
    specs: MultiLimitSpec[],
    slotId: string
  ): Promise<void> {
    if (!request.claimedKeys) return;
    const current = new Set(specs.map((spec) => spec.key));
    const concurrencyPrefix = `${this.getConcurrencyKey(request.grantId)}:`;
    for (const key of request.claimedKeys) {
      if (current.has(key) || !key.startsWith(concurrencyPrefix)) continue;
      await this.backend.releaseConcurrency(key, slotId);
    }
  }

  /**
   * How long before a decline is worth repeating.
   *
   * A concurrency budget declines with no deadline of its own — a slot is freed by
   * a completion, which pokes this loop directly — so the slot TTL stands in as the
   * latest moment capacity is guaranteed to exist. Without it the caller books a
   * 1ms wake-up and spins.
   */
  private declineWait(waitTime: number | undefined): number {
    if (waitTime !== undefined && waitTime > 0) return waitTime;
    const hasConcurrency = this.rateLimit.some(
      (limit) => limit.type === "concurrencyLimit"
    );
    return hasConcurrency ? this.requestTtl : (waitTime ?? 0);
  }

  /** True while any budget here is a slot ledger rather than a spend. */
  protected claimsReleasableCapacity(): boolean {
    return this.rateLimit.some((limit) => limit.type === "concurrencyLimit");
  }

  /** Hands back everything a claimed-but-never-dispatched attempt took. */
  protected async refundUnusedAdmission(
    request: RequestMetadata
  ): Promise<void> {
    if (!this.metered) return;
    await this.releaseClaim(request, request.cost ?? 1);
  }

  // -------------------------------------------------------------- completion

  /**
   * Releases the concurrency budgets. Tokens are not returned: they were spent on
   * a request that ran, which is what spending means.
   */
  protected async handleOwnTypeRequestDone(data: RequestDoneData) {
    const concurrencyPrefix = `${this.getConcurrencyKey(data.grantId)}:`;
    const heldSlots = data.claimedKeys
      ? data.claimedKeys.some((key) => key.startsWith(concurrencyPrefix))
      : this.rateLimit.some((limit) => limit.type === "concurrencyLimit");
    if (!heldSlots) return;
    // A retry's payload is built after `incrementRetries`, so `data.retries` names
    // the next attempt while the slot being given back belongs to the one that
    // just ran. `retry` and the increment are biconditional, so the adjustment
    // applies exactly when it should.
    //
    // Abandonment is the one caller that reports `retry: false` on a request whose
    // counter was already incremented, so it releases a slot id for an attempt
    // that never reached admission. Harmless only because the ledger is a set and
    // release is delete-by-member: anything that makes release arithmetic rather
    // than idempotent must pass the abandoned attempt's true retry count.
    const attempt =
      data.responseStatus === "retry"
        ? { ...data, retries: Math.max(0, data.retries - 1) }
        : data;
    const slotId = this.getSlotId(attempt);
    const specs = this.getLimitSpecs(data.grantId, slotId, data.cost, true);
    await this.releaseStaleSlots(data, specs, slotId);
    for (const spec of specs) {
      if (spec.kind !== "concurrency" || !wasClaimed(data, spec.key)) continue;
      await this.backend.releaseConcurrency(spec.key, slotId);
    }
    this.logger.debug(
      `Request ${data.requestId} released concurrency slot for ${this.name}`
    );
    // A freed slot is the only thing a queued request may be waiting for, and
    // nothing else would tell the admission loop it exists.
    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed after releasing a slot`
      );
    });
  }

  /**
   * Empties the bucket only when there is exactly one to empty.
   *
   * A 429 proves the client's picture of the vendor was wrong, and with one
   * `requestLimit` that says which bucket is wrong. With several the response does
   * not say which limit was breached, and zeroing a per-day bucket over a
   * per-second breach would stand the client down until tomorrow — so the freeze
   * window is the whole stand-down there, floored at the shortest refill interval
   * by `getFreezeBaseTime` below. Counted among `requestLimit`s only: a `noLimit`
   * placeholder or a concurrency cap beside one bucket leaves no doubt about it.
   */
  protected async handleFreezeOwnTypeRequests(grantId?: string): Promise<void> {
    if (!this.emptiedOnFreeze()) return;
    const bucket = this.rateLimit.find(
      (limit) => limit.type === "requestLimit"
    )!;
    // Keyed by grant so an isolated grant's freeze empties its own bucket rather
    // than the client-level one.
    const key = this.getBucketKey(bucket.name, grantId);
    if (this.backend.emptyTokenBucket) {
      await this.backend.emptyTokenBucket(key, this.bucketConfig(bucket));
      return;
    }
    await this.backend.hset(key, {
      tokens: 0,
      lastUpdate: await this.backend.now(),
    });
  }

  /**
   * The refill interval floors this, but only for an actual rate limit: after a
   * 429 the buckets are empty, so retrying sooner is pointless. A 5xx leaves them
   * untouched, and flooring there would stop the fleet for a window over one bad
   * response.
   */
  protected getRetryBackoffBaseTime(isRateLimited = false): number {
    const base = this.retryOptions.retryBackoffBaseTime;
    const interval = shortestRefillInterval(this.rateLimit);
    if (!isRateLimited || interval === undefined) return base;
    return Math.max(interval, base);
  }

  /**
   * A 429's freeze is not over until a budget can refill, so the shortest refill
   * interval floors it — the shortest because it is the soonest anything here can
   * hand out a token at all, where the longest would pause the fleet for a day
   * over a per-second refusal. A 5xx or dropped connection says nothing about
   * the budget, so its freeze takes the plain back-off, as its retry does.
   *
   * Through `super`, never `this.retryOptions`: `Math.max(interval, NaN)` is NaN,
   * which serialises as `null` and reads as falsy at the arming gate, so a raw
   * read arms no freeze at all for an unset `Number(process.env.BACKOFF)`.
   */
  protected getFreezeBaseTime(isRateLimited = false): number {
    const base = super.getFreezeBaseTime();
    const interval = shortestRefillInterval(this.rateLimit);
    if (!isRateLimited || interval === undefined) return base;
    return Math.max(interval, base);
  }

  /** Only a 429, and only where there is exactly one bucket to empty. */
  protected freezeEmptiesBucket(isRateLimited: boolean): boolean {
    return isRateLimited && this.emptiedOnFreeze();
  }

  // ------------------------------------------------------------------- stats

  protected async getRateLimitStats(): Promise<NamedRateLimitStats[]> {
    const limits: NamedRateLimitStats[] = [];
    for (const limit of this.rateLimit) {
      if (limit.type !== "requestLimit") {
        limits.push(limit);
        continue;
      }
      const state = await this.backend.getTokenBucketState(
        this.getBucketKey(limit.name),
        this.bucketConfig(limit)
      );
      limits.push({ ...limit, tokens: state.tokens });
    }
    return limits;
  }

  /**
   * Clears no fleet-wide state. Even a final removal can race requests still
   * running on peers, and deleting their slots would admit over the cap if the
   * name were re-added before they finished. The keys already expire, and
   * ordinary completion and slot TTL cleanup reclaim them safely.
   */
  protected handleDestroy(): void {}
}

/** True unless the attempt recorded its claims and this key is not among them. */
/** See `RequestOptions.settleWindow`. */
const DEFAULT_SETTLE_WINDOW_MS = 900_000;

function wasClaimed(request: RequestMetadata, key: string): boolean {
  return request.claimedKeys?.includes(key) ?? true;
}

/**
 * Refuses to build a client whose budgets the backend cannot claim atomically.
 *
 * Only a client with more than one budget needs the multi-limit methods; a single
 * budget uses the calls every backend has always had. They are optional on
 * `DianemoBackend` so a backend written against an earlier version still
 * satisfies the interface, and falling back to claiming one budget at a time
 * would be worse than refusing: every decline would leak whatever the earlier
 * budgets had already handed over.
 */
function assertBackendCanClaim(
  backend: ClientConstructorData["backend"],
  clientName: string,
  rateLimit: NamedRateLimitData[]
): void {
  if (soleRateLimit(rateLimit)) return;
  const missing = (["acquireMultiLimit", "releaseMultiLimit"] as const).filter(
    (method) => !backend[method]
  );
  if (missing.length === 0) return;
  throw new ConfigurationError(
    "backend_missing_multi_limit",
    `Client "${clientName}" declares ${rateLimit.length} rate limits, which needs a backend implementing acquireMultiLimit and releaseMultiLimit. This backend lacks ${missing.join(
      " and "
    )}, and claiming the budgets one at a time would leak the ones claimed before a decline.`
  );
}

export default MeteredClient;
