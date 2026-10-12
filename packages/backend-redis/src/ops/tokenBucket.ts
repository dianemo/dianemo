import { KeyIndex, wire } from "./multiLimit.js";
import { parseLuaNumber } from "../luaNumber.js";
import type { Ctx } from "../context.js";
import type {
  AcquireTokensResult,
  ClaimMark,
  SettleResult,
  SettleSpec,
  TokenBucketConfig,
} from "@dianemo/core";
import {
  claimMarkKey,
  normalizeStateTtlSeconds,
  parseFiniteStored,
} from "@dianemo/core";

/**
 * A mark's key, lifetime and presence flag as the scripts take them. With no
 * mark the key slot carries the bucket's own key and the flag says `"0"`: a
 * script's key count is fixed, and ioredis prefixes every `KEYS` entry, so an
 * empty one would arrive naming the key the bare `keyPrefix` spells. Normalised
 * before the script runs, which spends before it writes the mark — a TTL Redis
 * rejects would raise with the budget already gone.
 */
function markArgs(bucketKey: string, mark?: ClaimMark): MarkArgs {
  if (!mark) return [bucketKey, 0, "0"];
  return [
    claimMarkKey(bucketKey, mark.id),
    normalizeStateTtlSeconds(mark.ttlSeconds),
    "1",
  ];
}

/** `[markKey, markTtl, hasMark]`, in the order the scripts read them. */
type MarkArgs = [string, number, "0" | "1"];

/** One entry of the `settleTokens` reply: amounts as `exact` strings. */
interface SettledReply {
  settled: boolean;
  applied: string;
  unapplied: string;
}

/** Token-bucket operations. Twin of the memory backend's token-bucket section. */
export function tokenBucketOps(ctx: Ctx) {
  return {
    /**
     * Attempts to admit a request without queueing. See `tryAdmitImmediately`
     * in `lua/tokenBucket.ts` for the conditions.
     */
    async tryAdmitImmediately(
      queueKey: string,
      bucketKey: string,
      freezeKey: string,
      cost: number,
      config: TokenBucketConfig,
      ttl = 86400,
      mark?: ClaimMark
    ): Promise<boolean> {
      // Normalised here rather than left to the script, which spends the tokens
      // before its EXPIRE: a TTL Redis rejects would raise with the budget
      // already gone and nothing on this path to refund it.
      const ttlSeconds = normalizeStateTtlSeconds(ttl);
      const [markKey, markTtl, hasMark] = markArgs(bucketKey, mark);
      const result = await ctx.run(
        "dianemoTryAdmitImmediately",
        queueKey,
        bucketKey,
        freezeKey,
        markKey,
        cost,
        config.maxTokens,
        config.tokensToAdd,
        config.interval,
        ttlSeconds,
        markTtl,
        hasMark
      );
      return result === 1;
    },

    async acquireTokens(
      key: string,
      cost: number,
      config: TokenBucketConfig,
      mark?: ClaimMark
    ): Promise<AcquireTokensResult> {
      const [markKey, markTtl, hasMark] = markArgs(key, mark);
      const result = await ctx.run(
        "dianemoTokenBucket",
        key,
        markKey,
        cost,
        config.maxTokens,
        config.tokensToAdd,
        config.interval,
        86400, // TTL for the key
        markTtl,
        hasMark
      );

      const parsed = JSON.parse(result as string) as AcquireTokensResult;
      if (parsed.remainingTokens !== undefined) {
        parsed.remainingTokens = parseLuaNumber(parsed.remainingTokens);
      }
      if (parsed.waitTime !== undefined) {
        parsed.waitTime = parseLuaNumber(parsed.waitTime);
      }
      return parsed;
    },

    async getTokenBucketState(
      key: string,
      config: TokenBucketConfig
    ): Promise<{ tokens: number; lastUpdate: number }> {
      // Redis's clock, not this process's. `lastUpdate` was written by whichever
      // replica last refilled the bucket, and every acquire script measures
      // elapsed time against redis TIME — so a reader on a local clock running
      // ahead reports refill the bucket will not actually grant, and one running
      // behind hides refill it will.
      //
      // One MULTI, not concurrent reads: Redis runs a transaction to completion
      // before any other client's command, and separate reads let an
      // `acquireTokens` land between two of them.
      const results = await ctx.redis
        .multi()
        .time()
        .hmget(key, "tokens", "lastUpdate")
        .exec();
      const failed = results?.find(([error]) => error);
      if (failed?.[0]) throw failed[0];
      if (!results) {
        throw new Error(`token bucket read for "${key}" was discarded`);
      }
      const [seconds, micros] = results[0][1] as [string, string];
      const now = Number(seconds) * 1000 + Math.floor(Number(micros) / 1000);
      const [tokens, lastUpdate] = results[1][1] as (string | null)[];

      // The shared parse, not parseFloat: it keeps a fractional balance that
      // truncation would disagree with the memory twin about.
      let currentTokens = parseFiniteStored(
        tokens ?? undefined,
        config.maxTokens
      );
      let currentLastUpdate = parseFiniteStored(lastUpdate ?? undefined, now);

      const elapsed = now - currentLastUpdate;
      if (elapsed > 0 && config.interval > 0) {
        const intervalsElapsed = Math.floor(elapsed / config.interval);
        if (intervalsElapsed > 0) {
          currentTokens = Math.min(
            currentTokens + intervalsElapsed * config.tokensToAdd,
            config.maxTokens
          );
          // Advanced with the credit, as the acquire scripts and the memory twin
          // both do, so a caller deriving progress toward the next refill from
          // it is not a whole interval out.
          currentLastUpdate += intervalsElapsed * config.interval;
        }
      }
      // Matches the clamp in the acquire scripts, so reported stats agree with
      // what the bucket will actually hand out after a budget reduction.
      currentTokens = Math.min(currentTokens, config.maxTokens);

      return { tokens: currentTokens, lastUpdate: currentLastUpdate };
    },

    /**
     * Expires the bucket too, so one reset and then never acquired against does
     * not outlive every other write to it. The memory twin sizes the same 86400
     * through `writeBucket`.
     */
    async resetTokenBucket(key: string, maxTokens: number): Promise<void> {
      await ctx.run("dianemoResetTokenBucket", key, maxTokens);
    },

    async emptyTokenBucket(
      key: string,
      config: TokenBucketConfig
    ): Promise<void> {
      await ctx.run(
        "dianemoEmptyTokenBucket",
        key,
        wire(config.maxTokens),
        wire(config.tokensToAdd),
        wire(config.interval),
        86400
      );
    },

    async settleTokens(specs: SettleSpec[]): Promise<SettleResult[]> {
      // An empty list would come back from cjson as `{}`, not `[]`.
      if (specs.length === 0) return [];
      const index = new KeyIndex();
      const entries = specs.map((spec) => ({
        k: index.of(spec.key),
        mk: index.of(claimMarkKey(spec.key, spec.markId)),
        actual: wire(spec.actual),
        maxTokens: wire(spec.config.maxTokens),
        tokensToAdd: wire(spec.config.tokensToAdd),
        interval: wire(spec.config.interval),
        maxDebt: wire(spec.maxDebt),
      }));
      const result = await ctx.runVariadic(
        "dianemoSettleTokens",
        index.keys,
        JSON.stringify(entries),
        86400
      );
      const parsed = JSON.parse(result as string) as SettledReply[];
      return specs.map((spec, i) => ({
        key: spec.key,
        settled: parsed[i].settled,
        applied: parseLuaNumber(parsed[i].applied),
        unapplied: parseLuaNumber(parsed[i].unapplied),
      }));
    },

    async touchClaimMark(
      bucketKey: string,
      markId: string,
      ttlSeconds: number
    ): Promise<boolean> {
      const ttl = normalizeStateTtlSeconds(ttlSeconds);
      return (
        (await ctx.redis.expire(claimMarkKey(bucketKey, markId), ttl)) === 1
      );
    },

    /**
     * Returns budget claimed for a request that was never dispatched.
     *
     * `freezeKey` is optional to the caller but not to the script: a custom command
     * is registered with a fixed key count, so an absent one is passed as the
     * bucket's own key with a `"0"` flag, for the reason `markArgs` gives.
     */
    async refundTokens(
      key: string,
      cost: number,
      maxTokens: number,
      freezeKey?: string
    ): Promise<void> {
      await ctx.run(
        "dianemoRefundTokens",
        key,
        freezeKey || key,
        cost,
        maxTokens,
        freezeKey ? "1" : "0"
      );
    },
  };
}
