import { normalizeStateTtlSeconds } from "@dianemo/core/backend";
import { parseLuaNumber } from "../luaNumber.js";
import type { Ctx } from "../context.js";
import type {
  AcquireConcurrencyResult,
  ConcurrencyConfig,
} from "@dianemo/core/backend";

/** A reply whose occupancy the script sent as an `exact` string. */
function readConcurrency<T extends { currentConcurrency?: number }>(
  result: unknown
): T {
  const parsed = JSON.parse(result as string) as T;
  if (parsed.currentConcurrency === undefined) return parsed;
  return {
    ...parsed,
    currentConcurrency: parseLuaNumber(parsed.currentConcurrency),
  };
}

/** Concurrency operations. Twin of the memory backend's concurrency section. */
export function concurrencyOps(ctx: Ctx) {
  return {
    async tryAdmitConcurrency(
      queueKey: string,
      concurrencyKey: string,
      freezeKey: string,
      cost: number,
      requestId: string,
      config: ConcurrencyConfig,
      ttl = 86400
    ): Promise<boolean> {
      // Normalised here rather than left to the script, which claims the slot
      // before its EXPIRE: a TTL Redis rejects would raise with the slot held
      // and the caller holding no request to release it by.
      const ttlSeconds = normalizeStateTtlSeconds(ttl);
      const result = await ctx.run(
        "dianemoTryAdmitConcurrency",
        queueKey,
        concurrencyKey,
        freezeKey,
        cost,
        config.maxConcurrency,
        requestId,
        config.requestTtl,
        ttlSeconds
      );
      return result === 1;
    },

    async acquireConcurrency(
      key: string,
      cost: number,
      requestId: string,
      config: ConcurrencyConfig
    ): Promise<AcquireConcurrencyResult> {
      const result = await ctx.run(
        "dianemoAcquireConcurrency",
        key,
        cost,
        config.maxConcurrency,
        requestId,
        config.requestTtl,
        86400 // TTL for the key
      );

      return readConcurrency<AcquireConcurrencyResult>(result);
    },

    async acquireQueuedConcurrency(
      key: string,
      metadataKey: string,
      cost: number,
      requestId: string,
      config: ConcurrencyConfig
    ): Promise<AcquireConcurrencyResult> {
      const result = await ctx.run(
        "dianemoAcquireQueuedConcurrency",
        key,
        metadataKey,
        cost,
        config.maxConcurrency,
        requestId,
        config.requestTtl,
        86400
      );
      return readConcurrency<AcquireConcurrencyResult>(result);
    },

    /**
     * Idempotent: releasing a slot that is already gone is a no-op, which is what
     * makes an abandoned attempt safe to release twice.
     */
    async releaseConcurrency(key: string, requestId: string): Promise<void> {
      await ctx.run("dianemoReleaseConcurrency", key, requestId);
    },

    async touchConcurrency(
      key: string,
      slotId: string,
      ttlSeconds: number
    ): Promise<boolean> {
      const ttl = normalizeStateTtlSeconds(ttlSeconds);
      return (await ctx.run("dianemoTouchConcurrency", key, slotId, ttl)) === 1;
    },

    async getConcurrencyState(
      key: string,
      requestTtl: number
    ): Promise<{ currentConcurrency: number; activeRequests: string[] }> {
      // Refused here so both backends raise the same class for the same input.
      // The script already fails on a non-finite cutoff, but as an ioredis
      // ReplyError from `ZRANGEBYSCORE key '(nan' '+inf'`. See the memory twin.
      if (!Number.isFinite(requestTtl)) {
        throw new RangeError(
          `requestTtl must be a finite number, received ${String(requestTtl)}`
        );
      }
      const result = await ctx.run(
        "dianemoGetConcurrencyState",
        key,
        requestTtl
      );

      return readConcurrency<{
        currentConcurrency: number;
        activeRequests: string[];
      }>(result);
    },

    /**
     * Never call this from one replica while peers may still be running: it drops
     * their slots too, and the cap stops holding until they finish.
     */
    async clearConcurrency(key: string): Promise<void> {
      // One `DEL` naming both, so no acquire can land between the two and leave
      // a slot whose cost entry is gone.
      await ctx.redis.del(key, `${key}:costs`);
    },
  };
}
