import type { AcquireMultiLimitResult, MultiLimitSpec } from "@dianemo/core";
import { claimMarkKey, normalizeStateTtlSeconds } from "@dianemo/core";
import { parseLuaNumber } from "../luaNumber.js";
import type { Ctx } from "../context.js";

/**
 * Several budgets claimed as one operation. Twin of the memory backend's
 * several-budgets section.
 */
export function multiLimitOps(ctx: Ctx) {
  return {
    async acquireMultiLimit(
      specs: MultiLimitSpec[]
    ): Promise<AcquireMultiLimitResult> {
      const { keys, payload } = encodeSpecs(specs, { includeMarks: true });
      const result = await ctx.runVariadic(
        "dianemoAcquireMultiLimit",
        keys,
        JSON.stringify(payload),
        86400
      );
      const parsed = JSON.parse(result as string) as AcquireMultiLimitResult;
      if (parsed.waitTime !== undefined) {
        parsed.waitTime = parseLuaNumber(parsed.waitTime);
      }
      return parsed;
    },

    async releaseMultiLimit(specs: MultiLimitSpec[]): Promise<void> {
      if (specs.length === 0) return;
      const { keys, payload } = encodeSpecs(specs, { includeFreezeKeys: true });
      await ctx.runVariadic(
        "dianemoReleaseMultiLimit",
        keys,
        JSON.stringify(payload)
      );
    },

    async tryAdmitMultiLimit(
      queueKey: string,
      freezeKeys: string[],
      specs: MultiLimitSpec[],
      ttl = 86400
    ): Promise<boolean> {
      // Normalised here rather than left to the script, which spends the budgets
      // before its EXPIRE: a TTL Redis rejects would raise with them already
      // claimed and nothing on this path to hand them back.
      const ttlSeconds = normalizeStateTtlSeconds(ttl);
      const index = new KeyIndex([queueKey]);
      const freezeIndices = freezeKeys.map((key) => index.of(key));
      const encoded = encodeSpecs(specs, { index, includeMarks: true });
      const result = await ctx.runVariadic(
        "dianemoTryAdmitMultiLimit",
        index.keys,
        JSON.stringify({
          specs: encoded.payload,
          freezeKeys: freezeIndices,
        }),
        ttlSeconds
      );
      return result === 1;
    },
  };
}

/**
 * Assigns each distinct key one 1-based `KEYS` position.
 *
 * Deduplicating matters beyond size: the same key appearing twice in `KEYS` is
 * what a cross-slot check in Redis Cluster would count twice, and the scripts
 * address budgets by index, so one position per key keeps the two views of a
 * budget identical.
 */
export class KeyIndex {
  readonly keys: string[];
  private positions = new Map<string, number>();

  constructor(initial: string[] = []) {
    this.keys = [];
    for (const key of initial) this.of(key);
  }

  of(key: string): number {
    const existing = this.positions.get(key);
    if (existing !== undefined) return existing;
    this.keys.push(key);
    const position = this.keys.length;
    this.positions.set(key, position);
    return position;
  }
}

/** One JSON object per budget, addressing its key by `KEYS` position. */
type SpecPayload = {
  kind: MultiLimitSpec["kind"];
  k: number;
  /** What the request spends against this budget. */
  c: number | string;
  /** The claim mark's `KEYS` position, when the claim asked for one. */
  mk?: number;
  /** The claim mark's lifetime in seconds. */
  mt?: number;
  fk?: number;
  maxTokens?: number | string;
  tokensToAdd?: number | string;
  interval?: number | string;
  maxConcurrency?: number | string;
  requestTtl?: number | string;
  slotId?: string;
};

/**
 * A number as JSON can carry it. JSON has no NaN or Infinity, so
 * `JSON.stringify` writes `null`, which `cjson` decodes to userdata that every
 * guard in the scripts lets past. As a string, Lua's `tonumber` reads it back
 * through `strtod` and the guards see what the memory backend sees.
 */
export function wire(value: number): number | string {
  return Number.isFinite(value) ? value : String(value);
}

function encodeSpecs(
  specs: MultiLimitSpec[],
  options: {
    index?: KeyIndex;
    includeFreezeKeys?: boolean;
    includeMarks?: boolean;
  } = {}
): { keys: string[]; payload: SpecPayload[] } {
  const index = options.index ?? new KeyIndex();
  const payload = specs.map((spec): SpecPayload => {
    const base = { kind: spec.kind, k: index.of(spec.key), c: wire(spec.cost) };
    // Only the release path consults them, so the admission scripts are not
    // handed keys they would never read — a key in `KEYS` is a key Redis Cluster
    // requires to hash to this slot.
    const fk =
      options.includeFreezeKeys && spec.freezeKey
        ? { fk: index.of(spec.freezeKey) }
        : {};
    if (spec.kind === "tokenBucket") {
      // Claims only: releasing one never writes a mark.
      const mark =
        options.includeMarks && spec.mark
          ? {
              mk: index.of(claimMarkKey(spec.key, spec.mark.id)),
              mt: normalizeStateTtlSeconds(spec.mark.ttlSeconds),
            }
          : {};
      return {
        ...base,
        ...fk,
        ...mark,
        maxTokens: wire(spec.config.maxTokens),
        tokensToAdd: wire(spec.config.tokensToAdd),
        interval: wire(spec.config.interval),
      };
    }
    return {
      ...base,
      ...fk,
      maxConcurrency: wire(spec.config.maxConcurrency),
      requestTtl: wire(spec.config.requestTtl),
      slotId: spec.slotId,
    };
  });
  return { keys: index.keys, payload };
}
