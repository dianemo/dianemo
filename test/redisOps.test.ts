import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { redisBackend } from "../packages/backend-redis/src/index.js";
import { Redis } from "ioredis";
import type {
  ConcurrencyConfig,
  DianemoBackend,
  QueuedRequest,
  TokenBucketConfig,
} from "../packages/core/src/backend/types.js";

/**
 * What the Redis backend's TypeScript layer hands Redis, and the connection it
 * owns.
 *
 * The conformance suite asks whether both backends agree about behaviour. This
 * one asks the narrower question underneath it: whether an argument reaches
 * Redis in a form Redis accepts, and whether it is checked before or after the
 * write it belongs to. A script that writes and then fails at its `EXPIRE`
 * leaves state no caller asked for, so "it throws" is never the whole assertion
 * here — each of these also asserts what Redis holds afterwards.
 *
 * Skipped unless REDIS_URL is set:
 *   REDIS_URL=redis://localhost:6402 npx vitest run test/redisOps.test.ts
 */

const REDIS_URL = process.env.REDIS_URL;
const describeIfRedis = REDIS_URL ? describe : describe.skip;

/**
 * SHARED with `clientTypes/requestLimit.test`, which reaches db5 through
 * `harnesses(5)`.
 *
 * Both flush it, so what keeps them apart is `fileParallelism: false` in
 * `vitest.config.ts` running one file at a time — not the key prefixes, which a
 * flush ignores. See the hazard `clientTypes/harness.ts` documents.
 */
const TEST_DB = 5;

const BUCKET: TokenBucketConfig = {
  maxTokens: 100,
  tokensToAdd: 1,
  interval: 1000,
};
/** No refill, so a reported balance is the stored one and nothing else. */
const FROZEN_BUCKET: TokenBucketConfig = {
  maxTokens: 100,
  tokensToAdd: 1,
  interval: 0,
};
const CONCURRENCY: ConcurrencyConfig = { maxConcurrency: 5, requestTtl: 60000 };

/** Above `REDIS_MAX_TTL_SECONDS`, so `normalizeTtlSeconds` refuses it. */
const OUT_OF_RANGE = 9.3e15;

function queued(requestId: string): QueuedRequest {
  return {
    requestId,
    clientName: "c",
    requestName: "r",
    status: "pending",
    priority: 0,
    cost: 1,
    retries: 0,
    timestamp: Date.now(),
    isThawRequest: false,
    ownerId: "owner",
  };
}

describeIfRedis("redis backend — ops layer", () => {
  let redis: Redis;
  const opened: DianemoBackend[] = [];
  const connections: Redis[] = [];

  /** A backend on the shared connection, closed after the test that made it. */
  function open(): DianemoBackend {
    const backend = redisBackend(redis);
    opened.push(backend);
    return backend;
  }

  /** A backend on its own connection carrying an ioredis `keyPrefix`. */
  function openPrefixed(keyPrefix: string): DianemoBackend {
    const connection = new Redis(`${REDIS_URL}/${TEST_DB}`, { keyPrefix });
    connections.push(connection);
    const backend = redisBackend(connection);
    opened.push(backend);
    return backend;
  }

  /** The pub/sub duplicate, which the class keeps private. */
  function listenerOf(backend: DianemoBackend): Redis | null {
    return (backend as unknown as { listener: Redis | null }).listener;
  }

  beforeAll(async () => {
    redis = new Redis(`${REDIS_URL}/${TEST_DB}`);
    await redis.flushdb();
  });

  afterEach(async () => {
    await Promise.all(opened.splice(0).map((backend) => backend.close()));
    for (const connection of connections.splice(0)) connection.disconnect();
    await redis.flushdb();
  });

  afterAll(async () => {
    await redis.quit();
  });

  // ------------------------------------------------- TTLs the scripts are given

  describe("TTL arguments handed to a script", () => {
    it("queues a request with a fractional TTL rather than orphaning it", async () => {
      const backend = open();

      await expect(
        backend.addRequest("q:frac", "q:frac:m", queued("a"), 86400.5)
      ).resolves.toBe(true);
      expect(await redis.zcard("q:frac")).toBe(1);
      expect(await redis.ttl("q:frac:m:a")).toBe(86401);
    });

    it("leaves nothing queued when the queue TTL is out of range", async () => {
      const backend = open();

      await expect(
        backend.addRequest("q:bad", "q:bad:m", queued("a"), OUT_OF_RANGE)
      ).rejects.toThrow(RangeError);
      expect(await redis.zcard("q:bad")).toBe(0);
      expect(await redis.exists("q:bad:m:a")).toBe(0);
    });

    it("admits on a fractional bucket TTL rather than spending the tokens and throwing", async () => {
      const backend = open();

      await expect(
        backend.tryAdmitImmediately(
          "q:tb",
          "tb:frac",
          "fz:tb",
          1,
          BUCKET,
          86400.5
        )
      ).resolves.toBe(true);
      expect(await redis.hget("tb:frac", "tokens")).toBe("99");
      expect(await redis.ttl("tb:frac")).toBe(86401);
    });

    it("spends no tokens when the bucket TTL is out of range", async () => {
      const backend = open();

      await expect(
        backend.tryAdmitImmediately(
          "q:tb2",
          "tb:bad",
          "fz:tb2",
          1,
          BUCKET,
          OUT_OF_RANGE
        )
      ).rejects.toThrow(RangeError);
      expect(await redis.exists("tb:bad")).toBe(0);
    });

    it("admits on a fractional concurrency TTL rather than holding a slot and throwing", async () => {
      const backend = open();

      await expect(
        backend.tryAdmitConcurrency(
          "q:cc",
          "cc:frac",
          "fz:cc",
          1,
          "req-a",
          CONCURRENCY,
          86400.5
        )
      ).resolves.toBe(true);
      expect(await redis.ttl("cc:frac")).toBe(86401);
    });

    it("holds no concurrency slot when its TTL is out of range", async () => {
      const backend = open();

      await expect(
        backend.tryAdmitConcurrency(
          "q:cc2",
          "cc:bad",
          "fz:cc2",
          1,
          "req-a",
          CONCURRENCY,
          OUT_OF_RANGE
        )
      ).rejects.toThrow(RangeError);
      const state = await backend.getConcurrencyState("cc:bad", 60000);
      expect(state.currentConcurrency).toBe(0);
    });

    it("refuses a non-finite requestTtl the way the memory twin does", async () => {
      const backend = open();

      await expect(backend.getConcurrencyState("cc:nan", NaN)).rejects.toThrow(
        RangeError
      );
      await expect(
        backend.getConcurrencyState("cc:inf", Infinity)
      ).rejects.toThrow(
        "requestTtl must be a finite number, received Infinity"
      );
    });
  });

  // ---------------------------------------------------------- expire immediately

  describe("a TTL at or below zero", () => {
    it("deletes the key on set instead of leaving the old value", async () => {
      const backend = open();
      await redis.set("ex:zero", "old");

      await backend.set("ex:zero", "new", 0);

      expect(await redis.get("ex:zero")).toBeNull();
    });

    it("deletes the key on a negative set TTL", async () => {
      const backend = open();
      await redis.set("ex:neg", "old");

      await backend.set("ex:neg", "new", -5);

      expect(await redis.get("ex:neg")).toBeNull();
    });

    it("deletes the key in a batch without failing its siblings", async () => {
      const backend = open();
      await redis.set("ex:batch", "old");

      await backend.batch(
        [
          { op: "set", key: "ex:batch", value: "new", ttlSeconds: 0 },
          { op: "set", key: "ex:sibling", value: "kept" },
        ],
        { atomic: true }
      );

      expect(await redis.get("ex:batch")).toBeNull();
      expect(await redis.get("ex:sibling")).toBe("kept");
    });
  });

  // --------------------------------------------------------------- empty hset

  describe("hset with no fields", () => {
    it("writes nothing instead of failing on arity", async () => {
      const backend = open();

      await expect(backend.hset("h:empty", {})).resolves.toBeUndefined();
      expect(await redis.exists("h:empty")).toBe(0);
    });

    it("writes nothing when a TTL is given too", async () => {
      const backend = open();

      await expect(
        backend.hset("h:empty:ttl", {}, 60)
      ).resolves.toBeUndefined();
      expect(await redis.exists("h:empty:ttl")).toBe(0);
    });

    it("still refuses a TTL the backend cannot store", async () => {
      const backend = open();

      await expect(backend.hset("h:empty:bad", {}, Infinity)).rejects.toThrow(
        RangeError
      );
    });

    it("still moves an existing hash's expiry", async () => {
      const backend = open();
      await redis.hset("h:bump", "f", "v");
      await redis.expire("h:bump", 30);

      await backend.hset("h:bump", {}, 600);

      expect(await redis.ttl("h:bump")).toBeGreaterThan(500);
      expect(await redis.hgetall("h:bump")).toEqual({ f: "v" });
    });

    it("still moves an existing hash's expiry from a batch", async () => {
      const backend = open();
      await redis.hset("h:bump:batch", "f", "v");
      await redis.expire("h:bump:batch", 30);

      await backend.batch(
        [{ op: "hset", key: "h:bump:batch", fields: {}, ttlSeconds: 600 }],
        { atomic: true }
      );

      expect(await redis.ttl("h:bump:batch")).toBeGreaterThan(500);
      expect(await redis.hgetall("h:bump:batch")).toEqual({ f: "v" });
    });

    it("leaves the rest of an atomic batch applied", async () => {
      const backend = open();

      await backend.batch(
        [
          { op: "hset", key: "h:batch", fields: {} },
          { op: "set", key: "h:batch:sibling", value: "kept" },
        ],
        { atomic: true }
      );

      expect(await redis.get("h:batch:sibling")).toBe("kept");
    });
  });

  // -------------------------------------------------------------------- locks

  describe("acquireLock", () => {
    it("rounds a fractional millisecond TTL up rather than refusing it", async () => {
      const backend = open();

      await expect(backend.acquireLock("l:frac", "tok", 1500.5)).resolves.toBe(
        true
      );
      const pttl = await redis.pttl("l:frac");
      expect(pttl).toBeGreaterThan(0);
      expect(pttl).toBeLessThanOrEqual(1501);
    });

    it("refuses a non-positive or non-finite TTL before taking the key", async () => {
      const backend = open();

      await expect(backend.acquireLock("l:zero", "tok", 0)).rejects.toThrow(
        RangeError
      );
      await expect(backend.acquireLock("l:neg", "tok", -1)).rejects.toThrow(
        RangeError
      );
      await expect(
        backend.acquireLock("l:inf", "tok", Infinity)
      ).rejects.toThrow(RangeError);
      await expect(backend.acquireLock("l:nan", "tok", NaN)).rejects.toThrow(
        RangeError
      );
      expect(await redis.exists("l:zero", "l:neg", "l:inf", "l:nan")).toBe(0);
    });
  });

  // ------------------------------------------------------------------ pub/sub

  describe("subscribe", () => {
    it("keeps one message listener however many times it is called", async () => {
      const backend = open();

      for (let i = 0; i < 25; i++) {
        await backend.subscribe([`ps:many:${i}`], () => {});
      }

      expect(listenerOf(backend)?.listenerCount("message")).toBe(1);
    });

    it("delivers once to a handler subscribed twice", async () => {
      const backend = open();
      const seen: string[] = [];
      const handler = (_channel: string, message: string) => {
        seen.push(message);
      };

      await backend.subscribe(["ps:dup"], handler);
      await backend.subscribe(["ps:dup"], handler);
      await backend.publish("ps:dup", "once");
      await new Promise((r) => setTimeout(r, 150));

      expect(seen).toEqual(["once"]);
    });

    it("gives a second subscriber only the channels it asked for", async () => {
      const backend = open();
      const first: string[] = [];
      const second: string[] = [];

      await backend.subscribe(["ps:a"], (channel) => first.push(channel));
      await backend.subscribe(["ps:b"], (channel) => second.push(channel));
      await backend.publish("ps:a", "x");
      await new Promise((r) => setTimeout(r, 150));

      expect(first).toEqual(["ps:a"]);
      expect(second).toEqual([]);
    });

    it("keeps delivering after its pub/sub connection is killed and reconnects", async () => {
      const backend = open();
      const seen: string[] = [];
      await backend.subscribe(["ps:killed"], (_c, m) => seen.push(m));

      await redis.call("CLIENT", "KILL", "TYPE", "pubsub");
      await new Promise((r) => setTimeout(r, 500));
      await backend.publish("ps:killed", "after-kill");
      await new Promise((r) => setTimeout(r, 200));

      expect(seen).toEqual(["after-kill"]);
    });
  });

  // ------------------------------------------- a connection's ioredis keyPrefix

  describe("a connection with an ioredis keyPrefix", () => {
    it("finds a live peer instance on a connection with an ioredis keyPrefix", async () => {
      const backend = openPrefixed("app:");
      await backend.batch([
        {
          op: "set",
          key: "ns:instance:peer",
          value: '{"id":"peer"}',
          ttlSeconds: 60,
        },
        { op: "sadd", key: "ns:instances", member: "peer" },
      ]);
      expect(await redis.get("app:ns:instance:peer")).toBe('{"id":"peer"}');

      const peers = await backend.getInstances(
        "ns:instances",
        "ns:instance:",
        "me"
      );

      expect(peers).toEqual([{ id: "peer", data: '{"id":"peer"}' }]);
      expect(await redis.smembers("app:ns:instances")).toEqual(["peer"]);
    });

    it("keeps a grant whose freeze still stands in the frozen-grants set on a connection with an ioredis keyPrefix", async () => {
      const backend = openPrefixed("app:");
      const now = await backend.now();
      await backend.setFreezeState(
        "ns:c:grant:g1:freezeState",
        now + 60_000,
        1
      );
      await backend.sadd("ns:c:frozenGrants", "g1");

      const cleaned = await backend.cleanupStaleFrozenGrants(
        "ns:c:frozenGrants",
        "ns:c:queue",
        "ns:c:meta",
        "ns:c:grant:"
      );

      expect(cleaned).toBe(0);
      expect(await backend.smembers("ns:c:frozenGrants")).toEqual(["g1"]);
    });

    it("does not deliver a message published under another ioredis keyPrefix", async () => {
      const a = openPrefixed("appA:");
      const b = openPrefixed("appB:");
      const seen: string[] = [];
      await b.subscribe(["requestHandler:requestDone"], (_c, m) =>
        seen.push(m)
      );

      await a.publish("requestHandler:requestDone", "from-A");
      await a.batch([
        {
          op: "publish",
          channel: "requestHandler:requestDone",
          message: "batched-from-A",
        },
      ]);
      await new Promise((r) => setTimeout(r, 150));

      expect(seen).toEqual([]);
    });

    it("delivers under its own ioredis keyPrefix with the channel name unprefixed", async () => {
      const publisher = openPrefixed("appA:");
      const subscriber = openPrefixed("appA:");
      const seen: Array<[string, string]> = [];
      await subscriber.subscribe(["requestHandler:requestDone"], (c, m) =>
        seen.push([c, m])
      );

      await publisher.publish("requestHandler:requestDone", "direct");
      await publisher.batch([
        {
          op: "publish",
          channel: "requestHandler:requestDone",
          message: "batched",
        },
      ]);
      await new Promise((r) => setTimeout(r, 150));

      expect(seen).toEqual([
        ["requestHandler:requestDone", "direct"],
        ["requestHandler:requestDone", "batched"],
      ]);
      expect(await redis.pubsub("CHANNELS", "appA:*")).toEqual([
        "appA:requestHandler:requestDone",
      ]);
    });

    it("stamps no epoch on a bucket claimed without a mark on a prefixed connection", async () => {
      await open().acquireTokens("plain:tb", 1, BUCKET);
      expect(await redis.hget("plain:tb", "epoch")).toBeNull();

      await openPrefixed("app:").acquireTokens("tb", 1, BUCKET);
      expect(await redis.hget("app:tb", "epoch")).toBeNull();
    });

    it("does not write to the key named by the bare ioredis prefix when a claim asks for no mark", async () => {
      await redis.hset("app:", "owner", "someone-else");
      const backend = openPrefixed("app:");

      await backend.acquireTokens("tb", 1, BUCKET);
      await backend.tryAdmitImmediately("q", "tb", "fz", 1, BUCKET);

      expect(await redis.hgetall("app:")).toEqual({ owner: "someone-else" });
      expect(await redis.ttl("app:")).toBe(-1);
    });

    it("reads no freeze from the key named by the bare ioredis prefix when a refund names none", async () => {
      const backend = openPrefixed("app:");
      const now = await backend.now();
      await redis.hset("app:", {
        frozenUntil: now + 60_000,
        emptiesBucket: "1",
      });
      await backend.acquireTokens("tb", 5, BUCKET);

      await backend.refundTokens!("tb", 5, BUCKET.maxTokens);

      expect(await redis.hget("app:tb", "tokens")).toBe("100");
    });
  });

  // ---------------------------------------------------------- token bucket read

  describe("getTokenBucketState", () => {
    it("reads the balance and the clock from one snapshot", async () => {
      // A spend forced into the widest window the implementation leaves open:
      // between the two field reads when they are separate commands, and
      // immediately before `EXEC` when they are one transaction. Redis runs a
      // transaction to completion, so only the first ordering can report a
      // pre-spend balance against a post-spend `lastUpdate`.
      const backend = open();
      const key = "tb:snap";
      await backend.acquireTokens(key, 1, FROZEN_BUCKET);

      const realHget = redis.hget.bind(redis);
      const realMulti = redis.multi.bind(redis);
      let spent = false;
      const spendOnce = async () => {
        if (spent) return;
        spent = true;
        await backend.acquireTokens(key, 1, FROZEN_BUCKET);
      };
      const patched = redis as unknown as {
        hget: unknown;
        multi: unknown;
      };
      patched.hget = async (...args: [string, string]) => {
        if (args[1] === "lastUpdate") await spendOnce();
        return realHget(...args);
      };
      patched.multi = (...args: Parameters<typeof realMulti>) => {
        const chain = realMulti(...args);
        const realExec = chain.exec.bind(chain);
        chain.exec = (async () => {
          await spendOnce();
          return realExec();
        }) as typeof chain.exec;
        return chain;
      };

      let state;
      try {
        state = await backend.getTokenBucketState(key, FROZEN_BUCKET);
      } finally {
        patched.hget = realHget;
        patched.multi = realMulti;
      }

      expect(spent).toBe(true);
      const stored = await redis.hmget(key, "tokens", "lastUpdate");
      expect(state.tokens).toBe(Number(stored[0]));
      expect(state.lastUpdate).toBe(Number(stored[1]));
    });
  });

  // ------------------------------------------------- single-command writes

  describe("writes that must not be split", () => {
    /** Records each call to `method` on the shared connection while `fn` runs. */
    async function recording(
      method: "time" | "pipeline" | "del",
      fn: () => Promise<void>
    ): Promise<unknown[][]> {
      const target = redis as unknown as Record<string, unknown>;
      const real = target[method] as (...args: unknown[]) => unknown;
      const calls: unknown[][] = [];
      target[method] = (...args: unknown[]) => {
        calls.push(args);
        return real.apply(redis, args);
      };
      try {
        await fn();
      } finally {
        target[method] = real;
      }
      return calls;
    }

    it("resets a bucket in one script that reads the clock itself", async () => {
      const backend = open();
      const reset = () => backend.resetTokenBucket("tb:reset", 7);
      expect(await recording("time", reset)).toEqual([]);
      expect(await recording("pipeline", reset)).toEqual([]);

      const [tokens, lastUpdate] = await redis.hmget(
        "tb:reset",
        "tokens",
        "lastUpdate"
      );
      expect(tokens).toBe("7");
      expect(Math.abs(Number(lastUpdate) - (await backend.now()))).toBeLessThan(
        1000
      );
      expect(await redis.ttl("tb:reset")).toBeGreaterThan(86_000);
    });

    it("clears a slot ledger and its costs in one DEL", async () => {
      const backend = open();
      await backend.acquireConcurrency("cc:clear", 1, "a", CONCURRENCY);
      const calls = await recording("del", () =>
        backend.clearConcurrency("cc:clear")
      );
      expect(calls).toEqual([["cc:clear", "cc:clear:costs"]]);
      expect(await redis.exists("cc:clear", "cc:clear:costs")).toBe(0);
    });
  });

  // ---------------------------------------------------------------- lifecycle

  describe("close", () => {
    it("drops the handlers a closed subscription was delivering to", async () => {
      // One backend outliving the handler that subscribed through it is the
      // shape `auditRound2`'s `rotate` uses: stop, build another handler, start
      // again. The second subscription must not resurrect the first's handlers.
      const backend = open();
      const stale: string[] = [];
      await backend.subscribe(["ps:reused"], (_c, m) => stale.push(m));

      await backend.close();

      const fresh: string[] = [];
      await backend.subscribe(["ps:reused"], (_c, m) => fresh.push(m));
      await backend.publish("ps:reused", "after");
      await new Promise((r) => setTimeout(r, 150));

      expect(fresh).toEqual(["after"]);
      expect(stale).toEqual([]);
      expect(listenerOf(backend)?.listenerCount("message")).toBe(1);
    });

    it("leaves the caller's connection usable, twice over", async () => {
      const backend = open();
      await backend.subscribe(["ps:mine"], () => {});

      await backend.close();
      await expect(backend.close()).resolves.toBeUndefined();

      expect(redis.status).toBe("ready");
      expect(await redis.ping()).toBe("PONG");
    });
  });
});
