import type { AttemptOutcome } from "../../packages/core/src/request/types.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { harnesses, withHandler } from "./harness.js";
import { createServer, type Server } from "node:http";

/**
 * Settlement through a client: a request reserves an estimate, its response
 * reports what it used, and the bucket ends where the actual usage would have
 * put it. The arithmetic itself is held to both backends by the conformance
 * suite; these cover the path from `settle` to the bucket.
 */

const HOUR = 3_600_000;

/** Answers with the usage named in the query, and fails with the status asked for. */
async function startUsageUpstream() {
  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const status = Number(url.searchParams.get("status") ?? 200);
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({ usage: { output: Number(url.searchParams.get("out")) } })
    );
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/** Reads the output-token count an upstream response reported. */
const reported = (outcome: AttemptOutcome) =>
  (outcome.response?.data as { usage: { output: number } }).usage.output;

describe.each(harnesses(6))("settlement — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUsageUpstream>>;
  beforeAll(async () => {
    upstream = await startUsageUpstream();
  });
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "ai:_:a";
  const OTPM = "ct:requestHandler:ai:_:a:rateLimit:otpm";
  const rpm = {
    name: "rpm",
    type: "requestLimit",
    interval: 1_000,
    tokensToAdd: 10,
    maxTokens: 10,
  };
  const otpmConfig = { maxTokens: 1_000, tokensToAdd: 1_000, interval: HOUR };
  const otpm = (extra: Record<string, unknown> = {}) => ({
    name: "otpm",
    type: "requestLimit",
    ...otpmConfig,
    ...extra,
  });

  const run = (
    rateLimit: Record<string, unknown>[],
    fn: Parameters<typeof withHandler>[3]
  ) =>
    withHandler(
      harness,
      upstream.baseURL,
      [{ name: "ai", rateLimit, retryOptions: { maxRetries: 0 } }],
      fn
    );

  const send = (
    handler: Parameters<Parameters<typeof withHandler>[3]>[0],
    out: number,
    reserve: number,
    extra: Record<string, unknown> = {}
  ) =>
    handler.handleRequest({
      clientName: CLIENT,
      requestName: "ai.messages",
      method: "GET",
      url: `/?out=${out}`,
      cost: { rpm: 1, otpm: reserve },
      settle: (outcome) => ({ rpm: 1, otpm: reported(outcome) }),
      ...extra,
    });

  const balance = async (
    backend: Parameters<Parameters<typeof withHandler>[3]>[1]
  ) => (await backend.getTokenBucketState(OTPM, otpmConfig)).tokens;

  it("charges what the response reports rather than what was reserved", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      await send(handler, 400, 100);
      expect(await balance(backend)).toBe(600);
    });
  }, 30_000);

  it("hands back an over-reservation", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      await send(handler, 50, 500);
      expect(await balance(backend)).toBe(950);
    });
  }, 30_000);

  it("lets the claim stand when settle returns undefined", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      await send(handler, 400, 100, { settle: () => undefined });
      expect(await balance(backend)).toBe(900);
    });
  }, 30_000);

  it("settles a failed attempt with what its error response reports", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      await expect(
        send(handler, 0, 300, { url: "/?out=0&status=400" })
      ).rejects.toThrow();
      expect(await balance(backend)).toBe(1_000);
    });
  }, 30_000);

  it("takes the balance into debt as far as maxDebt allows", async () => {
    await run([rpm, otpm({ maxDebt: 500 })], async (handler, backend) => {
      await send(handler, 1_300, 100);
      expect(await balance(backend)).toBe(-300);
    });
  }, 30_000);

  it("stops at zero without maxDebt", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      await send(handler, 1_300, 100);
      expect(await balance(backend)).toBe(0);
    });
  }, 30_000);

  it("drops a result that names the wrong limits, leaving the claim", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      // Resolves: the vendor answered, so a broken settle is logged, not thrown.
      await send(handler, 400, 100, { settle: () => ({ otpm: 400 }) });
      expect(await balance(backend)).toBe(900);
    });
  }, 30_000);

  it("settles a request that waited in the queue", async () => {
    // One request per window: the second queues, and is claimed by the drain
    // loop from what the queue stored — which must say it settles.
    const tight = { ...rpm, tokensToAdd: 1, maxTokens: 1, interval: 300 };
    await run([tight, otpm()], async (handler, backend) => {
      await Promise.all([send(handler, 200, 100), send(handler, 300, 100)]);
      expect(await balance(backend)).toBe(500);
    });
  }, 30_000);

  it("settles a sharedLimit client's request against its owner's budget", async () => {
    await withHandler(
      harness,
      upstream.baseURL,
      [
        { name: "ai", rateLimit: [rpm, otpm()] },
        {
          name: "aishare",
          rateLimit: [{ type: "sharedLimit", clientName: CLIENT }],
        },
      ],
      async (handler, backend) => {
        await send(handler, 400, 100, { clientName: "aishare:_:a" });
        expect(await balance(backend)).toBe(600);
      }
    );
  }, 30_000);

  it("refuses a settling request on a backend that cannot settle", async () => {
    await run([rpm, otpm()], async (handler, backend) => {
      (backend as { settleTokens?: unknown }).settleTokens = undefined;
      await expect(send(handler, 400, 100)).rejects.toThrow(
        /backend implementing settleTokens/
      );
    });
  }, 30_000);
});
