import { afterAll, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../../packages/core/src/index.js";
import { harnesses, startUpstream } from "./harness.js";

/**
 * A client's limits changing while requests are in flight.
 *
 * A limit's name is its budget's key, so a change that renames or removes one
 * points the client at different keys than the ones an in-flight request
 * claimed from. What must survive that is the ledger: every slot claimed is
 * released, and a limit retuned without repeating its name keeps its budget.
 */

const KEY = "0123456789abcdef0123456789abcdef";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe.each(harnesses(9))("limits changing mid-flight — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;
  beforeAll(async () => {
    upstream = await startUpstream(0);
  });
  afterAll(async () => {
    await upstream.close();
  });

  async function rig(rateLimit: unknown, extra: Record<string, unknown> = {}) {
    const { backend, cleanup } = await harness.create();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "lc" + Math.random().toString(36).slice(2, 8),
    });
    await handler.registerClientTemplate(
      "t" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `t:_:${creds.instanceId}`,
          rateLimit,
          requestOptions: {
            defaults: { baseURL: upstream.baseURL },
            cleanupTimeout: 3000,
          },
          ...extra,
        },
      ]) as never
    );
    await handler.start();
    await handler.addTemplateClient("t" as never, { instanceId: "a" } as never);
    return {
      handler,
      stop: async () => {
        await handler.stop();
        await cleanup();
      },
    };
  }

  const fire = (handler: RequestHandler) =>
    handler.handleRequest({
      clientName: "t:_:a",
      requestName: "t",
      method: "GET",
      url: "/",
    });

  it("releases an in-flight slot after an override drops its concurrency limit", async () => {
    upstream.setDelay(400);
    const { handler, stop } = await rig([
      { type: "concurrencyLimit", maxConcurrency: 1 },
    ]);
    try {
      const inFlight = fire(handler);
      await sleep(100);
      await handler.addTemplateClient(
        "t" as never,
        { instanceId: "a" } as never,
        {
          rateLimitOverrides: {
            "": [
              {
                type: "requestLimit",
                maxTokens: 100,
                tokensToAdd: 100,
                interval: 60_000,
              },
            ],
          },
        }
      );
      await inFlight;
      await sleep(200);
      // Back on the one-slot default. A slot left behind would hold it until
      // its TTL, well past this request's admission timeout.
      await handler.addTemplateClient(
        "t" as never,
        { instanceId: "a" } as never,
        { rateLimitOverrides: {} }
      );
      upstream.setDelay(0);

      await expect(fire(handler)).resolves.toBeDefined();
    } finally {
      upstream.setDelay(0);
      await stop();
    }
  }, 20_000);

  it("keeps a named budget when rateLimitChange returns the limit without its name", async () => {
    upstream.setDelay(0);
    const hourly = {
      type: "requestLimit",
      maxTokens: 2,
      tokensToAdd: 2,
      interval: 3_600_000,
    };
    const { handler, stop } = await rig([{ ...hourly, name: "per_hour" }], {
      rateLimitChange: () => [hourly],
    });
    try {
      // Spaced so each response's rateLimitChange lands before the next claim.
      const results = await Promise.allSettled(
        Array.from({ length: 5 }, async (_, i) => {
          await sleep(i * 150);
          return fire(handler);
        })
      );

      expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(2);
    } finally {
      await stop();
    }
  }, 20_000);
});
