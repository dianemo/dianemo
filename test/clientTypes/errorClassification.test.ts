import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { ErrorDecision } from "../../packages/core/src/client/types.js";
import { harnesses, withHandler } from "./harness.js";
import { createServer, type Server } from "node:http";

/**
 * How a failure decides the retry and the freeze: the vendor's `retry-after`,
 * and a client's own `classifyError`.
 */

interface Scripted {
  status: number;
  headers?: Record<string, string>;
}

/** Answers from a script, then with 200s, recording when each request came in. */
async function startScriptedUpstream() {
  let script: Scripted[] = [];
  const arrivals: number[] = [];
  const server: Server = createServer((_req, res) => {
    arrivals.push(Date.now());
    const next = script.shift() ?? { status: 200 };
    res.writeHead(next.status, {
      "Content-Type": "application/json",
      ...next.headers,
    });
    res.end("{}");
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    respond: (...responses: Scripted[]) => {
      script = responses;
    },
    arrivals,
    reset: () => {
      script = [];
      arrivals.length = 0;
    },
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

describe.each(harnesses(14))("error classification — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startScriptedUpstream>>;
  beforeAll(async () => {
    upstream = await startScriptedUpstream();
  });
  beforeEach(() => upstream.reset());
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "vendor:_:a";
  // Generous and quick, so neither the bucket nor the built-in back-off is
  // what holds a request back.
  const limit = {
    type: "requestLimit",
    interval: 50,
    tokensToAdd: 100,
    maxTokens: 100,
  };
  const fast = {
    retryBackoffBaseTime: 10,
    retryBackoffMethod: "linear",
    thawRequestCount: 0,
  };

  const run = (
    retryOptions: Record<string, unknown>,
    fn: Parameters<typeof withHandler>[3]
  ) =>
    withHandler(
      harness,
      upstream.baseURL,
      [{ name: "vendor", rateLimit: [limit], retryOptions }],
      fn
    );

  const send = (handler: Parameters<Parameters<typeof withHandler>[3]>[0]) =>
    handler.handleRequest({
      clientName: CLIENT,
      requestName: "vendor.call",
      method: "GET",
      url: "/",
    });

  /** Lets the controller arm the freeze a completion asked for. */
  const settle = () => new Promise((r) => setTimeout(r, 100));

  const gap = () => upstream.arrivals[1] - upstream.arrivals[0];

  describe("retry-after", () => {
    it("holds a retry back for as long as the response asks", async () => {
      await run(fast, async (handler) => {
        upstream.respond({ status: 429, headers: { "retry-after": "1" } });
        await send(handler);
        expect(upstream.arrivals).toHaveLength(2);
        expect(gap()).toBeGreaterThanOrEqual(950);
      });
    }, 30_000);

    it("freezes the fleet for as long as the response asks", async () => {
      await run({ ...fast, maxRetries: 0 }, async (handler) => {
        upstream.respond({ status: 429, headers: { "retry-after": "1" } });
        await expect(send(handler)).rejects.toThrow();
        await settle();
        await send(handler);
        expect(gap()).toBeGreaterThanOrEqual(950);
      });
    }, 30_000);

    it("holds back a retry the client asked for by status", async () => {
      await run({ ...fast, retryStatusCodes: [409] }, async (handler) => {
        upstream.respond({ status: 409, headers: { "retry-after": "1" } });
        await send(handler);
        expect(gap()).toBeGreaterThanOrEqual(950);
      });
    }, 30_000);
  });

  describe("classifyError", () => {
    const classifying = (
      decide: () => ErrorDecision | undefined,
      extra: Record<string, unknown> = {}
    ) => ({ ...fast, classifyError: decide, ...extra });

    it("fails a 429 at once, and freezes nothing, when told it is terminal", async () => {
      await run(
        classifying(() => ({ action: "fail" })),
        async (handler) => {
          upstream.respond({ status: 429, headers: { "retry-after": "5" } });
          await expect(send(handler)).rejects.toThrow();
          expect(upstream.arrivals).toHaveLength(1);
          await settle();
          const before = Date.now();
          await send(handler);
          expect(upstream.arrivals[1] - before).toBeLessThan(500);
        }
      );
    }, 30_000);

    it("retries a status the built-in rules would fail, without a freeze", async () => {
      await run(
        classifying(() => ({ action: "retry" }), { maxRetries: 1 }),
        async (handler) => {
          upstream.respond({ status: 400 }, { status: 400 });
          await expect(send(handler)).rejects.toThrow();
          expect(upstream.arrivals).toHaveLength(2);
          await settle();
          const before = Date.now();
          await send(handler);
          expect(upstream.arrivals[2] - before).toBeLessThan(500);
        }
      );
    }, 30_000);

    it("freezes the fleet for at least freezeMs", async () => {
      await run(
        classifying(
          () => ({ action: "retry", freeze: true, freezeMs: 1_000 }),
          { maxRetries: 0 }
        ),
        async (handler) => {
          upstream.respond({ status: 400 });
          await expect(send(handler)).rejects.toThrow();
          await settle();
          await send(handler);
          expect(gap()).toBeGreaterThanOrEqual(950);
        }
      );
    }, 30_000);

    it("lets retry-after outlast a shorter freezeMs", async () => {
      await run(
        classifying(() => ({ action: "retry", freeze: true, freezeMs: 10 }), {
          maxRetries: 0,
        }),
        async (handler) => {
          upstream.respond({ status: 400, headers: { "retry-after": "1" } });
          await expect(send(handler)).rejects.toThrow();
          await settle();
          await send(handler);
          expect(gap()).toBeGreaterThanOrEqual(950);
        }
      );
    }, 30_000);

    it("leaves a failure it does not classify to the built-in rules", async () => {
      await run(
        classifying(() => undefined),
        async (handler) => {
          upstream.respond({ status: 503 });
          await send(handler);
          expect(upstream.arrivals).toHaveLength(2);
        }
      );
    }, 30_000);

    it("falls back to the built-in rules when it throws", async () => {
      await run(
        {
          ...fast,
          classifyError: () => {
            throw new Error("broken classifier");
          },
        },
        async (handler) => {
          upstream.respond({ status: 503 });
          await send(handler);
          expect(upstream.arrivals).toHaveLength(2);
        }
      );
    }, 30_000);

    it("falls back to the built-in rules on a decision it does not know", async () => {
      await run(
        classifying(() => ({ action: "maybe" }) as never),
        async (handler) => {
          upstream.respond({ status: 503 });
          await send(handler);
          expect(upstream.arrivals).toHaveLength(2);
        }
      );
    }, 30_000);
  });
});
