import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { memoryBackend } from "../../packages/core/src/backend/memory.js";
import { redisBackend } from "../../packages/backend-redis/src/index.js";
import { createServer, type ServerResponse } from "node:http";
import RequestHandler from "../../packages/core/src/index.js";
import { harnesses, type Harness } from "./harness.js";
import { describe, expect, it } from "vitest";
import { Redis } from "ioredis";

const KEY = "0123456789abcdef0123456789abcdef";
const PREFIX = "aud";
const NAME = "aud:_:a";
const NS = `${PREFIX}:requestHandler:${NAME}`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function startUpstream() {
  const holds = new Map<string, () => void>();
  const server = createServer(async (req, res: ServerResponse) => {
    const path = (req.url ?? "/").split("?")[0];
    if (path.startsWith("/hold/")) {
      await new Promise<void>((resolve) => holds.set(path, resolve));
      res.writeHead(200).end("{}");
    } else if (path.startsWith("/status/")) {
      res.writeHead(Number(path.slice("/status/".length))).end("{}");
    } else if (path === "/429") {
      res.writeHead(429).end("{}");
    } else {
      res.writeHead(200).end("{}");
    }
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const a = server.address();
  const port = typeof a === "object" && a ? a.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    held: (path: string) => holds.has(path),
    releaseAll: () => {
      for (const r of holds.values()) r();
      holds.clear();
    },
    release: (path: string) => {
      holds.get(path)?.();
      holds.delete(path);
    },
    close: async () => {
      for (const r of holds.values()) r();
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

async function withClient(
  harness: Harness,
  client: Record<string, unknown>,
  fn: (handler: RequestHandler, backend: DianemoBackend) => Promise<void>,
  onExit: () => void = () => undefined
) {
  const { backend, cleanup } = await harness.create();
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix: PREFIX });
  await handler.registerClientTemplate(
    "aud" as never,
    (() => [{ ...client, name: NAME }]) as never
  );
  await handler.start();
  await handler.addTemplateClient("aud" as never, { instanceId: "a" } as never);
  try {
    await fn(handler, backend);
  } finally {
    onExit();
    await handler.stop().catch(() => undefined);
    await cleanup().catch(() => undefined);
  }
}

const waitUntil = async (check: () => Promise<boolean>, ms = 5000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await check()) return true;
    await sleep(20);
  }
  return false;
};

describe.each(harnesses(0))(
  "a freeze across limit changes — $name",
  (harness) => {
    it("a declined thaw probe does not keep its probe mark into a later freeze", async () => {
      const upstream = await startUpstream();
      try {
        await withClient(
          harness,
          {
            rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 3 }],
            requestOptions: { defaults: { baseURL: upstream.baseURL } },
            retryOptions: {
              maxRetries: 0,
              retryBackoffBaseTime: 400,
              thawRequestCount: 1,
            },
          },
          async (handler, backend) => {
            const send = (url: string, cost = 1, priority = 1) =>
              handler.handleRequest({
                clientName: NAME,
                requestName: "t",
                method: "GET",
                url,
                cost,
                priority,
              } as never);
            const FREEZE = `${NS}:freezeState`;
            const frozen = async () =>
              (await backend.canProcessRequest(FREEZE)).canProcess === false;

            // R1 holds 2 of 3 slots for the whole first freeze and thaw.
            const r1 = send("/hold/r1", 2);
            await waitUntil(async () => upstream.held("/hold/r1"));

            // A 429 freezes the client for one probe.
            await send("/429").catch(() => undefined);
            expect(await waitUntil(frozen)).toBe(true);

            // C (cost 2) queues during the freeze. At thaw it is nominated as the
            // probe, then declined for slots (2 + 2 > 3).
            const c = send("/hold/c", 2, 0);
            expect(await waitUntil(async () => !(await frozen()), 3000)).toBe(
              true
            );
            await sleep(100);

            // D, higher priority and fitting the last slot, becomes the actual
            // probe, succeeds, and ends the thaw.
            await send("/ok", 1, 5);
            await sleep(100);
            expect(await backend.getFreezeState(FREEZE)).toBeNull();

            const cId = (
              await backend.getAllRequests(`${NS}:queue`, `${NS}:request`)
            )[0];
            // C was declined, so it went back to the queue without the mark.
            expect(cId.isThawRequest).toBe(false);

            // R1 finishes; C is admitted normally, still marked, and held.
            upstream.release("/hold/r1");
            await r1;
            expect(await waitUntil(async () => upstream.held("/hold/c"))).toBe(
              true
            );

            // A fresh 429 arms a new freeze while C is upstream.
            await send("/429").catch(() => undefined);
            expect(await waitUntil(frozen)).toBe(true);

            // C completes inside that new window. It is not a probe of this
            // freeze, so the freeze must still stand.
            const armed = await backend.getFreezeState(FREEZE);
            upstream.release("/hold/c");
            await c;
            await sleep(100);
            // Still inside the window the second 429 asked for...
            expect(Date.now()).toBeLessThan(armed!.frozenUntil);
            expect(await backend.getFreezeState(FREEZE)).not.toBeNull();
            expect(await frozen()).toBe(true);
          },
          () => upstream.releaseAll()
        );
      } finally {
        await upstream.close();
      }
    }, 20_000);

    it("does not refund into a bucket a standing 429 freeze emptied, after a second requestLimit is added", async () => {
      const A = {
        name: "per_min",
        type: "requestLimit",
        maxTokens: 5,
        tokensToAdd: 5,
        interval: 60_000,
      };
      const B = {
        name: "per_day",
        type: "requestLimit",
        maxTokens: 100,
        tokensToAdd: 100,
        interval: 86_400_000,
      };
      await withClient(
        harness,
        { rateLimit: [A] },
        async (handler, backend) => {
          const client = (handler as any).clients.get(NAME);
          const BUCKET = `${NS}:rateLimit:per_min`;
          const FREEZE = `${NS}:freezeState`;
          const meta = {
            requestId: "r1",
            clientName: NAME,
            requestName: "t",
            status: "inProgress",
            priority: 1,
            cost: 1,
            retries: 0,
            timestamp: Date.now(),
            isThawRequest: false,
            ownerId: "x",
          };

          // Admitted under the one-bucket config.
          expect((await client.tryAcquireTurn(meta)).acquired).toBe(true);
          expect(meta).toHaveProperty("claimedKeys", [BUCKET]);

          // Another request's 429 lands, as the controller applies it for a client
          // with one requestLimit: an emptying freeze, then the bucket zeroed.
          await backend.armFreeze(FREEZE, (await backend.now()) + 30_000, 3, {
            emptiesBucket: client.freezeEmptiesBucket(true),
          });
          await client.handleFreezeOwnTypeRequests();
          const tokens = async () =>
            (await backend.getTokenBucketState(BUCKET, A as never)).tokens;
          expect(await tokens()).toBe(0);

          // A second limit is added (rateLimitChange / operator), keeping per_min.
          client.handleRateLimitUpdated({
            clientName: NAME,
            rateLimit: [A, B],
            source: "operator",
          });

          // The admitted request is abandoned before dispatch.
          await client.releaseUnusedAdmission(meta);

          // The freeze that emptied per_min still stands, so per_min must stay at 0,
          // as the single-bucket path and refundTokens' own gate would keep it.
          expect(await client.isFrozen()).toBe(true);
          expect(await tokens()).toBe(0);
        }
      );
    });
  }
);

describe.each(harnesses(0))("what a thaw probe learns — $name", (harness) => {
  const FREEZE = `${NS}:freezeState`;

  /** Freezes on a 429, then resolves once the window has lapsed. */
  async function freezeAndLapse(
    handler: RequestHandler,
    backend: DianemoBackend
  ) {
    await send(handler, "/429").catch(() => undefined);
    const lapsed = await waitUntil(async () => {
      const state = await backend.getFreezeState(FREEZE);
      return state !== null && (await backend.now()) >= state.frozenUntil;
    });
    expect(lapsed).toBe(true);
  }

  const send = (handler: RequestHandler, url: string) =>
    handler.handleRequest({
      clientName: NAME,
      requestName: "t",
      method: "GET",
      url,
    } as never);

  const probing = (upstream: { baseURL: string }, extra = {}) => ({
    rateLimit: [{ type: "noLimit" }],
    requestOptions: { defaults: { baseURL: upstream.baseURL } },
    retryOptions: {
      maxRetries: 0,
      retryBackoffBaseTime: 50,
      thawRequestCount: 1,
      ...extra,
    },
  });

  it("does not hold a client in single-file probing for a week when every probe gets a 404", async () => {
    const upstream = await startUpstream();
    try {
      await withClient(harness, probing(upstream), async (handler, backend) => {
        await freezeAndLapse(handler, backend);
        for (let i = 0; i < 3; i++) {
          await expect(send(handler, "/status/404")).rejects.toThrow();
        }
        expect(
          await waitUntil(
            async () => (await backend.getFreezeState(FREEZE)) === null
          )
        ).toBe(true);
      });
    } finally {
      await upstream.close();
    }
  }, 20_000);

  it("keeps the client frozen when its probe gets a 5xx", async () => {
    const upstream = await startUpstream();
    try {
      await withClient(harness, probing(upstream), async (handler, backend) => {
        await freezeAndLapse(handler, backend);
        await expect(send(handler, "/status/503")).rejects.toThrow();
        await sleep(200);
        expect(await backend.getFreezeState(FREEZE)).toMatchObject({
          thawRequestCount: 1,
        });

        await send(handler, "/ok");
        expect(
          await waitUntil(
            async () => (await backend.getFreezeState(FREEZE)) === null
          )
        ).toBe(true);
      });
    } finally {
      await upstream.close();
    }
  }, 20_000);

  it("keeps a 429 freeze whose thaw probe gets a 503 (control)", async () => {
    const upstream = await startUpstream();
    try {
      await withClient(harness, probing(upstream), async (handler, backend) => {
        await freezeAndLapse(handler, backend);
        await send(handler, "/status/503").catch(() => undefined);
        await sleep(200);
        expect(await backend.getFreezeState(FREEZE)).not.toBeNull();
      });
    } finally {
      await upstream.close();
    }
  }, 20_000);

  it("does not count a thaw probe its caller aborted toward thawing a 429 freeze", async () => {
    const upstream = await startUpstream();
    try {
      await withClient(
        harness,
        probing(upstream),
        async (handler, backend) => {
          await freezeAndLapse(handler, backend);

          // The probe reaches the vendor, and its caller gives up before the
          // vendor answers.
          const controller = new AbortController();
          const probe = handler
            .handleRequest({
              clientName: NAME,
              requestName: "t",
              method: "GET",
              url: "/hold/probe",
              signal: controller.signal,
            } as never)
            .catch(() => undefined);
          expect(
            await waitUntil(async () => upstream.held("/hold/probe"))
          ).toBe(true);
          controller.abort();
          await probe;
          await sleep(200);

          expect(await backend.getFreezeState(FREEZE)).toMatchObject({
            thawRequestCount: 1,
          });
          // And it gave the probe back: the next request probes and thaws.
          await send(handler, "/ok");
          expect(
            await waitUntil(
              async () => (await backend.getFreezeState(FREEZE)) === null
            )
          ).toBe(true);
        },
        () => upstream.releaseAll()
      );
    } finally {
      await upstream.close();
    }
  }, 20_000);

  it("does not count a thaw probe abandoned before dispatch toward thawing a 429 freeze", async () => {
    const upstream = await startUpstream();
    const spec = probing(upstream);
    try {
      await withClient(
        harness,
        {
          ...spec,
          requestOptions: {
            ...spec.requestOptions,
            requestInterceptor: async (config: { url?: string }) => {
              if (config.url === "/never") throw new Error("interceptor");
              return config;
            },
          },
        },
        async (handler, backend) => {
          await freezeAndLapse(handler, backend);
          // Nominated as the probe, then fails in its interceptor.
          await send(handler, "/never").catch(() => undefined);
          await sleep(200);

          expect(await backend.getFreezeState(FREEZE)).toMatchObject({
            thawRequestCount: 1,
          });
          await send(handler, "/ok");
          expect(
            await waitUntil(
              async () => (await backend.getFreezeState(FREEZE)) === null
            )
          ).toBe(true);
        }
      );
    } finally {
      await upstream.close();
    }
  }, 20_000);

  it("does not count a thaw probe refused by an unreachable upstream toward thawing a 429 freeze", async () => {
    const upstream = await startUpstream();
    // Nothing listens on a port just closed, so the probe is refused.
    const dead = await startUpstream();
    await dead.close();
    try {
      await withClient(harness, probing(upstream), async (handler, backend) => {
        await freezeAndLapse(handler, backend);
        await send(handler, `${dead.baseURL}/x`).catch(() => undefined);
        await sleep(200);

        expect(await backend.getFreezeState(FREEZE)).toMatchObject({
          thawRequestCount: 1,
        });
        await send(handler, "/ok");
        expect(
          await waitUntil(
            async () => (await backend.getFreezeState(FREEZE)) === null
          )
        ).toBe(true);
      });
    } finally {
      await upstream.close();
    }
  }, 20_000);

  it("keeps the client frozen when its probe gets a 4xx that classifyError freezes on", async () => {
    const upstream = await startUpstream();
    const classifyError = (error: { response?: { status?: number } }) =>
      error.response?.status === 403
        ? { action: "retry" as const, freeze: true as const }
        : undefined;
    try {
      await withClient(
        harness,
        probing(upstream, { classifyError }),
        async (handler, backend) => {
          await freezeAndLapse(handler, backend);
          await expect(send(handler, "/status/403")).rejects.toThrow();
          await sleep(200);
          expect(await backend.getFreezeState(FREEZE)).toMatchObject({
            thawRequestCount: 1,
          });
        }
      );
    } finally {
      await upstream.close();
    }
  }, 20_000);
});

// ---- invariants across two handlers sharing one backend

/** An upstream that measures what the vendor sees, and fails on a schedule. */
async function startFailingUpstream(fail: (n: number) => number | undefined) {
  let n = 0;
  let inFlight = 0;
  let peak = 0;
  const arrivals: number[] = [];
  const server = createServer(async (_req, res) => {
    const i = n++;
    arrivals.push(Date.now());
    inFlight++;
    peak = Math.max(peak, inFlight);
    const status = fail(i);
    await sleep(status ? 5 : 15 + (i % 4) * 10);
    inFlight--;
    if (!res.destroyed) res.writeHead(status ?? 200).end("{}");
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const a = server.address();
  const port = typeof a === "object" && a ? a.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    peak: () => peak,
    arrivals: () => arrivals,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

interface SharedBackend {
  name: string;
  backends: () => Promise<{
    backends: DianemoBackend[];
    close: () => Promise<void>;
  }>;
}

/** One store, reached through two backends, as two replicas reach Redis. */
function sharedBackends(): SharedBackend[] {
  const list: SharedBackend[] = [
    {
      name: "memory",
      backends: async () => {
        const backend = memoryBackend();
        return { backends: [backend, backend], close: () => backend.close() };
      },
    },
  ];
  const url = process.env.REDIS_URL;
  if (url) {
    list.push({
      name: "redis",
      backends: async () => {
        const conns = [new Redis(`${url}/0`), new Redis(`${url}/0`)];
        await conns[0].flushdb();
        const backends = conns.map((c) => redisBackend(c));
        return {
          backends,
          close: async () => {
            for (const b of backends) await b.close().catch(() => undefined);
            for (const c of conns) await c.quit().catch(() => undefined);
          },
        };
      },
    });
  }
  return list;
}

async function withTwoHandlers(
  shared: SharedBackend,
  client: Record<string, unknown>,
  fn: (handlers: RequestHandler[]) => Promise<void>
) {
  const { backends, close } = await shared.backends();
  const handlers: RequestHandler[] = [];
  try {
    for (const [i, backend] of backends.entries()) {
      const handler = new RequestHandler({
        key: KEY,
        backend,
        keyPrefix: PREFIX,
      });
      handlers.push(handler);
      await handler.registerClientTemplate(
        "aud" as never,
        (() => [{ ...client, name: NAME }]) as never
      );
      await handler.start();
      if (i === 0) {
        await handler.addTemplateClient(
          "aud" as never,
          { instanceId: "a" } as never
        );
      }
    }
    await waitUntil(async () =>
      handlers.every((h) => h.getMetadata().registeredClients.includes(NAME))
    );
    await fn(handlers);
  } finally {
    for (const h of handlers) await h.stop().catch(() => undefined);
    await close();
  }
}

describe.each(sharedBackends())(
  "invariants while failures freeze and thaw — $name",
  (shared) => {
    const sendVia =
      (handler: RequestHandler) =>
      (url: string, extra: Record<string, unknown> = {}) =>
        handler.handleRequest({
          clientName: NAME,
          requestName: "t",
          method: "GET",
          url,
          ...extra,
        } as never);

    it("never has more than maxConcurrency requests at the vendor while failures freeze and thaw the client", async () => {
      const upstream = await startFailingUpstream((i) =>
        i % 9 === 4 ? 429 : i % 13 === 7 ? 503 : undefined
      );
      try {
        await withTwoHandlers(
          shared,
          {
            rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 3 }],
            requestOptions: {
              defaults: { baseURL: upstream.baseURL },
              cleanupTimeout: 20_000,
            },
            retryOptions: {
              maxRetries: 2,
              retryBackoffBaseTime: 60,
              thawRequestCount: 2,
            },
          },
          async (handlers) => {
            const all = Array.from({ length: 80 }, (_, i) =>
              sendVia(handlers[i % 2])("/x", { priority: i % 3 }).catch(
                () => undefined
              )
            );
            await Promise.all(all);
            expect(upstream.arrivals().length).toBeGreaterThan(60);
            expect(upstream.peak()).toBeLessThanOrEqual(3);
          }
        );
      } finally {
        await upstream.close();
      }
    }, 60_000);

    it("never admits more than a bucket's worth per interval while failures freeze and thaw the client", async () => {
      const upstream = await startFailingUpstream((i) =>
        i % 7 === 3 ? 429 : i % 11 === 5 ? 503 : undefined
      );
      const interval = 500;
      const maxTokens = 4;
      try {
        await withTwoHandlers(
          shared,
          {
            rateLimit: [
              {
                type: "requestLimit",
                maxTokens,
                tokensToAdd: maxTokens,
                interval,
              },
            ],
            requestOptions: {
              defaults: { baseURL: upstream.baseURL },
              cleanupTimeout: 30_000,
            },
            retryOptions: {
              maxRetries: 1,
              retryBackoffBaseTime: 60,
              thawRequestCount: 1,
            },
          },
          async (handlers) => {
            const all = Array.from({ length: 40 }, (_, i) =>
              sendVia(handlers[i % 2])("/x").catch(() => undefined)
            );
            await Promise.all(all);
            const t = upstream.arrivals();
            expect(t.length).toBeGreaterThan(20);
            // Any window one interval long sees at most a full bucket plus one
            // refill.
            let worst = 0;
            for (let i = 0; i < t.length; i++) {
              let j = i;
              while (j < t.length && t[j] - t[i] < interval) j++;
              worst = Math.max(worst, j - i);
            }
            expect(worst).toBeLessThanOrEqual(2 * maxTokens);
          }
        );
      } finally {
        await upstream.close();
      }
    }, 90_000);
  }
);
