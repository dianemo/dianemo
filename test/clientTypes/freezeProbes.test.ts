import type { DianemoBackend } from "../../packages/core/src/backend/types.js";
import { createServer, type ServerResponse } from "node:http";
import RequestHandler from "../../packages/core/src/index.js";
import { harnesses, type Harness } from "./harness.js";
import { describe, expect, it } from "vitest";

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
