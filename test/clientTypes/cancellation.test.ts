import { afterAll, beforeAll, describe, expect, it } from "vitest";
import RequestHandler from "../../packages/core/src/index.js";
import { type Harness, harnesses } from "./harness.js";
import { createServer, type Server } from "node:http";
import { getEventListeners } from "node:events";
import {
  ClientUnavailableError,
  RequestAbortedError,
} from "../../packages/core/src/errors.js";

/**
 * A request that is waiting — out a retry's back-off, on a token refresh, or
 * on a requestInterceptor — still answers to its signal and to shutdown, and
 * gives back what it held.
 */

/**
 * Answers with the scripted statuses, then 200. `/token` answers after
 * `tokenDelay` ms, or never while it is negative. Records every arrival.
 */
async function startUpstream() {
  const arrivals: string[] = [];
  let tokenCalls = 0;
  let tokenDelay = -1;
  let script: number[] = [];
  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname === "/token") {
      tokenCalls++;
      if (tokenDelay < 0) return;
      setTimeout(() => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end('{"access_token":"T","expires_in":3600}');
      }, tokenDelay);
      return;
    }
    arrivals.push(url.searchParams.get("id") ?? "");
    const status = script.shift() ?? 200;
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end("{}");
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    arrivals: (id: string) => arrivals.filter((seen) => seen === id).length,
    tokenCalls: () => tokenCalls,
    respond: (...statuses: number[]) => {
      script = statuses;
    },
    setTokenDelay: (ms: number) => {
      tokenDelay = ms;
    },
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const CLIENT = "cx:_:a";

describe.each(harnesses(9))("cancellation — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;
  beforeAll(async () => {
    upstream = await startUpstream();
  });
  afterAll(async () => {
    await upstream.close();
  });

  const slot = { name: "slots", type: "concurrencyLimit", maxConcurrency: 1 };

  /**
   * A started handler with one `cx` client, torn down afterwards whether or
   * not `fn` already stopped it.
   */
  async function withClient(
    client: Record<string, unknown>,
    fn: (handler: RequestHandler) => Promise<void>
  ) {
    const { backend, cleanup } = await (harness as Harness).create();
    const handler = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend,
      keyPrefix: "cx",
    });
    await handler.registerClientTemplate(
      "cx" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `cx:_:${creds.instanceId}`,
          rateLimit: [slot],
          requestOptions: { defaults: { baseURL: upstream.baseURL } },
          ...client,
        },
      ]) as never
    );
    await handler.start();
    await handler.addTemplateClient(
      "cx" as never,
      { instanceId: "a" } as never
    );
    try {
      await fn(handler);
    } finally {
      handler.setDrainTimeout(100);
      await handler.stop();
      await cleanup();
    }
  }

  const send = (
    handler: RequestHandler,
    id: string,
    signal?: AbortSignal
  ): Promise<unknown> =>
    handler
      .handleRequest({
        clientName: CLIENT,
        requestName: "cx.get",
        method: "GET",
        url: `/?id=${id}`,
        ...(signal ? { signal } : {}),
      })
      .catch((error: unknown) => error);

  const slowRetries = {
    retryOptions: {
      maxRetries: 2,
      retryBackoffBaseTime: 5_000,
      retryBackoffMethod: "linear",
    },
  };

  describe("a retry's back-off", () => {
    it("ends when the request's signal aborts", async () => {
      await withClient(slowRetries, async (handler) => {
        upstream.respond(503);
        const controller = new AbortController();
        const started = Date.now();
        const pending = send(handler, "backoff-abort", controller.signal);
        await sleep(200);
        controller.abort();
        expect(await pending).toBeInstanceOf(RequestAbortedError);
        expect(Date.now() - started).toBeLessThan(2_000);
        // The queue entry the retry kept is released with the abandonment.
        await sleep(100);
        const clients = (
          handler as unknown as {
            clients: Map<string, { getQueueStats(): Promise<unknown> }>;
          }
        ).clients;
        const queue = clients.get(CLIENT)!;
        expect(await queue.getQueueStats()).toEqual({
          pending: 0,
          inProgress: 0,
        });
      });
    }, 30_000);

    it("ends with handler_stopped when the handler stops, sending nothing after", async () => {
      await withClient(slowRetries, async (handler) => {
        upstream.respond(503);
        const pending = send(handler, "backoff-stop");
        await sleep(200);
        handler.setDrainTimeout(100);
        await handler.stop();
        const error = await pending;
        expect(error).toBeInstanceOf(ClientUnavailableError);
        expect((error as ClientUnavailableError).code).toBe("handler_stopped");
        await sleep(300);
        expect(upstream.arrivals("backoff-stop")).toBe(1);
      });
    }, 30_000);

    it("leaves no abort listener on the signal after retries that sleep out their back-off", async () => {
      await withClient(
        {
          retryOptions: {
            maxRetries: 3,
            retryBackoffBaseTime: 50,
            retryBackoffMethod: "linear",
          },
        },
        async (handler) => {
          upstream.respond(503, 503, 503);
          const controller = new AbortController();
          const started = Date.now();
          const res = await send(
            handler,
            "backoff-listeners",
            controller.signal
          );
          expect(res).toMatchObject({ status: 200 });
          expect(upstream.arrivals("backoff-listeners")).toBe(4);
          expect(Date.now() - started).toBeGreaterThanOrEqual(250);
          expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
        }
      );
    }, 30_000);
  });

  describe("an OAuth2 token refresh", () => {
    const oauth = (axiosOptions?: Record<string, unknown>) => ({
      retryOptions: { maxRetries: 0 },
      ...(axiosOptions ? { axiosOptions } : {}),
      authentication: {
        type: "oauth2",
        clientId: "CID",
        clientSecret: "SECRET",
        refreshConfig: {
          url: `${upstream.baseURL}/token`,
          dataLocation: "urlEncodedForm",
          data: { grant_type: "client_credentials" },
        },
      },
    });

    it("lets an aborted request stop waiting on a refresh that never returns", async () => {
      upstream.setTokenDelay(-1);
      await withClient(oauth(), async (handler) => {
        const controller = new AbortController();
        const pending = send(handler, "refresh-abort", controller.signal);
        setTimeout(() => controller.abort(), 200);
        const outcome = await Promise.race([
          pending,
          sleep(3_000).then(() => "still waiting"),
        ]);
        expect(outcome).toBeInstanceOf(RequestAbortedError);
      });
    }, 30_000);

    it("keeps refreshing for the other waiters when one of them aborts", async () => {
      upstream.setTokenDelay(500);
      const calledBefore = upstream.tokenCalls();
      await withClient(
        { ...oauth(), rateLimit: [{ ...slot, maxConcurrency: 2 }] },
        async (handler) => {
          const controller = new AbortController();
          const leaving = send(handler, "refresh-leaves", controller.signal);
          const staying = send(handler, "refresh-stays");
          setTimeout(() => controller.abort(), 100);
          expect(await leaving).toBeInstanceOf(RequestAbortedError);
          expect(await staying).toMatchObject({ status: 200 });
          expect(upstream.tokenCalls() - calledBefore).toBe(1);
        }
      );
    }, 30_000);

    it("gives up on a token endpoint that never answers within the client's timeout", async () => {
      upstream.setTokenDelay(-1);
      await withClient(oauth({ timeout: 300 }), async (handler) => {
        const outcome = await Promise.race([
          send(handler, "refresh-timeout"),
          sleep(3_000).then(() => "still waiting"),
        ]);
        expect(outcome).toBeInstanceOf(Error);
        expect(upstream.arrivals("refresh-timeout")).toBe(0);
        upstream.setTokenDelay(0);
      });
    }, 30_000);

    it("sends nothing upstream after stop() returns when a token refresh outlives the drain", async () => {
      upstream.setTokenDelay(1_000);
      try {
        await withClient(oauth(), async (handler) => {
          const pending = send(handler, "refresh-outlives-stop");
          await sleep(100);
          handler.setDrainTimeout(100);
          await handler.stop();
          expect(upstream.arrivals("refresh-outlives-stop")).toBe(0);
          const error = await pending;
          expect((error as ClientUnavailableError).code).toBe(
            "handler_stopped"
          );
          await sleep(200);
          expect(upstream.arrivals("refresh-outlives-stop")).toBe(0);
        });
      } finally {
        upstream.setTokenDelay(0);
      }
    }, 30_000);
  });

  describe("a requestInterceptor", () => {
    it("sends nothing upstream after stop() returns when the requestInterceptor outlives the drain", async () => {
      await withClient(
        {
          requestOptions: {
            defaults: { baseURL: upstream.baseURL },
            requestInterceptor: async (config: unknown) => {
              await sleep(1_000);
              return config;
            },
          },
        },
        async (handler) => {
          const pending = send(handler, "interceptor-outlives-stop");
          await sleep(100);
          handler.setDrainTimeout(100);
          await handler.stop();
          expect(upstream.arrivals("interceptor-outlives-stop")).toBe(0);
          const error = await pending;
          expect((error as ClientUnavailableError).code).toBe(
            "handler_stopped"
          );
          await sleep(200);
          expect(upstream.arrivals("interceptor-outlives-stop")).toBe(0);
        }
      );
    }, 30_000);
  });
});
