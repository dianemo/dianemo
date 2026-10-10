import type { CreateClientData } from "../packages/core/src/client/types.js";
import type { RequestHandlerPlugin } from "../packages/core/src/plugin.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { ConfigurationError } from "../packages/core/src/errors.js";
import { harnesses, uniquePrefix } from "./clientTypes/harness.js";
import { createServer, type IncomingMessage } from "node:http";
import type { Logger } from "../packages/core/src/logger.js";
import RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it } from "vitest";
import http from "node:http";

/**
 * The template and handler API: plans, rebuilds, removal, sub-client merging
 * and plugin registration. The cases touching stored credentials run on every
 * backend, since that is where the two could disagree.
 */

const KEY = "0123456789abcdef0123456789abcdef";

const PER_SECOND = (n: number) => [
  {
    type: "requestLimit" as const,
    interval: 1000,
    tokensToAdd: n,
    maxTokens: n,
  },
];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function recordingLogger() {
  const lines: { level: string; msg: string }[] = [];
  const at =
    (level: string) =>
    (a: unknown, b?: unknown): void => {
      lines.push({ level, msg: typeof a === "string" ? a : String(b ?? "") });
    };
  const logger: Logger = {
    debug: at("debug"),
    info: at("info"),
    warn: at("warn"),
    error: at("error"),
  };
  return { logger, lines };
}

/**
 * An upstream with a token endpoint per path prefix: `/token/<id>` answers with
 * `TOKEN_<id>_<n>`, and everything else echoes the bearer it was sent.
 */
async function startAuthServer() {
  let tokenDelayMs = 0;
  let tokenStatus = 200;
  let expiresIn = 3600;
  const tokenCalls: string[] = [];
  const bearers: string[] = [];
  const server = createServer((req: IncomingMessage, res) => {
    const url = req.url ?? "";
    if (url.startsWith("/token/")) {
      const id = url.slice("/token/".length);
      tokenCalls.push(id);
      const n = tokenCalls.length;
      req.resume();
      req.on("end", () => {
        setTimeout(() => {
          if (tokenStatus !== 200) {
            res.writeHead(tokenStatus, { "Content-Type": "application/json" });
            res.end('{"error":"unavailable"}');
            return;
          }
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({
              access_token: `TOKEN_${id}_${n}`,
              expires_in: expiresIn,
              token_type: "Bearer",
            })
          );
        }, tokenDelayMs);
      });
      return;
    }
    bearers.push(String(req.headers.authorization ?? ""));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  const baseURL = `http://127.0.0.1:${port}`;
  return {
    baseURL,
    tokenCalls,
    bearers,
    setTokenDelay: (ms: number) => (tokenDelayMs = ms),
    setTokenStatus: (status: number) => (tokenStatus = status),
    setExpiresIn: (seconds: number) => (expiresIn = seconds),
    oauth: (id: string) => ({
      type: "oauth2" as const,
      clientId: id,
      clientSecret: `${id}_SECRET`,
      refreshConfig: {
        url: `${baseURL}/token/${id}`,
        dataLocation: "urlEncodedForm" as const,
        data: {
          grant_type: "client_credentials",
          client_id: "{{clientId}}",
          client_secret: "{{clientSecret}}",
        },
      },
    }),
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

const limitOf = (handler: RequestHandler, name: string) =>
  handler.getLoadedClients().find((c) => c.name === name)?.rateLimit;

// ---- plans

describe("a rate-limit plan", () => {
  it("keeps a sharedLimit sibling on its owner's budget when the plan covers the root", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "tp" });
    await handler.registerClientTemplate(
      "acme" as never,
      ((c: { instanceId: string }) => [
        { name: `acme:_:${c.instanceId}`, rateLimit: PER_SECOND(5) },
        {
          name: `acme:_:${c.instanceId}:eu`,
          rateLimit: [
            { type: "sharedLimit", clientName: `acme:_:${c.instanceId}` },
          ],
        },
      ]) as never,
      { rateLimitOptions: { pro: PER_SECOND(9) } }
    );
    try {
      await handler.start();
      await handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "pro" }
      );

      expect(limitOf(handler, "acme:_:a")).toEqual([
        { ...PER_SECOND(9)[0], name: "default" },
      ]);
      expect(limitOf(handler, "acme:_:a:eu")).toEqual([
        { type: "sharedLimit", clientName: "acme:_:a", name: "default" },
      ]);
    } finally {
      await handler.stop();
    }
  });

  it("applies its root entry to every root that meters its own budget", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "tp" });
    await handler.registerClientTemplate(
      "acme" as never,
      ((c: { instanceId: string }) => [
        { name: `acme:_:${c.instanceId}`, rateLimit: PER_SECOND(5) },
        { name: `acme:_:${c.instanceId}:eu`, rateLimit: PER_SECOND(5) },
      ]) as never,
      { rateLimitOptions: { pro: PER_SECOND(9) } }
    );
    try {
      await handler.start();
      await handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "pro" }
      );
      for (const name of ["acme:_:a", "acme:_:a:eu"]) {
        expect(limitOf(handler, name)?.[0]).toMatchObject({ maxTokens: 9 });
      }
    } finally {
      await handler.stop();
    }
  });

  it("reports at error level a stored plan the template no longer declares", async () => {
    const backend = memoryBackend();
    const { logger, lines } = recordingLogger();
    const builder = ((c: { instanceId: string }) => [
      { name: `acme:_:${c.instanceId}` },
    ]) as never;
    const first = new RequestHandler({ key: KEY, backend, keyPrefix: "tp" });
    await first.registerClientTemplate("acme" as never, builder, {
      rateLimitOptions: { pro: PER_SECOND(9) },
    });
    await first.start();
    await first.addTemplateClient(
      "acme" as never,
      { instanceId: "a" } as never,
      { rateLimitOption: "pro" }
    );

    // The same backend, now read by a deploy that renamed the plan.
    const renamed = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "tp",
      logger,
    });
    await renamed.registerClientTemplate("acme" as never, builder, {
      rateLimitOptions: { professional: PER_SECOND(9) },
    });
    try {
      await renamed.start();
      expect(
        lines.some(
          (l) => l.level === "error" && l.msg.includes('rateLimitOption "pro"')
        )
      ).toBe(true);
    } finally {
      await renamed.stop();
      await first.stop();
    }
  });
});

// ---- rebuilds

describe("a rebuild whose new config cannot be built", () => {
  const register = (handler: RequestHandler, baseURL: string) =>
    handler.registerClientTemplate(
      "acme" as never,
      ((c: { instanceId: string; limitType: string }) => [
        {
          name: `acme:_:${c.instanceId}`,
          rateLimit: [
            {
              type: c.limitType,
              interval: 2000,
              tokensToAdd: 1,
              maxTokens: 1,
            },
          ],
          requestOptions: { defaults: { baseURL } },
        },
      ]) as never
    );

  it("leaves the working client in place and keeps its parked requests", async () => {
    const upstream = await startAuthServer();
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "rb" });
    await register(handler, upstream.baseURL);
    try {
      await handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a", limitType: "requestLimit" } as never
      );
      const send = () =>
        handler.handleRequest({
          clientName: "acme:_:a",
          requestName: "t",
          method: "GET",
          url: "/x",
        });
      await send();
      // The bucket is empty, so this one parks until the refill.
      const parked = send();
      await sleep(50);

      await expect(
        handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a", limitType: "noSuchLimit" } as never
        )
      ).rejects.toBeInstanceOf(ConfigurationError);

      expect(limitOf(handler, "acme:_:a")?.[0]?.type).toBe("requestLimit");
      await expect(parked).resolves.toMatchObject({ status: 200 });
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 10_000);

  it("does not store a config that cannot be built, so a restart keeps the tenant", async () => {
    const upstream = await startAuthServer();
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "rb" });
    await register(handler, upstream.baseURL);
    await handler.addTemplateClient(
      "acme" as never,
      { instanceId: "a", limitType: "requestLimit" } as never
    );
    await expect(
      handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a", limitType: "noSuchLimit" } as never
      )
    ).rejects.toBeInstanceOf(ConfigurationError);

    const restarted = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "rb",
    });
    await register(restarted, upstream.baseURL);
    try {
      await restarted.start();
      expect(limitOf(restarted, "acme:_:a")?.[0]?.type).toBe("requestLimit");
    } finally {
      await restarted.stop();
      await handler.stop();
      await upstream.close();
    }
  });

  it("keeps the working client when a stored record from elsewhere fails to build", async () => {
    const upstream = await startAuthServer();
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "rb" });
    await register(handler, upstream.baseURL);
    try {
      await handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a", limitType: "requestLimit" } as never
      );
      // What a peer running a different builder could have written.
      const { encryptCredentials } =
        await import("../packages/core/src/utils/templateClients.js");
      await backend.set(
        "rb:requestHandler:template:acme::a",
        encryptCredentials({ instanceId: "a", limitType: "noSuchLimit" }, KEY)
      );

      await expect(
        handler.rebuildTemplateClient("acme", "a")
      ).rejects.toBeInstanceOf(ConfigurationError);
      expect(limitOf(handler, "acme:_:a")?.[0]?.type).toBe("requestLimit");
      await expect(
        handler.handleRequest({
          clientName: "acme:_:a",
          requestName: "t",
          method: "GET",
          url: "/x",
        })
      ).resolves.toMatchObject({ status: 200 });
    } finally {
      await handler.stop();
      await upstream.close();
    }
  });
});

// ---- sub-clients

describe("a sub-client", () => {
  it("uses its parent's httpAgent as the same object", async () => {
    const upstream = await startAuthServer();
    let connections = 0;
    class CountingAgent extends http.Agent {
      createConnection(
        ...args: Parameters<http.Agent["createConnection"]>
      ): ReturnType<http.Agent["createConnection"]> {
        connections++;
        return super.createConnection(...args);
      }
    }
    const agent = new CountingAgent({ keepAlive: true });
    const backend = memoryBackend();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "sc",
      defaultClientOptions: {
        name: "default",
        axiosOptions: { httpAgent: agent },
        requestOptions: { defaults: { baseURL: upstream.baseURL } },
        subClients: [{ name: "bulk" }],
      },
    });
    try {
      await expect(
        handler.handleRequest({
          clientName: "default:bulk",
          requestName: "t",
          method: "GET",
          url: "/x",
        })
      ).resolves.toMatchObject({ status: 200 });
      expect(connections).toBe(1);
    } finally {
      await handler.stop();
      agent.destroy();
      await upstream.close();
    }
  });

  it("sends its own token when it declares its own authentication", async () => {
    const upstream = await startAuthServer();
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "sc" });
    await handler.registerClientTemplate(
      "acme" as never,
      ((c: { instanceId: string }): CreateClientData[] => [
        {
          name: `acme:_:${c.instanceId}`,
          authentication: upstream.oauth("PARENT"),
          requestOptions: { defaults: { baseURL: upstream.baseURL } },
          subClients: [{ name: "eu", authentication: upstream.oauth("EU") }],
        },
      ]) as never
    );
    try {
      await handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never
      );
      for (const clientName of ["acme:_:a", "acme:_:a:eu"]) {
        await handler.handleRequest({
          clientName,
          requestName: "t",
          method: "GET",
          url: "/x",
        });
      }
      expect(upstream.bearers).toEqual([
        "Bearer TOKEN_PARENT_1",
        "Bearer TOKEN_EU_2",
      ]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  });
});

// ---- handler API

describe("the handler API", () => {
  it("registers a plugin's template when use() is called after start()", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "ha" });
    const plugin: RequestHandlerPlugin<"late", object> = {
      name: "late",
      registerTemplate: (h) =>
        h.registerClientTemplate(
          "late" as never,
          ((c: { instanceId: string }) => [
            { name: `late:_:${c.instanceId}` },
          ]) as never
        ),
      createRequests: () => ({}),
    };
    try {
      await handler.start();
      handler.use(plugin);
      expect(handler.getRegisteredTemplates()).toContain("late");
      await handler.addTemplateClient(
        "late" as never,
        { instanceId: "a" } as never
      );
      expect(handler.getMetadata().registeredClients).toContain("late:_:a");
    } finally {
      await handler.stop();
    }
  });

  it("rejects an addLocalTemplateClient instanceId that cannot be a name segment", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "ha" });
    await handler.registerClientTemplate(
      "acme" as never,
      ((c: { instanceId: string }) => [
        { name: `acme:_:${c.instanceId}` },
      ]) as never
    );
    try {
      await expect(
        handler.addLocalTemplateClient(
          "acme" as never,
          { instanceId: "a:b" } as never
        )
      ).rejects.toMatchObject({ code: "invalid_client_name_segment" });
      await expect(
        handler.addLocalTemplateClient(
          "acme" as never,
          { instanceId: "a", organizationId: "x:y" } as never
        )
      ).rejects.toMatchObject({ code: "invalid_client_name_segment" });
    } finally {
      await handler.stop();
    }
  });

  it("returns copies of each client's limits from getLoadedClients", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "ha",
      defaultClientOptions: { name: "default", rateLimit: PER_SECOND(5) },
    });
    try {
      await handler.start();
      const loaded = handler.getLoadedClients()[0]!.rateLimit;
      (loaded[0] as { maxTokens: number }).maxTokens = 500;
      loaded.push({ type: "noLimit", name: "extra" });
      expect(handler.getLoadedClients()[0]!.rateLimit).toEqual([
        { ...PER_SECOND(5)[0], name: "default" },
      ]);
    } finally {
      await handler.stop();
    }
  });
});

// ---- stored credentials, on every backend

for (const harness of harnesses(8)) {
  describe(`stored OAuth credentials (${harness.name})`, () => {
    it("are not written back by a refresh still in flight when the instance is removed", async () => {
      const upstream = await startAuthServer();
      upstream.setTokenDelay(300);
      const { backend, cleanup } = await harness.create();
      const prefix = uniquePrefix("purge");
      const handler = new RequestHandler({
        key: KEY,
        backend,
        keyPrefix: prefix,
      });
      await handler.registerClientTemplate(
        "acme" as never,
        ((c: { instanceId: string }): CreateClientData[] => [
          {
            name: `acme:_:${c.instanceId}`,
            authentication: upstream.oauth("A"),
            requestOptions: { defaults: { baseURL: upstream.baseURL } },
          },
        ]) as never
      );
      try {
        await handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never
        );
        const request = handler
          .handleRequest({
            clientName: "acme:_:a",
            requestName: "t",
            method: "GET",
            url: "/x",
          })
          .catch(() => undefined);
        await sleep(100);
        await handler.removeTemplateClient("acme" as never, "a");
        await sleep(400);
        await request;

        expect(upstream.tokenCalls).toEqual(["A"]);
        expect(
          await backend.hgetall(`${prefix}:requestHandler:acme:_:a:oauth2`)
        ).toEqual({});
      } finally {
        await handler.stop();
        await cleanup();
        await upstream.close();
      }
    }, 10_000);

    it("serves a token still valid when its early renewal fails", async () => {
      const upstream = await startAuthServer();
      // A 3s token renews from 1s on (the 2s floor on the margin) and is
      // valid until 3s.
      upstream.setExpiresIn(3);
      const { backend, cleanup } = await harness.create();
      const prefix = uniquePrefix("renew");
      const handler = new RequestHandler({
        key: KEY,
        backend,
        keyPrefix: prefix,
      });
      await handler.registerClientTemplate(
        "acme" as never,
        ((c: { instanceId: string }): CreateClientData[] => [
          {
            name: `acme:_:${c.instanceId}`,
            authentication: upstream.oauth("A"),
            requestOptions: { defaults: { baseURL: upstream.baseURL } },
            retryOptions: { maxRetries: 0 },
          },
        ]) as never
      );
      const send = () =>
        handler.handleRequest({
          clientName: "acme:_:a",
          requestName: "t",
          method: "GET",
          url: "/x",
        });
      try {
        await handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never
        );
        await send();
        upstream.setTokenStatus(503);
        await sleep(1300);
        await expect(send()).resolves.toMatchObject({ status: 200 });
        expect(upstream.bearers).toEqual([
          "Bearer TOKEN_A_1",
          "Bearer TOKEN_A_1",
        ]);

        await sleep(2000);
        await expect(send()).rejects.toBeDefined();
      } finally {
        await handler.stop();
        await cleanup();
        await upstream.close();
      }
    }, 15_000);

    it("tells a replica waiting on the refresh lock promptly when the holder's refresh fails", async () => {
      const upstream = await startAuthServer();
      upstream.setExpiresIn(3);
      const { backend, cleanup } = await harness.create();
      const prefix = uniquePrefix("peer");
      const builder = ((c: { instanceId: string }): CreateClientData[] => [
        {
          name: `acme:_:${c.instanceId}`,
          authentication: upstream.oauth("A"),
          requestOptions: { defaults: { baseURL: upstream.baseURL } },
          retryOptions: { maxRetries: 0 },
        },
      ]) as never;
      const holder = new RequestHandler({
        key: KEY,
        backend,
        keyPrefix: prefix,
      });
      const waiter = new RequestHandler({
        key: KEY,
        backend,
        keyPrefix: prefix,
      });
      await holder.registerClientTemplate("acme" as never, builder);
      await waiter.registerClientTemplate("acme" as never, builder);
      const send = (handler: RequestHandler) =>
        handler.handleRequest({
          clientName: "acme:_:a",
          requestName: "t",
          method: "GET",
          url: "/x",
        });
      try {
        await holder.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never
        );
        await waiter.start();
        await waiter.awaitClient("acme:_:a", { timeoutMs: 5000 });
        await send(holder);

        await sleep(1300);
        upstream.setTokenStatus(503);
        upstream.setTokenDelay(400);
        const fromHolder = send(holder);
        await sleep(100);
        const startedAt = Date.now();
        const fromWaiter = send(waiter);

        await expect(fromHolder).resolves.toMatchObject({ status: 200 });
        await expect(fromWaiter).resolves.toMatchObject({ status: 200 });
        expect(Date.now() - startedAt).toBeLessThan(1500);
        expect(upstream.tokenCalls).toHaveLength(2);
      } finally {
        await waiter.stop();
        await holder.stop();
        await cleanup();
        await upstream.close();
      }
    }, 15_000);
  });
}
