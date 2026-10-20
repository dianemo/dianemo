import { memoryBackend } from "../packages/core/src/backend/memory.js";
import RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it } from "vitest";
import { createServer } from "node:http";

/**
 * `purgeClientCredentials` against a client-credentials upstream that mints
 * `TOKEN_<n>` per call to its token endpoint and records the bearer it is sent.
 */

const KEY = "0123456789abcdef0123456789abcdef";

async function startUpstream() {
  let tokenCalls = 0;
  const bearers: string[] = [];
  const server = createServer((req, res) => {
    if ((req.url ?? "").startsWith("/token")) {
      req.resume();
      req.on("end", () => {
        tokenCalls += 1;
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            access_token: `TOKEN_${tokenCalls}`,
            expires_in: 3600,
            token_type: "Bearer",
          })
        );
      });
      return;
    }
    bearers.push(String(req.headers.authorization ?? ""));
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const baseURL = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`;
  return {
    baseURL,
    bearers,
    tokenCalls: () => tokenCalls,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

type Upstream = Awaited<ReturnType<typeof startUpstream>>;

const oauthTemplate = (upstream: Upstream) => () => [
  {
    name: "acme:_:a",
    authentication: {
      type: "oauth2" as const,
      clientId: "ID",
      clientSecret: "SECRET",
      refreshConfig: {
        url: `${upstream.baseURL}/token`,
        dataLocation: "urlEncodedForm" as const,
        data: {
          grant_type: "client_credentials",
          client_id: "{{clientId}}",
          client_secret: "{{clientSecret}}",
        },
      },
      grantRefreshConfig: {
        url: `${upstream.baseURL}/token`,
        dataLocation: "urlEncodedForm" as const,
        data: { grant_type: "refresh_token" },
      },
    },
    requestOptions: { defaults: { baseURL: upstream.baseURL } },
  },
];

async function newHandler(
  upstream: Upstream,
  backend = memoryBackend(),
  keyPrefix = "pc"
) {
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix });
  await handler.registerClientTemplate(
    "acme" as never,
    oauthTemplate(upstream) as never
  );
  await handler.start();
  await handler.addTemplateClient(
    "acme" as never,
    { instanceId: "a" } as never
  );
  return handler;
}

const call = (handler: RequestHandler, grantId?: string) =>
  handler.handleRequest({
    clientName: "acme:_:a",
    requestName: "t.call",
    method: "GET",
    url: "/api",
    grantId,
  });

describe("purgeClientCredentials", () => {
  it("fetches a new token on the next request after a purge", async () => {
    const upstream = await startUpstream();
    const handler = await newHandler(upstream);
    try {
      await call(handler);
      await call(handler);
      expect(upstream.tokenCalls()).toBe(1);

      await handler.purgeClientCredentials("acme:_:a");
      await call(handler);

      expect(upstream.tokenCalls()).toBe(2);
      expect(upstream.bearers).toEqual([
        "Bearer TOKEN_1",
        "Bearer TOKEN_1",
        "Bearer TOKEN_2",
      ]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 30_000);

  it("purges every grant's token", async () => {
    const upstream = await startUpstream();
    const backend = memoryBackend();
    const handler = await newHandler(upstream, backend);
    const grantKey = (id: string) => `pc:requestHandler:acme:_:a:oauth2:${id}`;
    try {
      for (const id of ["g1", "g2"]) {
        await handler.setGrantTokens("acme:_:a", id, {
          accessToken: `AT_${id}`,
          refreshToken: `RT_${id}`,
          expiresAt: Date.now() + 3_600_000,
          refreshTokenExpiresAt: Date.now() + 86_400_000,
        });
        expect(await backend.hgetall(grantKey(id))).not.toEqual({});
      }

      await handler.purgeClientCredentials("acme:_:a");

      for (const id of ["g1", "g2"]) {
        expect(await backend.hgetall(grantKey(id))).toEqual({});
        await call(handler, id);
      }
      expect(upstream.tokenCalls()).toBe(2);
      expect(upstream.bearers).toEqual(["Bearer TOKEN_1", "Bearer TOKEN_2"]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 30_000);

  it("reaches a second handler on the same backend", async () => {
    const upstream = await startUpstream();
    const backend = memoryBackend();
    const a = await newHandler(upstream, backend);
    const b = await newHandler(upstream, backend);
    try {
      await call(b);
      expect(upstream.tokenCalls()).toBe(1);

      await a.purgeClientCredentials("acme:_:a");
      await call(b);

      expect(upstream.tokenCalls()).toBe(2);
      expect(upstream.bearers.at(-1)).toBe("Bearer TOKEN_2");
    } finally {
      await a.stop();
      await b.stop();
      await upstream.close();
    }
  }, 30_000);

  it("rejects an unknown client with ClientNotFoundError", async () => {
    const handler = new RequestHandler({
      key: KEY,
      backend: memoryBackend(),
      keyPrefix: "pc",
    });
    try {
      await expect(
        handler.purgeClientCredentials("nope:_:a")
      ).rejects.toMatchObject({ statusCode: 404, code: "client_not_found" });
    } finally {
      await handler.stop();
    }
  });

  it("rejects after stop with handler_stopped", async () => {
    const upstream = await startUpstream();
    const handler = await newHandler(upstream);
    try {
      await handler.stop();
      await expect(
        handler.purgeClientCredentials("acme:_:a")
      ).rejects.toMatchObject({ code: "handler_stopped" });
    } finally {
      await handler.stop();
      await upstream.close();
    }
  });
});
