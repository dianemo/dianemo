import type { OAuthRefreshConfig } from "../packages/core/src/client/types.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it } from "vitest";
import { createServer } from "node:http";

/**
 * A `requestInterceptor` on an OAuth2 `refreshConfig`, against a token endpoint
 * that records the form body of every token request it is sent.
 */

const KEY = "0123456789abcdef0123456789abcdef";

type RefreshInterceptor = NonNullable<OAuthRefreshConfig["requestInterceptor"]>;

async function startUpstream() {
  const tokenBodies: Array<Record<string, string>> = [];
  const server = createServer((req, res) => {
    if ((req.url ?? "").startsWith("/token")) {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        tokenBodies.push(Object.fromEntries(new URLSearchParams(body)));
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify({
            access_token: `TOKEN_${tokenBodies.length}`,
            expires_in: 3600,
            token_type: "Bearer",
          })
        );
      });
      return;
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const baseURL = `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`;
  return {
    baseURL,
    tokenBodies,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

type Upstream = Awaited<ReturnType<typeof startUpstream>>;

const jwtBearerTemplate =
  (upstream: Upstream, requestInterceptor: RefreshInterceptor) => () => [
    {
      name: "signer:_:a",
      authentication: {
        type: "oauth2" as const,
        clientId: "ID",
        clientSecret: "SECRET",
        refreshConfig: {
          url: `${upstream.baseURL}/token`,
          dataLocation: "urlEncodedForm" as const,
          data: {
            grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
            assertion: "",
          },
          requestInterceptor,
        },
      },
      requestOptions: { defaults: { baseURL: upstream.baseURL } },
    },
  ];

async function newHandler(
  upstream: Upstream,
  requestInterceptor: RefreshInterceptor,
  add: "addTemplateClient" | "addLocalTemplateClient" = "addTemplateClient"
) {
  const handler = new RequestHandler({
    key: KEY,
    backend: memoryBackend(),
    keyPrefix: "ri",
  });
  await handler.registerClientTemplate(
    "signer" as never,
    jwtBearerTemplate(upstream, requestInterceptor) as never
  );
  await handler.start();
  await handler[add]("signer" as never, { instanceId: "a" } as never);
  return handler;
}

const call = (handler: RequestHandler) =>
  handler.handleRequest({
    clientName: "signer:_:a",
    requestName: "t.call",
    method: "GET",
    url: "/api",
  });

const sign: RefreshInterceptor = (config) => ({
  ...config,
  data: { ...config.data, assertion: "signed" },
});

describe("refreshConfig.requestInterceptor", () => {
  it("sends the token request the interceptor returns", async () => {
    const upstream = await startUpstream();
    const handler = await newHandler(upstream, sign);
    try {
      const res = await call(handler);

      expect(res.status).toBe(200);
      expect(upstream.tokenBodies).toEqual([
        {
          grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
          assertion: "signed",
        },
      ]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 30_000);

  it("awaits an async interceptor", async () => {
    const upstream = await startUpstream();
    const handler = await newHandler(upstream, async (config) => {
      await new Promise((r) => setTimeout(r, 5));
      return sign(config);
    });
    try {
      await call(handler);

      expect(upstream.tokenBodies.map((body) => body.assertion)).toEqual([
        "signed",
      ]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 30_000);

  it("runs for a client added with addLocalTemplateClient", async () => {
    const upstream = await startUpstream();
    const handler = await newHandler(upstream, sign, "addLocalTemplateClient");
    try {
      await call(handler);

      expect(upstream.tokenBodies.map((body) => body.assertion)).toEqual([
        "signed",
      ]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 30_000);

  it("hands the interceptor a copy, so writing into it does not reach the next refresh", async () => {
    const upstream = await startUpstream();
    const handler = await newHandler(upstream, (config) => {
      config.data.assertion += "signed";
      return config;
    });
    try {
      await call(handler);
      await handler.purgeClientCredentials("signer:_:a");
      await call(handler);

      expect(upstream.tokenBodies.map((body) => body.assertion)).toEqual([
        "signed",
        "signed",
      ]);
    } finally {
      await handler.stop();
      await upstream.close();
    }
  }, 30_000);
});
