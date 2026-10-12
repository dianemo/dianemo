import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { harnesses, withHandler, type ClientSpec } from "./harness.js";
import { createServer, type Server } from "node:http";
import type { Readable } from "node:stream";
import type {
  DianemoResponse,
  StreamObserver,
  StreamResult,
} from "../../packages/core/src/request/types.js";
import {
  RequestAbortedError,
  StreamClosedError,
  StreamTimeoutError,
} from "../../packages/core/src/errors.js";

/**
 * A streamed response holds its slot, stays in flight and stays alive until
 * the stream ends, and settles at what the stream reported.
 */

/**
 * Streams `chunks` server-sent events `gap` ms apart, then a usage event of
 * `out` (or an error event with `error`), unless `hang` is set. Records when
 * each request, named by `id`, arrived and when its response ended.
 */
async function startStreamingUpstream() {
  const arrivals = new Map<string, number>();
  const ends = new Map<string, number>();
  let script: number[] = [];
  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    const param = (name: string, fallback: number) =>
      Number(url.searchParams.get(name) ?? fallback);
    const id = url.searchParams.get("id") ?? "";
    arrivals.set(id, Date.now());
    const status = script.shift() ?? 200;
    if (status !== 200) {
      res.writeHead(status, { "Content-Type": "application/json" });
      res.end("{}");
      ends.set(id, Date.now());
      return;
    }
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    const chunks = param("chunks", 3);
    const gap = param("gap", 20);
    let sent = 0;
    const tick = () => {
      if (res.destroyed) return;
      if (sent < chunks) {
        res.write(`data: {"n":${sent++}}\n\n`);
        setTimeout(tick, gap);
        return;
      }
      if (url.searchParams.has("hang")) return;
      res.end(
        url.searchParams.has("error")
          ? `data: {"error":"overloaded_error"}\n\n`
          : `data: {"usage":${param("out", 0)}}\n\n`
      );
      ends.set(id, Date.now());
    };
    res.on("close", () => {
      if (!ends.has(id)) ends.set(id, Date.now());
    });
    tick();
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;
  return {
    baseURL: `http://127.0.0.1:${port}`,
    arrivals,
    ends,
    /** Statuses for the next requests, in order; 200 after that. */
    respond: (...statuses: number[]) => {
      script = statuses;
    },
    reset: () => {
      arrivals.clear();
      ends.clear();
      script = [];
    },
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}

/** Reads a stream to its end. */
function readAll(stream: Readable): Promise<string> {
  return new Promise((resolve, reject) => {
    let text = "";
    stream.on("data", (chunk: Buffer) => {
      text += chunk.toString();
    });
    stream.on("end", () => resolve(text));
    stream.on("error", reject);
  });
}

/**
 * Reads what the upstream streams: the usage event as output tokens, an error
 * event as an error, and otherwise ten tokens per event seen.
 */
function usageObserver(): StreamObserver {
  let text = "";
  return {
    onChunk: (chunk) => {
      text += chunk.toString();
    },
    result: (): StreamResult => {
      if (text.includes('"error"')) return { error: "overloaded_error" };
      const usage = /"usage":(\d+)/.exec(text);
      if (usage) return { otpm: Number(usage[1]) };
      return { otpm: (text.match(/"n":/g) ?? []).length * 10 };
    },
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe.each(harnesses(15))("streaming — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startStreamingUpstream>>;
  beforeAll(async () => {
    upstream = await startStreamingUpstream();
  });
  beforeEach(() => upstream.reset());
  afterAll(async () => {
    await upstream.close();
  });

  const CLIENT = "ai:_:a";
  const OTPM = "ct:requestHandler:ai:_:a:rateLimit:otpm";
  const otpmConfig = {
    maxTokens: 1_000,
    tokensToAdd: 1_000,
    interval: 3_600_000,
  };
  const slot = { name: "slots", type: "concurrencyLimit", maxConcurrency: 1 };
  const otpm = { name: "otpm", type: "requestLimit", ...otpmConfig };

  const run = (
    spec: Omit<ClientSpec, "name">,
    fn: Parameters<typeof withHandler>[3]
  ) =>
    withHandler(
      harness,
      upstream.baseURL,
      [
        {
          name: "ai",
          retryOptions: { maxRetries: 0, retryBackoffBaseTime: 20 },
          ...spec,
        },
      ],
      fn
    );

  type Handler = Parameters<Parameters<typeof withHandler>[3]>[0];

  const stream = (
    handler: Handler,
    query: string,
    extra: Record<string, unknown> = {}
  ) =>
    handler.handleRequest({
      clientName: CLIENT,
      requestName: "ai.stream",
      method: "GET",
      url: `/?${query}`,
      responseType: "stream",
      ...extra,
    }) as Promise<DianemoResponse<Readable>>;

  const plain = (handler: Handler, id: string, cost?: unknown) =>
    handler.handleRequest({
      clientName: CLIENT,
      requestName: "ai.plain",
      method: "GET",
      url: `/?id=${id}&chunks=0`,
      ...(cost === undefined ? {} : { cost: cost as never }),
    });

  const balance = async (
    backend: Parameters<Parameters<typeof withHandler>[3]>[1]
  ) => (await backend.getTokenBucketState(OTPM, otpmConfig)).tokens;

  describe("the slot", () => {
    it("is held until the stream ends, not released at the headers", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        const res = await stream(handler, "id=a&chunks=5&gap=60");
        const next = plain(handler, "b");
        await readAll(res.data);
        await next;
        expect(upstream.arrivals.get("b")!).toBeGreaterThanOrEqual(
          upstream.ends.get("a")!
        );
      });
    }, 30_000);

    it("is kept alive past concurrencySlotTtl while the stream runs", async () => {
      await run(
        { rateLimit: [slot], requestOptions: { concurrencySlotTtl: 300 } },
        async (handler) => {
          const res = await stream(handler, "id=a&chunks=10&gap=100");
          const next = plain(handler, "b");
          await readAll(res.data);
          await next;
          expect(upstream.arrivals.get("b")!).toBeGreaterThanOrEqual(
            upstream.ends.get("a")!
          );
        }
      );
    }, 30_000);

    it("is released when the caller closes the stream", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        const res = await stream(handler, "id=a&hang");
        res.data.destroy();
        expect(await res.dianemo!.finished).toBe("closed");
        await plain(handler, "b");
        expect(upstream.arrivals.has("b")).toBe(true);
      });
    }, 30_000);

    it("is released when the request's signal aborts mid-stream", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        const controller = new AbortController();
        const res = await stream(handler, "id=a&hang", {
          signal: controller.signal,
        });
        const read = readAll(res.data).catch((error: unknown) => error);
        controller.abort();
        const error = await read;
        expect(error).toBeInstanceOf(RequestAbortedError);
        expect((error as Error).message).toMatch(/while its response streamed/);
        expect(await res.dianemo!.finished).toBe("closed");
        await plain(handler, "b");
        expect(upstream.arrivals.has("b")).toBe(true);
      });
    }, 30_000);

    it("is released when streamTimeout closes the stream", async () => {
      await run(
        { rateLimit: [slot], requestOptions: { streamTimeout: 300 } },
        async (handler) => {
          const res = await stream(handler, "id=a&hang");
          await expect(readAll(res.data)).rejects.toBeInstanceOf(
            StreamTimeoutError
          );
          expect(await res.dianemo!.finished).toBe("timeout");
          await plain(handler, "b");
          expect(upstream.arrivals.has("b")).toBe(true);
        }
      );
    }, 30_000);
  });

  describe("settlement", () => {
    it("settles at what the observer read from the stream", async () => {
      await run({ rateLimit: [otpm] }, async (handler, backend) => {
        const res = await stream(handler, "id=a&out=400", {
          cost: 100,
          settleStream: usageObserver,
        });
        await readAll(res.data);
        await res.dianemo!.finished;
        expect(await balance(backend)).toBe(600);
      });
    }, 30_000);

    it("settles a stream cut short at what it generated", async () => {
      await run({ rateLimit: [otpm] }, async (handler, backend) => {
        const res = await stream(handler, "id=a&chunks=100&gap=20", {
          cost: 500,
          settleStream: usageObserver,
        });
        const read: string[] = [];
        res.data.on("data", (chunk: Buffer) => {
          read.push(chunk.toString());
          if (read.length === 3) res.data.destroy();
        });
        expect(await res.dianemo!.finished).toBe("closed");
        // Each event the observer saw was ten tokens; far fewer than reserved.
        expect(await balance(backend)).toBeGreaterThan(500);
      });
    }, 30_000);

    it("settles at what the caller reports through dianemo.settle", async () => {
      await run({ rateLimit: [otpm] }, async (handler, backend) => {
        const res = await stream(handler, "id=a&out=400", { cost: 100 });
        await readAll(res.data);
        await res.dianemo!.settle({ otpm: 250 });
        await res.dianemo!.settle({ otpm: 900 });
        expect(await balance(backend)).toBe(750);
      });
    }, 30_000);

    it("lets the first settlement win over the observer's", async () => {
      await run({ rateLimit: [otpm] }, async (handler, backend) => {
        const res = await stream(handler, "id=a&out=400&chunks=5&gap=40", {
          cost: 100,
          settleStream: usageObserver,
        });
        await res.dianemo!.settle({ otpm: 50 });
        await readAll(res.data);
        await res.dianemo!.finished;
        expect(await balance(backend)).toBe(950);
      });
    }, 30_000);

    it("refuses an observer on a backend that cannot settle", async () => {
      await run({ rateLimit: [otpm] }, async (handler, backend) => {
        (backend as { settleTokens?: unknown }).settleTokens = undefined;
        await expect(
          stream(handler, "id=a", { settleStream: usageObserver })
        ).rejects.toThrow(/backend implementing settleTokens/);
      });
    }, 30_000);
  });

  describe("errors inside the stream", () => {
    it("freezes the client when the observer reports one", async () => {
      await run(
        {
          rateLimit: [slot],
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 600,
            retryBackoffMethod: "linear",
          },
        },
        async (handler) => {
          const res = await stream(handler, "id=a&error", {
            settleStream: usageObserver,
          });
          await readAll(res.data);
          await res.dianemo!.finished;
          await sleep(50);
          const before = Date.now();
          await plain(handler, "b");
          expect(upstream.arrivals.get("b")! - before).toBeGreaterThanOrEqual(
            400
          );
        }
      );
    }, 30_000);
  });

  describe("a thaw probe", () => {
    it("lets the fleet thaw at its headers rather than its end", async () => {
      const requests = {
        name: "rpm",
        type: "requestLimit",
        interval: 50,
        tokensToAdd: 100,
        maxTokens: 100,
      };
      await run(
        {
          rateLimit: [requests],
          retryOptions: {
            maxRetries: 0,
            retryBackoffBaseTime: 10,
            retryBackoffMethod: "linear",
            thawRequestCount: 1,
          },
        },
        async (handler) => {
          upstream.respond(429);
          await expect(plain(handler, "x")).rejects.toThrow();
          await sleep(150);
          const res = await stream(handler, "id=a&chunks=10&gap=100");
          await plain(handler, "b");
          expect(upstream.arrivals.get("b")!).toBeLessThan(
            upstream.ends.get("a") ?? Infinity
          );
          await readAll(res.data);
        }
      );
    }, 30_000);
  });

  describe("shutdown", () => {
    it("closes a stream still open when the handler stops", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        const res = await stream(handler, "id=a&hang");
        // Caught now: it rejects inside `stop()`, before the assertion below.
        const read = readAll(res.data).catch((error: unknown) => error);
        handler.setDrainTimeout(200);
        await handler.stop();
        expect(await read).toBeInstanceOf(StreamClosedError);
        expect(await res.dianemo!.finished).toBe("shutdown");
      });
    }, 30_000);
  });
});
