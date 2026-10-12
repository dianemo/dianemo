import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { harnesses, withHandler, type ClientSpec } from "./harness.js";
import type RequestHandler from "../../packages/core/src/index.js";
import type { Logger } from "../../packages/core/src/logger.js";
import { createServer, type Server } from "node:http";
import { type AxiosError, isAxiosError } from "axios";
import type { Readable } from "node:stream";
import { inspect } from "node:util";
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
 * Streams `chunks` server-sent events `gap` ms apart, each padded by `pad`
 * bytes, then a usage event of `out` (or an error event with `error`), unless
 * `hang` is set. A scripted error status sends its body and, with `holdError`,
 * keeps it open. Records when each request, named by `id`, arrived and when its
 * response ended.
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
      if (url.searchParams.has("holdError")) {
        res.write('{"error":"overloaded"}');
        res.on("close", () => ends.set(`${id}:error`, Date.now()));
        return;
      }
      res.end("{}");
      ends.set(id, Date.now());
      return;
    }
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    const chunks = param("chunks", 3);
    const gap = param("gap", 20);
    const pad = "x".repeat(param("pad", 0));
    let sent = 0;
    const tick = () => {
      if (res.destroyed) return;
      if (sent < chunks) {
        res.write(`data: {"n":${sent++},"pad":"${pad}"}\n\n`);
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

/** The parts of a client these tests reach into. */
interface ClientInternals {
  logger: Logger;
}

function clientOf(handler: RequestHandler, name: string): ClientInternals {
  const clients = (
    handler as unknown as { clients: Map<string, ClientInternals> }
  ).clients;
  return clients.get(name)!;
}

/** Collects what `client` warns, leaving the rest of its logger in place. */
function captureWarnings(client: ClientInternals): string[] {
  const warnings: string[] = [];
  const logger: Logger = Object.create(client.logger);
  logger.warn = (...args: unknown[]) => {
    warnings.push(args.map(String).join(" "));
  };
  client.logger = logger;
  return warnings;
}

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

    it("completes an unread body that fits the relay's buffers", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        // ~96 KB: past one default 64 KiB buffer, within the relay's 128 KiB.
        const res = await stream(handler, "id=a&chunks=96&gap=1&pad=1000");
        const reason = await Promise.race([
          res.dianemo!.finished,
          sleep(2_000).then(() => "still open"),
        ]);
        res.data.destroy();
        expect(reason).toBe("completed");
      });
    }, 30_000);

    it("is kept alive past concurrencySlotTtl while the stream runs", async () => {
      await run(
        { rateLimit: [slot], requestOptions: { concurrencySlotTtl: 750 } },
        async (handler) => {
          const res = await stream(handler, "id=a&chunks=25&gap=100");
          const next = plain(handler, "b");
          await readAll(res.data);
          await next;
          expect(upstream.arrivals.get("b")!).toBeGreaterThanOrEqual(
            upstream.ends.get("a")!
          );
        }
      );
    }, 30_000);

    it("refreshes its claim marks more often than a short settleWindow lets them live", async () => {
      // Refills from empty in 500 ms, so with a 600 ms settleWindow its marks
      // live the whole-second minimum.
      const bucket = { ...otpm, interval: 500 };
      await run(
        { rateLimit: [slot, bucket], requestOptions: { settleWindow: 600 } },
        async (handler, backend) => {
          const touches: { at: number; ttlSeconds: number }[] = [];
          const touch = backend.touchClaimMark!.bind(backend);
          backend.touchClaimMark = async (key, markId, ttlSeconds) => {
            touches.push({ at: Date.now(), ttlSeconds });
            return touch(key, markId, ttlSeconds);
          };
          const sent = Date.now();
          const res = await stream(handler, "id=a&chunks=12&gap=100", {
            cost: { slots: 1, otpm: 10 },
            settle: () => ({ slots: 1, otpm: 10 }),
          });
          await readAll(res.data);
          await res.dianemo!.finished;
          expect(touches.length).toBeGreaterThanOrEqual(2);
          const times = [sent, ...touches.map((touch) => touch.at)];
          const gaps = times.slice(1).map((at, i) => at - times[i]);
          expect(Math.max(...gaps)).toBeLessThan(1_000);
          expect(touches.every((touch) => touch.ttlSeconds === 1)).toBe(true);
        }
      );
    }, 30_000);

    it("does not refresh the backend more often than a claim mark's whole-second TTL needs", async () => {
      await run(
        { rateLimit: [slot, otpm], requestOptions: { settleWindow: 1 } },
        async (handler, backend) => {
          let touches = 0;
          const touch = backend.touchConcurrency!.bind(backend);
          backend.touchConcurrency = async (...args) => {
            touches++;
            return touch(...args);
          };
          const res = await stream(handler, "id=a&chunks=5&gap=100", {
            cost: { slots: 1, otpm: 10 },
            settle: () => ({ slots: 1, otpm: 10 }),
          });
          await readAll(res.data);
          await res.dianemo!.finished;
          // An hour's mark and the default two-minute slot need nothing in 0.5 s.
          expect(touches).toBeLessThan(10);
        }
      );
    }, 30_000);

    it("does not hammer the backend when concurrencySlotTtl is very long", async () => {
      // A third of 1e10 ms is past setInterval's 2^31-1 ms limit, above which
      // Node fires every 1 ms.
      await run(
        { rateLimit: [slot], requestOptions: { concurrencySlotTtl: 1e10 } },
        async (handler, backend) => {
          let touches = 0;
          const touch = backend.touchConcurrency!.bind(backend);
          backend.touchConcurrency = async (...args) => {
            touches++;
            return touch(...args);
          };
          const res = await stream(handler, "id=a&chunks=5&gap=100");
          await readAll(res.data);
          await res.dianemo!.finished;
          expect(touches).toBeLessThan(10);
        }
      );
    }, 30_000);

    it("warns when a keep-alive finds the slot already gone", async () => {
      await run(
        { rateLimit: [slot], requestOptions: { concurrencySlotTtl: 300 } },
        async (handler, backend) => {
          const warnings = captureWarnings(clientOf(handler, CLIENT));
          backend.touchConcurrency = async () => false;
          const res = await stream(handler, "id=a&chunks=5&gap=100");
          await readAll(res.data);
          expect(
            warnings.some((line) => /no longer holds its slot/.test(line))
          ).toBe(true);
        }
      );
    }, 30_000);

    it("warns once, not on every keep-alive tick, when the slot is gone", async () => {
      await run(
        { rateLimit: [slot], requestOptions: { concurrencySlotTtl: 150 } },
        async (handler, backend) => {
          const warnings = captureWarnings(clientOf(handler, CLIENT));
          backend.touchConcurrency = async () => false;
          const res = await stream(handler, "id=a&chunks=10&gap=100");
          await readAll(res.data);
          await res.dianemo!.finished;
          const lost = warnings.filter((line) =>
            /no longer holds its slot/.test(line)
          );
          expect(lost.length).toBe(1);
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

    it("settles at the last chunk when the caller reads after the upstream finished", async () => {
      await run({ rateLimit: [otpm] }, async (handler, backend) => {
        const res = await stream(
          handler,
          "id=a&chunks=200&gap=1&pad=1000&out=400",
          { cost: 100, settleStream: usageObserver }
        );
        await sleep(1_000);
        for await (const _chunk of res.data) await sleep(10);
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

  describe("an error at the headers", () => {
    it("closes the body of an attempt it retries", async () => {
      await run(
        {
          rateLimit: [slot],
          retryOptions: { maxRetries: 1, retryBackoffBaseTime: 10 },
        },
        async (handler) => {
          upstream.respond(503);
          const res = await stream(handler, "id=a&holdError&chunks=0");
          await readAll(res.data);
          await sleep(200);
          expect(upstream.ends.has("a:error")).toBe(true);
        }
      );
    }, 30_000);

    it("hands the caller the error body as a readable stream", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        upstream.respond(400);
        const error = await stream(handler, "id=a&holdError").catch(
          (caught: unknown) => caught
        );
        expect(isAxiosError(error)).toBe(true);
        const body = (error as AxiosError<Readable>).response?.data;
        expect(typeof body?.pipe).toBe("function");
        body?.destroy();
      });
    }, 30_000);

    it("relays the whole error body to a caller that reads it", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        upstream.respond(400);
        const error = await stream(handler, "id=a").catch(
          (caught: unknown) => caught
        );
        const body = (error as AxiosError<Readable>).response!.data!;
        expect(await readAll(body)).toBe("{}");
      });
    }, 30_000);

    it("closes the upstream connection when the caller destroys the error body", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        upstream.respond(400);
        const error = await stream(handler, "id=a&holdError").catch(
          (caught: unknown) => caught
        );
        (error as AxiosError<Readable>).response!.data!.destroy();
        await sleep(200);
        expect(upstream.ends.has("a:error")).toBe(true);
      });
    }, 30_000);

    it("lets the next request through once a terminal error's body is destroyed", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        upstream.respond(500);
        const error = await stream(handler, "id=a&holdError").catch(
          (caught: unknown) => caught
        );
        (error as AxiosError<Readable>).response!.data!.destroy();
        const res = await stream(handler, "id=b");
        await readAll(res.data);
        expect(await res.dianemo!.finished).toBe("completed");
      });
    }, 30_000);

    it("keeps the request's credentials out of a thrown streamed error body", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        upstream.respond(400);
        const error = await stream(handler, "id=a&holdError", {
          headers: { Authorization: "Bearer s3cr3t-value-that-must-not-leak" },
        }).catch((caught: unknown) => caught);
        const dump = inspect(error, { depth: null });
        (error as AxiosError<Readable>).response?.data?.destroy();
        expect(dump).not.toContain("s3cr3t-value");
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

    it("keeps credentials out of the error a mid-stream failure destroys the caller's stream with", async () => {
      await run({ rateLimit: [slot] }, async (handler) => {
        const res = await stream(handler, "id=overflow&chunks=20&gap=5", {
          headers: { Authorization: "Bearer s3cr3t-value-that-must-not-leak" },
          maxContentLength: 60,
        });
        const error = await readAll(res.data).then(
          () => undefined,
          (caught: unknown) => caught
        );
        await res.dianemo!.finished;
        expect(error).toBeDefined();
        expect(inspect(error, { depth: null })).not.toContain("s3cr3t-value");
      });
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

    it("lets an admitted request in a slow interceptor finish during a graceful stop", async () => {
      await run(
        {
          rateLimit: [slot],
          requestOptions: {
            requestInterceptor: async (config: unknown) => {
              await sleep(300);
              return config;
            },
          },
        },
        async (handler) => {
          const pending = stream(handler, "id=a&chunks=1").then(
            async (res) => {
              await readAll(res.data);
              return "ok";
            },
            (error: unknown) => error
          );
          await sleep(50);
          await handler.stop();
          expect(await pending).toBe("ok");
        }
      );
    }, 30_000);
  });
});
