import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { redisBackend } from "../../packages/backend-redis/src/index.js";
import RequestHandler from "../../packages/core/src/index.js";
import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { build } from "esbuild";

/**
 * Real host processes, because what is under test is whether one exits: a
 * listener on a signal, or a timer left armed, is invisible inside the runner.
 *
 * The hosts are fixtures under test/fixtures/, bundled from this checkout's
 * sources before the suite runs. The Redis-backed case skips without
 * REDIS_URL; the signal cases run on the memory backend.
 */
const REDIS_URL = process.env.REDIS_URL;
const REDIS_DB = 9;
const repo = fileURLToPath(new URL("../../", import.meta.url));

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

let outdir: string;
const children = new Set<ChildProcess>();

beforeAll(async () => {
  outdir = mkdtempSync(join(tmpdir(), "dianemo-process-exit-"));
  await build({
    entryPoints: [
      join(repo, "test/fixtures/signalHost.ts"),
      join(repo, "test/fixtures/stoppedHost.ts"),
    ],
    outdir,
    outExtension: { ".js": ".mjs" },
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node20",
    // The subpath first: esbuild matches an alias as a prefix, so the bare
    // entry alone would send `@dianemo/core/backend` to `src/index.ts/backend`.
    alias: {
      "@dianemo/core/backend": join(repo, "packages/core/src/backend/index.ts"),
      "@dianemo/core": join(repo, "packages/core/src/index.ts"),
    },
    // ioredis is CommonJS and calls `require` for Node built-ins, which an ES
    // module bundle does not define.
    banner: {
      js: "import { createRequire } from 'node:module'; const require = createRequire(import.meta.url);",
    },
    logLevel: "warning",
  });
}, 60_000);

afterEach(() => {
  for (const child of children) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
  }
  children.clear();
});

afterAll(() => {
  rmSync(outdir, { recursive: true, force: true });
});

interface Exit {
  code: number | null;
  signal: NodeJS.Signals | null;
}

function host(fixture: string, env: Record<string, string>) {
  const child = spawn(process.execPath, [join(outdir, `${fixture}.mjs`)], {
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.add(child);
  let out = "";
  child.stdout?.on("data", (d) => (out += d));
  child.stderr?.on("data", (d) => (out += d));
  const exited = new Promise<Exit>((resolve) =>
    child.on("exit", (code, signal) => resolve({ code, signal }))
  );
  return {
    child,
    output: () => out,
    async waitFor(line: string, ms = 10_000) {
      const deadline = Date.now() + ms;
      while (!out.includes(line)) {
        if (Date.now() > deadline || child.exitCode !== null) {
          throw new Error(`host never printed "${line}"; output:\n${out}`);
        }
        await sleep(25);
      }
    },
    exit: (ms: number) =>
      Promise.race([exited, sleep(ms).then(() => "still running" as const)]),
  };
}

describe("signal handling in a host process", () => {
  it("lets a host process exit after SIGTERM once stop() has run", async () => {
    const proc = host("signalHost", { HOST_MODE: "plain" });
    await proc.waitFor("ready");
    proc.child.kill("SIGTERM");
    const exit = await proc.exit(10_000);
    expect({ exit, stopped: proc.output().includes("stopped") }).toEqual({
      exit: { code: null, signal: "SIGTERM" },
      stopped: true,
    });
  }, 30_000);

  it("lets a host process exit after SIGINT once stop() has run", async () => {
    const proc = host("signalHost", { HOST_MODE: "plain" });
    await proc.waitFor("ready");
    proc.child.kill("SIGINT");
    const exit = await proc.exit(10_000);
    expect({ exit, stopped: proc.output().includes("stopped") }).toEqual({
      exit: { code: null, signal: "SIGINT" },
      stopped: true,
    });
  }, 30_000);

  it("leaves the exit to a host that listens for the signal itself", async () => {
    const proc = host("signalHost", { HOST_MODE: "hostListener" });
    await proc.waitFor("ready");
    proc.child.kill("SIGTERM");
    const exit = await proc.exit(10_000);
    expect({ exit, stopped: proc.output().includes("stopped") }).toEqual({
      exit: { code: 7, signal: null },
      stopped: true,
    });
  }, 30_000);

  it.each(["onceBefore", "onceAfter"])(
    "leaves the exit to a host whose process.once SIGTERM listener is still shutting down (%s start())",
    async (mode) => {
      const proc = host("signalHost", { HOST_MODE: mode });
      await proc.waitFor("ready");
      proc.child.kill("SIGTERM");
      const exit = await proc.exit(10_000);
      expect({ exit, done: proc.output().includes("host-done") }).toEqual({
        exit: { code: 0, signal: null },
        done: true,
      });
    },
    30_000
  );

  it("exits at once on a second signal while stop() is still draining", async () => {
    const proc = host("signalHost", { HOST_MODE: "slowStop" });
    await proc.waitFor("ready");
    proc.child.kill("SIGINT");
    await proc.waitFor("stopping");
    // The drain budget is 30 s and the request upstream never returns.
    await sleep(300);
    expect(proc.child.exitCode).toBeNull();
    proc.child.kill("SIGINT");
    const exit = await proc.exit(5_000);
    expect({ exit, stopped: proc.output().includes("stopped") }).toEqual({
      exit: { code: null, signal: "SIGINT" },
      stopped: false,
    });
  }, 30_000);
});

describe.skipIf(!REDIS_URL)("host process exit after stop()", () => {
  it("exits promptly once stop() resolved and the host closed its connection, with a peer alive", async () => {
    const slow = createServer((_req, res) => {
      setTimeout(() => res.end("{}"), 4000);
    });
    await new Promise<void>((r) => slow.listen(0, "127.0.0.1", () => r()));
    const { port } = slow.address() as { port: number };
    const keyPrefix = `exit-${process.pid}-${Date.now()}`;
    const peerRedis = new Redis(`${REDIS_URL}/${REDIS_DB}`);
    const peer = new RequestHandler({
      key: "0123456789abcdef0123456789abcdef",
      backend: redisBackend(peerRedis),
      keyPrefix,
    });
    await peer.start();
    try {
      const proc = host("stoppedHost", {
        REDIS_URL: `${REDIS_URL}/${REDIS_DB}`,
        KEY_PREFIX: keyPrefix,
        UPSTREAM: `http://127.0.0.1:${port}`,
      });
      const exit = await proc.exit(30_000);
      const closed = Number(/closed (\d+)/.exec(proc.output())?.[1]);
      const exited = Number(/exit (\d+)/.exec(proc.output())?.[1]);
      expect({
        exit,
        livedAfterClose: exited - closed < 2000 ? "<2s" : exited - closed,
      }).toEqual({ exit: { code: 0, signal: null }, livedAfterClose: "<2s" });
    } finally {
      await peer.stop();
      await peerRedis.quit();
      await new Promise<void>((r) => slow.close(() => r()));
    }
  }, 60_000);
});
