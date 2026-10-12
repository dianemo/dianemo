import { createServer } from "node:http";
import { writeSync } from "node:fs";
import RequestHandler, {
  memoryBackend,
} from "../../packages/core/src/index.js";

/**
 * A host process for test/clientTypes/processExit.test.ts, which bundles it
 * from source and signals it. Prints `ready` once the handler has started,
 * `stopping` when `stop()` is called and `stopped` when it resolves.
 *
 * HOST_MODE:
 * - `plain`: no signal listener of its own.
 * - `hostListener`: listens for SIGTERM and exits with status 7 itself.
 * - `slowStop`: holds a request upstream so `stop()` takes its whole budget.
 */

// Synchronous: a write to a pipe is asynchronous on macOS, and the process
// may end by signal before an asynchronous one flushes.
function say(line: string): void {
  writeSync(1, `${line}\n`);
}

const mode = process.env.HOST_MODE ?? "plain";

// Stands in for what a real host keeps open past `stop()`, such as its own
// server or database connection.
setInterval(() => {}, 60_000);

const handler = new RequestHandler({
  key: "0123456789abcdef0123456789abcdef",
  backend: memoryBackend(),
});
const stop = handler.stop.bind(handler);
handler.stop = async () => {
  say("stopping");
  await stop();
  say("stopped");
};

if (mode === "hostListener") {
  process.on("SIGTERM", () => {
    void handler.stop().then(() => {
      setTimeout(() => process.exit(7), 300);
    });
  });
}

await handler.start();

if (mode === "slowStop") {
  handler.setDrainTimeout(30_000);
  const upstream = createServer(() => {
    say("ready");
  });
  await new Promise<void>((r) => upstream.listen(0, "127.0.0.1", () => r()));
  const { port } = upstream.address() as { port: number };
  void handler
    .handleRequest({
      clientName: "default",
      requestName: "fixture.hang",
      method: "GET",
      url: `http://127.0.0.1:${port}/`,
    })
    .catch(() => undefined);
} else {
  say("ready");
}
