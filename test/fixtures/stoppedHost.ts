import { redisBackend } from "../../packages/backend-redis/src/index.js";
import RequestHandler from "../../packages/core/src/index.js";
import { writeSync } from "node:fs";
import { Redis } from "ioredis";

/**
 * A host process for test/clientTypes/processExit.test.ts: one handler on
 * Redis with a peer alive, one request to a slow upstream, then `stop()` with
 * a short budget and its own connection quit. Prints `closed <ms>` then, on
 * the way out, `exit <ms>`, so the test can measure what outlived `stop()`.
 *
 * Reads REDIS_URL, KEY_PREFIX and UPSTREAM.
 */

const redis = new Redis(process.env.REDIS_URL!);
const handler = new RequestHandler({
  key: "0123456789abcdef0123456789abcdef",
  backend: redisBackend(redis),
  keyPrefix: process.env.KEY_PREFIX,
});
await handler.registerClientTemplate(
  "hb" as never,
  (() => [
    {
      name: "hb:_:a",
      rateLimit: [{ type: "noLimit" }],
      requestOptions: { defaults: { baseURL: process.env.UPSTREAM } },
    },
  ]) as never
);
await handler.start();
await handler.addLocalTemplateClient(
  "hb" as never,
  { instanceId: "a" } as never
);
// Long enough to hear the peer's heartbeat.
await new Promise((r) => setTimeout(r, 1500));
handler.setDrainTimeout(1500);
const inFlight = handler
  .handleRequest({
    clientName: "hb:_:a",
    requestName: "hb.slow",
    method: "GET",
    url: "/",
  })
  .catch(() => undefined);
await new Promise((r) => setTimeout(r, 200));
await handler.stop();
await inFlight;
await redis.quit();
writeSync(1, `closed ${Date.now()}\n`);
process.on("exit", () => writeSync(1, `exit ${Date.now()}\n`));
