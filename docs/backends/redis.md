# Redis backend

```bash
npm install @dianemo/core @dianemo/backend-redis ioredis
```

```ts
import { Redis } from "ioredis";
import RequestHandler from "@dianemo/core";
import { redisBackend } from "@dianemo/backend-redis";

const redis = new Redis(process.env.REDIS_URL!);

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY!,
  backend: redisBackend(redis),
});
```

## Why this one

Every process talks to the same Redis, so the token bucket, the request queue
and the freeze state are **one set of numbers rather than one set per process**.
A vendor that allows 100 requests a second sees 100 requests a second whether
you run one replica or twenty.

It is also a perfectly good choice for a single process. If you are unsure which
backend you need, use this one.

## Versions

- **Redis 5.0 or later.** Several scripts write after reading `TIME`, which
  Redis allows without `redis.replicate_commands()` only from 5.0, when effects
  replication became the default. Redis 4 refuses them with "Write commands not
  allowed after non deterministic commands". CI runs against Redis 7.
- **`ioredis` `^5.4.0`**, the package's peer range.

## Valkey and DragonflyDB

Valkey is wire-compatible with Redis, so no separate adapter exists or is
needed. Construct its connection with `ioredis` and pass it to `redisBackend()`
unchanged.

DragonflyDB is not a drop-in replacement. Some scripts touch keys they do not
declare in `KEYS`, and Dragonfly refuses that by default with "script tried
accessing undeclared key". Starting it with
`--default_lua_flags=allow-undeclared-keys` should lift the refusal, but that
configuration is untested.

## You own the connection

The backend duplicates your connection for its pub/sub listener, because a
connection in subscriber mode cannot serve any other command. `close()` closes
**only that duplicate**.

The connection you passed in is yours. Dianemo will not quit a connection it did
not create, since in most processes that connection is doing other work too.

**One backend per handler, one handler per process.** A backend owns a single
pub/sub duplicate, and `close()` quits it — so sharing one backend between two
handlers means either handler's `stop()` leaves the other receiving no
coordination messages at all. Give each handler its own `redisBackend()`.

## An ioredis `keyPrefix`

A connection built with ioredis's own `keyPrefix` option is supported, and it
isolates fleets: two fleets on one Redis whose connections differ only in that
prefix share no keys and receive none of each other's pub/sub messages.

ioredis prepends the prefix to keys it can see as keys, but not to script
arguments or channel names. The backend reads the connection's
`options.keyPrefix` and applies it to those itself, on the pub/sub duplicate as
well. Handlers still receive the channel name without the prefix.

It stacks with the handler's own `keyPrefix` option rather than replacing it.
The handler's prefix goes inside the key names
[listed under Clearing a freeze by hand](../concepts.md#clearing-a-freeze-by-hand),
and the connection's goes in front of those, so with both set a freeze key is
stored as `<ioredis keyPrefix><keyPrefix>:requestHandler:<clientName>:freezeState`.
Either one keeps two fleets apart. The backend's methods take keys without the
connection's prefix, so pass the handler-level form to `clearFreezeState` and
the other manual calls.

## How atomicity is achieved

Every operation that spends from a shared budget has to be atomic against every
other process spending from it — between a read and a write from one process,
another will interleave. All such operations are therefore Lua scripts, executed
inside Redis rather than in the client.

Scripts are registered as ioredis custom commands, so calls send `EVALSHA`
rather than the full script body. Shipping the text every time sends kilobytes of
script per request across the scripts a single request touches, which becomes
megabytes a second at a few thousand requests a second. There is a one-time
`EVAL` fallback if Redis reports `NOSCRIPT` after a restart or a `SCRIPT FLUSH`.

## What it costs

The handler is **round-trip-bound, not Redis-bound**. In benchmarking, Redis sat
at 7.6% CPU with capacity for 312K commands/second against 63K consumed — the
limit was the number of sequential round-trips per request, not Redis itself.

That is why the uncontended fast path matters: it decides admission in a single
atomic script, taking a request from nine round-trips down to two, or one for a
`noLimit` client. See [benchmarks](../benchmarks.md).

## Operational notes

- **Redis must be reachable before `start()`.** The handler does not construct
  connections or read `REDIS_*` environment variables; you build the connection
  and hand it over.
- **Redis must not evict dianemo's keys.** Run it with
  `maxmemory-policy noeviction`, or give dianemo an instance that is not also a
  cache. An evicted token bucket is indistinguishable from one that was never
  written, and a bucket that was never written reads as a **full** budget — so
  the fleet resumes spending from the top of the interval without waiting for a
  refill. An evicted freeze reads as no freeze, and the fleet goes back to full
  rate into a vendor that has just rate limited you. Neither leaves a trace: no
  error, no log line, only the vendor's counters disagreeing with yours.

  **A TTL is not protection.** Nearly every key dianemo writes carries one — a
  token bucket expires a day after its last use, or once it could have refilled
  from empty if that takes longer; an instance registration in seconds — which
  makes them _candidates_ under `volatile-lru` and the other `volatile-*`
  policies rather than exempt from them. Only the instance and template
  registries, the template credentials and settings filed beside them, and each
  instance's client-name set
  (`requestHandler:templateClients:<template>::<instance>`, behind any
  `keyPrefix`) are TTL-less. `noeviction` is the setting; there is no safe middle ground.

- **A shared Redis is fine for collisions, not for memory.** Keys are prefixed,
  so dianemo can live alongside a cache or a queue in the same instance and
  neither sees the other's keys. What the two cannot share is an eviction
  policy, and a cache is usually tuned to evict. If you must share one instance,
  make it `noeviction` and size it for the whole working set — a cache can still
  bound itself with TTLs, which expire under `noeviction` exactly as they do
  under any other policy.
- **One logical keyspace only.** Several scripts touch keys they do not declare
  in `KEYS`, so anything that routes by key — Redis Cluster, a key-routing
  proxy, an active-active deployment — is unsupported. See
  [design notes](../design-notes.md#the-redis-backend-requires-one-logical-keyspace).
- **Failover.** ioredis handles reconnection. Coordination state is rebuilt from
  what remains; a lost queue means in-flight requests are re-driven by their
  callers rather than silently dropped.
- **Replica clock skew does not affect coordination.** Every script that decides
  fleet-wide state reads Redis's own clock with `TIME`, so a freeze deadline, a
  token bucket's `lastUpdate` and a concurrency slot's age are all written and
  compared against one clock no matter which replica got there first. You do not
  need replica clocks in sync for the budget to hold.

  The one value that does come from the calling process is a request's **arrival
  timestamp**, which is approximate by design: skew between replicas can reorder
  requests _within_ one priority band and never across bands. See
  [Concepts](../concepts.md#cost-and-priority).
