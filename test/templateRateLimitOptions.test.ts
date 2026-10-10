import type { DeclaredRateLimit } from "../packages/core/src/client/types.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { ConfigurationError } from "../packages/core/src/errors.js";
import RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it } from "vitest";
import type {
  ClientTemplateContext,
  ClientTemplateOptions,
} from "../packages/core/src/types.js";

/**
 * Rate limits a template lets its callers choose between.
 *
 * The point of the mechanism is what a caller *cannot* do: supply a limit the
 * template never wrote. So the assertions come in pairs — the chosen plan really
 * does reach the client, and anything the template did not declare is refused at
 * the boundary rather than quietly ignored. The boundary matters because the key
 * is stored: one accepted now is read back by every replica and every restart,
 * and would leave a client on the template default with its record saying
 * otherwise.
 */

const KEY = "0123456789abcdef0123456789abcdef";

const FREE = [
  {
    type: "requestLimit" as const,
    interval: 1000,
    tokensToAdd: 2,
    maxTokens: 2,
  },
];
const PRO = [
  {
    name: "per_second",
    type: "requestLimit" as const,
    interval: 1000,
    tokensToAdd: 20,
    maxTokens: 20,
  },
  {
    name: "per_day",
    type: "requestLimit" as const,
    interval: 86_400_000,
    tokensToAdd: 50_000,
    maxTokens: 50_000,
  },
];

interface Rig {
  handler: RequestHandler;
  backend: ReturnType<typeof memoryBackend>;
  /** What the builder was handed on its most recent call. */
  seen: () => ClientTemplateContext | undefined;
  stop: () => Promise<void>;
}

/**
 * `fallback` is what the builder puts on the client when no plan reaches it,
 * standing in for a template's own hard-coded default.
 */
async function build(
  options: ClientTemplateOptions,
  fallback: unknown = [{ type: "noLimit" }]
): Promise<Rig> {
  const backend = memoryBackend();
  const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "tro" });
  let context: ClientTemplateContext | undefined;

  await handler.registerClientTemplate(
    "acme" as never,
    ((creds: { instanceId: string }, ctx: ClientTemplateContext) => {
      context = ctx;
      return [
        {
          name: `acme:_:${creds.instanceId}`,
          // The whole point of handing the builder the limit rather than
          // merging it: the builder decides which client it lands on.
          rateLimit: ctx.rateLimit ?? fallback,
        },
      ];
    }) as never,
    options
  );
  await handler.start();

  return {
    handler,
    backend,
    seen: () => context,
    stop: async () => {
      await handler.stop();
      await backend.close();
    },
  };
}

/**
 * A declared plan as the client will report it: every limit named, with an
 * omitted name resolved to `default`. Mirrors what `createClient` does, so these
 * assertions read as the plans were written.
 */
const withNames = (limits: DeclaredRateLimit[]) =>
  limits.map((limit) => ({ ...limit, name: limit.name ?? "default" }));

const limitOf = (handler: RequestHandler) =>
  handler.getLoadedClients().find((c) => c.name === "acme:_:a")?.rateLimit;

describe("rate limits a template offers its callers", () => {
  it("hands the builder the chosen plan's limit and its key", async () => {
    const rig = await build({ rateLimitOptions: { free: FREE, pro: PRO } });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "pro" }
      );

      expect(rig.seen()?.rateLimitOption).toBe("pro");
      expect(rig.seen()?.rateLimit).toEqual(PRO);
      expect(limitOf(rig.handler)).toEqual(withNames(PRO));
    } finally {
      await rig.stop();
    }
  });

  it("applies the declared default when the caller names no plan", async () => {
    const rig = await build({
      rateLimitOptions: { free: FREE, pro: PRO },
      defaultRateLimitOption: "free",
    });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        {
          instanceId: "a",
        } as never
      );

      expect(rig.seen()?.rateLimitOption).toBe("free");
      expect(limitOf(rig.handler)).toEqual(withNames(FREE));
    } finally {
      await rig.stop();
    }
  });

  it("tells a builder nothing when the template offers nothing", async () => {
    // A template that declares no options gets the same empty context every
    // build, which is what keeps a builder written before any of this existed
    // behaving exactly as it did.
    const rig = await build({});
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        {
          instanceId: "a",
        } as never
      );

      expect(rig.seen()).toEqual({});
      expect(limitOf(rig.handler)).toEqual(withNames([{ type: "noLimit" }]));
    } finally {
      await rig.stop();
    }
  });

  it("refuses a plan the template does not offer, and names the ones it does", async () => {
    const rig = await build({ rateLimitOptions: { free: FREE, pro: PRO } });
    try {
      await expect(
        rig.handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never,
          { rateLimitOption: "enterprise" }
        )
      ).rejects.toThrow(/no rateLimitOption "enterprise".*"free", "pro"/s);
    } finally {
      await rig.stop();
    }
  });

  it("refuses any plan for a template that declares none", async () => {
    const rig = await build({});
    try {
      await expect(
        rig.handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never,
          { rateLimitOption: "pro" }
        )
      ).rejects.toThrow(/declares no rateLimitOptions/);
    } finally {
      await rig.stop();
    }
  });

  it("refuses the same plan on the per-replica path", async () => {
    // `addLocalTemplateClient` writes nothing to the backend, so it is a
    // separate boundary and a separate chance to let an unknown key through.
    const rig = await build({ rateLimitOptions: { free: FREE } });
    try {
      await expect(
        rig.handler.addLocalTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never,
          { rateLimitOption: "pro" }
        )
      ).rejects.toBeInstanceOf(ConfigurationError);
    } finally {
      await rig.stop();
    }
  });

  it("lists the plans on offer, for a caller building a picker", async () => {
    const rig = await build({ rateLimitOptions: { free: FREE, pro: PRO } });
    try {
      expect(rig.handler.getRateLimitOptions("acme" as never)).toEqual([
        "free",
        "pro",
      ]);
    } finally {
      await rig.stop();
    }
  });

  it("keeps the chosen plan across a rebuild", async () => {
    // The key is stored beside the credentials, so a replica that never saw the
    // call still builds the client on the plan the caller chose.
    const rig = await build({
      rateLimitOptions: { free: FREE, pro: PRO },
      defaultRateLimitOption: "free",
    });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "pro" }
      );
      await rig.handler.rebuildTemplateClient("acme", "a");

      expect(rig.seen()?.rateLimitOption).toBe("pro");
      expect(limitOf(rig.handler)).toEqual(withNames(PRO));
    } finally {
      await rig.stop();
    }
  });

  it("falls back to the default when a stored plan is no longer offered", async () => {
    // A plugin renaming a plan must not make every client already on it
    // unbuildable at the next restart.
    const rig = await build({
      rateLimitOptions: { free: FREE, pro: PRO },
      defaultRateLimitOption: "free",
    });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "pro" }
      );
      await rig.handler.registerClientTemplate(
        "acme" as never,
        ((creds: { instanceId: string }, ctx: ClientTemplateContext) => [
          {
            name: `acme:_:${creds.instanceId}`,
            rateLimit: ctx.rateLimit ?? [{ type: "noLimit" }],
          },
        ]) as never,
        { rateLimitOptions: { free: FREE } }
      );

      expect(limitOf(rig.handler)).toEqual(withNames([{ type: "noLimit" }]));
    } finally {
      await rig.stop();
    }
  });
});

describe("what a template may declare", () => {
  const register = (options: ClientTemplateOptions) => build(options);

  it("refuses a default naming a plan it does not declare", async () => {
    await expect(
      register({
        rateLimitOptions: { free: FREE },
        defaultRateLimitOption: "pro",
      })
    ).rejects.toThrow(/defaultRateLimitOption/);
  });

  it("refuses a plan whose budget can never hand out a token", async () => {
    // At registration, because these are the plugin author's own constants: the
    // deploy that introduced the broken plan should fail, not the first tenant
    // who picks it.
    await expect(
      register({
        rateLimitOptions: {
          broken: [{ ...FREE[0], tokensToAdd: 0 }],
        },
      })
    ).rejects.toThrow(/unusable requestLimit/);
  });

  it("refuses a plan whose limits would share one budget", async () => {
    // A name may be omitted once, where it is `default`. Omitting it twice is
    // two limits metering against one bucket, which is caught at registration
    // rather than at the first tenant who picks the plan.
    await expect(
      register({
        rateLimitOptions: { pro: [{ ...FREE[0] }, { ...FREE[0] }] },
      })
    ).rejects.toThrow(
      /two rate limits named "default", the name a limit without one takes/
    );
  });

  it("accepts a plan that names its limits", async () => {
    const rig = await register({
      rateLimitOptions: { pro: PRO },
      defaultRateLimitOption: "pro",
    });
    await rig.stop();
  });
});

/**
 * A plan covering sub-clients as well as the root.
 *
 * This is the shape a vendor with a different limit per endpoint needs — Amazon
 * SP-API being the standard example — where a subscription tier moves every one
 * of them at once. The builder declares the tree; the plan says what each node's
 * budget is, and core places them.
 */
describe("a plan that covers sub-clients", () => {
  const ORDERS_STD = [
    {
      type: "requestLimit" as const,
      interval: 1000,
      tokensToAdd: 6,
      maxTokens: 6,
    },
  ];
  const ORDERS_PRO = [{ ...ORDERS_STD[0], tokensToAdd: 60, maxTokens: 60 }];
  const REPORTS_PRO = [
    {
      name: "per_second",
      type: "requestLimit" as const,
      interval: 1000,
      tokensToAdd: 20,
      maxTokens: 20,
    },
    {
      name: "per_day",
      type: "requestLimit" as const,
      interval: 86_400_000,
      tokensToAdd: 50_000,
      maxTokens: 50_000,
    },
  ];

  const TIERS = {
    standard: { "": FREE, orders: ORDERS_STD, reports: FREE },
    premium: { "": PRO, orders: ORDERS_PRO, reports: REPORTS_PRO },
  };

  interface TreeRig {
    handler: RequestHandler;
    warnings: string[];
    seen: () => ClientTemplateContext | undefined;
    stop: () => Promise<void>;
  }

  /**
   * The builder places nothing at all — it only says which clients exist. Every
   * limit below therefore came from the plan.
   */
  async function buildTree(
    options: ClientTemplateOptions,
    subClients = [{ name: "orders" }, { name: "reports" }]
  ): Promise<TreeRig> {
    const backend = memoryBackend();
    const warnings: string[] = [];
    const noop = () => {};
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "tree",
      logger: {
        debug: noop,
        info: noop,
        warn: (...args: unknown[]) => {
          warnings.push(args.map((a) => String(a)).join(" "));
        },
        error: noop,
      } as never,
    });
    let context: ClientTemplateContext | undefined;

    await handler.registerClientTemplate(
      "sp" as never,
      ((creds: { instanceId: string }, ctx: ClientTemplateContext) => {
        context = ctx;
        return [{ name: `sp:_:${creds.instanceId}`, subClients }];
      }) as never,
      options
    );
    await handler.start();

    return {
      handler,
      warnings,
      seen: () => context,
      stop: async () => {
        await handler.stop();
        await backend.close();
      },
    };
  }

  const limits = (handler: RequestHandler) =>
    Object.fromEntries(
      handler
        .getLoadedClients()
        .filter((c) => c.name.startsWith("sp:_:a"))
        .map((c) => [c.name, c.rateLimit])
    );

  it("places a limit on the root and on every sub-client", async () => {
    const rig = await buildTree({ rateLimitOptions: TIERS });
    try {
      await rig.handler.addTemplateClient(
        "sp" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "premium" }
      );

      expect(limits(rig.handler)).toEqual({
        "sp:_:a": withNames(PRO),
        "sp:_:a:orders": withNames(ORDERS_PRO),
        // A sub-client's own limit may itself be several.
        "sp:_:a:reports": withNames(REPORTS_PRO),
      });
    } finally {
      await rig.stop();
    }
  });

  it("moves every sub-client at once when the tier changes", async () => {
    const rig = await buildTree({ rateLimitOptions: TIERS });
    try {
      await rig.handler.addTemplateClient(
        "sp" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "premium" }
      );
      await rig.handler.addTemplateClient(
        "sp" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "standard" }
      );

      expect(limits(rig.handler)).toEqual({
        "sp:_:a": withNames(FREE),
        "sp:_:a:orders": withNames(ORDERS_STD),
        "sp:_:a:reports": withNames(FREE),
      });
    } finally {
      await rig.stop();
    }
  });

  it("hands the builder the whole plan, for a tree it wants to shape itself", async () => {
    const rig = await buildTree({ rateLimitOptions: TIERS });
    try {
      await rig.handler.addTemplateClient(
        "sp" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "premium" }
      );

      expect(rig.seen()?.rateLimits).toEqual(TIERS.premium);
      expect(rig.seen()?.rateLimit).toEqual(PRO);
      expect(rig.seen()?.rateLimitOption).toBe("premium");
    } finally {
      await rig.stop();
    }
  });

  it("reports a plan path that matches no client it built", async () => {
    // A renamed sub-client, most likely. Dropping it silently would leave that
    // endpoint running unlimited with the plan still claiming to cover it.
    const rig = await buildTree({ rateLimitOptions: TIERS }, [
      { name: "orders" },
    ]);
    try {
      await rig.handler.addTemplateClient(
        "sp" as never,
        { instanceId: "a" } as never,
        { rateLimitOption: "premium" }
      );

      expect(rig.warnings.join("\n")).toMatch(
        /path "reports", which no client it built matches/
      );
    } finally {
      await rig.stop();
    }
  });

  it("lets an operator override one path of the chosen plan", async () => {
    const rig = await buildTree({ rateLimitOptions: TIERS });
    try {
      await rig.handler.addTemplateClient(
        "sp" as never,
        { instanceId: "a" } as never,
        {
          rateLimitOption: "premium",
          rateLimitOverrides: {
            orders: [{ ...ORDERS_PRO[0], maxTokens: 7 }],
          },
        }
      );

      const loaded = limits(rig.handler);
      // The override wins for its path, and the plan still holds everywhere else.
      expect(loaded["sp:_:a:orders"]).toEqual(
        withNames([{ ...ORDERS_PRO[0], maxTokens: 7 }])
      );
      expect(loaded["sp:_:a:reports"]).toEqual(withNames(REPORTS_PRO));
    } finally {
      await rig.stop();
    }
  });

  it("refuses a plan whose SUB-CLIENT budget can never hand out a token", async () => {
    // The reason to declare the tree rather than keep it private to the plugin:
    // every path is checked at registration, not when a tenant first picks it.
    await expect(
      buildTree({
        rateLimitOptions: {
          premium: {
            "": PRO,
            orders: [{ ...ORDERS_PRO[0], tokensToAdd: 0 }],
          },
        },
      })
    ).rejects.toThrow(/path "orders".*unusable requestLimit/s);
  });
});

describe("operator overrides beside a plan", () => {
  it("still accepts a bare overrides record in the third argument", async () => {
    // The argument used to be exactly this, and callers that pass it keep working.
    const rig = await build({}, FREE);
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { "": [{ ...FREE[0], maxTokens: 42 }] }
      );

      expect(limitOf(rig.handler)).toEqual(
        withNames([{ ...FREE[0], maxTokens: 42 }])
      );
    } finally {
      await rig.stop();
    }
  });

  it("reads back an overrides record stored before plans existed", async () => {
    // The stored value used to be the record itself. Reading one of those as an
    // empty settings object would drop an operator's overrides on the first
    // restart after an upgrade, silently and fleet-wide.
    const rig = await build({}, FREE);
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        {
          instanceId: "a",
        } as never
      );
      await rig.backend.set(
        "tro:requestHandler:overrides:acme::a",
        JSON.stringify({ "": [{ ...FREE[0], maxTokens: 7 }] })
      );
      await rig.handler.rebuildTemplateClient("acme", "a");

      expect(limitOf(rig.handler)).toEqual(
        withNames([{ ...FREE[0], maxTokens: 7 }])
      );
    } finally {
      await rig.stop();
    }
  });

  it("applies an override on top of the chosen plan", async () => {
    const rig = await build({ rateLimitOptions: { free: FREE, pro: PRO } });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        {
          rateLimitOption: "free",
          rateLimitOverrides: {
            "": [{ ...FREE[0], maxTokens: 99, tokensToAdd: 99 }],
          },
        }
      );

      expect(rig.seen()?.rateLimitOption).toBe("free");
      expect(limitOf(rig.handler)).toEqual(
        withNames([{ ...FREE[0], maxTokens: 99, tokensToAdd: 99 }])
      );
    } finally {
      await rig.stop();
    }
  });

  it("may retune one limit into several", async () => {
    // One limit or several is a matter of budgets, not of what kind of client
    // this is, so an operator may go from one to the other.
    const rig = await build({
      rateLimitOptions: { free: FREE },
      defaultRateLimitOption: "free",
    });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOverrides: { "": PRO } }
      );

      expect(limitOf(rig.handler)).toEqual(withNames(PRO));
    } finally {
      await rig.stop();
    }
  });

  it("refuses to swap a client's own budget for another client's", async () => {
    // The one distinction an override may not cross: a client that borrows
    // another's budget owns no queue, so this is a different client rather than
    // a different limit.
    const rig = await build({
      rateLimitOptions: { free: FREE },
      defaultRateLimitOption: "free",
    });
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        {
          rateLimitOverrides: {
            "": [{ type: "sharedLimit", clientName: "somewhere:_:else" }],
          },
        }
      );

      expect(limitOf(rig.handler)).toEqual(withNames(FREE));
    } finally {
      await rig.stop();
    }
  });

  it("keeps a named limit's name when an override leaves it out", async () => {
    const PER_HOUR = [{ ...FREE[0], name: "per_hour", interval: 3_600_000 }];
    const rig = await build({
      rateLimitOptions: { hourly: PER_HOUR },
      defaultRateLimitOption: "hourly",
    });
    try {
      const { name: _name, ...unnamed } = PER_HOUR[0];
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOverrides: { "": [{ ...unnamed, maxTokens: 9 }] } }
      );

      expect(limitOf(rig.handler)).toEqual([{ ...PER_HOUR[0], maxTokens: 9 }]);
    } finally {
      await rig.stop();
    }
  });
});

describe("template settings that could not be built from", () => {
  /** A second replica on `backend`, started cold, as a restart would be. */
  async function restart(backend: ReturnType<typeof memoryBackend>) {
    const errors: string[] = [];
    const noop = () => {};
    const handler = new RequestHandler({
      key: KEY,
      backend,
      keyPrefix: "tro",
      logger: {
        debug: noop,
        info: noop,
        warn: noop,
        error: (...args: unknown[]) => {
          errors.push(args.map((a) => String(a)).join(" "));
        },
      } as never,
    });
    await handler.registerClientTemplate(
      "acme" as never,
      ((creds: { instanceId: string }, ctx: ClientTemplateContext) => [
        {
          name: `acme:_:${creds.instanceId}`,
          rateLimit: ctx.rateLimit ?? FREE,
        },
      ]) as never
    );
    await handler.start();
    return { handler, errors };
  }

  const loaded = (handler: RequestHandler) =>
    handler.getLoadedClients().map((c) => c.name);

  it("reads a 1.x override record, whose limits were single objects, as lists", async () => {
    const rig = await build({}, FREE);
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never
      );
      await rig.backend.set(
        "tro:requestHandler:overrides:acme::a",
        JSON.stringify({ "": { ...FREE[0], maxTokens: 7 } })
      );
      await rig.handler.rebuildTemplateClient("acme", "a");

      expect(limitOf(rig.handler)).toEqual(
        withNames([{ ...FREE[0], maxTokens: 7 }])
      );
    } finally {
      await rig.stop();
    }
  });

  it("starts every other tenant when one tenant's stored settings cannot be built", async () => {
    const rig = await build({}, FREE);
    for (const instanceId of ["a", "b"]) {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId } as never
      );
    }
    await rig.backend.set(
      "tro:requestHandler:overrides:acme::a",
      JSON.stringify({ "": [{ ...FREE[0], tokensToAdd: 0 }] })
    );

    const { handler, errors } = await restart(rig.backend);
    try {
      expect(loaded(handler)).toContain("acme:_:b");
      expect(loaded(handler)).not.toContain("acme:_:a");
      expect(errors.join("\n")).toContain("acme::a");
    } finally {
      await handler.stop();
      await rig.stop();
    }
  });

  it("refuses an override that could not be built, before storing it or touching the client", async () => {
    const rig = await build({}, FREE);
    try {
      await rig.handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never
      );

      await expect(
        rig.handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never,
          { rateLimitOverrides: { "": [{ ...FREE[0], tokensToAdd: 0 }] } }
        )
      ).rejects.toBeInstanceOf(ConfigurationError);

      expect(
        await rig.backend.get("tro:requestHandler:overrides:acme::a")
      ).toBeNull();
      expect(limitOf(rig.handler)).toEqual(withNames(FREE));
    } finally {
      await rig.stop();
    }
  });

  it("refuses an override given as a single limit, and shows how to wrap it", async () => {
    const rig = await build({}, FREE);
    try {
      await expect(
        rig.handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never,
          { rateLimitOverrides: { "": FREE[0] as never } }
        )
      ).rejects.toThrow(/wrap it: \[/);
    } finally {
      await rig.stop();
    }
  });

  it("refuses options that mix a plan choice with path overrides", async () => {
    // TypeScript accepts this, through the overrides record's index signature,
    // and the path would otherwise be dropped without a word.
    const rig = await build({ rateLimitOptions: { free: FREE, pro: PRO } });
    try {
      await expect(
        rig.handler.addTemplateClient(
          "acme" as never,
          { instanceId: "a" } as never,
          { rateLimitOption: "pro", "": FREE } as never
        )
      ).rejects.toThrow(/under rateLimitOverrides/);
    } finally {
      await rig.stop();
    }
  });
});

describe("plans a template cannot be trusted with", () => {
  it("refuses a plan written as a single limit, naming the template", async () => {
    const handler = new RequestHandler({
      key: KEY,
      backend: memoryBackend(),
      keyPrefix: "tro",
    });
    await expect(
      handler.registerClientTemplate("acme" as never, (() => []) as never, {
        rateLimitOptions: { free: FREE[0] as never },
      })
    ).rejects.toThrow(/acme \(option "free"\).*wrap it/);
  });

  it("refuses a default naming a property every object has", async () => {
    const handler = new RequestHandler({
      key: KEY,
      backend: memoryBackend(),
      keyPrefix: "tro",
    });
    await expect(
      handler.registerClientTemplate("acme" as never, (() => []) as never, {
        rateLimitOptions: { free: FREE },
        defaultRateLimitOption: "toString",
      })
    ).rejects.toBeInstanceOf(ConfigurationError);
  });

  it("gives each tenant its own copy of the plan", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "tro" });
    await handler.registerClientTemplate(
      "acme" as never,
      ((creds: { instanceId: string }, ctx: ClientTemplateContext) => {
        // A builder tuning the plan for one tenant, as a quota from its
        // credentials might.
        if (creds.instanceId === "a") {
          (ctx.rateLimit![0] as { maxTokens: number }).maxTokens = 1;
        }
        return [
          { name: `acme:_:${creds.instanceId}`, rateLimit: ctx.rateLimit },
        ];
      }) as never,
      { rateLimitOptions: { free: FREE }, defaultRateLimitOption: "free" }
    );
    await handler.start();
    try {
      for (const instanceId of ["a", "b"]) {
        await handler.addTemplateClient(
          "acme" as never,
          { instanceId } as never
        );
      }
      const of = (name: string) =>
        handler.getLoadedClients().find((c) => c.name === name)?.rateLimit;

      // Literals, not `FREE`: a leak would have mutated that constant too.
      expect(of("acme:_:a")?.[0]).toMatchObject({ maxTokens: 1 });
      expect(of("acme:_:b")?.[0]).toMatchObject({ maxTokens: 2 });
      expect(FREE[0].maxTokens).toBe(2);
    } finally {
      await handler.stop();
      await backend.close();
    }
  });

  it("gives a sub-client named like an Object property only the limits it inherits", async () => {
    const backend = memoryBackend();
    const handler = new RequestHandler({ key: KEY, backend, keyPrefix: "tro" });
    await handler.registerClientTemplate(
      "acme" as never,
      ((creds: { instanceId: string }) => [
        {
          name: `acme:_:${creds.instanceId}`,
          rateLimit: FREE,
          subClients: [{ name: "constructor" }],
        },
      ]) as never
    );
    await handler.start();
    try {
      await handler.addTemplateClient(
        "acme" as never,
        { instanceId: "a" } as never,
        { rateLimitOverrides: { "": [{ ...FREE[0], maxTokens: 5 }] } }
      );
      const sub = handler
        .getLoadedClients()
        .find((c) => c.name === "acme:_:a:constructor");

      expect(sub?.rateLimit).toEqual(withNames([{ ...FREE[0], maxTokens: 5 }]));
    } finally {
      await handler.stop();
      await backend.close();
    }
  });
});
