import type { CreateClientData, DeclaredRateLimit } from "./client/types.js";
import type { DianemoBackend } from "./backend/types.js";
import type RequestHandler from "./index.js";
import type { Logger } from "./logger.js";

/** Lifecycle state, as `getMetadata` reports it; a stopping handler reads as `stopped`. */
export type RequestHandlerStatus = "stopped" | "starting" | "started";

/**
 * Turns one instance's credentials into the client or clients it needs.
 *
 * Runs on every replica that builds the instance, on every rebuild, so it
 * should be a pure function of its arguments. Every client it returns must carry the
 * credentials' `instanceId` as a segment of its `name` — `buildClientName()`
 * does this — or the build throws `ConfigurationError`
 * `template_client_name_mismatch`.
 *
 * `context` carries the rate-limit plan the instance chose, when the template
 * declared any; see {@link ClientTemplateContext}. A builder may ignore it.
 */
export type ClientTemplateBuilder<T = unknown> = (
  credentials: T,
  context: ClientTemplateContext
) => CreateClientData | CreateClientData[];

/**
 * What a template builder is told beyond the credentials.
 *
 * Every field is set only when the instance is on one of the rate-limit options
 * the template declared — chosen by the caller, or the template's
 * `defaultRateLimitOption` — and all are absent otherwise, including when a
 * template declares no options at all, which is how a builder that ignores this
 * argument keeps behaving exactly as it did. Each build gets its own copy, so
 * a builder may modify it.
 *
 * A builder does not have to place any of this. The chosen plan is applied to the
 * client tree by path after the builder returns, so a builder that only declares
 * `subClients: [{ name: "orders" }]` still gets the plan's `orders` limit. These
 * are here for a builder that wants to read the plan rather than receive it —
 * to decide whether a sub-client should exist at all, say.
 */
export interface ClientTemplateContext {
  /**
   * The chosen plan's limits for the root client: its `""` path. Absent when
   * the plan is a path map with no `""` entry.
   */
  rateLimit?: DeclaredRateLimit[];
  /**
   * The whole chosen plan, keyed by sub-client path the way
   * {@link RateLimitOverrides} is: `""` is the root, `"orders"` its sub-client of
   * that name, `"a:b"` a nested one.
   */
  rateLimits?: Record<string, DeclaredRateLimit[]>;
  /**
   * Which option was chosen. A plan usually changes more than a rate limit, so
   * this is here to branch on — sub-clients an entry-level plan does not get, a
   * different endpoint for enterprise.
   */
  rateLimitOption?: string;
}

/**
 * One plan a template offers.
 *
 * Either the root client's list of limits, or a whole tree of them keyed by
 * sub-client path — which is what a vendor publishing a different limit per
 * endpoint needs, since a tier changes all of them at once.
 *
 * The two are told apart by shape: an array is the root's limits, and anything
 * else is read as a path map. A path matching no client the builder returns is
 * logged and not applied.
 */
export type RateLimitPlan =
  DeclaredRateLimit[] | Record<string, DeclaredRateLimit[]>;

/**
 * How a template lets callers choose its rate limit.
 *
 * The limits are the template's own, named by keys the caller picks from: there
 * is no way for a caller to supply a limit the plugin did not write. A template
 * declaring no options accepts no choice, and `addTemplateClient` rejects one.
 *
 * Validated by `registerClientTemplate`, which throws `ConfigurationError`:
 * `unknown_rate_limit_type` for a plan or path that is not a list, the codes
 * `CreateClientData.rateLimit` lists for an unusable limit, and
 * `unknown_rate_limit_option` for a `defaultRateLimitOption` that names no
 * plan. A chosen plan replaces whatever limits the builder declared on the
 * paths it covers; an operator's {@link RateLimitOverrides} apply on top.
 *
 * ```ts
 * await handler.registerClientTemplate("acme", builder, {
 *   rateLimitOptions: {
 *     free: [{ type: "requestLimit", interval: 1000, tokensToAdd: 2, maxTokens: 2 }],
 *     pro: {
 *       "": [{ type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 }],
 *       reports: [
 *         { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 2, maxTokens: 2 },
 *         { name: "per_day", type: "requestLimit", interval: 86400000, tokensToAdd: 500, maxTokens: 500 },
 *       ],
 *     },
 *   },
 *   defaultRateLimitOption: "free",
 * });
 * ```
 */
export interface ClientTemplateOptions {
  /**
   * Plans a caller may choose between, by key. Each is a list of limits for the
   * root client, or a path map covering its sub-clients too; see
   * {@link RateLimitPlan}.
   */
  rateLimitOptions?: Record<string, RateLimitPlan>;
  /** Applied when a caller names none. Must be a key of `rateLimitOptions`. */
  defaultRateLimitOption?: string;
}

/** A registered template: its builder, and what it lets callers choose. */
export interface RegisteredTemplate {
  builder: ClientTemplateBuilder<unknown>;
  options: ClientTemplateOptions;
}

/**
 * Per-instance settings that travel with a template client's credentials.
 *
 * Stored beside them rather than inside, because they are not sensitive and a
 * replica that cannot decrypt the credentials can still read these.
 */
export interface TemplateClientOptions {
  /**
   * Which of the template's declared `rateLimitOptions` this instance is on —
   * a subscription plan, typically. Rejected if the template declared none, or
   * if it declared some and this is not one of them.
   */
  rateLimitOption?: string;
  /** Operator-side overrides, documented on {@link RateLimitOverrides}. */
  rateLimitOverrides?: RateLimitOverrides;
}

/**
 * Rate-limit overrides keyed by sub-client path: `""` is the parent, `"rest"` its
 * sub-client of that name, `"a:b"` a nested one. An override may change the type
 * and number of limits freely, but not swap a client's own budgets for a
 * `sharedLimit` or the reverse — that would change the client class, so such an
 * override is skipped with a warning. An unnamed limit in an override keeps the
 * name, and so the budget, of the one limit of its type it replaces. A
 * sub-client with no limits of its own inherits its parent's after the
 * parent's override. See docs/concepts.md#rate-limit-overrides.
 *
 * `addTemplateClient` validates overrides before storing them, throwing
 * `ConfigurationError` `unknown_rate_limit_type` for a value that is not a
 * list and the codes `CreateClientData.rateLimit` lists for an unusable limit.
 *
 * This is the operator-side escape hatch and needs no permission from the template.
 * For a choice the template itself sanctions, see {@link ClientTemplateOptions}.
 */
export type RateLimitOverrides = Record<string, DeclaredRateLimit[]>;

/**
 * Registry of known client templates, so a typo at a call site is a compile error
 * rather than a runtime warn-and-skip. Augment it:
 *
 * ```ts
 * declare module "@dianemo/core" {
 *   interface ClientTemplates {
 *     fedex: OAuth2Credentials;
 *   }
 * }
 * ```
 */
// Empty by design; `object` here would accept any template name.
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface ClientTemplates {}

/** Runs inside `start()`; see `RequestHandler.registerStartupHook`. */
export type StartupHook = (handler: RequestHandler) => Promise<void> | void;

export interface RequestHandlerConstructorOptions {
  /** Encrypts every stored credential. Changing it orphans what is already stored. */
  key: string;
  /**
   * Where the shared state lives. Pick by how many processes will run this handler:
   * `redisBackend(connection)` for more than one, `memoryBackend()` only for exactly
   * one forever — it shares nothing, so two processes send twice the agreed rate and
   * nothing reports it. See docs/backends/README.md.
   *
   * You build the backend; the handler never constructs a connection of its own.
   */
  backend: DianemoBackend;
  /**
   * Where the handler writes its logs. Defaults to silence — a library
   * embedded in someone else's process should not claim their stdout.
   *
   * A `pino` instance satisfies this structurally.
   */
  logger?: Logger;
  /** Namespaces every backend key, so one Redis can host several handlers. */
  keyPrefix?: string;
  /**
   * Configures the built-in client, registered as `"default"` and reachable by
   * passing that as `clientName` — for requests that belong to no template.
   *
   * **Defaults to a `noLimit` client named `default`**
   */
  defaultClientOptions?: CreateClientData;
  /** Election weight for becoming a queue controller; higher wins. */
  priority?: number;
}

/** A snapshot of one handler, from `RequestHandler.getMetadata`. */
export interface RequestHandlerMetadata {
  /** This replica's instance id. */
  id: string;
  status: RequestHandlerStatus;
  /** Election weight, from the constructor's `priority`. */
  priority: number;
  /** Every client registered on this replica. */
  registeredClients: string[];
  /** The clients this replica is the queue controller for. */
  ownedClients: string[];
}
