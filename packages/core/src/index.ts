import type { DianemoResponse, RequestConfig } from "./request/types.js";
import { createTryHandleRequest } from "./tryHandleRequest.js";
import startSubscriptions from "./utils/startSubscriptions.js";
import updateClientRoles from "./utils/updateClientRoles.js";
import { generateClients } from "./utils/createClients.js";
import { assertNameSegment } from "./utils/clientName.js";
import type { DianemoBackend } from "./backend/types.js";
import { noopLogger, type Logger } from "./logger.js";
import type BaseClient from "./client/index.js";
import EventEmitter from "node:events";
import crypto from "node:crypto";
import {
  loadTemplateClientsFromBackend,
  buildAndRegisterTemplateClients,
  destroyTemplateClients,
  encryptCredentials,
  decryptCredentials,
  templateClientNamesKey,
  assertTemplateOptions,
  assertTemplateClientBuildable,
  normalizeTemplateClientOptions,
  serializeTemplateClientOptions,
  readTemplateClientOptions,
  templateEntryGeneration,
} from "./utils/templateClients.js";
import type {
  CreateClientData,
  NamedRateLimitData,
  ProbeRequestConfig,
  SetGrantTokensData,
} from "./client/types.js";
import {
  ClientNotFoundError,
  ClientUnavailableError,
  ConfigurationError,
} from "./errors.js";
import {
  type MergedNamespaces,
  type PluginContext,
  type RequestHandlerPlugin,
} from "./plugin.js";
import type {
  ClientTemplateBuilder,
  ClientTemplateOptions,
  ClientTemplates,
  RateLimitOverrides,
  RegisteredTemplate,
  RequestHandlerConstructorOptions,
  RequestHandlerMetadata,
  RequestHandlerStatus,
  StartupHook,
  TemplateClientOptions,
} from "./types.js";

export type {
  SetGrantTokensData,
  OAuth2Credentials,
  TokenCredentials,
  BasicCredentials,
} from "./client/types.js";
export type {
  ClientTemplateBuilder,
  ClientTemplateContext,
  ClientTemplateOptions,
  ClientTemplates,
  StartupHook,
  RateLimitOverrides,
  RateLimitPlan,
  TemplateClientOptions,
} from "./types.js";

export {
  buildClientName,
  parseClientName,
  assertNameSegment,
  GLOBAL_ORGANIZATION_SEGMENT,
  type ParsedClientName,
} from "./utils/clientName.js";

export {
  RequestHandlerError,
  ClientNotFoundError,
  ClientConflictError,
  ConfigurationError,
  RequestTimeoutError,
  StreamClosedError,
  StreamTimeoutError,
  NoResponseError,
  ClientUnavailableError,
  NotOAuth2ClientError,
  GrantRefreshTokenMissingError,
  RequestCostExceedsBudgetError,
  RequestCostMismatchError,
  RequestAbortedError,
  RequestError,
  getOriginalStatus,
  getOriginalResponseData,
  type RequestErrorMetadata,
} from "./errors.js";

export { noopLogger, type LogFn, type Logger } from "./logger.js";

export { default as MemoryBackend, memoryBackend } from "./backend/memory.js";
export {
  calculateQueueScore,
  MAX_QUEUE_PRIORITY,
  MAX_QUEUE_RETRIES,
  QUEUE_PRIORITY_BAND,
  QUEUE_RETRY_BAND,
  QUEUE_SCORE_EPOCH_MS,
} from "./backend/queueScore.js";
export { QUEUE_CLAIM_LEASE_MS } from "./backend/queueClaim.js";
export {
  formatStoredCost,
  parseFiniteStored,
  parsePriority,
  parseStoredCost,
  parseStoredNumber,
} from "./backend/parseStored.js";
export {
  FREEZE_PROBE_TTL_SECONDS,
  normalizeStateTtlSeconds,
  normalizeTombstoneTtl,
  normalizeTtlSeconds,
  REDIS_MAX_TTL_SECONDS,
  REQUEST_TOMBSTONE_TTL_SECONDS,
} from "./backend/ttl.js";
export { claimMarkKey } from "./backend/settlement.js";
export { BALANCE_TOLERANCE } from "./backend/balance.js";
export type {
  AcquireConcurrencyResult,
  AcquireMultiLimitResult,
  AcquireTokensResult,
  BatchOp,
  BatchOptions,
  ClaimMark,
  ConcurrencyConfig,
  DianemoBackend,
  FreezeState,
  MessageHandler,
  MultiLimitSpec,
  QueueStats,
  QueuedRequest,
  SettleResult,
  SettleSpec,
  TokenBucketConfig,
} from "./backend/types.js";

export {
  definePlugin,
  type MergedNamespaces,
  type PluginContext,
  type RequestHandlerPlugin,
} from "./plugin.js";

export {
  createTryHandleRequest,
  type BoundTryHandleRequest,
  type TryHandleRequestOptions,
} from "./tryHandleRequest.js";

export type {
  AttemptOutcome,
  DianemoResponse,
  RequestConfig,
  RequestCost,
  RequestMetadata,
  SettleFunction,
  StreamControls,
  StreamEndReason,
  StreamError,
  StreamObserver,
  StreamResult,
} from "./request/types.js";
export type {
  CreateClientData,
  RateLimitData,
  DeclaredRateLimit,
  NamedRateLimitData,
  ProbeRequestConfig,
  NoLimitClientOptions,
  RequestLimitClientOptions,
  ConcurrencyLimitClientOptions,
  SharedLimitClientOptions,
  RateLimitStats,
  NamedRateLimitStats,
  ErrorClassifier,
  ErrorDecision,
} from "./client/types.js";
export type {
  RequestHandlerConstructorOptions,
  RequestHandlerMetadata,
  RequestHandlerStatus,
} from "./types.js";

/**
 * At the boundary, so a bad identifier is rejected by the call that introduced
 * it rather than later, from inside client generation.
 */
function assertInstanceSegments(credentials: unknown): void {
  const { instanceId, organizationId } = credentials as {
    instanceId: string;
    organizationId?: string | null;
  };
  assertNameSegment(instanceId, "instanceId");
  if (organizationId !== undefined && organizationId !== null) {
    assertNameSegment(organizationId, "organizationId");
  }
}

/** How long shutdown waits for closed streams to release their slots. */
const STREAM_CLOSE_TIMEOUT_MS = 5_000;

/**
 * The host for every outbound client in one process, and this process's seat
 * in the fleet that shares their budgets through `backend`.
 *
 * Construct one per process. It starts itself on the first request or
 * template mutation, and stops on SIGTERM, SIGINT or SIGHUP. Once stopped it
 * stays stopped: work is refused until `start()` is called again.
 *
 * ```ts
 * import RequestHandler, { memoryBackend } from "@dianemo/core";
 *
 * const handler = new RequestHandler({
 *   key: "a-high-entropy-secret-from-your-secret-store",
 *   backend: memoryBackend(),
 *   defaultClientOptions: {
 *     name: "default",
 *     rateLimit: [
 *       { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 },
 *       { name: "per_day", type: "requestLimit", interval: 86_400_000, tokensToAdd: 50_000, maxTokens: 50_000 },
 *     ],
 *   },
 * });
 *
 * const res = await handler.handleRequest({
 *   clientName: "default",
 *   requestName: "example.get",
 *   method: "GET",
 *   url: "https://api.example.com/things",
 * });
 * ```
 *
 * @throws {ConfigurationError} `backend_required` without a `backend`, and
 * `key_required` for an empty or non-string `key`.
 */
export default class RequestHandler {
  protected id: string;
  protected priority: number;
  protected status: RequestHandlerStatus | "stopping";
  protected backend: DianemoBackend;
  protected namespace: string;
  protected logger: Logger;
  protected heartbeatTimeouts: Map<string, NodeJS.Timeout> = new Map();
  protected heartbeatInterval?: NodeJS.Timeout;
  protected emitter: NodeJS.EventEmitter = new EventEmitter();
  protected key: string;
  protected clients: Map<string, BaseClient> = new Map();
  protected defaultClient: CreateClientData;
  protected templates: Map<string, RegisteredTemplate> = new Map();
  protected templateClientMap: Map<string, Map<string, Set<string>>> =
    new Map();
  protected localTemplateClients: Set<string> = new Set();
  protected startupHooks: StartupHook[] = [];
  /** Whether the current start has passed its loop over `startupHooks`. */
  private startupHooksRan = false;
  /** Set whenever `stop()` resolves, in any state; only `start()` clears it. */
  private stoppedByCaller = false;
  protected startPromise: Promise<void> | null = null;
  protected stopPromise: Promise<void> | null = null;
  protected shutdownHooksRegistered = false;
  /** Whether pub/sub handlers are installed, so a retried start cannot duplicate them. */
  public subscriptionsStarted = false;
  protected shutdownHandler?: () => void;
  private rolesRunning = false;
  private rolesRerun = false;
  private rolesRerunStartup = false;
  private rolesPromise: Promise<void> = Promise.resolve();
  /** Plugin names already registered via `use()`, for duplicate detection. */
  private pluginNames: Set<string> = new Set();

  constructor(data: RequestHandlerConstructorOptions) {
    if (!data.backend) {
      throw new ConfigurationError(
        "backend_required",
        "RequestHandler requires a `backend`. Use redisBackend(connection) for anything running more than one process, or memoryBackend() for a single process."
      );
    }
    // Every credential this handler stores is encrypted under this one string,
    // and the type alone does not stop it arriving empty at runtime — which is
    // what an unset environment variable produces. Caught here, because
    // otherwise it surfaces as a TypeError from inside node:crypto.
    if (typeof data.key !== "string" || data.key.length === 0) {
      throw new ConfigurationError(
        "key_required",
        "RequestHandler requires a non-empty `key`. It encrypts stored credentials, so use a high-entropy secret (32+ characters) from your secret store, not a literal."
      );
    }
    this.id = crypto.randomUUID();
    this.priority = data.priority || 1;
    this.status = "stopped";
    this.namespace = `${
      data.keyPrefix ? `${data.keyPrefix}:` : ""
    }requestHandler`;
    this.key = data.key;
    this.defaultClient = data.defaultClientOptions || {
      rateLimit: [{ type: "noLimit" }],
      name: "default",
    };
    this.logger = data.logger ?? noopLogger;
    this.backend = data.backend;
  }

  /**
   * Registers plugins and returns their merged request namespaces. See
   * docs/writing-plugins.md.
   *
   * Synchronous and safe at module scope: template registration is queued as a
   * startup hook. Called after `start()`, it also registers the template at
   * once — before returning, for a plugin that calls `registerClientTemplate`
   * before its first `await` — and builds the template's stored tenants. A
   * registration that fails then has no caller to reject, so it is logged as
   * an error. Plugin names must be unique across every call.
   *
   * @throws {ConfigurationError} `duplicate_plugin` when a name is already
   * registered on this handler or repeats within the call. Every name is
   * checked before any plugin is registered, so a refused call registers
   * nothing.
   */
  public use<const P extends readonly RequestHandlerPlugin[]>(
    ...plugins: P
  ): MergedNamespaces<P> {
    const namespaces = {} as Record<string, unknown>;
    const tryHandleRequest = createTryHandleRequest(this, this.logger);

    // Template names are the first segment of a `:`-delimited client name and
    // the key credentials are filed under. Two plugins claiming one name would
    // silently route one's credentials to the other's builder, so refuse — and
    // before registering any, so a refused call leaves nothing behind.
    const claimed = new Set<string>();
    for (const plugin of plugins) {
      if (this.pluginNames.has(plugin.name) || claimed.has(plugin.name)) {
        throw new ConfigurationError(
          "duplicate_plugin",
          `Plugin "${plugin.name}" is already registered on this handler.`
        );
      }
      claimed.add(plugin.name);
    }

    for (const plugin of plugins) {
      this.pluginNames.add(plugin.name);

      const ctx: PluginContext = {
        tryHandleRequest,
        logger: this.logger,
        backend: this.backend,
        handler: this,
      };
      namespaces[plugin.name] = plugin.createRequests(ctx);
      const hook = () => plugin.registerTemplate(this);
      this.registerStartupHook(hook);
      // A start still short of its hook loop runs this hook from the list.
      const queued = this.status === "starting" && !this.startupHooksRan;
      if (this.status !== "stopped" && !queued) {
        this.runLateStartupHook(plugin.name, hook);
      }
    }

    return namespaces as MergedNamespaces<P>;
  }

  /**
   * Runs a plugin's registration for a `use()` that came after `start()` began.
   *
   * Called synchronously, so a plugin whose `registerTemplate` reaches
   * `registerClientTemplate` before its first `await` has its template in place
   * when `use()` returns. During a start that has passed its startup hooks it
   * waits for the start instead, since a template registered before `started`
   * would not build its stored tenants. Not called during a start that has
   * yet to reach them: that start runs the hook itself.
   */
  private runLateStartupHook(pluginName: string, hook: StartupHook): void {
    const report = (error: unknown) => {
      this.logger.error(
        { error },
        `Plugin "${pluginName}" failed to register its template after start()`
      );
    };
    if (this.status === "started") {
      try {
        void Promise.resolve(hook(this)).catch(report);
      } catch (error) {
        report(error);
      }
      return;
    }
    // A start that fails leaves the hook queued for the next one.
    void (this.startPromise ?? this.stopPromise ?? Promise.resolve())
      .then(
        () => this.status === "started",
        () => false
      )
      .then((started) => (started ? hook(this) : undefined))
      .catch(report);
  }

  /** Plugin names registered on this handler, in registration order. */
  public getRegisteredPlugins(): string[] {
    return Array.from(this.pluginNames);
  }

  /**
   * Subscribes to the backend, builds the default client, runs the startup
   * hooks, rebuilds every stored template client and joins the election.
   *
   * Idempotent, and called for you by `handleRequest` and the template
   * methods. Concurrent calls share one start; a call during `stop()` waits for
   * it and starts again. One stored template client that cannot be decrypted
   * or built is logged and skipped. Anything else that fails — a startup hook,
   * the backend — rejects this, closes what the backend opened, and leaves the
   * handler stopped so a later call can retry.
   */
  public async start(): Promise<void> {
    if (this.stopPromise) await this.stopPromise;
    this.stoppedByCaller = false;
    if (this.status === "started") return;
    if (this.startPromise) return this.startPromise;
    this.startPromise = this.doStart().catch(async (err) => {
      this.startPromise = null;
      // Back to "stopped", and tear down what did get set up. `handleRequest`
      // calls `start()` whenever the status is not "started", so a status left at
      // "starting" wedges the handler: every later request re-runs `doStart` and
      // adds another pub/sub listener to the same connection.
      this.status = "stopped";
      // Both of these belong to a start that did not finish, and only `doStop`
      // otherwise clears them — which this path never reaches. Leaving the flag set
      // makes the next `start()` skip subscribing and reach "started" deaf; leaving
      // the connection open keeps the event loop alive after the caller gives up.
      this.subscriptionsStarted = false;
      await this.backend.close().catch((closeError) => {
        this.logger.error(
          { error: closeError },
          "Failed to close the backend after a failed start"
        );
      });
      throw err;
    });
    return this.startPromise;
  }

  private async doStart(): Promise<void> {
    this.status = "starting";
    this.startupHooksRan = false;
    await startSubscriptions.bind(this)();

    // Builds the default client's `subClients` too.
    await generateClients.bind(this)([this.defaultClient]);

    for (const hook of this.startupHooks) await hook(this);
    this.startupHooksRan = true;

    await loadTemplateClientsFromBackend.bind(this)();

    await this.scheduleClientRoles(true);
    this.status = "started";
    this.registerShutdownHooks();
    this.emitter.emit("instanceStarted");
    this.logger.info(`Started request handler instance with ID ${this.id}`);
  }

  /**
   * Runs during `start()`, after the backend is up but before credentials are
   * hydrated. Hooks run in registration order, so register a template-registration
   * hook before one that loads credentials for it. Throwing fails `start()`.
   */
  public registerStartupHook(hook: StartupHook): void {
    this.startupHooks.push(hook);
  }

  private registerShutdownHooks(): void {
    if (this.shutdownHooksRegistered) return;
    this.shutdownHooksRegistered = true;

    this.shutdownHandler = () => {
      this.stop().catch((err) => {
        this.logger.error(
          {
            error: err,
          },
          "Failed to stop request handler during shutdown"
        );
      });
    };

    process.once("SIGTERM", this.shutdownHandler);
    process.once("SIGINT", this.shutdownHandler);
    process.once("SIGHUP", this.shutdownHandler);
  }

  private unregisterShutdownHooks(): void {
    if (!this.shutdownHooksRegistered || !this.shutdownHandler) return;
    process.removeListener("SIGTERM", this.shutdownHandler);
    process.removeListener("SIGINT", this.shutdownHandler);
    process.removeListener("SIGHUP", this.shutdownHandler);
    this.shutdownHandler = undefined;
    this.shutdownHooksRegistered = false;
  }

  /**
   * Shuts down gracefully: refuses new requests, waits up to the drain budget
   * (10 s; see `setDrainTimeout`) for this replica's queued work, fails what is
   * still waiting with `ClientUnavailableError` `handler_stopped`, waits up to
   * the same budget again for requests already upstream, deregisters, and
   * closes what the backend opened — never the connection you passed in.
   *
   * Idempotent; a stop during `start()` lets the start finish first. Always
   * ends `stopped`, even when deregistration fails, though that error still
   * rejects this. Requests and template mutations are then refused with
   * `ClientUnavailableError` `handler_stopped` rather than restarting the
   * handler — also after a stop before the first start, or during a start
   * that fails; call `start()` to bring it back.
   */
  public async stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    if (this.status !== "started" && this.status !== "starting") {
      this.stoppedByCaller = true;
      return;
    }
    this.stopPromise = this.finishStartupThenStop().finally(() => {
      this.stoppedByCaller = true;
      this.stopPromise = null;
    });
    return this.stopPromise;
  }

  private async finishStartupThenStop(): Promise<void> {
    if (this.status === "starting" && this.startPromise) {
      try {
        await this.startPromise;
      } catch {
        // start() restored the stopped state. Preserve its original rejection;
        // there is no running handler left for stop() to tear down.
        return;
      }
    }
    await this.doStop();
  }

  /**
   * Bounded because an orchestrator sends SIGKILL a few seconds after SIGTERM, so a
   * drain outliving that window helps nobody.
   */
  private drainTimeoutMs = 10_000;

  /**
   * Overrides the shutdown drain budget, in ms. Default 10000. `stop()` may
   * spend it twice: once on queued work and once on requests already upstream.
   */
  public setDrainTimeout(ms: number): void {
    this.drainTimeoutMs = ms;
  }

  /**
   * Waits for each client's queue to empty, up to the drain budget. Ordering is
   * unchanged: the normal drain loop is what serves the backlog.
   */
  private async drainClients(): Promise<void> {
    const deadline = Date.now() + this.drainTimeoutMs;
    const clients = [...this.clients.values()];
    if (!clients.length) return;

    while (Date.now() < deadline) {
      let outstanding = 0;
      for (const client of clients) {
        try {
          // This replica's own work, not the queue's total. The queue key is
          // fleet-wide, and a worker cannot advance a peer's entry at all —
          // `processRequests` returns immediately for that role — so counting all of
          // it spends the whole drain budget on someone else's backlog.
          if (await client.hasOutstandingWork()) outstanding++;
        } catch {
          // A backend already going away cannot say what is left.
          return;
        }
      }
      if (outstanding === 0) return;
      // Nothing else pokes the loop once new requests stop arriving.
      for (const client of clients) {
        client.processRequests().catch(() => {});
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    this.logger.warn(
      `Shutdown drain did not finish within ${this.drainTimeoutMs}ms; failing what is left`
    );
  }

  /**
   * Stops admitting, then waits for what is already upstream to come back.
   *
   * A request outliving the drain budget still holds a concurrency slot, and the
   * call that releases it runs when the response arrives — so closing first left the
   * release with no connection to run on. Nothing then reclaims the slot except its
   * `concurrencySlotTtl`, which is measured from the claim, so a replacement replica
   * starts against a cap that is already occupied by a request that finished.
   *
   * Bounded by the same budget as the drain, because an orchestrator's SIGKILL is
   * the real deadline and an upstream that never answers must not hold the process.
   */
  private async awaitInFlightRequests(): Promise<void> {
    const clients = [...this.clients.values()];
    for (const client of clients) client.haltAdmission();
    if (!clients.length) return;

    const deadline = Date.now() + this.drainTimeoutMs;
    while (Date.now() < deadline) {
      const inFlight = clients.reduce(
        (total, client) => total + client.countInFlight(),
        0
      );
      if (inFlight === 0) return;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const remaining = clients.reduce(
      (total, client) => total + client.countInFlight(),
      0
    );
    const streaming = clients.flatMap((client) => client.openStreamIds());
    if (streaming.length) {
      this.logger.warn(
        { requestIds: streaming },
        `Shutdown is closing ${streaming.length} streamed response(s) still open`
      );
    }
    // Closed rather than left to run on: a stream's end releases its slot, and
    // that needs the backend this shutdown is about to close. Bounded, because
    // a backend that is not answering would otherwise hold the process.
    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      Promise.all(clients.map((client) => client.closeStreams())),
      new Promise((resolve) => {
        timer = setTimeout(resolve, STREAM_CLOSE_TIMEOUT_MS);
      }),
    ]);
    clearTimeout(timer);
    if (remaining > streaming.length) {
      this.logger.warn(
        `Shutdown closed with ${remaining - streaming.length} request(s) still upstream; their concurrency slots lapse on the slot TTL`
      );
    }
  }

  private async doStop(): Promise<void> {
    if (this.status !== "started" && this.status !== "starting") return;
    this.status = "stopping";
    this.unregisterShutdownHooks();

    try {
      // Before tearing anything down, since the signal handlers exist for this.
      await this.drainClients();

      for (const timeout of this.heartbeatTimeouts.values()) {
        clearTimeout(timeout);
      }
      this.heartbeatTimeouts.clear();
      for (const client of this.clients.values()) {
        // Halted before the timers are cleared, so a 429 landing while requests
        // are still upstream cannot book a freeze wake-up behind the clear.
        client.haltAdmission();
        client.removeHealthCheckInterval();
        // Whatever did not drain in time fails with a reason, rather than an
        // opaque timeout long after the process meant to exit. These hold no slot —
        // they were never admitted — so failing them releases nothing.
        client.cancelWaitingRequests();
      }
      // After the waiters are failed and before anything is torn down: a request
      // already upstream releases its slot when the response lands, and that call
      // needs the backend still open.
      await this.awaitInFlightRequests();
      await this.backend.batch([
        { op: "srem", key: `${this.namespace}:instances`, member: this.id },
        { op: "del", key: `${this.namespace}:instance:${this.id}` },
        {
          op: "publish",
          channel: `${this.namespace}:instanceStopped`,
          message: this.id,
        },
      ]);
    } finally {
      // Reaching the terminal state is not optional even when a step above throws,
      // and this path is reachable in ordinary operation: deregistration fails
      // whenever the backend goes away first during a pod teardown, and `batch`
      // reports a failed pipeline command rather than swallowing it. A status left at
      // "stopping" wedges the handler — `stop()` becomes a no-op, `start()` returns
      // the resolved `startPromise` without re-running `doStart`, every request is
      // refused, and the heartbeat keeps the event loop alive so the process cannot
      // exit. The error still propagates; only the state is guaranteed.
      try {
        // Closes only what the backend opened for itself. A connection handed to
        // the backend belongs to the caller, who may still be using it — closing
        // that here would break unrelated work in the same process.
        await this.backend.close();
      } catch (error) {
        this.logger.error(
          { error },
          "Failed to close the backend during shutdown"
        );
      }
      // The connection that carried them is gone, so a later `start()` has to
      // install them again.
      this.subscriptionsStarted = false;
      // Drop the client instances too. Retaining them makes a later start()
      // treat them as unchanged and skip init(), leaving them with no
      // health-check interval — no orphan cleanup, no queue poke. The template
      // map goes with them, or the next start() skips every stored tenant as
      // already built.
      this.clients.clear();
      this.templateClientMap.clear();
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = undefined;
      this.startPromise = null;
      this.status = "stopped";
    }
    this.logger.warn(`Stopped request handler instance with ID ${this.id}`);
  }

  /**
   * Key prefix for this handler, for an observer subscribing to handler-scoped
   * channels (`requestDone`, `freezeStateChanged`) or reading its keys.
   */
  public getNamespace(): string {
    return this.namespace;
  }

  /**
   * This replica's id. A persisting listener compares it against
   * `publisherInstanceId` so only one replica writes a fanned-out broadcast.
   */
  public getInstanceId(): string {
    return this.id;
  }

  /** Every template registered here; anything in this list is a valid `templateName`. */
  public getRegisteredTemplates(): string[] {
    return Array.from(this.templates.keys());
  }

  /**
   * The rate-limit options a template lets callers choose between, for building a
   * plan picker. Empty when the template declares none, which also means
   * `addTemplateClient` will reject a `rateLimitOption` for it.
   */
  public getRateLimitOptions<K extends keyof ClientTemplates>(
    templateName: K
  ): string[] {
    const declared = this.templates.get(templateName as string)?.options
      .rateLimitOptions;
    return declared ? Object.keys(declared) : [];
  }

  /**
   * Every client registered on this replica with its effective limits — the
   * template default, then the chosen plan, then overrides and any
   * `rateLimitChange` since — every name resolved. `name` is the registered
   * name, which for a `sharedLimit` client is its own rather than its owner's.
   * The limits are copies: editing them changes no client.
   */
  public getLoadedClients(): Array<{
    name: string;
    rateLimit: NamedRateLimitData[];
  }> {
    return Array.from(this.clients.values()).map((c) => ({
      name: c.getName(),
      rateLimit: c.getRateLimit().map((limit) => ({ ...limit })),
    }));
  }

  /** Names produced by the most recent build; empty if it never ran here. */
  public getTemplateClientNames(
    templateName: string,
    instanceId: string
  ): string[] {
    const set = this.templateClientMap.get(templateName)?.get(instanceId);
    return set ? Array.from(set) : [];
  }

  /** Stored for an external scheduler to fire; dianemo never fires it itself. */
  public getProbeConfig(clientName: string): ProbeRequestConfig | undefined {
    return this.clients.get(clientName)?.getProbeConfig();
  }

  /** This replica's id, status, election priority and clients. */
  public getMetadata(this: RequestHandler): RequestHandlerMetadata {
    const metadata: RequestHandlerMetadata = {
      id: this.id,
      // Preserve the public status union for source compatibility. A stopping
      // handler no longer accepts work and is externally equivalent to stopped.
      status: this.status === "stopping" ? "stopped" : this.status,
      priority: this.priority,
      registeredClients: Array.from(this.clients.keys()),
      ownedClients: [],
    };
    for (const c of this.clients.values()) {
      if (c.getRole() !== "controller") continue;
      metadata.ownedClients.push(c.getName());
    }
    return metadata;
  }

  /**
   * Sends one request through `config.clientName`'s budgets, waiting its turn
   * in the fleet-wide queue, and resolves with the axios response.
   *
   * Starts the handler if it is not started, unless `stop()` stopped it.
   * Retries 429s, 5xxs and network
   * failures (`ECONNRESET`, `ETIMEDOUT`, `ECONNABORTED`) per the client's
   * `retryOptions`, freezing the client fleet-wide on each. Rejects with:
   *
   * - `ClientUnavailableError` — `handler_stopping` during `stop()`,
   *   `handler_stopped` after it, or if shutdown or the removal of its client
   *   failed it while queued or waiting to retry, and
   *   `shared_limit_parent_missing` for a `sharedLimit` client whose owner is
   *   gone.
   * - `ClientNotFoundError` when no client of that name appears within 5 s.
   * - `ConfigurationError` — `invalid_request_name`, `invalid_request_cost`,
   *   `invalid_request_priority`, `invalid_grant_id` or
   *   `invalid_request_signal` for an unusable field.
   * - `RequestCostExceedsBudgetError` for a `cost` above the client's tightest
   *   `maxTokens` or `maxConcurrency`, now or after a budget change while it
   *   queued.
   * - `RequestTimeoutError` when it is not admitted within the client's
   *   `requestOptions.cleanupTimeout` (default 60 s).
   * - `RequestAbortedError` when `config.signal` aborts before dispatch,
   *   including during a retry's back-off and after admission while the
   *   interceptor or a token refresh runs;
   *   what the request claimed is handed back. An abort once it is upstream
   *   reaches axios instead.
   * - From pre-request auth on an OAuth2 client:
   *   `GrantRefreshTokenMissingError` for a grant with no stored refresh
   *   token, `ConfigurationError` `missing_grant_refresh_config` or
   *   `invalid_token_response`, and the token endpoint's own axios error.
   * - Anything the client's `requestInterceptor` throws, unchanged.
   * - The axios error itself for an upstream failure that is not retried or
   *   has run out of retries.
   *
   * A pre-request failure hands back what admission claimed before it rejects.
   */
  public async handleRequest<TResponse = unknown, TRequestData = unknown>(
    config: RequestConfig<TRequestData>
  ): Promise<DianemoResponse<TResponse, TRequestData>> {
    if (this.status === "stopping" || this.stopPromise) {
      throw new ClientUnavailableError(
        "handler_stopping",
        "Request handler is stopping and cannot accept new requests."
      );
    }
    this.assertNotStopped("accept new requests");
    if (this.status !== "started") await this.start();
    const client = await this.waitForClient(config.clientName);
    return (await client.handleRequest(config as never)) as DianemoResponse<
      TResponse,
      TRequestData
    >;
  }

  /**
   * Briefly waits for a client whose credentials may still be in flight over
   * pub/sub, then throws `ClientNotFoundError` so a typo still surfaces loudly.
   */
  private async waitForClient(clientName: string): Promise<BaseClient> {
    const cached = this.clients.get(clientName);
    if (cached) return cached;

    const WAIT_FOR_CLIENT_TIMEOUT_MS = 5_000;
    const POLL_MS = 50;
    const deadline = Date.now() + WAIT_FOR_CLIENT_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, POLL_MS));
      const client = this.clients.get(clientName);
      if (client) return client;
    }
    throw new ClientNotFoundError(clientName);
  }

  /**
   * Resolves once `clientName` is registered locally, for a consumer whose first
   * call happens during its own boot.
   *
   * Warns at 30 s and throws a 503 at `timeoutMs` (default 5 min, overridable with
   * `DIANEMO_AWAIT_CLIENT_TIMEOUT_MS`), so a credential source that never comes up
   * fails a liveness probe rather than hanging. Deliberately more patient than the
   * per-request `waitForClient`, which stays impatient to surface typos.
   *
   * @throws {ClientUnavailableError} `client_unavailable` when `timeoutMs`
   * elapses, and `shutdown_aborted` when `signal` aborts, before or during the
   * wait.
   */
  public async awaitClient(
    clientName: string,
    opts: { signal?: AbortSignal; timeoutMs?: number } = {}
  ): Promise<void> {
    if (this.clients.has(clientName)) return;
    if (opts.signal?.aborted) {
      throw new ClientUnavailableError(
        "shutdown_aborted",
        "awaitClient was aborted"
      );
    }

    const channel = `clientRegistered:${clientName}`;
    const STALE_WAIT_WARN_MS = 30_000;
    const envTimeoutRaw = process.env.DIANEMO_AWAIT_CLIENT_TIMEOUT_MS;
    const envTimeout = envTimeoutRaw ? Number(envTimeoutRaw) : NaN;
    const timeoutMs =
      opts.timeoutMs ??
      (Number.isFinite(envTimeout) && envTimeout > 0 ? envTimeout : 300_000);

    await new Promise<void>((resolve, reject) => {
      const onRegistered = () => {
        cleanup();
        resolve();
      };
      const onAbort = () => {
        cleanup();
        reject(
          new ClientUnavailableError(
            "shutdown_aborted",
            "awaitClient was aborted"
          )
        );
      };
      const warnTimer = setTimeout(() => {
        this.logger.warn(
          { clientName },
          "Still awaiting client registration after 30s — has the credential source registered this client?"
        );
      }, STALE_WAIT_WARN_MS);
      const timeoutTimer = setTimeout(() => {
        cleanup();
        reject(
          new ClientUnavailableError(
            "client_unavailable",
            `Timed out after ${timeoutMs}ms awaiting client "${clientName}"`
          )
        );
      }, timeoutMs);
      const cleanup = () => {
        clearTimeout(warnTimer);
        clearTimeout(timeoutTimer);
        this.emitter.off(channel, onRegistered);
        opts.signal?.removeEventListener("abort", onAbort);
      };

      this.emitter.once(channel, onRegistered);
      opts.signal?.addEventListener("abort", onAbort);

      // Re-check after subscribing to close the TOCTOU window where the
      // client could have been registered between the cache check above
      // and the listener being installed.
      if (this.clients.has(clientName)) {
        cleanup();
        resolve();
      }
    });
  }

  /**
   * Registers a template builder. The name must be declared in `ClientTemplates`
   * via module augmentation, which is what types the credentials at the call site.
   *
   * `options` declares the rate-limit plans callers may choose between; see
   * {@link ClientTemplateOptions}. The plans are copied, so mutating the object
   * afterwards changes nothing.
   *
   * Called after `start()`, it immediately builds any credentials already
   * stored, logging and skipping one whose stored settings cannot be built.
   *
   * @throws {ConfigurationError} `invalid_template_name` for an empty name or
   * one containing `:` or whitespace, and the codes
   * {@link ClientTemplateOptions} lists for unusable plans.
   */
  public async registerClientTemplate<K extends keyof ClientTemplates>(
    name: K,
    builder: ClientTemplateBuilder<ClientTemplates[K]>,
    options: ClientTemplateOptions = {}
  ): Promise<void> {
    // The same rules `assertNameSegment` applies to the other segments: a `:`
    // shifts every later segment of the client name, and whitespace is
    // normalized to `_` in backend keys, so "my tpl" and "my_tpl" would share
    // one budget and one credential entry.
    const templateName: unknown = name;
    if (
      typeof templateName !== "string" ||
      templateName.length === 0 ||
      templateName.includes(":") ||
      /\s/.test(templateName)
    ) {
      throw new ConfigurationError(
        "invalid_template_name",
        `Template name ${JSON.stringify(name)} must be a non-empty string with no ":" and no whitespace — it is the first segment of every client name it builds, and backend keys normalize whitespace to "_".`
      );
    }
    assertTemplateOptions(name as string, options);
    this.templates.set(name as string, {
      builder: builder as ClientTemplateBuilder<unknown>,
      // A copy of the plans, so a caller mutating its own object afterwards
      // cannot change what was validated above.
      options: {
        ...options,
        rateLimitOptions: options.rateLimitOptions
          ? structuredClone(options.rateLimitOptions)
          : undefined,
      },
    });

    if (this.status !== "started") return;

    const entries = await this.backend.smembers(`${this.namespace}:templates`);
    const prefix = `${name}::`;
    const myEntries = entries.filter((e) => e.startsWith(prefix));

    for (const entry of myEntries) {
      // Through the rebuild path rather than reading the blob here, so a client
      // built by a late registration gets the same stored plan and overrides one
      // built at startup does. One that fails is logged and stepped over, as at
      // startup, so it neither strands the tenants after it nor skips election.
      try {
        await this.rebuildTemplateClient(
          name as string,
          entry.slice(prefix.length)
        );
      } catch (error) {
        this.logger.error(
          { error },
          `Template client ${entry} could not be rebuilt for the late registration of "${String(name)}"; skipping it`
        );
      }
    }

    if (myEntries.length > 0) {
      await this.scheduleClientRoles();
    }
  }

  /**
   * Coalesces role re-elections into one in-flight run plus at most one rerun, the
   * same lock+pending idiom as `processRequests`. One credential change triggers
   * every instance at once, and election is O(clients) plus a backend read.
   *
   * The returned promise resolves only after a run that began after this call, so a
   * caller still observes its own change.
   */
  public scheduleClientRoles(isStartup = false): Promise<void> {
    if (isStartup) this.rolesRerunStartup = true;
    if (this.rolesRunning) {
      this.rolesRerun = true;
      return this.rolesPromise;
    }
    this.rolesRunning = true;
    this.rolesPromise = (async () => {
      try {
        do {
          this.rolesRerun = false;
          const startup = this.rolesRerunStartup;
          this.rolesRerunStartup = false;
          await updateClientRoles.bind(this)(startup);
        } while (this.rolesRerun);
      } finally {
        this.rolesRunning = false;
      }
    })();
    return this.rolesPromise;
  }

  /**
   * Adds a template client, encrypting its credentials into the backend so peers and
   * restarts can rebuild it. Auto-starts the handler unless `stop()` stopped
   * it.
   *
   * For credentials one writer distributes to many readers; use
   * {@link addLocalTemplateClient} for per-replica ones.
   *
   * Credentials are stored as JSON, and every build — this one included — is
   * handed them as JSON reads them back: a `Date` arrives as its ISO string, a
   * `Map` as an empty object, and an `undefined` field not at all.
   *
   * `options` carries this instance's `rateLimitOption` — one of the plans the
   * template declared — and any operator `rateLimitOverrides`. A bare overrides
   * record is still accepted in that position, as the argument used to be.
   * Re-adding an instance replaces its credentials and options; passing none
   * clears any it had.
   *
   * An unregistered template is logged and skipped rather than thrown.
   *
   * @throws {ConfigurationError} before anything is stored:
   * `unknown_rate_limit_option` for a plan the template does not offer,
   * `invalid_template_client_options` for an object mixing `rateLimitOption` or
   * `rateLimitOverrides` with path keys, the codes {@link RateLimitOverrides}
   * lists for an unusable override, and `invalid_client_name_segment` for an
   * `instanceId` or `organizationId` that cannot be a name segment,
   * `template_client_name_mismatch` from a builder whose client names omit the
   * `instanceId`, and anything else building the clients would throw — the
   * codes `CreateClientData.rateLimit` lists, or the builder's own error. A
   * re-add refused this way leaves the instance's existing clients running on
   * what was stored before.
   * @throws {ClientUnavailableError} `handler_stopping` during `stop()`, and
   * `handler_stopped` after it until `start()` is called.
   */
  public async addTemplateClient<K extends keyof ClientTemplates>(
    templateName: K,
    credentials: ClientTemplates[K] & { instanceId: string },
    options?: TemplateClientOptions | RateLimitOverrides
  ): Promise<void> {
    // status === "starting" means we're inside doStart already (e.g., called from a startup
    // hook); the backend is initialized at that point, so it's safe to proceed without auto-starting
    // — and awaiting start() from within itself would deadlock.
    this.assertTemplateMutationAllowed();
    if (this.status === "stopped") await this.start();
    await this.doAddTemplateClient(
      templateName,
      credentials,
      normalizeTemplateClientOptions(templateName as string, options)
    );
    await this.scheduleClientRoles();
  }

  /**
   * Builds a template client in-process, writing nothing to the backend and
   * broadcasting nothing.
   *
   * For credentials each replica already holds itself, injected via env: there is no
   * shared secret to distribute, and putting a per-replica identity on the shared bus
   * would leak it. Every replica calls this at startup and they converge without
   * pub/sub. Contrast {@link addTemplateClient}.
   *
   * `options` is as {@link addTemplateClient} takes it, and `instanceId` and
   * `organizationId` are validated the same way, with the same
   * `ConfigurationError` codes. An unregistered template is logged and skipped.
   *
   * @throws {ClientUnavailableError} `handler_stopping` during `stop()`, and
   * `handler_stopped` after it until `start()` is called.
   */
  public async addLocalTemplateClient<K extends keyof ClientTemplates>(
    templateName: K,
    credentials: ClientTemplates[K] & { instanceId: string },
    options?: TemplateClientOptions | RateLimitOverrides
  ): Promise<void> {
    this.assertTemplateMutationAllowed();
    if (this.status === "stopped") await this.start();
    if (!this.templates.has(templateName)) {
      this.logger.warn(
        `addLocalTemplateClient called for unregistered template "${templateName}", skipping`
      );
      return;
    }
    const settings = normalizeTemplateClientOptions(
      templateName as string,
      options
    );
    this.assertRateLimitOptionAllowed(templateName, settings.rateLimitOption);
    const { instanceId } = credentials as { instanceId: string };
    assertInstanceSegments(credentials);
    this.localTemplateClients.add(`${templateName}::${instanceId}`);
    await buildAndRegisterTemplateClients.bind(this)(
      templateName,
      instanceId,
      credentials,
      settings
    );
    await this.scheduleClientRoles();
  }

  /**
   * Refuses a plan the template does not offer.
   *
   * At the boundary rather than at build time: a key stored now is read back by
   * every replica and every restart, so accepting one the template never declared
   * would put a client on the template default while its record says otherwise.
   */
  private assertRateLimitOptionAllowed(
    templateName: string,
    rateLimitOption: string | undefined
  ): void {
    if (rateLimitOption === undefined) return;
    const declared = this.templates.get(templateName)?.options.rateLimitOptions;
    const available = declared ? Object.keys(declared) : [];
    if (available.includes(rateLimitOption)) return;
    throw new ConfigurationError(
      "unknown_rate_limit_option",
      available.length === 0
        ? `Template "${templateName}" declares no rateLimitOptions, so it accepts no rateLimitOption. A template chooses which limits callers may pick from; add them at registerClientTemplate.`
        : `Template "${templateName}" has no rateLimitOption "${rateLimitOption}". It offers: ${available
            .map((key) => `"${key}"`)
            .join(", ")}.`
    );
  }

  private async doAddTemplateClient(
    templateName: string,
    credentials: { instanceId: string },
    settings: TemplateClientOptions
  ): Promise<void> {
    if (!this.templates.has(templateName)) {
      this.logger.warn(
        `addTemplateClient called for unregistered template "${templateName}", skipping`
      );
      return;
    }

    this.assertRateLimitOptionAllowed(templateName, settings.rateLimitOption);

    const { instanceId } = credentials;
    assertInstanceSegments(credentials);
    // Before the write, so credentials whose clients cannot be built are refused
    // with nothing stored, and the running clients and every restart keep the
    // record that works. Checked as they will be read back: every build after
    // this one decodes them from JSON, which turns a `Date` into a string.
    assertTemplateClientBuildable.bind(this)(
      templateName,
      instanceId,
      JSON.parse(JSON.stringify(credentials)),
      settings
    );
    const entry = `${templateName}::${instanceId}`;
    const encrypted = encryptCredentials(credentials, this.key);

    // MULTI/EXEC for atomicity — peers reading mid-write would otherwise
    // see template (new) but settings (stale-or-not-yet-deleted), and the
    // subsequent broadcast would race ahead of the cleanup. The window is
    // small but the cost of MULTI vs pipeline is negligible.
    const settingsJson = serializeTemplateClientOptions(settings);
    await this.backend.batch(
      [
        {
          op: "set",
          key: `${this.namespace}:template:${entry}`,
          value: encrypted,
        },
        { op: "sadd", key: `${this.namespace}:templates`, member: entry },
        settingsJson
          ? {
              op: "set" as const,
              key: `${this.namespace}:overrides:${entry}`,
              value: settingsJson,
            }
          : { op: "del" as const, key: `${this.namespace}:overrides:${entry}` },
      ],
      { atomic: true }
    );

    // Re-read from the backend so concurrent writers can't leave us with stale local
    // creds — last-write-wins from the backend is the source of truth on every replica.
    await this.rebuildTemplateClient(templateName, instanceId);

    await this.backend.publish(
      `${this.namespace}:templateClientAdded`,
      JSON.stringify({ templateName, instanceId })
    );
  }

  /**
   * Rebuilds from what the backend currently holds, rather than from what a caller
   * or a broadcast said — so cross-publisher pub/sub ordering cannot desync replicas.
   */
  public async rebuildTemplateClient(
    templateName: string,
    instanceId: string
  ): Promise<void> {
    const entry = `${templateName}::${instanceId}`;
    const generation = templateEntryGeneration.call(
      this,
      templateName,
      instanceId
    );
    const encrypted = await this.backend.get(
      `${this.namespace}:template:${entry}`
    );
    if (!encrypted) return;
    const credentials = decryptCredentials(encrypted, this.key);
    const settings = await readTemplateClientOptions.bind(this)(entry);
    await buildAndRegisterTemplateClients.bind(this)(
      templateName,
      instanceId,
      credentials,
      settings,
      generation
    );
  }

  /**
   * Removes a template instance everywhere: destroys its clients here, failing
   * their parked requests, deletes its stored credentials, settings and OAuth
   * tokens, and tells every replica to destroy theirs. Starts the handler if it
   * is `"stopped"` and `stop()` did not stop it; one already starting is not
   * awaited, so this is safe from a startup hook. A no-op for an instance that
   * does not exist.
   *
   * @throws {ClientUnavailableError} `handler_stopping` during `stop()`, and
   * `handler_stopped` after it until `start()` is called.
   */
  public async removeTemplateClient<K extends keyof ClientTemplates>(
    templateName: K,
    instanceId: string
  ): Promise<void> {
    this.assertTemplateMutationAllowed();
    if (this.status === "stopped") await this.start();

    // Purges credentials, unlike the rebuild and peer-notification paths.
    await destroyTemplateClients.bind(this)(templateName, instanceId, true);

    const entry = `${templateName}::${instanceId}`;
    await this.backend.batch([
      { op: "del", key: `${this.namespace}:template:${entry}` },
      { op: "del", key: `${this.namespace}:overrides:${entry}` },
      { op: "srem", key: `${this.namespace}:templates`, member: entry },
      {
        op: "del",
        key: templateClientNamesKey(this.namespace, entry),
      },
    ]);

    await this.backend.publish(
      `${this.namespace}:templateClientRemoved`,
      JSON.stringify({ templateName, instanceId })
    );
  }

  private assertTemplateMutationAllowed(): void {
    if (this.status === "stopping" || this.stopPromise) {
      throw new ClientUnavailableError(
        "handler_stopping",
        "Request handler is stopping and cannot mutate template clients."
      );
    }
    this.assertNotStopped("mutate template clients");
  }

  /**
   * Refuses work after `stop()`, which would otherwise auto-start the handler
   * again — re-registering it with the fleet and re-arming the heartbeat that
   * keeps the process alive.
   */
  private assertNotStopped(action: string): void {
    if (!this.stoppedByCaller) return;
    throw new ClientUnavailableError(
      "handler_stopped",
      `Request handler was stopped and cannot ${action} until start() is called again.`
    );
  }

  protected getClient(clientName: string): BaseClient {
    const client = this.clients.get(clientName);
    if (client) return client;
    throw new ClientNotFoundError(clientName);
  }

  /**
   * A client's freeze state, the client-level balance of each declared limit
   * (`rateLimit`, one entry per limit), and what is queued and in flight
   * fleet-wide. See `ClientStatistics` in `@dianemo/core/client/types`.
   *
   * @throws {ClientNotFoundError} when no client of that name is registered
   * on this replica.
   */
  public async getClientStats(clientName: string) {
    return this.getClient(clientName).getStats();
  }

  /**
   * Stores a grant's OAuth2 tokens, for a host seeding them after a user
   * completes authorization. Requests naming that `grantId` then use them, and
   * they are refreshed through the client's `grantRefreshConfig`.
   *
   * @throws {ClientNotFoundError} when no client of that name is registered.
   * @throws {NotOAuth2ClientError} `client_not_oauth2` for a client whose
   * authentication is not OAuth2.
   * @throws {ClientUnavailableError} `client_removed` for a client removed while
   * the call was in flight, whose credentials have already been purged.
   */
  public async setGrantTokens(
    clientName: string,
    grantId: string,
    data: SetGrantTokensData
  ): Promise<void> {
    const client = this.getClient(clientName);
    await client.setGrantTokens(grantId, data);
  }
}
