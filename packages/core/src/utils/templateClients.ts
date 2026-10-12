import type { CreateClientData, DeclaredRateLimit } from "../client/types.js";
import { retireCredentialWrites } from "../client/methods/authenticate.js";
import { noopLogger, type Logger } from "../logger.js";
import { encrypt, decrypt } from "./encryption.js";
import { ConfigurationError } from "../errors.js";
import type RequestHandler from "../index.js";
import {
  collectClientNames,
  deepCloneWithFunctions,
  prepareClients,
} from "./createClients.js";
import {
  assertUsableRateLimits,
  isSharedLimitOnly,
  normalizeRateLimit,
} from "./rateLimit.js";
import type {
  ClientTemplateContext,
  ClientTemplateOptions,
  RateLimitOverrides,
  RateLimitPlan,
  TemplateClientOptions,
} from "../types.js";

/**
 * Serializes the builds and removals of one `(templateName, instanceId)`, per
 * handler, so two in one process stay independent. A local writer races its
 * own `templateClientAdded` broadcast, and a removal can arrive while a rebuild
 * sits between reading the record and registering its clients.
 */
const entryLocks = new WeakMap<RequestHandler, Map<string, Promise<void>>>();

/**
 * Bumped by every removal, so a build holding a record read before one can
 * tell that the record is gone and register nothing.
 */
const entryGenerations = new WeakMap<RequestHandler, Map<string, number>>();

function generationsOf(handler: RequestHandler): Map<string, number> {
  let generations = entryGenerations.get(handler);
  if (!generations) {
    generations = new Map();
    entryGenerations.set(handler, generations);
  }
  return generations;
}

/**
 * How many removals this entry has seen. Read it before reading the record a
 * build will use, and hand it to {@link buildAndRegisterTemplateClients}.
 */
export function templateEntryGeneration(
  this: RequestHandler,
  templateName: string,
  instanceId: string
): number {
  return generationsOf(this).get(`${templateName}::${instanceId}`) ?? 0;
}

async function underEntryLock(
  handler: RequestHandler,
  entry: string,
  fn: () => Promise<void>
): Promise<void> {
  let locks = entryLocks.get(handler);
  if (!locks) {
    locks = new Map();
    entryLocks.set(handler, locks);
  }
  const previous = locks.get(entry);
  const run = (async () => {
    // A failed predecessor does not stop this one.
    if (previous) await previous.catch(() => undefined);
    await fn();
  })();
  locks.set(entry, run);
  try {
    await run;
  } finally {
    if (locks.get(entry) === run) locks.delete(entry);
  }
}

/**
 * Entries already present in `templateClientMap` (i.e. built earlier in the
 * same `start()` by a startup hook calling `addTemplateClient`) are skipped,
 * so a client built moments ago is not torn down and rebuilt from the same
 * record.
 */
export async function loadTemplateClientsFromBackend(
  this: RequestHandler
): Promise<void> {
  const entries = await this.backend.smembers(`${this.namespace}:templates`);
  const seenInBackend = new Set<string>();

  for (const entry of entries) {
    const separatorIdx = entry.indexOf("::");
    if (separatorIdx === -1) continue;
    const templateName = entry.slice(0, separatorIdx);
    const instanceId = entry.slice(separatorIdx + 2);
    seenInBackend.add(`${templateName}::${instanceId}`);

    if (!this.templates.has(templateName)) {
      this.logger.warn(
        `Template "${templateName}" is in the backend but not registered on this instance — skipping`
      );
      continue;
    }
    if (this.templateClientMap.get(templateName)?.has(instanceId)) continue;

    const generation = templateEntryGeneration.call(
      this,
      templateName,
      instanceId
    );
    const encrypted = await this.backend.get(
      `${this.namespace}:template:${entry}`
    );
    if (!encrypted) continue;

    // Ciphertext written under a previous handler key is an expected,
    // recoverable state — the OAuth token path treats it that way too — so it
    // is logged and stepped over rather than failing `start()` and every later
    // template in this loop.
    let credentials: unknown;
    try {
      credentials = decryptCredentials(encrypted, this.key);
    } catch {
      this.logger.error(
        `Stored credentials for ${entry} could not be decrypted with the current key; skipping this client. Re-register it to store credentials under the new key.`
      );
      continue;
    }

    // Same reasoning as the decryption failure above: one tenant whose stored
    // settings no longer build must not keep every other tenant offline.
    try {
      const settings = await readTemplateClientOptions.bind(this)(entry);
      await buildAndRegisterTemplateClients.bind(this)(
        templateName,
        instanceId,
        credentials,
        settings,
        generation
      );
    } catch (error) {
      this.logger.error(
        { error },
        `Template client ${entry} could not be built from its stored settings; skipping it. Re-add it with addTemplateClient to replace them.`
      );
    }
  }

  for (const [templateName, instances] of this.templateClientMap) {
    for (const instanceId of Array.from(instances.keys())) {
      const key = `${templateName}::${instanceId}`;
      if (seenInBackend.has(key)) continue;
      if (this.localTemplateClients.has(key)) continue;
      await destroyTemplateClients.bind(this)(templateName, instanceId);
    }
  }
}

/**
 * Sister to the encrypted credentials key — these aren't sensitive, so they are
 * stored as plain JSON and a replica that cannot decrypt the credentials can
 * still read them. Missing key, or unparseable JSON, means template defaults.
 */
export async function readTemplateClientOptions(
  this: RequestHandler,
  entry: string
): Promise<TemplateClientOptions | undefined> {
  const raw = await this.backend.get(`${this.namespace}:overrides:${entry}`);
  if (!raw) return undefined;
  try {
    const settings = decodeTemplateClientOptions(JSON.parse(raw));
    // Stored by 1.x, whose overrides were single limits rather than lists. Read
    // as lists rather than refused, because refusing would make every tenant
    // with an override unbuildable on the first start after the upgrade.
    const overrides = settings.rateLimitOverrides;
    if (overrides) {
      for (const [path, value] of Object.entries(overrides)) {
        if (isLimitObject(value)) overrides[path] = [value];
      }
    }
    return settings;
  } catch {
    this.logger.warn(`Malformed template options JSON for ${entry}; ignoring`);
    return undefined;
  }
}

function isLimitObject(value: unknown): value is DeclaredRateLimit {
  return (
    !!value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    typeof (value as { type?: unknown }).type === "string"
  );
}

/**
 * Reads back what this key holds, in either of the two shapes it has had.
 *
 * Before rate-limit options existed the value was the overrides record itself,
 * and those entries are still in every backend that predates this — so an object
 * naming neither of the current fields is read as that record rather than as an
 * empty settings object, which would silently drop an operator's overrides on
 * the first restart after an upgrade.
 */
function decodeTemplateClientOptions(parsed: unknown): TemplateClientOptions {
  if (!parsed || typeof parsed !== "object") return {};
  const value = parsed as Record<string, unknown>;
  if ("rateLimitOption" in value || "rateLimitOverrides" in value) {
    return value as TemplateClientOptions;
  }
  return { rateLimitOverrides: value as RateLimitOverrides };
}

/**
 * Accepts either shape at the call site too, so the argument that used to be a
 * bare overrides record still is one.
 *
 * Validated here, before anything is stored: a record that cannot be built from
 * would otherwise be written first and refused only by the rebuild after it,
 * leaving the tenant with no client and the backend holding the bad record for
 * every restart to trip on.
 */
export function normalizeTemplateClientOptions(
  templateName: string,
  options?: TemplateClientOptions | RateLimitOverrides
): TemplateClientOptions {
  if (!options) return {};
  const settings = decodeTemplateClientOptions(options);
  // Typed as a union whose overrides arm has an index signature, so TypeScript
  // accepts `{ rateLimitOption, "": [...] }` and the path would be dropped here.
  const extra =
    settings === options
      ? Object.keys(options).filter(
          (key) => key !== "rateLimitOption" && key !== "rateLimitOverrides"
        )
      : [];
  if (extra.length > 0) {
    throw new ConfigurationError(
      "invalid_template_client_options",
      `Template "${templateName}" was given options mixing rateLimitOption or rateLimitOverrides with ${extra
        .map((key) => `"${key}"`)
        .join(", ")}. Put path overrides under rateLimitOverrides.`
    );
  }
  for (const [path, value] of Object.entries(
    settings.rateLimitOverrides ?? {}
  )) {
    const label = `${templateName} (override${path === "" ? "" : ` for path "${path}"`})`;
    if (!Array.isArray(value)) {
      throw new ConfigurationError(
        "unknown_rate_limit_type",
        `Client template ${label} gives a rate limit that is not a list. It takes a list, so wrap it: [${JSON.stringify(
          value
        )}].`
      );
    }
    assertUsableRateLimits(normalizeRateLimit(value), label);
  }
  return settings;
}

/** The JSON to store, or `undefined` when there is nothing worth a key. */
export function serializeTemplateClientOptions(
  settings: TemplateClientOptions
): string | undefined {
  const overrides = settings.rateLimitOverrides;
  const hasOverrides = !!overrides && Object.keys(overrides).length > 0;
  if (!hasOverrides && settings.rateLimitOption === undefined) return undefined;
  return JSON.stringify({
    ...(settings.rateLimitOption === undefined
      ? {}
      : { rateLimitOption: settings.rateLimitOption }),
    ...(hasOverrides ? { rateLimitOverrides: overrides } : {}),
  });
}

/**
 * Rejects a template whose declared options could not be built from.
 *
 * At registration, because these are the plugin author's own constants: a plan
 * with an unusable budget should fail the deploy that introduced it, not the
 * first tenant who picks it.
 */
export function assertTemplateOptions(
  templateName: string,
  options: ClientTemplateOptions
): void {
  const declared = options.rateLimitOptions;
  if (declared) {
    for (const [key, plan] of Object.entries(declared)) {
      // Shape first: plans reach here from untyped plugin code too, and anything
      // but a list or a map of lists fails inside normalisation as a TypeError
      // naming neither the template nor the fix.
      const paths =
        isPathMap(plan) && !isLimitObject(plan)
          ? Object.entries(plan)
          : [["", plan]];
      for (const [path, rateLimit] of paths as [string, unknown][]) {
        const label = `${templateName} (option "${key}"${path === "" ? "" : `, path "${path}"`})`;
        if (!Array.isArray(rateLimit)) {
          throw new ConfigurationError(
            "unknown_rate_limit_type",
            `Client template ${label} gives a rate limit that is not a list. A plan is a list of limits, or an object mapping sub-client paths to lists: wrap it as [${JSON.stringify(
              rateLimit ?? null
            )}].`
          );
        }
        // Names resolved first, as `buildClient` will: a plan whose two limits
        // both take the default is a duplicate, and the deploy should say so.
        assertUsableRateLimits(normalizeRateLimit(rateLimit), label);
      }
    }
  }
  const fallback = options.defaultRateLimitOption;
  if (fallback === undefined) return;
  if (declared && Object.hasOwn(declared, fallback)) return;
  throw new ConfigurationError(
    "unknown_rate_limit_option",
    `Template "${templateName}" names "${fallback}" as its defaultRateLimitOption, which is not one of its rateLimitOptions${
      declared ? `: ${Object.keys(declared).join(", ")}` : " (it declares none)"
    }.`
  );
}

/**
 * Builds and registers an instance's clients from a record the caller read.
 * `generation` is {@link templateEntryGeneration} as it stood before that
 * read: when a removal has happened since, the record is stale and nothing is
 * registered.
 */
export async function buildAndRegisterTemplateClients(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  credentials: unknown,
  settings?: TemplateClientOptions,
  generation = templateEntryGeneration.call(this, templateName, instanceId)
): Promise<void> {
  const superseded = () =>
    templateEntryGeneration.call(this, templateName, instanceId) !== generation;
  await underEntryLock(this, `${templateName}::${instanceId}`, async () => {
    if (superseded()) return;
    await doBuildAndRegister.call(
      this,
      templateName,
      instanceId,
      credentials,
      superseded,
      settings
    );
  });
}

async function doBuildAndRegister(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  credentials: unknown,
  superseded: () => boolean,
  settings?: TemplateClientOptions
): Promise<void> {
  const build = planTemplateBuild.call(
    this,
    this.logger,
    templateName,
    instanceId,
    credentials,
    settings
  );
  if (!build) {
    this.logger.warn(
      `No builder registered for template "${templateName}", skipping`
    );
    return;
  }
  const { clientDataList, newClientNames } = build;
  const retiring = retiringNames.call(
    this,
    templateName,
    instanceId,
    newClientNames
  );
  // Before anything is torn down, so a config that cannot be built throws here
  // and leaves the working clients in place.
  const commit = prepareClients.bind(this)(clientDataList, retiring);

  for (const prevName of retiring) {
    const stale = this.clients.get(prevName);
    if (stale) {
      // A name the template stopped producing: nothing replaces it, so its parked
      // requests are failed rather than handed on.
      await stale.destroy("removal");
      this.clients.delete(prevName);
    }
  }

  if (!this.templateClientMap.has(templateName)) {
    this.templateClientMap.set(templateName, new Map());
  }
  this.templateClientMap.get(templateName)!.set(instanceId, newClientNames);

  // Recorded so a replica that never built these clients can still find their
  // credential keys when the instance is removed.
  const namesKey = templateClientNamesKey(
    this.namespace,
    `${templateName}::${instanceId}`
  );
  await this.backend.del(namesKey).catch(() => {});
  for (const name of newClientNames) {
    await this.backend.sadd(namesKey, name).catch(() => {});
  }

  // After the last await: a removal arriving during one is queued on this
  // lock, and would find nothing registered to destroy.
  if (superseded()) return;
  await commit();
}

/**
 * Throws whatever building these credentials and settings would throw, and
 * changes nothing — for a caller about to store them, so a record that cannot
 * be built is refused rather than written.
 */
export function assertTemplateClientBuildable(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  credentials: unknown,
  settings?: TemplateClientOptions
): void {
  const build = planTemplateBuild.call(
    this,
    noopLogger,
    templateName,
    instanceId,
    credentials,
    settings
  );
  if (!build) return;
  prepareClients.bind(this)(
    build.clientDataList,
    retiringNames.call(this, templateName, instanceId, build.newClientNames)
  );
}

function retiringNames(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  newClientNames: ReadonlySet<string>
): Set<string> {
  const previous =
    this.templateClientMap.get(templateName)?.get(instanceId) ?? [];
  return new Set([...previous].filter((name) => !newClientNames.has(name)));
}

/**
 * Runs the builder and places the plan and overrides on a copy of what it
 * returns.
 * `undefined` when the template is not registered here.
 */
function planTemplateBuild(
  this: RequestHandler,
  log: Logger,
  templateName: string,
  instanceId: string,
  credentials: unknown,
  settings?: TemplateClientOptions
):
  | { clientDataList: CreateClientData[]; newClientNames: Set<string> }
  | undefined {
  const template = this.templates.get(templateName);
  if (!template) return undefined;

  const context = resolveTemplateContext(
    log,
    templateName,
    template.options,
    settings
  );
  const result = template.builder(credentials, context);
  // Copied before the plan and overrides are written in: a builder may return
  // objects it shares between tenants or keeps from one build to the next.
  const clientDataList: CreateClientData[] = (
    Array.isArray(result) ? result : [result]
  ).map((client) => deepCloneWithFunctions(client));

  // The plan first, then the overrides on top: the plan is what the template
  // says this tenant is entitled to, and an override is the operator overruling
  // it. The other order would let a plan quietly undo an operator's decision.
  if (context.rateLimits) {
    applyPlan(
      log,
      templateName,
      context.rateLimitOption!,
      clientDataList,
      context.rateLimits
    );
  }

  const overrides = settings?.rateLimitOverrides;
  // A shape mismatch skips the override — changing shape would switch the
  // underlying client class.
  if (overrides && Object.keys(overrides).length > 0) {
    for (const root of clientDataList) {
      applyOverrides(log, root, overrides, "");
    }
  }

  // The names are third-party builder output, and `doAddTemplateClient` validates
  // only the segments it was given, not that the builder used them. A name that does
  // not vary by instance collapses every tenant onto one client, which
  // `generateClients` destroys and recreates per tenant rather than reporting a
  // conflict — so each tenant's requests go out under whichever authenticated first.
  //
  // Only the instance segment is required: a template may legitimately build several
  // differently-named root clients.
  for (const client of clientDataList) {
    const segments = client.name.split(":");
    if (!segments.includes(instanceId)) {
      throw new ConfigurationError(
        "template_client_name_mismatch",
        `Template "${templateName}" built a client named "${client.name}" for instance "${instanceId}", which does not appear in the name. Client names must identify their instance — use buildClientName() — or two tenants resolve to one client and one credential entry.`
      );
    }
  }

  return { clientDataList, newClientNames: collectClientNames(clientDataList) };
}

/**
 * Resolves the plan a template client is on into what its builder is handed.
 *
 * An option this template no longer declares is dropped rather than refused: the
 * key was written when it was valid, and a plugin that renames a plan should not
 * make every client already on it unbuildable on the next restart. Logged as an
 * error, since a tenant built purely from the plan is now on the builder's own
 * limits — `noLimit`, for a builder that declares none.
 */
function resolveTemplateContext(
  log: Logger,
  templateName: string,
  options: ClientTemplateOptions,
  settings?: TemplateClientOptions
): ClientTemplateContext {
  const chosen = settings?.rateLimitOption ?? options.defaultRateLimitOption;
  if (chosen === undefined) return {};
  const declared = options.rateLimitOptions;
  const plan =
    declared && Object.hasOwn(declared, chosen) ? declared[chosen] : undefined;
  if (plan === undefined) {
    log.error(
      `Template "${templateName}" no longer declares a rateLimitOption "${chosen}"; building from the limits its builder declares instead`
    );
    return {};
  }
  // A copy per build: the plan is shared by every tenant on it, and a builder
  // tuning `ctx.rateLimit` for one tenant would otherwise retune it for all of
  // them, past the validation registration gave it.
  const rateLimits = structuredClone(planPaths(plan));
  return {
    rateLimit: ownEntry(rateLimits, ""),
    rateLimits,
    rateLimitOption: chosen,
  };
}

/**
 * A plan as the path map everything downstream works in.
 *
 * A plan is either the root client's limits or a limit list per sub-client path,
 * and since a limit list is always an array, `Array.isArray` tells them apart
 * outright — no inspecting the value to guess which it is.
 */
export function planPaths(
  plan: RateLimitPlan
): Record<string, DeclaredRateLimit[]> {
  return Array.isArray(plan) ? { "": plan } : plan;
}

function isPathMap(plan: unknown): plan is Record<string, unknown> {
  return !!plan && typeof plan === "object" && !Array.isArray(plan);
}

/**
 * A path's entry, read only from the map itself: paths are sub-client names, and
 * a sub-client called `constructor` would otherwise be handed
 * `Object.prototype.constructor` as its limits.
 */
function ownEntry<T>(map: Record<string, T>, path: string): T | undefined {
  return Object.hasOwn(map, path) ? map[path] : undefined;
}

/**
 * Places a chosen plan's limits onto the clients the builder produced.
 *
 * Held to the same shape rule as an override: a plan retunes budgets, and never
 * turns a client that borrows another's budget into one with its own, or the
 * reverse. So `""` reaches every root client except a `sharedLimit` one, which
 * keeps drawing on its owner — the owner that the same `""` entry just retuned.
 *
 * A path matching no client is a plugin bug — a renamed sub-client, most likely —
 * so it is reported rather than dropped, since the symptom otherwise is one
 * endpoint quietly running unlimited. So is a path that only clients of the
 * other shape match.
 */
function applyPlan(
  log: Logger,
  templateName: string,
  option: string,
  clients: CreateClientData[],
  rateLimits: Record<string, DeclaredRateLimit[]>
): void {
  const applied = new Set<string>();
  const refused = new Map<string, string>();
  const walk = (
    client: CreateClientData,
    path: string,
    inherited?: DeclaredRateLimit[]
  ): void => {
    let effective: DeclaredRateLimit[] = client.rateLimit ??
      inherited ?? [{ type: "noLimit" }];
    const limit = ownEntry(rateLimits, path);
    if (limit !== undefined) {
      const mismatch = describeShapeMismatch(effective, limit);
      if (mismatch) {
        refused.set(path, mismatch);
      } else {
        client.rateLimit = limit;
        effective = limit;
        applied.add(path);
      }
    }
    for (const sub of client.subClients ?? []) {
      walk(sub, path === "" ? sub.name : `${path}:${sub.name}`, effective);
    }
  };
  for (const root of clients) walk(root, "");

  for (const path of Object.keys(rateLimits)) {
    if (applied.has(path)) continue;
    const mismatch = refused.get(path);
    log.warn(
      mismatch
        ? `Template "${templateName}" has a rate-limit option "${option}" whose limits for path "${path}" ${mismatch}; that limit was not applied`
        : `Template "${templateName}" has a rate-limit option covering path "${path}", which no client it built matches; that limit was not applied`
    );
  }
}

/**
 * `inherited` is the parent's effective limits: a sub-client declaring none
 * takes its parent's, so that, not `noLimit`, is what an override replaces.
 */
function applyOverrides(
  log: Logger,
  client: CreateClientData,
  overrides: RateLimitOverrides,
  path: string,
  inherited?: DeclaredRateLimit[]
): void {
  const existing: DeclaredRateLimit[] = client.rateLimit ??
    inherited ?? [{ type: "noLimit" }];
  const override = ownEntry(overrides, path);
  let effective = existing;
  if (override !== undefined) {
    const mismatch = describeShapeMismatch(existing, override);
    if (mismatch) {
      log.warn(
        `Rate-limit override for path "${path}" ${mismatch} — skipping override`
      );
    } else {
      // Named against what it replaces, as a `rateLimitChange` is: an override
      // retuning a named limit without repeating its name keeps its budget.
      effective = normalizeRateLimit(
        override,
        Array.isArray(existing) ? normalizeRateLimit(existing) : undefined
      );
      client.rateLimit = effective;
    }
  }
  if (client.subClients) {
    for (const sub of client.subClients) {
      const subPath = path === "" ? sub.name : `${path}:${sub.name}`;
      applyOverrides(log, sub, overrides, subPath, effective);
    }
  }
}

/**
 * Why an override may not replace a template default, or `undefined` when it may.
 *
 * An override retunes budgets the template declared; it does not change what
 * kind of client is built. Only one distinction still does that: a client that
 * borrows another's budget owns no queue, so swapping a `sharedLimit` for
 * budgets of its own — or the reverse — is a different client, not a different
 * limit. Everything else an override may freely change.
 */
function describeShapeMismatch(
  existing: readonly DeclaredRateLimit[],
  override: readonly DeclaredRateLimit[]
): string | undefined {
  const existingIsShared = isSharedLimitOnly(existing);
  if (existingIsShared === isSharedLimitOnly(override)) return undefined;
  return existingIsShared
    ? "declares budgets of its own where the template default shares another client's"
    : "shares another client's budget where the template default declares its own";
}

/**
 * Removes a template instance's clients from this replica, failing their parked
 * requests.
 *
 * Their credential writes are stopped before anything else, so a refresh still
 * in flight cannot write tokens back after they are purged. When the backend no
 * longer lists the instance, its credentials are purged here as well: a peer's
 * refresh can land between the removing replica's purge and this replica
 * hearing of it.
 *
 * Runs under the same per-instance lock as a build, and makes any build that
 * read its record before this call register nothing.
 */
export async function destroyTemplateClients(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  /**
   * Whether this replica is the one removing the instance, which also decides
   * whether fleet-wide state may be cleared. A peer hearing of the removal, or
   * a startup finding the instance gone from the backend, passes false.
   */
  purgeCredentials = false
): Promise<void> {
  const entry = `${templateName}::${instanceId}`;
  // Before the lock is awaited, so a build already holding it sees the bump.
  const generations = generationsOf(this);
  generations.set(entry, (generations.get(entry) ?? 0) + 1);
  await underEntryLock(this, entry, () =>
    doDestroy.call(this, templateName, instanceId, purgeCredentials)
  );
}

async function doDestroy(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  purgeCredentials: boolean
): Promise<void> {
  const entry = `${templateName}::${instanceId}`;
  const instances = this.templateClientMap.get(templateName);
  const names = instances?.get(instanceId);
  const clients = [...(names ?? [])].flatMap((name) => {
    const client = this.clients.get(name);
    return client ? [{ name, client }] : [];
  });
  for (const { client } of clients) retireCredentialWrites(client);
  for (const { name, client } of clients) {
    // Either way this client is going away with no replacement, so its parked
    // requests are failed with a reason instead of waiting out `cleanupTimeout`.
    await client.destroy(purgeCredentials ? "finalRemoval" : "removal");
    if (this.clients.get(name) === client) this.clients.delete(name);
  }

  const purge =
    purgeCredentials ||
    (clients.length > 0 &&
      !(await this.backend
        .smembers(`${this.namespace}:templates`)
        .then((entries) => entries.includes(entry))
        .catch(() => true)));
  if (purge) {
    for (const { client } of clients) await client.purgeCredentials();
  }

  // A replica that never built these clients has nothing to enumerate, but the
  // caller is about to delete the template blob and the registry entry — after
  // which no purge can ever find the credentials again, and they sit in the
  // backend for their full TTL. Deriving the keys from the name means a removal
  // handled by a read-only replica still deletes what the tenant asked us to.
  if (purgeCredentials && clients.length === 0) {
    await purgeCredentialsByName
      .bind(this)(templateName, instanceId)
      .catch((error: unknown) => {
        this.logger.error(
          { error },
          `Failed to purge credentials for ${entry} without a local client`
        );
      });
  }
  if (names && instances?.get(instanceId) === names) {
    instances.delete(instanceId);
  }
  this.localTemplateClients.delete(entry);
}

/** Where the client names for a template instance are recorded, for any replica to read. */
export function templateClientNamesKey(
  namespace: string,
  entry: string
): string {
  return `${namespace}:templateClients:${entry}`;
}

/**
 * Deletes credential keys for a template instance using the client names
 * recorded at registration.
 *
 * Needed because the names cannot be reconstructed from `templateName` and
 * `instanceId` alone — the organization segment sits between them — and the
 * replica handling a removal may never have built these clients. Without this
 * the credentials outlive the removal by their full TTL, unreachable, because
 * the registry entry that would have named them is deleted in the same call.
 */
async function purgeCredentialsByName(
  this: RequestHandler,
  templateName: string,
  instanceId: string
): Promise<void> {
  const entry = `${templateName}::${instanceId}`;
  const names = await this.backend.smembers(
    templateClientNamesKey(this.namespace, entry)
  );
  if (names.length === 0) {
    this.logger.warn(
      `No recorded client names for ${entry}; stored credentials may remain until their TTL expires`
    );
    return;
  }

  for (const name of names) {
    const authNamespace = `${this.namespace}:${name.replaceAll(/ /g, "_")}`;
    const keys = [`${authNamespace}:oauth2`];
    for (const grantId of await this.backend.smembers(
      `${authNamespace}:grants`
    )) {
      keys.push(`${authNamespace}:oauth2:${grantId}`);
    }
    keys.push(`${authNamespace}:grants`);
    for (const key of keys) {
      await this.backend.del(key);
    }
  }
}

export function encryptCredentials(credentials: unknown, key: string): string {
  return encrypt(JSON.stringify(credentials), key);
}

export function decryptCredentials(encrypted: string, key: string): unknown {
  return JSON.parse(decrypt(encrypted, key));
}
