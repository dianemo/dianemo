import type { AxiosError, AxiosResponse, CreateAxiosDefaults } from "axios";
import type { RequestConfig, RequestMetadata } from "../request/types.js";
import type { DianemoBackend } from "../backend/types.js";
import type { Logger } from "../logger.js";
import type EventEmitter from "events";

export type ClientRole = "controller" | "worker";

/**
 * Why a client is being torn down. Three cases rather than a boolean, because a
 * rebuild and a removal both answer "no" to "may I clear fleet-wide state?" yet want
 * opposite treatment for the requests parked here.
 *
 * - `rebuild` — a replacement for this name follows, and inherits the parked requests.
 * - `removal` — going away, credentials kept.
 * - `finalRemoval` — going away for good, so fleet-wide state may be cleared.
 */
export type ClientDestroyReason = "rebuild" | "removal" | "finalRemoval";

/**
 * Outcome of a non-blocking attempt to claim the budget a request needs.
 *
 * `waitTime` is how long until the attempt is worth repeating, so the admission
 * loop can schedule its own return instead of sleeping on the spot.
 */
export interface AcquireTurnResult {
  acquired: boolean;
  waitTime?: number;
  /** Set when no amount of waiting will help, so the caller fails the request. */
  error?: string;
}

export interface BaseCredentialsData {
  instanceId: string;
  baseUrl: string;
  isSandbox?: boolean;
  /**
   * Organization scope for multi-tenant deployments. `null` (or undefined)
   * means "no org" — internal-service rows and single-tenant integrations
   * resolve to a literal `_` segment in the resulting clientName so the
   * format `<template>:<orgId>:<alias>[:<sub>...]` always has the same
   * shape and can be parsed positionally.
   */
  organizationId?: string | null;
}

export interface OAuth2Credentials extends BaseCredentialsData {
  clientId: string;
  clientSecret: string;
}

export interface TokenCredentials extends BaseCredentialsData {
  token: string;
}

export interface BasicCredentials extends BaseCredentialsData {
  username: string;
  password: string;
}

/**
 * Recomputes a client's limits from a successful response — a vendor that
 * reports its remaining quota in headers, say.
 *
 * Called with the client's current limits, every name resolved. Return the
 * full replacement list, or `undefined` to leave the limits as they are. A
 * returned limit without a `name` keeps the name of the one current limit of
 * its type, so it keeps that budget's balance; with several of a type, name
 * them. Leave at most one unnamed: a list with two is refused, as it would be
 * at construction. A `sharedLimit` child's result is named against its owner's
 * limits, since those are what it replaces. The result is broadcast to every
 * replica, this one included, as a `dynamic` update. Receivers log and ignore
 * one that is unusable, and ignore one that would change the client class (to
 * or from a `sharedLimit`). A throw is logged and never fails the request.
 *
 * ```ts
 * const rateLimitChange: RateLimitChange = (current, response) => {
 *   const limit = Number(response.headers["x-ratelimit-limit"]);
 *   if (!Number.isFinite(limit) || limit <= 0) return undefined;
 *   const perSecond = { interval: 1000, tokensToAdd: limit, maxTokens: limit };
 *   return [{ type: "requestLimit", ...perSecond }];
 * };
 * ```
 */
export type RateLimitChange<
  R extends RateLimitData = RateLimitData,
  N extends RateLimitData = R,
> = (
  oldRateLimit: (R & { name: string })[],
  response: AxiosResponse
) =>
  | Promise<DeclaredRateLimit<N>[] | undefined>
  | DeclaredRateLimit<N>[]
  | undefined;

export interface ClientConstructorData {
  client: CreateClientData;
  backend: DianemoBackend;
  handlerNamespace: string;
  logger: Logger;
  key: string;
  emitter: EventEmitter;
  /** Owns every request this replica originates. */
  instanceId: string;
}

/**
 * A request an external scheduler fires to detect recovery from sustained downtime.
 * It goes through the normal pipeline — auth, rate limit, retries — so make it a
 * cheap read. Dianemo stores this and never fires it itself.
 */
export interface ProbeRequestConfig {
  /** The request to fire. `clientName` is filled in by the scheduler. */
  config: Omit<RequestConfig, "clientName">;
  /** Cadence between probe attempts while the integration is considered down. */
  intervalMs: number;
}

/**
 * Everything a client is built from: what a template builder returns, and what
 * `defaultClientOptions` takes.
 *
 * `R` narrows the limit types this client may declare, which in turn types the
 * `rateLimitChange` callback.
 */
export interface CreateClientData<R extends RateLimitData = RateLimitData> {
  /**
   * Unique on the handler. A template client's name must contain its
   * `instanceId` as a `:`-separated segment — `buildClientName()` produces
   * one — and a sub-client's is joined to its parent's with `:`.
   */
  name: string;
  /**
   * Which client's credential entry this one uses. Set internally when a
   * sub-client is merged onto its parent and declares no `authentication` of its
   * own, so it shares the parent's tokens; one that declares its own files them
   * under its own name.
   *
   * Not for callers to set — `generateClients` derives it.
   */
  authOwnerName?: string;
  /**
   * Every limit this client is metered against. Defaults to
   * `[{ type: "noLimit" }]`; a sub-client that omits it inherits its parent's.
   *
   * Always a list, even for one limit: a request is sent only when **all** of
   * them admit it, so a per-second and a per-day cap live side by side here.
   * `name` keys each limit's budget and may be omitted once per client, where it
   * is `"default"`. A `sharedLimit` must be the only entry. See
   * docs/rate-limits/multiple-limits.md.
   *
   * Building the client throws `ConfigurationError` for a list it cannot meter:
   * `unknown_rate_limit_type` for a bare object, `null`, or an unknown `type`;
   * `invalid_rate_limit` for an empty list, a duplicate name or an unusable
   * number; `invalid_rate_limit_name`; `shared_limit_not_alone`;
   * `shared_limit_parent_not_found` and `shared_limit_parent_is_shared` for a
   * `sharedLimit` owner that is missing or itself shared; and
   * `backend_missing_multi_limit` for several limits on a backend without the
   * multi-limit methods.
   */
  rateLimit?: DeclaredRateLimit<R>[];
  /**
   * Computes a dynamic budget update; see {@link RateLimitChange}. Shared
   * clients receive their shared-limit descriptor but update the concrete
   * budget owned by their parent.
   */
  rateLimitChange?: RateLimitChange<
    R,
    [RateLimitData] extends [R]
      ? RateLimitData
      : R extends SharedLimitClientOptions
        ? Exclude<RateLimitData, SharedLimitClientOptions>
        : R
  >;
  requestOptions?: RequestOptions;
  retryOptions?: Partial<RetryOptions>;
  /** Any HTTP status code included in this array will result in a debug log rather than an error log */
  httpStatusCodesToMute?: number[];
  /**
   * Reclaims orphaned requests, re-runs election and nudges the queue.
   *
   * **Default: 10000 ms (10 seconds)**
   */
  healthCheckIntervalMs?: number;
  /** Carried through untouched; dianemo never reads it. */
  metadata?: { [key: string]: unknown };
  axiosOptions?: CreateAxiosDefaults;
  authentication?: AuthCreateData;
  /**
   * Clients built from this one. A sub-client inherits every field of its
   * parent that it does not declare itself, `rateLimit` included; `metadata`,
   * `axiosOptions`, `requestOptions` (and its `defaults`) and `retryOptions`
   * merge key by key rather than being replaced whole. `subClients` is not
   * inherited. Its name is joined to its parent's with `:`, so `"orders"`
   * under `"acme"` registers as `"acme:orders"`.
   */
  subClients?: CreateClientData[];
  probeRequest?: ProbeRequestConfig;
}

export interface RateLimitUpdatedData {
  clientName: string;
  /**
   * Published with every name resolved. Typed as the declared shape because a
   * receiver also has to accept what an operator sends, and every
   * `handleRateLimitUpdated` resolves names again on receipt.
   */
  rateLimit: DeclaredRateLimit[];
  /**
   * `init` at construction, `operator` for an admin update, `dynamic` from a
   * `rateLimitChange` callback. A listener that persists changes filters on this.
   */
  source: "init" | "operator" | "dynamic";
  /**
   * Instance id of the request handler that published this event. Used by
   * persistence-side listeners so only one replica writes to the DB — every
   * replica receives the broadcast, so without this filter we'd take N writes
   * per dynamic event.
   */
  publisherInstanceId: string;
}

/** One limit, without a name: the four strategies a client can meter by. */
export type RateLimitData =
  | RequestLimitClientOptions
  | ConcurrencyLimitClientOptions
  | NoLimitClientOptions
  | SharedLimitClientOptions;

/**
 * One limit as a caller writes it.
 *
 * `name` keys the limit's budget — `<client namespace>:rateLimit:<name>` for a
 * `requestLimit`, `<client namespace>:concurrency:<name>` for a
 * `concurrencyLimit` — and is what makes reordering the list safe. It may be
 * omitted, where it is `"default"`, so a client with one limit never has to
 * invent one; a second unnamed limit is refused, since both would meter
 * against one bucket. In a `rateLimitChange` result or an override, a list's
 * one unnamed limit instead keeps the name of the one limit of its type it
 * replaces; a list with two unnamed limits is refused there too, every time.
 */
export type DeclaredRateLimit<R extends RateLimitData = RateLimitData> = R & {
  name?: string;
};

/**
 * One limit once its name has been resolved. This is the shape everything past
 * client construction works in — there is no second shape to branch on.
 *
 * Names must match `/^[a-z0-9_]{1,64}$/` and be unique within the client,
 * because they become backend key segments.
 */
export type NamedRateLimitData = RateLimitData & { name: string };

/**
 * Meters nothing. Requests still queue behind a freeze. See
 * docs/rate-limits/no-limit.md.
 */
export interface NoLimitClientOptions {
  type: "noLimit";
}

/**
 * A token bucket: up to `maxTokens` at once, refilled by `tokensToAdd` every
 * `interval` ms. See docs/rate-limits/request-limit.md.
 */
export interface RequestLimitClientOptions {
  type: "requestLimit";
  /** Milliseconds between refills. */
  interval: number;
  /** Tokens added per `interval`. */
  tokensToAdd: number;
  /** Bucket ceiling, and so the largest `cost` this client can ever admit. */
  maxTokens: number;
  /**
   * How far below zero settlement may take the balance when a response reports
   * more than the request reserved. A request is admitted only once the bucket
   * has refilled past its debt, so the client waits for the vendor's budget to
   * recover instead of meeting a 429. Settlement only — admission never spends
   * below zero. See `RequestConfig.settle`.
   *
   * **Default value: 0**, so an under-reservation beyond the balance goes
   * uncharged and is logged.
   */
  maxDebt?: number;
}

/**
 * A ceiling on the total cost in flight at once. See
 * docs/rate-limits/concurrency-limit.md.
 */
export interface ConcurrencyLimitClientOptions {
  type: "concurrencyLimit";
  /** Slots in flight at once, and so the largest admissible `cost`. */
  maxConcurrency: number;
}

/**
 * Spends another client's budgets, all of them, through that client's queue.
 * Must be the only entry in its client's list, and the owner must be
 * registered first and own its budgets rather than share another's. See
 * docs/rate-limits/shared-limit.md.
 */
export interface SharedLimitClientOptions {
  type: "sharedLimit";
  /** The name of the client to share a rate limit with */
  clientName: string;
}

/** What one declared limit reports, before its name is attached. */
export type RateLimitStats =
  | RequestLimitClientStats
  | ConcurrencyLimitClientOptions
  | NoLimitClientOptions
  | SharedLimitClientOptions;

/** How `getClientStats` reports a limit: one entry per declared limit. */
export type NamedRateLimitStats = RateLimitStats & { name: string };

/** A `requestLimit` with its current balance, as `getClientStats` reports it. */
export interface RequestLimitClientStats extends RequestLimitClientOptions {
  /** Tokens available now, refill included. */
  tokens: number;
}

export interface RequestOptions {
  /**
   * How long after admission a request's `settle` may still correct what it
   * claimed, in milliseconds. A token bucket's window is never shorter than the
   * time it takes to refill from empty, since until then the claim still shapes
   * its balance.
   *
   * Default: 900000 milliseconds (15 minutes), long enough for a non-streamed
   * response that takes ten minutes to generate.
   */
  settleWindow?: number;
  /**
   * How long a streamed response may stay open before it is closed and its
   * slot released, so a forgotten stream cannot hold a slot for ever. Long,
   * because a large generation streams for half an hour or more, and its slot
   * is kept alive meanwhile rather than reaped at `concurrencySlotTtl`.
   *
   * Default: 3600000 milliseconds (60 minutes)
   */
  streamTimeout?: number;
  /**
   * When cleaning up requests, how long until the request is counted as timed-out.
   *
   * Default: 60000 milliseconds (1 minute)
   */
  cleanupTimeout?: number;
  /**
   * Crash recovery only. Must exceed the slowest response the upstream can
   * legitimately produce, or a slot is reaped while its request is still in flight and
   * the effective cap becomes a multiple of `maxConcurrency`. A streamed response
   * counts only to its headers: its slot is refreshed while the stream is open.
   *
   * Default: 120000 milliseconds (2 minutes)
   */
  concurrencySlotTtl?: number;
  /** Metadata to carry with the request. It is up to the user to validate the metadata */
  metadata?: Record<string, unknown>;
  /**
   * Default values to set for each request
   */
  defaults?: RequestDefaults;
  /** Runs before any global request interceptor. */
  requestInterceptor?: RequestInterceptor;
  /** Runs before any global response interceptor. */
  responseInterceptor?: ResponseInterceptor;
}

export interface RetryOptions {
  /** Default: 3. */
  maxRetries: number;
  /**
   * The base wait in ms before any retry, scaled by `retryBackoffMethod`: the
   * attempt number squared for `"exponential"`, the attempt number for
   * `"linear"`. After a 429, the base is floored at the shortest `interval`
   * among the client's `requestLimit`s, if it has any.
   *
   * Also the base of the client-wide freeze a 429, 5xx or dropped connection
   * arms, scaled the same way. Only a 429's freeze is floored at the shortest
   * refill interval — the budget owner's, for a `sharedLimit` client, whose
   * own retries are not floored — and a zero, negative or `NaN` value bases
   * the freeze on 1000 ms instead.
   *
   * **Default value: 1000 ms**
   */
  retryBackoffBaseTime: number;
  /** Default: `"exponential"`. */
  retryBackoffMethod: BackoffType;
  /** Default: true. Setting it false also disarms the fleet-wide freeze. */
  retry429s: boolean;
  /** Default: true. */
  retry5xxs: boolean;
  retryHandler?: RetryHandler;
  /**
   * Classifies a failure before the built-in rules do, deciding whether it is
   * retried and whether it freezes the fleet. Return `undefined` to leave a
   * failure to the built-in rules. Runs for every failed attempt, the last one
   * included, since a freeze is about the upstream rather than this request.
   *
   * A throw is logged and the built-in rules decide. See
   * docs/rate-limits/README.md#classifying-failures.
   */
  classifyError?: ErrorClassifier;
  /** Retried without freezing the fleet, unlike a 429 or 5xx. */
  retryStatusCodes: number[];
  /**
   * How many thaw probes must succeed, after a 429's freeze has lapsed, before
   * the client sends at full rate again. A probe succeeds on any response that
   * would not itself freeze the client — a 404 included — and fails on a 429,
   * a 5xx, a dropped connection, or a failure `classifyError` freezes on.
   *
   * **Default value: 3**
   */
  thawRequestCount: number;
}

export interface RequestDefaults {
  headers?: Record<string, string>;
  baseURL?: string;
  params?: Record<string, string>;
}

export type BackoffType = "exponential" | "linear";

/**
 * Decides whether a failed request is retried, for statuses dianemo does not
 * classify itself. A retry granted here does not freeze the fleet.
 */
export type RetryHandler = (error: unknown) => Promise<boolean> | boolean;

/** What `classifyError` decides about a failed attempt. */
export type ErrorDecision =
  /** Terminal: the caller gets the error now, and nothing is frozen. */
  | { action: "fail" }
  /** Retry this request, within `maxRetries`, without freezing the fleet. */
  | { action: "retry"; freeze?: false }
  /**
   * Retry this request and freeze the fleet. `freezeMs` is a floor on the
   * freeze and the retry wait, like `retry-after`: the longer wins.
   */
  | { action: "retry"; freeze: true; freezeMs?: number };

export type ErrorClassifier = (
  error: AxiosError
) => ErrorDecision | undefined | Promise<ErrorDecision | undefined>;

/**
 * Last chance to change a request before it is sent. **Must return a config
 * object** — a void return drops the request's configuration.
 */
export type RequestInterceptor = (
  config: RequestConfig
) => Promise<RequestConfig> | RequestConfig;

/** Last chance to reshape a response before the caller sees it. */
export type ResponseInterceptor = (
  config: RequestConfig,
  response: AxiosResponse
) => Promise<void> | void;

/** What `RequestHandler.getClientStats` returns. */
export interface ClientStatistics {
  clientName: string;
  /** Inside the client-level freeze window, on the backend's clock. */
  isFrozen: boolean;
  /** Past the freeze window with thaw probes still owed. */
  isThawing: boolean;
  thawRequestCount: number;
  /**
   * One entry per declared limit, in declaration order, each with its resolved
   * `name`; a `requestLimit` carries its current `tokens`. Client-level only:
   * an isolated grant's balance is not reported here.
   */
  rateLimit: NamedRateLimitStats[];
  requestsInQueue: ClientRequestsStatistics;
  requestsInProgress: ClientRequestsStatistics;
}

export interface ClientRequestsStatistics {
  count: number;
  /**
   * Total cost against each limit the client meters, by limit name. A number
   * cost counts against every one of them; a client that meters nothing has no
   * entries.
   */
  cost: Record<string, number>;
  requests: RequestMetadata[];
}

export interface ClientTokensUpdatedData {
  clientId: string;
  clientName: string;
  tokens: number;
}

export type AuthCreateData = AuthDataOAuth2 | AuthDataToken | AuthDataBasic;

/**
 * Determines how grant children handle rate limiting:
 * - "shared" (default): All grants share the parent's rate limit bucket
 * - "isolated": Each grant gets its own rate limit bucket using parent's rate limit rules
 */
export type GrantRateLimitBehavior = "shared" | "isolated";

export interface AuthDataOAuth2 extends CustomHeader {
  type: "oauth2";
  clientId: string;
  clientSecret: string;
  grantRefreshConfig?: OAuthRefreshConfig;
  refreshConfig: OAuthRefreshConfig;
  metadata?: { [key: string]: unknown };
  grantRateLimitBehavior?: GrantRateLimitBehavior;
}

export interface OAuthRefreshConfig {
  url: string;
  /**
   * Where to put the authentication data in the request:
   *
   * - `jsonBody`: The data will be sent in the body of the request as JSON
   * - `urlQuery`: The data will be sent in the URL as query parameters
   * - `urlEncodedForm`: The data will be sent in the body of the request as a URL encoded form data
   */
  dataLocation: "jsonBody" | "urlQuery" | "urlEncodedForm";
  /**
   * The data to send in the request
   *
   * Some values are available via keywords so you don't have to hardcode them
   * - `{{clientId}}`: The clientId from the authentication data
   * - `{{clientSecret}}`: The clientSecret from the authentication data
   * - `{{refreshToken}}`: The refreshToken from the authentication data
   */
  data: Record<string, string>;
  /**
   * Any custom headers to include in the request
   *
   * NOTE: If using Basic Auth, use the useBasicAuth property instead
   */
  customHeaders?: { [key: string]: string };
  /** Sends `clientId:clientSecret`, base64-encoded, as an `Authorization: Basic` header instead of in `data`. */
  useBasicAuth?: boolean;
  /**
   * Reshapes the refresh request for a provider needing per-call data (a nonce, a
   * signature). The result stays local: it is never written back onto shared
   * `authData`, so a non-idempotent interceptor cannot accumulate across refreshes.
   */
  requestInterceptor?: (
    config: OAuthRefreshConfig
  ) => Promise<OAuthRefreshConfig> | OAuthRefreshConfig;
  /** Normalises a non-standard token response into the OAuth2 shape dianemo expects. */
  responseInterceptor?: (
    res: AxiosResponse
  ) =>
    | Promise<OAuthResponse | OAuthGrantTypeResponse>
    | OAuthResponse
    | OAuthGrantTypeResponse;
}

export interface AuthDataToken extends CustomHeader {
  type: "token";
  token: string;
  encodeBase64?: boolean;
}

export interface AuthDataBasic extends CustomHeader {
  type: "basic";
  username: string;
  password: string;
}

export interface OAuthResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
  refresh_token?: string;
  refresh_token_expires_in?: number;
}

export interface OAuthGrantTypeResponse extends OAuthResponse {
  refresh_token: string;
}

/** Seeds a grant's OAuth2 tokens from a store dianemo did not obtain them from. */
export interface SetGrantTokensData {
  accessToken: string;
  /** When the access token expires (Unix timestamp in milliseconds) */
  expiresAt: number;
  /** Default: `"Bearer"`. */
  tokenType?: string;
  refreshToken: string;
  /** When the refresh token expires (Unix timestamp in milliseconds) */
  refreshTokenExpiresAt: number;
}

export interface CustomHeader {
  /** Sends the token here instead of in `Authorization`. */
  customHeaderName?: string;
  /** Scheme in front of the token. Default: `"Basic"` for basic auth, else `"Bearer"`. */
  customPrefix?: string;
  /** Sends the bare token with no scheme prefix. Default: false. */
  excludePrefix?: boolean;
}
