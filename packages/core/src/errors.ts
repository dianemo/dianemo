/**
 * Base class for the errors the handler raises about its own state and
 * configuration. Carries `statusCode` and `code` so a host can map errors
 * structurally without importing these types — duck-type on those two fields
 * when the handler and the catch site may resolve to different copies of the
 * package. See docs/concepts.md#errors.
 *
 * Not everything that reaches a caller extends it: {@link RequestError} (from
 * `tryHandleRequest`) does not, an upstream failure arrives as the axios error
 * itself, and the backend helpers and backends throw a plain `RangeError` for
 * an unusable TTL, score or lock argument.
 */
export class RequestHandlerError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** A request named a client that is not registered on this instance. */
export class ClientNotFoundError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      404,
      "client_not_found",
      `Client with name ${clientName} does not exist.`
    );
  }
}

/**
 * A client with this name is already registered.
 *
 * Effectively unreachable: building a client whose name is taken replaces the
 * registered one rather than conflicting with it. Kept because it is exported.
 */
export class ClientConflictError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      409,
      "client_conflict",
      `Client with name ${clientName} already exists.`
    );
  }
}

/**
 * The handler was constructed or driven with unusable configuration — a
 * template name containing `:`, a duplicate plugin registration, a missing
 * backend. `code` names which.
 *
 * Mostly a programming error caught on arrival, but not only. Some codes
 * arise at runtime from what a vendor or shared state hands back:
 * `invalid_token_response` for a token response with no usable
 * `access_token`, `unusable_rate_limit` when the backend cannot claim from a
 * client's budgets, and `invalid_request_cost` for an unspendable cost read
 * back out of the shared queue.
 */
export class ConfigurationError extends RequestHandlerError {
  constructor(code: string, message: string) {
    super(500, code, message);
  }
}

/** A queued request was never admitted by the controller before its deadline. */
export class RequestTimeoutError extends RequestHandlerError {
  constructor(requestId: string, timeoutMs: number) {
    super(
      408,
      "request_timeout",
      `Request ${requestId} timed out waiting for controller after ${timeoutMs}ms`
    );
  }
}

/**
 * A streamed response stayed open past the client's `streamTimeout`, so it was
 * closed and its slot released. The caller's stream is destroyed with this.
 */
export class StreamTimeoutError extends RequestHandlerError {
  constructor(requestId: string, timeoutMs: number) {
    super(
      504,
      "stream_timeout",
      `Streamed response for request ${requestId} was still open after ${timeoutMs}ms`
    );
  }
}

/**
 * The handler stopped while a streamed response was still open, so the stream
 * was closed. The caller's stream is destroyed with this.
 */
export class StreamClosedError extends RequestHandlerError {
  constructor(requestId: string) {
    super(
      503,
      "stream_closed_on_shutdown",
      `Streamed response for request ${requestId} was closed because the handler stopped`
    );
  }
}

/**
 * The retry loop ended without a response or an error to rethrow.
 *
 * When retries run out normally the last axios error is rethrown instead, so
 * this is reachable only with an unusable `retryOptions.maxRetries`, such as
 * `NaN`, that ends the loop before any attempt is judged final.
 */
export class NoResponseError extends RequestHandlerError {
  constructor() {
    super(500, "no_response", "No response received after maximum retries.");
  }
}

/**
 * A client did not become available in time, or the wait was cancelled by
 * shutdown or removal. Distinct from `ClientNotFoundError`: this is a transient
 * availability problem, not a misconfigured name. `code` is one of:
 *
 * - `client_unavailable` — `awaitClient` timed out.
 * - `shutdown_aborted` — the `signal` passed to `awaitClient` aborted.
 * - `handler_stopping` — a request or template mutation arrived during
 *   `stop()`.
 * - `handler_stopped` — a queued request was failed because the handler
 *   stopped or its client was removed.
 * - `client_removed` — grant tokens were stored for a client that was removed
 *   before or during the write; nothing is left stored.
 * - `shared_limit_parent_missing` — a `sharedLimit` client's budget owner is
 *   no longer registered.
 */
export class ClientUnavailableError extends RequestHandlerError {
  constructor(code: string, message: string) {
    super(503, code, message);
  }
}

/** Grant-token operations were attempted on a client that is not OAuth2. */
export class NotOAuth2ClientError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      400,
      "client_not_oauth2",
      `Client ${clientName} does not support OAuth2 grant tokens.`
    );
  }
}

/** An OAuth2 grant was asked to refresh but has no stored refresh token. */
export class GrantRefreshTokenMissingError extends RequestHandlerError {
  constructor(grantId: string) {
    super(
      400,
      "grant_refresh_token_missing",
      `No refresh token stored for grant "${grantId}"`
    );
  }
}

/**
 * A request declared a cost larger than a limit's ceiling — its `maxTokens` or
 * `maxConcurrency` — so no amount of waiting could ever admit it.
 */
export class RequestCostExceedsBudgetError extends RequestHandlerError {
  constructor(
    clientName: string,
    cost: number,
    ceiling: number,
    /** The limit whose ceiling was exceeded, when the client declares several. */
    limitName?: string
  ) {
    const limit =
      limitName === undefined
        ? "its smallest maxTokens or maxConcurrency"
        : `the maxTokens or maxConcurrency of rate limit "${limitName}"`;
    super(
      400,
      "request_cost_exceeds_budget",
      `Request cost (${cost}) exceeds the maximum client ${clientName} can ever admit (${ceiling}), ${limit}`
    );
  }
}

/**
 * A request's per-limit cost does not name exactly the limits the client
 * declares, so some budget would go unmetered or some amount would meter nothing.
 *
 * Raised where the request is made, and again when it is admitted: the client's
 * limits can be renamed while the request waits in the queue.
 */
export class RequestCostMismatchError extends RequestHandlerError {
  constructor(
    clientName: string,
    missing: readonly string[],
    unknown: readonly string[],
    declared: readonly string[]
  ) {
    const problems = [
      missing.length ? `omits ${quoteList(missing)}` : "",
      unknown.length
        ? `names ${quoteList(unknown)}, which it does not declare`
        : "",
    ].filter(Boolean);
    super(
      400,
      "request_cost_mismatch",
      `Request cost for client ${clientName} ${problems.join(" and ")}. A per-limit cost must name every limit the client meters: ${
        declared.length
          ? quoteList(declared)
          : "it meters none, so pass a number"
      }.`
    );
  }
}

function quoteList(names: readonly string[]): string {
  return names.map((name) => `"${name}"`).join(", ");
}

/**
 * The caller's `AbortSignal` fired before the request was dispatched: while it
 * was queued, or after admission while the `requestInterceptor` or a token
 * refresh ran. Whatever it had claimed is handed back.
 *
 * Raised instead of letting the request wait for capacity it will never use,
 * or spend a token or a concurrency slot on a request axios would then refuse.
 * An abort once the request is upstream reaches axios and rejects with its
 * own cancellation error, except while a streamed response is being read: the
 * caller's stream is then destroyed with this, `whileStreaming` set.
 */
export class RequestAbortedError extends RequestHandlerError {
  constructor(requestId: string, whileStreaming = false) {
    super(
      499,
      "request_aborted",
      whileStreaming
        ? `Request ${requestId} was aborted while its response streamed`
        : `Request ${requestId} was aborted before it was sent`
    );
  }
}

/**
 * Internal control-flow signal: the client froze while this request waited for
 * its turn. Never reaches the caller — `processRequests` catches it and returns
 * the request to the queue.
 */
export class ClientFrozenError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      503,
      "client_frozen",
      `Client ${clientName} froze while the request was waiting for its turn.`
    );
  }
}

/**
 * What a `RequestError` carries beside its code: the caller's own fields,
 * `client`, `context`, and `originalError` — the underlying failure's message,
 * code, status and redacted response body.
 */
export interface RequestErrorMetadata {
  context?: string;
  errors?: unknown[];
  [key: string]: unknown;
}

/**
 * A failed outbound call, wrapped with a stable error code and the original
 * error's salient fields under `metadata.originalError`.
 *
 * Thrown by `tryHandleRequest`, which is also what logs it — constructing one
 * has no side effects, so it is safe to build in a test or rethrow without
 * emitting a duplicate log line.
 */
export class RequestError extends Error {
  public readonly errorId: string;
  public readonly code: string;
  public readonly statusCode: number;
  public readonly metadata?: RequestErrorMetadata;

  constructor(
    code: string,
    message: string,
    options?: {
      statusCode?: number;
      metadata?: RequestErrorMetadata;
      cause?: Error;
      /** Injectable for deterministic tests; defaults to a random UUID. */
      errorId?: string;
    }
  ) {
    super(message);
    this.name = "RequestError";
    this.code = code;
    this.errorId = options?.errorId ?? crypto.randomUUID();
    this.statusCode = options?.statusCode ?? 500;
    this.metadata = options?.metadata;
    if (options?.cause) {
      // Non-enumerable, as `new Error(msg, { cause })` produces. Plain
      // assignment would put the cause in `JSON.stringify(err)`, and for an
      // Axios cause that reaches `toJSON()` and its headers. Still readable as
      // `err.cause`.
      Object.defineProperty(this, "cause", {
        value: options.cause,
        writable: true,
        enumerable: false,
        configurable: true,
      });
    }
  }
}

/**
 * The upstream status behind a `RequestError`, for a caller mapping one to a
 * domain result (404 → null). Undefined for a network-level failure, which has none.
 */
export function getOriginalStatus(err: unknown): number | undefined {
  if (!(err instanceof RequestError)) return undefined;
  const original = err.metadata?.originalError;
  if (!original || typeof original !== "object") return undefined;
  const status = (original as { status?: unknown }).status;
  return typeof status === "number" ? status : undefined;
}

/**
 * Extract the response body of the underlying Axios error attached to a
 * `RequestError`. Returns `undefined` when no response was captured.
 *
 * Useful for callers that need to read structured error payloads — e.g.,
 * a 410 Gone response carrying a `mergedIntoId` pointer.
 */
export function getOriginalResponseData(err: unknown): unknown {
  if (!(err instanceof RequestError)) return undefined;
  const original = err.metadata?.originalError;
  if (!original || typeof original !== "object") return undefined;
  return (original as { data?: unknown }).data;
}
