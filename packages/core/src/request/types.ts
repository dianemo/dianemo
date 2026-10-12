import type { AxiosRequestConfig, AxiosResponse, Method } from "axios";

export type RequestStatus = "inQueue" | "inProgress";

/**
 * What a request spends: one amount charged to every limit the client declares,
 * or an amount per limit, keyed by limit name.
 *
 * The keyed form must name every `requestLimit` and `concurrencyLimit` the client
 * declares and nothing else. An entry may be 0 — an embeddings call that produces
 * no output tokens — but at least one must be positive. A `sharedLimit` client
 * names its owner's limits, since those are the budgets it spends.
 */
export type RequestCost = number | Readonly<Record<string, number>>;

/** What one attempt came back with, as `settle` receives it. */
export type AttemptOutcome =
  | { response: AxiosResponse; error?: undefined }
  | { response?: AxiosResponse; error: unknown };

/**
 * Reports what an attempt actually cost, once its response is in. Returns the
 * same shape as `cost`, or `undefined` to let the claim stand — after a network
 * error, say, where the vendor's charge is unknown.
 *
 * A keyed result must name every `requestLimit` the client declares and no
 * `concurrencyLimit`: a slot is returned at completion whatever the response
 * said. Entries may be 0.
 */
export type SettleFunction = (
  outcome: AttemptOutcome
) => RequestCost | undefined | Promise<RequestCost | undefined>;

/**
 * Watches a streamed response pass through on its way to the caller, to read
 * the usage a vendor reports inside the stream — Anthropic in
 * `message_start` and `message_delta`, for instance. One is made per attempt.
 * See docs/rate-limits/streaming.md.
 */
export interface StreamObserver {
  /** Sees each chunk as the caller will, without consuming it. Must not throw. */
  onChunk?(chunk: Buffer | string): void;
  /**
   * Called once, however the stream ends — so a stream cut short settles at
   * what was generated. Returns the cost to settle at, `undefined` to leave it
   * to `settle` or the response's `dianemo.settle`, or `{ error }` for an error
   * the vendor reported mid-stream, which freezes the client like a 5xx.
   */
  result?(): StreamResult | undefined;
}

/**
 * What a stream observer reports. An object carrying a string `error` is an
 * error report, optionally with what the stream cost before it; anything else
 * is a cost.
 */
export type StreamResult = RequestCost | StreamError;

export interface StreamError {
  /** The vendor's error, such as `"overloaded_error"`, for the logs. */
  error: string;
  cost?: RequestCost;
}

/**
 * Reads a stream observer's result as the handler does: the error form when it
 * carries a string `error`, otherwise `undefined`, for a cost. For an observer
 * that wraps another and must tell the two apart, or a test of an observer.
 * See docs/rate-limits/streaming.md.
 */
export function streamErrorOf(result: StreamResult): StreamError | undefined {
  if (typeof result !== "object" || result === null) return undefined;
  const error = (result as { error?: unknown }).error;
  return typeof error === "string" ? (result as StreamError) : undefined;
}

/** How a streamed response came to an end. */
export type StreamEndReason =
  /** The upstream finished sending. */
  | "completed"
  /** The caller destroyed the stream, or its `signal` aborted. */
  | "closed"
  /** The upstream connection failed mid-stream. */
  | "error"
  /** `streamTimeout` elapsed. */
  | "timeout"
  /** The handler stopped with the stream still open. */
  | "shutdown";

/**
 * Attached to a streamed response as `response.dianemo`.
 */
export interface StreamControls {
  /**
   * Settles the attempt at what the caller read from the stream. The first
   * settlement wins, whether this or an observer's; later ones are no-ops.
   * Resolves once applied, and never rejects.
   */
  settle(cost: RequestCost): Promise<void>;
  /** Resolves when the stream has ended and its slot has been released. */
  finished: Promise<StreamEndReason>;
}

/** A response as `handleRequest` returns it. */
export type DianemoResponse<T = unknown, D = unknown> = AxiosResponse<T, D> & {
  /** Present when the request set `responseType: "stream"`. */
  dianemo?: StreamControls;
};

export interface RequestConfig<
  TRequestData = unknown,
> extends AxiosRequestConfig<TRequestData> {
  clientName: "default" | string;
  method: Method;
  /** Logical name for this request used for metrics labeling (e.g. "netSuite.records.salesOrder.get"). */
  requestName: string;
  /** Selects which grant's credentials and budget this request uses. */
  grantId?: string;
  /**
   * Integer 0-10, higher served first. Strict, not aging-based, so a continuous
   * supply of higher-priority work can starve lower.
   *
   * **Default value: 1**
   */
  priority?: number;
  metadata?: Record<string, unknown>;
  /**
   * Budget this request spends, for vendors that do not price every endpoint the
   * same — or, keyed by limit name, a different amount against each limit, as for
   * an API that meters requests and tokens separately. Fractional costs spend
   * fractionally, and cost decides what a request spends rather than where it
   * sits in the queue. See {@link RequestCost}.
   *
   * **Default value: 1**
   */
  cost?: RequestCost;
  /**
   * Corrects what this request spent to what its response reports — for a
   * vendor that meters usage known only afterwards, such as the tokens an AI
   * response generated. Runs once per attempt, on the replica that sent it. See
   * {@link SettleFunction} and docs/rate-limits/settlement.md.
   *
   * @throws {ConfigurationError} `backend_missing_settlement` when the backend
   * does not implement `settleTokens`, before anything is spent.
   */
  settle?: SettleFunction;
  /**
   * For a request with `responseType: "stream"`: makes the observer that reads
   * usage from the stream as it passes, and settles at what it reports when the
   * stream ends. Usually declared by a plugin's request definition, where the
   * vendor's event format is known. See docs/rate-limits/streaming.md.
   *
   * @throws {ConfigurationError} `backend_missing_settlement` when the backend
   * does not implement `settleTokens`, before anything is spent.
   */
  settleStream?: () => StreamObserver;
  /**
   * Set by the handler before any `requestInterceptor` runs, and unique per
   * `handleRequest` call: every retry of that call carries the same id. A value
   * the caller sets is replaced. An interceptor needing a correlation id should
   * read this rather than mint one.
   */
  requestId?: string;
  /**
   * Cancels the request at every stage. Aborted before dispatch — while queued,
   * or after admission while the `requestInterceptor` or a token refresh runs —
   * it rejects with `RequestAbortedError` and whatever it claimed is handed
   * back. Once the request is upstream the abort reaches axios, which cancels
   * the call. See docs/concepts.md#cancellation.
   *
   * Narrower than axios's `GenericAbortSignal`, which makes `addEventListener`
   * optional — its Node adapter calls it unguarded, so the looser shape throws from
   * inside axios after this client has already spent on the request.
   */
  signal?: AbortSignal;
}

export interface RequestMetadata {
  requestId: string;
  status: RequestStatus;
  priority: number;
  timestamp: number;
  clientName: string;
  cost: RequestCost;
  /**
   * `cost` as the call site actually set it, undefined when it did not — which is how
   * the metrics path tells a cost-budgeted endpoint from a count-budgeted one. The
   * rate limiter uses the post-default `cost` above.
   */
  explicitCost?: RequestCost;
  requestName: string;
  retries: number;
  grantId?: string;
  /** The grant's single nominated recovery probe. */
  isThawRequest: boolean;
  /** Admitted without queueing; no queue entry exists for it. */
  fastPath?: boolean;
  /**
   * Declares `settle` or `settleStream`, or streams its response, so its claim
   * leaves a mark. See `QueuedRequest.settles`.
   */
  settles?: boolean;
  /**
   * The budget keys this attempt claimed from, set by whoever claimed it.
   * Release and refund act on these rather than on the keys the client's
   * current limits name, which differ once an override or `rateLimitChange`
   * has renamed or removed a limit while the request ran. Absent from a replica
   * predating the field, where both fall back to the current limits.
   */
  claimedKeys?: string[];
  /** Whoever originated it. Orphan cleanup spares an entry whose owner is alive. */
  ownerId: string;
}

export interface RequestRetryData {
  retry: boolean;
  message: string;
  isRateLimited: boolean;
  waitTime: number;
  /**
   * Whether this failure warrants freezing the whole client rather than backing
   * off just this request. True for 429s and upstream failures, which apply to
   * everyone; false for a retry the consumer asked for by status or handler.
   */
  freezeClient: boolean;
  /**
   * How long the client-wide freeze should last, when `freezeClient` is set. Separate
   * from `waitTime`, which paces only this request: the two can legitimately disagree.
   * See docs/design-notes.md#freeze-duration-and-retry-wait-are-separate-numbers.
   */
  freezeTime?: number;
}

export interface RequestDoneData extends RequestMetadata {
  responseStatus: "success" | "failure" | "retry";
  /**
   * For a streamed response, which of its two announcements this is: at the
   * headers, which settles the queue entry and the thaw probe, or at the end of
   * the stream, which releases the slot and may freeze. Absent for anything
   * else, which announces once. See docs/rate-limits/streaming.md.
   */
  phase?: "headers" | "end";
  waitTime: number;
  /** How long the client-wide freeze should last. See `RequestRetryData`. */
  freezeTime?: number;
  isRateLimited: boolean;
  /** Whether the controller should freeze the client. See `RequestRetryData`. */
  freezeClient: boolean;
  /** Wall-clock time when the request lifecycle terminated, set by getRequestDoneData. */
  completedAt: number;
  /** HTTP status code observed (response or error.response). Absent for network errors. */
  httpStatus?: number;
}
