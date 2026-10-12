import { type Span, trace, SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { clampDelay, retryAfterMs } from "../../utils/retryAfter.js";
import { PassThrough, type Readable, Transform } from "node:stream";
import type { AxiosError, AxiosResponse } from "axios";
import { streamErrorOf } from "../../request/types.js";
import type * as ClientTypes from "../types.js";
import authenticate from "./authenticate.js";
import Request from "../../request/index.js";
import type BaseClient from "../index.js";
import type {
  AttemptOutcome,
  DianemoResponse,
  RequestConfig,
  RequestCost,
  RequestDoneData,
  RequestMetadata,
  RequestRetryData,
  StreamEndReason,
  StreamObserver,
  StreamResult,
} from "../../request/types.js";
import {
  isReadableStream,
  redactBody,
  redactConfig,
  redactHeaders,
  redactUrl,
  sanitizeError,
} from "../../utils/redact.js";
import {
  ClientUnavailableError,
  ConfigurationError,
  NoResponseError,
  RequestAbortedError,
  RequestTimeoutError,
  StreamClosedError,
  StreamTimeoutError,
} from "../../errors.js";

const tracer = trace.getTracer("@dianemo/core");

/**
 * How long to spend announcing an abandonment. Short, because the caller is already
 * being failed — but still awaited, so a caller waits its own budget plus this one.
 * That is deliberate: the announcement releases the queue entry and any slot, so
 * doing it first keeps a retry from racing its predecessor's cleanup.
 */
const ABANDONMENT_PUBLISH_TIMEOUT_MS = 2000;

async function handleRequest(this: BaseClient, config: RequestConfig) {
  return tracer.startActiveSpan(
    `dianemo ${config.requestName}`,
    {
      kind: SpanKind.CLIENT,
      attributes: {
        "dianemo.client_name": this.name,
        "dianemo.request_name": config.requestName,
        "dianemo.priority": config.priority ?? 1,
        ...costAttributes(config.cost ?? 1),
        ...(config.grantId ? { "dianemo.grant_id": config.grantId } : {}),
      },
    },
    async (span) => {
      await this.assertReadyToQueue();
      // Constructed before that test, because its constructor validates the shape
      // of `cost`, `priority` and `grantId`. Testing the budget first makes the
      // error a caller sees depend on whether their malformed value also exceeded
      // it, reporting `cost: Infinity` as "budget too small".
      const request = new Request(
        this.name,
        config,
        this.instanceId,
        this.requestOptions
      );
      // A cost no wait could admit — over a ceiling, or naming the wrong
      // limits — is refused to the caller rather than occupying the queue
      // until it times out on whichever replica is the controller.
      const costProblem = this.getCostProblem(config.cost ?? 1);
      if (costProblem) throw costProblem;
      // Refused here rather than discovered after the response, when the claim
      // could no longer be corrected and the caller would never hear why.
      if (
        (config.settle || config.settleStream) &&
        !this.backend.settleTokens
      ) {
        throw new ConfigurationError(
          "backend_missing_settlement",
          `Client ${this.name} was given a request that settles, which needs a backend implementing settleTokens.`
        );
      }
      span.setAttribute("dianemo.request_id", request.id);
      this.logger.debug(`Request ID: ${request.id} | Waiting...`);
      // Counted for the shutdown drain, which otherwise cannot see a fast-path
      // request — it has no queue entry, but it may hold a concurrency slot.
      this.trackRequestStarted();
      // A streamed response hands its bookkeeping to the stream's end, which
      // outlives this function.
      let handedToStream = false;
      let backoffMs = 0;
      try {
        do {
          request.setStatus("inQueue");
          // Everything in here runs after admission has already claimed a slot
          // or spent tokens, so a throw has to hand those back explicitly.
          try {
            if (backoffMs > 0) {
              await waitOutBackoff.bind(this)(request, backoffMs);
            }
            await handlePreRequest.bind(this)(request);
            // The interceptor and a token refresh run after admission and can
            // outlive the shutdown drain. Without these, an abort landing there
            // reaches axios as its own `CanceledError` with the claim still
            // spent, and a stop lets the request go out after `stop()` returned.
            if (request.config.signal?.aborted) {
              throw new RequestAbortedError(request.id);
            }
            if (this.admissionHalted) throw admissionHaltedError(this);
          } catch (error) {
            await releaseAbandonedRequest.bind(this)(request, error);
            throw error;
          }
          let res;
          const sentAt = Date.now();
          try {
            res = await this.http.request(request.config);
          } catch (error: unknown) {
            // handleError throws when retry is exhausted; otherwise returns
            // retryData and we loop. The catch below records it on the span.
            const axiosErr = error as AxiosError;
            const retryData = await handleError.bind(this)(request, axiosErr);
            const status = axiosErr.response?.status;
            if (retryData.isRateLimited) {
              span.addEvent("rate_limited", {
                "dianemo.wait_ms": retryData.waitTime,
                ...(status ? { "http.response.status_code": status } : {}),
              });
            }
            // Record per-attempt exception details so deep retry chains are
            // debuggable in a trace viewer without paging through logs.
            span.addEvent("retry", {
              "dianemo.attempt": request.retries,
              "exception.type": axiosErr.name || axiosErr.code || "Error",
              "exception.message": axiosErr.message,
              ...(status ? { "http.response.status_code": status } : {}),
            });
            // Always wait, even with a client-wide freeze on its way: that freeze
            // is applied by the controller after a pub/sub round trip while this
            // loop re-enters admission immediately, so deferring to it sends the
            // request the vendor just rejected straight back out. The freeze is the
            // fleet-wide gate; this is the local one.
            backoffMs = retryData.waitTime;
            continue;
          }
          if (isStreamed(request, res)) {
            handedToStream = true;
            return await handleStreamedResponse.bind(this)(
              request,
              res,
              span,
              sentAt
            );
          }
          return await handleResponse.bind(this)(request, res);
        } while (request.retries <= this.retryOptions.maxRetries);
        throw new NoResponseError();
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        span.recordException(e);
        span.setStatus({ code: SpanStatusCode.ERROR, message: e.message });
        const axiosStatus = (err as AxiosError)?.response?.status;
        if (axiosStatus)
          span.setAttribute("http.response.status_code", axiosStatus);
        throw err;
      } finally {
        if (!handedToStream) {
          this.trackRequestFinished();
          span.setAttribute("dianemo.retries", request.retries);
          span.end();
        }
      }
    }
  );
}

/** Hands back everything admission granted, for a request that will not run. */
async function releaseAbandonedRequest(
  this: BaseClient,
  request: Request,
  cause: unknown
) {
  // The admission timeout and the abort path already announced this one. Both
  // reject, and the rejection lands in the catch that calls this — so without
  // the guard every such request emits `requestDone` twice on a public channel.
  if (request.abandonmentPublished) return;
  request.abandonmentPublished = true;
  const metadata = request.getMetadata();
  // `inProgress` is the record that admission completed, and only then is there
  // anything to give back — refunding for a request that never reached admission
  // would mint budget. The announcement below covers a concurrency slot but not a
  // token bucket, whose `handleOwnTypeRequestDone` has nothing to release.
  if (metadata.status === "inProgress") {
    await this.releaseUnusedAdmission(metadata).catch((error) => {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | failed to return admission after a pre-request failure`
      );
    });
  }
  const message = cause instanceof Error ? cause.message : String(cause);
  await publishAbandoned.bind(this)(
    request,
    `Request abandoned before dispatch: ${message}`
  );
}

/**
 * Announces that a request will not run, so the controller releases whatever
 * admission granted it.
 *
 * The controller owns the queue entry and the concurrency slot and releases both
 * only in response to this event — so abandoning a request means announcing it,
 * not just dropping it.
 */
async function publishAbandoned(
  this: BaseClient,
  request: Request,
  message: string
) {
  const data = request.getRequestDoneData({
    retry: false,
    message,
    isRateLimited: false,
    waitTime: 0,
    freezeClient: false,
  });
  // Cleanup is local and retryable; pub/sub below is notification, not the
  // mechanism that makes queue/slot state correct.
  void this.finalizeOwnedRequest(data).catch(() => undefined);
  try {
    // Bounded, because this runs on a path that has already given up, and the
    // caller's `RequestTimeoutError` waits behind it. A Redis that is up but
    // not answering (saturated, swapping, paused) leaves the command in
    // ioredis's queue indefinitely rather than rejecting it, so only a deadline
    // gets the caller its answer.
    await withDeadline(
      this.backend.publish(
        `${this.handlerNamespace}:requestDone`,
        JSON.stringify(data)
      ),
      ABANDONMENT_PUBLISH_TIMEOUT_MS,
      request.id
    );
  } catch (err) {
    this.logger.error(
      { error: err },
      `Failed to announce abandoned request ${request.id}; its retained completion will be republished after role reconciliation`
    );
  }
}

/**
 * Sleeps out a retry's back-off. Inside the loop's abandonment handling, so an
 * abort or a shutdown here releases the queue entry the retry kept.
 */
async function waitOutBackoff(
  this: BaseClient,
  request: Request,
  waitMs: number
): Promise<void> {
  const signal = request.config.signal;
  if (signal?.aborted) throw new RequestAbortedError(request.id);
  if (this.admissionHalted) throw admissionHaltedError(this);
  await new Promise<void>((resolve, reject) => {
    const finish = (error?: Error) => {
      clearTimeout(timer);
      signal?.removeEventListener?.("abort", onAbort);
      this.unregisterWaitingRequest(request.id);
      if (error) reject(error);
      else resolve();
    };
    const onAbort = () => finish(new RequestAbortedError(request.id));
    const timer = setTimeout(() => finish(), clampDelay(waitMs));
    signal?.addEventListener?.("abort", onAbort, { once: true });
    // Shutdown and client removal cancel waiters through this registration.
    this.registerWaitingRequest(request.id, finish);
  });
}

function admissionHaltedError(client: BaseClient): ClientUnavailableError {
  return new ClientUnavailableError(
    "handler_stopped",
    `Request handler stopped before client ${client.getName()} could admit this request.`
  );
}

async function handlePreRequest(this: BaseClient, request: Request) {
  await waitForRequestReady.bind(this)(request);
  if (this.requestOptions.requestInterceptor) {
    request.config = await this.requestOptions.requestInterceptor(
      request.config
    );
  }
  const authHeader = await authenticate.bind(this)(request);
  if (authHeader) {
    request.config = {
      ...request.config,
      headers: { ...request.config.headers, ...authHeader },
    };
  }
  const { method, baseURL, url } = request.config;
  // Some vendors want their key in the query string.
  this.logger.debug(
    `Request ID: ${request.id} | ${method} | ${redactUrl(baseURL) || ""}${
      redactUrl(url) || ""
    }${request.retries ? ` | Retry Attempt: ${request.retries}` : ""}`
  );
}

/**
 * Rejects with `RequestTimeoutError` if a backend call outlives the admission
 * budget, so a flapping backend cannot hold a caller forever.
 *
 * The underlying promise is left to settle on its own — there is no way to cancel
 * an in-flight Redis command, and abandoning it is what the caller wanted.
 */
async function withDeadline<T>(
  work: Promise<T>,
  timeoutMs: number,
  requestId: string
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new RequestTimeoutError(requestId, timeoutMs)),
          timeoutMs
        );
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function waitForRequestReady(this: BaseClient, request: Request) {
  const timeoutMs = clampDelay(this.requestOptions.cleanupTimeout || 60000);

  // A retry re-enters on the same Request object, and `fastPath` and
  // `claimedKeys` describe how *this* attempt was admitted, so both are cleared
  // first. A stale `fastPath` makes `handleRequestDone` skip the queue removal,
  // stranding an entry that disables the fast path for good; stale keys make an
  // abandonment release the previous attempt's claims.
  request.fastPath = false;
  request.claimedKeys = undefined;
  const metadata = request.getMetadata();

  // Checked before the fast path, not after: the fast path spends, and a caller who
  // has already given up must not be charged a token or a slot for a request axios
  // will then refuse to send.
  const signal = request.config.signal;
  if (signal?.aborted) {
    throw new RequestAbortedError(request.id);
  }
  // A retry re-enters here, and the fast path would otherwise send it upstream
  // after shutdown has stopped the drain loop admitting anything.
  if (this.admissionHalted) throw admissionHaltedError(this);

  // With nothing contending the queue has no ordering to do, so the enqueue, the
  // admission pass, both broadcasts and the removal are avoidable. Atomic, so it
  // cannot overtake queued work or double-spend.
  //
  // Deadlined like the wait that follows: this is the first backend call a request
  // makes, and ioredis resets its retry counter on every successful reconnect, so
  // against a Redis that accepts connections and drops them a queued command is
  // never rejected and the caller would wait forever.
  if (
    await withDeadline(
      this.tryAdmitImmediately(metadata),
      timeoutMs,
      request.id
    )
  ) {
    if (signal?.aborted) {
      void this.releaseUnusedAdmission(metadata).catch((error) => {
        this.logger.error(
          { error },
          `Request ID: ${request.id} | failed to return admission after abort`
        );
      });
      throw new RequestAbortedError(request.id);
    }
    request.fastPath = true;
    request.claimedKeys = metadata.claimedKeys;
    request.setStatus("inProgress");
    return true;
  }

  // Re-read, because the awaited call above may have spanned the abort. An
  // already-aborted request must not take a place in the queue: it would be
  // admitted in turn, spend, then fail inside axios with real traffic behind it.
  if (signal?.aborted) {
    throw new RequestAbortedError(request.id);
  }

  const readyPromise = new Promise<boolean>((resolve, reject) => {
    // Lets shutdown fail this request with a reason, and clears the timer so
    // the event loop can drain.
    const cancel = (error: Error) => {
      clearTimeout(timeout);
      signal?.removeEventListener?.("abort", onAbort);
      this.emitter.off(`requestReady:${request.id}`, onReady);
      this.emitter.off(`requestFailed:${request.id}`, onFailed);
      reject(error);
    };

    // What `failWaitingRequest` reaches when the client failing this request is
    // not the one it was queued through — a `sharedLimit` budget's owner.
    const onFailed = (error: Error) => {
      this.unregisterWaitingRequest(request.id);
      cancel(error);
    };

    // Aborting while queued releases the place rather than holding it until the
    // budget arrives.
    const onAbort = () => {
      clearTimeout(timeout);
      this.unregisterWaitingRequest(request.id);
      this.emitter.off(`requestReady:${request.id}`, onReady);
      this.emitter.off(`requestFailed:${request.id}`, onFailed);
      // Not awaited: an abort must return control promptly, and awaiting would
      // block up to ABANDONMENT_PUBLISH_TIMEOUT_MS on a wedged backend for a caller
      // who has stopped waiting. A request aborted while queued holds no slot, so
      // only the queue entry is released.
      //
      // The trailing catch is load-bearing: `publishAbandoned` swallows failures by
      // logging, and the logger is consumer code, so a throw from inside its catch
      // would reject this floating promise with nothing attached.
      void publishAbandoned
        .bind(this)(request, "Request aborted while queued")
        .catch(() => undefined);
      // Same reason as the timeout branch: this rejection lands in the catch
      // that calls `releaseAbandonedRequest`, which would announce it again.
      request.abandonmentPublished = true;
      reject(new RequestAbortedError(request.id));
    };

    const timeout = setTimeout(() => {
      this.unregisterWaitingRequest(request.id);
      signal?.removeEventListener?.("abort", onAbort);
      this.emitter.off(`requestReady:${request.id}`, onReady);
      this.emitter.off(`requestFailed:${request.id}`, onFailed);
      // Same reasoning as the abort branch for the trailing catch: the logger
      // inside `publishAbandoned`'s own catch is consumer code, and a throw there
      // would leave this floating promise rejected with nothing attached.
      void publishAbandoned
        .bind(this)(request, "Request timed out waiting for admission")
        .catch(() => undefined);
      // Marked so the catch in `handlePreRequest` does not announce the same
      // abandonment a second time — `requestDone` is a public channel, and a
      // consumer metering completions would double-count every timeout.
      request.abandonmentPublished = true;
      reject(new RequestTimeoutError(request.id, timeoutMs));
    }, timeoutMs);

    const onReady = (ready: RequestMetadata) => {
      clearTimeout(timeout);
      signal?.removeEventListener?.("abort", onAbort);
      this.emitter.off(`requestFailed:${request.id}`, onFailed);
      this.unregisterWaitingRequest(request.id);
      request.isThawRequest = ready.isThawRequest;
      request.claimedKeys = ready.claimedKeys;
      request.setStatus("inProgress");
      resolve(true);
    };

    // The `?.` on the method is unreachable — the `Request` constructor rejects a
    // signal without these two — and kept deliberately, so this code does not
    // depend on that check for a signal to get abort handling at all.
    signal?.addEventListener?.("abort", onAbort, { once: true });
    this.emitter.once(`requestReady:${request.id}`, onReady);
    this.emitter.once(`requestFailed:${request.id}`, onFailed);
    this.registerWaitingRequest(request.id, cancel);
  });

  // Nothing is attached to `readyPromise` until this function returns it, and the
  // enqueue below is awaited in between, so an abort landing in that window would
  // reject a promise with no handler — which under Node's default
  // `--unhandled-rejections=throw` terminates the host process. A sink rather than
  // registering the listener after the add, which would silently ignore an abort in
  // the same window; the promise returned below still rejects for the caller.
  void readyPromise.catch(() => undefined);

  // A retry re-enters with the same id and keeps its metadata. The add is
  // idempotent in the backend, so it needs no existence check first. Deadlined
  // for the reason given above the fast-path probe, and because nothing else
  // bounds it: `readyPromise`'s own timer cannot answer the caller while this
  // await still holds the function.
  const created = await withDeadline(
    this.addRequestToQueue({
      ...metadata,
      status: "pending",
    }),
    timeoutMs,
    request.id
  );

  // An idempotent add leaves an existing entry untouched, so a retry whose entry
  // survived the previous attempt has to be made claimable explicitly — here
  // rather than in `handleError`, so it lands after the listener above exists.
  //
  // Only when it already existed: a retry of a fast-path attempt has no entry, so
  // the add above created one already `pending`, and flipping it again would
  // un-claim a request the drain loop has since claimed, letting the next pass
  // spend a second token on it.
  if (request.retries > 0 && !created) {
    await this.updateRequestInQueue(request.id, { status: "pending" });
    await this.notifyRequestAdded();
  }

  return readyPromise;
}

/**
 * Announces a completion without letting the announcement change the outcome: the
 * vendor has already acted, so a backend hiccup must not fail a served request for a
 * caller who would then retry a non-idempotent operation, nor replace the vendor's
 * status. Cleanup is retained and retried by the request owner regardless.
 */
async function publishRequestDone(
  this: BaseClient,
  request: Request,
  payload: string
): Promise<void> {
  const data = JSON.parse(payload) as RequestDoneData;
  const attempts = 3;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await this.backend.publish(
        `${this.handlerNamespace}:requestDone`,
        payload
      );
      break;
    } catch (error) {
      if (attempt === attempts) {
        this.logger.error(
          { error },
          `Client ${this.name} | could not announce completion of request ${request.id} after ${attempts} attempts; the retained completion will be republished after role reconciliation`
        );
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
    }
  }
  // Publish first so peers retain a fallback before the owner can disappear
  // mid-cleanup. Cleanup remains asynchronous so retry latency is governed by
  // the configured back-off, not by a slow freeze write; controller shutdown
  // observes the retained pending record and drains it before closing.
  void this.finalizeOwnedRequest(data).catch(() => undefined);
}

/**
 * Runs the request's `settle` for the attempt that just came back, and applies
 * what it reports. Called before anything moves the retry count on, since the
 * claim to correct is filed under this attempt's.
 *
 * Never throws: the vendor has already answered, and a broken `settle` or a
 * backend hiccup must not fail a request that succeeded or turn a retryable
 * failure into a terminal one. The claim then stands.
 */
async function runSettle(
  this: BaseClient,
  request: Request,
  outcome: AttemptOutcome
): Promise<void> {
  const settle = request.config.settle;
  if (!settle) return;
  try {
    const settled = await settle(outcome);
    if (settled === undefined) return;
    await this.settleAttempt(request.getMetadata(), settled);
  } catch (error) {
    this.logger.error(
      { error },
      `Request ID: ${request.id} | settlement failed; the attempt keeps what it claimed`
    );
  }
}

async function handleResponse(
  this: BaseClient,
  request: Request,
  res: AxiosResponse
) {
  await runSettle.bind(this)(request, { response: res });
  await publishRequestDone.bind(this)(
    request,
    JSON.stringify(request.getRequestDoneData(undefined, res.status))
  );
  await runResponseHooks.bind(this)(request, res);
  this.logger.debug(`Request ID: ${request.id} | Status: ${res.status}`);
  return res;
}

/**
 * The client's `responseInterceptor` and `rateLimitChange`, which never fail a
 * served request.
 */
async function runResponseHooks(
  this: BaseClient,
  request: Request,
  res: AxiosResponse
) {
  if (this.requestOptions?.responseInterceptor) {
    try {
      await this.requestOptions.responseInterceptor(request.config, res);
    } catch (error) {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | responseInterceptor failed after the upstream request succeeded`
      );
    }
  }
  if (this.rateLimitChange) {
    try {
      const change = this.rateLimitChange as ClientTypes.RateLimitChange;
      const newLimit = await change(this.rateLimit, res);
      if (newLimit) await this.updateRateLimit(newLimit, "dynamic");
    } catch (error) {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | rateLimitChange failed after the upstream request succeeded`
      );
    }
  }
}

// ---- streamed responses

/** The default for `requestOptions.streamTimeout`: an hour. */
const DEFAULT_STREAM_TIMEOUT_MS = 3_600_000;

/**
 * How long after a stream ends an explicit `dianemo.settle` may still arrive
 * before a missing settlement is warned about. It often runs just after the
 * last chunk, once the caller has parsed it.
 */
const SETTLE_GRACE_MS = 5_000;

/**
 * Roughly the largest body that completes unread, since the stream completes
 * once every chunk has passed into the relay's readable side. 128 KiB is both
 * sides of a default `PassThrough` on Node 22, whose byte `highWaterMark` is
 * 64 KiB. See docs/rate-limits/streaming.md#the-two-moments.
 */
const RELAY_BUFFERS = { readableHighWaterMark: 128 * 1024 };

function isStreamed(request: Request, res: AxiosResponse): boolean {
  return request.config.responseType === "stream" && isReadableStream(res.data);
}

/**
 * The streamed half of a response: announced once when the headers arrive and
 * again when the stream ends, with the slot held, kept alive and in flight in
 * between. See docs/rate-limits/streaming.md.
 *
 * The caller gets a `PassThrough` in place of axios's stream, which keeps
 * backpressure and lets the end be observed without the caller's help.
 */
async function handleStreamedResponse(
  this: BaseClient,
  request: Request,
  res: AxiosResponse,
  span: Span,
  sentAt: number
): Promise<DianemoResponse> {
  span.setAttribute("dianemo.time_to_headers_ms", Date.now() - sentAt);
  const source = res.data as Readable;
  // Held until the listeners below exist: the announcements are awaited first,
  // and an `error` with no listener would be thrown out of the host.
  let earlyError: Error | undefined;
  const onEarlyError = (error: Error) => {
    earlyError ??= error;
  };
  source.on("error", onEarlyError);
  // A fast-path request has no queue entry and is never a thaw probe, so its
  // headers have nothing to settle.
  if (!request.fastPath) {
    await publishRequestDone.bind(this)(
      request,
      JSON.stringify(
        request.getRequestDoneData(undefined, res.status, "headers")
      )
    );
  }
  await runResponseHooks.bind(this)(request, res);

  const metadata = request.getMetadata();
  const observer = startObserver.bind(this)(request);
  const relay = observingRelay.bind(this)(request, observer);
  // A stream destroyed with an error emits `error`, and Node throws one that
  // has no listener — so a timeout or a shutdown would crash a host that reads
  // without listening for errors. Listeners the caller adds still receive it.
  relay.on("error", () => undefined);

  let settled = false;
  const settle = async (cost: RequestCost): Promise<void> => {
    if (settled) return;
    settled = true;
    try {
      await this.settleAttempt(metadata, cost);
    } catch (error) {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | settlement failed; the attempt keeps what it claimed`
      );
    }
  };

  let resolveFinished!: (reason: StreamEndReason) => void;
  const finished = new Promise<StreamEndReason>((resolve) => {
    resolveFinished = resolve;
  });

  const lostSlots = new Set<string>();
  const keepAlive = setInterval(() => {
    void this.keepAttemptAlive(metadata)
      .then((lost) => {
        for (const key of lost) {
          if (lostSlots.has(key)) continue;
          lostSlots.add(key);
          this.logger.warn(
            `Client ${this.name} | request ${request.id} no longer holds its slot in ${key}; the stream carries on outside maxConcurrency`
          );
        }
      })
      .catch((error) => {
        this.logger.error(
          { error },
          `Request ID: ${request.id} | could not keep the streaming request's claims alive`
        );
      });
  }, this.streamKeepAliveMs(metadata));
  keepAlive.unref?.();

  const timeoutMs = streamTimeoutOf(this.requestOptions.streamTimeout);
  const timeout = setTimeout(
    () => end("timeout", new StreamTimeoutError(request.id, timeoutMs)),
    timeoutMs
  );
  timeout.unref?.();

  const signal = request.config.signal;
  const onAbort = () =>
    end("closed", new RequestAbortedError(request.id, true));
  // axios's own abort listener usually destroys the source first, with its
  // `CanceledError`; the caller sees the same error whichever wins.
  // Sanitized because it becomes the caller's stream error: axios destroys the
  // source with an `AxiosError` that carries the request's config and headers.
  const onSourceError = (error: Error) =>
    signal?.aborted
      ? onAbort()
      : end(
          "error",
          sanitizeError(error, this.getSensitiveHeaderNames()) as Error
        );
  signal?.addEventListener?.("abort", onAbort, { once: true });

  let ended = false;
  const end = (reason: StreamEndReason, error?: Error) => {
    if (ended) return;
    ended = true;
    clearInterval(keepAlive);
    clearTimeout(timeout);
    signal?.removeEventListener?.("abort", onAbort);
    // Closing the upstream stops a generation nobody will read, and with it
    // the vendor's meter.
    if (reason !== "completed") source.destroy();
    if (error) relay.destroy(error);
    void finishStream
      .bind(this)(request, res, span, reason, observer, settle, () => settled)
      .catch((finishError) => {
        this.logger.error(
          { error: finishError },
          `Request ID: ${request.id} | could not finish the streamed response`
        );
      })
      .finally(() => {
        this.unregisterStream(request.id);
        resolveFinished(reason);
      });
  };

  // The relay's `finish`, not the source's `end`: a slow reader leaves the
  // last chunks buffered in the relay, unseen by the observer, after the
  // source ends.
  relay.once("finish", () => end("completed"));
  source.once("error", onSourceError);
  relay.once("close", () => end("closed"));
  source.off("error", onEarlyError);
  source.pipe(relay);
  if (earlyError) onSourceError(earlyError);

  this.registerStream(request.id, {
    close: () => end("shutdown", new StreamClosedError(request.id)),
    finished,
  });

  res.data = relay;
  const response = res as DianemoResponse;
  response.dianemo = { settle, finished };
  this.logger.debug(
    `Request ID: ${request.id} | Status: ${res.status} | Streaming...`
  );
  return response;
}

/**
 * What the caller reads in place of axios's stream: a `PassThrough`, or with an
 * observer a transform that shows it each chunk on the way through. Not a
 * `data` listener, which would switch the stream to flowing and read ahead of
 * the caller.
 */
function observingRelay(
  this: BaseClient,
  request: Request,
  observer: StreamObserver | undefined
): Transform {
  const onChunk = observer?.onChunk?.bind(observer);
  if (!onChunk) return new PassThrough(RELAY_BUFFERS);
  return new Transform({
    ...RELAY_BUFFERS,
    transform: (chunk: Buffer | string, _encoding, callback) => {
      try {
        onChunk(chunk);
      } catch (error) {
        this.logger.error(
          { error },
          `Request ID: ${request.id} | settleStream's onChunk threw; the stream continues`
        );
      }
      callback(null, chunk);
    },
  });
}

/** The request's `settleStream` observer, if it declares one that can be made. */
function startObserver(
  this: BaseClient,
  request: Request
): StreamObserver | undefined {
  const make = request.config.settleStream;
  if (!make) return undefined;
  try {
    return make();
  } catch (error) {
    this.logger.error(
      { error },
      `Request ID: ${request.id} | settleStream threw; the stream is relayed without it`
    );
    return undefined;
  }
}

function streamTimeoutOf(configured: number | undefined): number {
  return configured !== undefined &&
    Number.isFinite(configured) &&
    configured > 0
    ? clampDelay(configured)
    : DEFAULT_STREAM_TIMEOUT_MS;
}

/**
 * The second announcement: settles the attempt, releases its slot, and ends
 * its span and its count in flight. Runs once, however the stream ended.
 */
async function finishStream(
  this: BaseClient,
  request: Request,
  res: AxiosResponse,
  span: Span,
  reason: StreamEndReason,
  observer: StreamObserver | undefined,
  settle: (cost: RequestCost) => Promise<void>,
  isSettled: () => boolean
): Promise<void> {
  try {
    let result: StreamResult | undefined;
    try {
      result = observer?.result?.();
    } catch (error) {
      this.logger.error(
        { error },
        `Request ID: ${request.id} | settleStream's result threw; the claim stands unless settled otherwise`
      );
    }
    const streamError =
      result === undefined ? undefined : streamErrorOf(result);
    const cost = streamError
      ? streamError.cost
      : (result as RequestCost | undefined);
    if (cost !== undefined) {
      await settle(cost);
    } else if (request.config.settle && !isSettled()) {
      await runSettle.bind(this)(request, { response: res });
    }

    let retryData: RequestRetryData | undefined;
    if (streamError) {
      // Never retried: the caller has already read part of the body. A freeze
      // for the rest of the fleet, as a 5xx would bring.
      retryData = {
        retry: false,
        message: `Error inside the stream: ${streamError.error}`,
        isRateLimited: false,
        waitTime: 0,
        freezeClient: true,
      };
      handleBackoff.bind(this)(request, retryData);
      this.logger.error(`Request ID: ${request.id} | ${retryData.message}`);
    } else if (reason === "error" || reason === "timeout") {
      retryData = {
        retry: false,
        message: `Stream ended early: ${reason}`,
        isRateLimited: false,
        waitTime: 0,
        freezeClient: false,
      };
    }
    await publishRequestDone.bind(this)(
      request,
      JSON.stringify(request.getRequestDoneData(retryData, res.status, "end"))
    );

    if (!isSettled() && (observer || request.config.settle)) {
      setTimeout(() => {
        if (isSettled()) return;
        this.logger.warn(
          `Request ID: ${request.id} | streamed response was not settled; the attempt keeps what it claimed`
        );
      }, SETTLE_GRACE_MS).unref?.();
    }

    span.setAttribute("dianemo.stream_end_reason", reason);
    if (retryData) {
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: retryData.message,
      });
    }
  } finally {
    this.trackRequestFinished();
    span.setAttribute("dianemo.retries", request.retries);
    span.end();
  }
}

async function handleError(
  this: BaseClient,
  request: Request,
  res: AxiosError
) {
  // Sanitize before anything else touches the error. `retryHandler` is consumer
  // code and the likeliest place a credential gets logged, and it runs from
  // `handleRetry` below — long before the throw at the end of this function.
  sanitizeError(res, this.getSensitiveHeaderNames());

  let thrownToCaller = false;
  try {
    // Before `handleRetry`, which moves the retry count on to the next attempt.
    await runSettle.bind(this)(request, {
      response: res.response,
      error: res,
    });

    const retryData = await handleRetry.bind(this)(request, res);
    handleLogError.bind(this)(request, res, retryData);

    if (retryData.retry) {
      // Only the retry count, which is what re-scores the entry. Deliberately not
      // `status: "pending"` — that would make the entry claimable before the owner
      // has re-installed its `requestReady` listener, and an admission landing in
      // that window emits into an EventEmitter nobody is listening to.
      // `waitForRequestReady` flips it to pending once the listener exists.
      await this.updateRequestInQueue(request.id, {
        retries: request.retries,
      });
    }

    // The controller handles freeze and concurrency release from this event, and
    // leaves a retry in the queue — it distinguishes them by `responseStatus`.
    await publishRequestDone.bind(this)(
      request,
      JSON.stringify(
        request.getRequestDoneData(retryData, res.response?.status)
      )
    );

    if (retryData.retry) return retryData;
    thrownToCaller = true;
    throw res;
  } finally {
    // Nobody will read a streamed error body the caller is not handed, and an
    // unread one holds its socket open.
    const body = res.response?.data;
    if (!thrownToCaller && isReadableStream(body)) body.destroy();
  }
}

async function handleRetry(
  this: BaseClient,
  request: Request,
  error: AxiosError
) {
  const { retry429s, retry5xxs, retryStatusCodes } = this.retryOptions;
  const retryableCodes = ["ECONNRESET", "ETIMEDOUT", "ECONNABORTED"];
  const status = error?.response?.status;
  const data: RequestRetryData = {
    retry: true,
    message: "",
    isRateLimited: false,
    waitTime: 0,
    freezeClient: false,
  };
  // Classify the failure before deciding whether to retry. The freeze is a
  // fleet-wide statement about the upstream, and a 429 says the same thing on
  // the last permitted attempt as on the first, so `maxRetries: 0` still arms
  // the back-off.
  const outOfRetries = request.retries >= this.retryOptions.maxRetries;
  // The vendor's own word on when to come back, which no back-off may undercut.
  let floorMs = retryAfterMs(error?.response?.headers) ?? 0;

  const decision = await classifyError.bind(this)(request, error);
  if (decision?.action === "fail") {
    data.message += "Classified as terminal";
    data.retry = false;
  } else if (decision?.action === "retry") {
    data.message += "Classified as retryable";
    if (decision.freeze) {
      data.freezeClient = true;
      // Still a rate limit only if the upstream said so: it is what empties a
      // bucket and owes the freeze its thaw probes.
      data.isRateLimited = status === 429;
      floorMs = Math.max(floorMs, clampDelay(decision.freezeMs ?? 0));
    }
  } else if (status && status === 429 && retry429s) {
    data.message += "Rate Limited";
    data.isRateLimited = true;
    data.freezeClient = true;
  } else if (status && status >= 500 && retry5xxs) {
    data.message += "Server Error";
    data.freezeClient = true;
  } else if (status && retryStatusCodes.includes(status)) {
    data.message += "Client Wants Retry By Status";
  } else if (error.code && retryableCodes.includes(error.code)) {
    data.message += "Server Error";
    data.freezeClient = true;
  } else if (!outOfRetries && this.retryOptions?.retryHandler) {
    // This runs before the error is logged and before `requestDone` is
    // published, so a throw here would strand the slot and lose the upstream
    // failure. A broken retry policy means "do not retry", not "abort".
    try {
      const retry = await this.retryOptions.retryHandler(error);
      if (retry) data.message += "Client Wants Retry";
      else data.retry = false;
    } catch (handlerError) {
      this.logger.error(
        { error: handlerError },
        `Client ${this.name} | retryHandler threw; treating the request as non-retryable`
      );
      data.message += "Retry handler failed";
      data.retry = false;
    }
  } else data.retry = false;

  if (outOfRetries) {
    data.message += data.message
      ? " | Maximum number of retries reached."
      : "Maximum number of retries reached.";
    data.retry = false;
  }

  if (!data.retry) {
    // A freeze still needs a duration even though this request is finished
    // retrying: the freeze is about the upstream, not about this attempt, and
    // `handleRequestDone` ignores a freeze with no duration.
    if (data.freezeClient) handleBackoff.bind(this)(request, data, floorMs);
    return data;
  }
  request.incrementRetries();
  return handleBackoff.bind(this)(request, data, floorMs);
}

/**
 * Runs the client's `classifyError`, if it has one. Like `retryHandler`, it runs
 * before the failure is logged or announced, so a throw is caught here and the
 * built-in rules decide rather than the slot being stranded.
 */
async function classifyError(
  this: BaseClient,
  request: Request,
  error: AxiosError
): Promise<ClientTypes.ErrorDecision | undefined> {
  const classify = this.retryOptions.classifyError;
  if (!classify) return undefined;
  try {
    const decision = await classify(error);
    if (
      decision === undefined ||
      decision?.action === "fail" ||
      decision?.action === "retry"
    ) {
      return decision;
    }
    this.logger.error(
      { decision },
      `Request ID: ${request.id} | classifyError returned an unknown decision; the built-in rules decide`
    );
  } catch (classifierError) {
    this.logger.error(
      { error: classifierError },
      `Request ID: ${request.id} | classifyError threw; the built-in rules decide`
    );
  }
  return undefined;
}

/**
 * How long to wait before the next attempt, and how long a freeze lasts.
 * `floorMs` lengthens both and shortens neither, so repeated failures still
 * stretch a freeze past it.
 */
function handleBackoff(
  this: BaseClient,
  request: Request,
  data: RequestRetryData,
  floorMs = 0
) {
  const power = this.retryOptions.retryBackoffMethod === "exponential" ? 2 : 1;
  // At least one attempt's worth. `retries` is 0 on a first failure, and zero
  // times anything is a wait of zero — which reads as "no freeze" downstream.
  const attempt = Math.max(1, request.retries);
  const multiplier = Math.pow(attempt, power);
  data.waitTime = Math.max(
    floorMs,
    multiplier * this.getRetryBackoffBaseTime(data.isRateLimited)
  );
  data.freezeTime = Math.max(
    floorMs,
    multiplier * this.getFreezeBaseTime(data.isRateLimited)
  );
  if (data.retry) data.message += ` | Will retry...`;
  return data;
}

function handleLogError(
  this: BaseClient,
  request: Request,
  res: AxiosError,
  retryData: RequestRetryData
) {
  const status = res.response?.status;
  const shouldMute = status && this.httpStatusCodesToMute?.includes(status);
  const message = `Request ID: ${request.id} | Status: ${status} | Code: ${
    res.code
  }${retryData.message ? ` | ${retryData.message}` : ""}`;
  const extraHeaderNames = this.getSensitiveHeaderNames();
  const payload = {
    error: {
      message: res.message,
      stack: res.stack,
      code: res.code,
      config: redactConfig(res.config, extraHeaderNames),
      response: {
        status: res.response?.status,
        statusText: res.response?.statusText,
        headers: redactHeaders(
          res.response?.headers as Record<string, unknown> | undefined,
          extraHeaderNames
        ),
        data: redactBody(res.response?.data),
      },
    },
  };
  if (shouldMute) this.logger.debug(payload, message);
  else this.logger.error(payload, message);
}

export default handleRequest;

/**
 * Span attributes for a cost. A per-limit cost becomes one attribute per limit,
 * since an attribute value cannot be an object.
 */
function costAttributes(cost: RequestCost): Record<string, number> {
  // Taken before `Request` validates the cost, so anything unusable is recorded
  // as it was passed and left for that validation to reject.
  if (typeof cost !== "object" || cost === null) {
    return { "dianemo.cost": cost };
  }
  const attributes: Record<string, number> = {};
  for (const [name, amount] of Object.entries(cost)) {
    if (typeof amount === "number") attributes[`dianemo.cost.${name}`] = amount;
  }
  return attributes;
}
