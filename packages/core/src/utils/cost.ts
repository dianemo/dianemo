import type { NamedRateLimitData } from "../client/types.js";
import type { RequestCost } from "../request/types.js";
import {
  ConfigurationError,
  RequestCostExceedsBudgetError,
  RequestCostMismatchError,
} from "../errors.js";

/** A limit that meters a request, and so one a per-limit cost must name. */
export type MeteredLimit = Extract<
  NamedRateLimitData,
  { type: "requestLimit" | "concurrencyLimit" }
>;

/**
 * Why `cost` could not be spent against any budget, or undefined when it can.
 *
 * Shape only; whether it names the right limits is {@link costProblem}'s
 * question. `Number.EPSILON` is the floor for a positive amount, not zero: a
 * positive value below it cannot be subtracted from a balance of 1 or more in
 * floating point — `tokens - cost` returns `tokens` — so `cost: 5e-324` would
 * spend nothing and buy a request.
 */
export function unusableCostReason(cost: unknown): string | undefined {
  if (typeof cost === "number") {
    if (Number.isFinite(cost) && cost >= Number.EPSILON) return undefined;
    return `cost must be a finite number of at least ${Number.EPSILON}. Received ${describe(cost)}.`;
  }
  if (!isPlainObject(cost)) {
    return `cost must be a number or an object of numbers keyed by rate limit name. Received ${describe(cost)}.`;
  }
  const entries = Object.entries(cost);
  if (entries.length === 0) {
    return "cost must name at least one rate limit when given as an object. Received {}.";
  }
  for (const [name, amount] of entries) {
    // Zero is allowed per entry, so a request can be free against one limit; the
    // floor applies above it for the reason given on this function.
    if (
      typeof amount !== "number" ||
      !Number.isFinite(amount) ||
      (amount !== 0 && amount < Number.EPSILON)
    ) {
      return `cost for rate limit "${name}" must be 0 or a finite number of at least ${Number.EPSILON}. Received ${describe(amount)}.`;
    }
  }
  // Every entry is a number by now: the loop above returned on anything else.
  if (!entries.some(([, amount]) => (amount as number) >= Number.EPSILON)) {
    return `cost must spend something against at least one rate limit. Received ${describe(cost)}.`;
  }
  return undefined;
}

/** Throws where a cost enters, so a malformed one is reported as itself. */
export function assertUsableCost(cost: unknown): asserts cost is RequestCost {
  const reason = unusableCostReason(cost);
  if (reason) throw new ConfigurationError("invalid_request_cost", reason);
}

/**
 * The limits a request on this config spends: its own, or the owner's for a
 * `sharedLimit` client. Undefined when the owner cannot be resolved, which the
 * queue path reports rather than this.
 */
export function budgetLimitsFor(
  config: readonly NamedRateLimitData[],
  resolveShared?: (
    clientName: string
  ) => readonly NamedRateLimitData[] | undefined
): MeteredLimit[] | undefined {
  const shared = config.find((limit) => limit.type === "sharedLimit");
  if (shared) {
    const owner = resolveShared?.(shared.clientName);
    // Not recursed through another `sharedLimit`: registration refuses a budget
    // owner that is itself shared, so one hop always reaches the owner.
    return owner ? owner.filter(isMetered) : undefined;
  }
  return config.filter(isMetered);
}

/**
 * The amount charged to one named limit.
 *
 * Callers check the cost against the client's limits first, so a missing entry
 * here means a limit was added between that check and this read.
 */
export function costFor(cost: RequestCost, limitName: string): number {
  if (typeof cost === "number") return cost;
  // Own properties only: a limit may legally be named `constructor`, and the
  // inherited one is not an amount.
  const amount = Object.hasOwn(cost, limitName) ? cost[limitName] : undefined;
  if (typeof amount === "number") return amount;
  throw new ConfigurationError(
    "invalid_request_cost",
    `cost names no amount for rate limit "${limitName}". Received ${describe(cost)}.`
  );
}

/**
 * The amount a claim made under `cost` took from one limit, for handing it back.
 *
 * 0 where `cost` does not name the limit, rather than throwing as
 * {@link costFor} does: a limit added after the claim took nothing, and a release
 * must not fail on it with other budgets still to return.
 */
export function claimedAmount(cost: RequestCost, limitName: string): number {
  if (typeof cost !== "number" && !Object.hasOwn(cost, limitName)) return 0;
  return costFor(cost, limitName);
}

/**
 * Why a well-formed cost cannot be admitted by these limits, or undefined.
 *
 * Either it does not name exactly the metered limits, or an amount exceeds its
 * own limit's ceiling — the largest amount that limit could ever admit, so no
 * wait would help. A number is charged to every limit and so is measured
 * against the tightest.
 */
export function costProblem(
  clientName: string,
  cost: RequestCost,
  limits: readonly MeteredLimit[] | undefined
): RequestCostMismatchError | RequestCostExceedsBudgetError | undefined {
  if (!limits) return undefined;

  if (typeof cost !== "number") {
    const declared = limits.map((limit) => limit.name);
    const missing = declared.filter((name) => !Object.hasOwn(cost, name));
    const unknown = Object.keys(cost).filter(
      (name) => !declared.includes(name)
    );
    if (missing.length || unknown.length) {
      return new RequestCostMismatchError(
        clientName,
        missing,
        unknown,
        declared
      );
    }
  }

  // The tightest breached ceiling, which for a number is the tightest of all —
  // the figure a caller would have to get under.
  let breach:
    { limit: MeteredLimit; amount: number; ceiling: number } | undefined;
  for (const limit of limits) {
    const amount = costFor(cost, limit.name);
    const ceiling = ceilingOf(limit);
    if (amount <= ceiling) continue;
    if (!breach || ceiling < breach.ceiling)
      breach = { limit, amount, ceiling };
  }
  if (!breach) return undefined;
  return new RequestCostExceedsBudgetError(
    clientName,
    breach.amount,
    breach.ceiling,
    limits.length > 1 ? breach.limit.name : undefined
  );
}

function ceilingOf(limit: MeteredLimit): number {
  return limit.type === "requestLimit" ? limit.maxTokens : limit.maxConcurrency;
}

function isMetered(limit: NamedRateLimitData): limit is MeteredLimit {
  return limit.type === "requestLimit" || limit.type === "concurrencyLimit";
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * Renders a rejected value readably. `JSON.stringify(NaN)` is `"null"`, which
 * sends a caller looking for a null they never passed.
 */
export function describe(value: unknown): string {
  if (typeof value === "number" && !Number.isFinite(value))
    return String(value);
  return JSON.stringify(value) ?? String(value);
}
