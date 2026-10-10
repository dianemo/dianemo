import type { NamedRateLimitData } from "../packages/core/src/client/types.js";
import { describe, expect, it } from "vitest";
import {
  budgetLimitsFor,
  claimedAmount,
  costFor,
  costProblem,
  unusableCostReason,
} from "../packages/core/src/utils/cost.js";
import {
  RequestCostExceedsBudgetError,
  RequestCostMismatchError,
} from "../packages/core/src/errors.js";

const rpm: NamedRateLimitData = {
  name: "rpm",
  type: "requestLimit",
  interval: 1_000,
  tokensToAdd: 1,
  maxTokens: 60,
};
const otpm: NamedRateLimitData = {
  name: "otpm",
  type: "requestLimit",
  interval: 1_000,
  tokensToAdd: 6_667,
  maxTokens: 400_000,
};
const slots: NamedRateLimitData = {
  name: "slots",
  type: "concurrencyLimit",
  maxConcurrency: 4,
};
const unmetered: NamedRateLimitData = { name: "none", type: "noLimit" };

describe("unusableCostReason", () => {
  it.each([[1], [0.5], [Number.EPSILON]])("accepts the number %s", (cost) => {
    expect(unusableCostReason(cost)).toBeUndefined();
  });

  it.each([[0], [-1], [NaN], [Infinity], [5e-324]])(
    "refuses the number %s",
    (cost) => {
      expect(unusableCostReason(cost)).toMatch(/finite number of at least/);
    }
  );

  it("accepts a per-limit cost with a zero entry", () => {
    expect(unusableCostReason({ rpm: 1, otpm: 0 })).toBeUndefined();
  });

  it("refuses a per-limit cost that spends nothing anywhere", () => {
    expect(unusableCostReason({ rpm: 0, otpm: 0 })).toMatch(
      /spend something against at least one/
    );
  });

  it("refuses an empty per-limit cost", () => {
    expect(unusableCostReason({})).toMatch(/at least one rate limit/);
  });

  it.each([[{ rpm: -1 }], [{ rpm: NaN }], [{ rpm: "1" }], [{ rpm: 5e-324 }]])(
    "refuses the entry in %j",
    (cost) => {
      expect(unusableCostReason(cost)).toMatch(/for rate limit "rpm"/);
    }
  );

  it.each([[null], ["1"], [[1]], [new Map([["rpm", 1]])]])(
    "refuses %j as neither a number nor an object of numbers",
    (cost) => {
      expect(unusableCostReason(cost)).toMatch(/a number or an object/);
    }
  );
});

describe("costProblem", () => {
  it("measures a number against the tightest ceiling", () => {
    const problem = costProblem("c", 100, [rpm, otpm]);
    expect(problem).toBeInstanceOf(RequestCostExceedsBudgetError);
    expect(problem?.message).toMatch(
      /\(60\), the maxTokens or maxConcurrency of rate limit "rpm"/
    );
  });

  it("measures each amount against its own limit", () => {
    expect(costProblem("c", { rpm: 1, otpm: 300_000 }, [rpm, otpm])).toBe(
      undefined
    );
    expect(
      costProblem("c", { rpm: 1, otpm: 500_000 }, [rpm, otpm])?.message
    ).toMatch(/of rate limit "otpm"/);
  });

  it("names a single limit's breach without naming the limit", () => {
    expect(costProblem("c", 100, [{ ...rpm, name: "default" }])?.message).toBe(
      "Request cost (100) exceeds the maximum client c can ever admit (60), its smallest maxTokens or maxConcurrency"
    );
  });

  it("reports both what is missing and what is unknown", () => {
    const problem = costProblem("c", { rpm: 1, tpm: 1 }, [rpm, otpm, slots]);
    expect(problem).toBeInstanceOf(RequestCostMismatchError);
    expect(problem?.message).toMatch(/omits "otpm", "slots"/);
    expect(problem?.message).toMatch(/names "tpm", which it does not declare/);
  });

  it("refuses any per-limit cost for a client that meters nothing", () => {
    expect(costProblem("c", { rpm: 1 }, [])?.message).toMatch(
      /it meters none, so pass a number/
    );
  });

  it("does not take an inherited property for a limit's amount", () => {
    const named = { ...rpm, name: "constructor" };
    expect(costProblem("c", { rpm: 1 }, [named])).toBeInstanceOf(
      RequestCostMismatchError
    );
  });

  it("has nothing to say when the shared owner cannot be resolved", () => {
    expect(costProblem("c", { anything: 1 }, undefined)).toBeUndefined();
  });
});

describe("budgetLimitsFor", () => {
  it("keeps only the limits that meter", () => {
    expect(budgetLimitsFor([rpm, unmetered, slots])).toEqual([rpm, slots]);
  });

  it("answers for the owner of a shared limit", () => {
    const shared: NamedRateLimitData = {
      name: "default",
      type: "sharedLimit",
      clientName: "owner",
    };
    expect(budgetLimitsFor([shared], () => [rpm, otpm])).toEqual([rpm, otpm]);
    expect(budgetLimitsFor([shared], () => undefined)).toBeUndefined();
  });
});

describe("costFor and claimedAmount", () => {
  it("charges a number to every limit", () => {
    expect(costFor(3, "anything")).toBe(3);
  });

  it("throws for a limit a per-limit cost does not name", () => {
    expect(() => costFor({ rpm: 1 }, "otpm")).toThrow(/no amount/);
  });

  it("hands back nothing for a limit the claim did not name", () => {
    expect(claimedAmount({ rpm: 1 }, "otpm")).toBe(0);
    expect(claimedAmount({ rpm: 1 }, "rpm")).toBe(1);
  });
});
