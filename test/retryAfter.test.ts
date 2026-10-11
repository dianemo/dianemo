import { describe, expect, it } from "vitest";
import { AxiosHeaders } from "axios";
import {
  clampDelay,
  MAX_TIMER_MS,
  retryAfterMs,
} from "../packages/core/src/utils/retryAfter.js";

describe("retryAfterMs", () => {
  const now = Date.parse("2026-10-11T12:00:00Z");

  it("reads delay-seconds", () => {
    expect(retryAfterMs({ "retry-after": "20" }, now)).toBe(20_000);
  });

  it("reads a fractional number of seconds", () => {
    expect(retryAfterMs({ "retry-after": "1.5" }, now)).toBe(1_500);
  });

  it("reads an HTTP date as the time left until it", () => {
    expect(
      retryAfterMs({ "retry-after": "Sun, 11 Oct 2026 12:00:30 GMT" }, now)
    ).toBe(30_000);
  });

  it("reads a date already past as no wait", () => {
    expect(
      retryAfterMs({ "retry-after": "Sun, 11 Oct 2026 11:59:00 GMT" }, now)
    ).toBe(0);
  });

  it.each([["soon"], ["-5"], [""], ["1e3"]])(
    "ignores a value of %j",
    (value) => {
      expect(retryAfterMs({ "retry-after": value }, now)).toBe(undefined);
    }
  );

  it("reports nothing without the header", () => {
    expect(retryAfterMs({}, now)).toBe(undefined);
    expect(retryAfterMs(undefined, now)).toBe(undefined);
  });

  it("finds the header whatever its case", () => {
    expect(retryAfterMs({ "Retry-After": "2" }, now)).toBe(2_000);
    expect(retryAfterMs(new AxiosHeaders({ "Retry-After": "3" }), now)).toBe(
      3_000
    );
  });

  it("caps a wait at what a timer can hold", () => {
    expect(retryAfterMs({ "retry-after": "99999999" }, now)).toBe(MAX_TIMER_MS);
  });
});

describe("clampDelay", () => {
  it.each([[NaN], [Infinity], [-1], [0]])("reads %s as no delay", (ms) => {
    expect(clampDelay(ms)).toBe(0);
  });

  it("rounds a fraction of a millisecond up", () => {
    expect(clampDelay(0.2)).toBe(1);
  });
});
