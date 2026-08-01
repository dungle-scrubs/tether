import { describe, expect, it } from "vitest";

import { boundedExponentialRetryDelayMs } from "../src/retry.js";

describe("boundedExponentialRetryDelayMs", () => {
  it("grows exponentially and caps at the configured maximum", () => {
    expect(boundedExponentialRetryDelayMs(0, 100, 250, () => 1)).toBe(100);
    expect(boundedExponentialRetryDelayMs(1, 100, 250, () => 1)).toBe(200);
    expect(boundedExponentialRetryDelayMs(2, 100, 250, () => 1)).toBe(250);
  });

  it("applies injectable equal jitter within half to all of the bounded delay", () => {
    expect(boundedExponentialRetryDelayMs(1, 100, 1_000, () => 0)).toBe(100);
    expect(boundedExponentialRetryDelayMs(1, 100, 1_000, () => 0.5)).toBe(150);
    expect(boundedExponentialRetryDelayMs(1, 100, 1_000, () => 1)).toBe(200);
  });

  it("accepts deterministic zero-delay policies and rejects invalid inputs", () => {
    expect(boundedExponentialRetryDelayMs(3, 0, 0)).toBe(0);
    expect(boundedExponentialRetryDelayMs(1_024, 0, 2_000)).toBe(0);
    expect(() => boundedExponentialRetryDelayMs(-1, 100, 200)).toThrow();
    expect(() => boundedExponentialRetryDelayMs(0.5, 100, 200)).toThrow();
    expect(() => boundedExponentialRetryDelayMs(0, Number.POSITIVE_INFINITY, 200)).toThrow();
  });
});
