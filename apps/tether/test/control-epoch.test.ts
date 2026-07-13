import { describe, expect, it } from "vitest";

import {
  ControlEpochStaleError,
  controlEpochStaleErrorCode,
  isControlEpochCurrent,
  isControlEpochStale,
  isControlEpochStaleError,
  isPositiveSafeIntegerEpoch,
  nextControlEpoch,
  parseControlEpoch,
} from "../src/control-epoch.js";

describe("control epoch predicates", () => {
  it("accepts only positive safe integers as epochs", () => {
    expect(isPositiveSafeIntegerEpoch(1)).toBe(true);
    expect(isPositiveSafeIntegerEpoch(42)).toBe(true);
    expect(isPositiveSafeIntegerEpoch(0)).toBe(false);
    expect(isPositiveSafeIntegerEpoch(-1)).toBe(false);
    expect(isPositiveSafeIntegerEpoch(1.5)).toBe(false);
    expect(isPositiveSafeIntegerEpoch(Number.NaN)).toBe(false);
    expect(isPositiveSafeIntegerEpoch(Number.MAX_SAFE_INTEGER + 1)).toBe(false);
    expect(isPositiveSafeIntegerEpoch("3")).toBe(false);
  });

  it("parses positive numeric strings and bigints from the durable column", () => {
    expect(parseControlEpoch(7)).toBe(7);
    expect(parseControlEpoch("7")).toBe(7);
    expect(parseControlEpoch(7n)).toBe(7);
    expect(parseControlEpoch("0")).toBeNull();
    expect(parseControlEpoch("-4")).toBeNull();
    expect(parseControlEpoch("abc")).toBeNull();
    expect(parseControlEpoch(0n)).toBeNull();
    expect(parseControlEpoch(null)).toBeNull();
    expect(parseControlEpoch(undefined)).toBeNull();
  });

  it("advances epochs strictly and starts a fresh owner at one", () => {
    expect(nextControlEpoch(null)).toBe(1);
    expect(nextControlEpoch(1)).toBe(2);
    expect(nextControlEpoch(41)).toBe(42);
    expect(() => nextControlEpoch(0)).toThrow();
    expect(() => nextControlEpoch(Number.MAX_SAFE_INTEGER)).toThrow();
  });

  it("treats only the exact current generation as fresh", () => {
    expect(isControlEpochCurrent(8, 8)).toBe(true);
    expect(isControlEpochCurrent(8, 7)).toBe(false);
    expect(isControlEpochCurrent(8, 9)).toBe(false);
    expect(isControlEpochStale(8, 8)).toBe(false);
    expect(isControlEpochStale(8, 7)).toBe(true);
    expect(isControlEpochStale(8, null)).toBe(true);
    expect(isControlEpochStale(8, undefined)).toBe(true);
  });
});

describe("ControlEpochStaleError", () => {
  it("carries the public code and fencing diagnostics", () => {
    const error = new ControlEpochStaleError({
      controlChannel: "ws",
      currentEpoch: 8,
      participantId: "part_1",
      providedEpoch: 7,
      sessionId: "sess_1",
    });
    expect(error.code).toBe(controlEpochStaleErrorCode);
    expect(error._tag).toBe("ControlEpochStale");
    expect(error.currentEpoch).toBe(8);
    expect(error.providedEpoch).toBe(7);
    expect(isControlEpochStaleError(error)).toBe(true);
    expect(isControlEpochStaleError(new Error("other"))).toBe(false);
  });
});
