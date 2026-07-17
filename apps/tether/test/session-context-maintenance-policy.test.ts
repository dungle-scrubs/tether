import { describe, expect, it, vi } from "vitest";

import {
  decideSessionContextMaintenance,
  enqueueSessionContextMaintenance,
  sessionContextMaintenancePolicies,
  truncatedSessionContextSuffixTokenFloor,
} from "../src/session-context-maintenance-policy.js";

describe("Session Context maintenance policy", () => {
  it.each([
    ["2k", 3_000],
    ["8k", 12_000],
    ["16k", 24_000],
    ["32k", 48_000],
  ] as const)("uses a deterministic hysteresis threshold for %s", (budgetClass, triggerTokens) => {
    expect(sessionContextMaintenancePolicies[budgetClass]).toEqual({
      targetTokens: triggerTokens / 1.5,
      triggerTokens,
    });
    expect(
      decideSessionContextMaintenance({ budgetClass, unsummarizedTokens: triggerTokens - 1 }),
    ).toMatchObject({ status: "below_threshold" });
    expect(
      decideSessionContextMaintenance({ budgetClass, unsummarizedTokens: triggerTokens }),
    ).toEqual({ budgetClass, status: "enqueue", triggerTokens, unsummarizedTokens: triggerTokens });
  });

  it("enqueues without waiting for maintenance completion", async () => {
    let resolveMaintenance: (() => void) | undefined;
    const schedule = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveMaintenance = resolve;
        }),
    );

    enqueueSessionContextMaintenance(
      { budgetClass: "8k", status: "enqueue", triggerTokens: 12_000, unsummarizedTokens: 12_500 },
      schedule,
      vi.fn(),
    );

    expect(schedule).not.toHaveBeenCalled();
    await Promise.resolve();
    expect(schedule).toHaveBeenCalledOnce();
    resolveMaintenance?.();
  });

  it("conservatively enqueues every class when the bounded 10k tail is truncated", () => {
    expect(truncatedSessionContextSuffixTokenFloor).toBe(
      Math.max(
        ...Object.values(sessionContextMaintenancePolicies).map((policy) => policy.triggerTokens),
      ),
    );
    for (const budgetClass of ["2k", "8k", "16k", "32k"] as const) {
      expect(
        decideSessionContextMaintenance({
          budgetClass,
          unsummarizedTokens: truncatedSessionContextSuffixTokenFloor,
        }).status,
      ).toBe("enqueue");
    }
  });

  it.each([
    "sync",
    "async",
  ] as const)("reports bounded %s scheduler failures outside the context read", async (failureMode) => {
    const reportFailure = vi.fn();
    expect(() =>
      enqueueSessionContextMaintenance(
        {
          budgetClass: "2k",
          status: "enqueue",
          triggerTokens: 3_000,
          unsummarizedTokens: 3_000,
        },
        () => {
          if (failureMode === "sync") {
            throw new Error("scheduler unavailable");
          }
          return Promise.reject(new Error("scheduler unavailable"));
        },
        reportFailure,
      ),
    ).not.toThrow();

    await vi.waitFor(() =>
      expect(reportFailure).toHaveBeenCalledWith({
        budgetClass: "2k",
        code: "schedule_failed",
      }),
    );
  });
});
