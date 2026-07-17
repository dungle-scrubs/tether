import { Cause, Effect, Exit, Option } from "effect";
import { describe, expect, it } from "vitest";

import {
  ParticipantRuntimeCommandError,
  type ParticipantTaskExecutor,
} from "../src/participant-runtime-client.js";
import {
  buildParticipantTaskClaimFlow,
  invalidClaimDeadlineReason,
  nextClaimRefreshDelayMs,
  type ParticipantTaskClaimFlowClient,
  type ParticipantTaskClaimFlowContext,
  type ParticipantTaskClaimFlowInput,
  type ParticipantTaskClaimFlowLogger,
  ParticipantTaskClaimRejectedError,
  runParticipantTaskClaimFlow,
} from "../src/participant-task-claim-flow.js";
import { ParticipantTaskExecutionError } from "../src/participant-task-execution-error.js";
import type { TaskRecord } from "../src/types.js";

interface FlowHarness {
  readonly completedTaskIds: string[];
  readonly debugMessages: string[];
  readonly executorCalls: number;
  readonly failedFailures: Record<string, unknown>[];
  readonly refreshCalls: number;
  readonly releasedTaskIds: string[];
  run(): Promise<void>;
  runExit(): Promise<Exit.Exit<void, unknown>>;
}

interface HarnessOptions {
  readonly claimRefreshMs?: number;
  readonly claimTask?: () => Promise<TaskRecord | null>;
  readonly executor?: ParticipantTaskExecutor;
  readonly refreshTaskClaim?: () => Promise<TaskRecord | null>;
}

const farFutureDeadline = "2099-07-17T00:00:00.000Z";

describe("participant task claim flow claim deadline validation", () => {
  it("declines a claim whose deadline lacks an RFC 3339 offset before the executor runs", async () => {
    const harness = createHarness("2099-07-17T00:00:00");

    await harness.run();

    expect(harness.executorCalls).toBe(0);
    expect(harness.completedTaskIds).toEqual([]);
    expect(harness.debugMessages).toContain(invalidClaimDeadlineReason);
  });

  it("declines a claim with a null deadline before the executor runs", async () => {
    const harness = createHarness(null);

    await harness.run();

    expect(harness.executorCalls).toBe(0);
    expect(harness.debugMessages).toContain(invalidClaimDeadlineReason);
  });

  it("runs the executor when the deadline is a valid offset timestamp", async () => {
    const harness = createHarness(farFutureDeadline);

    await harness.run();

    expect(harness.executorCalls).toBe(1);
    expect(harness.completedTaskIds).toEqual(["task_1"]);
    expect(harness.debugMessages).not.toContain(invalidClaimDeadlineReason);
  });
});

describe("participant task claim flow executor failure resolution", () => {
  it("releases the claim instead of failing the task when the executor failure is retryable", async () => {
    const harness = createHarness(farFutureDeadline, {
      executor: async () => {
        throw new ParticipantTaskExecutionError("Generation backend restarting", {
          code: "generation_unavailable",
          retryable: true,
        });
      },
    });

    await harness.run();

    expect(harness.releasedTaskIds).toEqual(["task_1"]);
    expect(harness.failedFailures).toEqual([]);
    expect(harness.completedTaskIds).toEqual([]);
    expect(harness.debugMessages).toContain("task.released_for_retry");
  });

  it("fails the task terminally when the executor failure is not retryable", async () => {
    const harness = createHarness(farFutureDeadline, {
      executor: async () => {
        throw new ParticipantTaskExecutionError("Poisoned input", {
          code: "poison_range",
          retryable: false,
        });
      },
    });

    await harness.run();

    expect(harness.releasedTaskIds).toEqual([]);
    expect(harness.failedFailures).toEqual([{ code: "poison_range", retryable: false }]);
  });

  it("fails the task terminally for executor errors without structured failure metadata", async () => {
    const harness = createHarness(farFutureDeadline, {
      executor: async () => {
        throw new Error("Unexpected executor crash");
      },
    });

    await harness.run();

    expect(harness.releasedTaskIds).toEqual([]);
    expect(harness.failedFailures).toEqual([{ error: "Unexpected executor crash" }]);
  });
});

describe("participant task claim flow claim command rejection", () => {
  it("propagates a server claim rejection as a typed error instead of a debug log", async () => {
    const rejection = new ParticipantRuntimeCommandError({
      message: "participant_required",
      op: "task.claim",
      pendingCommandCount: 0,
      requestId: "req_1",
      taskId: "task_1",
    });
    const harness = createHarness(farFutureDeadline, {
      claimTask: async () => {
        throw rejection;
      },
    });

    const exit = await harness.runExit();

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      const failure = Option.getOrThrow(Cause.failureOption(exit.cause));
      expect(failure).toBeInstanceOf(ParticipantTaskClaimRejectedError);
      const claimRejected = failure as ParticipantTaskClaimRejectedError;
      expect(claimRejected.taskId).toBe("task_1");
      expect(claimRejected.participantId).toBe("participant_1");
      expect(claimRejected.sessionId).toBe("sess_1");
      expect(claimRejected.cause).toBe(rejection);
    }
    expect(harness.executorCalls).toBe(0);
    expect(harness.debugMessages).not.toContain("task.claim_transport_unknown");
  });

  it("rejects the flow promise so the runtime error boundary observes the rejection", async () => {
    const harness = createHarness(farFutureDeadline, {
      claimTask: async () => {
        throw new ParticipantRuntimeCommandError({
          message: "authorization_failed",
          op: "task.claim",
          pendingCommandCount: 0,
          requestId: "req_2",
          taskId: "task_1",
        });
      },
    });

    await expect(harness.run()).rejects.toThrow(
      "Task claim command rejected: authorization_failed",
    );
  });

  it("keeps transport claim failures on the debug-only lease-expiry fallback path", async () => {
    const harness = createHarness(farFutureDeadline, {
      claimTask: async () => {
        throw new Error("WebSocket closed");
      },
    });

    await harness.run();

    expect(harness.executorCalls).toBe(0);
    expect(harness.debugMessages).toContain("task.claim_transport_unknown");
  });
});

describe("participant task claim flow refresh scheduling", () => {
  it("clamps the refresh delay to a safe fraction of the remaining lease", () => {
    const now = Date.parse("2026-07-17T00:00:00.000Z");
    const leaseDeadline = "2026-07-17T00:00:30.000Z";

    expect(nextClaimRefreshDelayMs(3_600_000, leaseDeadline, now)).toBe(15_000);
    expect(nextClaimRefreshDelayMs(30_000, leaseDeadline, now)).toBe(15_000);
  });

  it("keeps a configured interval that is already below the safe lease fraction", () => {
    const now = Date.parse("2026-07-17T00:00:00.000Z");
    const leaseDeadline = "2026-07-17T00:00:30.000Z";

    expect(nextClaimRefreshDelayMs(5_000, leaseDeadline, now)).toBe(5_000);
  });

  it("falls back to the configured interval when no deadline is available", () => {
    expect(nextClaimRefreshDelayMs(7_500, null)).toBe(7_500);
    expect(nextClaimRefreshDelayMs(7_500, "not-a-timestamp")).toBe(7_500);
  });

  it("floors the delay when the lease has already elapsed", () => {
    const now = Date.parse("2026-07-17T00:01:00.000Z");
    const leaseDeadline = "2026-07-17T00:00:30.000Z";

    expect(nextClaimRefreshDelayMs(30_000, leaseDeadline, now)).toBe(50);
  });

  it("refreshes before an unsafe interval lets the lease elapse during execution", async () => {
    const leaseMs = 400;
    const mintDeadline = (): string => new Date(Date.now() + leaseMs).toISOString();
    const harness = createHarness(mintDeadline(), {
      claimRefreshMs: 3_600_000,
      executor: async () => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        return { output: null, result: {} };
      },
      refreshTaskClaim: async () =>
        Promise.resolve({
          ...createTaskFixture(),
          claimExpiresAt: mintDeadline(),
          claimId: "claim_1",
        }),
    });

    await harness.run();

    expect(harness.refreshCalls).toBeGreaterThanOrEqual(1);
    expect(harness.completedTaskIds).toEqual(["task_1"]);
    expect(harness.debugMessages).toContain("task.claim_refresh_interval_clamped");
  });
});

/** Builds a claim-flow harness whose claim mints the supplied deadline. */
function createHarness(claimExpiresAt: string | null, options: HarnessOptions = {}): FlowHarness {
  const debugMessages: string[] = [];
  const completedTaskIds: string[] = [];
  const failedFailures: Record<string, unknown>[] = [];
  const releasedTaskIds: string[] = [];
  let executorCalls = 0;
  let refreshCalls = 0;
  const claimedTask: TaskRecord = { ...createTaskFixture(), claimExpiresAt, claimId: "claim_1" };
  const executor: ParticipantTaskExecutor = async (executorContext) => {
    executorCalls += 1;
    if (options.executor) {
      return options.executor(executorContext);
    }
    return { output: null, result: {} };
  };

  const client: ParticipantTaskClaimFlowClient = {
    appendEvent: async () => undefined,
    claimTask: options.claimTask ?? (async () => claimedTask),
    completeTask: async (taskId) => {
      completedTaskIds.push(taskId);
    },
    failTask: async (_taskId, failure) => {
      failedFailures.push(failure);
    },
    refreshTaskClaim: async () => {
      refreshCalls += 1;
      return options.refreshTaskClaim ? options.refreshTaskClaim() : claimedTask;
    },
    releaseTask: async (taskId) => {
      releasedTaskIds.push(taskId);
    },
  };
  const context: ParticipantTaskClaimFlowContext = {
    instanceId: "instance_1",
    lastObservedSeq: 0,
    participantId: "participant_1",
    recentEvents: [],
    sessionId: "sess_1",
  };
  const logger: ParticipantTaskClaimFlowLogger = {
    debug: (_boundary, message) => {
      debugMessages.push(message);
    },
  };
  const input: ParticipantTaskClaimFlowInput = {
    cancellation: {
      abortActive: () => undefined,
      isCancelled: () => false,
      signal: new AbortController().signal,
    },
    claimRefreshMs: options.claimRefreshMs ?? 3_600_000,
    executor,
    task: createTaskFixture(),
  };

  return {
    completedTaskIds,
    debugMessages,
    get executorCalls() {
      return executorCalls;
    },
    failedFailures,
    get refreshCalls() {
      return refreshCalls;
    },
    releasedTaskIds,
    run: () => runParticipantTaskClaimFlow(client, context, logger, input),
    runExit: () =>
      Effect.runPromiseExit(buildParticipantTaskClaimFlow(client, context, logger, input)),
  };
}

/** Builds a minimal claimable task fixture. */
function createTaskFixture(): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: null,
    kind: "generic_request",
    objective: "handle request",
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: "sess_1",
    taskId: "task_1",
  };
}
