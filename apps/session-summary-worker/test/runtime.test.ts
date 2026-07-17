import type {
  ParticipantTaskExecutorContext,
  RunParticipantRuntimeInput,
} from "@dungle-scrubs/tether-client";
import type { SessionSummaryGenerationJob, TaskRecord } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it, vi } from "vitest";

import type { EvaluatedSessionSummarySelection } from "../src/executor.js";
import { productionStartupStatus } from "../src/production-selection.js";
import { SessionSummaryWorkerRuntime } from "../src/runtime.js";

const config = {
  afterSeq: 0,
  authToken: "secret-token-value",
  claimRefreshMs: 1_000,
  concurrency: 2,
  displayName: "Summary Worker",
  instanceId: "sensitive-instance-id",
  participantId: "sensitive-participant-id",
  queueSize: 3,
  serviceUrl: "http://tether.test",
  sessionId: "sensitive-session-id",
} as const;

describe("SessionSummaryWorkerRuntime", () => {
  it("rejects unbounded concurrency and queue configuration", () => {
    expect(
      () =>
        new SessionSummaryWorkerRuntime({
          config: { ...config, concurrency: 65, queueSize: 1_025 },
          executor: async () => ({ result: {} }),
          selection: { reason: "hard_gates_not_passed", status: "disabled" },
        }),
    ).toThrow(TypeError);
  });

  it("does not start the participant runtime while the evaluated selection is disabled", async () => {
    const runner = vi.fn(async (_input: RunParticipantRuntimeInput) => undefined);
    const runtime = new SessionSummaryWorkerRuntime({
      config,
      executor: async () => ({ result: {} }),
      runner,
      selection: { reason: "hard_gates_not_passed", status: "disabled" },
    });

    await expect(runtime.run()).resolves.toEqual({
      reason: "hard_gates_not_passed",
      status: "disabled",
    });
    expect(runner).not.toHaveBeenCalled();
    expect(productionStartupStatus()).toEqual({
      reason: "hard_gates_not_passed",
      status: "disabled",
    });
  });

  it("delegates task lifecycle policy to the shared client runner", async () => {
    const received: RunParticipantRuntimeInput[] = [];
    const runtime = new SessionSummaryWorkerRuntime({
      config,
      executor: async () => ({ result: {} }),
      runner: async (input) => {
        received.push(input);
      },
      selection: {
        candidate: {
          identity: {
            contextSize: 1,
            model: "model",
            quantization: "q",
            revision: "revision",
            thinkingMode: "disabled",
          },
          outputSchemaVersion: "v1",
          promptVersion: "v1",
        },
        status: "enabled",
      },
    });

    await expect(runtime.run()).resolves.toEqual({ status: "stopped" });
    expect(received[0]).toMatchObject({
      claimRefreshMs: 1_000,
      participantId: config.participantId,
      workKinds: ["session_summary_generation"],
    });
    expect(received[0]?.executor).toBeTypeOf("function");
    expect(received[0]?.shouldClaimTask).toBeTypeOf("function");
  });

  it("declines claims while the pool budget is fully reserved so tasks stay claimable", async () => {
    let captured: RunParticipantRuntimeInput | undefined;
    const runtime = new SessionSummaryWorkerRuntime({
      config: { ...config, concurrency: 1, queueSize: 1 },
      executor: async () => ({ result: {} }),
      runner: async (input) => {
        captured = input;
      },
      selection: enabledSelection(),
    });
    await runtime.run();
    const shouldClaimTask = captured?.shouldClaimTask;
    if (!shouldClaimTask) {
      throw new Error("expected a claim selector");
    }

    expect(shouldClaimTask(claimableTask("task-invalid", {}))).toBe(false);
    expect(shouldClaimTask(claimableTask("task-1"))).toBe(true);
    expect(shouldClaimTask(claimableTask("task-2"))).toBe(true);
    expect(shouldClaimTask(claimableTask("task-3"))).toBe(false);
    expect(runtime.debugInfo()).toMatchObject({ rejected: 1, reserved: 2 });
  });

  it("frees reserved pool capacity once a claimed task executes", async () => {
    let captured: RunParticipantRuntimeInput | undefined;
    const runtime = new SessionSummaryWorkerRuntime({
      config: { ...config, concurrency: 1, queueSize: 1 },
      executor: async () => ({ result: {} }),
      runner: async (input) => {
        captured = input;
      },
      selection: enabledSelection(),
    });
    await runtime.run();
    const shouldClaimTask = captured?.shouldClaimTask;
    const executor = captured?.executor;
    if (!shouldClaimTask || !executor) {
      throw new Error("expected a claim selector and executor");
    }

    expect(shouldClaimTask(claimableTask("task-1"))).toBe(true);
    expect(shouldClaimTask(claimableTask("task-2"))).toBe(true);
    expect(shouldClaimTask(claimableTask("task-3"))).toBe(false);

    await executor(createExecutorContext("task-1"));

    expect(shouldClaimTask(claimableTask("task-3"))).toBe(true);
    expect(runtime.debugInfo()).toMatchObject({ reserved: 2, succeeded: 1 });
  });

  it("keeps debug state free of credentials and identifiers", async () => {
    const runtime = new SessionSummaryWorkerRuntime({
      config,
      executor: async () => ({ result: {} }),
      runner: async (input) => {
        await input.executor(createExecutorContext());
      },
      selection: {
        candidate: {
          identity: {
            contextSize: 1,
            model: "model",
            quantization: "q",
            revision: "revision",
            thinkingMode: "disabled",
          },
          outputSchemaVersion: "v1",
          promptVersion: "v1",
        },
        status: "enabled",
      },
    });

    await runtime.run();
    const serialized = JSON.stringify(runtime.debugInfo());
    expect(serialized).not.toContain(config.authToken);
    expect(serialized).not.toContain(config.sessionId);
    expect(serialized).not.toContain(config.participantId);
    expect(serialized).not.toContain(config.instanceId);
    expect(runtime.debugInfo()).toMatchObject({ active: 0, started: 1, succeeded: 1 });
  });
});

function createExecutorContext(taskId = "sensitive-task-id"): ParticipantTaskExecutorContext {
  return {
    controlEpoch: 1,
    instanceId: config.instanceId,
    participantId: config.participantId,
    publishOutput: async () => undefined,
    publishProgress: async () => undefined,
    recentEvents: [],
    sessionId: config.sessionId,
    signal: new AbortController().signal,
    task: claimableTask(taskId, {}),
  };
}

function enabledSelection(): EvaluatedSessionSummarySelection {
  return {
    candidate: {
      identity: {
        contextSize: 1,
        model: "model",
        quantization: "q",
        revision: "revision",
        thinkingMode: "disabled",
      },
      outputSchemaVersion: "v1",
      promptVersion: "v1",
    },
    status: "enabled",
  };
}

function claimableTask(taskId: string, input?: Record<string, unknown>): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-07-17T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: input ?? { ...generationJob(taskId) },
    kind: "session_summary_generation",
    objective: "test",
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: config.sessionId,
    taskId,
  };
}

function generationJob(taskId: string): SessionSummaryGenerationJob {
  return {
    budgetClass: "standard",
    deadlineAt: "2099-07-17T00:00:00.000Z",
    expectedPrevious: { coversSeqTo: null, summaryId: null },
    inputLimitBytes: 64_000,
    kind: "session_summary.generate.v1",
    ollama: {
      contextSize: 32_768,
      model: "evaluated-model",
      quantization: "Q4_K_M",
      revision: `sha256:${"a".repeat(64)}`,
      thinkingMode: "disabled",
    },
    outputLimitBytes: 64_000,
    outputSchemaVersion: "session-summary.v1",
    previousSummary: null,
    producer: { id: "session-summary-worker", version: "1" },
    promptVersion: "session-summary.v1",
    range: { from: 10, to: 11 },
    sessionId: config.sessionId,
    source: {
      eventCount: 2,
      firstEventId: "event-10",
      lastEventId: "event-11",
      rangeHash: "b".repeat(64),
    },
    summaryId: "summary-1",
    taskId,
  };
}
