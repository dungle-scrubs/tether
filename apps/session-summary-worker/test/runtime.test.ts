import type {
  ParticipantTaskExecutorContext,
  RunParticipantRuntimeInput,
} from "@dungle-scrubs/tether-client";
import { describe, expect, it, vi } from "vitest";

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

function createExecutorContext(): ParticipantTaskExecutorContext {
  return {
    controlEpoch: 1,
    instanceId: config.instanceId,
    participantId: config.participantId,
    publishOutput: async () => undefined,
    publishProgress: async () => undefined,
    recentEvents: [],
    sessionId: config.sessionId,
    signal: new AbortController().signal,
    task: {
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
      input: {},
      kind: "session_summary_generation",
      objective: "test",
      releasedAt: null,
      releasedBy: null,
      result: null,
      sessionId: config.sessionId,
      taskId: "sensitive-task-id",
    },
  };
}
