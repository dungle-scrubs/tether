import { describe, expect, it } from "vitest";

import {
  ParticipantRuntimeClient,
  runParticipantRuntime,
  shouldClaimParticipantTask,
  type ParticipantTaskExecutor,
  type ParticipantTaskExecutorContext,
  type RunParticipantRuntimeInput,
  type TaskRecord,
} from "../src/client.js";

const adapterTask: TaskRecord = {
  cancelledAt: null,
  claimExpiredAt: null,
  claimExpiredBy: null,
  claimExpiresAt: null,
  claimId: null,
  claimedAt: null,
  claimedBy: null,
  completedAt: null,
  createdAt: "2026-05-21T00:00:00.000Z",
  failedAt: null,
  failure: null,
  input: null,
  kind: "text",
  objective: "Handle a public client adapter task",
  releasedAt: null,
  releasedBy: null,
  result: null,
  sessionId: "sess_public_client",
  taskId: "task_public_client",
};

describe("public client adapter surface", () => {
  it("exports the high-level runner and lower-level client from one boundary", () => {
    expect(typeof runParticipantRuntime).toBe("function");
    expect(typeof ParticipantRuntimeClient.connect).toBe("function");
  });

  it("lets mock adapters define task policy without worker-internal imports", async () => {
    const executor: ParticipantTaskExecutor = async ({ signal, task }) => {
      expect(signal.aborted).toBe(false);
      return {
        output: `handled ${task.taskId}`,
        result: { output: `handled ${task.taskId}` },
      };
    };
    const signal = new AbortController().signal;
    const input: RunParticipantRuntimeInput = {
      afterSeq: 0,
      capabilities: { status: "listening", workKinds: ["text"] },
      claimRefreshMs: 1_000,
      displayName: "Public Client Adapter",
      executor,
      instanceId: "inst_public_client",
      participantId: "part_public_client",
      runtimeKind: "generic_agent",
      serviceUrl: "http://localhost:3025",
      sessionId: "sess_public_client",
      shouldClaimTask: (task) => shouldClaimParticipantTask(task, ["text"], "part_public_client"),
      workKinds: ["text"],
    };

    expect(input.shouldClaimTask?.(adapterTask)).toBe(true);
    await expect(input.executor(createExecutorContext(signal))).resolves.toEqual({
      output: "handled task_public_client",
      result: { output: "handled task_public_client" },
    });
  });
});

/**
 * Builds the minimal executor context external adapters receive from the public
 * client runtime.
 */
function createExecutorContext(signal: AbortSignal): ParticipantTaskExecutorContext {
  return {
    instanceId: "inst_public_client",
    participantId: "part_public_client",
    recentEvents: [],
    publishOutput: async () => undefined,
    publishProgress: async () => undefined,
    sessionId: adapterTask.sessionId,
    signal,
    task: adapterTask,
  };
}
