import { describe, expect, it } from "vitest";

import {
  buildParticipantRuntimeStreamUrl,
  shouldClaimParticipantTask,
  TaskCancellationRegistry,
  type ParticipantRuntimeClientConfig,
  type SessionEvent,
  type TaskRecord,
} from "../src/client.js";

const baseConfig: ParticipantRuntimeClientConfig = {
  afterSeq: 12,
  capabilities: { model: "gpt-5.4-nano", status: "listening", workKinds: ["text"] },
  displayName: "Runtime Client",
  instanceId: "inst_runtime_client",
  participantId: "part_runtime_client",
  runtimeKind: "generic_agent",
  serviceUrl: "https://tether.test",
  sessionId: "sess_runtime_client",
};

const baseTask: TaskRecord = {
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
  objective: "Summarize the session",
  releasedAt: null,
  releasedBy: null,
  result: null,
  sessionId: "sess_runtime_client",
  taskId: "task_runtime_client",
};

describe("participant runtime client", () => {
  it("builds a resumable WebSocket stream URL with participant identity", () => {
    const streamUrl = new URL(buildParticipantRuntimeStreamUrl(baseConfig));
    const capabilities = JSON.parse(streamUrl.searchParams.get("capabilities") ?? "{}") as unknown;

    expect(streamUrl.protocol).toBe("wss:");
    expect(streamUrl.pathname).toBe("/sessions/sess_runtime_client/stream");
    expect(streamUrl.searchParams.get("after")).toBe("12");
    expect(streamUrl.searchParams.get("displayName")).toBe("Runtime Client");
    expect(streamUrl.searchParams.get("instanceId")).toBe("inst_runtime_client");
    expect(streamUrl.searchParams.get("participantId")).toBe("part_runtime_client");
    expect(streamUrl.searchParams.get("runtimeKind")).toBe("generic_agent");
    expect(capabilities).toEqual(baseConfig.capabilities);
  });

  it("tracks cancelled tasks and aborts active work", () => {
    const registry = new TaskCancellationRegistry();
    const signal = registry.begin("task_runtime_client");
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:00:00.000Z",
      eventId: "evt_cancel",
      payload: { participantId: "part_controller", reason: {}, task: baseTask },
      producerId: "tether",
      seq: 2,
      sessionId: "sess_runtime_client",
      type: "control.cancel",
    };

    expect(registry.observe(event)).toBe("task_runtime_client");
    expect(registry.isCancelled("task_runtime_client")).toBe(true);
    expect(signal.aborted).toBe(true);
  });

  it("selects claimable tasks by work kind and participant ownership", () => {
    expect(shouldClaimParticipantTask(baseTask, ["text"], "part_runtime_client")).toBe(true);
    expect(shouldClaimParticipantTask(baseTask, ["software_dev"], "part_runtime_client")).toBe(
      false,
    );
    expect(
      shouldClaimParticipantTask(
        { ...baseTask, claimedBy: "part_other" },
        ["text"],
        "part_runtime_client",
      ),
    ).toBe(false);
    expect(
      shouldClaimParticipantTask(
        { ...baseTask, claimedBy: "part_runtime_client" },
        ["text"],
        "part_runtime_client",
      ),
    ).toBe(true);
    expect(
      shouldClaimParticipantTask(
        { ...baseTask, completedAt: "2026-05-21T00:01:00.000Z" },
        ["text"],
        "part_runtime_client",
      ),
    ).toBe(false);
  });
});
