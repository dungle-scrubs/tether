import { describe, expect, it } from "vitest";

import type { CandidateScheduleIdentity, TaskRecord } from "../src/index.js";
import { parseWebSocketServerEnvelope } from "../src/index.js";

describe("WebSocket task command results", () => {
  it("preserves scheduled task identity", () => {
    const task = createScheduledTaskFixture();
    const parsed = parseWebSocketServerEnvelope({
      command: "task.claim",
      op: "command.result",
      requestId: "req_1",
      task,
    });

    expect(parsed).toMatchObject({
      command: "task.claim",
      op: "command.result",
      task: { schedule: task.schedule },
    });
  });

  it("returns null for a malformed scheduled task", () => {
    const task = createScheduledTaskFixture();
    const schedule = createScheduleFixture();

    expect(
      parseWebSocketServerEnvelope({
        command: "task.claim",
        op: "command.result",
        task: {
          ...task,
          schedule: {
            ...schedule,
            scheduleWindow: {
              ...schedule.scheduleWindow,
              endMs: schedule.scheduleWindow.endMs + 1,
            },
          },
        },
      }),
    ).toBeNull();
  });
});

/** Builds a complete durable schedule identity fixture. */
function createScheduleFixture(): CandidateScheduleIdentity {
  return {
    identityVersion: 2,
    scheduleWindow: {
      algorithmVersion: 1,
      endMs: 1_700_002_800_000,
      intervalMs: 3_600_000,
      startMs: 1_699_999_200_000,
    },
    scopeKey: "scope_01JEMAIL",
  };
}

/** Builds a complete scheduled durable task fixture. */
function createScheduledTaskFixture(): TaskRecord {
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
    kind: "email_organization",
    objective: "organize mailbox",
    releasedAt: null,
    releasedBy: null,
    result: null,
    schedule: createScheduleFixture(),
    sessionId: "sess_1",
    taskId: "task_1",
  };
}
