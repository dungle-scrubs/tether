import { describe, expect, it } from "vitest";

import type { CandidateScheduleIdentity, SessionEvent, TaskRecord } from "../src/index.js";
import {
  parseAfterSeq,
  taskClaimExpiredPayloadSchema,
  taskCreatedPayloadSchema,
  taskFromClaimableEvent,
  taskFromCreatedEvent,
  taskParticipantPayloadSchema,
} from "../src/index.js";

describe("parseAfterSeq", () => {
  it("accepts only positive safe integer cursors", () => {
    expect(parseAfterSeq(undefined)).toBe(0);
    expect(parseAfterSeq("42")).toBe(42);
    expect(parseAfterSeq(Number.MAX_SAFE_INTEGER.toString())).toBe(Number.MAX_SAFE_INTEGER);
    expect(parseAfterSeq((Number.MAX_SAFE_INTEGER + 1).toString())).toBe(0);
    expect(parseAfterSeq("1.5")).toBe(0);
    expect(parseAfterSeq("bad")).toBe(0);
  });
});

describe("task-bearing event payloads", () => {
  it("preserves scheduled task identity in task-created validation and extraction", () => {
    const task = createScheduledTaskFixture();
    const event = createTaskEvent("task.created", { task });

    expect(taskCreatedPayloadSchema.parse(event.payload).task.schedule).toEqual(task.schedule);
    expect(taskFromCreatedEvent(event)?.schedule).toEqual(task.schedule);
  });

  it("preserves scheduled task identity in claim-expired validation and extraction", () => {
    const task = createScheduledTaskFixture();
    const event = createTaskEvent("task.claim_expired", {
      previousClaimedBy: "part_stale",
      task,
    });

    expect(taskClaimExpiredPayloadSchema.parse(event.payload).task.schedule).toEqual(task.schedule);
    expect(taskFromClaimableEvent(event)?.schedule).toEqual(task.schedule);
  });

  it("preserves scheduled task identity in released-task validation and extraction", () => {
    const task = createScheduledTaskFixture();
    const event = createTaskEvent("task.released", {
      participantId: "part_worker",
      task,
    });

    expect(taskParticipantPayloadSchema.parse(event.payload).task.schedule).toEqual(task.schedule);
    expect(taskFromClaimableEvent(event)?.schedule).toEqual(task.schedule);
  });

  it("returns null for malformed scheduled tasks in task-bearing events", () => {
    const task = createScheduledTaskFixture();
    const schedule = createScheduleFixture();
    const malformedTask = {
      ...task,
      schedule: {
        ...schedule,
        scheduleWindow: {
          ...schedule.scheduleWindow,
          endMs: schedule.scheduleWindow.endMs + 1,
        },
      },
    };

    expect(
      taskFromCreatedEvent(createTaskEvent("task.created", { task: malformedTask })),
    ).toBeNull();
    expect(
      taskFromClaimableEvent(
        createTaskEvent("task.claim_expired", {
          previousClaimedBy: "part_stale",
          task: malformedTask,
        }),
      ),
    ).toBeNull();
    expect(
      taskFromClaimableEvent(
        createTaskEvent("task.released", {
          participantId: "part_worker",
          task: malformedTask,
        }),
      ),
    ).toBeNull();
  });
});

/** Builds a complete durable schedule identity fixture. */
function createScheduleFixture(): CandidateScheduleIdentity {
  return {
    mailboxScope: { accountId: "acct_opaque_1", provider: "fastmail" },
    scheduleWindow: {
      algorithmVersion: 1,
      endMs: 1_700_002_800_000,
      intervalMs: 3_600_000,
      startMs: 1_699_999_200_000,
    },
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

/** Builds a public task-bearing session event fixture. */
function createTaskEvent(type: string, payload: Record<string, unknown>): SessionEvent {
  return {
    createdAt: "2026-01-01T00:00:00.000Z",
    eventId: "evt_1",
    payload,
    producerId: "tether",
    seq: 1,
    sessionId: "sess_1",
    type,
  };
}
