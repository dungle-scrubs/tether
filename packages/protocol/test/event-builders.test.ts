import { describe, expect, it } from "vitest";

import type { CandidateScheduleIdentity, SessionEvent, TaskRecord } from "../src/index.js";
import {
  buildTaskApprovalRecordedEventInput,
  parseAfterSeq,
  taskApprovalRecordedPayloadSchema,
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
  it("carries the canonical approval record in new approval events", () => {
    const task = createScheduledTaskFixture();
    const approval = {
      approvalEventId: "evt_approval_1",
      decidedAt: "2026-01-01T00:01:00.000Z",
      decidedByParticipantId: "operator_1",
      decision: "approved" as const,
      reason: {},
      sessionId: task.sessionId,
      targetKey: "approvalTarget:v2:sha256:opaque",
      taskId: task.taskId,
    };
    const event = buildTaskApprovalRecordedEventInput({
      approval,
      decision: approval.decision,
      eventId: approval.approvalEventId,
      participantId: approval.decidedByParticipantId,
      reason: approval.reason,
      sessionId: task.sessionId,
      task,
    });

    expect(taskApprovalRecordedPayloadSchema.parse(event.payload).approval).toEqual(approval);
    expect(event.eventId).toBe(approval.approvalEventId);
  });

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
