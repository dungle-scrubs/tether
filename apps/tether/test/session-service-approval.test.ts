import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type {
  PersistedTaskApprovalResult,
  SessionPersistenceStores,
} from "../src/db-store-contracts.js";
import { ApprovalTargetManifestError } from "../src/db.js";
import { ModuleObservability, type StructuredLogEntry } from "../src/observability.js";
import {
  createSessionTaskEffects,
  mapTaskApprovalRejection,
} from "../src/session-service-task-effects.js";
import type { ApprovalTarget, SessionEvent, TaskRecord } from "../src/types.js";

describe("session service approval recording", () => {
  it("delegates duplicate detection to TaskStore.recordApproval without listing events", async () => {
    const task = createCompletedApprovalTask();
    const event = createApprovalEvent(task);
    const logs: StructuredLogEntry[] = [];
    const stores = createApprovalStores(task, {
      approval: createApprovalRecord(task),
      decision: "approved",
      event,
      events: [event],
      status: "recorded",
      targetKey: "task",
      task,
    });
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: () => undefined,
      eventSourceId: "src_service_approval_test",
      observability: new ModuleObservability({
        debugEnabled: true,
        logger: { log: (entry) => logs.push(entry) },
        moduleName: "SessionServiceApprovalTest",
      }),
      stores,
      taskClaimLeaseTtlMs: 1_000,
    });

    const result = await Effect.runPromise(
      mapTaskApprovalRejection(
        effects.recordTaskApprovalEffect({
          decision: "approved",
          participantId: "part_service_approval_test",
          reason: {},
          sessionId: task.sessionId,
          taskId: task.taskId,
        }),
      ),
    );

    expect(result.status).toBe("recorded");
    expect(result.events).toHaveLength(1);
    expect(logs).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          approvalEventId: event.eventId,
          resultStatus: "recorded",
          targetKey: "task",
        }),
        message: "approval.recording.result",
      }),
    );
  });

  it("reports ignored approval diagnostics without leaking persistence errors", async () => {
    const task = createCompletedApprovalTask();
    const logs: StructuredLogEntry[] = [];
    const stores = createApprovalStores(task, {
      approval: {
        approvalEventId: "evt_existing_service_approval",
        decidedAt: new Date().toISOString(),
        decidedByParticipantId: "part_first_service_approval_test",
        decision: "approved",
        reason: {},
        sessionId: task.sessionId,
        targetKey: "task",
        taskId: task.taskId,
      },
      decision: "rejected",
      events: [],
      existingDecision: "approved",
      status: "ignored",
      targetKey: "task",
      task,
    });
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: () => undefined,
      eventSourceId: "src_service_approval_test",
      observability: new ModuleObservability({
        debugEnabled: true,
        logger: { log: (entry) => logs.push(entry) },
        moduleName: "SessionServiceApprovalTest",
      }),
      stores,
      taskClaimLeaseTtlMs: 1_000,
    });

    const result = await Effect.runPromise(
      mapTaskApprovalRejection(
        effects.recordTaskApprovalEffect({
          decision: "rejected",
          participantId: "part_service_approval_test",
          reason: {},
          sessionId: task.sessionId,
          taskId: task.taskId,
        }),
      ),
    );

    expect(result).toMatchObject({
      approval: {
        decidedByParticipantId: "part_first_service_approval_test",
        decision: "approved",
      },
      existingDecision: "approved",
      ignoredReason: "already_approved",
      status: "ignored",
    });
    expect(logs).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          existingDecision: "approved",
          resultStatus: "ignored",
          targetKey: "task",
        }),
        message: "approval.recording.result",
      }),
    );
  });

  it("passes the submitted opaque target into the atomic approval store operation", async () => {
    const task = createCompletedApprovalTask();
    const event = createApprovalEvent(task);
    const target: ApprovalTarget = {
      action: "action_opaque_1",
      digest: "digest_opaque_1",
      scopeKey: "scope_opaque_1",
      targetId: "target_opaque_1",
      targetKind: "kind_opaque_1",
      targetRevision: "revision_opaque_1",
    };
    let persistedTarget: ApprovalTarget | undefined;
    const stores = createApprovalStores(
      task,
      {
        approval: createApprovalRecord(task),
        decision: "approved",
        event,
        events: [event],
        status: "recorded",
        targetKey: "target_key",
        task,
      },
      (recordedTarget) => {
        persistedTarget = recordedTarget;
      },
    );
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: () => undefined,
      eventSourceId: "src_service_approval_test",
      observability: new ModuleObservability({ moduleName: "SessionServiceApprovalTest" }),
      stores,
      taskClaimLeaseTtlMs: 1_000,
    });

    await Effect.runPromise(
      effects.recordTaskApprovalEffect({
        decision: "approved",
        participantId: "part_service_approval_test",
        reason: {},
        sessionId: task.sessionId,
        target,
        taskId: task.taskId,
      }),
    );

    expect(persistedTarget).toEqual(target);
  });

  it("maps atomic target-manifest refusal to a typed approval rejection", async () => {
    const task = createCompletedApprovalTask();
    const stores = createApprovalStores(
      task,
      null as never,
      () => undefined,
      new ApprovalTargetManifestError("digest_mismatch"),
    );
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: () => undefined,
      eventSourceId: "src_service_approval_test",
      observability: new ModuleObservability({ moduleName: "SessionServiceApprovalTest" }),
      stores,
      taskClaimLeaseTtlMs: 1_000,
    });

    const result = await Effect.runPromise(
      mapTaskApprovalRejection(
        effects.recordTaskApprovalEffect({
          decision: "approved",
          participantId: "part_service_approval_test",
          reason: {},
          sessionId: task.sessionId,
          target: {
            action: "action_opaque_1",
            digest: "digest_wrong",
            scopeKey: "scope_opaque_1",
            targetId: "target_opaque_1",
            targetKind: "kind_opaque_1",
            targetRevision: "revision_opaque_1",
          },
          taskId: task.taskId,
        }),
      ),
    );

    expect(result).toMatchObject({ rejectionReason: "digest_mismatch", status: "rejected" });
  });
});

function createApprovalStores(
  task: TaskRecord,
  approvalResult: PersistedTaskApprovalResult,
  onRecordApproval: (target: ApprovalTarget | undefined) => void = () => undefined,
  recordApprovalError?: Error | undefined,
): SessionPersistenceStores {
  return {
    clientBindings: {
      archive: async () => null,
      find: async () => null,
      list: async () => [],
      upsert: async () => {
        throw new Error("unexpected client binding upsert");
      },
    },
    controlLeases: {
      claim: async () => {
        throw new Error("unexpected control lease claim");
      },
      listSnapshots: async () => [],
      renew: async () => {
        throw new Error("unexpected control lease renew");
      },
      release: async () => undefined,
      releaseRest: async () => {
        throw new Error("approval service should not release REST control");
      },
    },
    events: {
      append: async () => {
        throw new Error("approval service should not append directly");
      },
      appendIdempotent: async () => {
        throw new Error("approval service should not append idempotently");
      },
      list: async () => {
        throw new Error("approval service should not list events for duplicate detection");
      },
      listContextSuffix: async () => {
        throw new Error("approval service should not list context events");
      },
    },
    participants: {
      heartbeat: async () => null,
      heartbeatWithEvent: async () => ({ participant: null }),
      list: async () => [],
      listRuntimeSnapshots: async () => [],
      upsert: async () => {
        throw new Error("unexpected participant upsert");
      },
      upsertWithEvent: async () => {
        throw new Error("unexpected participant upsert with event");
      },
    },
    sessions: {
      create: async () => ({
        created: true,
        session: {
          createdAt: new Date().toISOString(),
          sessionId: task.sessionId,
        },
      }),
      delete: async () => ({ status: "deleted" }) as const,
      list: async () => [],
      read: async () => ({
        createdAt: new Date().toISOString(),
        sessionId: task.sessionId,
      }),
      readDebugSummary: async () => {
        throw new Error("unexpected debug summary read");
      },
    },
    tasks: {
      cancelWithEvent: async () => null,
      claimWithEvent: async () => null,
      completeWithEvent: async () => null,
      createWithEvent: async () => {
        throw new Error("unexpected task create with event");
      },
      ensureScheduledRun: async () => {
        throw new Error("unexpected ensure scheduled run");
      },
      expireClaims: async () => [],
      failWithEvent: async () => null,
      get: async () => task,
      list: async () => [],
      listSnapshots: async () => [],
      recordApproval: async (input) => {
        onRecordApproval(input.target);
        if (recordApprovalError !== undefined) {
          throw recordApprovalError;
        }
        return approvalResult;
      },
      refreshClaim: async () => task,
      releaseWithEvent: async () => null,
      supersedeScheduled: async () => ({ events: [], tasks: [] }),
    },
  };
}

function createCompletedApprovalTask(): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: new Date().toISOString(),
    createdAt: new Date().toISOString(),
    failedAt: null,
    failure: null,
    input: null,
    kind: "approval_test",
    objective: "record approval",
    releasedAt: null,
    releasedBy: null,
    result: {
      dryRun: {
        approvalSummary: ["Approve test"],
        authorization: "needs_approval",
        request: {},
        target: "test",
      },
      kind: "approval_test",
    },
    sessionId: "sess_service_approval_test",
    taskId: "task_service_approval_test",
  };
}

function createApprovalEvent(task: TaskRecord): SessionEvent {
  return {
    createdAt: new Date().toISOString(),
    eventId: "evt_service_approval_test",
    payload: {
      decision: "approved",
      participantId: "part_service_approval_test",
      reason: {},
      task,
    },
    producerId: "system",
    seq: 1,
    sessionId: task.sessionId,
    type: "approval.recorded",
  };
}

function createApprovalRecord(task: TaskRecord) {
  return {
    approvalEventId: "evt_service_approval_test",
    decidedAt: "2026-08-01T00:00:00.000Z",
    decidedByParticipantId: "part_service_approval_test",
    decision: "approved" as const,
    reason: {},
    sessionId: task.sessionId,
    targetKey: "task",
    taskId: task.taskId,
  };
}
