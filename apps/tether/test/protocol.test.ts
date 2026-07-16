import { describe, expect, it } from "vitest";

import {
  approvalDecisionSchema,
  buildTaskApprovalRecordedEventInput,
  buildTaskCancelledEventInput,
  buildTaskClaimExpiredEventInput,
  buildTaskCreatedEventInput,
  buildTaskReleasedEventInput,
  buildWsPublishMessage,
  buildWsTaskCancelMessage,
  buildWsTaskClaimMessage,
  buildWsTaskRefreshMessage,
  cancelTaskSchema,
  completeTaskSchema,
  controlChannelSchema,
  eventListResponseSchema,
  failTaskSchema,
  heartbeatParticipantSchema,
  parseAfterSeq,
  parseWebSocketServerEnvelope,
  refreshTaskClaimSchema,
  registerParticipantSchema,
  releaseTaskSchema,
  serializeCommandResultEnvelope,
  serializeErrorEnvelope,
  serializeEventEnvelope,
  taskFromClaimableEvent,
  taskFromCreatedEvent,
  taskIdFromCancelledEvent,
  wsPublishMessageSchema,
  wsTaskCancelMessageSchema,
  wsTaskClaimMessageSchema,
  wsTaskCompleteMessageSchema,
  wsTaskRefreshMessageSchema,
} from "../src/protocol.js";
import type { SessionEvent, TaskRecord } from "../src/types.js";

const baseTask: TaskRecord = {
  cancelledAt: null,
  claimExpiredAt: null,
  claimExpiredBy: null,
  claimExpiresAt: null,
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
  sessionId: "sess_test",
  taskId: "task_test",
};

const scheduledTask: TaskRecord = {
  ...baseTask,
  schedule: {
    mailboxScope: { accountId: "acct_opaque_1", provider: "fastmail" },
    scheduleWindow: {
      algorithmVersion: 1,
      endMs: 1_700_002_800_000,
      intervalMs: 3_600_000,
      startMs: 1_699_999_200_000,
    },
  },
};

describe("parseAfterSeq", () => {
  it("re-exports the shared event cursor parser", () => {
    expect(parseAfterSeq("42")).toBe(42);
  });
});

describe("participant schemas", () => {
  it("defaults unspecified participant runtimes to a generic agent kind", () => {
    const parsed = registerParticipantSchema.parse({
      displayName: "Generic Runtime",
    });

    expect(parsed.capabilities).toEqual({});
    expect(parsed.runtimeKind).toBe("generic_agent");
  });

  it("accepts public and compatibility participant runtimes", () => {
    for (const runtimeKind of [
      "claude_code",
      "codex",
      "generic_agent",
      "pi_coding_agent",
      "openai_agent",
    ]) {
      expect(
        registerParticipantSchema.parse({
          capabilities: { workKinds: ["software_dev"] },
          participantId: `part_${runtimeKind}`,
          runtimeKind,
        }).runtimeKind,
      ).toBe(runtimeKind);
    }
  });

  it("keeps heartbeat capabilities optional", () => {
    expect(heartbeatParticipantSchema.parse({})).toEqual({});
  });

  it("defaults REST registration to the REST control channel", () => {
    expect(registerParticipantSchema.parse({ runtimeKind: "generic_agent" }).controlChannel).toBe(
      "rest",
    );
    expect(controlChannelSchema.parse("ws")).toBe("ws");
  });
});

describe("task lifecycle schemas", () => {
  it("defaults cancellation, completion, and failure payloads to empty records", () => {
    expect(cancelTaskSchema.parse({ participantId: "part_codex" }).reason).toEqual({});
    expect(completeTaskSchema.parse({ participantId: "part_codex" }).result).toEqual({});
    expect(failTaskSchema.parse({ participantId: "part_codex" }).failure).toEqual({});
  });

  it("requires a participant to refresh a task claim", () => {
    expect(refreshTaskClaimSchema.parse({ participantId: "part_codex" })).toEqual({
      participantId: "part_codex",
    });
  });

  it("requires a participant to release a task", () => {
    expect(releaseTaskSchema.parse({ participantId: "part_codex" })).toEqual({
      participantId: "part_codex",
    });
  });
});

describe("websocket task command schemas", () => {
  it("accepts task claim, refresh, cancellation, and completion commands", () => {
    expect(wsTaskClaimMessageSchema.parse({ op: "task.claim", taskId: "task_1" })).toEqual({
      op: "task.claim",
      taskId: "task_1",
    });
    expect(wsTaskRefreshMessageSchema.parse({ op: "task.refresh", taskId: "task_1" })).toEqual({
      op: "task.refresh",
      taskId: "task_1",
    });
    expect(wsTaskCancelMessageSchema.parse({ op: "task.cancel", taskId: "task_1" })).toEqual({
      op: "task.cancel",
      reason: {},
      taskId: "task_1",
    });
    expect(
      wsTaskCompleteMessageSchema.parse({
        op: "task.complete",
        result: { output: "done" },
        taskId: "task_1",
      }).result,
    ).toEqual({ output: "done" });
  });

  it("builds task claim and refresh command messages", () => {
    expect(buildWsTaskClaimMessage({ requestId: "req_1", taskId: "task_1" })).toEqual({
      op: "task.claim",
      requestId: "req_1",
      taskId: "task_1",
    });
    expect(buildWsTaskRefreshMessage({ requestId: "req_2", taskId: "task_1" })).toEqual({
      op: "task.refresh",
      requestId: "req_2",
      taskId: "task_1",
    });
  });

  it("builds task cancel command messages", () => {
    expect(
      buildWsTaskCancelMessage({
        reason: { message: "obsolete" },
        requestId: "req_1",
        taskId: "task_1",
      }),
    ).toEqual({
      op: "task.cancel",
      reason: { message: "obsolete" },
      requestId: "req_1",
      taskId: "task_1",
    });
  });

  it("builds publish messages with request ids for durable append acknowledgement", () => {
    const message = buildWsPublishMessage({
      payload: { output: "done", taskId: "task_1" },
      producerId: "part_worker",
      requestId: "req_publish",
      type: "agent.output",
    });

    expect(wsPublishMessageSchema.parse(message)).toEqual({
      op: "publish",
      payload: { output: "done", taskId: "task_1" },
      producerId: "part_worker",
      requestId: "req_publish",
      type: "agent.output",
    });
  });
});

describe("protocol event builders and envelopes", () => {
  it("preserves correlated error request ids and bounded diagnostics", () => {
    const parsed = parseWebSocketServerEnvelope(
      JSON.parse(
        serializeErrorEnvelope({
          details: {
            category: "command_processing_failed",
            command: "task.claim",
            taskId: "task_protocol",
          },
          error: "Claim failed",
          requestId: "req_protocol_error",
        }),
      ) as unknown,
    );

    expect(parsed).toMatchObject({
      category: "command_processing_failed",
      command: "task.claim",
      error: "Claim failed",
      op: "error",
      requestId: "req_protocol_error",
      taskId: "task_protocol",
    });
  });

  it("builds and reads task-created event payloads", () => {
    const eventInput = buildTaskCreatedEventInput({
      sessionId: "sess_test",
      task: baseTask,
    });
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:00:00.000Z",
      eventId: eventInput.eventId,
      payload: eventInput.payload,
      producerId: eventInput.producerId,
      seq: 2,
      sessionId: eventInput.sessionId,
      type: eventInput.type,
    };

    expect(eventInput.producerId).toBe("tether");
    expect(eventInput.type).toBe("task.created");
    expect(taskFromCreatedEvent(event)?.taskId).toBe("task_test");
  });

  it("preserves scheduled task identity through serialization, parsing, and extraction", () => {
    const eventInput = buildTaskCreatedEventInput({
      sessionId: "sess_test",
      task: scheduledTask,
    });
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:00:00.000Z",
      eventId: eventInput.eventId,
      payload: eventInput.payload,
      producerId: eventInput.producerId,
      seq: 2,
      sessionId: eventInput.sessionId,
      type: eventInput.type,
    };
    const envelope = parseWebSocketServerEnvelope(JSON.parse(serializeEventEnvelope(event)));

    expect(envelope?.op).toBe("event");
    if (envelope?.op !== "event") {
      throw new Error("Expected a parsed WebSocket event envelope");
    }
    expect(taskFromCreatedEvent(envelope.event)?.schedule).toEqual(scheduledTask.schedule);
  });

  it("builds task cancellation events", () => {
    const eventInput = buildTaskCancelledEventInput({
      participantId: "part_codex",
      reason: { message: "obsolete" },
      sessionId: "sess_test",
      task: { ...baseTask, cancelledAt: "2026-05-21T00:00:00.000Z" },
    });

    expect(eventInput.producerId).toBe("tether");
    expect(eventInput.type).toBe("control.cancel");
    expect(eventInput.payload).toMatchObject({
      participantId: "part_codex",
      reason: { message: "obsolete" },
      task: { taskId: "task_test" },
    });
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:00:00.000Z",
      eventId: eventInput.eventId,
      payload: eventInput.payload,
      producerId: eventInput.producerId,
      seq: 3,
      sessionId: eventInput.sessionId,
      type: eventInput.type,
    };

    expect(taskIdFromCancelledEvent(event)).toBe("task_test");
  });

  it("builds task approval recorded events", () => {
    expect(approvalDecisionSchema.parse("approved")).toBe("approved");
    const eventInput = buildTaskApprovalRecordedEventInput({
      decision: "approved",
      participantId: "part_external_bridge",
      reason: { source: "external-chat" },
      sessionId: "sess_test",
      task: baseTask,
    });

    expect(eventInput.producerId).toBe("tether");
    expect(eventInput.type).toBe("approval.recorded");
    expect(eventInput.payload).toMatchObject({
      decision: "approved",
      participantId: "part_external_bridge",
      reason: { source: "external-chat" },
      task: { taskId: "task_test" },
    });
  });

  it("builds and reads task claim-expired events", () => {
    const releasedTask = {
      ...baseTask,
      releasedAt: "2026-05-21T00:01:00.000Z",
    };
    const eventInput = buildTaskClaimExpiredEventInput({
      previousClaimedBy: "part_stale",
      sessionId: "sess_test",
      task: releasedTask,
    });
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:01:00.000Z",
      eventId: eventInput.eventId,
      payload: eventInput.payload,
      producerId: eventInput.producerId,
      seq: 4,
      sessionId: eventInput.sessionId,
      type: eventInput.type,
    };

    expect(eventInput.type).toBe("task.claim_expired");
    expect(eventInput.payload).toMatchObject({
      previousClaimedBy: "part_stale",
      task: { taskId: "task_test" },
    });
    expect(taskFromClaimableEvent(event)?.taskId).toBe("task_test");
  });

  it("builds and reads task released events as claimable", () => {
    const releasedTask = {
      ...baseTask,
      releasedAt: "2026-05-21T00:01:00.000Z",
      releasedBy: "part_stale",
    };
    const eventInput = buildTaskReleasedEventInput({
      participantId: "part_stale",
      sessionId: "sess_test",
      task: releasedTask,
    });
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:01:00.000Z",
      eventId: eventInput.eventId,
      payload: eventInput.payload,
      producerId: eventInput.producerId,
      seq: 5,
      sessionId: eventInput.sessionId,
      type: eventInput.type,
    };

    expect(eventInput.type).toBe("task.released");
    expect(eventInput.payload).toMatchObject({
      participantId: "part_stale",
      task: { taskId: "task_test" },
    });
    expect(taskFromClaimableEvent(event)?.taskId).toBe("task_test");
  });

  it("serializes and parses websocket envelopes", () => {
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:00:00.000Z",
      eventId: "evt_test",
      payload: { text: "hello" },
      producerId: "producer-1",
      seq: 1,
      sessionId: "sess_test",
      type: "user.message",
    };

    expect(parseWebSocketServerEnvelope(JSON.parse(serializeEventEnvelope(event)))?.op).toBe(
      "event",
    );
    expect(
      parseWebSocketServerEnvelope(
        JSON.parse(
          serializeCommandResultEnvelope({
            command: "task.claim",
            payload: { task: baseTask },
            requestId: "req_1",
          }),
        ),
      ),
    ).toMatchObject({ command: "task.claim", op: "command.result", requestId: "req_1" });
  });
});

describe("event list REST response schema", () => {
  it("accepts bounded event pages with explicit pagination metadata", () => {
    const event: SessionEvent = {
      createdAt: "2026-05-21T00:00:00.000Z",
      eventId: "evt_page_1",
      payload: { text: "hello" },
      producerId: "producer-1",
      seq: 7,
      sessionId: "sess_test",
      type: "user.message",
    };

    expect(
      eventListResponseSchema.parse({
        events: [event],
        pagination: {
          afterSeq: 6,
          hasMore: false,
          limit: 500,
          nextAfterSeq: 7,
          returned: 1,
        },
      }),
    ).toEqual({
      events: [event],
      pagination: {
        afterSeq: 6,
        hasMore: false,
        limit: 500,
        nextAfterSeq: 7,
        returned: 1,
      },
    });
  });
});
