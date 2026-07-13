import { z } from "zod";
import type { ParticipantRecord, SessionEvent, SessionEventType, TaskRecord } from "./records.js";
import { sessionEventType, systemProducerId, taskRecordSchema } from "./records.js";
import { approvalDecisionSchema } from "./rest-schemas.js";

const sessionEventTypeSchema = z
  .string()
  .min(1)
  .transform((value): SessionEventType => value as SessionEventType);

/** Payload schema for task-created events. */
export const taskCreatedPayloadSchema = z.object({
  task: taskRecordSchema,
});

/** Payload schema for task-claim-expired events. */
export const taskClaimExpiredPayloadSchema = z.object({
  previousClaimedBy: z.string().min(1),
  task: taskRecordSchema,
});

/** Payload schema for task participant lifecycle events that include a task. */
export const taskParticipantPayloadSchema = z.object({
  participantId: z.string().min(1),
  task: taskRecordSchema,
});

/** Payload schema for task cancellation events. */
export const taskCancelledPayloadSchema = z.object({
  participantId: z.string().min(1),
  reason: z.record(z.string(), z.unknown()).default({}),
  task: taskRecordSchema,
});

/** Event payload schema for task approval decisions. */
export const taskApprovalRecordedPayloadSchema = z.object({
  decision: approvalDecisionSchema,
  participantId: z.string().min(1),
  reason: z.record(z.string(), z.unknown()).default({}),
  task: taskRecordSchema,
});

/** Runtime validator for public session events. */
export const sessionEventSchema = z.object({
  createdAt: z.string(),
  eventId: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
  producerId: z.string().min(1),
  seq: z.number(),
  sessionId: z.string().min(1),
  type: sessionEventTypeSchema,
});

/** Input for appending a canonical session event. */
export interface AppendSessionEventInput {
  readonly eventId: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly sessionId: string;
  readonly type: SessionEventType;
}

type BuildParticipantEventInput =
  | {
      readonly participant: ParticipantRecord;
      readonly sessionId: string;
      readonly status: "joined" | "refreshed";
    }
  | {
      readonly participant: ParticipantRecord;
      readonly previousParticipant: ParticipantRecord;
      readonly sessionId: string;
      readonly status: "updated";
    };

interface BuildTaskParticipantEventInput {
  readonly participantId: string;
  readonly sessionId: string;
  readonly task: TaskRecord;
}

interface BuildTaskCancelledEventInput extends BuildTaskParticipantEventInput {
  readonly reason: Record<string, unknown>;
}

interface BuildTaskApprovalRecordedEventInput extends BuildTaskParticipantEventInput {
  readonly decision: z.infer<typeof approvalDecisionSchema>;
  readonly reason: Record<string, unknown>;
}

interface BuildTaskCreatedEventInput {
  readonly sessionId: string;
  readonly task: TaskRecord;
}

interface BuildTaskClaimExpiredEventInput {
  readonly previousClaimedBy: string;
  readonly sessionId: string;
  readonly task: TaskRecord;
}

interface BuildPublishedEventInput {
  readonly eventId?: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly sessionId: string;
  readonly type: string;
}

interface BuildTaskOutputEventInput {
  readonly output: string;
  readonly participantId: string;
  readonly sessionId: string;
  readonly taskId: string;
}

interface BuildTaskProgressEventInput {
  readonly participantId: string;
  readonly sessionId: string;
  readonly taskId: string;
}

/** Creates a unique event id in Tether's public event-id namespace. */
export function newEventId(): string {
  return `evt_${newProtocolId()}`;
}

/** Creates a participant id scoped by runtime kind for generated REST participants. */
export function newParticipantId(runtimeKind: string): string {
  return `part_${runtimeKind}_${newProtocolId()}`;
}

/** Creates a unique runtime instance id. */
export function newParticipantInstanceId(): string {
  return `inst_${newProtocolId()}`;
}

/** Creates a unique session id. */
export function newSessionId(): string {
  return `sess_${newProtocolId()}`;
}

/** Creates a unique task id. */
export function newTaskId(): string {
  return `task_${newProtocolId()}`;
}

/** Parses an event replay cursor, falling back to the beginning of the stream. */
export function parseAfterSeq(value: string | null | undefined): number {
  return parsePositiveSafeInteger(value) ?? 0;
}

/** Parses a positive safe integer without coercing partial decimal strings. */
export function parsePositiveSafeInteger(
  value: string | number | bigint | null | undefined,
): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

/** Builds the durable event for a newly-created session. */
export function buildSessionCreatedEventInput(sessionId: string): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: {},
    producerId: systemProducerId,
    sessionId,
    type: sessionEventType.sessionCreated,
  };
}

/** Builds a caller-published session event while applying Tether id defaults. */
export function buildPublishedEventInput(input: BuildPublishedEventInput): AppendSessionEventInput {
  return {
    eventId: input.eventId ?? newEventId(),
    payload: input.payload,
    producerId: input.producerId,
    sessionId: input.sessionId,
    type: input.type,
  };
}

/** Builds participant join/update events. */
export function buildParticipantRegistrationEventInput(
  input: BuildParticipantEventInput,
): AppendSessionEventInput | null {
  if (input.status === "refreshed") {
    return null;
  }
  return {
    eventId: newEventId(),
    payload:
      input.status === "updated"
        ? {
            participant: input.participant,
            previousParticipant: input.previousParticipant,
          }
        : { participant: input.participant },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type:
      input.status === "joined"
        ? sessionEventType.participantJoined
        : sessionEventType.participantUpdated,
  };
}

/** Builds the visible heartbeat event emitted after presence state refreshes. */
export function buildParticipantHeartbeatEventInput(input: {
  readonly participant: ParticipantRecord;
  readonly sessionId: string;
}): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: { participant: input.participant },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type: sessionEventType.participantHeartbeat,
  };
}

/** Builds the canonical event for a newly-created task. */
export function buildTaskCreatedEventInput(
  input: BuildTaskCreatedEventInput,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: { task: input.task },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type: sessionEventType.taskCreated,
  };
}

/** Builds the canonical event for a user or bridge approval decision. */
export function buildTaskApprovalRecordedEventInput(
  input: BuildTaskApprovalRecordedEventInput,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: {
      decision: input.decision,
      participantId: input.participantId,
      reason: input.reason,
      task: input.task,
    },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type: sessionEventType.approvalRecorded,
  };
}

/** Builds the canonical event for a successful task claim. */
export function buildTaskClaimedEventInput(
  input: BuildTaskParticipantEventInput,
): AppendSessionEventInput {
  return buildTaskParticipantEventInput(input, sessionEventType.taskClaimed);
}

/** Builds the canonical event for an expired task claim. */
export function buildTaskClaimExpiredEventInput(
  input: BuildTaskClaimExpiredEventInput,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: { previousClaimedBy: input.previousClaimedBy, task: input.task },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type: sessionEventType.taskClaimExpired,
  };
}

/** Builds the canonical event for a cancelled task. */
export function buildTaskCancelledEventInput(
  input: BuildTaskCancelledEventInput,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: {
      participantId: input.participantId,
      reason: input.reason,
      task: input.task,
    },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type: sessionEventType.controlCancel,
  };
}

/** Builds the canonical event for a completed task. */
export function buildTaskCompletedEventInput(
  input: BuildTaskParticipantEventInput,
): AppendSessionEventInput {
  return buildTaskParticipantEventInput(input, sessionEventType.taskCompleted);
}

/** Builds the canonical event for a failed task. */
export function buildTaskFailedEventInput(
  input: BuildTaskParticipantEventInput,
): AppendSessionEventInput {
  return buildTaskParticipantEventInput(input, sessionEventType.taskFailed);
}

/** Builds the canonical event for a released task claim. */
export function buildTaskReleasedEventInput(
  input: BuildTaskParticipantEventInput,
): AppendSessionEventInput {
  return buildTaskParticipantEventInput(input, sessionEventType.taskReleased);
}

/** Builds the standard progress event published by simple participant workers. */
export function buildTaskProgressEventInput(
  input: BuildTaskProgressEventInput,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: { taskId: input.taskId },
    producerId: input.participantId,
    sessionId: input.sessionId,
    type: sessionEventType.taskProgress,
  };
}

/** Builds the standard output event published by simple participant workers. */
export function buildTaskOutputEventInput(
  input: BuildTaskOutputEventInput,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: { output: input.output, taskId: input.taskId },
    producerId: input.participantId,
    sessionId: input.sessionId,
    type: sessionEventType.agentOutput,
  };
}

/** Extracts a task from a task-created event payload. */
export function taskFromCreatedEvent(event: SessionEvent): TaskRecord | null {
  if (event.type !== sessionEventType.taskCreated) {
    return null;
  }
  const parsed = taskCreatedPayloadSchema.safeParse(event.payload);
  return parsed.success ? parsed.data.task : null;
}

/** Extracts a task that has become claimable from the public event stream. */
export function taskFromClaimableEvent(event: SessionEvent): TaskRecord | null {
  if (event.type === sessionEventType.taskCreated) {
    return taskFromCreatedEvent(event);
  }
  if (event.type === sessionEventType.taskClaimExpired) {
    const parsed = taskClaimExpiredPayloadSchema.safeParse(event.payload);
    return parsed.success ? parsed.data.task : null;
  }
  if (event.type !== sessionEventType.taskReleased) {
    return null;
  }
  const parsed = taskParticipantPayloadSchema.safeParse(event.payload);
  return parsed.success ? parsed.data.task : null;
}

/** Extracts the cancelled task id from a task cancellation event payload. */
export function taskIdFromCancelledEvent(event: SessionEvent): string | null {
  if (event.type !== sessionEventType.controlCancel) {
    return null;
  }
  const parsed = taskCancelledPayloadSchema.safeParse(event.payload);
  return parsed.success ? parsed.data.task.taskId : null;
}

function buildTaskParticipantEventInput(
  input: BuildTaskParticipantEventInput,
  type: SessionEventType,
): AppendSessionEventInput {
  return {
    eventId: newEventId(),
    payload: { participantId: input.participantId, task: input.task },
    producerId: systemProducerId,
    sessionId: input.sessionId,
    type,
  };
}

function newProtocolId(): string {
  const randomUuid = readGlobalRandomUuid();
  return randomUuid ?? `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 12)}`;
}

function readGlobalRandomUuid(): string | null {
  const candidate = globalThis as {
    readonly crypto?: { readonly randomUUID?: () => string };
  };
  return candidate.crypto?.randomUUID?.() ?? null;
}
