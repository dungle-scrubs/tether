import { z } from "zod";
import { sessionEventSchema } from "./event-builders.js";
import type { LiveHostPresence, SessionEvent } from "./records.js";
import {
  liveHostPresenceSchema,
  replicaPresenceScope,
  taskRecordSchema,
  webSocketOperation,
} from "./records.js";

/** WebSocket task-claim command schema. */
export const wsTaskClaimMessageSchema = z.object({
  op: z.literal(webSocketOperation.taskClaim),
  requestId: z.string().min(1).optional(),
  taskId: z.string().min(1),
});

/** WebSocket task claim-refresh command schema. */
export const wsTaskRefreshMessageSchema = z.object({
  claimId: z.string().min(1),
  op: z.literal(webSocketOperation.taskRefresh),
  requestId: z.string().min(1).optional(),
  taskId: z.string().min(1),
});

/** WebSocket task-cancel command schema. */
export const wsTaskCancelMessageSchema = z.object({
  op: z.literal(webSocketOperation.taskCancel),
  reason: z.record(z.string(), z.unknown()).default({}),
  requestId: z.string().min(1).optional(),
  taskId: z.string().min(1),
});

/** WebSocket task-complete command schema. */
export const wsTaskCompleteMessageSchema = z.object({
  claimId: z.string().min(1),
  op: z.literal(webSocketOperation.taskComplete),
  requestId: z.string().min(1).optional(),
  result: z.record(z.string(), z.unknown()).default({}),
  taskId: z.string().min(1),
});

/** WebSocket task-fail command schema. */
export const wsTaskFailMessageSchema = z.object({
  claimId: z.string().min(1),
  failure: z.record(z.string(), z.unknown()).default({}),
  op: z.literal(webSocketOperation.taskFail),
  requestId: z.string().min(1).optional(),
  taskId: z.string().min(1),
});

/** WebSocket task-release command schema. */
export const wsTaskReleaseMessageSchema = z.object({
  claimId: z.string().min(1),
  op: z.literal(webSocketOperation.taskRelease),
  requestId: z.string().min(1).optional(),
  taskId: z.string().min(1),
});

/** WebSocket publish command schema. */
export const wsPublishMessageSchema = z.object({
  eventId: z.string().min(1).optional(),
  op: z.literal(webSocketOperation.publish),
  payload: z.record(z.string(), z.unknown()).default({}),
  producerId: z.string().min(1),
  requestId: z.string().min(1).optional(),
  type: z.string().min(1),
});

/** WebSocket server event envelope schema. */
export const webSocketEventEnvelopeSchema = z.object({
  event: sessionEventSchema,
  op: z.literal(webSocketOperation.event),
});

/** WebSocket replay-complete envelope schema. */
export const webSocketReplayCompleteEnvelopeSchema = z
  .object({
    controlEpoch: z.number().int().positive().safe().optional(),
    instanceId: z.string().min(1).optional(),
    op: z.literal(webSocketOperation.replayComplete),
    participantId: z.string().min(1).optional(),
  })
  .refine(
    (value) =>
      [value.controlEpoch, value.instanceId, value.participantId].every(
        (entry) => entry === undefined,
      ) ||
      [value.controlEpoch, value.instanceId, value.participantId].every(
        (entry) => entry !== undefined,
      ),
    { message: "fenced participant identity must be complete" },
  );

/** WebSocket Replica Scope Host Presence envelope schema. */
export const webSocketPresenceEnvelopeSchema = z.object({
  hosts: z.array(liveHostPresenceSchema),
  op: z.literal(webSocketOperation.presence),
  replicaId: z.string().min(1),
  scope: z.literal(replicaPresenceScope),
});

/** WebSocket command-result envelope schema. */
export const webSocketCommandResultEnvelopeSchema = z
  .object({
    command: z.string().min(1),
    op: z.literal(webSocketOperation.commandResult),
    requestId: z.string().min(1).optional(),
    task: taskRecordSchema.nullable().optional(),
  })
  .passthrough();

/** WebSocket error envelope schema. */
export const webSocketErrorEnvelopeSchema = z
  .object({
    error: z.string(),
    op: z.literal(webSocketOperation.error),
    reason: z.string().min(1).optional(),
    requestId: z.string().min(1).optional(),
  })
  .passthrough();

/** Runtime validator for server-to-client WebSocket envelopes. */
export const webSocketServerEnvelopeSchema = z.union([
  webSocketCommandResultEnvelopeSchema,
  webSocketErrorEnvelopeSchema,
  webSocketEventEnvelopeSchema,
  webSocketPresenceEnvelopeSchema,
  webSocketReplayCompleteEnvelopeSchema,
]);

/** Server-to-client operations covered by the owned envelope union. */
const knownWebSocketServerEnvelopeOps: ReadonlySet<string> = new Set([
  webSocketOperation.commandResult,
  webSocketOperation.error,
  webSocketOperation.event,
  webSocketOperation.presence,
  webSocketOperation.replayComplete,
]);

/** Classification of one raw server frame against the owned envelope union. */
export type WebSocketServerEnvelopeClassification =
  | { readonly envelope: WebSocketServerEnvelope; readonly kind: "envelope" }
  | { readonly kind: "malformed" }
  | { readonly kind: "unknown-op"; readonly op: string };

/** Parsed WebSocket command-result envelope. */
export type CommandResultEnvelope = z.infer<typeof webSocketCommandResultEnvelopeSchema>;

/** Parsed Replica Scope Host Presence envelope. */
export type WebSocketPresenceEnvelope = z.infer<typeof webSocketPresenceEnvelopeSchema>;

/** Parsed server-to-client WebSocket envelope. */
export type WebSocketServerEnvelope = z.infer<typeof webSocketServerEnvelopeSchema>;

/** Stable recovery vocabulary shared by transport producers and consumers. */
export const webSocketRecoveryReason = {
  recoveryRequired: "recovery_required",
  replayWindowExceeded: "replay_window_exceeded",
} as const;

/** Recovery reason currently emitted or reserved by the protocol. */
export type WebSocketRecoveryReason =
  (typeof webSocketRecoveryReason)[keyof typeof webSocketRecoveryReason];

/** Safe, bounded recovery metadata preserved across client boundaries. */
export interface WebSocketRecoveryCondition {
  readonly limit?: number;
  readonly reason: WebSocketRecoveryReason;
}

/**
 * Projects only protocol-owned recovery fields from an error envelope. Unknown
 * passthrough fields are deliberately excluded from the returned condition.
 */
export function parseWebSocketRecoveryCondition(
  envelope: WebSocketServerEnvelope,
): WebSocketRecoveryCondition | null {
  if (envelope.op !== webSocketOperation.error) {
    return null;
  }
  const reason = parseWebSocketRecoveryReason(envelope.reason);
  if (reason === null) {
    return null;
  }
  const limit = envelope.limit;
  return {
    ...(typeof limit === "number" && Number.isSafeInteger(limit) && limit > 0 ? { limit } : {}),
    reason,
  };
}

/** Narrows arbitrary server reason strings to the owned recovery taxonomy. */
function parseWebSocketRecoveryReason(value: unknown): WebSocketRecoveryReason | null {
  switch (value) {
    case webSocketRecoveryReason.recoveryRequired:
    case webSocketRecoveryReason.replayWindowExceeded:
      return value;
    default:
      return null;
  }
}

/** Parsed WebSocket task command message. */
export type WebSocketTaskCommandMessage =
  | z.infer<typeof wsTaskCancelMessageSchema>
  | z.infer<typeof wsTaskClaimMessageSchema>
  | z.infer<typeof wsTaskCompleteMessageSchema>
  | z.infer<typeof wsTaskFailMessageSchema>
  | z.infer<typeof wsTaskRefreshMessageSchema>
  | z.infer<typeof wsTaskReleaseMessageSchema>;

/**
 * WebSocket client command messages that can be correlated with a command
 * result envelope.
 */
export type WebSocketCommandMessage =
  | WebSocketTaskCommandMessage
  | z.infer<typeof wsPublishMessageSchema>;

interface SerializeCommandEnvelopeInput {
  readonly command: string;
  readonly payload: Record<string, unknown>;
  readonly requestId?: string;
}

interface SerializeErrorEnvelopeInput {
  readonly details?: Record<string, unknown>;
  readonly error: string;
  readonly requestId?: string;
}

/** Inputs required to serialize a Replica Scope presence envelope. */
export interface SerializePresenceEnvelopeInput {
  readonly hosts: readonly LiveHostPresence[];
  readonly replicaId: string;
}

/** Creates a WebSocket publish message for participant-originated events. */
export function buildWsPublishMessage(input: {
  readonly eventId?: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly requestId?: string;
  readonly type: string;
}): z.infer<typeof wsPublishMessageSchema> {
  return {
    ...(input.eventId !== undefined ? { eventId: input.eventId } : {}),
    op: webSocketOperation.publish,
    payload: input.payload,
    producerId: input.producerId,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    type: input.type,
  };
}

/** Creates a WebSocket task-claim command. */
export function buildWsTaskClaimMessage(input: {
  readonly requestId?: string;
  readonly taskId: string;
}): z.infer<typeof wsTaskClaimMessageSchema> {
  return {
    op: webSocketOperation.taskClaim,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    taskId: input.taskId,
  };
}

/** Creates a WebSocket task claim-refresh command. */
export function buildWsTaskRefreshMessage(input: {
  readonly claimId: string;
  readonly requestId?: string;
  readonly taskId: string;
}): z.infer<typeof wsTaskRefreshMessageSchema> {
  return {
    claimId: input.claimId,
    op: webSocketOperation.taskRefresh,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    taskId: input.taskId,
  };
}

/** Creates a WebSocket task-cancel command. */
export function buildWsTaskCancelMessage(input: {
  readonly reason?: Record<string, unknown>;
  readonly requestId?: string;
  readonly taskId: string;
}): z.infer<typeof wsTaskCancelMessageSchema> {
  return {
    op: webSocketOperation.taskCancel,
    reason: input.reason ?? {},
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    taskId: input.taskId,
  };
}

/** Creates a WebSocket task-complete command. */
export function buildWsTaskCompleteMessage(input: {
  readonly claimId: string;
  readonly requestId?: string;
  readonly result: Record<string, unknown>;
  readonly taskId: string;
}): z.infer<typeof wsTaskCompleteMessageSchema> {
  return {
    claimId: input.claimId,
    op: webSocketOperation.taskComplete,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    result: input.result,
    taskId: input.taskId,
  };
}

/** Creates a WebSocket task-fail command. */
export function buildWsTaskFailMessage(input: {
  readonly claimId: string;
  readonly failure: Record<string, unknown>;
  readonly requestId?: string;
  readonly taskId: string;
}): z.infer<typeof wsTaskFailMessageSchema> {
  return {
    claimId: input.claimId,
    failure: input.failure,
    op: webSocketOperation.taskFail,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    taskId: input.taskId,
  };
}

/** Creates a WebSocket task-release command. */
export function buildWsTaskReleaseMessage(input: {
  readonly claimId: string;
  readonly requestId?: string;
  readonly taskId: string;
}): z.infer<typeof wsTaskReleaseMessageSchema> {
  return {
    claimId: input.claimId,
    op: webSocketOperation.taskRelease,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
    taskId: input.taskId,
  };
}

/** Builds a mandatory Replica Scope Host Presence envelope. */
export function buildWebSocketPresenceEnvelope(
  input: SerializePresenceEnvelopeInput,
): WebSocketPresenceEnvelope {
  return {
    hosts: [...input.hosts],
    op: webSocketOperation.presence,
    replicaId: input.replicaId,
    scope: replicaPresenceScope,
  };
}

/** Parses server-to-client WebSocket envelopes into the known protocol union. */
export function parseWebSocketServerEnvelope(value: unknown): WebSocketServerEnvelope | null {
  const parsed = webSocketServerEnvelopeSchema.safeParse(value);
  return parsed.success ? parsed.data : null;
}

/**
 * Classifies one raw server frame so clients can skip forward-compatible
 * unknown operations while keeping strict validation for known ones. A frame
 * whose `op` is outside the owned server envelope union is `unknown-op`; a
 * frame carrying a known `op` that fails strict validation is `malformed`.
 *
 * Skipping unknown-op frames preserves seq contiguity because event ordering
 * is carried exclusively by `event` envelopes and their `seq` values. If a
 * future server ever attached an event delivery to a new operation, the
 * skipped delivery would surface as a non-contiguous `event` frame and halt
 * delivery through the existing typed outcome rather than losing the event
 * silently.
 */
export function classifyWebSocketServerEnvelope(
  value: unknown,
): WebSocketServerEnvelopeClassification {
  const parsed = webSocketServerEnvelopeSchema.safeParse(value);
  if (parsed.success) {
    return { envelope: parsed.data, kind: "envelope" };
  }
  const op = readEnvelopeOp(value);
  if (op !== null && !knownWebSocketServerEnvelopeOps.has(op)) {
    return { kind: "unknown-op", op };
  }
  return { kind: "malformed" };
}

/** Reads the operation discriminator from one untrusted frame value. */
function readEnvelopeOp(value: unknown): string | null {
  if (typeof value !== "object" || value === null) {
    return null;
  }
  const op = (value as Record<string, unknown>).op;
  return typeof op === "string" ? op : null;
}

/** Serializes a session event envelope for WebSocket subscribers. */
export function serializeEventEnvelope(event: SessionEvent): string {
  return JSON.stringify({ event, op: webSocketOperation.event });
}

/** Fenced participant identity optionally attached to replay completion. */
export interface ReplayCompleteParticipantContext {
  readonly controlEpoch: number;
  readonly instanceId: string;
  readonly participantId: string;
}

/** Serializes the replay-complete marker sent after historical events. */
export function serializeReplayCompleteEnvelope(
  participant?: ReplayCompleteParticipantContext,
): string {
  return JSON.stringify({
    ...(participant ?? {}),
    op: webSocketOperation.replayComplete,
  });
}

/** Serializes a mandatory Replica Scope Host Presence envelope. */
export function serializePresenceEnvelope(input: SerializePresenceEnvelopeInput): string {
  return JSON.stringify(buildWebSocketPresenceEnvelope(input));
}

/** Serializes a command result envelope while preserving command-specific fields. */
export function serializeCommandResultEnvelope(input: SerializeCommandEnvelopeInput): string {
  return JSON.stringify({
    ...input.payload,
    command: input.command,
    op: webSocketOperation.commandResult,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
  });
}

/** Serializes a command or connection error envelope. */
export function serializeErrorEnvelope(input: SerializeErrorEnvelopeInput): string {
  return JSON.stringify({
    ...(input.details ?? {}),
    error: input.error,
    op: webSocketOperation.error,
    ...(input.requestId !== undefined ? { requestId: input.requestId } : {}),
  });
}
