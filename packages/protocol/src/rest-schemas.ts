import { z } from "zod";

import {
  approvalDecisionSchema,
  approvalTargetSchema,
  taskResultSchema,
} from "./approval-targets.js";
import { sessionEventSchema } from "./event-builders.js";
import { controlChannelSchema, participantRuntimeKindSchema } from "./records.js";
import { recurringWorkScopeKeySchema } from "./task-contracts.js";

/** Bounded readiness failure reasons safe for unauthenticated responses. */
export const readinessFailureReason = {
  databaseUnavailable: "database_unavailable",
  fanoutCatchUpStale: "fanout_catchup_stale",
} as const;

/** Protocol-owned readiness response schema. */
export const readinessResponseSchema = z.discriminatedUnion("ready", [
  z.object({
    ready: z.literal(true),
    replicaId: z.string().min(1),
    runtimeTopology: z.union([z.literal("single"), z.literal("multi")]),
  }),
  z.object({
    ready: z.literal(false),
    reason: z.union([
      z.literal(readinessFailureReason.databaseUnavailable),
      z.literal(readinessFailureReason.fanoutCatchUpStale),
    ]),
    replicaId: z.string().min(1),
    runtimeTopology: z.union([z.literal("single"), z.literal("multi")]),
  }),
]);

/** Parsed bounded readiness response. */
export type ReadinessResponse = z.infer<typeof readinessResponseSchema>;

/**
 * Server-issued Control Epoch carried by control-protected REST requests. It is
 * a positive safe integer; the server validates it against the current durable
 * lease generation in the same transaction as the protected mutation.
 */
export const controlEpochSchema = z
  .number()
  .int()
  .positive()
  .refine((value) => Number.isSafeInteger(value), {
    message: "controlEpoch must be a positive safe integer",
  });

/** Client-generated retry identity for one logical REST control acquisition. */
export const controlAcquisitionIdSchema = z.string().trim().min(1).max(128);

/** Pagination metadata returned by bounded session event-list responses. */
export const eventListPaginationSchema = z.object({
  afterSeq: z.number().int().nonnegative(),
  hasMore: z.boolean(),
  limit: z.number().int().positive(),
  nextAfterSeq: z.number().int().nonnegative(),
  returned: z.number().int().nonnegative(),
});

/** HTTP response schema for bounded session event-list responses. */
export const eventListResponseSchema = z.object({
  events: z.array(z.lazy(() => sessionEventSchema)),
  pagination: eventListPaginationSchema,
});

/** Pagination metadata returned by bounded session event-list responses. */
export type EventListPagination = z.infer<typeof eventListPaginationSchema>;

/** HTTP response body for bounded session event-list responses. */
export type EventListResponse = z.infer<typeof eventListResponseSchema>;

/** HTTP body schema for appending a generic session event. */
export const appendEventSchema = z.object({
  controlEpoch: controlEpochSchema.optional(),
  eventId: z.string().min(1).optional(),
  instanceId: z.string().min(1).optional(),
  payload: z.record(z.string(), z.unknown()).default({}),
  producerId: z.string().min(1),
  type: z.string().min(1),
});

/** HTTP body schema for session creation. */
export const createSessionSchema = z.object({
  sessionId: z.string().min(1).optional(),
});

/** HTTP body schema for resolving external client conversations. */
export const resolveClientSessionSchema = z.object({
  externalId: z.string().min(1),
  provider: z.string().min(1),
  sessionId: z.string().min(1).optional(),
});

/** HTTP body schema for participant registration. */
export const registerParticipantSchema = z.object({
  acquisitionId: controlAcquisitionIdSchema.optional(),
  capabilities: z.record(z.string(), z.unknown()).default({}),
  controlChannel: controlChannelSchema.default("rest"),
  displayName: z.string().min(1).optional(),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1).optional(),
  runtimeKind: participantRuntimeKindSchema,
});

/** HTTP response returned after REST participant control acquisition. */
export const restControlAcquisitionResponseSchema = z.object({
  acquisitionId: controlAcquisitionIdSchema,
  acquisitionStatus: z.union([
    z.literal("claimed"),
    z.literal("replayed"),
    z.literal("superseded"),
  ]),
  controlEpoch: controlEpochSchema,
  leaseExpiresAt: z.string().datetime({ offset: true }),
  participant: z.record(z.string(), z.unknown()),
  registrationStatus: z.union([z.literal("joined"), z.literal("refreshed"), z.literal("updated")]),
  renewAfterMs: z.number().int().positive(),
});

/** Parsed REST participant control acquisition response. */
export type RestControlAcquisitionResponse = z.infer<typeof restControlAcquisitionResponseSchema>;

/** HTTP body schema for participant heartbeat refresh. */
export const heartbeatParticipantSchema = z.object({
  capabilities: z.record(z.string(), z.unknown()).optional(),
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1).optional(),
});

/** HTTP response returned after exact-epoch REST control renewal. */
export const restControlRenewalResponseSchema = z.object({
  controlEpoch: controlEpochSchema,
  leaseExpiresAt: z.string().datetime({ offset: true }),
  participant: z.record(z.string(), z.unknown()),
  renewAfterMs: z.number().int().positive(),
});

/** Parsed REST participant control renewal response. */
export type RestControlRenewalResponse = z.infer<typeof restControlRenewalResponseSchema>;

/** HTTP route envelope used to distinguish an absent release epoch from a malformed one. */
export const releaseParticipantControlEnvelopeSchema = z.object({
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1),
});

/** HTTP body for exact-generation REST participant control release. */
export const releaseParticipantControlSchema = releaseParticipantControlEnvelopeSchema.extend({
  controlEpoch: controlEpochSchema,
});

/** HTTP response for idempotent REST participant control release. */
export const releaseParticipantControlResponseSchema = z.object({
  released: z.boolean(),
});

/**
 * Deterministic schedule and opaque scope identity a recurring-work run
 * carries at creation. Interval and algorithm version are part of task identity.
 */
export const scheduledTaskIdentitySchema = z
  .object({
    scheduleAlgorithmVersion: z.number().int().positive(),
    scheduleIntervalMs: z.number().int().positive(),
    scheduleWindowStart: z.number().int().nonnegative(),
    scopeKey: recurringWorkScopeKeySchema,
  })
  .strict()
  .refine(
    (identity) => Number.isSafeInteger(identity.scheduleWindowStart + identity.scheduleIntervalMs),
    {
      message: "Schedule Window start plus interval must be a safe integer",
      path: ["scheduleWindowStart"],
    },
  );

/** HTTP body schema for task creation. */
export const createTaskSchema = z.object({
  input: z.record(z.string(), z.unknown()).nullable().optional(),
  kind: z.string().min(1),
  objective: z.string().min(1),
  requireContract: z.boolean().optional(),
  schedule: scheduledTaskIdentitySchema.optional(),
  taskId: z.string().min(1).optional(),
});

/** HTTP body schema for task claim. */
export const claimTaskSchema = z.object({
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
});

/** HTTP body schema for task claim refresh. */
export const refreshTaskClaimSchema = z.object({
  claimId: z.string().min(1),
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
});

/** HTTP body schema for task cancellation. */
export const cancelTaskSchema = z.object({
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
  reason: z.record(z.string(), z.unknown()).default({}),
});

/** HTTP body schema for task completion. */
export const completeTaskSchema = z.object({
  claimId: z.string().min(1),
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
  result: taskResultSchema.default({}),
});

/** HTTP body schema for task failure. */
export const failTaskSchema = z.object({
  claimId: z.string().min(1),
  controlEpoch: controlEpochSchema.optional(),
  failure: z.record(z.string(), z.unknown()).default({}),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
});

/** HTTP body schema for task claim release. */
export const releaseTaskSchema = z.object({
  claimId: z.string().min(1),
  controlEpoch: controlEpochSchema.optional(),
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
});

/** HTTP payload schema for recording approval intent for a task. */
export const recordTaskApprovalSchema = z.object({
  controlEpoch: controlEpochSchema.optional(),
  decision: approvalDecisionSchema,
  instanceId: z.string().min(1).optional(),
  participantId: z.string().min(1),
  reason: z.record(z.string(), z.unknown()).default({}),
  target: approvalTargetSchema.optional(),
});
