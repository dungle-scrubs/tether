import {
  approvalDecisionSchema,
  sessionEventSchema,
  taskContractSummarySchema,
  taskRecordSchema,
} from "@dungle-scrubs/tether-protocol";
import { z } from "zod";

/** Durable client session record returned by Tether bridge endpoints. */
export const clientSessionRecordSchema = z.object({
  archivedAt: z.string().nullable(),
  createdAt: z.string(),
  sessionId: z.string().min(1),
});

/** Durable external-client binding returned by Tether bridge endpoints. */
export const clientSessionBindingRecordSchema = z.object({
  archivedAt: z.string().nullable(),
  createdAt: z.string(),
  externalId: z.string().min(1),
  lastSeenAt: z.string(),
  provider: z.string().min(1),
  sessionId: z.string().min(1),
});

/** Response schema for resolving one client session binding. */
export const clientSessionBindingResponseSchema = z.object({
  binding: clientSessionBindingRecordSchema,
  created: z.boolean(),
  session: clientSessionRecordSchema,
});

/** Response schema for listing client session bindings. */
export const clientSessionBindingsResponseSchema = z.object({
  bindings: z.array(clientSessionBindingRecordSchema),
});

/** Participant task contract record returned by task inspection endpoints. */
export const taskContractRecordSchema = taskContractSummarySchema.extend({
  displayName: z.string().min(1),
  participantId: z.string().min(1),
  runtimeKind: z.string().min(1),
  sessionId: z.string().min(1),
});

/** Response schema for one durable task. */
export const taskResponseSchema = z.object({
  task: taskRecordSchema,
});

/** Response schema for one task plus its current matching contract. */
export const taskInspectionResponseSchema = z.object({
  contract: taskContractRecordSchema.nullable().optional(),
  task: taskRecordSchema,
});

/** Response schema for recording task approval intent. */
export const taskApprovalResponseSchema = z.discriminatedUnion("status", [
  z.object({
    decision: approvalDecisionSchema,
    event: sessionEventSchema,
    status: z.literal("recorded"),
    task: taskRecordSchema,
  }),
  z.object({
    decision: approvalDecisionSchema,
    existingDecision: approvalDecisionSchema,
    ignoredReason: z.union([z.literal("already_approved"), z.literal("already_rejected")]),
    status: z.literal("ignored"),
    task: taskRecordSchema,
  }),
]);

/** Response schema for listing durable tasks. */
export const tasksResponseSchema = z.object({
  tasks: z.array(taskRecordSchema),
});
