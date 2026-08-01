import { z } from "zod";

import { approvalDecisionSchema, approvalTargetSchema } from "./approval-targets.js";
import { sessionEventSchema } from "./event-builders.js";
import { operatorGrantScopeSchema, type OperatorPermission } from "./operator-authority.js";
import {
  participantRuntimeKindSchema,
  taskApprovalRecordSchema,
  taskRecordSchema,
} from "./records.js";
import { recurringWorkScopeKeySchema } from "./task-contracts.js";

/** Fixed provider-neutral commands exposed by the non-participant browser boundary. */
export const operatorCommands = ["authority-revoke", "backlog-preview", "scan"] as const;

/** Runtime validator for one browser operator command. */
export const operatorCommandSchema = z.enum(operatorCommands);

/** Fixed provider-neutral browser operator command. */
export type OperatorCommand = z.infer<typeof operatorCommandSchema>;

const operatorCommandPermissionByCommand = {
  "authority-revoke": "authority.revoke",
  "backlog-preview": "backlog-preview.request",
  scan: "scan.request",
} as const satisfies Record<OperatorCommand, OperatorPermission>;

/** Returns the single permission that authorizes one fixed operator command. */
export function operatorCommandPermission(
  command: OperatorCommand,
): (typeof operatorCommandPermissionByCommand)[OperatorCommand] {
  return operatorCommandPermissionByCommand[command];
}

/** Bounded request that can create only a server-selected operator command task. */
export const operatorCommandRequestSchema = z
  .object({
    command: operatorCommandSchema,
    scopeKey: recurringWorkScopeKeySchema,
    targetId: z.string().min(1).max(512).optional(),
  })
  .strict()
  .superRefine((request, context) => {
    if (request.command === "authority-revoke" && request.targetId === undefined) {
      context.addIssue({
        code: "custom",
        message: "Authority revocation requires targetId",
        path: ["targetId"],
      });
    }
    if (request.command !== "authority-revoke" && request.targetId !== undefined) {
      context.addIssue({
        code: "custom",
        message: "Only authority revocation accepts targetId",
        path: ["targetId"],
      });
    }
  });

/** Browser decision payload with no participant identity or Control Epoch escape hatch. */
export const operatorTaskApprovalRequestSchema = z
  .object({
    decision: approvalDecisionSchema,
    reason: z.record(z.string(), z.unknown()).default({}),
    target: approvalTargetSchema,
  })
  .strict();

/** Cookie-authenticated bootstrap response exposing only the grant's allowed resources. */
export const browserOperatorSessionSchema = z
  .object({
    expiresAt: z.string().datetime({ offset: true }),
    grantJti: z.string().min(1).max(128),
    scope: operatorGrantScopeSchema,
    status: z.literal("active"),
    subject: z.string().min(1).max(255),
  })
  .strict();

/** Provider-neutral non-participant snapshot used before WebSocket replay begins. */
export const browserSessionSnapshotSchema = z
  .object({
    cursor: z.number().int().nonnegative().safe(),
    events: z.array(sessionEventSchema).max(1_000),
    participants: z
      .array(
        z
          .object({
            capabilities: z.record(z.string(), z.unknown()),
            displayName: z.string(),
            joinedAt: z.string(),
            lastSeenAt: z.string(),
            participantId: z.string().min(1),
            runtimeKind: participantRuntimeKindSchema,
            sessionId: z.string().min(1),
          })
          .strict(),
      )
      .max(1_000),
    sessionId: z.string().min(1).max(512),
    tasks: z.array(taskRecordSchema).max(10_000),
    truncated: z
      .object({
        events: z.boolean(),
        participants: z.boolean(),
        tasks: z.boolean(),
      })
      .strict(),
  })
  .strict();

/** Canonical operator command admission response, including coalesced retries. */
export const browserOperatorCommandResponseSchema = z
  .object({
    status: z.enum(["created", "replayed"]),
    task: taskRecordSchema,
  })
  .strict();

/** Canonical browser approval response for recorded, duplicate, and rejected decisions. */
export const browserOperatorApprovalResponseSchema = z.discriminatedUnion("status", [
  z
    .object({
      approval: taskApprovalRecordSchema,
      decision: approvalDecisionSchema,
      event: sessionEventSchema,
      status: z.literal("recorded"),
      task: taskRecordSchema,
    })
    .strict(),
  z
    .object({
      approval: taskApprovalRecordSchema,
      decision: approvalDecisionSchema,
      existingDecision: approvalDecisionSchema,
      ignoredReason: z.enum(["already_approved", "already_rejected"]),
      status: z.literal("ignored"),
      task: taskRecordSchema,
    })
    .strict(),
  z
    .object({
      rejectionReason: z.string().min(1).max(128),
      status: z.literal("rejected"),
      task: taskRecordSchema.nullable(),
    })
    .strict(),
]);

/** One-time browser WebSocket ticket response. */
export const browserWebSocketTicketResponseSchema = z
  .object({
    expiresAt: z.string().datetime({ offset: true }),
    ticket: z.string().regex(/^[A-Za-z0-9_-]{43}$/u),
  })
  .strict();

/** Parsed provider-neutral browser operator command. */
export type OperatorCommandRequest = z.infer<typeof operatorCommandRequestSchema>;

/** Parsed manifest-bound browser approval request. */
export type OperatorTaskApprovalRequest = z.infer<typeof operatorTaskApprovalRequestSchema>;

/** Parsed browser operator session bootstrap response. */
export type BrowserOperatorSession = z.infer<typeof browserOperatorSessionSchema>;

/** Parsed non-participant browser snapshot. */
export type BrowserSessionSnapshot = z.infer<typeof browserSessionSnapshotSchema>;

/** Parsed canonical operator command response. */
export type BrowserOperatorCommandResponse = z.infer<typeof browserOperatorCommandResponseSchema>;

/** Parsed canonical browser approval response. */
export type BrowserOperatorApprovalResponse = z.infer<typeof browserOperatorApprovalResponseSchema>;

/** Parsed browser WebSocket ticket response. */
export type BrowserWebSocketTicketResponse = z.infer<typeof browserWebSocketTicketResponseSchema>;
