import { z } from "zod";

import { rejectDuplicateEntries } from "./schema-refinements.js";
import { recurringWorkScopeKeySchema } from "./task-contracts.js";

/** Runtime validator for one bounded opaque approval-target value. */
export const opaqueTargetValueSchema = z.string().min(1).max(512);

/** Durable approval decision values recorded in task approval history. */
export const approvalDecisionSchema = z.union([z.literal("approved"), z.literal("rejected")]);

/** Durable approval decision parsed by the public protocol. */
export type ApprovalDecision = z.infer<typeof approvalDecisionSchema>;

/** Provider-neutral descriptor for one approvable target produced by a task. */
export const approvalTargetSchema = z
  .object({
    action: opaqueTargetValueSchema,
    digest: opaqueTargetValueSchema,
    scopeKey: recurringWorkScopeKeySchema,
    targetId: opaqueTargetValueSchema,
    targetKind: opaqueTargetValueSchema,
    targetRevision: opaqueTargetValueSchema,
  })
  .strict();

/** Public provider-neutral approval target. Every field is opaque to Tether. */
export type ApprovalTarget = z.infer<typeof approvalTargetSchema>;

/** Collision-free canonical encoding of one opaque target identity tuple. */
export function approvalTargetIdentityKey(target: ApprovalTarget): string {
  return JSON.stringify([
    target.targetKind,
    target.scopeKey,
    target.targetId,
    target.targetRevision,
  ]);
}

/** Bounded target manifest persisted inside a completed task result. */
export const targetManifestSchema = z
  .array(approvalTargetSchema)
  .max(1_000)
  .superRefine(
    rejectDuplicateEntries({
      identity: approvalTargetIdentityKey,
      message: "Target manifest identities must be unique",
    }),
  );

/** Target manifest embedded in a generic task result. */
export type TargetManifest = z.infer<typeof targetManifestSchema>;

/** Generic task result with optional validated target-manifest content. */
export const taskResultSchema = z.looseObject({
  targetManifest: targetManifestSchema.optional(),
});
