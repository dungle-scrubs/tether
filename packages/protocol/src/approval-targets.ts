import { z } from "zod";

import { recurringWorkScopeKeySchema } from "./task-contracts.js";

const opaqueTargetValueSchema = z.string().min(1).max(512);

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
  .superRefine((entries, context) => {
    const identities = new Set<string>();
    for (const [index, entry] of entries.entries()) {
      const identity = approvalTargetIdentityKey(entry);
      if (identities.has(identity)) {
        context.addIssue({
          code: "custom",
          message: "Target manifest identities must be unique",
          path: [index],
        });
      }
      identities.add(identity);
    }
  });

/** Target manifest embedded in a generic task result. */
export type TargetManifest = z.infer<typeof targetManifestSchema>;

/** Generic task result with optional validated target-manifest content. */
export const taskResultSchema = z.record(z.string(), z.unknown()).superRefine((result, context) => {
  if (!("targetManifest" in result)) {
    return;
  }
  const parsed = targetManifestSchema.safeParse(result.targetManifest);
  if (!parsed.success) {
    context.addIssue({
      code: "custom",
      message: "Task result targetManifest is invalid",
      path: ["targetManifest"],
    });
  }
});
