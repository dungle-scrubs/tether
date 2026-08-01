import { z } from "zod";

import { recurringWorkScopeKeySchema } from "./task-contracts.js";

/** Stable non-participant capabilities available to a browser operator grant. */
export const operatorPermissions = [
  "approval.submit",
  "authority.revoke",
  "backlog-preview.request",
  "browser-session.read",
  "browser-session.revoke",
  "scan.request",
  "session.read",
  "websocket.connect",
] as const;

/** Runtime validator for one non-participant operator capability. */
export const operatorPermissionSchema = z.enum(operatorPermissions);

/** Non-participant operator capability names. */
export type OperatorPermission = z.infer<typeof operatorPermissionSchema>;

const opaqueOperatorValueSchema = recurringWorkScopeKeySchema;
export const operatorGrantScopeMaxCompactBytes = 7 * 1_024;
const utf8Encoder = new TextEncoder();

/** Rejects duplicate values so scope comparisons remain canonical. */
function rejectDuplicates(values: readonly string[], context: z.RefinementCtx): void {
  const seen = new Set<string>();
  for (const [index, value] of values.entries()) {
    if (seen.has(value)) {
      context.addIssue({ code: "custom", message: "Scope values must be unique", path: [index] });
    }
    seen.add(value);
  }
}

const opaqueScopeArraySchema = z
  .array(opaqueOperatorValueSchema)
  .max(32)
  .superRefine(rejectDuplicates);

/** Provider-neutral resource and action boundary for one browser operator grant. */
export const operatorGrantScopeSchema = z
  .object({
    actions: opaqueScopeArraySchema,
    commands: opaqueScopeArraySchema,
    permissions: z.array(operatorPermissionSchema).min(1).max(16).superRefine(rejectDuplicates),
    scopeKeys: opaqueScopeArraySchema.min(1),
    sessionIds: opaqueScopeArraySchema.min(1),
    targetKinds: opaqueScopeArraySchema,
  })
  .strict()
  .superRefine((scope, context) => {
    if (utf8Encoder.encode(JSON.stringify(scope)).byteLength > operatorGrantScopeMaxCompactBytes) {
      context.addIssue({ code: "custom", message: "Operator grant scope exceeds byte limit" });
    }
  });

/** Parsed non-participant operator authority boundary. */
export type OperatorGrantScope = z.infer<typeof operatorGrantScopeSchema>;
