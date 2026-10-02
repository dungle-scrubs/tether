import { z } from "zod";

import { rejectDuplicateEntries } from "./schema-refinements.js";
import { utf8ByteLength } from "./text.js";

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

/** Maximum encoded size of one opaque operator scope value. */
export const operatorScopeValueMaxBytes = 512;

/** Maximum compact JSON size of one complete operator grant scope. */
export const operatorGrantScopeMaxCompactBytes = 7 * 1_024;

const opaqueOperatorValueSchema = z
  .string()
  .min(1)
  .refine((value) => utf8ByteLength(value) <= operatorScopeValueMaxBytes, {
    message: `Operator scope value must not exceed ${operatorScopeValueMaxBytes} UTF-8 bytes`,
  });

/** Runtime validator for one operator subject recorded on a browser grant. */
export const operatorSubjectSchema = z.string().min(1).max(255);

/** Runtime validator for one durable operator grant identifier. */
export const operatorGrantJtiSchema = z.string().min(1).max(128);

const rejectDuplicateScopeValues = rejectDuplicateEntries<string>({
  identity: (value) => value,
  message: "Scope values must be unique",
});

const opaqueScopeArraySchema = z
  .array(opaqueOperatorValueSchema)
  .max(32)
  .superRefine(rejectDuplicateScopeValues);

/** Provider-neutral resource and action boundary for one browser operator grant. */
export const operatorGrantScopeSchema = z
  .object({
    actions: opaqueScopeArraySchema,
    commands: opaqueScopeArraySchema,
    permissions: z
      .array(operatorPermissionSchema)
      .min(1)
      .max(16)
      .superRefine(rejectDuplicateScopeValues),
    scopeKeys: opaqueScopeArraySchema.min(1),
    sessionIds: opaqueScopeArraySchema.min(1),
    targetKinds: opaqueScopeArraySchema,
  })
  .strict()
  .superRefine((scope, context) => {
    if (utf8ByteLength(JSON.stringify(scope)) > operatorGrantScopeMaxCompactBytes) {
      context.addIssue({ code: "custom", message: "Operator grant scope exceeds byte limit" });
    }
  });

/** Parsed non-participant operator authority boundary. */
export type OperatorGrantScope = z.infer<typeof operatorGrantScopeSchema>;
