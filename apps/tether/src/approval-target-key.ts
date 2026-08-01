/**
 * Owns canonical approval target identity for durable approval deduplication.
 * This module does not own bridge prompt deduplication keys, which have
 * different provider-specific semantics.
 */

import type { ApprovalTarget } from "./types.js";

/** Reserved key used when an approval applies to the whole task. */
export const wholeTaskApprovalTargetKey = "task";

/**
 * Builds the stable approval target key stored in `task_approvals`.
 */
export function approvalTargetKey(reason: unknown, target?: ApprovalTarget | undefined): string {
  if (target !== undefined) {
    return `approvalTarget:v1:${encodePart(target.targetKind)}${encodePart(target.scopeKey)}${encodePart(target.targetId)}${encodePart(target.targetRevision)}`;
  }
  if (typeof reason !== "object" || reason === null) {
    return wholeTaskApprovalTargetKey;
  }
  const record = reason as Readonly<Record<string, unknown>>;
  const legacyTarget = record.approvalTarget;
  if (typeof legacyTarget !== "object" || legacyTarget === null) {
    return wholeTaskApprovalTargetKey;
  }
  const targetRecord = legacyTarget as Readonly<Record<string, unknown>>;
  const key = targetRecord.key;
  if (typeof key !== "string" || key.length === 0) {
    return wholeTaskApprovalTargetKey;
  }
  const action = targetRecord.action;
  return typeof action === "string" && action.length > 0
    ? `approvalTarget:${action}:${key}`
    : `approvalTarget:${key}`;
}

/** Collision-free length-prefixed encoding for one opaque target identity part. */
function encodePart(value: string): string {
  return `${value.length}:${value}`;
}
