/**
 * Owns canonical approval target identity for durable approval deduplication.
 * This module does not own bridge prompt deduplication keys, which have
 * different provider-specific semantics.
 */

/** Reserved key used when an approval applies to the whole task. */
export const wholeTaskApprovalTargetKey = "task";

/**
 * Builds the stable approval target key stored in `task_approvals`.
 */
export function approvalTargetKey(reason: unknown): string {
  if (typeof reason !== "object" || reason === null) {
    return wholeTaskApprovalTargetKey;
  }
  const record = reason as Readonly<Record<string, unknown>>;
  const target = record.approvalTarget;
  if (typeof target !== "object" || target === null) {
    return wholeTaskApprovalTargetKey;
  }
  const targetRecord = target as Readonly<Record<string, unknown>>;
  const key = targetRecord.key;
  if (typeof key !== "string" || key.length === 0) {
    return wholeTaskApprovalTargetKey;
  }
  const action = targetRecord.action;
  return typeof action === "string" && action.length > 0
    ? `approvalTarget:${action}:${key}`
    : `approvalTarget:${key}`;
}
