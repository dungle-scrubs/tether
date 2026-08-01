/**
 * Owns canonical approval target identity for durable approval deduplication.
 * This module does not own bridge prompt deduplication keys, which have
 * different provider-specific semantics.
 */

import { createHash } from "node:crypto";

import { approvalTargetIdentityKey } from "@dungle-scrubs/tether-protocol";

import type { ApprovalTarget } from "./types.js";

/** Reserved key used when an approval applies to the whole task. */
export const wholeTaskApprovalTargetKey = "task";

/**
 * Builds the stable approval target key stored in `task_approvals`.
 */
export function approvalTargetKey(reason: unknown, target?: ApprovalTarget | undefined): string {
  if (target !== undefined) {
    const digest = createHash("sha256").update(approvalTargetIdentityKey(target)).digest("hex");
    return `approvalTarget:v2:sha256:${digest}`;
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
