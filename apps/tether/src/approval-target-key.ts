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
 * Builds the approval target key stored in `task_approvals` for one typed
 * provider-neutral target.
 */
export function approvalTargetKeyFromTarget(target: ApprovalTarget): string {
  const digest = createHash("sha256").update(approvalTargetIdentityKey(target)).digest("hex");
  return `approvalTarget:v2:sha256:${digest}`;
}

/**
 * Builds the approval target key for an approval that carries no typed target,
 * reading the pre-manifest `reason.approvalTarget` shape still present in
 * durable history. New callers should supply a typed target instead.
 */
export function legacyApprovalTargetKey(reason: unknown): string {
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
