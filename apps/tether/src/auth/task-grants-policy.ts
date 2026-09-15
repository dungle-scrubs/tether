import type { PoolClient } from "pg";

import type { TaskGrantAction, TaskGrantRecord } from "./grant-stores.js";

/**
 * Pure eligibility policy for durable task delegation grants (Phase A).
 *
 * A task grant authorizes one bounded operation (`task.create` or
 * `task.claim`) for one subject inside one session scope. Enforcement stays
 * out of the auth mode: grants never mint credentials, never replace the
 * role/scope matrix in `authorize.ts`, and the operator task namespace is
 * always denied regardless of allowlists.
 *
 * Phase semantics encoded here:
 * - P1: the admin flag never bypasses checks. An admin-issued grant passes
 *   the same kind and assignee checks as any other grant.
 * - P2: scope-label enforcement is deferred behind a session-first default. A
 *   task without a label always passes, and an empty scope-label allowlist
 *   matches any label; only an explicit non-empty allowlist constrains a
 *   labelled task.
 * - P4: a grant authorizes one terminal write under a live claim lease, but
 *   never a lease refresh. Refresh stays bound to the claim-id lease path.
 */

/** Bounded task-grant eligibility denial reasons. */
export type TaskGrantDenialReason =
  | "task_grant_action_denied"
  | "task_grant_assignee_denied"
  | "task_grant_expired"
  | "task_grant_kind_denied"
  | "task_grant_kind_reserved"
  | "task_grant_no_grant"
  | "task_grant_refresh_denied"
  | "task_grant_revoked"
  | "task_grant_scope_label_denied"
  | "task_grant_session_denied";

/** Every task-grant denial reason, for exhaustive validation. */
export const taskGrantDenialReasons = [
  "task_grant_action_denied",
  "task_grant_assignee_denied",
  "task_grant_expired",
  "task_grant_kind_denied",
  "task_grant_kind_reserved",
  "task_grant_no_grant",
  "task_grant_refresh_denied",
  "task_grant_revoked",
  "task_grant_scope_label_denied",
  "task_grant_session_denied",
] as const satisfies readonly TaskGrantDenialReason[];

/** One task.create attempt evaluated against the grant pool. */
export interface TaskGrantCreateAttempt {
  readonly action: "task.create";
  /** Participant performing the create; owns the task when no assignee is set. */
  readonly actorParticipantId: string;
  /** Participant the task is assigned to, or null when unassigned. */
  readonly assigneeParticipantId: string | null;
  /** Task kind requested. */
  readonly kind: string;
  /** Delegation scope label carried by the create, or null when absent. */
  readonly scopeLabel: string | null;
  /** Session the task is created in. */
  readonly sessionId: string;
}

/** One task.claim attempt evaluated against the grant pool. */
export interface TaskGrantClaimAttempt {
  readonly action: "task.claim";
  /** Task kind of the claimed task. */
  readonly kind: string;
  /** Participant performing the claim. */
  readonly participantId: string;
  /** Delegation scope label of the claimed task, or null when absent. */
  readonly scopeLabel: string | null;
  /** Session owning the claimed task. */
  readonly sessionId: string;
  /** Participant the task is assigned to, or null when unassigned. */
  readonly taskAssigneeParticipantId: string | null;
}

/** One task operation attempt evaluated against the grant pool. */
export type TaskGrantAttempt = TaskGrantCreateAttempt | TaskGrantClaimAttempt;

/** Outcome of evaluating one attempt against the grant pool. */
export type TaskGrantEligibility =
  | { readonly eligible: true; readonly grant: TaskGrantRecord }
  | { readonly eligible: false; readonly reason: TaskGrantDenialReason };

/**
 * Evaluates one task attempt against a pool of candidate grants. Pure: the
 * caller supplies the pool (for example from `listLiveForSubject` or from the
 * transaction-integrated check below) and the clock. The first grant that
 * satisfies every dimension wins; otherwise the most specific denial observed
 * is reported. An empty pool reports `task_grant_no_grant`.
 */
export function checkTaskGrantEligibility(
  grants: readonly TaskGrantRecord[],
  attempt: TaskGrantAttempt,
  now: Date,
): TaskGrantEligibility {
  if (grants.length === 0) {
    return { eligible: false, reason: "task_grant_no_grant" };
  }
  let denial: TaskGrantDenialReason = "task_grant_no_grant";
  for (const grant of grants) {
    const outcome = checkSingleTaskGrant(grant, attempt, now);
    if (outcome.eligible) {
      return outcome;
    }
    denial = outcome.reason;
  }
  return { eligible: false, reason: denial };
}

/**
 * Evaluates one attempt against a single grant across every enforced
 * dimension: action, revocation, expiry, session scope, kind (with the
 * operator namespace always reserved), assignee binding, and scope label.
 */
export function checkSingleTaskGrant(
  grant: TaskGrantRecord,
  attempt: TaskGrantAttempt,
  now: Date,
): TaskGrantEligibility {
  if (grant.action !== attempt.action) {
    return { eligible: false, reason: "task_grant_action_denied" };
  }
  if (grant.revokedAt !== null) {
    return { eligible: false, reason: "task_grant_revoked" };
  }
  if (!(grant.expiresAt > now)) {
    return { eligible: false, reason: "task_grant_expired" };
  }
  if (grant.sessionScope !== "*" && grant.sessionScope !== attempt.sessionId) {
    return { eligible: false, reason: "task_grant_session_denied" };
  }
  const kind = attempt.kind;
  if (kind.startsWith("operator.")) {
    return { eligible: false, reason: "task_grant_kind_reserved" };
  }
  if (grant.kindAllowlist.length > 0 && !grant.kindAllowlist.includes(kind)) {
    return { eligible: false, reason: "task_grant_kind_denied" };
  }
  const assigneeDenial = checkTaskGrantAssignee(grant, attempt);
  if (assigneeDenial !== null) {
    return { eligible: false, reason: assigneeDenial };
  }
  const scopeLabel = attempt.scopeLabel;
  if (
    scopeLabel !== null &&
    grant.scopeLabelAllowlist.length > 0 &&
    !grant.scopeLabelAllowlist.includes(scopeLabel)
  ) {
    return { eligible: false, reason: "task_grant_scope_label_denied" };
  }
  return { eligible: true, grant };
}

/**
 * Enforces the subject/assignee binding. A grant is issued to the identity
 * that owns the work: for a create, the assignee when one is set, otherwise
 * the creating actor; for a claim, the claiming participant, who must also
 * equal the task assignee when the task names one. The admin flag plays no
 * role here (P1): admin-issued grants pass the same binding checks.
 */
function checkTaskGrantAssignee(
  grant: TaskGrantRecord,
  attempt: TaskGrantAttempt,
): TaskGrantDenialReason | null {
  if (attempt.action === "task.create") {
    const owner = attempt.assigneeParticipantId ?? attempt.actorParticipantId;
    return grant.subject === owner ? null : "task_grant_assignee_denied";
  }
  if (grant.subject !== attempt.participantId) {
    return "task_grant_assignee_denied";
  }
  if (
    attempt.taskAssigneeParticipantId !== null &&
    attempt.taskAssigneeParticipantId !== attempt.participantId
  ) {
    return "task_grant_assignee_denied";
  }
  return null;
}

/** Minimal query seam for the transaction-integrated grant check. */
export interface TaskGrantQueryClient {
  readonly query: (
    text: string,
    values: readonly unknown[],
  ) => Promise<{ readonly rows: readonly TaskGrantRow[] }>;
}

/** Raw `task_grants` row as returned by the transaction-integrated check. */
export interface TaskGrantRow {
  readonly action: string;
  readonly createdAuditId: string;
  readonly expiresAt: Date;
  readonly issuedAt: Date;
  readonly issuer: string;
  readonly jti: string;
  readonly kindAllowlist: unknown;
  readonly revokedAt: Date | null;
  readonly scopeLabelAllowlist: unknown;
  readonly sessionScope: string;
  readonly subject: string;
}

/**
 * Transaction-integrated eligibility check. Reads the candidate grant pool on
 * the caller's transaction client — same-session create/claim admission runs
 * inside the task write transaction — then evaluates the pure policy.
 * Revoked, expired, and out-of-scope rows are deliberately INCLUDED so the
 * pure check can classify them into the typed `grant_revoked`,
 * `grant_expired`, and `unauthorized_scope` denials; the surviving live
 * candidates are re-validated by the pure check so a row that elapsed
 * mid-transaction still fails closed.
 */
export async function checkTaskGrantWithClient(
  client: TaskGrantQueryClient | PoolClient,
  attempt: TaskGrantAttempt,
  now: Date,
): Promise<TaskGrantEligibility> {
  const subject =
    attempt.action === "task.create"
      ? (attempt.assigneeParticipantId ?? attempt.actorParticipantId)
      : attempt.participantId;
  const result = await client.query(
    `SELECT
       action,
       created_audit_id AS "createdAuditId",
       expires_at AS "expiresAt",
       issued_at AS "issuedAt",
       issuer,
       jti,
       kind_allowlist AS "kindAllowlist",
       revoked_at AS "revokedAt",
       scope_label_allowlist AS "scopeLabelAllowlist",
       session_scope AS "sessionScope",
       subject
     FROM task_grants
     WHERE subject = $1
       AND action = $2`,
    [subject, attempt.action],
  );
  const grants: TaskGrantRecord[] = [];
  for (const row of result.rows) {
    const grant = parseTaskGrantRow(row);
    if (grant !== null) {
      grants.push(grant);
    }
  }
  return checkTaskGrantEligibility(grants, attempt, now);
}

/** Parses one raw task-grant row, dropping malformed rows fail-closed. */
function parseTaskGrantRow(row: TaskGrantRow): TaskGrantRecord | null {
  if (
    typeof row.jti !== "string" ||
    typeof row.subject !== "string" ||
    typeof row.sessionScope !== "string" ||
    typeof row.issuer !== "string" ||
    typeof row.createdAuditId !== "string" ||
    (row.action !== "task.create" && row.action !== "task.claim") ||
    !(row.issuedAt instanceof Date) ||
    !(row.expiresAt instanceof Date) ||
    (row.revokedAt !== null && !(row.revokedAt instanceof Date)) ||
    !Array.isArray(row.kindAllowlist) ||
    !Array.isArray(row.scopeLabelAllowlist)
  ) {
    return null;
  }
  const kindAllowlist = row.kindAllowlist.filter(
    (entry): entry is string => typeof entry === "string",
  );
  const scopeLabelAllowlist = row.scopeLabelAllowlist.filter(
    (entry): entry is string => typeof entry === "string",
  );
  const action: TaskGrantAction = row.action;
  return {
    action,
    createdAuditId: row.createdAuditId,
    expiresAt: row.expiresAt,
    issuedAt: row.issuedAt,
    issuer: row.issuer,
    jti: row.jti,
    kindAllowlist,
    revokedAt: row.revokedAt,
    scopeLabelAllowlist,
    sessionScope: row.sessionScope,
    subject: row.subject,
  };
}

/** Live claim lease view a grant-authorized terminal write is checked against. */
export interface TaskGrantClaimLease {
  /** Server-issued identity of the current claim generation. */
  readonly claimId: string;
  /** Lease expiry; the write is allowed only while this is in the future. */
  readonly claimExpiresAt: Date | null;
  /** Participant holding the claim. */
  readonly claimedBy: string | null;
  /** Terminal-write kind the claim was taken under. */
  readonly kind: string;
  /** Session owning the claimed task. */
  readonly sessionId: string;
}

/**
 * Authorizes one grant-backed terminal write (complete/fail) under a live
 * claim lease (P4). The grant must be live and cover the claim's session and
 * kind, and the lease must be live and held by the grant subject. Refresh is
 * never authorized here; see `denyTaskGrantClaimRefresh`.
 */
export function checkTaskGrantTerminalWrite(
  grant: TaskGrantRecord,
  lease: TaskGrantClaimLease,
  claimId: string,
  now: Date,
): TaskGrantEligibility {
  if (grant.revokedAt !== null) {
    return { eligible: false, reason: "task_grant_revoked" };
  }
  if (!(grant.expiresAt > now)) {
    return { eligible: false, reason: "task_grant_expired" };
  }
  if (grant.sessionScope !== "*" && grant.sessionScope !== lease.sessionId) {
    return { eligible: false, reason: "task_grant_session_denied" };
  }
  if (lease.kind.startsWith("operator.")) {
    return { eligible: false, reason: "task_grant_kind_reserved" };
  }
  if (grant.kindAllowlist.length > 0 && !grant.kindAllowlist.includes(lease.kind)) {
    return { eligible: false, reason: "task_grant_kind_denied" };
  }
  if (
    lease.claimedBy === null ||
    lease.claimedBy !== grant.subject ||
    lease.claimId !== claimId ||
    lease.claimExpiresAt === null ||
    !(lease.claimExpiresAt > now)
  ) {
    return { eligible: false, reason: "task_grant_assignee_denied" };
  }
  return { eligible: true, grant };
}

/**
 * Denies every grant-backed claim-lease refresh (P4). A grant authorizes at
 * most one terminal write under the live lease it helped acquire; extending
 * the lease requires the claim-id lease path, never grant authority.
 */
export function denyTaskGrantClaimRefresh(): TaskGrantEligibility {
  return { eligible: false, reason: "task_grant_refresh_denied" };
}

/**
 * Public task-grant enforcement denial reasons (Phase B). These are the only
 * denial codes that cross a service boundary: the internal `task_grant_*`
 * reasons stay inside the policy, while claim/create/refresh denials surface
 * one of these five codes. They are deliberately distinct from the race-loss
 * `rejected` outcome, which carries no reason and commits zero events.
 */
export type TaskGrantEnforcementDenial =
  | "unauthorized_kind"
  | "unauthorized_scope"
  | "unauthorized_assignee"
  | "grant_expired"
  | "grant_revoked";

/** Every public task-grant enforcement denial reason, for exhaustive handling. */
export const taskGrantEnforcementDenials = [
  "unauthorized_kind",
  "unauthorized_scope",
  "unauthorized_assignee",
  "grant_expired",
  "grant_revoked",
] as const satisfies readonly TaskGrantEnforcementDenial[];

/**
 * Maps one internal eligibility denial onto the public enforcement taxonomy:
 * kind allowlist and operator-namespace reservations become `unauthorized_kind`,
 * session and scope-label mismatches become `unauthorized_scope`, expiry and
 * revocation keep their own codes, and every identity/binding failure
 * (assignee, action, empty pool, refresh authority) becomes
 * `unauthorized_assignee`.
 */
export function toTaskGrantEnforcementDenial(
  reason: TaskGrantDenialReason,
): TaskGrantEnforcementDenial {
  switch (reason) {
    case "task_grant_kind_denied":
    case "task_grant_kind_reserved":
      return "unauthorized_kind";
    case "task_grant_scope_label_denied":
    case "task_grant_session_denied":
      return "unauthorized_scope";
    case "task_grant_expired":
      return "grant_expired";
    case "task_grant_revoked":
      return "grant_revoked";
    case "task_grant_assignee_denied":
    case "task_grant_action_denied":
    case "task_grant_no_grant":
    case "task_grant_refresh_denied":
      return "unauthorized_assignee";
  }
}

/**
 * Renders a public enforcement denial into a stable caller-facing message
 * shared by HTTP and WebSocket boundaries.
 */
export function describeTaskGrantEnforcementDenial(reason: TaskGrantEnforcementDenial): string {
  switch (reason) {
    case "unauthorized_kind":
      return "Task kind is not authorized by a task grant";
    case "unauthorized_scope":
      return "Task scope is not authorized by a task grant";
    case "unauthorized_assignee":
      return "Participant is not authorized by a task grant";
    case "grant_expired":
      return "Task grant has expired";
    case "grant_revoked":
      return "Task grant has been revoked";
  }
}

/** Error thrown inside a task write transaction when a grant denies the attempt. */
export class TaskGrantDeniedError extends Error {
  readonly name = "TaskGrantDeniedError";

  constructor(readonly reason: TaskGrantEnforcementDenial) {
    super(`Task grant denied: ${reason}`);
  }
}

/** Outcome of the single transaction-integrated enforcement check. */
export type TaskGrantEnforcement =
  | { readonly grant: TaskGrantRecord | null; readonly status: "allowed" }
  | { readonly reason: TaskGrantEnforcementDenial; readonly status: "denied" };

/** Options for the single transaction-integrated enforcement check. */
export interface TaskGrantEnforcementOptions {
  /**
   * Auth mode of the calling service. `disabled` never enforces: every attempt
   * is allowed without touching the grant table.
   */
  readonly authMode?: "disabled" | "required" | undefined;
}

/**
 * Single transaction-integrated enforcement funnel shared by every task
 * create/claim/refresh path (HTTP, WebSocket, and REST converge on the same
 * store functions, so one check here covers all three with no route fork).
 *
 * Passthrough preserves current behavior: auth-disabled callers are always
 * allowed, and when the grant table holds no policy rows every attempt is
 * allowed without evaluating eligibility. Otherwise the attempt is evaluated
 * against the caller's live grant pool and denied with a typed public reason.
 * Parent-child linkage (`parentTaskId`) is lineage only and never consults
 * grants; grants are only ever issued through the owner/admin lifecycle, never
 * minted here.
 */
export async function enforceTaskGrantPolicyWithClient(
  client: TaskGrantQueryClient,
  attempt: TaskGrantAttempt,
  now: Date,
  options: TaskGrantEnforcementOptions = {},
): Promise<TaskGrantEnforcement> {
  if (options.authMode === "disabled") {
    return { grant: null, status: "allowed" };
  }
  if (!(await hasTaskGrantPolicyRows(client))) {
    return { grant: null, status: "allowed" };
  }
  const eligibility = await checkTaskGrantWithClient(client, attempt, now);
  if (eligibility.eligible) {
    return { grant: eligibility.grant, status: "allowed" };
  }
  return { reason: toTaskGrantEnforcementDenial(eligibility.reason), status: "denied" };
}

/**
 * Reports whether the grant table holds any policy rows. The probe is a plain
 * row-returning read (not `SELECT EXISTS`) so scripted doubles that answer
 * unknown queries with zero rows read as no rows and keep current behavior; a
 * missing table (a database that predates the task-grants migration) likewise
 * counts as no rows instead of failing the write.
 */
export async function hasTaskGrantPolicyRows(client: TaskGrantQueryClient): Promise<boolean> {
  try {
    const result = await client.query(`SELECT 1 AS "present" FROM task_grants LIMIT 1`, []);
    return result.rows.length > 0;
  } catch (error) {
    if ((error as { readonly code?: unknown } | null)?.code === "42P01") {
      return false;
    }
    throw error;
  }
}
