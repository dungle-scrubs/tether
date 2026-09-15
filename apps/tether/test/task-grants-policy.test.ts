import { describe, expect, it } from "vitest";

import type { TaskGrantRecord } from "../src/auth/grant-stores.js";
import {
  checkSingleTaskGrant,
  checkTaskGrantEligibility,
  checkTaskGrantTerminalWrite,
  denyTaskGrantClaimRefresh,
  type TaskGrantClaimAttempt,
  type TaskGrantCreateAttempt,
} from "../src/auth/task-grants-policy.js";

const now = new Date("2026-09-15T00:00:00.000Z");
const issuedAt = new Date("2026-09-14T00:00:00.000Z");
const expiresAt = new Date("2026-09-16T00:00:00.000Z");

function grant(overrides: Partial<TaskGrantRecord> = {}): TaskGrantRecord {
  return {
    action: "task.create",
    createdAuditId: "audit_seed",
    expiresAt,
    issuedAt,
    issuer: "https://auth.tether.test",
    jti: "tgrant_seed",
    kindAllowlist: [],
    revokedAt: null,
    scopeLabelAllowlist: [],
    sessionScope: "*",
    subject: "worker_one",
    ...overrides,
  };
}

function createAttempt(overrides: Partial<TaskGrantCreateAttempt> = {}): TaskGrantCreateAttempt {
  return {
    action: "task.create",
    actorParticipantId: "worker_one",
    assigneeParticipantId: null,
    kind: "build.widget",
    scopeLabel: null,
    sessionId: "sess_one",
    ...overrides,
  };
}

function claimAttempt(overrides: Partial<TaskGrantClaimAttempt> = {}): TaskGrantClaimAttempt {
  return {
    action: "task.claim",
    kind: "build.widget",
    participantId: "worker_one",
    scopeLabel: null,
    sessionId: "sess_one",
    taskAssigneeParticipantId: null,
    ...overrides,
  };
}

describe("task-grants policy eligibility", () => {
  it("reports no_grant for an empty pool", () => {
    expect(checkTaskGrantEligibility([], createAttempt(), now)).toEqual({
      eligible: false,
      reason: "task_grant_no_grant",
    });
  });

  it("authorizes a matching create and claim", () => {
    const create = checkTaskGrantEligibility([grant()], createAttempt(), now);
    expect(create.eligible).toBe(true);
    const claim = checkTaskGrantEligibility(
      [grant({ action: "task.claim", jti: "tgrant_claim" })],
      claimAttempt(),
      now,
    );
    expect(claim.eligible).toBe(true);
  });

  it("denies action, revocation, expiry, and session mismatches", () => {
    expect(checkSingleTaskGrant(grant({ action: "task.claim" }), createAttempt(), now)).toEqual({
      eligible: false,
      reason: "task_grant_action_denied",
    });
    expect(checkSingleTaskGrant(grant({ revokedAt: issuedAt }), createAttempt(), now)).toEqual({
      eligible: false,
      reason: "task_grant_revoked",
    });
    expect(checkSingleTaskGrant(grant({ expiresAt: issuedAt }), createAttempt(), now)).toEqual({
      eligible: false,
      reason: "task_grant_expired",
    });
    expect(
      checkSingleTaskGrant(grant({ sessionScope: "sess_other" }), createAttempt(), now),
    ).toEqual({ eligible: false, reason: "task_grant_session_denied" });
    expect(
      checkSingleTaskGrant(grant({ sessionScope: "sess_one" }), createAttempt(), now).eligible,
    ).toBe(true);
  });

  it("denies kinds outside the allowlist and always reserves operator kinds", () => {
    const scoped = grant({ kindAllowlist: ["build.widget"] });
    expect(checkSingleTaskGrant(scoped, createAttempt(), now).eligible).toBe(true);
    expect(checkSingleTaskGrant(scoped, createAttempt({ kind: "other.kind" }), now)).toEqual({
      eligible: false,
      reason: "task_grant_kind_denied",
    });
    expect(checkSingleTaskGrant(grant(), createAttempt({ kind: "operator.exec" }), now)).toEqual({
      eligible: false,
      reason: "task_grant_kind_reserved",
    });
    expect(
      checkSingleTaskGrant(
        grant({ kindAllowlist: ["operator.exec"] }),
        createAttempt({ kind: "operator.exec" }),
        now,
      ),
    ).toEqual({ eligible: false, reason: "task_grant_kind_reserved" });
  });

  it("binds creates to the assignee-or-actor owner (P1: no admin bypass)", () => {
    const adminIssued = grant({ issuer: "https://admin.tether.test" });
    // Assignee set: the grant subject must equal the assignee, even for admin-issued grants.
    expect(
      checkSingleTaskGrant(adminIssued, createAttempt({ assigneeParticipantId: "worker_one" }), now)
        .eligible,
    ).toBe(true);
    expect(
      checkSingleTaskGrant(
        adminIssued,
        createAttempt({ assigneeParticipantId: "worker_two" }),
        now,
      ),
    ).toEqual({ eligible: false, reason: "task_grant_assignee_denied" });
    // No assignee: the grant subject must equal the creating actor.
    expect(
      checkSingleTaskGrant(adminIssued, createAttempt({ actorParticipantId: "other" }), now),
    ).toEqual({ eligible: false, reason: "task_grant_assignee_denied" });
  });

  it("binds claims to the participant and the task assignee", () => {
    const claimGrant = grant({ action: "task.claim" });
    expect(checkSingleTaskGrant(claimGrant, claimAttempt({ participantId: "other" }), now)).toEqual(
      { eligible: false, reason: "task_grant_assignee_denied" },
    );
    expect(
      checkSingleTaskGrant(
        claimGrant,
        claimAttempt({ taskAssigneeParticipantId: "worker_two" }),
        now,
      ),
    ).toEqual({ eligible: false, reason: "task_grant_assignee_denied" });
    expect(
      checkSingleTaskGrant(
        claimGrant,
        claimAttempt({ taskAssigneeParticipantId: "worker_one" }),
        now,
      ).eligible,
    ).toBe(true);
  });

  it("defers scope labels behind the session-first default (P2)", () => {
    // Unlabelled tasks always pass; empty allowlists match any label.
    expect(
      checkSingleTaskGrant(grant(), createAttempt({ scopeLabel: "team-a" }), now).eligible,
    ).toBe(true);
    // A non-empty allowlist constrains labelled tasks only.
    const scoped = grant({ scopeLabelAllowlist: ["team-a"] });
    expect(checkSingleTaskGrant(scoped, createAttempt(), now).eligible).toBe(true);
    expect(
      checkSingleTaskGrant(scoped, createAttempt({ scopeLabel: "team-a" }), now).eligible,
    ).toBe(true);
    expect(checkSingleTaskGrant(scoped, createAttempt({ scopeLabel: "team-b" }), now)).toEqual({
      eligible: false,
      reason: "task_grant_scope_label_denied",
    });
  });

  it("allows one live-lease terminal write but denies refresh (P4)", () => {
    const claimGrant = grant({ action: "task.claim" });
    const lease = {
      claimExpiresAt: new Date("2026-09-15T01:00:00.000Z"),
      claimId: "claim_1",
      claimedBy: "worker_one",
      kind: "build.widget",
      sessionId: "sess_one",
    };
    expect(checkTaskGrantTerminalWrite(claimGrant, lease, "claim_1", now).eligible).toBe(true);
    expect(checkTaskGrantTerminalWrite(claimGrant, lease, "claim_other", now)).toEqual({
      eligible: false,
      reason: "task_grant_assignee_denied",
    });
    expect(
      checkTaskGrantTerminalWrite(
        claimGrant,
        { ...lease, claimExpiresAt: new Date("2026-09-14T23:00:00.000Z") },
        "claim_1",
        now,
      ),
    ).toEqual({ eligible: false, reason: "task_grant_assignee_denied" });
    expect(denyTaskGrantClaimRefresh()).toEqual({
      eligible: false,
      reason: "task_grant_refresh_denied",
    });
  });
});
