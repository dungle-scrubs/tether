import { randomUUID } from "node:crypto";

import type {
  AuthGrantRecord,
  AuthPersistenceStores,
  AuthGrantSource,
  TaskGrantAction,
  TaskGrantRecord,
} from "./grant-stores.js";
import { taskGrantActions } from "./grant-stores.js";
import {
  maximumAuthGrantLifetimeSeconds,
  mintAuthGrantToken,
  verifyAuthGrantToken,
} from "./grant-token.js";
import { wholeAuthSecond } from "./opaque-credential.js";
import type { AuthRole, AuthSigningSecrets } from "./token.js";

/** Secret-free durable grant representation exposed by lifecycle boundaries. */
export type PublicAuthGrant = Omit<
  AuthGrantRecord,
  "metadata" | "expiresAt" | "issuedAt" | "revokedAt"
> & {
  readonly expiresAt: string;
  readonly issuedAt: string;
  readonly revokedAt: string | null;
};

/** One-time grant creation response. The bearer must never be persisted or logged. */
export interface CreatedAuthGrant {
  /** Raw one-time credential returned only from successful creation. */
  readonly bearer: string;
  /** Public durable authority metadata. */
  readonly grant: PublicAuthGrant;
}

/** Validated lifecycle creation command. */
export interface CreateAuthGrantInput {
  /** Authenticated operator or fixed bootstrap actor. */
  readonly actorSubject: string;
  /** Bounded reason recorded in the durable audit. */
  readonly reasonCode: "bootstrap" | "key-rotation" | "operator-request" | "security-response";
  /** Authority role granted to the subject. */
  readonly role: AuthRole;
  /** Durable session id or service-wide `*`. */
  readonly sessionScope: string;
  /** Bounded origin recorded with the grant. */
  readonly source: AuthGrantSource;
  /** Durable identity receiving the grant. */
  readonly subject: string;
  /** Optional lifetime in seconds, bounded to seven days. */
  readonly ttlSeconds?: number;
}

/** Deep lifecycle interface used by REST and database-aware bootstrap boundaries. */
export interface AuthGrantLifecycle {
  /** Reports whether the provisional rollout gate permits tgr2 issuance. */
  readonly issuanceEnabled: boolean;
  /** Atomically creates audited authority and returns its bearer once. */
  readonly create: (input: CreateAuthGrantInput) => Promise<CreatedAuthGrant>;
  /** Reads public metadata for one durable grant. */
  readonly inspect: (jti: string) => Promise<PublicAuthGrant | null>;
  /** Lists a bounded newest-first page of public grant metadata. */
  readonly list: (limit: number) => Promise<readonly PublicAuthGrant[]>;
  /** Idempotently revokes a grant and audits only the first transition. */
  readonly revoke: (
    jti: string,
    actorSubject: string,
    reasonCode: "key-rotation" | "operator-request" | "security-response",
  ) => Promise<{
    readonly grant: PublicAuthGrant | null;
    readonly status: "already_revoked" | "not_found" | "revoked";
  }>;
}

/** Runtime dependencies and policy needed by the grant lifecycle. */
export interface AuthGrantLifecycleOptions {
  /** Key id used for new grants. */
  readonly activeKid: string;
  /** Required configured issuer, or null when issuance is unavailable. */
  readonly issuer: string | null;
  /** Explicit rollout gate that remains false until every replica can verify tgr2 grants. */
  readonly issuanceEnabled?: boolean;
  /** Deterministic lifecycle clock. */
  readonly now?: () => Date;
  /** Accepted signing secrets keyed by key id. */
  readonly secrets: AuthSigningSecrets;
  /** Transaction-owning persistence interface. */
  readonly stores: AuthPersistenceStores;
}

/** Owns token minting and the matching atomic durable lifecycle transitions. */
export function createAuthGrantLifecycle(options: AuthGrantLifecycleOptions): AuthGrantLifecycle {
  const now = options.now ?? (() => new Date());
  return {
    create: async (input) => {
      if (!options.issuanceEnabled || !options.issuer || !options.secrets[options.activeKid]) {
        throw new Error("auth_grant_issuance_unavailable");
      }
      const occurredAt = wholeAuthSecond(now());
      const jti = `grant_${randomUUID()}`;
      const bearer = mintAuthGrantToken(
        {
          issuer: options.issuer,
          jti,
          kid: options.activeKid,
          role: input.role,
          sessionScope: input.sessionScope,
          subject: input.subject,
          ...(input.ttlSeconds === undefined ? {} : { ttlSeconds: input.ttlSeconds }),
        },
        options.secrets,
        { now: occurredAt },
      );
      const claims = verifyAuthGrantToken(bearer, {
        audience: "tether-rest",
        issuer: options.issuer,
        now: occurredAt,
        secrets: options.secrets,
      });
      const requestId = `req_${randomUUID()}`;
      const grant: AuthGrantRecord = {
        audience: claims.aud,
        expiresAt: new Date(claims.exp * 1_000),
        issuedAt: new Date(claims.iat * 1_000),
        issuer: claims.iss,
        jti: claims.jti,
        kid: claims.kid,
        metadata: { requestId, source: input.source },
        revokedAt: null,
        role: claims.role,
        sessionScope: claims.sessionScope,
        subject: claims.sub,
      };
      await options.stores.createGrantWithAudit({
        audit: {
          action: "grant.created",
          actorSubject: input.actorSubject,
          auditId: `audit_${randomUUID()}`,
          metadata: { requestId },
          occurredAt,
          reasonCode: input.reasonCode,
        },
        grant,
      });
      return { bearer, grant: toPublicAuthGrant(grant) };
    },
    inspect: async (jti) => {
      const grant = await options.stores.grants.findByJti(jti);
      return grant === null ? null : toPublicAuthGrant(grant);
    },
    issuanceEnabled: options.issuanceEnabled ?? false,
    list: async (limit) => (await options.stores.grants.list(limit)).map(toPublicAuthGrant),
    revoke: async (jti, actorSubject, reasonCode) => {
      const occurredAt = wholeAuthSecond(now());
      const result = await options.stores.revokeGrantWithAudit({
        audit: {
          action: "grant.revoked",
          actorSubject,
          auditId: `audit_${randomUUID()}`,
          metadata: { requestId: `req_${randomUUID()}` },
          occurredAt,
          reasonCode,
        },
        jti,
        revokedAt: occurredAt,
      });
      return {
        grant: result.grant === null ? null : toPublicAuthGrant(result.grant),
        status: result.status,
      };
    },
  };
}

/** Actor attestation for task-grant lifecycle operations. */
export interface TaskGrantActor {
  /** Authenticated subject performing the operation. */
  readonly subject: string;
  /**
   * True only for a service-scoped (`*`) admin. Mirrors the auth.grants HTTP
   * authorize rule: omitting a session scope admits only service-wide admins,
   * so a session-scoped admin can never mint, list, inspect, or revoke task
   * grants. Phase A trusts the caller to attest this from its auth context;
   * the auth mode itself is unchanged.
   */
  readonly serviceAdmin: boolean;
}

/** Secret-free durable task-grant representation exposed by lifecycle boundaries. */
export type PublicTaskGrant = Omit<TaskGrantRecord, "expiresAt" | "issuedAt" | "revokedAt"> & {
  readonly expiresAt: string;
  readonly issuedAt: string;
  readonly revokedAt: string | null;
};

/** Validated task-grant lifecycle creation command. */
export interface CreateTaskGrantInput {
  /** Bounded task operation the grant authorizes. */
  readonly action: TaskGrantAction;
  /** Service-scoped admin performing the mint. */
  readonly actor: TaskGrantActor;
  /** Allowed task kinds; empty matches any non-operator kind. */
  readonly kindAllowlist?: readonly string[];
  /** Bounded reason recorded in the durable audit. */
  readonly reasonCode: "bootstrap" | "key-rotation" | "operator-request" | "security-response";
  /** Allowed delegation scope labels; empty matches any label. */
  readonly scopeLabelAllowlist?: readonly string[];
  /** Durable session id or service-wide `*`. */
  readonly sessionScope: string;
  /** Durable identity receiving the grant. */
  readonly subject: string;
  /** Optional lifetime in seconds, bounded to seven days. */
  readonly ttlSeconds?: number;
}

/** Deep task-grant lifecycle interface mirroring the auth-grant lifecycle. */
export interface TaskGrantLifecycle {
  /** Reports whether task-grant issuance is enabled. */
  readonly issuanceEnabled: boolean;
  /** Atomically creates audited task-grant authority. No credential is minted: the grant is pure metadata. */
  readonly create: (input: CreateTaskGrantInput) => Promise<PublicTaskGrant>;
  /** Reads public metadata for one durable task grant. */
  readonly inspect: (actor: TaskGrantActor, jti: string) => Promise<PublicTaskGrant | null>;
  /** Lists a bounded newest-first page of public task-grant metadata. */
  readonly list: (actor: TaskGrantActor, limit: number) => Promise<readonly PublicTaskGrant[]>;
  /** Idempotently revokes a task grant and audits only the first transition. */
  readonly revoke: (
    actor: TaskGrantActor,
    jti: string,
    reasonCode: "key-rotation" | "operator-request" | "security-response",
  ) => Promise<{
    readonly grant: PublicTaskGrant | null;
    readonly status: "already_revoked" | "not_found" | "revoked";
  }>;
}

/** Runtime dependencies and policy needed by the task-grant lifecycle. */
export interface TaskGrantLifecycleOptions {
  /** Required configured issuer, or null when issuance is unavailable. */
  readonly issuer: string | null;
  /** Explicit opt-in gate mirroring the tgr2 issuance rollout discipline. */
  readonly issuanceEnabled?: boolean;
  /** Deterministic lifecycle clock. */
  readonly now?: () => Date;
  /** Transaction-owning persistence interface. */
  readonly stores: AuthPersistenceStores;
}

/** Owns the atomic durable task-grant lifecycle transitions. */
export function createTaskGrantLifecycle(options: TaskGrantLifecycleOptions): TaskGrantLifecycle {
  const now = options.now ?? (() => new Date());
  return {
    create: async (input) => {
      assertTaskGrantServiceAdmin(input.actor);
      if (options.issuanceEnabled !== true || !options.issuer) {
        throw new Error("task_grant_issuance_unavailable");
      }
      const issuedAt = wholeAuthSecond(now());
      const grant: TaskGrantRecord = {
        action: input.action,
        createdAuditId: `audit_${randomUUID()}`,
        expiresAt: resolveTaskGrantExpiry(issuedAt, input.ttlSeconds),
        issuedAt,
        issuer: options.issuer,
        jti: `tgrant_${randomUUID()}`,
        kindAllowlist: [...(input.kindAllowlist ?? [])],
        revokedAt: null,
        scopeLabelAllowlist: [...(input.scopeLabelAllowlist ?? [])],
        sessionScope: input.sessionScope,
        subject: input.subject,
      };
      validateTaskGrantCreationInput({ ...input, grant });
      validateTaskGrantRecord(grant);
      const occurredAt = grant.issuedAt;
      await options.stores.createTaskGrantWithAudit({
        audit: {
          action: "task_grant.created",
          actorSubject: input.actor.subject,
          auditId: grant.createdAuditId,
          metadata: { requestId: `req_${randomUUID()}` },
          occurredAt,
          reasonCode: input.reasonCode,
        },
        grant,
      });
      return toPublicTaskGrant(grant);
    },
    inspect: async (actor, jti) => {
      assertTaskGrantServiceAdmin(actor);
      const grant = await options.stores.taskGrants.findByJti(jti);
      return grant === null ? null : toPublicTaskGrant(grant);
    },
    issuanceEnabled: options.issuanceEnabled ?? false,
    list: async (actor, limit) => {
      assertTaskGrantServiceAdmin(actor);
      return (await options.stores.taskGrants.list(limit)).map(toPublicTaskGrant);
    },
    revoke: async (actor, jti, reasonCode) => {
      assertTaskGrantServiceAdmin(actor);
      const occurredAt = wholeAuthSecond(now());
      const result = await options.stores.revokeTaskGrantWithAudit({
        audit: {
          action: "task_grant.revoked",
          actorSubject: actor.subject,
          auditId: `audit_${randomUUID()}`,
          metadata: { requestId: `req_${randomUUID()}` },
          occurredAt,
          reasonCode,
        },
        jti,
        revokedAt: occurredAt,
      });
      return {
        grant: result.grant === null ? null : toPublicTaskGrant(result.grant),
        status: result.status,
      };
    },
  };
}

/** Projects the only task-grant fields permitted outside the lifecycle module. */
export function toPublicTaskGrant(record: TaskGrantRecord): PublicTaskGrant {
  return {
    ...record,
    expiresAt: record.expiresAt.toISOString(),
    issuedAt: record.issuedAt.toISOString(),
    kindAllowlist: [...record.kindAllowlist],
    revokedAt: record.revokedAt?.toISOString() ?? null,
    scopeLabelAllowlist: [...record.scopeLabelAllowlist],
  };
}

/** Rejects task-grant lifecycle calls that are not attested service admins. */
function assertTaskGrantServiceAdmin(actor: TaskGrantActor): void {
  if (!actor.serviceAdmin) {
    throw new Error("task_grant_admin_required");
  }
}

/** Maximum task-grant allowlist entries accepted in one mint. */
const maximumTaskGrantAllowlistEntries = 64;

/** Validates one task-grant creation command before any expiry is resolved. */
function validateTaskGrantCreationInput(
  input: CreateTaskGrantInput & { grant: TaskGrantRecord },
): void {
  if (!(taskGrantActions as readonly string[]).includes(input.action)) {
    throw new Error("task_grant_action_invalid");
  }
  validateTaskGrantSubject(input.subject);
  validateTaskGrantSessionScope(input.sessionScope);
  validateTaskGrantAllowlist(input.grant.kindAllowlist, 255, "task_grant_kind_invalid");
  validateTaskGrantAllowlist(
    input.grant.scopeLabelAllowlist,
    128,
    "task_grant_scope_label_invalid",
  );
  for (const kind of input.grant.kindAllowlist) {
    if (kind.startsWith("operator.")) {
      throw new Error("task_grant_kind_reserved");
    }
  }
  if (input.ttlSeconds !== undefined) {
    if (!Number.isSafeInteger(input.ttlSeconds) || input.ttlSeconds <= 0) {
      throw new Error("task_grant_ttl_invalid");
    }
    if (input.ttlSeconds > maximumAuthGrantLifetimeSeconds) {
      throw new Error("task_grant_ttl_invalid");
    }
  }
}

/** Validates one allowlist shape before persistence. */
function validateTaskGrantAllowlist(
  allowlist: readonly string[],
  maximumEntryLength: number,
  code: string,
): void {
  if (allowlist.length > maximumTaskGrantAllowlistEntries) {
    throw new Error(code);
  }
  for (const entry of allowlist) {
    if (typeof entry !== "string" || entry.length < 1 || entry.length > maximumEntryLength) {
      throw new Error(code);
    }
  }
}

/** Validates a task-grant subject without persisting credential-shaped values. */
function validateTaskGrantSubject(subject: string): void {
  if (typeof subject !== "string" || subject.length < 1 || subject.length > 255) {
    throw new Error("task_grant_subject_invalid");
  }
}

/** Validates a task-grant session scope. */
function validateTaskGrantSessionScope(sessionScope: string): void {
  if (typeof sessionScope !== "string" || sessionScope.length < 1 || sessionScope.length > 255) {
    throw new Error("task_grant_session_scope_invalid");
  }
}

/** Resolves the bounded expiry for one task-grant mint. */
function resolveTaskGrantExpiry(issuedAt: Date, ttlSeconds?: number): Date {
  const lifetimeSeconds = ttlSeconds ?? 86_400;
  return new Date(issuedAt.getTime() + lifetimeSeconds * 1_000);
}

/** Validates one fully resolved task-grant record before persistence. */
function validateTaskGrantRecord(grant: TaskGrantRecord): void {
  if (!(grant.expiresAt > grant.issuedAt)) {
    throw new Error("task_grant_expiry_invalid");
  }
  if (
    grant.expiresAt.getTime() - grant.issuedAt.getTime() >
    maximumAuthGrantLifetimeSeconds * 1_000
  ) {
    throw new Error("task_grant_ttl_invalid");
  }
  if (grant.issuer.length < 1 || grant.issuer.length > 512) {
    throw new Error("task_grant_issuer_invalid");
  }
  if (!/^tgrant_[A-Za-z0-9_-]{1,120}$/u.test(grant.jti)) {
    throw new Error("task_grant_jti_invalid");
  }
}

/** Projects the only grant fields permitted outside the lifecycle module. */
export function toPublicAuthGrant(record: AuthGrantRecord): PublicAuthGrant {
  const { metadata: _metadata, ...grant } = record;
  return {
    ...grant,
    expiresAt: record.expiresAt.toISOString(),
    issuedAt: record.issuedAt.toISOString(),
    revokedAt: record.revokedAt?.toISOString() ?? null,
  };
}

export { maximumAuthGrantLifetimeSeconds };
