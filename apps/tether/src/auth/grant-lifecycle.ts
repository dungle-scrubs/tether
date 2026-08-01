import { randomUUID } from "node:crypto";

import type { AuthGrantRecord, AuthPersistenceStores, AuthGrantSource } from "./grant-stores.js";
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
