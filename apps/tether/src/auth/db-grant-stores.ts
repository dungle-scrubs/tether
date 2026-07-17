import { and, asc, eq, isNull } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";

import type { DatabasePool } from "../db.js";
import type * as schema from "../schema.js";
import { authGrantAuditEvents, authGrants, authTickets } from "../schema.js";
import type {
  AuthGrantAuditMetadata,
  AuthGrantAuditRecord,
  AuthGrantMetadata,
  AuthGrantRecord,
  AuthGrantStore,
  AuthPersistenceStores,
  AuthTicketAdmissionMetadata,
  AuthTicketRecord,
  AuthTicketStore,
  CreateAuthGrantWithAuditInput,
  RevokeAuthGrantResult,
  RevokeAuthGrantWithAuditInput,
} from "./grant-stores.js";
import { AuthPersistenceError, type AuthPersistenceErrorCode } from "./grant-stores.js";
import { authGrantAuditReasonCodes } from "./grant-stores.js";
import { maximumAuthTicketAdmissionLifetimeMilliseconds } from "./grant-stores.js";
import { type AuthAudience, maximumAuthGrantLifetimeSeconds } from "./grant-token.js";
import type { AuthRole } from "./token.js";

type AuthStoreDatabase = Pick<NodePgDatabase<typeof schema>, "insert" | "select" | "update">;

const maximumAuditListLimit = 100;
const opaqueRequestIdPattern = /^req_[A-Za-z0-9_-]{1,120}$/u;
const replicaIdPattern = /^replica_[A-Za-z0-9_-]{1,120}$/u;
const sha256HexPattern = /^[0-9a-f]{64}$/u;

/** Builds narrow authentication stores over the shared Drizzle database handle. */
export function createAuthPersistenceStores(database: DatabasePool): AuthPersistenceStores {
  return {
    audits: createAuditStore(database.db),
    createGrantWithAudit: (input) => createGrantWithAudit(database, input),
    grants: createGrantStore(database.db),
    revokeGrantWithAudit: (input) => revokeGrantWithAudit(database, input),
    tickets: createTicketStore(database.db),
  };
}

/** Atomically creates a grant and its required audit without exposing a transaction callback. */
async function createGrantWithAudit(
  database: DatabasePool,
  input: CreateAuthGrantWithAuditInput,
): Promise<void> {
  validateGrantRecord(input.grant);
  validateAuditInput(input.audit, "grant.created");
  await runAuthStoreOperation(
    () =>
      database.db.transaction(async (transaction) => {
        await transaction.insert(authGrants).values(input.grant);
        await transaction.insert(authGrantAuditEvents).values({
          ...input.audit,
          grantJti: input.grant.jti,
        });
      }),
    "auth_grant_create_failed",
  );
}

/** Atomically revokes one grant and appends one audit only for the first revocation. */
async function revokeGrantWithAudit(
  database: DatabasePool,
  input: RevokeAuthGrantWithAuditInput,
): Promise<RevokeAuthGrantResult> {
  validateAuditInput(input.audit, "grant.revoked");
  return runAuthStoreOperation(
    () =>
      database.db.transaction(async (transaction) => {
        const revoked = await transaction
          .update(authGrants)
          .set({ revokedAt: input.revokedAt })
          .where(and(eq(authGrants.jti, input.jti), isNull(authGrants.revokedAt)))
          .returning({ jti: authGrants.jti });
        if (revoked[0] !== undefined) {
          await transaction.insert(authGrantAuditEvents).values({
            ...input.audit,
            grantJti: input.jti,
          });
          return "revoked";
        }
        const existing = await transaction
          .select({ revokedAt: authGrants.revokedAt })
          .from(authGrants)
          .where(eq(authGrants.jti, input.jti))
          .limit(1);
        return existing[0] === undefined ? "not_found" : "already_revoked";
      }),
    "auth_grant_revoke_failed",
  );
}

/** Creates the durable-grant read adapter. */
function createGrantStore(database: AuthStoreDatabase): AuthGrantStore {
  return {
    findByJti: async (jti) => {
      const rows = await runAuthStoreOperation(
        () => database.select().from(authGrants).where(eq(authGrants.jti, jti)).limit(1),
        "auth_grant_read_failed",
      );
      const row = rows[0];
      return row === undefined ? null : parseGrantRecord(row);
    },
  };
}

/** Creates the bounded grant-audit read adapter. */
function createAuditStore(database: AuthStoreDatabase): AuthPersistenceStores["audits"] {
  return {
    listForGrant: async (grantJti, limit) => {
      if (!Number.isSafeInteger(limit) || limit <= 0 || limit > maximumAuditListLimit) {
        throw new AuthPersistenceError("auth_audit_limit_invalid");
      }
      const rows = await runAuthStoreOperation(
        () =>
          database
            .select()
            .from(authGrantAuditEvents)
            .where(eq(authGrantAuditEvents.grantJti, grantJti))
            .orderBy(asc(authGrantAuditEvents.occurredAt), asc(authGrantAuditEvents.auditId))
            .limit(limit),
        "auth_audit_list_failed",
      );
      return rows as readonly AuthGrantAuditRecord[];
    },
  };
}

/** Creates the validated hashed-ticket adapter. */
function createTicketStore(database: AuthStoreDatabase): AuthTicketStore {
  return {
    create: async (record) => {
      validateTicketRecord(record);
      await runAuthStoreOperation(
        () => database.insert(authTickets).values(record),
        "auth_ticket_create_failed",
      );
    },
    findByHash: async (ticketHash) => {
      validateTicketHash(ticketHash);
      const rows = await runAuthStoreOperation(
        () =>
          database
            .select()
            .from(authTickets)
            .where(eq(authTickets.ticketHash, ticketHash))
            .limit(1),
        "auth_ticket_read_failed",
      );
      const row = rows[0];
      return row === undefined
        ? null
        : {
            ...row,
            admissionMetadata: row.admissionMetadata as AuthTicketAdmissionMetadata,
            audience: "tether-websocket",
          };
    },
  };
}

/** Validates explicit grant metadata and rejects extra credential-shaped fields. */
function validateGrantMetadata(metadata: AuthGrantMetadata): void {
  if (
    !isRecord(metadata) ||
    !hasExactKeys(metadata, ["requestId", "source"]) ||
    !["admin", "bootstrap", "migration"].includes(metadata.source) ||
    !isValidRequestId(metadata.requestId)
  ) {
    throw new AuthPersistenceError("auth_metadata_invalid");
  }
}

/** Validates explicit audit metadata and rejects extra credential-shaped fields. */
function validateAuditMetadata(metadata: AuthGrantAuditMetadata): void {
  if (
    !isRecord(metadata) ||
    !hasExactKeys(metadata, ["requestId"]) ||
    !isValidRequestId(metadata.requestId)
  ) {
    throw new AuthPersistenceError("auth_metadata_invalid");
  }
}

/** Validates every bounded audit field before a query can serialize it. */
function validateAuditInput(
  audit: CreateAuthGrantWithAuditInput["audit"] | RevokeAuthGrantWithAuditInput["audit"],
  expectedAction: "grant.created" | "grant.revoked",
): void {
  validateAuditMetadata(audit.metadata);
  if (
    audit.action !== expectedAction ||
    !authGrantAuditReasonCodes.includes(audit.reasonCode) ||
    audit.auditId.length === 0 ||
    audit.auditId.length > 128 ||
    audit.actorSubject.length === 0 ||
    audit.actorSubject.length > 255
  ) {
    throw new AuthPersistenceError("auth_metadata_invalid");
  }
}

/** Validates explicit admission metadata before any query can serialize it. */
function validateAdmissionMetadata(metadata: AuthTicketAdmissionMetadata): void {
  if (
    !isRecord(metadata) ||
    !hasExactKeys(metadata, ["remoteAddressHash", "replicaId", "transport"]) ||
    (metadata.remoteAddressHash !== null && !sha256HexPattern.test(metadata.remoteAddressHash)) ||
    !replicaIdPattern.test(metadata.replicaId) ||
    metadata.transport !== "websocket"
  ) {
    throw new AuthPersistenceError("auth_metadata_invalid");
  }
}

/** Validates a complete grant before it can reach Drizzle error serialization. */
function validateGrantRecord(record: AuthGrantRecord): void {
  validateGrantMetadata(record.metadata);
  const lifetimeMilliseconds = record.expiresAt.getTime() - record.issuedAt.getTime();
  if (
    record.audience !== "tether-rest" ||
    lifetimeMilliseconds <= 0 ||
    lifetimeMilliseconds > maximumAuthGrantLifetimeSeconds * 1_000
  ) {
    throw new AuthPersistenceError("auth_grant_create_failed");
  }
}

/** Validates a complete ticket before it can reach Drizzle error serialization. */
function validateTicketRecord(record: AuthTicketRecord): void {
  validateTicketHash(record.ticketHash);
  validateAdmissionMetadata(record.admissionMetadata);
  const admissionLifetimeMilliseconds = record.expiresAt.getTime() - record.createdAt.getTime();
  if (
    record.audience !== "tether-websocket" ||
    admissionLifetimeMilliseconds <= 0 ||
    admissionLifetimeMilliseconds > maximumAuthTicketAdmissionLifetimeMilliseconds ||
    (record.consumedAt !== null &&
      (record.consumedAt < record.createdAt || record.consumedAt > record.expiresAt))
  ) {
    throw new AuthPersistenceError("auth_ticket_create_failed");
  }
}

/** Validates a SHA-256 digest without accepting the opaque ticket itself. */
function validateTicketHash(ticketHash: string): void {
  if (!sha256HexPattern.test(ticketHash)) {
    throw new AuthPersistenceError("auth_ticket_hash_invalid");
  }
}

/** Parses a database grant row into its narrow domain record. */
function parseGrantRecord(row: typeof authGrants.$inferSelect): AuthGrantRecord {
  return {
    ...row,
    audience: row.audience as AuthAudience,
    metadata: row.metadata as AuthGrantMetadata,
    role: row.role as AuthRole,
  };
}

/** Accepts only documented keys, including against structurally forged runtime values. */
function hasExactKeys(value: object, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    actual.length === sortedExpected.length &&
    actual.every((key, index) => key === sortedExpected[index])
  );
}

/** Narrows forged runtime metadata before property access. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Accepts only request ids with an explicit non-credential namespace. */
function isValidRequestId(requestId: string | null): boolean {
  return requestId === null || opaqueRequestIdPattern.test(requestId);
}

/** Collapses Drizzle and PostgreSQL failures into a secret-safe typed code. */
async function runAuthStoreOperation<TValue>(
  operation: () => Promise<TValue>,
  errorCode: AuthPersistenceErrorCode,
): Promise<TValue> {
  try {
    return await operation();
  } catch (error) {
    if (error instanceof AuthPersistenceError) {
      throw error;
    }
    throw new AuthPersistenceError(errorCode);
  }
}
