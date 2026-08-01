import { and, asc, desc, eq, gt, isNull, sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type { PoolClient, QueryConfig } from "pg";

import type { DatabasePool } from "../db.js";
import type * as schema from "../schema.js";
import { authGrantAuditEvents, authGrants, authTickets } from "../schema.js";
import {
  authGrantRevocationNotificationChannel,
  serializeAuthGrantRevocationNotification,
} from "./grant-revocation-runtime.js";
import type {
  AuthGrantAuditMetadata,
  AuthGrantAuditRecord,
  AuthGrantMetadata,
  AuthGrantRecord,
  AuthGrantRevocationStore,
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
import { authGrantAuditReasonCodes, authGrantSources } from "./grant-stores.js";
import { maximumAuthTicketAdmissionLifetimeMilliseconds } from "./grant-stores.js";
import { type AuthAudience, maximumAuthGrantLifetimeSeconds } from "./grant-token.js";
import type { AuthRole } from "./token.js";

type AuthStoreDatabase = Pick<NodePgDatabase<typeof schema>, "insert" | "select" | "update">;

const maximumAuditListLimit = 100;
const maximumGrantListLimit = 100;
const opaqueRequestIdPattern = /^req_[A-Za-z0-9_-]{1,120}$/u;
const replicaIdPattern = /^replica_[A-Za-z0-9_-]{1,120}$/u;
const sha256HexPattern = /^[0-9a-f]{64}$/u;

/** Builds narrow authentication stores over the shared Drizzle database handle. */
export function createAuthPersistenceStores(database: DatabasePool): AuthPersistenceStores {
  return {
    audits: createAuditStore(database.db),
    createGrantWithAudit: (input) => createGrantWithAudit(database, input),
    grants: createGrantStore(database),
    revokeGrantWithAudit: (input) => revokeGrantWithAudit(database, input),
    tickets: createTicketStore(database.db),
  };
}

/** Atomically creates a grant and its required audit without exposing a transaction callback. */
async function createGrantWithAudit(
  database: DatabasePool,
  input: CreateAuthGrantWithAuditInput,
): Promise<void> {
  await runAuthStoreOperation(async () => {
    const client = await database.pool.connect();
    try {
      await client.query("BEGIN");
      await insertAuthGrantWithAuditOnClient(client, input);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }, "auth_grant_create_failed");
}

/** Inserts a validated grant and its creation audit on a caller-owned transaction. */
export async function insertAuthGrantWithAuditOnClient(
  client: PoolClient,
  input: CreateAuthGrantWithAuditInput,
): Promise<void> {
  validateAuthGrantCreationInput(input);
  await client.query(
    `INSERT INTO auth_grants (audience, expires_at, issued_at, issuer, jti, kid, metadata, revoked_at, role, session_scope, subject)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, NULL, $8, $9, $10)`,
    [
      input.grant.audience,
      input.grant.expiresAt,
      input.grant.issuedAt,
      input.grant.issuer,
      input.grant.jti,
      input.grant.kid,
      JSON.stringify(input.grant.metadata),
      input.grant.role,
      input.grant.sessionScope,
      input.grant.subject,
    ],
  );
  await client.query(
    `INSERT INTO auth_grant_audit_events (action, actor_subject, audit_id, grant_jti, metadata, occurred_at, reason_code)
     VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)`,
    [
      input.audit.action,
      input.audit.actorSubject,
      input.audit.auditId,
      input.grant.jti,
      JSON.stringify(input.audit.metadata),
      input.audit.occurredAt,
      input.audit.reasonCode,
    ],
  );
}

/** Validates one grant-and-audit creation before any persistence adapter serializes it. */
export function validateAuthGrantCreationInput(input: CreateAuthGrantWithAuditInput): void {
  validateGrantRecord(input.grant);
  validateAuditInput(input.audit, "grant.created");
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
          .returning();
        const revokedGrant = revoked[0];
        if (revokedGrant !== undefined) {
          await transaction.insert(authGrantAuditEvents).values({
            ...input.audit,
            grantJti: input.jti,
          });
          await transaction.execute(
            sql`SELECT pg_notify(
              ${authGrantRevocationNotificationChannel},
              ${serializeAuthGrantRevocationNotification(input.jti)}
            )`,
          );
          return { grant: parseGrantRecord(revokedGrant), status: "revoked" };
        }
        const existing = await transaction
          .select()
          .from(authGrants)
          .where(eq(authGrants.jti, input.jti))
          .limit(1);
        const existingGrant = existing[0];
        return existingGrant === undefined
          ? { grant: null, status: "not_found" }
          : {
              grant: parseGrantRecord(existingGrant),
              status: "already_revoked",
            };
      }),
    "auth_grant_revoke_failed",
  );
}

/** Creates the durable-grant read adapter. */
function createGrantStore(database: DatabasePool): AuthGrantRevocationStore & AuthGrantStore {
  const revocationConnections = createRevocationConnectionOwner(database);
  return {
    findManyByJti: (grantJtis, options) =>
      findManyGrantsByJti(revocationConnections, grantJtis, options),
    findByJti: async (jti) => {
      const rows = await runAuthStoreOperation(
        () => database.db.select().from(authGrants).where(eq(authGrants.jti, jti)).limit(1),
        "auth_grant_read_failed",
      );
      const row = rows[0];
      return row === undefined ? null : parseGrantRecord(row);
    },
    list: async (limit) => {
      if (!Number.isSafeInteger(limit) || limit <= 0 || limit > maximumGrantListLimit) {
        throw new AuthPersistenceError("auth_grant_limit_invalid");
      }
      const rows = await runAuthStoreOperation(
        () =>
          database.db
            .select()
            .from(authGrants)
            .orderBy(desc(authGrants.issuedAt), desc(authGrants.jti))
            .limit(limit),
        "auth_grant_list_failed",
      );
      return rows.map(parseGrantRecord);
    },
  };
}

/** Runs one cancellation-aware grant batch read on a disposable pool client. */
async function findManyGrantsByJti(
  connections: RevocationConnectionOwner,
  grantJtis: readonly string[],
  options: { readonly signal: AbortSignal; readonly timeoutMs: number },
): Promise<readonly AuthGrantRecord[]> {
  if (grantJtis.length === 0) {
    return [];
  }
  let client: PoolClient | null = null;
  let released = false;
  const release = (destroy: boolean): void => {
    if (client === null || released) {
      return;
    }
    released = true;
    client.release(destroy);
  };
  const abort = (): void => release(true);
  try {
    if (options.signal.aborted) {
      throw new AuthPersistenceError("auth_grant_read_failed");
    }
    client = await connections.acquire(options.signal);
    if (options.signal.aborted) {
      release(true);
      throw new AuthPersistenceError("auth_grant_read_failed");
    }
    options.signal.addEventListener("abort", abort, { once: true });
    const query: QueryConfig<string[][]> & { readonly query_timeout: number } = {
      name: "auth-grants-revocation-batch",
      query_timeout: options.timeoutMs,
      text: `
        SELECT
          audience,
          expires_at AS "expiresAt",
          issued_at AS "issuedAt",
          issuer,
          jti,
          kid,
          metadata,
          revoked_at AS "revokedAt",
          role,
          session_scope AS "sessionScope",
          subject
        FROM auth_grants
        WHERE jti = ANY($1::text[])
      `,
      values: [[...grantJtis]],
    };
    const result = await client.query<typeof authGrants.$inferSelect, string[][]>(query);
    return result.rows.map(parseGrantRecord);
  } catch (error) {
    release(true);
    if (error instanceof AuthPersistenceError) {
      throw error;
    }
    throw new AuthPersistenceError("auth_grant_read_failed");
  } finally {
    options.signal.removeEventListener("abort", abort);
    release(false);
  }
}

interface RevocationConnectionOwner {
  readonly acquire: (signal: AbortSignal) => Promise<PoolClient>;
}

interface PendingRevocationConnection {
  discard: boolean;
  readonly promise: Promise<PoolClient>;
}

/** Owns at most one unresolved pool waiter and destroys it after cancellation. */
function createRevocationConnectionOwner(database: DatabasePool): RevocationConnectionOwner {
  let pending: PendingRevocationConnection | null = null;
  return {
    acquire: async (signal) => {
      if (pending !== null || signal.aborted) {
        throw new AuthPersistenceError("auth_grant_read_failed");
      }
      const acquisition: PendingRevocationConnection = {
        discard: false,
        promise: database.pool.connect(),
      };
      pending = acquisition;
      let removeAbortListener = (): void => undefined;
      const aborted = new Promise<never>((_resolve, reject) => {
        const abort = (): void => {
          acquisition.discard = true;
          reject(new AuthPersistenceError("auth_grant_read_failed"));
        };
        signal.addEventListener("abort", abort, { once: true });
        removeAbortListener = () => signal.removeEventListener("abort", abort);
        if (signal.aborted) {
          abort();
        }
      });
      try {
        const client = await Promise.race([acquisition.promise, aborted]);
        if (acquisition.discard || signal.aborted) {
          throw new AuthPersistenceError("auth_grant_read_failed");
        }
        pending = null;
        return client;
      } catch (error) {
        if (acquisition.discard || signal.aborted) {
          void acquisition.promise
            .then(
              (lateClient) => lateClient.release(true),
              () => undefined,
            )
            .finally(() => {
              if (pending === acquisition) {
                pending = null;
              }
            });
        } else if (pending === acquisition) {
          pending = null;
        }
        throw error;
      } finally {
        removeAbortListener();
      }
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
    consume: async (ticketHash) => {
      validateTicketHash(ticketHash);
      const rows = await runAuthStoreOperation(
        () =>
          database
            .update(authTickets)
            .set({
              consumedAt: sql`GREATEST(statement_timestamp(), ${authTickets.createdAt})`,
            })
            .where(
              and(
                eq(authTickets.ticketHash, ticketHash),
                eq(authTickets.audience, "tether-websocket"),
                isNull(authTickets.consumedAt),
                gt(authTickets.expiresAt, sql`statement_timestamp()`),
                sql`EXISTS (
                  SELECT 1
                  FROM ${authGrants}
                  WHERE ${authGrants.jti} = ${authTickets.parentGrantJti}
                    AND ${authGrants.revokedAt} IS NULL
                    AND ${authGrants.expiresAt} > statement_timestamp()
                )`,
              ),
            )
            .returning(),
        "auth_ticket_consume_failed",
      );
      const row = rows[0];
      return row === undefined ? null : parseTicketRecord(row);
    },
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
      return row === undefined ? null : parseTicketRecord(row);
    },
  };
}

/** Validates explicit grant metadata and rejects extra credential-shaped fields. */
function validateGrantMetadata(metadata: AuthGrantMetadata): void {
  if (
    !isRecord(metadata) ||
    !hasExactKeys(metadata, ["requestId", "source"]) ||
    !authGrantSources.includes(metadata.source) ||
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

/** Parses a database ticket row without manufacturing a valid audience value. */
function parseTicketRecord(row: typeof authTickets.$inferSelect): AuthTicketRecord {
  return {
    ...row,
    admissionMetadata: row.admissionMetadata as AuthTicketAdmissionMetadata,
    audience: row.audience as AuthTicketRecord["audience"],
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
