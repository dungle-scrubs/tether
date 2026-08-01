import type { OperatorGrantScope } from "@dungle-scrubs/tether-protocol";
import { asc, inArray, lt } from "drizzle-orm";
import type pg from "pg";

import type { DatabasePool } from "../db.js";
import { browserPairingExchangeFailures } from "../schema.js";
import { insertAuthGrantWithAuditOnClient } from "./db-grant-stores.js";
import type { AuthGrantLifecycleAuditInput, AuthGrantRecord } from "./grant-stores.js";
import { opaqueCredentialHashesEqual } from "./opaque-credential.js";

/** Durable pairing request containing only credential hashes. */
export interface BrowserPairingRequestRecord {
  readonly confirmedAt: Date | null;
  readonly confirmedBySubject: string | null;
  readonly createdAt: Date;
  readonly exchangeSecretHash: string;
  readonly exchangedAt: Date | null;
  readonly expiresAt: Date;
  readonly failedAttempts: number;
  readonly invalidatedAt: Date | null;
  readonly operatorSubject: string;
  readonly origin: string;
  readonly publicNonce: string;
  readonly requestId: string;
  readonly requestedScope: OperatorGrantScope;
  readonly sourceAddressHash: string | null;
  readonly verificationPhrase: string;
}

/** Durable browser session state keyed by its revocable parent grant. */
export interface BrowserSessionRecord {
  readonly createdAt: Date;
  readonly csrfTokenHash: string;
  readonly grantJti: string;
  readonly origin: string;
}

/** Session and scope projection read together for one operator authorization. */
export interface BrowserAuthorityRecord {
  readonly scope: OperatorGrantScope;
  readonly session: BrowserSessionRecord;
}

/** Atomic grant material committed only by a successful exchange. */
export interface BrowserPairingGrantInput {
  readonly audit: AuthGrantLifecycleAuditInput & { readonly action: "grant.created" };
  readonly grant: AuthGrantRecord;
  readonly session: BrowserSessionRecord;
  readonly scope: OperatorGrantScope;
}

/** Stable result of creating one rate-limited pairing request. */
export type CreateBrowserPairingStoreResult =
  | { readonly request: BrowserPairingRequestRecord; readonly status: "created" }
  | { readonly status: "rate_limited" };

/** Stable result of explicit loopback confirmation. */
export type ConfirmBrowserPairingStoreResult =
  | { readonly request: BrowserPairingRequestRecord; readonly status: "confirmed" }
  | {
      readonly request: BrowserPairingRequestRecord;
      readonly status: "already_confirmed" | "expired" | "invalidated" | "exchanged";
    }
  | { readonly status: "not_found" };

/** Stable result of one atomic secret exchange attempt. */
export type ExchangeBrowserPairingStoreResult =
  | { readonly request: BrowserPairingRequestRecord; readonly status: "exchanged" }
  | {
      readonly failedAttempts: number;
      readonly status: "nonce_mismatch" | "origin_mismatch" | "secret_invalid";
    }
  | {
      readonly status:
        | "already_exchanged"
        | "expired"
        | "invalidated"
        | "not_confirmed"
        | "not_found"
        | "source_rate_limited";
    };

/** Transaction-owning persistence boundary for pairing and browser sessions. */
export interface BrowserPairingStore {
  readonly confirm: (input: {
    readonly actorSubject: string;
    readonly confirmedAt: Date;
    readonly requestId: string;
  }) => Promise<ConfirmBrowserPairingStoreResult>;
  readonly create: (input: {
    readonly request: BrowserPairingRequestRecord;
    readonly sourceWindowStartedAt: Date;
  }) => Promise<CreateBrowserPairingStoreResult>;
  readonly exchange: (input: {
    readonly attemptOrigin: string;
    readonly attemptSourceAddressHash: string | null;
    readonly attemptedAt: Date;
    readonly buildGrant: (request: BrowserPairingRequestRecord) => BrowserPairingGrantInput;
    readonly exchangeSecretHash: string;
    readonly failureId: string;
    readonly publicNonce: string;
    readonly requestId: string;
  }) => Promise<ExchangeBrowserPairingStoreResult>;
  readonly findBrowserAuthority: (grantJti: string) => Promise<BrowserAuthorityRecord | null>;
  readonly findBrowserSession: (grantJti: string) => Promise<BrowserSessionRecord | null>;
  readonly inspect: (requestId: string) => Promise<BrowserPairingRequestRecord | null>;
}

/** Creates PostgreSQL-backed atomic pairing persistence. */
export function createBrowserPairingStore(database: DatabasePool): BrowserPairingStore {
  return {
    confirm: (input) => confirmPairing(database, input),
    create: (input) => createPairing(database, input),
    exchange: async (input) => {
      await pruneExpiredPairingFailures(database, input.attemptedAt);
      return exchangePairing(database, input);
    },
    findBrowserAuthority: (grantJti) => findBrowserAuthority(database, grantJti),
    findBrowserSession: (grantJti) => findBrowserSession(database, grantJti),
    inspect: (requestId) => inspectPairing(database, requestId),
  };
}

/** Deletes only expired abuse counters in a bounded batch. */
async function pruneExpiredPairingFailures(
  database: DatabasePool,
  attemptedAt: Date,
): Promise<void> {
  const cutoff = new Date(attemptedAt.getTime() - 10 * 60 * 1_000);
  const expired = await database.db
    .select({ failureId: browserPairingExchangeFailures.failureId })
    .from(browserPairingExchangeFailures)
    .where(lt(browserPairingExchangeFailures.createdAt, cutoff))
    .orderBy(asc(browserPairingExchangeFailures.createdAt))
    .limit(1_000);
  if (expired.length === 0) return;
  await database.db.delete(browserPairingExchangeFailures).where(
    inArray(
      browserPairingExchangeFailures.failureId,
      expired.map((row) => row.failureId),
    ),
  );
}

/** Reads one request without exposing its stored secret hash. */
async function inspectPairing(
  database: DatabasePool,
  requestId: string,
): Promise<BrowserPairingRequestRecord | null> {
  const result = await database.pool.query<BrowserPairingRequestRecord>(pairingSelectSql, [
    requestId,
  ]);
  return result.rows[0] ?? null;
}

/** Serializes rate-limit accounting for one already-hashed transport source. */
async function lockPairingSource(client: pg.PoolClient, sourceAddressHash: string): Promise<void> {
  await client.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [sourceAddressHash]);
}

/** Creates one request unless its source exceeded the bounded creation window. */
async function createPairing(
  database: DatabasePool,
  input: {
    readonly request: BrowserPairingRequestRecord;
    readonly sourceWindowStartedAt: Date;
  },
): Promise<CreateBrowserPairingStoreResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (input.request.sourceAddressHash !== null) {
      await lockPairingSource(client, input.request.sourceAddressHash);
      const count = await client.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM browser_pairing_requests
         WHERE source_address_hash = $1 AND created_at >= $2`,
        [input.request.sourceAddressHash, input.sourceWindowStartedAt],
      );
      if ((count.rows[0]?.count ?? 0) >= 5) {
        await client.query("COMMIT");
        return { status: "rate_limited" };
      }
    }
    await client.query(
      `INSERT INTO browser_pairing_requests (
        confirmed_at, confirmed_by_subject, created_at, exchange_secret_hash,
        exchanged_at, expires_at, failed_attempts, invalidated_at, operator_subject,
        origin, public_nonce, request_id, requested_scope, source_address_hash,
        verification_phrase
      ) VALUES (NULL, NULL, $1, $2, NULL, $3, 0, NULL, $4, $5, $6, $7, $8::jsonb, $9, $10)`,
      [
        input.request.createdAt,
        input.request.exchangeSecretHash,
        input.request.expiresAt,
        input.request.operatorSubject,
        input.request.origin,
        input.request.publicNonce,
        input.request.requestId,
        JSON.stringify(input.request.requestedScope),
        input.request.sourceAddressHash,
        input.request.verificationPhrase,
      ],
    );
    await client.query("COMMIT");
    return { request: input.request, status: "created" };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/** Confirms one still-live request through an explicit loopback actor. */
async function confirmPairing(
  database: DatabasePool,
  input: { readonly actorSubject: string; readonly confirmedAt: Date; readonly requestId: string },
): Promise<ConfirmBrowserPairingStoreResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<BrowserPairingRequestRecord>(
      `${pairingSelectSql} FOR UPDATE`,
      [input.requestId],
    );
    const row = result.rows[0];
    if (!row) {
      await client.query("COMMIT");
      return { status: "not_found" };
    }
    const request = row;
    const terminal = pairingTerminalStatus(request, input.confirmedAt);
    if (terminal === "expired" || terminal === "invalidated") {
      await client.query("COMMIT");
      return { request, status: terminal };
    }
    if (terminal === "already_exchanged") {
      await client.query("COMMIT");
      return { request, status: "exchanged" };
    }
    if (request.confirmedAt !== null) {
      await client.query("COMMIT");
      return { request, status: "already_confirmed" };
    }
    await client.query(
      `UPDATE browser_pairing_requests SET confirmed_at = $2, confirmed_by_subject = $3
       WHERE request_id = $1`,
      [input.requestId, input.confirmedAt, input.actorSubject],
    );
    await client.query("COMMIT");
    return {
      request: {
        ...request,
        confirmedAt: input.confirmedAt,
        confirmedBySubject: input.actorSubject,
      },
      status: "confirmed",
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/** Atomically validates one exchange and commits its grant, audit, scope, and session. */
async function exchangePairing(
  database: DatabasePool,
  input: Parameters<BrowserPairingStore["exchange"]>[0],
): Promise<ExchangeBrowserPairingStoreResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (input.attemptSourceAddressHash !== null) {
      await lockPairingSource(client, input.attemptSourceAddressHash);
      const failureWindowStartedAt = new Date(input.attemptedAt.getTime() - 10 * 60 * 1_000);
      const failures = await client.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count
         FROM browser_pairing_exchange_failures
         WHERE source_address_hash = $1 AND created_at >= $2`,
        [input.attemptSourceAddressHash, failureWindowStartedAt],
      );
      if ((failures.rows[0]?.count ?? 0) >= 20) {
        await client.query("COMMIT");
        return { status: "source_rate_limited" };
      }
    }
    const result = await client.query<BrowserPairingRequestRecord>(
      `${pairingSelectSql} FOR UPDATE`,
      [input.requestId],
    );
    const request = result.rows[0];
    if (request === undefined) {
      await recordPairingSourceFailure(client, input);
      await client.query("COMMIT");
      return { status: "not_found" };
    }
    const terminal = pairingTerminalStatus(request, input.attemptedAt);
    if (terminal !== null) {
      await recordPairingSourceFailure(client, input);
      await client.query("COMMIT");
      return { status: terminal };
    }
    const mismatch =
      request.origin !== input.attemptOrigin
        ? "origin_mismatch"
        : request.publicNonce !== input.publicNonce
          ? "nonce_mismatch"
          : !opaqueCredentialHashesEqual(request.exchangeSecretHash, input.exchangeSecretHash)
            ? "secret_invalid"
            : null;
    if (mismatch !== null) {
      const failedAttempts = Math.min(request.failedAttempts + 1, 5);
      await recordPairingSourceFailure(client, input);
      await client.query(
        `UPDATE browser_pairing_requests
         SET failed_attempts = $2, invalidated_at = CASE WHEN $2 >= 5 THEN $3 ELSE invalidated_at END
         WHERE request_id = $1`,
        [input.requestId, failedAttempts, input.attemptedAt],
      );
      await client.query("COMMIT");
      return { failedAttempts, status: mismatch };
    }
    if (request.confirmedAt === null || request.confirmedBySubject === null) {
      await recordPairingSourceFailure(client, input);
      await client.query("COMMIT");
      return { status: "not_confirmed" };
    }
    await insertBrowserGrant(client, input.buildGrant(request));
    await client.query(
      `UPDATE browser_pairing_requests SET exchanged_at = $2 WHERE request_id = $1`,
      [input.requestId, input.attemptedAt],
    );
    await client.query("COMMIT");
    return { request: { ...request, exchangedAt: input.attemptedAt }, status: "exchanged" };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/** Records one redacted failed exchange against the actual transport source. */
async function recordPairingSourceFailure(
  client: pg.PoolClient,
  input: Parameters<BrowserPairingStore["exchange"]>[0],
): Promise<void> {
  if (input.attemptSourceAddressHash === null) return;
  await client.query(
    `INSERT INTO browser_pairing_exchange_failures (
      created_at, failure_id, source_address_hash
    ) VALUES ($1, $2, $3)`,
    [input.attemptedAt, input.failureId, input.attemptSourceAddressHash],
  );
}

/** Inserts the four records that together constitute browser authority. */
async function insertBrowserGrant(
  client: pg.PoolClient,
  input: BrowserPairingGrantInput,
): Promise<void> {
  await insertAuthGrantWithAuditOnClient(client, { audit: input.audit, grant: input.grant });
  await client.query(
    `INSERT INTO operator_grant_scopes (created_at, grant_jti, scope) VALUES ($1, $2, $3::jsonb)`,
    [input.session.createdAt, input.grant.jti, JSON.stringify(input.scope)],
  );
  await client.query(
    `INSERT INTO browser_sessions (created_at, csrf_token_hash, grant_jti, origin) VALUES ($1, $2, $3, $4)`,
    [input.session.createdAt, input.session.csrfTokenHash, input.grant.jti, input.session.origin],
  );
}

/** Reads browser session state needed for CSRF and exact-Origin authorization. */
async function findBrowserSession(
  database: DatabasePool,
  grantJti: string,
): Promise<BrowserSessionRecord | null> {
  const result = await database.pool.query<BrowserSessionRecord>(
    `SELECT created_at AS "createdAt", csrf_token_hash AS "csrfTokenHash", grant_jti AS "grantJti", origin
     FROM browser_sessions WHERE grant_jti = $1`,
    [grantJti],
  );
  return result.rows[0] ?? null;
}

/** Reads browser session and provider-neutral scope in one indexed projection. */
async function findBrowserAuthority(
  database: DatabasePool,
  grantJti: string,
): Promise<BrowserAuthorityRecord | null> {
  const result = await database.pool.query<
    BrowserSessionRecord & { readonly scope: OperatorGrantScope }
  >(
    `SELECT browser_sessions.created_at AS "createdAt",
       browser_sessions.csrf_token_hash AS "csrfTokenHash",
       browser_sessions.grant_jti AS "grantJti",
       browser_sessions.origin,
       operator_grant_scopes.scope
     FROM browser_sessions
     JOIN operator_grant_scopes ON operator_grant_scopes.grant_jti = browser_sessions.grant_jti
     WHERE browser_sessions.grant_jti = $1`,
    [grantJti],
  );
  const row = result.rows[0];
  if (row === undefined) return null;
  const { scope, ...session } = row;
  return { scope, session };
}

type PairingTerminalStatus = "already_exchanged" | "expired" | "invalidated";

/** Returns the terminal state that prevents confirmation or exchange. */
function pairingTerminalStatus(
  request: BrowserPairingRequestRecord,
  at: Date,
): PairingTerminalStatus | null {
  if (request.exchangedAt !== null) return "already_exchanged";
  if (request.invalidatedAt !== null) return "invalidated";
  return request.expiresAt.getTime() <= at.getTime() ? "expired" : null;
}

const pairingSelectSql = `SELECT
  confirmed_at AS "confirmedAt", confirmed_by_subject AS "confirmedBySubject",
  created_at AS "createdAt", exchange_secret_hash AS "exchangeSecretHash",
  exchanged_at AS "exchangedAt", expires_at AS "expiresAt", failed_attempts AS "failedAttempts",
  invalidated_at AS "invalidatedAt", operator_subject AS "operatorSubject", origin,
  public_nonce AS "publicNonce", request_id AS "requestId", requested_scope AS "requestedScope",
  source_address_hash AS "sourceAddressHash", verification_phrase AS "verificationPhrase"
FROM browser_pairing_requests WHERE request_id = $1`;
