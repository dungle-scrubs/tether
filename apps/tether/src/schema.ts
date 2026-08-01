import type {
  OperatorGrantScope,
  SessionSummaryContent,
  SessionSummaryFailure,
} from "@dungle-scrubs/tether-protocol";
import { sql } from "drizzle-orm";
import {
  bigint,
  check,
  foreignKey,
  index,
  integer,
  jsonb,
  pgTable,
  primaryKey,
  text,
  timestamp,
  unique,
  uniqueIndex,
} from "drizzle-orm/pg-core";

import type {
  AuthGrantAuditMetadata,
  AuthGrantMetadata,
  AuthTicketAdmissionMetadata,
} from "./auth/grant-stores.js";
import type {
  SessionProjectionActivity,
  SessionProjectionForkLineage,
  SessionProjectionTangentLineage,
} from "./session-projection.js";
import { snapshotRecordMaxBytes } from "./snapshot-limits.js";

const snapshotRecordMaxBytesSql = sql.raw(String(snapshotRecordMaxBytes));

/** Durable authorization grants. Bearer values are intentionally absent. */
export const authGrants = pgTable(
  "auth_grants",
  {
    audience: text("audience").notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    issuedAt: timestamp("issued_at", { withTimezone: true }).notNull(),
    issuer: text("issuer").notNull(),
    jti: text("jti").primaryKey(),
    kid: text("kid").notNull(),
    metadata: jsonb("metadata").$type<AuthGrantMetadata>().notNull(),
    revokedAt: timestamp("revoked_at", { withTimezone: true }),
    role: text("role").notNull(),
    sessionScope: text("session_scope").notNull(),
    subject: text("subject").notNull(),
  },
  (table) => [
    check("auth_grants_audience_check", sql`${table.audience} = 'tether-rest'`),
    check("auth_grants_expiry_check", sql`${table.expiresAt} > ${table.issuedAt}`),
    check(
      "auth_grants_lifetime_check",
      sql`${table.expiresAt} <= ${table.issuedAt} + interval '7 days'`,
    ),
    check("auth_grants_issuer_length_check", sql`char_length(${table.issuer}) BETWEEN 1 AND 512`),
    check("auth_grants_jti_length_check", sql`char_length(${table.jti}) BETWEEN 1 AND 128`),
    check("auth_grants_kid_length_check", sql`char_length(${table.kid}) BETWEEN 1 AND 128`),
    check("auth_grants_metadata_size_check", sql`octet_length(${table.metadata}::text) <= 4096`),
    check(
      "auth_grants_metadata_shape_check",
      sql`jsonb_typeof(${table.metadata}) = 'object'
        AND ${table.metadata} ? 'requestId'
        AND ${table.metadata} ? 'source'
        AND ${table.metadata} - ARRAY['requestId', 'source'] = '{}'::jsonb
        AND jsonb_typeof(${table.metadata}->'source') = 'string'
        AND ${table.metadata}->>'source' IN ('admin', 'bootstrap', 'browser', 'migration')
        AND (
          jsonb_typeof(${table.metadata}->'requestId') = 'null'
          OR (
            jsonb_typeof(${table.metadata}->'requestId') = 'string'
            AND ${table.metadata}->>'requestId' ~ '^req_[A-Za-z0-9_-]{1,120}$'
          )
        )`,
    ),
    check(
      "auth_grants_revoked_check",
      sql`${table.revokedAt} IS NULL OR ${table.revokedAt} >= ${table.issuedAt}`,
    ),
    check("auth_grants_role_check", sql`${table.role} IN ('observer', 'participant', 'admin')`),
    check(
      "auth_grants_session_scope_length_check",
      sql`char_length(${table.sessionScope}) BETWEEN 1 AND 255`,
    ),
    check("auth_grants_subject_length_check", sql`char_length(${table.subject}) BETWEEN 1 AND 255`),
    index("auth_grants_expiry_idx").on(table.expiresAt),
    index("auth_grants_revoked_expiry_idx").on(table.revokedAt, table.expiresAt),
  ],
);

/** Bounded durable grant lifecycle audit state without raw credential material. */
export const authGrantAuditEvents = pgTable(
  "auth_grant_audit_events",
  {
    action: text("action").notNull(),
    actorSubject: text("actor_subject").notNull(),
    auditId: text("audit_id").primaryKey(),
    grantJti: text("grant_jti")
      .notNull()
      .references(() => authGrants.jti),
    metadata: jsonb("metadata").$type<AuthGrantAuditMetadata>().notNull(),
    occurredAt: timestamp("occurred_at", { withTimezone: true }).notNull().defaultNow(),
    reasonCode: text("reason_code").notNull(),
  },
  (table) => [
    check(
      "auth_grant_audit_action_length_check",
      sql`char_length(${table.action}) BETWEEN 1 AND 64`,
    ),
    check(
      "auth_grant_audit_actor_length_check",
      sql`char_length(${table.actorSubject}) BETWEEN 1 AND 255`,
    ),
    check("auth_grant_audit_id_length_check", sql`char_length(${table.auditId}) BETWEEN 1 AND 128`),
    check(
      "auth_grant_audit_metadata_size_check",
      sql`octet_length(${table.metadata}::text) <= 4096`,
    ),
    check(
      "auth_grant_audit_metadata_shape_check",
      sql`jsonb_typeof(${table.metadata}) = 'object'
        AND ${table.metadata} ? 'requestId'
        AND ${table.metadata} - 'requestId' = '{}'::jsonb
        AND (
          jsonb_typeof(${table.metadata}->'requestId') = 'null'
          OR (
            jsonb_typeof(${table.metadata}->'requestId') = 'string'
            AND ${table.metadata}->>'requestId' ~ '^req_[A-Za-z0-9_-]{1,120}$'
          )
        )`,
    ),
    check(
      "auth_grant_audit_action_check",
      sql`${table.action} IN ('grant.created', 'grant.revoked')`,
    ),
    check(
      "auth_grant_audit_reason_length_check",
      sql`char_length(${table.reasonCode}) BETWEEN 1 AND 64`,
    ),
    check(
      "auth_grant_audit_reason_check",
      sql`${table.reasonCode} IN ('bootstrap', 'key-rotation', 'migration', 'operator-request', 'security-response')`,
    ),
    index("auth_grant_audit_grant_occurred_idx").on(table.grantJti, table.occurredAt),
    index("auth_grant_audit_occurred_idx").on(table.occurredAt),
  ],
);

/** Single-use WebSocket admission tickets stored only by SHA-256 hash. */
export const authTickets = pgTable(
  "auth_tickets",
  {
    admissionMetadata: jsonb("admission_metadata").$type<AuthTicketAdmissionMetadata>().notNull(),
    audience: text("audience").notNull(),
    consumedAt: timestamp("consumed_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    parentGrantJti: text("parent_grant_jti")
      .notNull()
      .references(() => authGrants.jti),
    ticketHash: text("ticket_hash").primaryKey(),
  },
  (table) => [
    check(
      "auth_tickets_admission_metadata_size_check",
      sql`octet_length(${table.admissionMetadata}::text) <= 4096`,
    ),
    check(
      "auth_tickets_admission_metadata_shape_check",
      sql`jsonb_typeof(${table.admissionMetadata}) = 'object'
        AND ${table.admissionMetadata} ? 'remoteAddressHash'
        AND ${table.admissionMetadata} ? 'replicaId'
        AND ${table.admissionMetadata} ? 'transport'
        AND ${table.admissionMetadata} - ARRAY['remoteAddressHash', 'replicaId', 'transport'] = '{}'::jsonb
        AND jsonb_typeof(${table.admissionMetadata}->'replicaId') = 'string'
        AND ${table.admissionMetadata}->>'replicaId' ~ '^replica_[A-Za-z0-9_-]{1,120}$'
        AND jsonb_typeof(${table.admissionMetadata}->'transport') = 'string'
        AND ${table.admissionMetadata}->>'transport' = 'websocket'
        AND (
          jsonb_typeof(${table.admissionMetadata}->'remoteAddressHash') = 'null'
          OR (
            jsonb_typeof(${table.admissionMetadata}->'remoteAddressHash') = 'string'
            AND ${table.admissionMetadata}->>'remoteAddressHash' ~ '^[0-9a-f]{64}$'
          )
        )`,
    ),
    check("auth_tickets_audience_check", sql`${table.audience} = 'tether-websocket'`),
    check("auth_tickets_expiry_check", sql`${table.expiresAt} > ${table.createdAt}`),
    check(
      "auth_tickets_lifetime_check",
      sql`${table.expiresAt} <= ${table.createdAt} + interval '30 seconds'`,
    ),
    check(
      "auth_tickets_consumed_check",
      sql`${table.consumedAt} IS NULL OR (${table.consumedAt} >= ${table.createdAt} AND ${table.consumedAt} <= ${table.expiresAt})`,
    ),
    check("auth_tickets_hash_check", sql`${table.ticketHash} ~ '^[0-9a-f]{64}$'`),
    index("auth_tickets_expiry_idx").on(table.expiresAt),
    index("auth_tickets_parent_expiry_idx").on(table.parentGrantJti, table.expiresAt),
    index("auth_tickets_consumed_idx").on(table.consumedAt),
  ],
);

/** Durable provider-neutral authority attached only to browser operator grants. */
export const operatorGrantScopes = pgTable(
  "operator_grant_scopes",
  {
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    grantJti: text("grant_jti")
      .primaryKey()
      .references(() => authGrants.jti, { onDelete: "cascade" }),
    scope: jsonb("scope").$type<OperatorGrantScope>().notNull(),
  },
  (table) => [
    check(
      "operator_grant_scopes_size_check",
      sql`octet_length(${table.scope}::text) BETWEEN 2 AND 8192`,
    ),
    check(
      "operator_grant_scopes_shape_check",
      sql`jsonb_typeof(${table.scope}) = 'object'
        AND ${table.scope} ?& ARRAY['actions', 'commands', 'permissions', 'scopeKeys', 'sessionIds', 'targetKinds']
        AND ${table.scope} - ARRAY['actions', 'commands', 'permissions', 'scopeKeys', 'sessionIds', 'targetKinds'] = '{}'::jsonb
        AND jsonb_typeof(${table.scope}->'actions') = 'array'
        AND jsonb_typeof(${table.scope}->'commands') = 'array'
        AND jsonb_typeof(${table.scope}->'permissions') = 'array'
        AND jsonb_typeof(${table.scope}->'scopeKeys') = 'array'
        AND jsonb_typeof(${table.scope}->'sessionIds') = 'array'
        AND jsonb_typeof(${table.scope}->'targetKinds') = 'array'`,
    ),
  ],
);

/** Short-lived pairing requests containing hashes only for browser credentials. */
export const browserPairingRequests = pgTable(
  "browser_pairing_requests",
  {
    confirmedAt: timestamp("confirmed_at", { withTimezone: true }),
    confirmedBySubject: text("confirmed_by_subject"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    exchangeSecretHash: text("exchange_secret_hash").notNull(),
    exchangedAt: timestamp("exchanged_at", { withTimezone: true }),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    failedAttempts: integer("failed_attempts").notNull().default(0),
    invalidatedAt: timestamp("invalidated_at", { withTimezone: true }),
    operatorSubject: text("operator_subject").notNull(),
    origin: text("origin").notNull(),
    publicNonce: text("public_nonce").notNull(),
    requestId: text("request_id").primaryKey(),
    requestedScope: jsonb("requested_scope").$type<OperatorGrantScope>().notNull(),
    sourceAddressHash: text("source_address_hash"),
    verificationPhrase: text("verification_phrase").notNull(),
  },
  (table) => [
    index("browser_pairing_requests_source_created_idx").on(
      table.sourceAddressHash,
      table.createdAt,
    ),
    check(
      "browser_pairing_requests_secret_hash_check",
      sql`${table.exchangeSecretHash} ~ '^[0-9a-f]{64}$'`,
    ),
    check(
      "browser_pairing_requests_source_hash_check",
      sql`${table.sourceAddressHash} IS NULL OR ${table.sourceAddressHash} ~ '^[0-9a-f]{64}$'`,
    ),
    check("browser_pairing_requests_attempts_check", sql`${table.failedAttempts} BETWEEN 0 AND 5`),
    check(
      "browser_pairing_requests_confirmation_check",
      sql`(${table.confirmedAt} IS NULL) = (${table.confirmedBySubject} IS NULL)
        AND (${table.confirmedAt} IS NULL OR ${table.confirmedAt} >= ${table.createdAt})
        AND (${table.confirmedBySubject} IS NULL OR char_length(${table.confirmedBySubject}) BETWEEN 1 AND 255)`,
    ),
    check(
      "browser_pairing_requests_exchange_check",
      sql`${table.exchangedAt} IS NULL OR (
        ${table.confirmedAt} IS NOT NULL
        AND ${table.exchangedAt} >= ${table.confirmedAt}
        AND ${table.exchangedAt} < ${table.expiresAt}
      )`,
    ),
    check(
      "browser_pairing_requests_identity_check",
      sql`char_length(${table.operatorSubject}) BETWEEN 1 AND 255
        AND char_length(${table.origin}) BETWEEN 1 AND 512
        AND char_length(${table.requestId}) BETWEEN 1 AND 128
        AND char_length(${table.verificationPhrase}) BETWEEN 1 AND 128`,
    ),
    check(
      "browser_pairing_requests_invalidation_check",
      sql`${table.invalidatedAt} IS NULL OR (
        ${table.failedAttempts} = 5
        AND ${table.invalidatedAt} >= ${table.createdAt}
        AND ${table.invalidatedAt} < ${table.expiresAt}
      )`,
    ),
    check(
      "browser_pairing_requests_lifetime_check",
      sql`${table.expiresAt} > ${table.createdAt} AND ${table.expiresAt} <= ${table.createdAt} + interval '15 minutes'`,
    ),
    check(
      "browser_pairing_requests_scope_check",
      sql`jsonb_typeof(${table.requestedScope}) = 'object' AND octet_length(${table.requestedScope}::text) BETWEEN 2 AND 8192`,
    ),
    check(
      "browser_pairing_requests_nonce_check",
      sql`char_length(${table.publicNonce}) BETWEEN 22 AND 86 AND ${table.publicNonce} ~ '^[A-Za-z0-9_-]+$'`,
    ),
  ],
);

/** Redacted exchange-failure counters used for source-scoped abuse limits. */
export const browserPairingExchangeFailures = pgTable(
  "browser_pairing_exchange_failures",
  {
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    failureId: text("failure_id").primaryKey(),
    sourceAddressHash: text("source_address_hash").notNull(),
  },
  (table) => [
    check(
      "browser_pairing_exchange_failures_id_check",
      sql`${table.failureId} ~ '^pairfail_[A-Za-z0-9_-]{1,120}$'`,
    ),
    check(
      "browser_pairing_exchange_failures_source_hash_check",
      sql`${table.sourceAddressHash} ~ '^[0-9a-f]{64}$'`,
    ),
    index("browser_pairing_exchange_failures_source_created_idx").on(
      table.sourceAddressHash,
      table.createdAt,
    ),
    index("browser_pairing_exchange_failures_created_idx").on(table.createdAt),
  ],
);

/** Grant-bound browser session state without a persisted cookie or CSRF credential. */
export const browserSessions = pgTable(
  "browser_sessions",
  {
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    csrfTokenHash: text("csrf_token_hash").notNull(),
    grantJti: text("grant_jti")
      .primaryKey()
      .references(() => authGrants.jti, { onDelete: "cascade" }),
    origin: text("origin").notNull(),
  },
  (table) => [
    check("browser_sessions_csrf_hash_check", sql`${table.csrfTokenHash} ~ '^[0-9a-f]{64}$'`),
    check(
      "browser_sessions_origin_length_check",
      sql`char_length(${table.origin}) BETWEEN 1 AND 512`,
    ),
  ],
);

/**
 * Durable session records. A session is the shared coordination object that
 * participants subscribe to; it is not an agent runtime by itself.
 */
export const sessions = pgTable("sessions", {
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  sessionId: text("session_id").primaryKey(),
});

/** Stable one-to-one deployment identity for idempotent session bootstrapping. */
export const sessionBootstrapIdentities = pgTable(
  "session_bootstrap_identities",
  {
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    identityKey: text("identity_key").primaryKey(),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "restrict" }),
  },
  (table) => [
    check(
      "session_bootstrap_identities_identity_key_size_check",
      sql`octet_length(${table.identityKey}) BETWEEN 1 AND 512`,
    ),
    unique("session_bootstrap_identities_session_id_unique").on(table.sessionId),
  ],
);

/**
 * Permanent tombstones for permanently deleted sessions. A tombstone fences a
 * deleted session id forever: sanctioned creator paths refuse to recreate the
 * id, so a durable client cursor held against the dropped event log can never
 * silently resume against a restarted sequence that reuses the same id. The
 * tombstone row commits in the same transaction that deletes the session row
 * and cascades the event log and sequence allocator. No foreign key exists by
 * design; the session row is gone once the tombstone is durable.
 */
export const sessionTombstones = pgTable("session_tombstones", {
  deletedAt: timestamp("deleted_at", { withTimezone: true }).notNull().defaultNow(),
  /** Last event sequence allocated before the log was dropped, for diagnostics. */
  lastSeq: bigint("last_seq", { mode: "number" }).notNull().default(0),
  sessionId: text("session_id").primaryKey(),
});

/**
 * Durable mapping between an external client conversation and the Tether
 * session it controls. Client bridges use this so external chats, Slack
 * threads, or other integration conversations survive bridge restarts.
 */
export const clientSessionBindings = pgTable(
  "client_session_bindings",
  {
    archivedAt: timestamp("archived_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    externalId: text("external_id").notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).notNull().defaultNow(),
    provider: text("provider").notNull(),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
  },
  (table) => [
    primaryKey({ columns: [table.provider, table.externalId] }),
    index("client_session_bindings_session_idx").on(table.sessionId),
  ],
);

/**
 * Per-session append-only sequence allocator. Postgres updates this row inside
 * the event append transaction so every event gets one canonical order.
 */
export const sessionEventSequences = pgTable("session_event_sequences", {
  nextSeq: bigint("next_seq", { mode: "number" }).notNull().default(1),
  sessionId: text("session_id")
    .primaryKey()
    .references(() => sessions.sessionId, { onDelete: "cascade" }),
});

/**
 * Append-only event log for session state, user messages, task state changes,
 * and participant presence updates.
 */
export const sessionEvents = pgTable(
  "session_events",
  {
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    eventId: text("event_id").notNull(),
    payload: jsonb("payload").$type<Record<string, unknown>>().notNull(),
    producerId: text("producer_id").notNull(),
    seq: bigint("seq", { mode: "number" }).notNull(),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
    type: text("type").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.sessionId, table.seq] }),
    unique("session_events_event_id_unique").on(table.eventId),
    index("session_events_session_created_idx").on(table.sessionId, table.createdAt),
  ],
);

/**
 * Durable current-state projection reduced from each Session Event stream.
 * Exact events remain authoritative; this table owns no participant, task, or
 * process-local Host Presence state.
 */
export const sessionProjections = pgTable("session_projections", {
  activeRunId: text("active_run_id"),
  activity: text("activity").$type<SessionProjectionActivity>().notNull(),
  activityChangedAt: timestamp("activity_changed_at", { withTimezone: true }),
  archivedAt: timestamp("archived_at", { withTimezone: true }),
  coversSeqTo: bigint("covers_seq_to", { mode: "number" }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  deletedAt: timestamp("deleted_at", { withTimezone: true }),
  eventCount: bigint("event_count", { mode: "number" }).notNull(),
  forkedFrom: jsonb("forked_from").$type<SessionProjectionForkLineage>(),
  hostMetadata: jsonb("host_metadata").$type<Record<string, unknown>>(),
  hostMetadataSourceSeq: bigint("host_metadata_source_seq", { mode: "number" }),
  lastEventAt: timestamp("last_event_at", { withTimezone: true }),
  reducerVersion: integer("reducer_version").notNull(),
  sessionId: text("session_id")
    .primaryKey()
    .references(() => sessions.sessionId, { onDelete: "cascade" }),
  tangentOf: jsonb("tangent_of").$type<SessionProjectionTangentLineage>(),
  title: text("title"),
  titleSourceSeq: bigint("title_source_seq", { mode: "number" }),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

/**
 * Runtime identities currently attached to a session. The actual Claude Code,
 * Codex, pi-coding-agent, or generic runtime process usually runs outside
 * Docker and connects to Tether over REST/WebSocket.
 */
export const participants = pgTable(
  "participants",
  {
    capabilities: jsonb("capabilities").$type<Record<string, unknown>>().notNull().default({}),
    displayName: text("display_name").notNull(),
    joinedAt: timestamp("joined_at", { withTimezone: true }).notNull().defaultNow(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).notNull().defaultNow(),
    participantId: text("participant_id").notNull(),
    runtimeKind: text("runtime_kind").notNull(),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
  },
  (table) => [
    primaryKey({ columns: [table.sessionId, table.participantId] }),
    check(
      "participants_snapshot_size_check",
      sql`octet_length(${table.capabilities}::text) + octet_length(${table.displayName}) <= ${snapshotRecordMaxBytesSql}`,
    ),
    index("participants_session_last_seen_idx").on(
      table.sessionId,
      table.lastSeenAt.desc(),
      table.participantId,
    ),
  ],
);

/**
 * Control-channel ownership history for concrete participant runtime instances.
 * Each control generation is its own immutable row: a same-instance reconnect
 * supersedes the prior row (`superseded_at`) and inserts a new row with the next
 * epoch, so history is retained rather than overwritten. A current row has neither
 * `released_at` nor `superseded_at`; the partial unique index makes the database
 * enforce one current owner per participant. Epoch is part of the primary key so a
 * new generation for the same instance never collides with its own history.
 */
export const participantControlLeases = pgTable(
  "participant_control_leases",
  {
    acquisitionId: text("acquisition_id"),
    claimedAt: timestamp("claimed_at", { withTimezone: true }).notNull().defaultNow(),
    controlChannel: text("control_channel").notNull(),
    epoch: bigint("epoch", { mode: "number" }).notNull().default(1),
    instanceId: text("instance_id").notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).notNull().defaultNow(),
    leaseExpiresAt: timestamp("lease_expires_at", {
      withTimezone: true,
    }).notNull(),
    participantId: text("participant_id").notNull(),
    releasedAt: timestamp("released_at", { withTimezone: true }),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
    supersededAt: timestamp("superseded_at", { withTimezone: true }),
  },
  (table) => [
    primaryKey({
      columns: [table.sessionId, table.participantId, table.instanceId, table.epoch],
    }),
    index("participant_control_leases_active_idx").on(
      table.sessionId,
      table.participantId,
      table.leaseExpiresAt,
    ),
    uniqueIndex("participant_control_leases_current_unique")
      .on(table.sessionId, table.participantId)
      .where(sql`${table.releasedAt} IS NULL AND ${table.supersededAt} IS NULL`),
    uniqueIndex("participant_control_leases_acquisition_unique")
      .on(table.sessionId, table.participantId, table.acquisitionId)
      .where(sql`${table.acquisitionId} IS NOT NULL`),
  ],
);

/**
 * Durable work items that compatible participants can claim atomically.
 */
export const tasks = pgTable(
  "tasks",
  {
    cancelledAt: timestamp("cancelled_at", { withTimezone: true }),
    claimExpiredAt: timestamp("claim_expired_at", { withTimezone: true }),
    claimExpiredBy: text("claim_expired_by"),
    claimExpiresAt: timestamp("claim_expires_at", { withTimezone: true }),
    /** Server-issued opaque identity of the current claim generation; null when unclaimed or claimed before Claim IDs existed. */
    claimId: text("claim_id"),
    claimedAt: timestamp("claimed_at", { withTimezone: true }),
    claimedBy: text("claimed_by"),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    failedAt: timestamp("failed_at", { withTimezone: true }),
    failure: jsonb("failure").$type<Record<string, unknown>>(),
    input: jsonb("input").$type<Record<string, unknown>>(),
    kind: text("kind").notNull(),
    // Legacy scheduled-work identity retained only for migration and rollback.
    mailboxAccountId: text("mailbox_account_id"),
    mailboxProvider: text("mailbox_provider"),
    objective: text("objective").notNull(),
    operatorCommandKey: text("operator_command_key"),
    operatorGrantJti: text("operator_grant_jti"),
    releasedAt: timestamp("released_at", { withTimezone: true }),
    releasedBy: text("released_by"),
    result: jsonb("result").$type<Record<string, unknown>>(),
    // Versioned provider-neutral recurring-work identity. Version 1 denotes a
    // row backfilled from legacy columns; version 2 is the opaque-scope contract.
    scheduleIdentityVersion: integer("schedule_identity_version"),
    scheduleAlgorithmVersion: integer("schedule_algorithm_version"),
    scheduleIntervalMs: bigint("schedule_interval_ms", { mode: "number" }),
    scheduleScopeKey: text("schedule_scope_key"),
    scheduleWindowStart: bigint("schedule_window_start", { mode: "number" }),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
    taskId: text("task_id").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.sessionId, table.taskId] }),
    index("tasks_claim_expiry_idx").on(table.claimExpiresAt),
    index("tasks_session_created_idx").on(table.sessionId, table.createdAt.desc(), table.taskId),
    index("tasks_operator_grant_created_idx")
      .on(table.operatorGrantJti, table.createdAt)
      .where(sql`${table.operatorGrantJti} IS NOT NULL`),
    index("tasks_operator_pending_idx")
      .on(table.createdAt)
      .where(
        sql`${table.operatorCommandKey} IS NOT NULL AND ${table.cancelledAt} IS NULL AND ${table.completedAt} IS NULL AND ${table.failedAt} IS NULL`,
      ),
    uniqueIndex("tasks_operator_command_active_unique")
      .on(table.sessionId, table.operatorCommandKey)
      .where(
        sql`${table.operatorCommandKey} IS NOT NULL AND ${table.cancelledAt} IS NULL AND ${table.completedAt} IS NULL AND ${table.failedAt} IS NULL`,
      ),
    // Unique so duplicate deterministic scheduled runs are impossible at the
    // database level: one row per (session, kind, identity version, scope key, algorithm
    // version, interval, window start). Manual tasks leave the schedule columns
    // null and never collide because NULLs are distinct in a Postgres unique
    // index. The deterministic scheduled task id derives from exactly these
    // fields, so this index and the task primary key agree on run identity.
    uniqueIndex("tasks_schedule_identity_idx").on(
      table.sessionId,
      table.kind,
      table.scheduleIdentityVersion,
      table.scheduleScopeKey,
      table.scheduleAlgorithmVersion,
      table.scheduleIntervalMs,
      table.scheduleWindowStart,
    ),
    check(
      "tasks_schedule_scope_key_size_check",
      sql`${table.scheduleScopeKey} IS NULL OR octet_length(${table.scheduleScopeKey}) BETWEEN 1 AND 512`,
    ),
    check(
      "tasks_operator_command_shape_check",
      sql`(${table.operatorCommandKey} IS NULL AND ${table.operatorGrantJti} IS NULL AND ${table.kind} NOT LIKE 'operator.%') OR (${table.operatorCommandKey} ~ '^[0-9a-f]{64}$' AND char_length(${table.operatorGrantJti}) BETWEEN 1 AND 128 AND ${table.kind} LIKE 'operator.%')`,
    ),
    check(
      "tasks_snapshot_size_check",
      sql`octet_length(coalesce(${table.input}::text, '')) + octet_length(coalesce(${table.failure}::text, '')) + octet_length(coalesce(${table.result}::text, '')) + octet_length(${table.kind}) + octet_length(${table.objective}) <= ${snapshotRecordMaxBytesSql}`,
    ),
  ],
);

/**
 * Durable Session Summary candidates and publication lifecycle. This table
 * owns persistence only; range and publication decisions belong to the
 * Session Summary publication-policy module.
 */
export const sessionSummaries = pgTable(
  "session_summaries",
  {
    budgetClass: text("budget_class").notNull(),
    content: jsonb("content").$type<SessionSummaryContent>(),
    coversSeqFrom: bigint("covers_seq_from", { mode: "number" }).notNull(),
    coversSeqTo: bigint("covers_seq_to", { mode: "number" }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    failure: jsonb("failure").$type<SessionSummaryFailure>(),
    generationTaskId: text("generation_task_id").notNull(),
    integrityAlgorithm: text("integrity_algorithm"),
    integrityHash: text("integrity_hash"),
    ollamaContextSize: integer("ollama_context_size").notNull(),
    ollamaModel: text("ollama_model").notNull(),
    ollamaQuantization: text("ollama_quantization").notNull(),
    ollamaRevision: text("ollama_revision").notNull(),
    ollamaThinkingMode: text("ollama_thinking_mode").notNull(),
    outputSchemaVersion: text("output_schema_version").notNull(),
    producerId: text("producer_id").notNull(),
    producerVersion: text("producer_version").notNull(),
    promptVersion: text("prompt_version").notNull(),
    publishedAt: timestamp("published_at", { withTimezone: true }),
    quarantinedAt: timestamp("quarantined_at", { withTimezone: true }),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
    sourceEventCount: bigint("source_event_count", {
      mode: "number",
    }).notNull(),
    sourceFirstEventId: text("source_first_event_id").notNull(),
    sourceLastEventId: text("source_last_event_id").notNull(),
    sourceRangeHash: text("source_range_hash").notNull(),
    summaryId: text("summary_id").primaryKey(),
    supersededAt: timestamp("superseded_at", { withTimezone: true }),
    validatedAt: timestamp("validated_at", { withTimezone: true }),
  },
  (table) => [
    check("session_summaries_range_check", sql`${table.coversSeqFrom} <= ${table.coversSeqTo}`),
    check(
      "session_summaries_integrity_pair_check",
      sql`(${table.integrityAlgorithm} IS NULL) = (${table.integrityHash} IS NULL)`,
    ),
    unique("session_summaries_generation_task_unique").on(table.generationTaskId),
    index("session_summaries_session_budget_created_idx").on(
      table.sessionId,
      table.budgetClass,
      table.createdAt.desc(),
    ),
    uniqueIndex("session_summaries_active_unique")
      .on(table.sessionId, table.budgetClass)
      .where(sql`${table.publishedAt} IS NOT NULL AND ${table.supersededAt} IS NULL`),
  ],
);

/**
 * First-class durable approval decisions. The composite primary key enforces
 * first-committer-wins semantics for each task approval target.
 */
export const taskApprovals = pgTable(
  "task_approvals",
  {
    approvalEventId: text("approval_event_id").notNull(),
    decidedAt: timestamp("decided_at", { withTimezone: true }).notNull().defaultNow(),
    decidedByParticipantId: text("decided_by_participant_id").notNull(),
    decision: text("decision").notNull(),
    reason: jsonb("reason").$type<Record<string, unknown>>().notNull().default({}),
    sessionId: text("session_id").notNull(),
    targetKey: text("target_key").notNull(),
    taskId: text("task_id").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.sessionId, table.taskId, table.targetKey] }),
    foreignKey({
      columns: [table.sessionId, table.taskId],
      foreignColumns: [tasks.sessionId, tasks.taskId],
      name: "task_approvals_task_fk",
    }).onDelete("cascade"),
    unique("task_approvals_event_id_unique").on(table.approvalEventId),
    index("task_approvals_task_decided_idx").on(table.sessionId, table.taskId, table.decidedAt),
  ],
);
