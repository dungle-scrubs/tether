import { sql } from "drizzle-orm";
import {
  bigint,
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
  SessionProjectionActivity,
  SessionProjectionForkLineage,
  SessionProjectionTangentLineage,
} from "./session-projection.js";

/**
 * Durable session records. A session is the shared coordination object that
 * participants subscribe to; it is not an agent runtime by itself.
 */
export const sessions = pgTable("sessions", {
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
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
    index("participants_session_last_seen_idx").on(table.sessionId, table.lastSeenAt.desc()),
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
    leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }).notNull(),
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
    claimedAt: timestamp("claimed_at", { withTimezone: true }),
    claimedBy: text("claimed_by"),
    claimExpiresAt: timestamp("claim_expires_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    failedAt: timestamp("failed_at", { withTimezone: true }),
    failure: jsonb("failure").$type<Record<string, unknown>>(),
    input: jsonb("input").$type<Record<string, unknown>>(),
    kind: text("kind").notNull(),
    // Immutable opaque configured-account identity for scheduled tasks; null for
    // manual tasks. Paired with mailboxProvider to form the Mailbox Scope.
    mailboxAccountId: text("mailbox_account_id"),
    mailboxProvider: text("mailbox_provider"),
    objective: text("objective").notNull(),
    releasedAt: timestamp("released_at", { withTimezone: true }),
    releasedBy: text("released_by"),
    result: jsonb("result").$type<Record<string, unknown>>(),
    // Schedule Window identity for scheduled maintenance runs; null for manual
    // tasks. Interval and algorithm version are part of the schedule identity.
    scheduleAlgorithmVersion: integer("schedule_algorithm_version"),
    scheduleIntervalMs: bigint("schedule_interval_ms", { mode: "number" }),
    scheduleWindowStart: bigint("schedule_window_start", { mode: "number" }),
    sessionId: text("session_id")
      .notNull()
      .references(() => sessions.sessionId, { onDelete: "cascade" }),
    taskId: text("task_id").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.sessionId, table.taskId] }),
    index("tasks_claim_expiry_idx").on(table.claimExpiresAt),
    // Unique so duplicate deterministic scheduled runs are impossible at the
    // database level: one row per (session, kind, Mailbox Scope, algorithm
    // version, interval, window start). Manual tasks leave the schedule columns
    // null and never collide because NULLs are distinct in a Postgres unique
    // index. The deterministic scheduled task id derives from exactly these
    // fields, so this index and the task primary key agree on run identity.
    uniqueIndex("tasks_schedule_identity_idx").on(
      table.sessionId,
      table.kind,
      table.mailboxProvider,
      table.mailboxAccountId,
      table.scheduleAlgorithmVersion,
      table.scheduleIntervalMs,
      table.scheduleWindowStart,
    ),
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
