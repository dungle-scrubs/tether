import { and, desc, eq, isNotNull, isNull, or, sql, type SQL } from "drizzle-orm";
import { readMigrationFiles } from "drizzle-orm/migrator";
import { migrate as runDrizzleMigrations } from "drizzle-orm/node-postgres/migrator";
import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres";
import { Context, Effect, Layer } from "effect";
import pg from "pg";

import { approvalTargetKey } from "./approval-target-key.js";
import { ControlEpochStaleError, nextControlEpoch, parseControlEpoch } from "./control-epoch.js";
import * as schema from "./schema.js";
import {
  clientSessionBindings,
  participantControlLeases,
  participants,
  sessionEvents,
  sessions,
  taskApprovals,
  tasks,
} from "./schema.js";
import { ServerConfigService } from "./config.js";
import {
  type AppendSessionEventInput,
  type ApprovalDecision,
  buildParticipantHeartbeatEventInput,
  buildParticipantRegistrationEventInput,
  buildTaskApprovalRecordedEventInput,
  buildTaskCancelledEventInput,
  buildTaskClaimedEventInput,
  buildTaskClaimExpiredEventInput,
  buildTaskCompletedEventInput,
  buildTaskCreatedEventInput,
  buildTaskFailedEventInput,
  buildTaskReleasedEventInput,
  deriveScheduledTaskId,
  newSessionId,
  parsePositiveSafeInteger,
} from "./protocol.js";
import type { ScheduledMaintenanceIdentity } from "./types.js";
import type {
  CandidateScheduleIdentity,
  ControlChannel,
  ControlLeaseSnapshot,
  ControlLeaseStatus,
  ClientSessionBindingRecord,
  ParticipantRecord,
  ParticipantRuntimeSnapshot,
  ParticipantRuntimeSnapshotStatus,
  SessionBindingSummary,
  SessionDebugSummary,
  SessionEvent,
  SessionEventListOptions,
  SessionListItem,
  SessionRecord,
  TaskApprovalRecord,
  TaskRecord,
  TaskListStatus,
  TaskSnapshot,
  TaskSnapshotStatus,
} from "./types.js";

const { Pool } = pg;
const migrationsFolder = "drizzle";

export type ParticipantRegistration =
  | {
      readonly participant: ParticipantRecord;
      readonly status: "joined";
    }
  | {
      readonly participant: ParticipantRecord;
      readonly status: "refreshed";
    }
  | {
      readonly participant: ParticipantRecord;
      readonly previousParticipant: ParticipantRecord;
      readonly status: "updated";
    };

export interface PersistedParticipantRegistrationResult {
  readonly events: readonly SessionEvent[];
  readonly registration: ParticipantRegistration;
}

export interface ControlLease {
  readonly claimedAt: string;
  readonly controlChannel: ControlChannel;
  /** Immutable, strictly-monotonic server-issued fencing generation. */
  readonly epoch: number;
  readonly instanceId: string;
  readonly lastSeenAt: string;
  readonly leaseExpiresAt: string;
  readonly participantId: string;
  readonly releasedAt: string | null;
  readonly sessionId: string;
}

/**
 * Result of acquire-or-supersede. `claimed` issues a fresh generation to an
 * unowned participant; `superseded` advances the generation for a
 * same-instance reconnect, fencing the prior epoch; `conflict` leaves the
 * active lease untouched for a different instance.
 */
export type ControlLeaseClaim =
  | { readonly lease: ControlLease; readonly status: "claimed" | "superseded" }
  | { readonly activeLease: ControlLease; readonly status: "conflict" };

/**
 * Result of renewing an existing control lease. Renewal preserves and compares
 * the current epoch: `renewed` refreshes the deadline for the current owner;
 * `stale` rejects a fenced epoch without mutating lease state; `conflict`
 * rejects a different instance; `absent` reports no current lease to renew.
 */
export type ControlLeaseRenewal =
  | { readonly lease: ControlLease; readonly status: "renewed" }
  | { readonly currentEpoch: number; readonly status: "stale" }
  | { readonly activeLease: ControlLease; readonly status: "conflict" }
  | { readonly status: "absent" };

/** Error raised when the database current-lease invariant rejects a write path. */
export class ControlLeaseCurrentInvariantError extends Error {
  readonly controlChannel: ControlChannel;
  readonly instanceId: string;
  readonly leaseTtlMs: number;
  readonly participantId: string;
  readonly sessionId: string;

  constructor(
    input: {
      readonly controlChannel: ControlChannel;
      readonly instanceId: string;
      readonly leaseTtlMs: number;
      readonly participantId: string;
      readonly sessionId: string;
    },
    readonly originalError: unknown,
  ) {
    super(
      `Control lease current-row invariant rejected claimControlLease for session ${input.sessionId}, participant ${input.participantId}, instance ${input.instanceId}, channel ${input.controlChannel}, ttl ${input.leaseTtlMs}ms`,
    );
    this.name = "ControlLeaseCurrentInvariantError";
    this.controlChannel = input.controlChannel;
    this.instanceId = input.instanceId;
    this.leaseTtlMs = input.leaseTtlMs;
    this.participantId = input.participantId;
    this.sessionId = input.sessionId;
  }
}

/**
 * Postgres notification channel used to fan out committed session events across
 * Tether replicas.
 */
export const sessionEventNotificationChannel = "tether_session_events";

/**
 * Minimal notification payload for fetching a committed session event from the
 * shared database.
 */
export interface SessionEventNotification {
  /** Durable event id that caused this notification. */
  readonly eventId: string;
  /** Durable sequence number for the committed event. */
  readonly seq: number;
  /** Session that owns the committed event. */
  readonly sessionId: string;
  /** Process-local source id for suppressing self-originated notifications. */
  readonly sourceId: string;
}

/**
 * Database handle used by the service. It keeps the raw pg pool available for
 * bootstrap SQL while application persistence goes through Drizzle.
 */
export interface DatabasePool {
  readonly db: NodePgDatabase<typeof schema>;
  readonly end: () => Promise<void>;
  readonly pool: pg.Pool;
}

/**
 * Effect service tag for the shared Postgres/Drizzle database pool.
 */
export class DatabaseService extends Context.Tag("tether/Database")<
  DatabaseService,
  DatabasePool
>() {}

/**
 * Live database layer. It acquires the Postgres pool, applies generated Drizzle
 * migrations, and releases all connections when its Effect scope closes.
 */
export const DatabaseLive = Layer.scoped(
  DatabaseService,
  Effect.gen(function* () {
    const config = yield* ServerConfigService;
    const database = yield* Effect.acquireRelease(
      Effect.sync(() => createPool(config.databaseUrl, { max: config.databasePoolMax })),
      (pool) => Effect.promise(() => pool.end()),
    );
    yield* Effect.tryPromise(() => migrate(database));
    return database;
  }),
);

/**
 * Options for creating the Postgres connection pool and typed Drizzle client.
 */
export interface CreatePoolOptions {
  /** Maximum number of connections in the pool. */
  readonly max?: number;
}

/**
 * Creates the Postgres connection pool and typed Drizzle client.
 */
export function createPool(databaseUrl: string, options?: CreatePoolOptions): DatabasePool {
  const pool = new Pool({
    ...(options?.max === undefined ? {} : { max: options.max }),
    connectionString: databaseUrl,
  });
  return {
    db: drizzle(pool, { schema }),
    end: () => pool.end(),
    pool,
  };
}

/**
 * Applies generated Drizzle migrations. The schema definitions and migration
 * files are the source of truth for runtime database bootstrapping.
 */
export async function migrate(database: DatabasePool): Promise<void> {
  await baselineLegacySchema(database);
  await runDrizzleMigrations(database.db, { migrationsFolder });
}

interface MigrationCountRow {
  readonly count: number;
}

interface SchemaObjectExistsRow {
  readonly exists: boolean;
}

interface SessionExistenceRow {
  readonly exists: boolean;
}

interface SequenceRow {
  readonly seq: unknown;
}

interface ExpiredTaskClaimRow {
  readonly cancelledAt: Date | null;
  readonly claimExpiredAt: Date | null;
  readonly claimExpiredBy: string | null;
  readonly claimExpiresAt: Date | null;
  readonly claimedAt: Date | null;
  readonly claimedBy: string | null;
  readonly completedAt: Date | null;
  readonly createdAt: Date;
  readonly failedAt: Date | null;
  readonly failure: Record<string, unknown> | null;
  readonly input: Record<string, unknown> | null;
  readonly kind: string;
  readonly mailboxAccountId: string | null;
  readonly mailboxProvider: string | null;
  readonly objective: string;
  readonly previousClaimedBy: string;
  readonly releasedAt: Date | null;
  readonly releasedBy: string | null;
  readonly result: Record<string, unknown> | null;
  readonly scheduleAlgorithmVersion: number | string | null;
  readonly scheduleIntervalMs: number | string | null;
  readonly scheduleWindowStart: number | string | null;
  readonly sessionId: string;
  readonly taskId: string;
}

interface PgTaskRow {
  readonly cancelledAt: Date | null;
  readonly claimExpiredAt: Date | null;
  readonly claimExpiredBy: string | null;
  readonly claimExpiresAt: Date | null;
  readonly claimedAt: Date | null;
  readonly claimedBy: string | null;
  readonly completedAt: Date | null;
  readonly createdAt: Date;
  readonly failedAt: Date | null;
  readonly failure: Record<string, unknown> | null;
  readonly input: Record<string, unknown> | null;
  readonly kind: string;
  readonly mailboxAccountId: string | null;
  readonly mailboxProvider: string | null;
  readonly objective: string;
  readonly releasedAt: Date | null;
  readonly releasedBy: string | null;
  readonly result: Record<string, unknown> | null;
  // bigint/integer columns arrive as numeric strings over the raw pg driver.
  readonly scheduleAlgorithmVersion: number | string | null;
  readonly scheduleIntervalMs: number | string | null;
  readonly scheduleWindowStart: number | string | null;
  readonly sessionId: string;
  readonly taskId: string;
}

/** Structural read of the durable schedule-identity task columns. */
interface ScheduleIdentityColumns {
  readonly mailboxAccountId?: string | null;
  readonly mailboxProvider?: string | null;
  readonly scheduleAlgorithmVersion?: number | string | null;
  readonly scheduleIntervalMs?: number | string | null;
  readonly scheduleWindowStart?: number | string | null;
}

/** Caller-supplied schedule and Mailbox Scope identity for a scheduled task. */
export interface ScheduledTaskIdentityInput {
  readonly mailboxAccountId: string;
  readonly mailboxProvider: string;
  readonly scheduleAlgorithmVersion: number;
  readonly scheduleIntervalMs: number;
  readonly scheduleWindowStart: number;
}

interface PgTaskApprovalRow {
  readonly approvalEventId: string;
  readonly decidedAt: Date;
  readonly decidedByParticipantId: string;
  readonly decision: string;
  readonly reason: Record<string, unknown>;
  readonly sessionId: string;
  readonly targetKey: string;
  readonly taskId: string;
}

interface PgControlLeaseRow {
  readonly claimedAt: Date;
  readonly controlChannel: string;
  readonly epoch: unknown;
  readonly instanceId: string;
  readonly lastSeenAt: Date;
  readonly leaseExpiresAt: Date;
  readonly participantId: string;
  readonly releasedAt: Date | null;
  readonly sessionId: string;
  readonly supersededAt: Date | null;
}

interface ControlLeaseSnapshotRow extends PgControlLeaseRow {
  readonly status: ControlLeaseStatus;
}

interface PgSessionEventRow {
  readonly createdAt: Date;
  readonly eventId: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly seq: unknown;
  readonly sessionId: string;
  readonly type: string;
}

interface PgSessionRow {
  readonly createdAt: Date;
  readonly sessionId: string;
}

interface PgClientSessionBindingRow {
  readonly archivedAt: Date | null;
  readonly createdAt: Date;
  readonly externalId: string;
  readonly lastSeenAt: Date;
  readonly provider: string;
  readonly sessionId: string;
}

interface PgParticipantRow {
  readonly capabilities: Record<string, unknown>;
  readonly displayName: string;
  readonly joinedAt: Date;
  readonly lastSeenAt: Date;
  readonly participantId: string;
  readonly runtimeKind: string;
  readonly sessionId: string;
}

/** Result of creating session prerequisites through an explicit creator path. */
export type CreateSessionResult =
  | { readonly created: false; readonly session: SessionRecord }
  | { readonly created: true; readonly session: SessionRecord };

/** Error raised when a session-scoped write targets a missing durable session. */
export class SessionNotFoundError extends Error {
  readonly operation: string;
  readonly sessionId: string;

  constructor(input: { readonly operation: string; readonly sessionId: string }) {
    super(`Session ${input.sessionId} does not exist for ${input.operation}`);
    this.name = "SessionNotFoundError";
    this.operation = input.operation;
    this.sessionId = input.sessionId;
  }
}

const clientSessionBindingReturningColumns = `
  archived_at AS "archivedAt",
  created_at AS "createdAt",
  external_id AS "externalId",
  last_seen_at AS "lastSeenAt",
  provider,
  session_id AS "sessionId"
`;

const participantReturningColumns = `
  capabilities,
  display_name AS "displayName",
  joined_at AS "joinedAt",
  last_seen_at AS "lastSeenAt",
  participant_id AS "participantId",
  runtime_kind AS "runtimeKind",
  session_id AS "sessionId"
`;

type ClientSessionBindingUpsert =
  | {
      readonly binding: ClientSessionBindingRecord;
      readonly created: false;
      readonly sessionCreated: boolean;
      readonly status: "rebound" | "refreshed";
    }
  | {
      readonly binding: ClientSessionBindingRecord;
      readonly created: true;
      readonly sessionCreated: boolean;
      readonly status: "inserted";
    };

interface TransactionClient {
  readonly query: <TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ) => Promise<{ readonly rows: TRow[] }>;
}

interface ReleasableTransactionClient extends TransactionClient {
  readonly release: () => void;
}
interface TaskClaimExpirationConnectionPool {
  readonly connect: () => Promise<ReleasableTransactionClient>;
}

const taskClaimExpirationDeadlockSqlState = "40P01";
const defaultTaskClaimExpirationMaxAttempts = 3;

export interface TaskClaimExpirationRetryDiagnostics {
  readonly expiredRowCount?: number;
  readonly maxAttempts: number;
  readonly operation: string;
  readonly requestedBatchSize: number;
  readonly retryAttempt: number;
  readonly sqlState: string;
}

export interface TaskClaimExpirationAttemptContext {
  readonly attempt: number;
  readonly recordExpiredRowCount: (expiredRowCount: number) => void;
}

/** Error raised after task claim expiration exhausts deadlock retries. */
export class TaskClaimExpirationDeadlockError extends Error {
  readonly code = taskClaimExpirationDeadlockSqlState;
  readonly diagnostics: TaskClaimExpirationRetryDiagnostics;
  readonly originalError: unknown;

  constructor(input: {
    readonly diagnostics: TaskClaimExpirationRetryDiagnostics;
    readonly originalError: unknown;
  }) {
    super(
      input.originalError instanceof Error
        ? input.originalError.message
        : "Task claim expiration deadlock retry exhausted",
      { cause: input.originalError },
    );
    this.diagnostics = input.diagnostics;
    this.name = "TaskClaimExpirationDeadlockError";
    this.originalError = input.originalError;
  }
}

/** Acquires a transaction-scoped advisory lock for a two-part identity key. */
async function acquireTransactionAdvisoryLock(
  client: TransactionClient,
  leftKey: string,
  rightKey: string,
): Promise<void> {
  await client.query(`SELECT pg_advisory_xact_lock(hashtext($1::text), hashtext($2::text))`, [
    leftKey,
    rightKey,
  ]);
}

export type PersistedTaskEventResult = {
  readonly event: SessionEvent;
  readonly task: TaskRecord;
} | null;

/** Persistence result for caller-supplied event ids. */
export type PersistedEventAppendResult =
  | {
      readonly event: SessionEvent;
      readonly events: readonly [SessionEvent];
      readonly status: "created";
    }
  | {
      readonly event: SessionEvent;
      readonly events: readonly [];
      readonly status: "replayed";
    }
  | {
      readonly conflictingFields: readonly string[];
      readonly eventId: string;
      readonly events: readonly [];
      readonly status: "conflict";
    };

/** Persistence result for caller-supplied task ids. */
export type PersistedTaskCreateResult =
  | {
      readonly event: SessionEvent;
      // Broadcast list. Usually the single created event, but a scheduled create
      // routed through the deterministic ensure path also carries the older-window
      // supersession cancellation events, so this is a list rather than a tuple.
      readonly events: readonly SessionEvent[];
      readonly status: "created";
      readonly task: TaskRecord;
    }
  | {
      // Broadcast list. Empty on a plain replay; a scheduled create that replays
      // the current window while superseding an older one carries the older-window
      // supersession cancellation events.
      readonly events: readonly SessionEvent[];
      readonly status: "replayed";
      readonly task: TaskRecord;
    }
  | {
      readonly conflictingFields: readonly string[];
      readonly events: readonly [];
      readonly status: "conflict";
      readonly task: null;
      readonly taskId: string;
    };

/** Error raised when an event sequence cannot be represented safely in JSON numbers. */
export class SessionEventSequenceRangeError extends Error {
  /** Largest sequence currently allowed by the public numeric cursor contract. */
  readonly cutoff = Number.MAX_SAFE_INTEGER;
  /** Sequence value that crossed the safe numeric boundary. */
  readonly attemptedSeq: string;
  /** Session whose event append attempted the unsafe sequence. */
  readonly sessionId: string;

  constructor(input: { readonly attemptedSeq: string; readonly sessionId: string }) {
    super(
      `Session event sequence ${input.attemptedSeq} for ${input.sessionId} exceeds safe integer cutoff ${Number.MAX_SAFE_INTEGER}`,
    );
    this.name = "SessionEventSequenceRangeError";
    this.attemptedSeq = input.attemptedSeq;
    this.sessionId = input.sessionId;
  }
}

export type PersistedTaskApprovalResult =
  | {
      readonly decision: ApprovalDecision;
      readonly event: SessionEvent;
      readonly events: readonly [SessionEvent];
      readonly status: "recorded";
      readonly task: TaskRecord;
      readonly targetKey: string;
    }
  | {
      readonly approval: TaskApprovalRecord;
      readonly decision: ApprovalDecision;
      readonly events: readonly [];
      readonly existingDecision: ApprovalDecision;
      readonly status: "ignored";
      readonly task: TaskRecord;
      readonly targetKey: string;
    };

const controlLeaseReturningColumns = `
  claimed_at AS "claimedAt",
  control_channel AS "controlChannel",
  epoch,
  instance_id AS "instanceId",
  last_seen_at AS "lastSeenAt",
  lease_expires_at AS "leaseExpiresAt",
  participant_id AS "participantId",
  released_at AS "releasedAt",
  session_id AS "sessionId",
  superseded_at AS "supersededAt"
`;

const taskReturningColumns = `
  cancelled_at AS "cancelledAt",
  claim_expired_at AS "claimExpiredAt",
  claim_expired_by AS "claimExpiredBy",
  claim_expires_at AS "claimExpiresAt",
  claimed_at AS "claimedAt",
  claimed_by AS "claimedBy",
  completed_at AS "completedAt",
  created_at AS "createdAt",
  failed_at AS "failedAt",
  failure,
  input,
  kind,
  mailbox_account_id AS "mailboxAccountId",
  mailbox_provider AS "mailboxProvider",
  objective,
  released_at AS "releasedAt",
  released_by AS "releasedBy",
  result,
  schedule_algorithm_version AS "scheduleAlgorithmVersion",
  schedule_interval_ms AS "scheduleIntervalMs",
  schedule_window_start AS "scheduleWindowStart",
  session_id AS "sessionId",
  task_id AS "taskId"
`;

const taskApprovalReturningColumns = `
  approval_event_id AS "approvalEventId",
  decided_at AS "decidedAt",
  decided_by_participant_id AS "decidedByParticipantId",
  decision,
  reason,
  session_id AS "sessionId",
  target_key AS "targetKey",
  task_id AS "taskId"
`;

interface LegacyMigrationProbe {
  readonly label: string;
  readonly represented: (database: DatabasePool) => Promise<boolean>;
}

/** Error raised when a legacy schema cannot be represented as a migration prefix. */
export class LegacySchemaBaselineError extends Error {
  readonly missingOrGappedObjects: readonly string[];
  readonly representedPrefix: number;

  constructor(input: {
    readonly missingOrGappedObjects: readonly string[];
    readonly representedPrefix: number;
  }) {
    super(
      `Unsupported legacy schema for Drizzle baseline: represented prefix ${input.representedPrefix}, missing or gapped objects: ${input.missingOrGappedObjects.join(", ")}`,
    );
    this.name = "LegacySchemaBaselineError";
    this.missingOrGappedObjects = input.missingOrGappedObjects;
    this.representedPrefix = input.representedPrefix;
  }
}

/**
 * Baselines old pre-migrator databases by probing schema shape before any
 * application-table mutation. Supported shapes are contiguous generated
 * migration prefixes; Drizzle then applies the remaining generated migrations.
 */
async function baselineLegacySchema(database: DatabasePool): Promise<void> {
  await database.pool.query(`
    CREATE SCHEMA IF NOT EXISTS drizzle;
    CREATE TABLE IF NOT EXISTS drizzle.__drizzle_migrations (
      id SERIAL PRIMARY KEY,
      hash text NOT NULL,
      created_at bigint
    );
  `);
  const migrationCount = await database.pool.query<MigrationCountRow>(
    `SELECT count(*)::int AS "count" FROM drizzle.__drizzle_migrations`,
  );
  if ((migrationCount.rows[0]?.count ?? 0) > 0) {
    return;
  }

  const probes = legacyMigrationProbes();
  const results: boolean[] = [];
  for (const probe of probes) {
    results.push(await probe.represented(database));
  }
  if (results.every((result) => !result)) {
    return;
  }
  const firstGap = results.findIndex((result) => !result);
  const representedPrefix =
    firstGap === -1 ? results.length - 1 : results.slice(0, firstGap).length - 1;
  const hasNonContiguousSuffix =
    firstGap !== -1 && results.slice(firstGap + 1).some((result) => result);
  if (hasNonContiguousSuffix) {
    throw new LegacySchemaBaselineError({
      missingOrGappedObjects: probes
        .filter((_, index) => !results[index] && results.slice(index + 1).some((result) => result))
        .map((probe) => probe.label),
      representedPrefix,
    });
  }

  const migrations = readMigrationFiles({ migrationsFolder }).slice(0, representedPrefix + 1);
  await database.pool.query("BEGIN");
  try {
    for (const migration of migrations) {
      await database.pool.query(
        `INSERT INTO drizzle.__drizzle_migrations ("hash", "created_at") VALUES ($1, $2)`,
        [migration.hash, migration.folderMillis],
      );
    }
    await database.pool.query("COMMIT");
  } catch (error) {
    await database.pool.query("ROLLBACK");
    throw error;
  }
}

/** Ordered probes for generated migrations that can be represented by schema shape alone. */
function legacyMigrationProbes(): readonly LegacyMigrationProbe[] {
  return [
    {
      label: "0000 core session, participant, event, and task tables",
      represented: async (database) =>
        (await hasTables(database, [
          "participants",
          "session_event_sequences",
          "session_events",
          "sessions",
          "tasks",
        ])) &&
        (await hasColumns(database, "sessions", ["created_at", "session_id"])) &&
        (await hasColumns(database, "tasks", [
          "claimed_at",
          "claimed_by",
          "completed_at",
          "created_at",
          "kind",
          "objective",
          "session_id",
          "task_id",
        ])),
    },
    {
      label: "0001 terminal task columns",
      represented: (database) =>
        hasColumns(database, "tasks", [
          "cancelled_at",
          "failed_at",
          "failure",
          "released_at",
          "result",
        ]),
    },
    {
      label: "0002 participant control leases",
      represented: (database) => hasTable(database, "participant_control_leases"),
    },
    {
      label: "0003 task claim expiry column",
      represented: (database) => hasColumn(database, "tasks", "claim_expires_at"),
    },
    {
      label: "0004 client session bindings",
      represented: (database) => hasTable(database, "client_session_bindings"),
    },
    {
      label: "0005 task input column",
      represented: (database) => hasColumn(database, "tasks", "input"),
    },
    {
      label: "0006 task claim expiry index",
      represented: (database) => hasIndex(database, "tasks_claim_expiry_idx"),
    },
    {
      label: "0007 task approval rows",
      represented: (database) =>
        hasTable(database, "task_approvals").then((hasApprovalTable) =>
          hasApprovalTable ? hasIndex(database, "task_approvals_task_decided_idx") : false,
        ),
    },
    {
      label: "0008 participant current lease index",
      represented: (database) =>
        hasColumn(database, "participant_control_leases", "superseded_at").then(
          async (hasSupersededAt) =>
            hasSupersededAt &&
            (await hasIndex(database, "participant_control_leases_current_unique")),
        ),
    },
    {
      label: "0009 task clear cause and removed session archival",
      represented: async (database) =>
        (await hasColumns(database, "tasks", [
          "claim_expired_at",
          "claim_expired_by",
          "released_by",
        ])) && !(await hasColumn(database, "sessions", "archived_at")),
    },
    {
      label: "0010 participant control lease epoch",
      represented: (database) => hasColumn(database, "participant_control_leases", "epoch"),
    },
    {
      label: "0011 task schedule and mailbox scope identity with unique schedule index",
      represented: (database) =>
        hasColumns(database, "tasks", [
          "mailbox_account_id",
          "mailbox_provider",
          "schedule_algorithm_version",
          "schedule_interval_ms",
          "schedule_window_start",
        ]).then((hasScheduleColumns) =>
          hasScheduleColumns ? hasUniqueIndex(database, "tasks_schedule_identity_idx") : false,
        ),
    },
    {
      label: "0012 control lease generation history primary key including epoch",
      represented: (database) =>
        hasConstraint(
          database,
          "participant_control_leases_session_id_participant_id_instance_id_epoch_pk",
        ),
    },
  ];
}

/** Returns whether all named public tables exist. */
async function hasTables(database: DatabasePool, tableNames: readonly string[]): Promise<boolean> {
  for (const tableName of tableNames) {
    if (!(await hasTable(database, tableName))) {
      return false;
    }
  }
  return true;
}

/** Returns whether one public table exists. */
async function hasTable(database: DatabasePool, tableName: string): Promise<boolean> {
  const result = await database.pool.query<SchemaObjectExistsRow>(
    `SELECT to_regclass($1) IS NOT NULL AS "exists"`,
    [`public.${tableName}`],
  );
  return result.rows[0]?.exists === true;
}

/** Returns whether all named columns exist on one public table. */
async function hasColumns(
  database: DatabasePool,
  tableName: string,
  columnNames: readonly string[],
): Promise<boolean> {
  for (const columnName of columnNames) {
    if (!(await hasColumn(database, tableName, columnName))) {
      return false;
    }
  }
  return true;
}

/** Returns whether one column exists on one public table. */
async function hasColumn(
  database: DatabasePool,
  tableName: string,
  columnName: string,
): Promise<boolean> {
  const result = await database.pool.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = $1
          AND column_name = $2
      ) AS "exists"
    `,
    [tableName, columnName],
  );
  return result.rows[0]?.exists === true;
}

/** Returns whether one public index exists. */
async function hasIndex(database: DatabasePool, indexName: string): Promise<boolean> {
  const result = await database.pool.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = $1
      ) AS "exists"
    `,
    [indexName],
  );
  return result.rows[0]?.exists === true;
}

/** Returns whether one named constraint exists in the public schema. */
async function hasConstraint(database: DatabasePool, constraintName: string): Promise<boolean> {
  const result = await database.pool.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM pg_constraint con
        JOIN pg_namespace ns ON ns.oid = con.connamespace
        WHERE ns.nspname = 'public'
          AND con.conname = $1
      ) AS "exists"
    `,
    [constraintName],
  );
  return result.rows[0]?.exists === true;
}

/** Returns whether one public index exists and enforces uniqueness. */
async function hasUniqueIndex(database: DatabasePool, indexName: string): Promise<boolean> {
  const result = await database.pool.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM pg_class idx
        JOIN pg_index ix ON ix.indexrelid = idx.oid
        JOIN pg_namespace ns ON ns.oid = idx.relnamespace
        WHERE ns.nspname = 'public'
          AND idx.relname = $1
          AND ix.indisunique
      ) AS "exists"
    `,
    [indexName],
  );
  return result.rows[0]?.exists === true;
}

/**
 * Finds an active client/session binding for one external conversation.
 */
export async function findClientSessionBinding(
  database: DatabasePool,
  input: {
    readonly externalId: string;
    readonly provider: string;
  },
): Promise<ClientSessionBindingRecord | null> {
  const row = await database.db.query.clientSessionBindings.findFirst({
    where: and(
      eq(clientSessionBindings.provider, input.provider),
      eq(clientSessionBindings.externalId, input.externalId),
      isNull(clientSessionBindings.archivedAt),
    ),
  });
  return row ? toClientSessionBindingRecord(row) : null;
}

/**
 * Archives an active client/session binding so it no longer participates in
 * bridge startup recovery.
 */
export async function archiveClientSessionBinding(
  database: DatabasePool,
  input: {
    readonly externalId: string;
    readonly provider: string;
  },
): Promise<ClientSessionBindingRecord | null> {
  const rows = await database.db
    .update(clientSessionBindings)
    .set({ archivedAt: sql`now()` })
    .where(
      and(
        eq(clientSessionBindings.provider, input.provider),
        eq(clientSessionBindings.externalId, input.externalId),
        isNull(clientSessionBindings.archivedAt),
      ),
    )
    .returning();
  return rows[0] ? toClientSessionBindingRecord(rows[0]) : null;
}

/**
 * Lists active client/session bindings, optionally scoped to one provider.
 */
export async function listClientSessionBindings(
  database: DatabasePool,
  input: {
    readonly provider?: string | undefined;
  } = {},
): Promise<ClientSessionBindingRecord[]> {
  const rows = await database.db
    .select()
    .from(clientSessionBindings)
    .where(
      input.provider === undefined
        ? isNull(clientSessionBindings.archivedAt)
        : and(
            eq(clientSessionBindings.provider, input.provider),
            isNull(clientSessionBindings.archivedAt),
          ),
    )
    .orderBy(clientSessionBindings.provider, clientSessionBindings.externalId);
  return rows.map(toClientSessionBindingRecord);
}

/**
 * Creates, rebounds, or refreshes the binding between an external conversation
 * and a session. The transaction owns selected-session choice so archived rows
 * and concurrent first resolves cannot leave unselected candidate sessions.
 */
export async function upsertClientSessionBinding(
  database: DatabasePool,
  input: {
    readonly externalId: string;
    readonly provider: string;
    readonly sessionId?: string | undefined;
  },
): Promise<ClientSessionBindingUpsert> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    await acquireTransactionAdvisoryLock(client, input.provider, input.externalId);
    const existingRows = await client.query<PgClientSessionBindingRow>(
      `
        SELECT ${clientSessionBindingReturningColumns}
        FROM client_session_bindings
        WHERE provider = $1
          AND external_id = $2
        FOR UPDATE
      `,
      [input.provider, input.externalId],
    );
    const existing = existingRows.rows[0];
    if (existing?.archivedAt === null) {
      const refreshedRows = await client.query<PgClientSessionBindingRow>(
        `
          UPDATE client_session_bindings
          SET last_seen_at = now()
          WHERE provider = $1
            AND external_id = $2
          RETURNING ${clientSessionBindingReturningColumns}
        `,
        [input.provider, input.externalId],
      );
      await client.query("COMMIT");
      return {
        binding: toClientSessionBindingRecord(refreshedRows.rows[0]),
        created: false,
        sessionCreated: false,
        status: "refreshed",
      };
    }

    const selectedSessionId = input.sessionId ?? newSessionId();
    const sessionCreate = await createSessionWithClient(client, selectedSessionId);
    if (existing) {
      const reboundRows = await client.query<PgClientSessionBindingRow>(
        `
          UPDATE client_session_bindings
          SET
            archived_at = NULL,
            last_seen_at = now(),
            session_id = $3
          WHERE provider = $1
            AND external_id = $2
          RETURNING ${clientSessionBindingReturningColumns}
        `,
        [input.provider, input.externalId, selectedSessionId],
      );
      await client.query("COMMIT");
      return {
        binding: toClientSessionBindingRecord(reboundRows.rows[0]),
        created: false,
        sessionCreated: sessionCreate.created,
        status: "rebound",
      };
    }

    const insertedRows = await client.query<PgClientSessionBindingRow>(
      `
        INSERT INTO client_session_bindings (
          external_id,
          provider,
          session_id
        )
        VALUES ($1, $2, $3)
        RETURNING ${clientSessionBindingReturningColumns}
      `,
      [input.externalId, input.provider, selectedSessionId],
    );
    await client.query("COMMIT");
    return {
      binding: toClientSessionBindingRecord(insertedRows.rows[0]),
      created: true,
      sessionCreated: sessionCreate.created,
      status: "inserted",
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Acquires or supersedes the single active control channel for one participant
 * identity, issuing an immutable, strictly-monotonic Control Epoch. A fresh
 * owner receives a new generation (`claimed`); a same-instance reconnect
 * advances the generation to N+1 and fences epoch N (`superseded`); a different
 * active instance returns a `conflict` without changing lease state. Parallel
 * runtimes must use distinct participant identities.
 */
export async function claimControlLease(
  database: DatabasePool,
  input: {
    readonly controlChannel: ControlChannel;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  },
): Promise<ControlLeaseClaim> {
  assertPositiveFiniteTtlMs(input.leaseTtlMs, "control lease TTL");
  await requireSession(database, input.sessionId, "claimControlLease");
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    await acquireTransactionAdvisoryLock(client, input.sessionId, input.participantId);
    const activeRows = await client.query<PgControlLeaseRow>(
      `
        SELECT ${controlLeaseReturningColumns}
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND released_at IS NULL
          AND superseded_at IS NULL
          AND lease_expires_at > now()
        ORDER BY lease_expires_at DESC, claimed_at DESC, instance_id
        FOR UPDATE
      `,
      [input.sessionId, input.participantId],
    );
    const conflictingActiveLease = activeRows.rows
      .map(toControlLease)
      .find(
        (lease) =>
          lease.instanceId !== input.instanceId || lease.controlChannel !== input.controlChannel,
      );
    if (conflictingActiveLease) {
      await client.query("COMMIT");
      return { activeLease: conflictingActiveLease, status: "conflict" };
    }
    const activeLease = activeRows.rows[0] ? toControlLease(activeRows.rows[0]) : null;
    const newEpoch = await allocateNextControlEpoch(client, input.sessionId, input.participantId);
    // Fence every current row for this participant, INCLUDING a same-instance one.
    // Generations are immutable rows, so the prior epoch's row is superseded in
    // place (retained as history) rather than overwritten. A different active
    // instance already returned a conflict above, so any current row reachable here
    // is this instance's own prior generation or a stale/expired row. Superseding
    // all of them keeps the single-current invariant before the new row is
    // inserted.
    await client.query(
      `
        UPDATE participant_control_leases
        SET superseded_at = now()
        WHERE session_id = $1
          AND participant_id = $2
          AND released_at IS NULL
          AND superseded_at IS NULL
      `,
      [input.sessionId, input.participantId],
    );
    // Plain insert of a new immutable generation row. The epoch is strictly greater
    // than every prior epoch (MAX over history + 1), so the (session, participant,
    // instance, epoch) primary key never collides with this instance's history and
    // no ON CONFLICT overwrite of a prior generation is possible.
    const leasedRows = await client.query<PgControlLeaseRow>(
      `
        INSERT INTO participant_control_leases (
          control_channel,
          epoch,
          instance_id,
          lease_expires_at,
          participant_id,
          session_id
        )
        VALUES ($1, $2, $3, now() + $4 * interval '1 millisecond', $5, $6)
        RETURNING ${controlLeaseReturningColumns}
      `,
      [
        input.controlChannel,
        newEpoch,
        input.instanceId,
        input.leaseTtlMs,
        input.participantId,
        input.sessionId,
      ],
    );
    const lease = toControlLease(leasedRows.rows[0]);
    await client.query("COMMIT");
    return { lease, status: activeLease ? "superseded" : "claimed" };
  } catch (error) {
    await client.query("ROLLBACK");
    if (isPgUniqueViolation(error, "participant_control_leases_current_unique")) {
      throw new ControlLeaseCurrentInvariantError(input, error);
    }
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Renews an existing control lease, preserving and comparing its Control Epoch.
 * Renewal never advances the generation: a matching current owner refreshes its
 * deadline (`renewed`); a fenced epoch is rejected without mutating lease state
 * (`stale`); a different instance is a `conflict`; a missing current lease is
 * `absent`. This is the heartbeat and REST epoch-validation boundary.
 */
export async function renewControlLease(
  database: DatabasePool,
  input: {
    readonly controlChannel: ControlChannel;
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  },
): Promise<ControlLeaseRenewal> {
  assertPositiveFiniteTtlMs(input.leaseTtlMs, "control lease TTL");
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    await acquireTransactionAdvisoryLock(client, input.sessionId, input.participantId);
    const activeRows = await client.query<PgControlLeaseRow>(
      `
        SELECT ${controlLeaseReturningColumns}
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND released_at IS NULL
          AND superseded_at IS NULL
          AND lease_expires_at > now()
        ORDER BY lease_expires_at DESC, claimed_at DESC, instance_id
        FOR UPDATE
      `,
      [input.sessionId, input.participantId],
    );
    const current = activeRows.rows[0] ? toControlLease(activeRows.rows[0]) : null;
    if (!current) {
      await client.query("COMMIT");
      return { status: "absent" };
    }
    if (
      current.instanceId !== input.instanceId ||
      current.controlChannel !== input.controlChannel
    ) {
      await client.query("COMMIT");
      return { activeLease: current, status: "conflict" };
    }
    if (current.epoch !== input.controlEpoch) {
      await client.query("COMMIT");
      return { currentEpoch: current.epoch, status: "stale" };
    }
    const renewedRows = await client.query<PgControlLeaseRow>(
      `
        UPDATE participant_control_leases
        SET
          last_seen_at = now(),
          lease_expires_at = now() + $5 * interval '1 millisecond'
        WHERE session_id = $1
          AND participant_id = $2
          AND instance_id = $3
          AND control_channel = $4
          AND epoch = $6
          AND released_at IS NULL
          AND superseded_at IS NULL
        RETURNING ${controlLeaseReturningColumns}
      `,
      [
        input.sessionId,
        input.participantId,
        input.instanceId,
        input.controlChannel,
        input.leaseTtlMs,
        input.controlEpoch,
      ],
    );
    const renewed = renewedRows.rows[0];
    if (!renewed) {
      await client.query("COMMIT");
      return { currentEpoch: current.epoch, status: "stale" };
    }
    const lease = toControlLease(renewed);
    await client.query("COMMIT");
    return { lease, status: "renewed" };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Identity and Control Epoch a protected mutation validates atomically against
 * the current durable lease, inside the mutation's own transaction. The RFC
 * requires epoch validation and the protected mutation to share one transaction
 * so a same-instance reconnect cannot advance the epoch in the gap between a
 * separate validation and the write.
 */
export interface ControlEpochGuard {
  readonly controlChannel: ControlChannel;
  readonly controlEpoch: number;
  readonly instanceId: string;
  readonly participantId: string;
  readonly sessionId: string;
}

/**
 * Locks the current control lease row and verifies the supplied Control Epoch is
 * the current generation, in the caller's transaction. It fails with
 * ControlEpochStaleError when no current lease exists, when a different instance
 * or channel owns it, or when the epoch was fenced by a newer acquisition. This
 * is the atomic epoch fence the RFC requires at the mutation boundary: because
 * the lease row is locked FOR UPDATE inside the same transaction as the mutation,
 * an interleaving same-instance reconnect cannot advance the generation between
 * this check and the write.
 */
async function assertControlEpochCurrentWithClient(
  client: TransactionClient,
  guard: ControlEpochGuard,
): Promise<void> {
  const rows = await client.query<{
    readonly controlChannel: string;
    readonly epoch: unknown;
    readonly instanceId: string;
  }>(
    `
      SELECT
        control_channel AS "controlChannel",
        epoch,
        instance_id AS "instanceId"
      FROM participant_control_leases
      WHERE session_id = $1
        AND participant_id = $2
        AND released_at IS NULL
        AND superseded_at IS NULL
        AND lease_expires_at > now()
      ORDER BY lease_expires_at DESC, claimed_at DESC, instance_id
      FOR UPDATE
    `,
    [guard.sessionId, guard.participantId],
  );
  const current = rows.rows[0];
  const currentEpoch = current ? parseControlEpoch(current.epoch) : null;
  if (
    !current ||
    current.instanceId !== guard.instanceId ||
    current.controlChannel !== guard.controlChannel ||
    currentEpoch === null ||
    currentEpoch !== guard.controlEpoch
  ) {
    throw new ControlEpochStaleError({
      controlChannel: guard.controlChannel,
      currentEpoch,
      participantId: guard.participantId,
      providedEpoch: guard.controlEpoch,
      sessionId: guard.sessionId,
    });
  }
}

/** Allocates the next strictly-monotonic Control Epoch for a participant. */
async function allocateNextControlEpoch(
  client: TransactionClient,
  sessionId: string,
  participantId: string,
): Promise<number> {
  const maxRows = await client.query<{ readonly maxEpoch: unknown }>(
    `
      SELECT max(epoch) AS "maxEpoch"
      FROM participant_control_leases
      WHERE session_id = $1
        AND participant_id = $2
    `,
    [sessionId, participantId],
  );
  const previousEpoch = parseControlEpoch(maxRows.rows[0]?.maxEpoch);
  return nextControlEpoch(previousEpoch);
}

/**
 * Releases a control-channel lease when a runtime instance intentionally
 * disconnects.
 */
export async function releaseControlLease(
  database: DatabasePool,
  input: {
    readonly controlChannel: ControlChannel;
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  },
): Promise<void> {
  await database.db
    .update(participantControlLeases)
    .set({ leaseExpiresAt: sql`now()`, releasedAt: sql`now()` })
    .where(
      and(
        eq(participantControlLeases.sessionId, input.sessionId),
        eq(participantControlLeases.participantId, input.participantId),
        eq(participantControlLeases.instanceId, input.instanceId),
        eq(participantControlLeases.controlChannel, input.controlChannel),
        // A fenced (stale) epoch must not release the replacement lease. When an
        // epoch is supplied the release only matches the exact current
        // generation; a superseded caller no longer owns the current row.
        ...(input.controlEpoch === undefined
          ? []
          : [eq(participantControlLeases.epoch, input.controlEpoch)]),
        isNull(participantControlLeases.releasedAt),
        isNull(participantControlLeases.supersededAt),
      ),
    );
}

/**
 * Lists read-only participant control-lease snapshots for operator debugging.
 */
export async function listControlLeaseSnapshots(
  database: DatabasePool,
  sessionId: string,
): Promise<ControlLeaseSnapshot[]> {
  const rows = await database.db
    .select({
      claimedAt: participantControlLeases.claimedAt,
      controlChannel: participantControlLeases.controlChannel,
      epoch: participantControlLeases.epoch,
      instanceId: participantControlLeases.instanceId,
      lastSeenAt: participantControlLeases.lastSeenAt,
      leaseExpiresAt: participantControlLeases.leaseExpiresAt,
      participantId: participantControlLeases.participantId,
      releasedAt: participantControlLeases.releasedAt,
      sessionId: participantControlLeases.sessionId,
      supersededAt: participantControlLeases.supersededAt,
      status: sql<ControlLeaseStatus>`
        CASE
          WHEN ${participantControlLeases.supersededAt} IS NOT NULL THEN 'superseded'
          WHEN ${participantControlLeases.releasedAt} IS NOT NULL THEN 'released'
          WHEN ${participantControlLeases.leaseExpiresAt} <= now() THEN 'expired'
          ELSE 'active'
        END
      `,
    })
    .from(participantControlLeases)
    .where(eq(participantControlLeases.sessionId, sessionId))
    .orderBy(
      participantControlLeases.participantId,
      participantControlLeases.instanceId,
      desc(participantControlLeases.claimedAt),
    );
  return rows.map(toControlLeaseSnapshot);
}

/**
 * Lists every session with aggregate activity counts for the operator session
 * list view. Counts come from grouped companion queries so the round-trip cost
 * stays constant regardless of session count, then are merged in memory.
 */
export async function listSessions(database: DatabasePool): Promise<SessionListItem[]> {
  const [sessionRows, participantCounts, taskCounts, eventCounts, bindingRows] = await Promise.all([
    database.db.select().from(sessions).orderBy(desc(sessions.createdAt)),
    database.db
      .select({
        sessionId: participants.sessionId,
        total: sql<number>`count(*)::int`,
      })
      .from(participants)
      .groupBy(participants.sessionId),
    database.db
      .select({
        active: sql<number>`(count(*) filter (
          where ${tasks.cancelledAt} is null
            and ${tasks.completedAt} is null
            and ${tasks.failedAt} is null
        ))::int`,
        sessionId: tasks.sessionId,
        total: sql<number>`count(*)::int`,
      })
      .from(tasks)
      .groupBy(tasks.sessionId),
    database.db
      .select({
        lastCreatedAt: sql<Date | null>`max(${sessionEvents.createdAt})`,
        sessionId: sessionEvents.sessionId,
        total: sql<number>`count(*)::int`,
      })
      .from(sessionEvents)
      .groupBy(sessionEvents.sessionId),
    database.db
      .select({
        externalId: clientSessionBindings.externalId,
        provider: clientSessionBindings.provider,
        sessionId: clientSessionBindings.sessionId,
      })
      .from(clientSessionBindings)
      .where(isNull(clientSessionBindings.archivedAt))
      .orderBy(clientSessionBindings.provider, clientSessionBindings.externalId),
  ]);

  const participantBySession = new Map(participantCounts.map((row) => [row.sessionId, row.total]));
  const taskBySession = new Map(taskCounts.map((row) => [row.sessionId, row]));
  const eventBySession = new Map(eventCounts.map((row) => [row.sessionId, row]));
  const bindingsBySession = new Map<string, SessionBindingSummary[]>();
  for (const row of bindingRows) {
    const list = bindingsBySession.get(row.sessionId) ?? [];
    list.push({ externalId: row.externalId, provider: row.provider });
    bindingsBySession.set(row.sessionId, list);
  }

  const items = sessionRows.map((row): SessionListItem => {
    const taskCount = taskBySession.get(row.sessionId);
    const eventCount = eventBySession.get(row.sessionId);
    const lastCreatedAt = eventCount?.lastCreatedAt ?? null;
    return {
      activeTaskCount: taskCount?.active ?? 0,
      bindings: bindingsBySession.get(row.sessionId) ?? [],
      createdAt: row.createdAt.toISOString(),
      eventCount: eventCount?.total ?? 0,
      lastEventAt: lastCreatedAt ? new Date(lastCreatedAt).toISOString() : null,
      participantCount: participantBySession.get(row.sessionId) ?? 0,
      sessionId: row.sessionId,
      taskCount: taskCount?.total ?? 0,
    };
  });

  return items.sort(compareSessionsByRecentActivity);
}

/** Orders sessions by most recent event, falling back to creation time. */
function compareSessionsByRecentActivity(left: SessionListItem, right: SessionListItem): number {
  const leftActivity = left.lastEventAt ?? left.createdAt;
  const rightActivity = right.lastEventAt ?? right.createdAt;
  if (leftActivity === rightActivity) {
    return left.sessionId < right.sessionId ? -1 : 1;
  }
  return leftActivity < rightActivity ? 1 : -1;
}

/**
 * Explicitly creates session prerequisites and reports whether a new session
 * row was inserted by this call.
 */
export async function createSession(
  database: DatabasePool,
  sessionId: string,
): Promise<CreateSessionResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    const result = await createSessionWithClient(client, sessionId);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/** Permanently deletes one durable session and all session-owned rows. */
export async function deleteSession(database: DatabasePool, sessionId: string): Promise<boolean> {
  const deleted = await database.db
    .delete(sessions)
    .where(eq(sessions.sessionId, sessionId))
    .returning({ sessionId: sessions.sessionId });
  return deleted.length > 0;
}

/**
 * Reads an existing session row without creating session prerequisites.
 */
export async function readSession(
  database: DatabasePool,
  sessionId: string,
): Promise<SessionRecord> {
  const row = await database.db.query.sessions.findFirst({
    where: eq(sessions.sessionId, sessionId),
  });
  return toSessionRecord(row);
}

/**
 * Appends one session event and allocates its canonical sequence number in the
 * same Drizzle transaction.
 */
export async function appendEvent(
  database: DatabasePool,
  input: AppendSessionEventInput,
  options: {
    /** Optional atomic Control Epoch fence for participant-owned producers. */
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly sourceId: string;
  },
): Promise<SessionEvent> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (options.controlGuard) {
      await assertControlEpochCurrentWithClient(client, options.controlGuard);
    }
    const event = await appendEventWithClient(client, input, options.sourceId);
    await client.query("COMMIT");
    return event;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Appends a generic caller-published event with retry-safe idempotency for a
 * caller-supplied event id. Existing rows are read before sequence allocation.
 */
export async function appendEventIdempotent(
  database: DatabasePool,
  input: AppendSessionEventInput,
  options: {
    /** Optional atomic Control Epoch fence for participant-owned producers. */
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly sourceId: string;
  },
): Promise<PersistedEventAppendResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (options.controlGuard) {
      await assertControlEpochCurrentWithClient(client, options.controlGuard);
    }
    await acquireTransactionAdvisoryLock(client, "session_event_id", input.eventId);
    const existing = await readSessionEventByEventIdWithClient(client, input.eventId);
    if (existing) {
      await client.query("COMMIT");
      const conflictingFields = compareEventCreateInput(existing, input);
      if (conflictingFields.length === 0) {
        return { event: existing, events: [], status: "replayed" };
      }
      return {
        conflictingFields,
        eventId: input.eventId,
        events: [],
        status: "conflict",
      };
    }
    const event = await appendEventWithClient(client, input, options.sourceId);
    await client.query("COMMIT");
    return { event, events: [event], status: "created" };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Expires active task claims whose lease elapsed, clearing the claim and
 * appending the durable claim-expired event in the same Postgres transaction.
 */
export async function expireTaskClaims(
  database: DatabasePool,
  input: {
    readonly batchSize: number;
    readonly sourceId: string;
  },
): Promise<SessionEvent[]> {
  if (input.batchSize <= 0) {
    return [];
  }
  return runTaskClaimExpirationTransactionWithDeadlockRetry({
    action: async (client, context) => {
      const expiredRows = await client.query<ExpiredTaskClaimRow>(
        `
        WITH expired AS (
          SELECT
            session_id,
            task_id,
            claimed_by AS previous_claimed_by
          FROM tasks
          WHERE claimed_at IS NOT NULL
            AND claimed_by IS NOT NULL
            AND claim_expires_at IS NOT NULL
            AND claim_expires_at <= now()
            AND completed_at IS NULL
            AND failed_at IS NULL
            AND cancelled_at IS NULL
          ORDER BY claim_expires_at, session_id, task_id
          LIMIT $1
          FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks
        SET
          claimed_at = NULL,
          claimed_by = NULL,
          claim_expires_at = NULL,
          claim_expired_at = now(),
          claim_expired_by = expired.previous_claimed_by,
          released_at = NULL,
          released_by = NULL
        FROM expired
        WHERE tasks.session_id = expired.session_id
          AND tasks.task_id = expired.task_id
        RETURNING
          tasks.cancelled_at AS "cancelledAt",
          tasks.claim_expired_at AS "claimExpiredAt",
          tasks.claim_expired_by AS "claimExpiredBy",
          tasks.claim_expires_at AS "claimExpiresAt",
          tasks.claimed_at AS "claimedAt",
          tasks.claimed_by AS "claimedBy",
          tasks.completed_at AS "completedAt",
          tasks.created_at AS "createdAt",
          tasks.failed_at AS "failedAt",
          tasks.failure,
          tasks.input,
          tasks.kind,
          tasks.mailbox_account_id AS "mailboxAccountId",
          tasks.mailbox_provider AS "mailboxProvider",
          tasks.objective,
          expired.previous_claimed_by AS "previousClaimedBy",
          tasks.released_at AS "releasedAt",
          tasks.released_by AS "releasedBy",
          tasks.result,
          tasks.schedule_algorithm_version AS "scheduleAlgorithmVersion",
          tasks.schedule_interval_ms AS "scheduleIntervalMs",
          tasks.schedule_window_start AS "scheduleWindowStart",
          tasks.session_id AS "sessionId",
          tasks.task_id AS "taskId"
      `,
        [input.batchSize],
      );
      context.recordExpiredRowCount(expiredRows.rows.length);
      const events: SessionEvent[] = [];
      for (const row of sortExpiredTaskClaimRows(expiredRows.rows)) {
        const task = toTaskRecord(row);
        const eventInput = buildTaskClaimExpiredEventInput({
          previousClaimedBy: row.previousClaimedBy,
          sessionId: task.sessionId,
          task,
        });
        events.push(await appendEventWithClient(client, eventInput, input.sourceId));
      }
      return events;
    },
    pool: database.pool,
    requestedBatchSize: input.batchSize,
  });
}

/**
 * Sorts expired task rows into a stable lock acquisition order before appending
 * claim-expired events.
 */
export function sortExpiredTaskClaimRows<
  TRow extends { readonly sessionId: string; readonly taskId: string },
>(rows: readonly TRow[]): TRow[] {
  return [...rows].sort((left, right) => {
    const sessionOrder = left.sessionId.localeCompare(right.sessionId);
    return sessionOrder === 0 ? left.taskId.localeCompare(right.taskId) : sessionOrder;
  });
}

/**
 * Runs one task claim expiration transaction with a bounded retry loop for
 * Postgres deadlocks.
 */
export async function runTaskClaimExpirationTransactionWithDeadlockRetry<TValue>(input: {
  readonly action: (
    client: TransactionClient,
    context: TaskClaimExpirationAttemptContext,
  ) => Promise<TValue>;
  readonly maxAttempts?: number;
  readonly operation?: string;
  readonly pool: TaskClaimExpirationConnectionPool;
  readonly requestedBatchSize: number;
}): Promise<TValue> {
  const maxAttempts = input.maxAttempts ?? defaultTaskClaimExpirationMaxAttempts;
  const operation = input.operation ?? "expireTaskClaims";
  let attempt = 1;
  while (attempt <= maxAttempts) {
    const client = await input.pool.connect();
    let expiredRowCount: number | undefined;
    try {
      await client.query("BEGIN");
      const value = await input.action(client, {
        attempt,
        recordExpiredRowCount: (rowCount) => {
          expiredRowCount = rowCount;
        },
      });
      await client.query("COMMIT");
      return value;
    } catch (error) {
      await rollbackTaskClaimExpirationAttempt(client);
      if (!isPostgresDeadlockError(error)) {
        throw error;
      }
      if (attempt >= maxAttempts) {
        throw new TaskClaimExpirationDeadlockError({
          diagnostics: {
            ...(expiredRowCount === undefined ? {} : { expiredRowCount }),
            maxAttempts,
            operation,
            requestedBatchSize: input.requestedBatchSize,
            retryAttempt: attempt,
            sqlState: taskClaimExpirationDeadlockSqlState,
          },
          originalError: error,
        });
      }
      attempt += 1;
    } finally {
      client.release();
    }
  }
  throw new Error("Task claim expiration retry loop ended without a result");
}

/** Rolls back a failed task-claim expiration attempt without masking the cause. */
async function rollbackTaskClaimExpirationAttempt(
  client: ReleasableTransactionClient,
): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {}
}

/** Detects Postgres deadlock errors without retrying unrelated failures. */
function isPostgresDeadlockError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { readonly code?: unknown }).code === taskClaimExpirationDeadlockSqlState
  );
}

/**
 * Inserts or refreshes visible participant presence and classifies whether the
 * update should produce joined, updated, or refreshed semantics. Classification
 * is serialized per `(session_id, participant_id)` so concurrent first
 * registrations cannot both observe an absent row and both report `joined`.
 */
export async function upsertParticipant(
  database: DatabasePool,
  input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly participantId: string;
    readonly runtimeKind: string;
    readonly sessionId: string;
  },
): Promise<ParticipantRegistration> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    const registration = await upsertParticipantWithClient(client, input);
    await client.query("COMMIT");
    return registration;
  } catch (error) {
    await client.query("ROLLBACK");
    throw new ParticipantRegistrationTransactionRollbackError("upsertParticipant", error);
  } finally {
    client.release();
  }
}

/**
 * Inserts or refreshes visible participant presence and appends the matching
 * registration event in the same database transaction.
 */
export async function upsertParticipantWithEvent(
  database: DatabasePool,
  input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly runtimeKind: string;
    readonly sessionId: string;
  },
): Promise<PersistedParticipantRegistrationResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    const registration = await upsertParticipantWithClient(client, input);
    const eventInput =
      registration.status === "updated"
        ? buildParticipantRegistrationEventInput({
            participant: registration.participant,
            previousParticipant: registration.previousParticipant,
            sessionId: input.sessionId,
            status: registration.status,
          })
        : buildParticipantRegistrationEventInput({
            participant: registration.participant,
            sessionId: input.sessionId,
            status: registration.status,
          });
    const events = eventInput
      ? [
          await appendEventWithClient(client, eventInput, input.eventSourceId, {
            ensureSession: false,
          }),
        ]
      : [];
    await client.query("COMMIT");
    return { events, registration };
  } catch (error) {
    await client.query("ROLLBACK");
    throw new ParticipantRegistrationTransactionRollbackError("upsertParticipantWithEvent", error);
  } finally {
    client.release();
  }
}

/** Error raised after participant registration rolls back its transaction. */
class ParticipantRegistrationTransactionRollbackError extends Error {
  constructor(
    readonly operation: string,
    readonly originalError: unknown,
  ) {
    super(
      `Participant registration transaction rollback during ${operation}: ${formatErrorMessage(originalError)}`,
    );
    this.name = "ParticipantRegistrationTransactionRollbackError";
  }
}

/**
 * Owns the serialized participant registration invariant. The advisory lock
 * covers first inserts where a row lock would otherwise have no row to lock.
 */
async function upsertParticipantWithClient(
  client: TransactionClient,
  input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly participantId: string;
    readonly runtimeKind: string;
    readonly sessionId: string;
  },
): Promise<ParticipantRegistration> {
  await requireSessionWithClient(client, input.sessionId, "upsertParticipant");
  await acquireTransactionAdvisoryLock(client, input.sessionId, input.participantId);
  const existingRows = await client.query<PgParticipantRow>(
    `
      SELECT ${participantReturningColumns}
      FROM participants
      WHERE session_id = $1
        AND participant_id = $2
      FOR UPDATE
    `,
    [input.sessionId, input.participantId],
  );
  const rows = await client.query<PgParticipantRow>(
    `
      INSERT INTO participants (
        capabilities,
        display_name,
        participant_id,
        runtime_kind,
        session_id
      )
      VALUES ($1::jsonb, $2, $3, $4, $5)
      ON CONFLICT (session_id, participant_id) DO UPDATE
      SET
        capabilities = $1::jsonb,
        display_name = $2,
        last_seen_at = now(),
        runtime_kind = $4
      RETURNING ${participantReturningColumns}
    `,
    [
      JSON.stringify(input.capabilities),
      input.displayName,
      input.participantId,
      input.runtimeKind,
      input.sessionId,
    ],
  );
  const participant = toParticipantRecord(rows.rows[0]);
  const existing = existingRows.rows[0];
  if (!existing) {
    return { participant, status: "joined" };
  }
  const previousParticipant = toParticipantRecord(existing);
  if (participantPresenceChanged(previousParticipant, participant)) {
    return { participant, previousParticipant, status: "updated" };
  }
  return { participant, status: "refreshed" };
}

/**
 * Refreshes participant liveness and optionally updates capabilities.
 */
export async function heartbeatParticipant(
  database: DatabasePool,
  input: {
    readonly capabilities?: Record<string, unknown>;
    readonly participantId: string;
    readonly sessionId: string;
  },
): Promise<ParticipantRecord | null> {
  const rows = await database.db
    .update(participants)
    .set({
      ...(input.capabilities ? { capabilities: input.capabilities } : {}),
      lastSeenAt: sql`now()`,
    })
    .where(
      and(
        eq(participants.sessionId, input.sessionId),
        eq(participants.participantId, input.participantId),
      ),
    )
    .returning();
  return rows[0] ? toParticipantRecord(rows[0]) : null;
}

/** Outcome of an atomic epoch-fenced participant heartbeat. */
export type HeartbeatParticipantWithEventResult =
  | { readonly participant: null }
  | { readonly event: SessionEvent; readonly participant: ParticipantRecord };

/**
 * Refreshes participant presence and appends the durable heartbeat event inside
 * one transaction, optionally fenced by a Control Epoch guard.
 *
 * When a guard is supplied, the current control lease row is locked and the epoch
 * is validated FIRST, so a superseded (stale-epoch) heartbeat rolls back before it
 * can refresh `last_seen_at` or capabilities. This keeps presence and the
 * heartbeat event on the same atomic fence the RFC requires at every mutation
 * boundary; without it, a stale heartbeat could still refresh presence outside the
 * fenced event append. When no guard is supplied (legacy enforcement-off path),
 * the presence refresh and event append still commit together but without an epoch
 * check, preserving existing behavior.
 */
export async function heartbeatParticipantWithEvent(
  database: DatabasePool,
  input: {
    readonly capabilities?: Record<string, unknown> | undefined;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  },
): Promise<HeartbeatParticipantWithEventResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (input.controlGuard) {
      await assertControlEpochCurrentWithClient(client, input.controlGuard);
    }
    const setCapabilities = input.capabilities !== undefined;
    const rows = await client.query<PgParticipantRow>(
      `
        UPDATE participants
        SET
          last_seen_at = now()${setCapabilities ? ",\n          capabilities = $3::jsonb" : ""}
        WHERE session_id = $1
          AND participant_id = $2
        RETURNING ${participantReturningColumns}
      `,
      setCapabilities
        ? [input.sessionId, input.participantId, JSON.stringify(input.capabilities)]
        : [input.sessionId, input.participantId],
    );
    const participantRow = rows.rows[0];
    if (!participantRow) {
      await client.query("COMMIT");
      return { participant: null };
    }
    const participant = toParticipantRecord(participantRow);
    const event = await appendEventWithClient(
      client,
      buildParticipantHeartbeatEventInput({ participant, sessionId: input.sessionId }),
      input.eventSourceId,
      { ensureSession: false },
    );
    await client.query("COMMIT");
    return { event, participant };
  } catch (error) {
    await client.query("ROLLBACK");
    if (error instanceof ControlEpochStaleError) {
      throw error;
    }
    throw new ParticipantRegistrationTransactionRollbackError(
      "heartbeatParticipantWithEvent",
      error,
    );
  } finally {
    client.release();
  }
}

/**
 * Lists participants in most-recently-seen order for one session.
 */
export async function listParticipants(
  database: DatabasePool,
  sessionId: string,
): Promise<ParticipantRecord[]> {
  const rows = await database.db
    .select()
    .from(participants)
    .where(eq(participants.sessionId, sessionId))
    .orderBy(desc(participants.lastSeenAt), participants.participantId);
  return rows.map(toParticipantRecord);
}

/**
 * Lists read-only participant runtime snapshots for operator debugging.
 */
export async function listParticipantRuntimeSnapshots(
  database: DatabasePool,
  sessionId: string,
): Promise<ParticipantRuntimeSnapshot[]> {
  const [participantRecords, controlLeases] = await Promise.all([
    listParticipants(database, sessionId),
    listControlLeaseSnapshots(database, sessionId),
  ]);
  return buildParticipantRuntimeSnapshots(sessionId, participantRecords, controlLeases);
}

/**
 * Builds a read-only aggregate debug summary for one session.
 */
export async function readSessionDebugSummary(
  database: DatabasePool,
  sessionId: string,
): Promise<SessionDebugSummary> {
  const [participantRecords, controlLeases, taskSnapshots] = await Promise.all([
    listParticipants(database, sessionId),
    listControlLeaseSnapshots(database, sessionId),
    listTaskSnapshots(database, sessionId),
  ]);
  const participantRuntimes = buildParticipantRuntimeSnapshots(
    sessionId,
    participantRecords,
    controlLeases,
  );
  return buildSessionDebugSummary(sessionId, participantRuntimes, controlLeases, taskSnapshots);
}

/**
 * Lists session events after the provided sequence cursor.
 */
export async function listEvents(
  database: DatabasePool,
  sessionId: string,
  afterSeq: number,
  options: SessionEventListOptions = {},
): Promise<SessionEvent[]> {
  const limitClause = options.limit === undefined || options.limit <= 0 ? "" : "LIMIT $3";
  const values =
    options.limit === undefined || options.limit <= 0
      ? [sessionId, afterSeq]
      : [sessionId, afterSeq, options.limit];
  const rows = await database.pool.query<PgSessionEventRow>(
    `
      SELECT
        created_at AS "createdAt",
        event_id AS "eventId",
        payload,
        producer_id AS "producerId",
        seq AS "seq",
        session_id AS "sessionId",
        type
      FROM session_events
      WHERE session_id = $1
        AND seq > $2
      ORDER BY seq
      ${limitClause}
    `,
    values,
  );
  return rows.rows.map(toSessionEvent);
}

/**
 * Creates a durable task and appends its canonical lifecycle event in one
 * database transaction.
 */
export async function createTaskWithEvent(
  database: DatabasePool,
  input: {
    readonly eventSourceId: string;
    readonly input?: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly schedule?: ScheduledTaskIdentityInput | undefined;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskEventResult> {
  return runTaskEventTransaction(database, {
    operation: "createTask",
    sourceId: input.eventSourceId,
    mutate: (client) => insertTaskWithClient(client, input),
    buildEvent: (task) => buildTaskCreatedEventInput({ sessionId: input.sessionId, task }),
  });
}

/**
 * Creates a task and task.created event with idempotent replay only when the
 * task id was supplied by the caller.
 */
export async function createTaskWithEventIdempotent(
  database: DatabasePool,
  input: {
    readonly eventSourceId: string;
    readonly input?: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly schedule?: ScheduledTaskIdentityInput | undefined;
    readonly sessionId: string;
    readonly taskId: string;
    readonly taskIdSource: "caller" | "generated";
  },
): Promise<PersistedTaskCreateResult> {
  if (input.taskIdSource === "generated") {
    const persisted = await createTaskWithEvent(database, input);
    if (!persisted) {
      throw new Error("Generated task id create returned no task");
    }
    return { ...persisted, events: [persisted.event], status: "created" };
  }

  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    await acquireTransactionAdvisoryLock(client, input.sessionId, input.taskId);
    const existing = await readTaskWithClient(client, input.sessionId, input.taskId);
    if (existing) {
      await client.query("COMMIT");
      const conflictingFields = compareTaskCreateInput(existing, input);
      if (conflictingFields.length === 0) {
        return { events: [], status: "replayed", task: existing };
      }
      return {
        conflictingFields,
        events: [],
        status: "conflict",
        task: null,
        taskId: input.taskId,
      };
    }

    const task = await insertTaskWithClient(client, input);
    const event = await appendEventWithClient(
      client,
      buildTaskCreatedEventInput({ sessionId: input.sessionId, task }),
      input.eventSourceId,
    );
    await client.query("COMMIT");
    return { event, events: [event], status: "created", task };
  } catch (error) {
    await client.query("ROLLBACK");
    throw new TaskEventTransactionRollbackError("createTask", error);
  } finally {
    client.release();
  }
}

/**
 * Lists tasks in newest-first order for one session and lifecycle filter.
 */
export async function listTasks(
  database: DatabasePool,
  sessionId: string,
  status: TaskListStatus = "active",
): Promise<TaskRecord[]> {
  const rows = await database.db
    .select()
    .from(tasks)
    .where(and(eq(tasks.sessionId, sessionId), taskListStatusWhere(status)))
    .orderBy(desc(tasks.createdAt), tasks.taskId);
  return rows.map(toTaskRecord);
}

/**
 * Reads one durable task by session and task id.
 */
export async function getTask(
  database: DatabasePool,
  input: {
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<TaskRecord | null> {
  const rows = await database.db
    .select()
    .from(tasks)
    .where(and(eq(tasks.sessionId, input.sessionId), eq(tasks.taskId, input.taskId)))
    .limit(1);
  return rows[0] ? toTaskRecord(rows[0]) : null;
}

/**
 * Converts a public task list filter into the matching Drizzle predicate.
 */
function taskListStatusWhere(status: TaskListStatus) {
  if (status === "all") {
    return undefined;
  }
  if (status === "terminal") {
    return or(
      isNotNull(tasks.cancelledAt),
      isNotNull(tasks.completedAt),
      isNotNull(tasks.failedAt),
    );
  }
  return and(isNull(tasks.cancelledAt), isNull(tasks.completedAt), isNull(tasks.failedAt));
}

/**
 * Lists read-only task snapshots with derived lifecycle state for operator
 * debugging.
 */
export async function listTaskSnapshots(
  database: DatabasePool,
  sessionId: string,
): Promise<TaskSnapshot[]> {
  const [rows, approvals] = await Promise.all([
    database.db
      .select()
      .from(tasks)
      .where(eq(tasks.sessionId, sessionId))
      .orderBy(desc(tasks.createdAt), tasks.taskId),
    listTaskApprovalsForSession(database, sessionId),
  ]);
  const approvalsByTask = groupTaskApprovalsByTask(approvals);
  const observedAt = new Date();
  return rows.map((row) => toTaskSnapshot(row, observedAt, approvalsByTask.get(row.taskId) ?? []));
}

/** Lists durable approval decisions for one task. */
export async function listTaskApprovals(
  database: DatabasePool,
  input: {
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<TaskApprovalRecord[]> {
  const rows = await database.db
    .select()
    .from(taskApprovals)
    .where(
      and(eq(taskApprovals.sessionId, input.sessionId), eq(taskApprovals.taskId, input.taskId)),
    )
    .orderBy(taskApprovals.decidedAt, taskApprovals.targetKey);
  return rows.map(toTaskApprovalRecord);
}

async function listTaskApprovalsForSession(
  database: DatabasePool,
  sessionId: string,
): Promise<TaskApprovalRecord[]> {
  const rows = await database.db
    .select()
    .from(taskApprovals)
    .where(eq(taskApprovals.sessionId, sessionId))
    .orderBy(taskApprovals.taskId, taskApprovals.decidedAt, taskApprovals.targetKey);
  return rows.map(toTaskApprovalRecord);
}

/**
 * Records one approval decision and appends its compatibility event atomically.
 */
export async function recordTaskApproval(
  database: DatabasePool,
  input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly decision: ApprovalDecision;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly reason: Record<string, unknown>;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskApprovalResult | null> {
  const targetKey = approvalTargetKey(input.reason);
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (input.controlGuard) {
      await assertControlEpochCurrentWithClient(client, input.controlGuard);
    }
    const task = await readTaskWithClient(client, input.sessionId, input.taskId);
    if (!task) {
      await client.query("COMMIT");
      return null;
    }
    const eventInput = buildTaskApprovalRecordedEventInput({
      decision: input.decision,
      participantId: input.participantId,
      reason: input.reason,
      sessionId: input.sessionId,
      task,
    });
    const insertedRows = await client.query<PgTaskApprovalRow>(
      `
        INSERT INTO task_approvals (
          approval_event_id,
          decided_by_participant_id,
          decision,
          reason,
          session_id,
          target_key,
          task_id
        )
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
        ON CONFLICT (session_id, task_id, target_key) DO NOTHING
        RETURNING ${taskApprovalReturningColumns}
      `,
      [
        eventInput.eventId,
        input.participantId,
        input.decision,
        JSON.stringify(input.reason),
        input.sessionId,
        targetKey,
        input.taskId,
      ],
    );
    if (!insertedRows.rows[0]) {
      const existing = await readTaskApprovalWithClient(client, {
        sessionId: input.sessionId,
        targetKey,
        taskId: input.taskId,
      });
      await client.query("COMMIT");
      if (!existing) {
        throw new Error("Approval conflict did not return an existing approval row");
      }
      return {
        approval: existing,
        decision: input.decision,
        events: [],
        existingDecision: existing.decision,
        status: "ignored",
        task,
        targetKey,
      };
    }
    const event = await appendEventWithClient(client, eventInput, input.eventSourceId);
    await client.query("COMMIT");
    return {
      decision: input.decision,
      event,
      events: [event] as const,
      status: "recorded",
      task,
      targetKey,
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Claims an unclaimed task and appends `task.claimed` in the same transaction.
 */
export async function claimTaskWithEvent(
  database: DatabasePool,
  input: {
    readonly claimLeaseTtlMs: number;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskEventResult> {
  assertPositiveFiniteTtlMs(input.claimLeaseTtlMs, "task claim TTL");
  return runTaskEventTransaction(database, {
    controlGuard: input.controlGuard,
    operation: "claimTask",
    sourceId: input.eventSourceId,
    mutate: async (client) => {
      const rows = await client.query<PgTaskRow>(
        `
          UPDATE tasks
          SET
            claimed_at = now(),
            claimed_by = $1,
            claim_expires_at = now() + ($2::text || ' milliseconds')::interval,
            claim_expired_at = NULL,
            claim_expired_by = NULL,
            released_at = NULL,
            released_by = NULL
          WHERE session_id = $3
            AND task_id = $4
            AND claimed_at IS NULL
            AND claimed_by IS NULL
            AND completed_at IS NULL
            AND failed_at IS NULL
            AND cancelled_at IS NULL
          RETURNING ${taskReturningColumns}
        `,
        [input.participantId, input.claimLeaseTtlMs, input.sessionId, input.taskId],
      );
      return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
    },
    buildEvent: (task) =>
      buildTaskClaimedEventInput({
        participantId: input.participantId,
        sessionId: input.sessionId,
        task,
      }),
  });
}

/**
 * Refreshes an active task claim before its lease expires.
 */
export async function refreshTaskClaim(
  database: DatabasePool,
  input: {
    readonly claimLeaseTtlMs: number;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<TaskRecord | null> {
  assertPositiveFiniteTtlMs(input.claimLeaseTtlMs, "task claim TTL");
  if (input.controlGuard) {
    return refreshTaskClaimGuarded(database, { ...input, controlGuard: input.controlGuard });
  }
  const rows = await database.db
    .update(tasks)
    .set({ claimExpiresAt: leaseDeadlineSql(input.claimLeaseTtlMs) })
    .where(
      and(
        eq(tasks.sessionId, input.sessionId),
        eq(tasks.taskId, input.taskId),
        eq(tasks.claimedBy, input.participantId),
        sql`${tasks.claimExpiresAt} > now()`,
        isNull(tasks.completedAt),
        isNull(tasks.failedAt),
        isNull(tasks.cancelledAt),
      ),
    )
    .returning();
  return rows[0] ? toTaskRecord(rows[0]) : null;
}

/**
 * Refreshes a task claim under an atomic Control Epoch fence. The epoch is
 * validated against the current durable lease inside the same transaction as the
 * claim-deadline update, so a fenced caller cannot extend a claim it no longer
 * controls.
 */
async function refreshTaskClaimGuarded(
  database: DatabasePool,
  input: {
    readonly claimLeaseTtlMs: number;
    readonly controlGuard: ControlEpochGuard;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<TaskRecord | null> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    await assertControlEpochCurrentWithClient(client, input.controlGuard);
    const rows = await client.query<PgTaskRow>(
      `
        UPDATE tasks
        SET claim_expires_at = now() + ($1::text || ' milliseconds')::interval
        WHERE session_id = $2
          AND task_id = $3
          AND claimed_by = $4
          AND claim_expires_at > now()
          AND completed_at IS NULL
          AND failed_at IS NULL
          AND cancelled_at IS NULL
        RETURNING ${taskReturningColumns}
      `,
      [input.claimLeaseTtlMs, input.sessionId, input.taskId, input.participantId],
    );
    await client.query("COMMIT");
    return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Cancels a non-terminal task and appends `control.cancel` in one transaction.
 */
export async function cancelTaskWithEvent(
  database: DatabasePool,
  input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly reason?: Record<string, unknown> | undefined;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskEventResult> {
  return runTaskEventTransaction(database, {
    controlGuard: input.controlGuard,
    operation: "cancelTask",
    sourceId: input.eventSourceId,
    mutate: async (client) => {
      const rows = await client.query<PgTaskRow>(
        `
          UPDATE tasks
          SET
            cancelled_at = now(),
            claim_expires_at = NULL
          WHERE session_id = $1
            AND task_id = $2
            AND completed_at IS NULL
            AND failed_at IS NULL
            AND cancelled_at IS NULL
          RETURNING ${taskReturningColumns}
        `,
        [input.sessionId, input.taskId],
      );
      return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
    },
    buildEvent: (task) =>
      buildTaskCancelledEventInput({
        participantId: input.participantId,
        reason: input.reason ?? {},
        sessionId: input.sessionId,
        task,
      }),
  });
}

/** Caller-owned inputs for one atomic scheduled-run supersession. */
export interface SupersedeScheduledRunsInput {
  /**
   * Explicit task ids the operator reviewed. When present and non-empty, the
   * atomic UPDATE is additionally bounded to exactly these ids so apply cannot
   * cancel a stale run that appeared between the operator's dry-run listing and
   * apply. When omitted or empty, all runs matching the schedule identity
   * predicate are superseded.
   */
  readonly candidateTaskIds?: readonly string[] | undefined;
  readonly eventSourceId: string;
  readonly kind: string;
  readonly mailboxAccountId: string;
  readonly mailboxProvider: string;
  /** Actor recorded on each supersession cancellation event. */
  readonly participantId: string;
  readonly reason?: Record<string, unknown> | undefined;
  readonly scheduleAlgorithmVersion: number;
  readonly scheduleIntervalMs: number;
  /** Start of the current window; only strictly older runs are superseded. */
  readonly scheduleWindowStart: number;
  readonly sessionId: string;
}

/** Result of one atomic scheduled-run supersession. */
export interface SupersededScheduledRunsResult {
  readonly events: readonly SessionEvent[];
  readonly tasks: readonly TaskRecord[];
}

/**
 * Atomically supersedes older scheduled runs that share one schedule identity.
 *
 * This is the single service-owned mutation the RFC's `pending(old) -> cancelled`
 * transition requires. A list-then-generic-cancel sequence is forbidden because
 * it races a worker claim: the predicate below is the atomic fence. It cancels
 * only rows that match the exact schedule identity (session, kind, Mailbox
 * Scope, algorithm version, interval), carry a strictly older Schedule Window,
 * and are still unclaimed and nonterminal. A claim that lands before this
 * transaction sets `claimed_by`, so the row no longer matches and cannot be
 * cancelled; manual tasks (null schedule columns), terminal tasks, and tasks
 * with a different schedule identity are excluded by the same predicate.
 *
 * When the caller supplies an explicit `candidateTaskIds` set (the ids the
 * operator reviewed at dry-run), the UPDATE is additionally bounded to those
 * ids so apply matches exactly the reviewed set and cannot cancel a stale run
 * that appeared between the dry-run listing and apply.
 */
export async function supersedeScheduledRunsWithEvent(
  database: DatabasePool,
  input: SupersedeScheduledRunsInput,
): Promise<SupersededScheduledRunsResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    // When the operator reviewed an explicit candidate set, bound the atomic
    // UPDATE to exactly those ids so apply cannot cancel a stale run that
    // appeared between the dry-run listing and apply. Omitted/empty preserves
    // the schedule-identity-only behavior.
    const candidateIds = input.candidateTaskIds ?? [];
    const boundToCandidates = candidateIds.length > 0;
    const params: unknown[] = [
      input.sessionId,
      input.kind,
      input.mailboxProvider,
      input.mailboxAccountId,
      input.scheduleAlgorithmVersion,
      input.scheduleIntervalMs,
      input.scheduleWindowStart,
    ];
    if (boundToCandidates) {
      params.push([...candidateIds]);
    }
    const rows = await client.query<PgTaskRow>(
      `
        UPDATE tasks
        SET
          cancelled_at = now(),
          claim_expires_at = NULL
        WHERE session_id = $1
          AND kind = $2
          AND mailbox_provider = $3
          AND mailbox_account_id = $4
          AND schedule_algorithm_version = $5
          AND schedule_interval_ms = $6
          AND schedule_window_start IS NOT NULL
          AND schedule_window_start < $7
          AND claimed_by IS NULL
          AND completed_at IS NULL
          AND failed_at IS NULL
          AND cancelled_at IS NULL
          ${boundToCandidates ? "AND task_id = ANY($8)" : ""}
        RETURNING ${taskReturningColumns}
      `,
      params,
    );
    const tasks = rows.rows
      .map((row) => toTaskRecord(row))
      .sort((left, right) => left.taskId.localeCompare(right.taskId));
    const reason = input.reason ?? { reason: "superseded_by_newer_window" };
    const events: SessionEvent[] = [];
    for (const task of tasks) {
      const event = await appendEventWithClient(
        client,
        buildTaskCancelledEventInput({
          participantId: input.participantId,
          reason,
          sessionId: input.sessionId,
          task,
        }),
        input.eventSourceId,
      );
      events.push(event);
    }
    await client.query("COMMIT");
    return { events, tasks };
  } catch (error) {
    await client.query("ROLLBACK");
    throw new TaskEventTransactionRollbackError("supersedeScheduledRuns", error);
  } finally {
    client.release();
  }
}

/** Error raised when a supplied scheduled task id does not match its derived identity. */
export class ScheduledTaskIdentityMismatchError extends Error {
  readonly derivedTaskId: string;
  readonly suppliedTaskId: string;

  constructor(input: { readonly derivedTaskId: string; readonly suppliedTaskId: string }) {
    super(
      `Supplied scheduled task id ${input.suppliedTaskId} does not match the derived deterministic identity ${input.derivedTaskId}`,
    );
    this.name = "ScheduledTaskIdentityMismatchError";
    this.derivedTaskId = input.derivedTaskId;
    this.suppliedTaskId = input.suppliedTaskId;
  }
}

/** Caller-owned inputs for one atomic ensure-scheduled-run operation. */
export interface EnsureScheduledRunInput {
  readonly eventSourceId: string;
  /** Optional caller-supplied task id; it MUST equal the derived deterministic id. */
  readonly expectedTaskId?: string | undefined;
  readonly input?: Record<string, unknown> | null;
  readonly kind: string;
  readonly mailboxAccountId: string;
  readonly mailboxProvider: string;
  readonly objective: string;
  /** Actor recorded on each supersession cancellation event. */
  readonly participantId: string;
  readonly reason?: Record<string, unknown> | undefined;
  readonly scheduleAlgorithmVersion: number;
  readonly scheduleIntervalMs: number;
  /** Start of the current window; only strictly older runs are superseded. */
  readonly scheduleWindowStart: number;
  readonly sessionId: string;
}

/** Insert-or-replay outcome for the current deterministic scheduled run. */
export type EnsureScheduledRunCurrent =
  | { readonly event: SessionEvent; readonly status: "created"; readonly task: TaskRecord }
  | { readonly status: "replayed"; readonly task: TaskRecord }
  | { readonly status: "superseded_by_newer"; readonly task: TaskRecord };

/** Result of one atomic ensure-scheduled-run operation. */
export interface EnsureScheduledRunResult {
  readonly current: EnsureScheduledRunCurrent;
  readonly supersededEvents: readonly SessionEvent[];
  readonly supersededTasks: readonly TaskRecord[];
  readonly taskId: string;
}

/**
 * Stable advisory-lock key for one schedule identity, EXCLUDING the Schedule
 * Window start. Every window of the same recurring schedule hashes to the same
 * key, so concurrent ensures for different windows serialize on one lock. Keying
 * on the window-bearing task id instead would let two different windows take two
 * different locks and both insert, leaving two active runs.
 */
function scheduleIdentityLockKey(input: {
  readonly kind: string;
  readonly mailboxAccountId: string;
  readonly mailboxProvider: string;
  readonly scheduleAlgorithmVersion: number;
  readonly scheduleIntervalMs: number;
  readonly sessionId: string;
}): string {
  return JSON.stringify([
    "scheduled_run",
    input.scheduleAlgorithmVersion,
    input.sessionId,
    input.kind,
    input.mailboxProvider,
    input.mailboxAccountId,
    input.scheduleIntervalMs,
  ]);
}

/**
 * Reads the newest non-cancelled scheduled run for one schedule identity whose
 * Schedule Window starts strictly after the supplied window, on the current
 * transaction client. A non-null result means a newer window has already been
 * ensured, so an older-window ensure must not insert a second active run.
 */
async function readNewerScheduledRunWithClient(
  client: TransactionClient,
  input: {
    readonly kind: string;
    readonly mailboxAccountId: string;
    readonly mailboxProvider: string;
    readonly scheduleAlgorithmVersion: number;
    readonly scheduleIntervalMs: number;
    readonly scheduleWindowStart: number;
    readonly sessionId: string;
  },
): Promise<TaskRecord | null> {
  const rows = await client.query<PgTaskRow>(
    `
      SELECT ${taskReturningColumns}
      FROM tasks
      WHERE session_id = $1
        AND kind = $2
        AND mailbox_provider = $3
        AND mailbox_account_id = $4
        AND schedule_algorithm_version = $5
        AND schedule_interval_ms = $6
        AND schedule_window_start IS NOT NULL
        AND schedule_window_start > $7
        AND cancelled_at IS NULL
      ORDER BY schedule_window_start DESC
      LIMIT 1
    `,
    [
      input.sessionId,
      input.kind,
      input.mailboxProvider,
      input.mailboxAccountId,
      input.scheduleAlgorithmVersion,
      input.scheduleIntervalMs,
      input.scheduleWindowStart,
    ],
  );
  return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
}

/**
 * Ensures the current deterministic scheduled run exists after atomically
 * superseding eligible older runs, in one transaction.
 *
 * This is the single service-owned scheduled-run creation entry point the RFC's
 * scheduled-run state machine requires. It (1) derives the deterministic task id
 * from the schedule identity and rejects a mismatched caller-supplied id, then
 * inside one transaction (2) supersedes older matching unclaimed nonterminal
 * runs and (3) inserts-or-replays the current-window run. Because the
 * supersession and the current-run insert commit together, the current run is
 * never claimable before the older runs are cancelled, closing the race a
 * separate create-then-supersede sequence would open.
 *
 * The critical section is serialized on the STABLE schedule identity (excluding
 * the window start), so an older-window ensure cannot interleave with a
 * newer-window ensure. Inside the lock it refuses to create a run when an
 * equal-or-newer window run already exists: the equal window replays, and a
 * strictly-newer window returns `superseded_by_newer` without inserting, so only
 * the newest window is ever ensured and two windows can never both be active.
 */
export async function ensureScheduledRunWithEvents(
  database: DatabasePool,
  input: EnsureScheduledRunInput,
): Promise<EnsureScheduledRunResult> {
  const identity: ScheduledMaintenanceIdentity = {
    kind: input.kind,
    mailboxScope: { accountId: input.mailboxAccountId, provider: input.mailboxProvider },
    scheduleWindow: {
      algorithmVersion: input.scheduleAlgorithmVersion,
      endMs: input.scheduleWindowStart + input.scheduleIntervalMs,
      intervalMs: input.scheduleIntervalMs,
      startMs: input.scheduleWindowStart,
    },
    sessionId: input.sessionId,
  };
  const taskId = deriveScheduledTaskId(identity);
  if (input.expectedTaskId !== undefined && input.expectedTaskId !== taskId) {
    throw new ScheduledTaskIdentityMismatchError({
      derivedTaskId: taskId,
      suppliedTaskId: input.expectedTaskId,
    });
  }
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    // Serialize concurrent ensures for the same schedule identity across ALL
    // windows so an older-window and a newer-window ensure cannot interleave and
    // both insert. The lock key deliberately excludes the window start.
    await acquireTransactionAdvisoryLock(client, input.sessionId, scheduleIdentityLockKey(input));
    const supersededRows = await client.query<PgTaskRow>(
      `
        UPDATE tasks
        SET
          cancelled_at = now(),
          claim_expires_at = NULL
        WHERE session_id = $1
          AND kind = $2
          AND mailbox_provider = $3
          AND mailbox_account_id = $4
          AND schedule_algorithm_version = $5
          AND schedule_interval_ms = $6
          AND schedule_window_start IS NOT NULL
          AND schedule_window_start < $7
          AND claimed_by IS NULL
          AND completed_at IS NULL
          AND failed_at IS NULL
          AND cancelled_at IS NULL
        RETURNING ${taskReturningColumns}
      `,
      [
        input.sessionId,
        input.kind,
        input.mailboxProvider,
        input.mailboxAccountId,
        input.scheduleAlgorithmVersion,
        input.scheduleIntervalMs,
        input.scheduleWindowStart,
      ],
    );
    const supersededTasks = supersededRows.rows
      .map((row) => toTaskRecord(row))
      .sort((left, right) => left.taskId.localeCompare(right.taskId));
    const reason = input.reason ?? { reason: "superseded_by_newer_window" };
    const supersededEvents: SessionEvent[] = [];
    for (const task of supersededTasks) {
      supersededEvents.push(
        await appendEventWithClient(
          client,
          buildTaskCancelledEventInput({
            participantId: input.participantId,
            reason,
            sessionId: input.sessionId,
            task,
          }),
          input.eventSourceId,
        ),
      );
    }
    const existing = await readTaskWithClient(client, input.sessionId, taskId);
    let current: EnsureScheduledRunCurrent;
    if (existing) {
      current = { status: "replayed", task: existing };
    } else {
      // Only the newest window is ensured. If a strictly-newer window run already
      // exists for this schedule identity, this older-window ensure must not
      // insert a second active run; it resolves to that newer run instead. This is
      // the in-lock companion to the window-agnostic advisory lock: whichever
      // ensure runs second observes the other's committed run.
      const newer = await readNewerScheduledRunWithClient(client, input);
      if (newer) {
        current = { status: "superseded_by_newer", task: newer };
      } else {
        const task = await insertTaskWithClient(client, {
          input: input.input ?? null,
          kind: input.kind,
          objective: input.objective,
          schedule: {
            mailboxAccountId: input.mailboxAccountId,
            mailboxProvider: input.mailboxProvider,
            scheduleAlgorithmVersion: input.scheduleAlgorithmVersion,
            scheduleIntervalMs: input.scheduleIntervalMs,
            scheduleWindowStart: input.scheduleWindowStart,
          },
          sessionId: input.sessionId,
          taskId,
        });
        const event = await appendEventWithClient(
          client,
          buildTaskCreatedEventInput({ sessionId: input.sessionId, task }),
          input.eventSourceId,
        );
        current = { event, status: "created", task };
      }
    }
    await client.query("COMMIT");
    return { current, supersededEvents, supersededTasks, taskId: current.task.taskId };
  } catch (error) {
    await client.query("ROLLBACK");
    if (error instanceof ScheduledTaskIdentityMismatchError) {
      throw error;
    }
    throw new TaskEventTransactionRollbackError("ensureScheduledRun", error);
  } finally {
    client.release();
  }
}

/**
 * Completes an actively claimed task and appends `task.completed` atomically.
 */
export async function completeTaskWithEvent(
  database: DatabasePool,
  input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly result: Record<string, unknown>;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskEventResult> {
  return runTaskEventTransaction(database, {
    controlGuard: input.controlGuard,
    operation: "completeTask",
    sourceId: input.eventSourceId,
    mutate: async (client) => {
      const rows = await client.query<PgTaskRow>(
        `
          UPDATE tasks
          SET
            claim_expires_at = NULL,
            completed_at = now(),
            result = $1::jsonb
          WHERE session_id = $2
            AND task_id = $3
            AND claimed_by = $4
            AND claim_expires_at > now()
            AND completed_at IS NULL
            AND failed_at IS NULL
            AND cancelled_at IS NULL
          RETURNING ${taskReturningColumns}
        `,
        [JSON.stringify(input.result), input.sessionId, input.taskId, input.participantId],
      );
      return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
    },
    buildEvent: (task) =>
      buildTaskCompletedEventInput({
        participantId: input.participantId,
        sessionId: input.sessionId,
        task,
      }),
  });
}

/**
 * Fails an actively claimed task and appends `task.failed` atomically.
 */
export async function failTaskWithEvent(
  database: DatabasePool,
  input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly failure: Record<string, unknown>;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskEventResult> {
  return runTaskEventTransaction(database, {
    controlGuard: input.controlGuard,
    operation: "failTask",
    sourceId: input.eventSourceId,
    mutate: async (client) => {
      const rows = await client.query<PgTaskRow>(
        `
          UPDATE tasks
          SET
            claim_expires_at = NULL,
            failed_at = now(),
            failure = $1::jsonb
          WHERE session_id = $2
            AND task_id = $3
            AND claimed_by = $4
            AND claim_expires_at > now()
            AND completed_at IS NULL
            AND failed_at IS NULL
            AND cancelled_at IS NULL
          RETURNING ${taskReturningColumns}
        `,
        [JSON.stringify(input.failure), input.sessionId, input.taskId, input.participantId],
      );
      return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
    },
    buildEvent: (task) =>
      buildTaskFailedEventInput({
        participantId: input.participantId,
        sessionId: input.sessionId,
        task,
      }),
  });
}

/**
 * Releases an active task claim and appends `task.released` atomically.
 */
export async function releaseTaskWithEvent(
  database: DatabasePool,
  input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<PersistedTaskEventResult> {
  return runTaskEventTransaction(database, {
    controlGuard: input.controlGuard,
    operation: "releaseTask",
    sourceId: input.eventSourceId,
    mutate: async (client) => {
      const rows = await client.query<PgTaskRow>(
        `
          UPDATE tasks
          SET
            claimed_at = NULL,
            claimed_by = NULL,
            claim_expires_at = NULL,
            claim_expired_at = NULL,
            claim_expired_by = NULL,
            released_at = now(),
            released_by = $3
          WHERE session_id = $1
            AND task_id = $2
            AND claimed_by = $3
            AND claim_expires_at > now()
            AND completed_at IS NULL
            AND failed_at IS NULL
            AND cancelled_at IS NULL
          RETURNING ${taskReturningColumns}
        `,
        [input.sessionId, input.taskId, input.participantId],
      );
      return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
    },
    buildEvent: (task) =>
      buildTaskReleasedEventInput({
        participantId: input.participantId,
        sessionId: input.sessionId,
        task,
      }),
  });
}

interface TaskEventTransactionInput {
  readonly buildEvent: (task: TaskRecord) => AppendSessionEventInput;
  /**
   * Optional atomic Control Epoch fence. When present it is validated against
   * the current durable lease inside this transaction, before the mutation, so a
   * fenced caller can never apply the protected write.
   */
  readonly controlGuard?: ControlEpochGuard | undefined;
  readonly mutate: (client: TransactionClient) => Promise<TaskRecord | null>;
  readonly operation: string;
  readonly sourceId: string;
}

/** Error raised after a composed task/event transaction rolls back. */
class TaskEventTransactionRollbackError extends Error {
  constructor(
    readonly operation: string,
    readonly originalError: unknown,
  ) {
    super(
      `Task event transaction rollback during ${operation}: ${formatErrorMessage(originalError)}`,
    );
    this.name = "TaskEventTransactionRollbackError";
  }
}

/**
 * Owns the ordering invariant for event-producing task lifecycle writes:
 * mutate the task row first, allocate the event sequence only after that
 * mutation succeeds, append the event, queue NOTIFY, and commit all of it on
 * the same pg client.
 */
async function runTaskEventTransaction(
  database: DatabasePool,
  input: TaskEventTransactionInput,
): Promise<PersistedTaskEventResult> {
  const client = await database.pool.connect();
  try {
    await client.query("BEGIN");
    if (input.controlGuard) {
      await assertControlEpochCurrentWithClient(client, input.controlGuard);
    }
    const task = await input.mutate(client);
    if (!task) {
      await client.query("COMMIT");
      return null;
    }
    const event = await appendEventWithClient(client, input.buildEvent(task), input.sourceId);
    await client.query("COMMIT");
    return { event, task };
  } catch (error) {
    await client.query("ROLLBACK");
    // A fenced epoch is a caller-facing control outcome, not a transaction
    // failure; surface it untouched so the service maps it to CONTROL_EPOCH_STALE.
    if (error instanceof ControlEpochStaleError) {
      throw error;
    }
    throw new TaskEventTransactionRollbackError(input.operation, error);
  } finally {
    client.release();
  }
}

/** Reads one task row on the current transaction client. */
async function readTaskWithClient(
  client: TransactionClient,
  sessionId: string,
  taskId: string,
): Promise<TaskRecord | null> {
  const rows = await client.query<PgTaskRow>(
    `
      SELECT ${taskReturningColumns}
      FROM tasks
      WHERE session_id = $1 AND task_id = $2
      LIMIT 1
    `,
    [sessionId, taskId],
  );
  return rows.rows[0] ? toTaskRecord(rows.rows[0]) : null;
}

/** Inserts one task row on the current transaction client. */
async function insertTaskWithClient(
  client: TransactionClient,
  input: {
    readonly input?: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly schedule?: ScheduledTaskIdentityInput | undefined;
    readonly sessionId: string;
    readonly taskId: string;
  },
): Promise<TaskRecord> {
  const schedule = input.schedule;
  const rows = await client.query<PgTaskRow>(
    `
      INSERT INTO tasks (
        input,
        kind,
        mailbox_account_id,
        mailbox_provider,
        objective,
        schedule_algorithm_version,
        schedule_interval_ms,
        schedule_window_start,
        session_id,
        task_id
      )
      VALUES ($1::jsonb, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING ${taskReturningColumns}
    `,
    [
      input.input === undefined || input.input === null ? null : JSON.stringify(input.input),
      input.kind,
      schedule?.mailboxAccountId ?? null,
      schedule?.mailboxProvider ?? null,
      input.objective,
      schedule?.scheduleAlgorithmVersion ?? null,
      schedule?.scheduleIntervalMs ?? null,
      schedule?.scheduleWindowStart ?? null,
      input.sessionId,
      input.taskId,
    ],
  );
  return toTaskRecord(rows.rows[0]);
}

/** Reads one session event by its global event id on the current client. */
async function readSessionEventByEventIdWithClient(
  client: TransactionClient,
  eventId: string,
): Promise<SessionEvent | null> {
  const rows = await client.query<PgSessionEventRow>(
    `
      SELECT
        created_at AS "createdAt",
        event_id AS "eventId",
        payload,
        producer_id AS "producerId",
        seq AS "seq",
        session_id AS "sessionId",
        type
      FROM session_events
      WHERE event_id = $1
      LIMIT 1
    `,
    [eventId],
  );
  return rows.rows[0] ? toSessionEvent(rows.rows[0]) : null;
}

/** Compares caller-owned event fields without exposing payload content. */
function compareEventCreateInput(
  existing: SessionEvent,
  input: AppendSessionEventInput,
): readonly string[] {
  const conflicts: string[] = [];
  if (existing.eventId !== input.eventId) {
    conflicts.push("eventId");
  }
  if (existing.sessionId !== input.sessionId) {
    conflicts.push("sessionId");
  }
  if (existing.producerId !== input.producerId) {
    conflicts.push("producerId");
  }
  if (existing.type !== input.type) {
    conflicts.push("type");
  }
  if (!jsonLikeEqual(existing.payload, input.payload)) {
    conflicts.push("payload");
  }
  return conflicts;
}

/** Compares caller-owned task create fields while ignoring lifecycle state. */
function compareTaskCreateInput(
  existing: TaskRecord,
  input: {
    readonly input?: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly schedule?: ScheduledTaskIdentityInput | undefined;
    readonly sessionId: string;
    readonly taskId: string;
  },
): readonly string[] {
  const conflicts: string[] = [];
  if (existing.sessionId !== input.sessionId) {
    conflicts.push("sessionId");
  }
  if (existing.taskId !== input.taskId) {
    conflicts.push("taskId");
  }
  if (existing.kind !== input.kind) {
    conflicts.push("kind");
  }
  if (existing.objective !== input.objective) {
    conflicts.push("objective");
  }
  if (!jsonLikeEqual(existing.input, input.input ?? null)) {
    conflicts.push("input");
  }
  conflicts.push(...compareScheduleIdentity(existing.schedule ?? null, input.schedule));
  return conflicts;
}

/** Compares the immutable schedule and Mailbox Scope identity of a replayed create. */
function compareScheduleIdentity(
  existing: CandidateScheduleIdentity | null,
  input: ScheduledTaskIdentityInput | undefined,
): readonly string[] {
  const conflicts: string[] = [];
  if ((existing === null) !== (input === undefined)) {
    conflicts.push("schedule");
    return conflicts;
  }
  if (existing === null || input === undefined) {
    return conflicts;
  }
  if (existing.mailboxScope.provider !== input.mailboxProvider) {
    conflicts.push("mailboxProvider");
  }
  if (existing.mailboxScope.accountId !== input.mailboxAccountId) {
    conflicts.push("mailboxAccountId");
  }
  if (existing.scheduleWindow.algorithmVersion !== input.scheduleAlgorithmVersion) {
    conflicts.push("scheduleAlgorithmVersion");
  }
  if (existing.scheduleWindow.intervalMs !== input.scheduleIntervalMs) {
    conflicts.push("scheduleIntervalMs");
  }
  if (existing.scheduleWindow.startMs !== input.scheduleWindowStart) {
    conflicts.push("scheduleWindowStart");
  }
  return conflicts;
}

/** Normalizes JSON-like values so omitted task input and null compare equal. */
function jsonLikeEqual(left: unknown, right: unknown): boolean {
  const options = { omitUndefinedProperties: true, undefinedAsNull: true } as const;
  return canonicalJsonString(left, options) === canonicalJsonString(right, options);
}

/** Formats an unknown error for rollback diagnostics. */
function formatErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Detects a named Postgres unique-constraint violation without depending on pg internals. */
function isPgUniqueViolation(error: unknown, constraint: string): boolean {
  if (!isPlainRecord(error) || error.code !== "23505") {
    return false;
  }
  const message = typeof error.message === "string" ? error.message : "";
  return error.constraint === constraint || message.includes(constraint);
}

/**
 * Appends an event using an existing pg client so callers can compose task
 * state changes and event writes in one transaction.
 */
async function appendEventWithClient(
  client: TransactionClient,
  input: AppendSessionEventInput,
  sourceId: string,
  options: { readonly ensureSession?: boolean } = {},
): Promise<SessionEvent> {
  if (options.ensureSession !== false) {
    await requireSessionWithClient(client, input.sessionId, "appendEvent");
  }
  const seqRows = await client.query<SequenceRow>(
    `
      UPDATE session_event_sequences
      SET next_seq = next_seq + 1
      WHERE session_id = $1
      RETURNING next_seq - 1 AS "seq"
    `,
    [input.sessionId],
  );
  const rawSeq = seqRows.rows[0]?.seq;
  if (rawSeq === undefined) {
    throw new Error(`Failed to allocate sequence for ${input.sessionId}`);
  }
  const seq = parseEventSequence(rawSeq, input.sessionId);
  const eventRows = await client.query<PgSessionEventRow>(
    `
      INSERT INTO session_events (
        event_id,
        payload,
        producer_id,
        seq,
        session_id,
        type
      )
      VALUES ($1, $2::jsonb, $3, $4, $5, $6)
      RETURNING
        created_at AS "createdAt",
        event_id AS "eventId",
        payload,
        producer_id AS "producerId",
        seq AS "seq",
        session_id AS "sessionId",
        type
    `,
    [
      input.eventId,
      JSON.stringify(input.payload),
      input.producerId,
      seq,
      input.sessionId,
      input.type,
    ],
  );
  const event = toSessionEvent(eventRows.rows[0]);
  await notifySessionEventWithClient(client, event, sourceId);
  return event;
}

/**
 * Queues a Postgres notification for a committed session event. Postgres only
 * delivers NOTIFY messages after the surrounding transaction commits.
 */
async function notifySessionEventWithClient(
  client: TransactionClient,
  event: SessionEvent,
  sourceId: string,
): Promise<void> {
  await client.query(`SELECT pg_notify($1, $2)`, [
    sessionEventNotificationChannel,
    serializeSessionEventNotification(event, sourceId),
  ]);
}

/**
 * Serializes a committed event notification without embedding the event
 * payload, keeping NOTIFY messages small and forcing listeners back to the
 * durable event log.
 */
function serializeSessionEventNotification(event: SessionEvent, sourceId: string): string {
  return JSON.stringify({
    eventId: event.eventId,
    seq: event.seq,
    sessionId: event.sessionId,
    sourceId,
  } satisfies SessionEventNotification);
}

/**
 * Parses a Postgres session-event notification payload.
 */
export function parseSessionEventNotification(
  value: string | undefined,
): SessionEventNotification | null {
  if (!value) {
    return null;
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    if (typeof parsed !== "object" || parsed === null) {
      return null;
    }
    const eventId = readStringField(parsed, "eventId");
    const seq = readNumberField(parsed, "seq");
    const sessionId = readStringField(parsed, "sessionId");
    const sourceId = readStringField(parsed, "sourceId");
    if (!eventId || !sessionId || !sourceId || seq === null || !Number.isInteger(seq) || seq <= 0) {
      return null;
    }
    return { eventId, seq, sessionId, sourceId };
  } catch {
    return null;
  }
}

/**
 * Reads a string field from an unknown object payload.
 */
function readStringField(value: object, field: string): string | null {
  if (!(field in value)) {
    return null;
  }
  const fieldValue = value[field as keyof typeof value];
  return typeof fieldValue === "string" ? fieldValue : null;
}

/**
 * Reads a number field from an unknown object payload.
 */
function readNumberField(value: object, field: string): number | null {
  if (!(field in value)) {
    return null;
  }
  const fieldValue = value[field as keyof typeof value];
  return typeof fieldValue === "number" ? fieldValue : null;
}

/** Requires an existing session before a session-scoped write can continue. */
async function requireSession(
  database: DatabasePool,
  sessionId: string,
  operation: string,
): Promise<void> {
  const rows = await database.pool.query<SessionExistenceRow>(
    `SELECT EXISTS (SELECT 1 FROM sessions WHERE session_id = $1) AS "exists"`,
    [sessionId],
  );
  if (rows.rows[0]?.exists !== true) {
    throw new SessionNotFoundError({ operation, sessionId });
  }
}

/** Requires an existing session inside a caller-owned transaction. */
async function requireSessionWithClient(
  client: TransactionClient,
  sessionId: string,
  operation: string,
): Promise<void> {
  const rows = await client.query<SessionExistenceRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM sessions
        WHERE session_id = $1
      ) AS "exists"
    `,
    [sessionId],
  );
  if (rows.rows[0]?.exists !== true) {
    throw new SessionNotFoundError({ operation, sessionId });
  }
}

/**
 * Creates session and sequence rows for sanctioned creator paths only.
 */
async function createSessionWithClient(
  client: TransactionClient,
  sessionId: string,
): Promise<CreateSessionResult> {
  const insertedRows = await client.query<Pick<PgSessionRow, "createdAt" | "sessionId">>(
    `
      INSERT INTO sessions (session_id)
      VALUES ($1)
      ON CONFLICT DO NOTHING
      RETURNING created_at AS "createdAt", session_id AS "sessionId"
    `,
    [sessionId],
  );
  await client.query(
    `
      INSERT INTO session_event_sequences (session_id)
      VALUES ($1)
      ON CONFLICT DO NOTHING
    `,
    [sessionId],
  );
  const inserted = insertedRows.rows[0];
  if (inserted) {
    return { created: true, session: toSessionRecord(inserted) };
  }
  const rows = await client.query<Pick<PgSessionRow, "createdAt" | "sessionId">>(
    `
      SELECT created_at AS "createdAt", session_id AS "sessionId"
      FROM sessions
      WHERE session_id = $1
    `,
    [sessionId],
  );
  return { created: false, session: toSessionRecord(rows.rows[0]) };
}

type DbParticipantRow = typeof participants.$inferSelect;
type DbClientSessionBindingRow = typeof clientSessionBindings.$inferSelect;
type DbControlLeaseRow = typeof participantControlLeases.$inferSelect;
type DbSessionEventRow = typeof sessionEvents.$inferSelect;
type DbSessionRow = typeof sessions.$inferSelect;
type DbTaskApprovalRow = typeof taskApprovals.$inferSelect;
type DbTaskRow = typeof tasks.$inferSelect;

/**
 * Converts a database session row to the public record shape.
 */
function toSessionRecord(row: DbSessionRow | undefined): SessionRecord {
  if (!row) {
    throw new Error("Missing session row");
  }
  return {
    createdAt: row.createdAt.toISOString(),
    sessionId: row.sessionId,
  };
}

/**
 * Converts a database client/session binding row to the public record shape.
 */
function toClientSessionBindingRecord(
  row: DbClientSessionBindingRow | undefined,
): ClientSessionBindingRecord {
  if (!row) {
    throw new Error("Missing client session binding row");
  }
  return {
    archivedAt: row.archivedAt?.toISOString() ?? null,
    createdAt: row.createdAt.toISOString(),
    externalId: row.externalId,
    lastSeenAt: row.lastSeenAt.toISOString(),
    provider: row.provider,
    sessionId: row.sessionId,
  };
}

/**
 * Converts a database control-lease row to the public lease shape.
 */
function toControlLease(row: DbControlLeaseRow | PgControlLeaseRow | undefined): ControlLease {
  if (!row) {
    throw new Error("Missing participant control lease row");
  }
  return {
    claimedAt: row.claimedAt.toISOString(),
    controlChannel: row.controlChannel as ControlChannel,
    epoch: parseControlLeaseEpoch(row.epoch, row.sessionId, row.participantId),
    instanceId: row.instanceId,
    lastSeenAt: row.lastSeenAt.toISOString(),
    leaseExpiresAt: row.leaseExpiresAt.toISOString(),
    participantId: row.participantId,
    releasedAt: row.releasedAt?.toISOString() ?? null,
    sessionId: row.sessionId,
  };
}

/** Parses a durable control-lease epoch, rejecting non-monotonic bad values. */
function parseControlLeaseEpoch(value: unknown, sessionId: string, participantId: string): number {
  const parsed = parseControlEpoch(value);
  if (parsed === null) {
    throw new Error(
      `Invalid control lease epoch ${String(value)} for session ${sessionId}, participant ${participantId}`,
    );
  }
  return parsed;
}

/**
 * Converts a control-lease query row to the operator diagnostic snapshot shape.
 */
function toControlLeaseSnapshot(row: ControlLeaseSnapshotRow | undefined): ControlLeaseSnapshot {
  if (!row) {
    throw new Error("Missing participant control lease snapshot row");
  }
  return {
    claimedAt: row.claimedAt.toISOString(),
    controlChannel: row.controlChannel as ControlChannel,
    epoch: parseControlLeaseEpoch(row.epoch, row.sessionId, row.participantId),
    instanceId: row.instanceId,
    lastSeenAt: row.lastSeenAt.toISOString(),
    leaseExpiresAt: row.leaseExpiresAt.toISOString(),
    participantId: row.participantId,
    releasedAt: row.releasedAt?.toISOString() ?? null,
    sessionId: row.sessionId,
    supersededAt: row.supersededAt?.toISOString() ?? null,
    status: toControlLeaseStatus(row.status),
  };
}

/**
 * Validates a database-derived control lease status literal.
 */
function toControlLeaseStatus(value: string): ControlLeaseStatus {
  if (value === "active" || value === "expired" || value === "released" || value === "superseded") {
    return value;
  }
  throw new Error(`Unexpected control lease status: ${value}`);
}

/**
 * Converts a database event row to the public event envelope shape.
 */
function toSessionEvent(row: DbSessionEventRow | PgSessionEventRow | undefined): SessionEvent {
  if (!row) {
    throw new Error("Missing session event row");
  }
  return {
    createdAt: row.createdAt.toISOString(),
    eventId: row.eventId,
    payload: row.payload,
    producerId: row.producerId,
    seq: parseEventSequence(row.seq, row.sessionId),
    sessionId: row.sessionId,
    type: row.type,
  };
}

/**
 * Converts a database participant row to the public participant shape.
 */
function toParticipantRecord(
  row: DbParticipantRow | PgParticipantRow | undefined,
): ParticipantRecord {
  if (!row) {
    throw new Error("Missing participant row");
  }
  return {
    capabilities: row.capabilities,
    displayName: row.displayName,
    joinedAt: row.joinedAt.toISOString(),
    lastSeenAt: row.lastSeenAt.toISOString(),
    participantId: row.participantId,
    runtimeKind: row.runtimeKind,
    sessionId: row.sessionId,
  };
}

/**
 * Converts a database task row to the public task shape.
 */
function toTaskRecord(row: DbTaskRow | ExpiredTaskClaimRow | PgTaskRow | undefined): TaskRecord {
  if (!row) {
    throw new Error("Missing task row");
  }
  return {
    cancelledAt: row.cancelledAt?.toISOString() ?? null,
    claimExpiredAt: row.claimExpiredAt?.toISOString() ?? null,
    claimExpiredBy: row.claimExpiredBy,
    claimExpiresAt: row.claimExpiresAt?.toISOString() ?? null,
    claimedAt: row.claimedAt?.toISOString() ?? null,
    claimedBy: row.claimedBy,
    completedAt: row.completedAt?.toISOString() ?? null,
    createdAt: row.createdAt.toISOString(),
    failedAt: row.failedAt?.toISOString() ?? null,
    failure: row.failure ?? null,
    input: row.input ?? null,
    kind: row.kind,
    objective: row.objective,
    releasedAt: row.releasedAt?.toISOString() ?? null,
    releasedBy: row.releasedBy,
    result: row.result ?? null,
    schedule: toTaskScheduleIdentity(row),
    sessionId: row.sessionId,
    taskId: row.taskId,
  };
}

/**
 * Builds the schedule and Mailbox Scope identity from durable task columns.
 * Returns null unless every schedule-identity column is present, so a manual
 * task or a partially populated legacy row is treated as unscheduled.
 */
function toTaskScheduleIdentity(row: ScheduleIdentityColumns): CandidateScheduleIdentity | null {
  const provider = row.mailboxProvider ?? null;
  const accountId = row.mailboxAccountId ?? null;
  const startMs = coerceNullableInteger(row.scheduleWindowStart);
  const intervalMs = coerceNullableInteger(row.scheduleIntervalMs);
  const algorithmVersion = coerceNullableInteger(row.scheduleAlgorithmVersion);
  if (
    provider === null ||
    accountId === null ||
    startMs === null ||
    intervalMs === null ||
    algorithmVersion === null
  ) {
    return null;
  }
  return {
    mailboxScope: { accountId, provider },
    scheduleWindow: { algorithmVersion, endMs: startMs + intervalMs, intervalMs, startMs },
  };
}

/** Normalizes a nullable bigint/integer column that pg may return as a string. */
function coerceNullableInteger(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

/**
 * Converts a database approval row to the diagnostic approval record shape.
 */
function toTaskApprovalRecord(
  row: DbTaskApprovalRow | PgTaskApprovalRow | undefined,
): TaskApprovalRecord {
  if (!row) {
    throw new Error("Missing task approval row");
  }
  return {
    approvalEventId: row.approvalEventId,
    decidedAt: row.decidedAt.toISOString(),
    decidedByParticipantId: row.decidedByParticipantId,
    decision: toApprovalDecision(row.decision),
    reason: row.reason,
    sessionId: row.sessionId,
    targetKey: row.targetKey,
    taskId: row.taskId,
  };
}

/**
 * Converts a database task row to the operator diagnostic snapshot shape.
 */
function toTaskSnapshot(
  row: DbTaskRow | undefined,
  observedAt: Date,
  approvals: readonly TaskApprovalRecord[],
): TaskSnapshot {
  const task = toTaskRecord(row);
  return { ...task, approvals, status: deriveTaskSnapshotStatus(task, observedAt) };
}

function groupTaskApprovalsByTask(
  approvals: readonly TaskApprovalRecord[],
): Map<string, TaskApprovalRecord[]> {
  const approvalsByTask = new Map<string, TaskApprovalRecord[]>();
  for (const approval of approvals) {
    const taskApprovals = approvalsByTask.get(approval.taskId) ?? [];
    taskApprovals.push(approval);
    approvalsByTask.set(approval.taskId, taskApprovals);
  }
  return approvalsByTask;
}

async function readTaskApprovalWithClient(
  client: TransactionClient,
  input: {
    readonly sessionId: string;
    readonly targetKey: string;
    readonly taskId: string;
  },
): Promise<TaskApprovalRecord | null> {
  const rows = await client.query<PgTaskApprovalRow>(
    `
      SELECT ${taskApprovalReturningColumns}
      FROM task_approvals
      WHERE session_id = $1
        AND task_id = $2
        AND target_key = $3
    `,
    [input.sessionId, input.taskId, input.targetKey],
  );
  return rows.rows[0] ? toTaskApprovalRecord(rows.rows[0]) : null;
}

function toApprovalDecision(value: string): ApprovalDecision {
  if (value === "approved" || value === "rejected") {
    return value;
  }
  throw new Error(`Unexpected approval decision: ${value}`);
}

/**
 * Derives the operator-facing state for a task at one observation time.
 */
export function deriveTaskSnapshotStatus(
  task: Pick<
    TaskRecord,
    | "cancelledAt"
    | "claimExpiredAt"
    | "claimExpiresAt"
    | "claimedAt"
    | "claimedBy"
    | "completedAt"
    | "failedAt"
    | "releasedAt"
  >,
  observedAt: Date = new Date(),
): TaskSnapshotStatus {
  if (task.cancelledAt !== null) {
    return "cancelled";
  }
  if (task.completedAt !== null) {
    return "completed";
  }
  if (task.failedAt !== null) {
    return "failed";
  }
  if (task.claimedAt !== null && task.claimedBy !== null && task.claimExpiresAt !== null) {
    return Date.parse(task.claimExpiresAt) <= observedAt.getTime()
      ? "claim_expired"
      : "claim_active";
  }
  if (task.claimExpiredAt !== null) {
    return "claim_expired";
  }
  if (task.releasedAt !== null) {
    return "claim_cleared";
  }
  return "unclaimed";
}

/**
 * Builds participant runtime snapshots from visible presence and control lease
 * diagnostics.
 */
function buildParticipantRuntimeSnapshots(
  sessionId: string,
  participantRecords: readonly ParticipantRecord[],
  controlLeases: readonly ControlLeaseSnapshot[],
): ParticipantRuntimeSnapshot[] {
  const participantsById = new Map(
    participantRecords.map((participant) => [participant.participantId, participant]),
  );
  const leasesByParticipant = groupControlLeasesByParticipant(controlLeases);
  const participantIds = new Set([
    ...participantRecords.map((participant) => participant.participantId),
    ...leasesByParticipant.keys(),
  ]);
  return [...participantIds].sort().map((participantId) => {
    const participant = participantsById.get(participantId) ?? null;
    const participantLeases = leasesByParticipant.get(participantId) ?? [];
    const currentControlLease = selectCurrentControlLease(participantLeases);
    const latestControlLease = selectLatestControlLease(participantLeases);
    return {
      controlLeaseCount: participantLeases.length,
      currentControlLease,
      latestControlLease,
      participant,
      participantId,
      registered: participant !== null,
      sessionId,
      status: deriveParticipantRuntimeSnapshotStatus(
        participant,
        currentControlLease,
        latestControlLease,
      ),
    };
  });
}

/**
 * Groups control lease snapshots by participant identity.
 */
function groupControlLeasesByParticipant(
  controlLeases: readonly ControlLeaseSnapshot[],
): Map<string, ControlLeaseSnapshot[]> {
  const leasesByParticipant = new Map<string, ControlLeaseSnapshot[]>();
  for (const controlLease of controlLeases) {
    const participantLeases = leasesByParticipant.get(controlLease.participantId) ?? [];
    participantLeases.push(controlLease);
    leasesByParticipant.set(controlLease.participantId, participantLeases);
  }
  return leasesByParticipant;
}

/**
 * Selects the active control lease with the most recent liveness timestamp.
 */
function selectCurrentControlLease(
  controlLeases: readonly ControlLeaseSnapshot[],
): ControlLeaseSnapshot | null {
  return (
    [...controlLeases]
      .filter((controlLease) => controlLease.status === "active")
      .sort(compareControlLeaseRecency)[0] ?? null
  );
}

/**
 * Selects the most recently observed control lease, regardless of state.
 */
function selectLatestControlLease(
  controlLeases: readonly ControlLeaseSnapshot[],
): ControlLeaseSnapshot | null {
  return [...controlLeases].sort(compareControlLeaseRecency)[0] ?? null;
}

/**
 * Sorts control leases by most recent liveness and claim time.
 */
function compareControlLeaseRecency(
  left: ControlLeaseSnapshot,
  right: ControlLeaseSnapshot,
): number {
  const lastSeenDelta = Date.parse(right.lastSeenAt) - Date.parse(left.lastSeenAt);
  if (lastSeenDelta !== 0) {
    return lastSeenDelta;
  }
  const claimedDelta = Date.parse(right.claimedAt) - Date.parse(left.claimedAt);
  if (claimedDelta !== 0) {
    return claimedDelta;
  }
  return left.instanceId.localeCompare(right.instanceId);
}

/**
 * Derives the operator-facing state for a participant runtime snapshot.
 */
function deriveParticipantRuntimeSnapshotStatus(
  participant: ParticipantRecord | null,
  currentControlLease: ControlLeaseSnapshot | null,
  latestControlLease: ControlLeaseSnapshot | null,
): ParticipantRuntimeSnapshotStatus {
  if (!participant) {
    return "lease_without_presence";
  }
  if (currentControlLease) {
    return "registered_control_active";
  }
  return latestControlLease ? "registered_control_inactive" : "registered_without_control";
}

/**
 * Builds aggregate debug counts from the read-only session snapshots.
 */
function buildSessionDebugSummary(
  sessionId: string,
  participantRuntimes: readonly ParticipantRuntimeSnapshot[],
  controlLeases: readonly ControlLeaseSnapshot[],
  taskSnapshots: readonly TaskSnapshot[],
): SessionDebugSummary {
  const completed = countTasksByStatus(taskSnapshots, "completed");
  const failed = countTasksByStatus(taskSnapshots, "failed");
  const cancelled = countTasksByStatus(taskSnapshots, "cancelled");
  const unclaimed = countTasksByStatus(taskSnapshots, "unclaimed");
  const claimCleared = countTasksByStatus(taskSnapshots, "claim_cleared");
  const claimActive = countTasksByStatus(taskSnapshots, "claim_active");
  const claimExpired = countTasksByStatus(taskSnapshots, "claim_expired");
  return {
    controlLeases: {
      active: countControlLeasesByStatus(controlLeases, "active"),
      expired: countControlLeasesByStatus(controlLeases, "expired"),
      released: countControlLeasesByStatus(controlLeases, "released"),
      superseded: countControlLeasesByStatus(controlLeases, "superseded"),
      total: controlLeases.length,
    },
    participants: {
      activeControl: participantRuntimes.filter(
        (participantRuntime) =>
          participantRuntime.registered && participantRuntime.currentControlLease !== null,
      ).length,
      leaseOnly: participantRuntimes.filter((participantRuntime) => !participantRuntime.registered)
        .length,
      registered: participantRuntimes.filter((participantRuntime) => participantRuntime.registered)
        .length,
      total: participantRuntimes.length,
      withoutActiveControl: participantRuntimes.filter(
        (participantRuntime) =>
          participantRuntime.registered && participantRuntime.currentControlLease === null,
      ).length,
    },
    sessionId,
    tasks: {
      activeClaims: claimActive,
      cancelled,
      claimable: unclaimed + claimCleared + claimExpired,
      claimActive,
      claimCleared,
      claimExpired,
      completed,
      expiredClaims: claimExpired,
      failed,
      terminal: completed + failed + cancelled,
      total: taskSnapshots.length,
      unclaimed,
    },
  };
}

/**
 * Counts control leases with the requested derived status.
 */
function countControlLeasesByStatus(
  controlLeases: readonly ControlLeaseSnapshot[],
  status: ControlLeaseStatus,
): number {
  return controlLeases.filter((controlLease) => controlLease.status === status).length;
}

/**
 * Counts task snapshots with the requested derived status.
 */
function countTasksByStatus(
  taskSnapshots: readonly TaskSnapshot[],
  status: TaskSnapshot["status"],
): number {
  return taskSnapshots.filter((task) => task.status === status).length;
}

/**
 * Detects visible participant metadata changes that should produce an update
 * event instead of a silent refresh.
 */
function participantPresenceChanged(
  previousParticipant: ParticipantRecord,
  participant: ParticipantRecord,
): boolean {
  return (
    previousParticipant.displayName !== participant.displayName ||
    previousParticipant.runtimeKind !== participant.runtimeKind ||
    canonicalJsonString(previousParticipant.capabilities) !==
      canonicalJsonString(participant.capabilities)
  );
}

interface CanonicalJsonOptions {
  readonly omitUndefinedProperties?: boolean;
  readonly undefinedAsNull?: boolean;
}

/** Serializes JSON-like records with stable object-key ordering. */
function canonicalJsonString(value: unknown, options: CanonicalJsonOptions = {}): string {
  return JSON.stringify(canonicalJsonValue(value, options));
}

/** Recursively sorts object keys so equivalent JSON records compare equal. */
function canonicalJsonValue(value: unknown, options: CanonicalJsonOptions): unknown {
  if (value === undefined && options.undefinedAsNull === true) {
    return null;
  }
  if (Array.isArray(value)) {
    return value.map((item) => canonicalJsonValue(item, options));
  }
  if (!isPlainRecord(value)) {
    return value;
  }
  const entries = Object.entries(value)
    .filter(
      ([, nestedValue]) => !(options.omitUndefinedProperties === true && nestedValue === undefined),
    )
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, nestedValue]) => [key, canonicalJsonValue(nestedValue, options)]);
  return Object.fromEntries(entries);
}

/** Checks for records whose enumerable keys can be canonicalized. */
function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Builds a DB-clock deadline expression for a positive millisecond TTL. */
function leaseDeadlineSql(leaseTtlMs: number): SQL<Date> {
  assertPositiveFiniteTtlMs(leaseTtlMs, "lease TTL");
  return sql<Date>`now() + ${leaseTtlMs} * interval '1 millisecond'`;
}

/** Rejects invalid TTLs before they reach interval SQL. */
function assertPositiveFiniteTtlMs(leaseTtlMs: number, label: string): void {
  if (!Number.isFinite(leaseTtlMs) || leaseTtlMs <= 0) {
    throw new Error(`${label} must be a positive finite millisecond value`);
  }
}

/** Parses int8 sequences returned by pg without allowing precision loss. */
function parseEventSequence(value: unknown, sessionId: string): number {
  const attemptedSeq = String(value);
  const parsed =
    typeof value === "number" || typeof value === "bigint" || typeof value === "string"
      ? parsePositiveSafeInteger(value)
      : null;
  if (parsed === null) {
    throw new SessionEventSequenceRangeError({ attemptedSeq, sessionId });
  }
  return parsed;
}
