/**
 * Owns Tether's complete startup-migration critical section: serialization,
 * legacy catalog recognition, journal seeding, and generated Drizzle execution.
 * This deep Module keeps those coupled invariants behind one migration Interface;
 * it does not own normal application persistence.
 */

import { createHash } from "node:crypto";

import type { MigrationMeta } from "drizzle-orm/migrator";
import { readMigrationFiles } from "drizzle-orm/migrator";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate as runDrizzleMigrations } from "drizzle-orm/node-postgres/migrator";
import type pg from "pg";

const MIGRATION_ADVISORY_LOCK_KEY = "8387255305985817959";
const migrationsFolder = "drizzle";
const generatedMigrations = readMigrationFiles({ migrationsFolder });
const tetherTableNames = [
  "auth_grant_audit_events",
  "auth_grants",
  "auth_tickets",
  "browser_pairing_exchange_failures",
  "browser_pairing_requests",
  "browser_sessions",
  "client_session_bindings",
  "operator_grant_scopes",
  "participant_control_leases",
  "participants",
  "session_event_sequences",
  "session_events",
  "session_bootstrap_identities",
  "session_projections",
  "session_summaries",
  "session_tombstones",
  "sessions",
  "task_approvals",
  "tasks",
] as const;

const legacyScheduleIdentityIndex: IndexSignature = {
  columnNames: [
    "session_id",
    "kind",
    "mailbox_provider",
    "mailbox_account_id",
    "schedule_algorithm_version",
    "schedule_interval_ms",
    "schedule_window_start",
  ],
  predicate: null,
  tableName: "tasks",
  unique: true,
};

const providerNeutralScheduleIdentityIndex: IndexSignature = {
  columnNames: [
    "session_id",
    "kind",
    "schedule_identity_version",
    "schedule_scope_key",
    "schedule_algorithm_version",
    "schedule_interval_ms",
    "schedule_window_start",
  ],
  predicate: null,
  tableName: "tasks",
  unique: true,
};

/** Minimal database Interface accepted by startup migration orchestration. */
export interface MigrationDatabase {
  readonly pool: pg.Pool;
}

/** Bounded migration state shared by startup and steady-state readiness. */
export type DatabaseMigrationReadiness = "current" | "incomplete" | "unavailable";

/** Reads complete durable journal readiness in one database round trip. */
export async function readDatabaseMigrationReadiness(
  database: MigrationDatabase,
): Promise<DatabaseMigrationReadiness> {
  let client: pg.PoolClient;
  try {
    client = await database.pool.connect();
  } catch {
    return "unavailable";
  }
  try {
    const inspection = await inspectMigrationJournal(client, generatedMigrations);
    return inspection.status === "valid" && inspection.appliedCount === generatedMigrations.length
      ? "current"
      : "incomplete";
  } catch (error) {
    return isMissingMigrationJournalError(error) ? "incomplete" : "unavailable";
  } finally {
    client.release();
  }
}

/** Distinguishes an absent migration catalog from transport and query outages. */
function isMissingMigrationJournalError(error: unknown): boolean {
  if (typeof error !== "object" || error === null || !("code" in error)) return false;
  return error.code === "42P01" || error.code === "3F000";
}

/** Checks whether the complete durable migration journal matches generated state. */
export async function isDatabaseMigrationCurrent(database: MigrationDatabase): Promise<boolean> {
  return (await readDatabaseMigrationReadiness(database)) === "current";
}

interface LegacyMigrationProbe {
  readonly contradictionObserved?: (client: pg.PoolClient) => Promise<boolean>;
  readonly label: string;
  readonly represented: (client: pg.PoolClient) => Promise<boolean>;
}

interface MigrationJournalRow {
  readonly createdAt: string | null;
  readonly hash: string;
}

/** Safe journal-head metadata exposed by recognized migration failures. */
export interface MigrationJournalHead {
  readonly hashMatchesKnownMigration: boolean;
  readonly position: number;
  readonly timestamp: string | null;
}

/** Stable reason codes for recognized startup-migration failures. */
export type DatabaseMigrationFailureReason = "invalid_journal" | "unsupported_schema";

/** Allowlisted context shared by typed migration failures and startup logging. */
export interface DatabaseMigrationFailureContext {
  readonly expectedFacts: readonly string[];
  readonly journalHead: MigrationJournalHead | null;
  readonly observedFacts: readonly string[];
  readonly reason: DatabaseMigrationFailureReason;
  readonly recognizedPrefix: number | null;
}

type MigrationJournalInspection =
  | { readonly status: "empty" }
  | { readonly appliedCount: number; readonly status: "valid" }
  | {
      readonly context: DatabaseMigrationFailureContext;
      readonly status: "invalid";
    };

interface ConstraintSignature {
  readonly columnNames: readonly string[];
  readonly constraintType: string;
  readonly tableName: string;
}

/** Structural constraint identity returned by the PostgreSQL catalogs. */
interface ConstraintSignatureRow {
  readonly columnNames: readonly string[];
  readonly constraintType: string;
  readonly tableName: string;
}

interface IndexSignature {
  readonly columnNames: readonly string[];
  readonly predicate: string | null;
  readonly tableName: string;
  readonly unique: boolean;
}

interface NamedConstraintExpectation {
  readonly constraintName: string;
  readonly constraintType: "c" | "f" | "p";
  readonly requiredDefinitionFragments: readonly string[];
  readonly tableName: string;
}

interface NamedConstraintRow {
  readonly constraintName: string;
  readonly constraintType: string;
  readonly deleteAction: string | null;
  readonly definition: string;
  readonly tableName: string;
  readonly updateAction: string | null;
  readonly validated: boolean;
}

interface SchemaObjectExistsRow {
  readonly exists: boolean;
}

interface AuthColumnExpectation {
  readonly columnDefault: string | null;
  readonly columnName: string;
  readonly dataType: "jsonb" | "text" | "timestamp with time zone";
  readonly isNullable: "NO" | "YES";
  readonly tableName: "auth_grant_audit_events" | "auth_grants" | "auth_tickets";
}

interface AuthColumnRow {
  readonly columnDefault: string | null;
  readonly columnName: string;
  readonly dataType: string;
  readonly isNullable: string;
  readonly tableName: string;
}

interface ColumnDefinitionExpectation {
  readonly columnDefault: string | null;
  readonly columnName: string;
  readonly dataType: string;
  readonly isNullable: "NO" | "YES";
}

interface UnlockRow {
  readonly unlocked: boolean;
}

/** Error raised for a recognized startup-migration state that cannot be safely advanced. */
export class DatabaseMigrationError extends Error implements DatabaseMigrationFailureContext {
  readonly expectedFacts: readonly string[];
  readonly journalHead: MigrationJournalHead | null;
  readonly observedFacts: readonly string[];
  readonly reason: DatabaseMigrationFailureReason;
  readonly recognizedPrefix: number | null;

  constructor(context: DatabaseMigrationFailureContext) {
    super(`Database migration failed: ${context.reason}`);
    this.name = "DatabaseMigrationError";
    this.expectedFacts = context.expectedFacts;
    this.journalHead = context.journalHead;
    this.observedFacts = context.observedFacts;
    this.reason = context.reason;
    this.recognizedPrefix = context.recognizedPrefix;
  }
}

/** Projects a typed migration failure onto the complete safe startup-log allowlist. */
export function projectDatabaseMigrationFailure(
  error: DatabaseMigrationError,
): DatabaseMigrationFailureContext {
  return {
    expectedFacts: [...error.expectedFacts],
    journalHead:
      error.journalHead === null
        ? null
        : {
            hashMatchesKnownMigration: error.journalHead.hashMatchesKnownMigration,
            position: error.journalHead.position,
            timestamp: error.journalHead.timestamp,
          },
    observedFacts: [...error.observedFacts],
    reason: error.reason,
    recognizedPrefix: error.recognizedPrefix,
  };
}

/**
 * Applies all generated migrations while holding one database-scoped session
 * advisory lock on the same checked-out client used for every migration query.
 */
export async function migrateDatabase(database: MigrationDatabase): Promise<void> {
  const client = await database.pool.connect();
  let lockAcquired = false;
  let cleanupFailure: { readonly error: unknown } | null = null;
  let migrationFailure: { readonly error: unknown } | null = null;
  try {
    await client.query("SELECT pg_advisory_lock($1::bigint)", [MIGRATION_ADVISORY_LOCK_KEY]);
    lockAcquired = true;
    await baselineLegacySchema(client);
    await runDrizzleMigrations(drizzle(client), { migrationsFolder });
  } catch (error) {
    migrationFailure = { error };
  } finally {
    let destroyClient = false;
    if (lockAcquired) {
      try {
        const unlock = await client.query<UnlockRow>(
          "SELECT pg_advisory_unlock($1::bigint) AS unlocked",
          [MIGRATION_ADVISORY_LOCK_KEY],
        );
        if (unlock.rows[0]?.unlocked !== true) {
          destroyClient = true;
          cleanupFailure = {
            error: new Error("Database migration advisory lock was not held during cleanup"),
          };
        }
      } catch (error) {
        destroyClient = true;
        cleanupFailure = { error };
      }
    }
    try {
      client.release(destroyClient);
    } catch (error) {
      cleanupFailure ??= { error };
    }
  }
  if (migrationFailure !== null) {
    throw migrationFailure.error;
  }
  if (cleanupFailure !== null) {
    throw cleanupFailure.error;
  }
}

/**
 * Baselines old pre-migrator databases by probing schema shape before any
 * application-table mutation. Supported shapes are contiguous generated
 * migration prefixes; Drizzle then applies the remaining generated migrations.
 */
async function baselineLegacySchema(client: pg.PoolClient): Promise<void> {
  await client.query(`
    CREATE SCHEMA IF NOT EXISTS drizzle;
    CREATE TABLE IF NOT EXISTS drizzle.__drizzle_migrations (
      id SERIAL PRIMARY KEY,
      hash text NOT NULL,
      created_at bigint
    );
  `);
  const migrations = readMigrationFiles({ migrationsFolder });
  const journal = await inspectMigrationJournal(client, migrations);
  if (journal.status === "invalid") {
    throw new DatabaseMigrationError(journal.context);
  }
  if (journal.status === "valid") {
    return;
  }

  const probes = legacyMigrationProbes();
  const results: boolean[] = [];
  for (const probe of probes) {
    results.push(await probe.represented(client));
  }
  if (results.every((result) => !result)) {
    const knownTetherTableCount = await countKnownTetherTables(client);
    if (knownTetherTableCount === 0) {
      return;
    }
    throw new DatabaseMigrationError({
      expectedFacts: ["migration_0000_complete=true"],
      journalHead: null,
      observedFacts: [
        "migration_0000_complete=false",
        `known_tether_table_count=${knownTetherTableCount}`,
      ],
      reason: "unsupported_schema",
      recognizedPrefix: null,
    });
  }
  const firstGap = results.findIndex((result) => !result);
  const representedPrefix =
    firstGap === -1 ? results.length - 1 : results.slice(0, firstGap).length - 1;
  for (const [index, probe] of probes.entries()) {
    if (
      results[index] === false &&
      probe.contradictionObserved !== undefined &&
      (await probe.contradictionObserved(client))
    ) {
      throw new DatabaseMigrationError({
        expectedFacts: [`migration_${String(index).padStart(4, "0")}_signature=true`],
        journalHead: null,
        observedFacts: [
          `migration_${String(index).padStart(4, "0")}_named_object=true`,
          `migration_${String(index).padStart(4, "0")}_signature=false`,
        ],
        reason: "unsupported_schema",
        recognizedPrefix: representedPrefix < 0 ? null : representedPrefix,
      });
    }
  }
  const hasNonContiguousSuffix =
    firstGap !== -1 && results.slice(firstGap + 1).some((result) => result);
  if (hasNonContiguousSuffix) {
    throw new DatabaseMigrationError({
      expectedFacts: ["contiguous_migration_prefix=true"],
      journalHead: null,
      observedFacts: results.map(
        (represented, index) =>
          `migration_${String(index).padStart(4, "0")}_represented=${represented}`,
      ),
      reason: "unsupported_schema",
      recognizedPrefix: representedPrefix < 0 ? null : representedPrefix,
    });
  }

  const representedMigrations = migrations.slice(0, representedPrefix + 1);
  await client.query("BEGIN");
  try {
    for (const migration of representedMigrations) {
      await client.query(
        `INSERT INTO drizzle.__drizzle_migrations ("hash", "created_at") VALUES ($1, $2)`,
        [migration.hash, migration.folderMillis],
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

/** Counts only Tether-owned public tables from the fixed schema allowlist. */
async function countKnownTetherTables(client: pg.PoolClient): Promise<number> {
  let count = 0;
  for (const tableName of tetherTableNames) {
    if (await hasTable(client, tableName)) {
      count += 1;
    }
  }
  return count;
}

/** Reads and validates the complete journal as an exact ordered migration prefix. */
async function inspectMigrationJournal(
  client: pg.PoolClient,
  migrations: readonly MigrationMeta[],
): Promise<MigrationJournalInspection> {
  const result = await client.query<MigrationJournalRow>(
    `
      SELECT created_at::text AS "createdAt", hash
      FROM drizzle.__drizzle_migrations
      ORDER BY id
    `,
  );
  if (result.rows.length === 0) {
    return { status: "empty" };
  }

  let matchingRows = 0;
  for (const [position, row] of result.rows.entries()) {
    const expectedMigration = migrations[position];
    const timestampMatches =
      expectedMigration !== undefined && row.createdAt === String(expectedMigration.folderMillis);
    const hashMatches = expectedMigration !== undefined && row.hash === expectedMigration.hash;
    if (!timestampMatches || !hashMatches) {
      const headPosition = result.rows.length - 1;
      const head = result.rows[headPosition];
      const expectedHeadMigration = migrations[headPosition];
      return {
        context: {
          expectedFacts: [
            `journal_position=${position}`,
            `known_migration_exists=${expectedMigration !== undefined}`,
          ],
          journalHead:
            head === undefined
              ? null
              : {
                  hashMatchesKnownMigration:
                    expectedHeadMigration !== undefined && head.hash === expectedHeadMigration.hash,
                  position: headPosition,
                  timestamp: head.createdAt,
                },
          observedFacts: [
            `journal_row_count=${result.rows.length}`,
            `timestamp_matches=${timestampMatches}`,
            `hash_matches=${hashMatches}`,
          ],
          reason: "invalid_journal",
          recognizedPrefix: matchingRows === 0 ? null : matchingRows - 1,
        },
        status: "invalid",
      };
    }
    matchingRows += 1;
  }

  return { appliedCount: matchingRows, status: "valid" };
}

/** Detects any partial browser-operator foundation artifact. */
async function hasAnyBrowserOperatorAuthorityArtifact(client: pg.PoolClient): Promise<boolean> {
  return (
    (await hasTable(client, "browser_pairing_requests")) ||
    (await hasTable(client, "browser_sessions")) ||
    (await hasTable(client, "operator_grant_scopes"))
  );
}

/** Recognizes the complete initial browser-operator schema migration. */
async function hasBrowserOperatorAuthorityFoundation(client: pg.PoolClient): Promise<boolean> {
  return (
    (await hasColumns(client, "browser_pairing_requests", [
      "confirmed_at",
      "created_at",
      "exchange_secret_hash",
      "exchanged_at",
      "expires_at",
      "failed_attempts",
      "invalidated_at",
      "operator_subject",
      "origin",
      "public_nonce",
      "request_id",
      "requested_scope",
      "source_address_hash",
      "verification_phrase",
    ])) &&
    (await hasColumns(client, "browser_sessions", [
      "created_at",
      "csrf_token_hash",
      "grant_jti",
      "origin",
    ])) &&
    (await hasColumns(client, "operator_grant_scopes", ["created_at", "grant_jti", "scope"])) &&
    (await hasIndexSignature(client, "browser_pairing_requests_source_created_idx", {
      columnNames: ["source_address_hash", "created_at"],
      predicate: null,
      tableName: "browser_pairing_requests",
      unique: false,
    })) &&
    (await hasConstraintSignature(client, {
      columnNames: ["grant_jti"],
      constraintType: "f",
      tableName: "browser_sessions",
    })) &&
    (await hasConstraintSignature(client, {
      columnNames: ["grant_jti"],
      constraintType: "f",
      tableName: "operator_grant_scopes",
    }))
  );
}

/** Recognizes the complete confirmation and integrity hardening migration. */
async function hasConfirmedBrowserPairingAuthority(client: pg.PoolClient): Promise<boolean> {
  const checks = [
    "browser_pairing_requests_confirmation_check",
    "browser_pairing_requests_exchange_check",
    "browser_pairing_requests_identity_check",
    "browser_pairing_requests_invalidation_check",
    "operator_grant_scopes_shape_check",
  ] as const;
  if (!(await hasColumn(client, "browser_pairing_requests", "confirmed_by_subject"))) {
    return false;
  }
  for (const constraintName of checks) {
    const tableName = constraintName.startsWith("operator_")
      ? "operator_grant_scopes"
      : "browser_pairing_requests";
    if (!(await hasNamedConstraint(client, tableName, constraintName))) {
      return false;
    }
  }
  return true;
}

/** Recognizes durable browser classification in the grant metadata contract. */
async function hasBrowserGrantClassification(client: pg.PoolClient): Promise<boolean> {
  return hasNamedCheckConstraint(client, {
    constraintName: "auth_grants_metadata_shape_check",
    requiredDefinitionFragments: ["'admin'", "'bootstrap'", "'browser'", "'migration'"],
    tableName: "auth_grants",
  });
}

/** Recognizes durable redacted counters for source-scoped exchange limits. */
async function hasBrowserPairingExchangeFailureCounters(client: pg.PoolClient): Promise<boolean> {
  return (
    (await hasColumns(client, "browser_pairing_exchange_failures", [
      "created_at",
      "failure_id",
      "source_address_hash",
    ])) &&
    (await hasIndexSignature(client, "browser_pairing_exchange_failures_source_created_idx", {
      columnNames: ["source_address_hash", "created_at"],
      predicate: null,
      tableName: "browser_pairing_exchange_failures",
      unique: false,
    }))
  );
}

/** Recognizes indexed server-owned operator admission and bounded snapshot rows. */
async function hasOperatorCommandAdmissionHardening(client: pg.PoolClient): Promise<boolean> {
  return (
    (await hasColumns(client, "tasks", ["operator_command_key", "operator_grant_jti"])) &&
    (await hasIndexSignature(client, "tasks_session_created_idx", {
      columnNames: ["session_id", "created_at", "task_id"],
      predicate: null,
      tableName: "tasks",
      unique: false,
    })) &&
    (await hasIndexSignature(client, "participants_session_last_seen_idx", {
      columnNames: ["session_id", "last_seen_at", "participant_id"],
      predicate: null,
      tableName: "participants",
      unique: false,
    })) &&
    (await hasIndexSignature(client, "tasks_operator_grant_created_idx", {
      columnNames: ["operator_grant_jti", "created_at"],
      predicate: "operator_grant_jti IS NOT NULL",
      tableName: "tasks",
      unique: false,
    })) &&
    (await hasIndexSignature(client, "tasks_operator_pending_idx", {
      columnNames: ["created_at"],
      predicate:
        "operator_command_key IS NOT NULL AND cancelled_at IS NULL AND completed_at IS NULL AND failed_at IS NULL",
      tableName: "tasks",
      unique: false,
    })) &&
    (await hasIndexSignature(client, "tasks_operator_command_active_unique", {
      columnNames: ["session_id", "operator_command_key"],
      predicate:
        "operator_command_key IS NOT NULL AND cancelled_at IS NULL AND completed_at IS NULL AND failed_at IS NULL",
      tableName: "tasks",
      unique: true,
    })) &&
    (await hasNamedCheckConstraint(client, {
      constraintName: "tasks_operator_command_shape_check",
      requiredDefinitionFragments: ["operator_command_key", "operator_grant_jti", "operator.%"],
      tableName: "tasks",
    })) &&
    (await hasNamedCheckConstraint(client, {
      constraintName: "tasks_snapshot_size_check",
      requiredDefinitionFragments: ["octet_length", "2097152"],
      tableName: "tasks",
    })) &&
    (await hasNamedCheckConstraint(client, {
      constraintName: "participants_snapshot_size_check",
      requiredDefinitionFragments: ["octet_length", "2097152"],
      tableName: "participants",
    })) &&
    (await hasIndexSignature(client, "browser_pairing_exchange_failures_created_idx", {
      columnNames: ["created_at"],
      predicate: null,
      tableName: "browser_pairing_exchange_failures",
      unique: false,
    }))
  );
}

/** Detects any partially applied indexed operator-admission hardening artifact. */
async function hasAnyOperatorCommandAdmissionHardeningArtifact(
  client: pg.PoolClient,
): Promise<boolean> {
  return (
    (await hasColumn(client, "tasks", "operator_command_key")) ||
    (await hasColumn(client, "tasks", "operator_grant_jti")) ||
    (await hasIndex(client, "tasks_operator_command_active_unique")) ||
    (await hasNamedConstraint(client, "tasks", "tasks_operator_command_shape_check")) ||
    (await hasNamedConstraint(client, "participants", "participants_snapshot_size_check"))
  );
}

/** Ordered probes for generated migrations that can be represented by schema shape alone. */
function legacyMigrationProbes(): readonly LegacyMigrationProbe[] {
  return [
    {
      label: "0000 core session, participant, event, and task tables",
      represented: async (client) =>
        (await hasTables(client, [
          "participants",
          "session_event_sequences",
          "session_events",
          "sessions",
          "tasks",
        ])) &&
        (await hasColumns(client, "sessions", ["created_at", "session_id"])) &&
        (await hasColumns(client, "tasks", [
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
      represented: (client) =>
        hasColumns(client, "tasks", [
          "cancelled_at",
          "failed_at",
          "failure",
          "released_at",
          "result",
        ]),
    },
    {
      label: "0002 participant control leases",
      represented: (client) => hasTable(client, "participant_control_leases"),
    },
    {
      label: "0003 task claim expiry column",
      represented: (client) => hasColumn(client, "tasks", "claim_expires_at"),
    },
    {
      label: "0004 client session bindings",
      represented: (client) => hasTable(client, "client_session_bindings"),
    },
    {
      label: "0005 task input column",
      represented: (client) => hasColumn(client, "tasks", "input"),
    },
    {
      label: "0006 task claim expiry index",
      contradictionObserved: (client) =>
        hasNamedIndexContradiction(client, "tasks_claim_expiry_idx", {
          columnNames: ["claim_expires_at"],
          predicate: null,
          tableName: "tasks",
          unique: false,
        }),
      represented: (client) =>
        hasIndexSignature(client, "tasks_claim_expiry_idx", {
          columnNames: ["claim_expires_at"],
          predicate: null,
          tableName: "tasks",
          unique: false,
        }),
    },
    {
      label: "0007 task approval rows",
      contradictionObserved: (client) =>
        hasNamedIndexContradiction(client, "task_approvals_task_decided_idx", {
          columnNames: ["session_id", "task_id", "decided_at"],
          predicate: null,
          tableName: "task_approvals",
          unique: false,
        }),
      represented: async (client) =>
        (await hasTable(client, "task_approvals")) &&
        (await hasIndexSignature(client, "task_approvals_task_decided_idx", {
          columnNames: ["session_id", "task_id", "decided_at"],
          predicate: null,
          tableName: "task_approvals",
          unique: false,
        })),
    },
    {
      label: "0008 participant current lease index",
      contradictionObserved: (client) =>
        hasNamedIndexContradiction(client, "participant_control_leases_current_unique", {
          columnNames: ["session_id", "participant_id"],
          predicate: "released_at IS NULL AND superseded_at IS NULL",
          tableName: "participant_control_leases",
          unique: true,
        }),
      represented: async (client) =>
        (await hasColumn(client, "participant_control_leases", "superseded_at")) &&
        (await hasIndexSignature(client, "participant_control_leases_current_unique", {
          columnNames: ["session_id", "participant_id"],
          predicate: "released_at IS NULL AND superseded_at IS NULL",
          tableName: "participant_control_leases",
          unique: true,
        })),
    },
    {
      label: "0009 task clear cause and removed session archival",
      represented: async (client) =>
        (await hasColumns(client, "tasks", [
          "claim_expired_at",
          "claim_expired_by",
          "released_by",
        ])) && !(await hasColumn(client, "sessions", "archived_at")),
    },
    {
      label: "0010 participant control lease epoch",
      represented: (client) => hasColumn(client, "participant_control_leases", "epoch"),
    },
    {
      label: "0011 task schedule and mailbox scope identity with unique schedule index",
      contradictionObserved: async (client) =>
        (await hasIndex(client, "tasks_schedule_identity_idx")) &&
        !(await hasIndexSignature(
          client,
          "tasks_schedule_identity_idx",
          legacyScheduleIdentityIndex,
        )) &&
        !(await hasIndexSignature(
          client,
          "tasks_schedule_identity_idx",
          providerNeutralScheduleIdentityIndex,
        )),
      represented: async (client) =>
        (await hasColumns(client, "tasks", [
          "mailbox_account_id",
          "mailbox_provider",
          "schedule_algorithm_version",
          "schedule_interval_ms",
          "schedule_window_start",
        ])) &&
        ((await hasIndexSignature(
          client,
          "tasks_schedule_identity_idx",
          legacyScheduleIdentityIndex,
        )) ||
          (await hasIndexSignature(
            client,
            "tasks_schedule_identity_idx",
            providerNeutralScheduleIdentityIndex,
          ))),
    },
    {
      label: "0012 control lease generation history primary key including epoch",
      represented: hasParticipantControlLeaseGenerationPrimaryKey,
    },
    {
      label: "0013 REST control acquisition identity",
      contradictionObserved: (client) =>
        hasNamedIndexContradiction(client, "participant_control_leases_acquisition_unique", {
          columnNames: ["session_id", "participant_id", "acquisition_id"],
          predicate: "acquisition_id IS NOT NULL",
          tableName: "participant_control_leases",
          unique: true,
        }),
      represented: async (client) =>
        (await hasColumn(client, "participant_control_leases", "acquisition_id")) &&
        (await hasIndexSignature(client, "participant_control_leases_acquisition_unique", {
          columnNames: ["session_id", "participant_id", "acquisition_id"],
          predicate: "acquisition_id IS NOT NULL",
          tableName: "participant_control_leases",
          unique: true,
        })),
    },
    {
      label: "0014 durable session projections",
      contradictionObserved: async (client) =>
        (await hasTable(client, "session_projections")) &&
        !(await hasColumns(client, "session_projections", [
          "activity",
          "covers_seq_to",
          "event_count",
          "reducer_version",
          "session_id",
          "updated_at",
        ])),
      represented: async (client) =>
        (await hasTable(client, "session_projections")) &&
        (await hasColumns(client, "session_projections", [
          "activity",
          "covers_seq_to",
          "event_count",
          "reducer_version",
          "session_id",
          "updated_at",
        ])),
    },
    {
      label: "0015 durable Session Summary lifecycle",
      contradictionObserved: async (client) =>
        ((await hasTable(client, "session_summaries")) ||
          (await hasIndex(client, "session_summaries_active_unique"))) &&
        !(
          (await hasColumns(client, "session_summaries", [
            "budget_class",
            "covers_seq_from",
            "covers_seq_to",
            "generation_task_id",
            "published_at",
            "quarantined_at",
            "session_id",
            "summary_id",
            "superseded_at",
            "validated_at",
          ])) &&
          (await hasIndexSignature(client, "session_summaries_active_unique", {
            columnNames: ["session_id", "budget_class"],
            predicate: "published_at IS NOT NULL AND superseded_at IS NULL",
            tableName: "session_summaries",
            unique: true,
          }))
        ),
      represented: async (client) =>
        (await hasColumns(client, "session_summaries", [
          "budget_class",
          "covers_seq_from",
          "covers_seq_to",
          "generation_task_id",
          "published_at",
          "quarantined_at",
          "session_id",
          "summary_id",
          "superseded_at",
          "validated_at",
        ])) &&
        (await hasIndexSignature(client, "session_summaries_active_unique", {
          columnNames: ["session_id", "budget_class"],
          predicate: "published_at IS NOT NULL AND superseded_at IS NULL",
          tableName: "session_summaries",
          unique: true,
        })),
    },
    {
      label: "0016 authentication grant foundation",
      contradictionObserved: async (client) =>
        (await hasAnyAuthFoundationArtifact(client)) && !(await hasAuthFoundationMigration(client)),
      represented: hasAuthFoundationMigration,
    },
    {
      label: "0017 task claim id",
      represented: async (client) => hasColumn(client, "tasks", "claim_id"),
    },
    {
      label: "0018 session tombstones",
      represented: (client) => hasTable(client, "session_tombstones"),
    },
    {
      label: "0019 provider-neutral recurring-work identity",
      contradictionObserved: async (client) => {
        const hasIdentityVersion = await hasColumn(client, "tasks", "schedule_identity_version");
        const hasScopeKey = await hasColumn(client, "tasks", "schedule_scope_key");
        if (!hasIdentityVersion && !hasScopeKey) {
          return false;
        }
        return (
          !hasIdentityVersion ||
          !hasScopeKey ||
          !(await hasIndexSignature(
            client,
            "tasks_schedule_identity_idx",
            providerNeutralScheduleIdentityIndex,
          ))
        );
      },
      represented: async (client) =>
        (await hasColumns(client, "tasks", ["schedule_identity_version", "schedule_scope_key"])) &&
        (await hasIndexSignature(
          client,
          "tasks_schedule_identity_idx",
          providerNeutralScheduleIdentityIndex,
        )),
    },
    {
      label: "0020 recurring-work scope size constraint",
      contradictionObserved: async (client) =>
        (await hasNamedConstraint(client, "tasks", "tasks_schedule_scope_key_size_check")) &&
        !(await hasNamedCheckConstraint(client, {
          constraintName: "tasks_schedule_scope_key_size_check",
          requiredDefinitionFragments: [
            "schedule_scope_key IS NULL",
            "octet_length(schedule_scope_key) >= 1",
            "octet_length(schedule_scope_key) <= 512",
          ],
          tableName: "tasks",
        })),
      represented: (client) =>
        hasNamedCheckConstraint(client, {
          constraintName: "tasks_schedule_scope_key_size_check",
          requiredDefinitionFragments: [
            "schedule_scope_key IS NULL",
            "octet_length(schedule_scope_key) >= 1",
            "octet_length(schedule_scope_key) <= 512",
          ],
          tableName: "tasks",
        }),
    },
    {
      label: "0021 browser operator authority foundation",
      contradictionObserved: async (client) =>
        (await hasAnyBrowserOperatorAuthorityArtifact(client)) &&
        !(await hasBrowserOperatorAuthorityFoundation(client)),
      represented: hasBrowserOperatorAuthorityFoundation,
    },
    {
      label: "0022 confirmed browser pairing authority",
      contradictionObserved: async (client) =>
        (await hasColumn(client, "browser_pairing_requests", "confirmed_by_subject")) &&
        !(await hasConfirmedBrowserPairingAuthority(client)),
      represented: hasConfirmedBrowserPairingAuthority,
    },
    {
      label: "0023 browser grant classification",
      represented: hasBrowserGrantClassification,
    },
    {
      label: "0024 browser pairing exchange source counters",
      contradictionObserved: async (client) =>
        (await hasTable(client, "browser_pairing_exchange_failures")) &&
        !(await hasBrowserPairingExchangeFailureCounters(client)),
      represented: hasBrowserPairingExchangeFailureCounters,
    },
    {
      label: "0025 indexed operator admission and bounded snapshot rows",
      contradictionObserved: async (client) =>
        (await hasAnyOperatorCommandAdmissionHardeningArtifact(client)) &&
        !(await hasOperatorCommandAdmissionHardening(client)),
      represented: hasOperatorCommandAdmissionHardening,
    },
    {
      label: "0026 stable session bootstrap identities",
      contradictionObserved: async (client) =>
        (await hasTable(client, "session_bootstrap_identities")) &&
        !(await hasStableSessionBootstrapIdentities(client)),
      represented: hasStableSessionBootstrapIdentities,
    },
  ];
}

/** Recognizes the one-to-one durable deployment bootstrap identity mapping. */
async function hasStableSessionBootstrapIdentities(client: pg.PoolClient): Promise<boolean> {
  return (
    (await hasExactColumnDefinitions(client, "session_bootstrap_identities", [
      {
        columnDefault: "now()",
        columnName: "created_at",
        dataType: "timestamp with time zone",
        isNullable: "NO",
      },
      {
        columnDefault: null,
        columnName: "identity_key",
        dataType: "text",
        isNullable: "NO",
      },
      {
        columnDefault: null,
        columnName: "session_id",
        dataType: "text",
        isNullable: "NO",
      },
    ])) &&
    (await hasNamedCheckConstraint(client, {
      constraintName: "session_bootstrap_identities_identity_key_size_check",
      requiredDefinitionFragments: [
        "octet_length(identity_key) >= 1",
        "octet_length(identity_key) <= 512",
      ],
      tableName: "session_bootstrap_identities",
    })) &&
    (await hasConstraintSignature(client, {
      columnNames: ["identity_key"],
      constraintType: "p",
      tableName: "session_bootstrap_identities",
    })) &&
    (await hasConstraintSignature(client, {
      columnNames: ["session_id"],
      constraintType: "u",
      tableName: "session_bootstrap_identities",
    })) &&
    (await hasExactForeignKey(client, {
      constraintName: "session_bootstrap_identities_session_id_sessions_session_id_fk",
      deleteAction: "r",
      localColumnNames: ["session_id"],
      referencedColumnNames: ["session_id"],
      referencedTableName: "sessions",
      tableName: "session_bootstrap_identities",
      updateAction: "a",
    }))
  );
}

const authFoundationColumns: readonly AuthColumnExpectation[] = [
  authColumn("auth_grant_audit_events", "action", "text", "NO"),
  authColumn("auth_grant_audit_events", "actor_subject", "text", "NO"),
  authColumn("auth_grant_audit_events", "audit_id", "text", "NO"),
  authColumn("auth_grant_audit_events", "grant_jti", "text", "NO"),
  authColumn("auth_grant_audit_events", "metadata", "jsonb", "NO"),
  authColumn("auth_grant_audit_events", "occurred_at", "timestamp with time zone", "NO", "now()"),
  authColumn("auth_grant_audit_events", "reason_code", "text", "NO"),
  authColumn("auth_grants", "audience", "text", "NO"),
  authColumn("auth_grants", "expires_at", "timestamp with time zone", "NO"),
  authColumn("auth_grants", "issued_at", "timestamp with time zone", "NO"),
  authColumn("auth_grants", "issuer", "text", "NO"),
  authColumn("auth_grants", "jti", "text", "NO"),
  authColumn("auth_grants", "kid", "text", "NO"),
  authColumn("auth_grants", "metadata", "jsonb", "NO"),
  authColumn("auth_grants", "revoked_at", "timestamp with time zone", "YES"),
  authColumn("auth_grants", "role", "text", "NO"),
  authColumn("auth_grants", "session_scope", "text", "NO"),
  authColumn("auth_grants", "subject", "text", "NO"),
  authColumn("auth_tickets", "admission_metadata", "jsonb", "NO"),
  authColumn("auth_tickets", "audience", "text", "NO"),
  authColumn("auth_tickets", "consumed_at", "timestamp with time zone", "YES"),
  authColumn("auth_tickets", "created_at", "timestamp with time zone", "NO"),
  authColumn("auth_tickets", "expires_at", "timestamp with time zone", "NO"),
  authColumn("auth_tickets", "parent_grant_jti", "text", "NO"),
  authColumn("auth_tickets", "ticket_hash", "text", "NO"),
];

const authFoundationIndexes: readonly (IndexSignature & {
  readonly indexName: string;
})[] = [
  {
    columnNames: ["grant_jti", "occurred_at"],
    indexName: "auth_grant_audit_grant_occurred_idx",
    predicate: null,
    tableName: "auth_grant_audit_events",
    unique: false,
  },
  {
    columnNames: ["occurred_at"],
    indexName: "auth_grant_audit_occurred_idx",
    predicate: null,
    tableName: "auth_grant_audit_events",
    unique: false,
  },
  {
    columnNames: ["expires_at"],
    indexName: "auth_grants_expiry_idx",
    predicate: null,
    tableName: "auth_grants",
    unique: false,
  },
  {
    columnNames: ["revoked_at", "expires_at"],
    indexName: "auth_grants_revoked_expiry_idx",
    predicate: null,
    tableName: "auth_grants",
    unique: false,
  },
  {
    columnNames: ["consumed_at"],
    indexName: "auth_tickets_consumed_idx",
    predicate: null,
    tableName: "auth_tickets",
    unique: false,
  },
  {
    columnNames: ["expires_at"],
    indexName: "auth_tickets_expiry_idx",
    predicate: null,
    tableName: "auth_tickets",
    unique: false,
  },
  {
    columnNames: ["parent_grant_jti", "expires_at"],
    indexName: "auth_tickets_parent_expiry_idx",
    predicate: null,
    tableName: "auth_tickets",
    unique: false,
  },
];

/** Creates one exact expected auth column fact. */
function authColumn(
  tableName: AuthColumnExpectation["tableName"],
  columnName: string,
  dataType: AuthColumnExpectation["dataType"],
  isNullable: AuthColumnExpectation["isNullable"],
  columnDefault: string | null = null,
): AuthColumnExpectation {
  return { columnDefault, columnName, dataType, isNullable, tableName };
}

const authFoundationConstraints: readonly NamedConstraintExpectation[] = [
  constraint("auth_grants_pkey", "p", "auth_grants", ["PRIMARY KEY (jti)"]),
  constraint("auth_grant_audit_events_pkey", "p", "auth_grant_audit_events", [
    "PRIMARY KEY (audit_id)",
  ]),
  constraint("auth_tickets_pkey", "p", "auth_tickets", ["PRIMARY KEY (ticket_hash)"]),
  constraint(
    "auth_grant_audit_events_grant_jti_auth_grants_jti_fk",
    "f",
    "auth_grant_audit_events",
    ["FOREIGN KEY (grant_jti) REFERENCES auth_grants(jti)"],
  ),
  constraint("auth_tickets_parent_grant_jti_auth_grants_jti_fk", "f", "auth_tickets", [
    "FOREIGN KEY (parent_grant_jti) REFERENCES auth_grants(jti)",
  ]),
  constraint("auth_grants_audience_check", "c", "auth_grants", ["audience = 'tether-rest'"]),
  constraint("auth_grants_expiry_check", "c", "auth_grants", ["expires_at > issued_at"]),
  constraint("auth_grants_lifetime_check", "c", "auth_grants", [
    "expires_at <= issued_at + '7 days'",
  ]),
  constraint("auth_grants_issuer_length_check", "c", "auth_grants", [
    "char_length(issuer) >= 1",
    "char_length(issuer) <= 512",
  ]),
  constraint("auth_grants_jti_length_check", "c", "auth_grants", [
    "char_length(jti) >= 1",
    "char_length(jti) <= 128",
  ]),
  constraint("auth_grants_kid_length_check", "c", "auth_grants", [
    "char_length(kid) >= 1",
    "char_length(kid) <= 128",
  ]),
  constraint("auth_grants_metadata_size_check", "c", "auth_grants", [
    "octet_length(metadata::text) <= 4096",
  ]),
  constraint("auth_grants_metadata_shape_check", "c", "auth_grants", [
    "jsonb_typeof(metadata) = 'object'",
  ]),
  constraint("auth_grants_revoked_check", "c", "auth_grants", [
    "revoked_at IS NULL OR revoked_at >= issued_at",
  ]),
  constraint("auth_grants_role_check", "c", "auth_grants", [
    "role = ANY (ARRAY['observer', 'participant', 'admin'])",
  ]),
  constraint("auth_grants_session_scope_length_check", "c", "auth_grants", [
    "char_length(session_scope) >= 1",
    "char_length(session_scope) <= 255",
  ]),
  constraint("auth_grants_subject_length_check", "c", "auth_grants", [
    "char_length(subject) >= 1",
    "char_length(subject) <= 255",
  ]),
  constraint("auth_grant_audit_action_check", "c", "auth_grant_audit_events", [
    "action = ANY (ARRAY['grant.created', 'grant.revoked'])",
  ]),
  constraint("auth_grant_audit_action_length_check", "c", "auth_grant_audit_events", [
    "char_length(action) >= 1",
    "char_length(action) <= 64",
  ]),
  constraint("auth_grant_audit_actor_length_check", "c", "auth_grant_audit_events", [
    "char_length(actor_subject) >= 1",
    "char_length(actor_subject) <= 255",
  ]),
  constraint("auth_grant_audit_id_length_check", "c", "auth_grant_audit_events", [
    "char_length(audit_id) >= 1",
    "char_length(audit_id) <= 128",
  ]),
  constraint("auth_grant_audit_metadata_size_check", "c", "auth_grant_audit_events", [
    "octet_length(metadata::text) <= 4096",
  ]),
  constraint("auth_grant_audit_metadata_shape_check", "c", "auth_grant_audit_events", [
    "jsonb_typeof(metadata) = 'object'",
    "metadata->>'requestId' ~ '^req_[A-Za-z0-9_-]{1,120}$'",
  ]),
  constraint("auth_grant_audit_reason_check", "c", "auth_grant_audit_events", [
    "reason_code = ANY (ARRAY['bootstrap', 'key-rotation', 'migration', 'operator-request', 'security-response'])",
  ]),
  constraint("auth_grant_audit_reason_length_check", "c", "auth_grant_audit_events", [
    "char_length(reason_code) >= 1",
    "char_length(reason_code) <= 64",
  ]),
  constraint("auth_tickets_admission_metadata_size_check", "c", "auth_tickets", [
    "octet_length(admission_metadata::text) <= 4096",
  ]),
  constraint("auth_tickets_admission_metadata_shape_check", "c", "auth_tickets", [
    "jsonb_typeof(admission_metadata) = 'object'",
    "jsonb_typeof(admission_metadata->'replicaId') = 'string'",
    "jsonb_typeof(admission_metadata->'transport') = 'string'",
    "admission_metadata->>'transport' = 'websocket'",
  ]),
  constraint("auth_tickets_audience_check", "c", "auth_tickets", ["audience = 'tether-websocket'"]),
  constraint("auth_tickets_consumed_check", "c", "auth_tickets", [
    "consumed_at IS NULL OR consumed_at >= created_at AND consumed_at <= expires_at",
  ]),
  constraint("auth_tickets_expiry_check", "c", "auth_tickets", ["expires_at > created_at"]),
  constraint("auth_tickets_lifetime_check", "c", "auth_tickets", [
    "expires_at <= created_at + '00:00:30'",
  ]),
  constraint("auth_tickets_hash_check", "c", "auth_tickets", ["ticket_hash ~ '^[0-9a-f]{64}$'"]),
];

const authFoundationConstraintFingerprints = new Set([
  "c5a21e2a79a5e62a2025c70eb95f998d9e3480afffcf4341545655898455f31b",
  "4915df2b217194414393739addbeab2126bd6c09a1e5b5444b5db9f3def257d9",
]);

/** Creates one named constraint expectation without exposing mutable arrays. */
function constraint(
  constraintName: string,
  constraintType: NamedConstraintExpectation["constraintType"],
  tableName: string,
  requiredDefinitionFragments: readonly string[],
): NamedConstraintExpectation {
  return {
    constraintName,
    constraintType,
    requiredDefinitionFragments,
    tableName,
  };
}

/** Recognizes only the complete security-relevant auth foundation migration. */
async function hasAuthFoundationMigration(client: pg.PoolClient): Promise<boolean> {
  if (
    !(await hasExactAuthFoundationColumns(client)) ||
    !(await hasNamedConstraintExpectations(client, authFoundationConstraints))
  ) {
    return false;
  }
  for (const { indexName, ...signature } of authFoundationIndexes) {
    if (!(await hasIndexSignature(client, indexName, signature))) {
      return false;
    }
  }
  return true;
}

/** Detects any partial auth migration artifact so startup rejects before DDL mutation. */
async function hasAnyAuthFoundationArtifact(client: pg.PoolClient): Promise<boolean> {
  return (
    (await hasTable(client, "auth_grants")) ||
    (await hasTable(client, "auth_grant_audit_events")) ||
    (await hasTable(client, "auth_tickets"))
  );
}

/** Returns whether all named public tables exist. */
async function hasTables(client: pg.PoolClient, tableNames: readonly string[]): Promise<boolean> {
  for (const tableName of tableNames) {
    if (!(await hasTable(client, tableName))) {
      return false;
    }
  }
  return true;
}

/** Returns whether one public table exists. */
async function hasTable(client: pg.PoolClient, tableName: string): Promise<boolean> {
  const result = await client.query<SchemaObjectExistsRow>(
    `SELECT to_regclass($1) IS NOT NULL AS "exists"`,
    [`public.${tableName}`],
  );
  return result.rows[0]?.exists === true;
}

/** Returns whether all named columns exist on one public table. */
async function hasColumns(
  client: pg.PoolClient,
  tableName: string,
  columnNames: readonly string[],
): Promise<boolean> {
  for (const columnName of columnNames) {
    if (!(await hasColumn(client, tableName, columnName))) {
      return false;
    }
  }
  return true;
}

/** Requires one table's exact column set, types, nullability, and defaults. */
async function hasExactColumnDefinitions(
  client: pg.PoolClient,
  tableName: string,
  expectations: readonly ColumnDefinitionExpectation[],
): Promise<boolean> {
  const result = await client.query<AuthColumnRow>(
    `
      SELECT
        column_default AS "columnDefault",
        column_name AS "columnName",
        data_type AS "dataType",
        is_nullable AS "isNullable",
        table_name AS "tableName"
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
    `,
    [tableName],
  );
  return (
    result.rows.length === expectations.length &&
    expectations.every((expected) =>
      result.rows.some(
        (row) =>
          row.columnDefault === expected.columnDefault &&
          row.columnName === expected.columnName &&
          row.dataType === expected.dataType &&
          row.isNullable === expected.isNullable &&
          row.tableName === tableName,
      ),
    )
  );
}

/** Requires the exact generated auth column set, types, nullability, and defaults. */
async function hasExactAuthFoundationColumns(client: pg.PoolClient): Promise<boolean> {
  const result = await client.query<AuthColumnRow>(
    `
      SELECT
        column_default AS "columnDefault",
        column_name AS "columnName",
        data_type AS "dataType",
        is_nullable AS "isNullable",
        table_name AS "tableName"
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = ANY($1::text[])
    `,
    [["auth_grant_audit_events", "auth_grants", "auth_tickets"]],
  );
  return (
    result.rows.length === authFoundationColumns.length &&
    authFoundationColumns.every((expected) =>
      result.rows.some(
        (row) =>
          row.columnDefault === expected.columnDefault &&
          row.columnName === expected.columnName &&
          row.dataType === expected.dataType &&
          row.isNullable === expected.isNullable &&
          row.tableName === expected.tableName,
      ),
    )
  );
}

/** Returns whether one column exists on one public table. */
async function hasColumn(
  client: pg.PoolClient,
  tableName: string,
  columnName: string,
): Promise<boolean> {
  const result = await client.query<SchemaObjectExistsRow>(
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

/** Returns whether a known public index name exists with a contradictory signature. */
async function hasNamedIndexContradiction(
  client: pg.PoolClient,
  indexName: string,
  expected: IndexSignature,
): Promise<boolean> {
  return (
    (await hasIndex(client, indexName)) && !(await hasIndexSignature(client, indexName, expected))
  );
}

/** Returns whether one public index name exists. */
async function hasIndex(client: pg.PoolClient, indexName: string): Promise<boolean> {
  const result = await client.query<SchemaObjectExistsRow>(
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

/** Returns whether a public index has the expected table-scoped structural signature. */
async function hasIndexSignature(
  client: pg.PoolClient,
  indexName: string,
  expected: IndexSignature,
): Promise<boolean> {
  const result = await client.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM pg_index index_record
        JOIN pg_class index_class ON index_class.oid = index_record.indexrelid
        JOIN pg_namespace index_namespace ON index_namespace.oid = index_class.relnamespace
        JOIN pg_class table_record ON table_record.oid = index_record.indrelid
        JOIN pg_namespace table_namespace ON table_namespace.oid = table_record.relnamespace
        WHERE index_namespace.nspname = 'public'
          AND table_namespace.nspname = 'public'
          AND index_class.relname = $1
          AND table_record.relname = $2
          AND index_record.indisunique = $3
          AND ARRAY(
            SELECT attribute.attname::text
            FROM unnest(index_record.indkey) WITH ORDINALITY AS key_column(attnum, position)
            JOIN pg_attribute attribute
              ON attribute.attrelid = index_record.indrelid
              AND attribute.attnum = key_column.attnum
            WHERE key_column.position <= index_record.indnkeyatts
            ORDER BY key_column.position
          ) = $4::text[]
          AND (
            ($5::text IS NULL AND index_record.indpred IS NULL)
            OR (
              $5::text IS NOT NULL
              AND regexp_replace(
                replace(
                  replace(lower(pg_get_expr(index_record.indpred, index_record.indrelid)), '"', ''),
                  lower($2::text) || '.',
                  ''
                ),
                '[[:space:]()]',
                '',
                'g'
              ) = regexp_replace(
                replace(replace(lower($5::text), '"', ''), lower($2::text) || '.', ''),
                '[[:space:]()]',
                '',
                'g'
              )
            )
          )
      ) AS "exists"
    `,
    [indexName, expected.tableName, expected.unique, expected.columnNames, expected.predicate],
  );
  return result.rows[0]?.exists === true;
}

/**
 * Returns whether the public Control Lease table has the migration 0012
 * primary-key structure, independently of PostgreSQL's truncated identifier.
 */
async function hasParticipantControlLeaseGenerationPrimaryKey(
  client: pg.PoolClient,
): Promise<boolean> {
  return hasConstraintSignature(client, {
    columnNames: ["session_id", "participant_id", "instance_id", "epoch"],
    constraintType: "p",
    tableName: "participant_control_leases",
  });
}

/** Returns whether a public constraint has the expected table-scoped signature. */
async function hasConstraintSignature(
  client: pg.PoolClient,
  expected: ConstraintSignature,
): Promise<boolean> {
  const result = await client.query<ConstraintSignatureRow>(
    `
      SELECT
        (
          SELECT jsonb_agg(attribute.attname ORDER BY key_column.position)
          FROM unnest(constraint_record.conkey) WITH ORDINALITY AS key_column(attnum, position)
          JOIN pg_attribute attribute
            ON attribute.attrelid = constraint_record.conrelid
            AND attribute.attnum = key_column.attnum
        ) AS "columnNames",
        constraint_record.contype::text AS "constraintType",
        table_record.relname AS "tableName"
      FROM pg_constraint constraint_record
      JOIN pg_class table_record ON table_record.oid = constraint_record.conrelid
      JOIN pg_namespace namespace_record ON namespace_record.oid = table_record.relnamespace
      WHERE namespace_record.nspname = 'public'
        AND table_record.relname = $1
        AND constraint_record.contype = $2
    `,
    [expected.tableName, expected.constraintType],
  );
  return result.rows.some(
    (row) =>
      row.tableName === expected.tableName &&
      row.constraintType === expected.constraintType &&
      row.columnNames.length === expected.columnNames.length &&
      row.columnNames.every((columnName, index) => columnName === expected.columnNames[index]),
  );
}

/** Verifies one exact validated foreign key, including referenced columns and actions. */
async function hasExactForeignKey(
  client: pg.PoolClient,
  expected: {
    readonly constraintName: string;
    readonly deleteAction: "a" | "c" | "n" | "r";
    readonly localColumnNames: readonly string[];
    readonly referencedColumnNames: readonly string[];
    readonly referencedTableName: string;
    readonly tableName: string;
    readonly updateAction: "a" | "c" | "n" | "r";
  },
): Promise<boolean> {
  const result = await client.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM pg_constraint constraint_record
        JOIN pg_class table_record ON table_record.oid = constraint_record.conrelid
        JOIN pg_class referenced_table ON referenced_table.oid = constraint_record.confrelid
        JOIN pg_namespace namespace_record ON namespace_record.oid = table_record.relnamespace
        JOIN pg_namespace referenced_namespace
          ON referenced_namespace.oid = referenced_table.relnamespace
        WHERE namespace_record.nspname = 'public'
          AND referenced_namespace.nspname = 'public'
          AND table_record.relname = $1
          AND constraint_record.conname = $2
          AND constraint_record.contype = 'f'
          AND referenced_table.relname = $3
          AND constraint_record.confdeltype = $4
          AND constraint_record.confupdtype = $5
          AND constraint_record.convalidated
          AND (
            SELECT jsonb_agg(attribute.attname ORDER BY key_column.position)
            FROM unnest(constraint_record.conkey) WITH ORDINALITY AS key_column(attnum, position)
            JOIN pg_attribute attribute
              ON attribute.attrelid = constraint_record.conrelid
              AND attribute.attnum = key_column.attnum
          ) = $6::jsonb
          AND (
            SELECT jsonb_agg(attribute.attname ORDER BY key_column.position)
            FROM unnest(constraint_record.confkey) WITH ORDINALITY AS key_column(attnum, position)
            JOIN pg_attribute attribute
              ON attribute.attrelid = constraint_record.confrelid
              AND attribute.attnum = key_column.attnum
          ) = $7::jsonb
      ) AS exists
    `,
    [
      expected.tableName,
      expected.constraintName,
      expected.referencedTableName,
      expected.deleteAction,
      expected.updateAction,
      JSON.stringify(expected.localColumnNames),
      JSON.stringify(expected.referencedColumnNames),
    ],
  );
  return result.rows[0]?.exists === true;
}

/** Returns whether a named public-table constraint exists. */
async function hasNamedConstraint(
  client: pg.PoolClient,
  tableName: string,
  constraintName: string,
): Promise<boolean> {
  const result = await client.query<SchemaObjectExistsRow>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM pg_constraint constraint_record
        JOIN pg_class table_record ON table_record.oid = constraint_record.conrelid
        JOIN pg_namespace namespace_record ON namespace_record.oid = table_record.relnamespace
        WHERE namespace_record.nspname = 'public'
          AND table_record.relname = $1
          AND constraint_record.conname = $2
      ) AS exists
    `,
    [tableName, constraintName],
  );
  return result.rows[0]?.exists === true;
}

/** Verifies one named check constraint by owner and normalized definition fragments. */
async function hasNamedCheckConstraint(
  client: pg.PoolClient,
  expectation: Omit<NamedConstraintExpectation, "constraintType">,
): Promise<boolean> {
  const result = await client.query<NamedConstraintRow>(
    `
      SELECT
        constraint_record.conname AS "constraintName",
        constraint_record.contype::text AS "constraintType",
        NULL::text AS "deleteAction",
        pg_get_constraintdef(constraint_record.oid, true) AS definition,
        table_record.relname AS "tableName",
        NULL::text AS "updateAction",
        constraint_record.convalidated AS validated
      FROM pg_constraint constraint_record
      JOIN pg_class table_record ON table_record.oid = constraint_record.conrelid
      JOIN pg_namespace namespace_record ON namespace_record.oid = table_record.relnamespace
      WHERE namespace_record.nspname = 'public'
        AND table_record.relname = $1
        AND constraint_record.conname = $2
        AND constraint_record.contype = 'c'
    `,
    [expectation.tableName, expectation.constraintName],
  );
  const row = result.rows[0];
  if (row === undefined || row.validated !== true) {
    return false;
  }
  const normalizedDefinition = normalizeConstraintDefinition(row.definition);
  return expectation.requiredDefinitionFragments.every((fragment) =>
    normalizedDefinition.includes(normalizeConstraintDefinition(fragment)),
  );
}

/** Verifies named constraints by type, owning table, and normalized definition fragments. */
async function hasNamedConstraintExpectations(
  client: pg.PoolClient,
  expectations: readonly NamedConstraintExpectation[],
): Promise<boolean> {
  const result = await client.query<NamedConstraintRow>(
    `
      SELECT
        constraint_record.conname AS "constraintName",
        constraint_record.contype::text AS "constraintType",
        CASE WHEN constraint_record.contype = 'f'
          THEN constraint_record.confdeltype::text
          ELSE NULL
        END AS "deleteAction",
        pg_get_constraintdef(constraint_record.oid, true) AS definition,
        table_record.relname AS "tableName",
        CASE WHEN constraint_record.contype = 'f'
          THEN constraint_record.confupdtype::text
          ELSE NULL
        END AS "updateAction",
        constraint_record.convalidated AS validated
      FROM pg_constraint constraint_record
      JOIN pg_class table_record ON table_record.oid = constraint_record.conrelid
      JOIN pg_namespace namespace_record ON namespace_record.oid = table_record.relnamespace
      WHERE namespace_record.nspname = 'public'
        AND table_record.relname = ANY($1::text[])
    `,
    [["auth_grant_audit_events", "auth_grants", "auth_tickets"]],
  );
  const expectedConstraintsPresent = expectations.every((expectation) => {
    const row = result.rows.find(
      (candidate) =>
        candidate.constraintName === expectation.constraintName &&
        candidate.constraintType === expectation.constraintType &&
        candidate.tableName === expectation.tableName,
    );
    if (
      row === undefined ||
      row.validated !== true ||
      (expectation.constraintType === "f"
        ? row.deleteAction !== "a" || row.updateAction !== "a"
        : row.deleteAction !== null || row.updateAction !== null)
    ) {
      return false;
    }
    const normalizedDefinition = normalizeConstraintDefinition(row.definition);
    return expectation.requiredDefinitionFragments.every((fragment) =>
      normalizedDefinition.includes(normalizeConstraintDefinition(fragment)),
    );
  });
  const fingerprint = fingerprintAuthConstraints(result.rows);
  return (
    expectedConstraintsPresent &&
    result.rows.length === expectations.length &&
    authFoundationConstraintFingerprints.has(fingerprint)
  );
}

/** Fingerprints exact normalized auth constraints, validation state, and FK actions. */
function fingerprintAuthConstraints(rows: readonly NamedConstraintRow[]): string {
  const facts = rows
    .map((row) => ({
      constraintName: row.constraintName,
      constraintType: row.constraintType,
      definition: normalizeConstraintDefinition(row.definition),
      deleteAction: row.deleteAction,
      tableName: row.tableName,
      updateAction: row.updateAction,
      validated: row.validated,
    }))
    .sort((left, right) =>
      `${left.tableName}:${left.constraintName}`.localeCompare(
        `${right.tableName}:${right.constraintName}`,
      ),
    );
  return createHash("sha256").update(JSON.stringify(facts)).digest("hex");
}

/** Normalizes catalog-rendered constraint SQL without discarding operators or literals. */
function normalizeConstraintDefinition(value: string): string {
  return value
    .toLowerCase()
    .replaceAll('"', "")
    .replaceAll("::text", "")
    .replaceAll("::jsonb", "")
    .replaceAll("::interval", "")
    .replaceAll("(", "")
    .replaceAll(")", "")
    .replaceAll(/\s+/gu, "");
}
