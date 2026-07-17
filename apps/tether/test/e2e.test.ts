import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";

import { readMigrationFiles } from "drizzle-orm/migrator";
import { Effect } from "effect";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it, onTestFinished } from "vitest";
import WebSocket from "ws";
import {
  mintTestAuthToken,
  testAuthSigningKid,
  testAuthSigningSecret,
} from "../src/auth/test-tokens.js";
import type { AuthRole } from "../src/auth/token.js";
import { ParticipantRuntimeClient } from "../src/client.js";
import {
  DatabaseMigrationError,
  projectDatabaseMigrationFailure,
} from "../src/database-migration.js";
import type { DatabasePool } from "../src/db.js";
import {
  acquireRestParticipantControl,
  appendEvent,
  archiveClientSessionBinding,
  claimTaskWithEvent,
  completeTaskWithEvent,
  createSession as createDbSession,
  createPool,
  createTaskWithEvent,
  expireTaskClaims,
  getTask,
  listEvents,
  listParticipants,
  listTaskApprovals,
  migrate,
  recordTaskApproval,
  releaseControlLease,
  upsertClientSessionBinding,
  upsertParticipant,
  upsertParticipantWithEvent,
} from "../src/db.js";
import type { AppServer, AppServerDebugInfo } from "../src/http.js";
import { createAppServer, createAppServerWithSessionService } from "../src/http.js";
import type { StructuredLogEntry } from "../src/observability.js";
import { sessionEventType, systemProducerId, webSocketOperation } from "../src/protocol.js";
import { defaultResourceLimits } from "../src/resource-limits.js";
import { clientPublishDenyReason } from "../src/session-event-publish-policy.js";
import { createSessionServiceEffect } from "../src/session-service.js";
import type {
  ControlLeaseSnapshot,
  ParticipantRuntimeSnapshot,
  SessionDebugSummary,
  SessionEvent,
} from "../src/types.js";
import {
  createPostgresConcurrencyCoordinator,
  PostgresConcurrencyCleanupError,
  wrapPoolQueries,
} from "./postgres-concurrency-coordinator.js";

const e2e = process.env.E2E === "true" ? describe : describe.skip;
const adminDatabaseUrl = readRequiredE2eAdminDatabaseUrl();
const e2eAuthOptions = {
  activeKid: testAuthSigningKid,
  mode: "required",
  secrets: { [testAuthSigningKid]: testAuthSigningSecret },
} as const;

/** Exact final Plan 32 current-lease fence used by protected mutations. */
const currentControlLeaseFenceQuery = `
      SELECT
        control_channel AS "controlChannel",
        epoch,
        instance_id AS "instanceId",
        lease_expires_at AS "leaseExpiresAt"
      FROM participant_control_leases
      WHERE session_id = $1
        AND participant_id = $2
        AND released_at IS NULL
        AND superseded_at IS NULL
      ORDER BY lease_expires_at DESC, claimed_at DESC, instance_id
      FOR UPDATE
    `;

/** Exact sequence-row allocator whose transaction lock orders event publishers. */
const eventSequenceAllocatorQuery = `
      UPDATE session_event_sequences
      SET next_seq = next_seq + 1
      WHERE session_id = $1
      RETURNING next_seq - 1 AS "seq"
    `;

/** Generated migration filenames in their authoritative application order. */
const generatedMigrationNames = [
  "0000_lively_enchantress.sql",
  "0001_far_mephisto.sql",
  "0002_left_havok.sql",
  "0003_productive_stone_men.sql",
  "0004_clean_arclight.sql",
  "0005_bored_roulette.sql",
  "0006_wandering_iron_monger.sql",
  "0007_unique_whirlwind.sql",
  "0008_flowery_the_watchers.sql",
  "0009_sticky_lucky_pierre.sql",
  "0010_true_human_torch.sql",
  "0011_special_blue_marvel.sql",
  "0012_control_lease_generation_history.sql",
  "0013_misty_leo.sql",
] as const;

interface JsonResponse {
  readonly [key: string]: unknown;
}

interface ServerProcessResult {
  readonly exitCode: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly stderr: string;
  readonly stdout: string;
}

interface RunServerProcessOptions {
  readonly env?: NodeJS.ProcessEnv;
  readonly shutdownAfterStdout?: string;
}

type AsyncOutcome<TValue> =
  | { readonly status: "fulfilled"; readonly value: TValue }
  | { readonly reason: unknown; readonly status: "rejected" };

/** Reads the admin database URL required for enabled e2e runs. */
function readRequiredE2eAdminDatabaseUrl(): string {
  const value = process.env.E2E_ADMIN_DATABASE_URL;
  if (process.env.E2E === "true" && (value === undefined || value.length === 0)) {
    throw new Error("E2E_ADMIN_DATABASE_URL is required when E2E=true");
  }
  return value ?? "postgres://e2e-disabled:e2e-disabled@127.0.0.1:54329/postgres";
}

interface RawJsonResponse<TResponse extends JsonResponse> {
  readonly body: TResponse;
  readonly status: number;
  readonly text: string;
}

interface SessionResponse extends JsonResponse {
  readonly session: {
    readonly sessionId: string;
  };
}

interface ClientBindingsResponse extends JsonResponse {
  readonly bindings: readonly {
    readonly externalId: string;
    readonly provider: string;
    readonly sessionId: string;
  }[];
}

interface ClientSessionBindingResponse extends JsonResponse {
  readonly binding: {
    readonly externalId: string;
    readonly provider: string;
    readonly sessionId: string;
  };
  readonly created: boolean;
  readonly session: {
    readonly sessionId: string;
  };
}

interface ArchivedClientSessionBindingResponse extends JsonResponse {
  readonly binding: {
    readonly externalId: string;
    readonly provider: string;
    readonly sessionId: string;
  };
}

interface TaskResponse extends JsonResponse {
  readonly contract?: TaskContractsResponse["taskContracts"][number] | null;
  readonly status?: "created" | "replayed";
  readonly task: {
    readonly cancelledAt: string | null;
    readonly claimExpiredAt: string | null;
    readonly claimExpiredBy: string | null;
    readonly claimExpiresAt: string | null;
    readonly claimedAt: string | null;
    readonly claimedBy: string | null;
    readonly completedAt: string | null;
    readonly createdAt: string;
    readonly failedAt: string | null;
    readonly failure: Record<string, unknown> | null;
    readonly input: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly releasedAt: string | null;
    readonly releasedBy: string | null;
    readonly result: Record<string, unknown> | null;
    readonly sessionId: string;
    readonly taskId: string;
  };
}

interface PublishedEventResponse extends JsonResponse {
  readonly event: SessionEvent;
  readonly status?: "created" | "replayed";
}

interface TaskApprovalResponse extends JsonResponse {
  readonly decision: "approved" | "rejected";
  readonly event?: SessionEvent;
  readonly existingDecision?: "approved" | "rejected";
  readonly ignoredReason?: "already_approved" | "already_rejected";
  readonly status: "ignored" | "recorded";
  readonly task: {
    readonly completedAt: string | null;
    readonly result: Record<string, unknown> | null;
    readonly taskId: string;
  };
}

interface EventsResponse extends JsonResponse {
  readonly events: readonly SessionEvent[];
  readonly pagination: {
    readonly afterSeq: number;
    readonly hasMore: boolean;
    readonly limit: number;
    readonly nextAfterSeq: number;
    readonly returned: number;
  };
}

interface ParticipantRegistrationResponse extends JsonResponse {
  readonly acquisitionId?: string;
  readonly acquisitionStatus?: "claimed" | "replayed" | "superseded";
  readonly controlEpoch?: number;
  readonly registrationStatus: string;
}

interface ParticipantsResponse extends JsonResponse {
  readonly participants: readonly {
    readonly displayName?: string;
    readonly lastSeenAt?: string;
    readonly participantId: string;
    readonly runtimeKind?: string;
  }[];
}

interface TaskContractsResponse extends JsonResponse {
  readonly taskContracts: readonly {
    readonly approval: "none" | "optional" | "required_for_mutation";
    readonly description: string;
    readonly displayName: string;
    readonly inputJsonSchema?: Record<string, unknown>;
    readonly inputSchemaRef: string;
    readonly participantId: string;
    readonly participantRuntimeKind: string;
    readonly readOnlyByDefault: boolean;
    readonly resultJsonSchema?: Record<string, unknown>;
    readonly resultSchemaRef: string;
    readonly runtimeKind: string;
    readonly sessionId: string;
    readonly taskKind: string;
    readonly title: string;
    readonly version: string;
  }[];
}

interface TaskContractResponse extends TaskContractsResponse {
  readonly taskContract: TaskContractsResponse["taskContracts"][number];
}

interface SessionContextResponse extends JsonResponse {
  readonly context: {
    readonly activeTasks: readonly {
      readonly kind: string;
      readonly taskId: string;
    }[];
    readonly budget: {
      readonly estimatedTokens: number;
      readonly omittedEventCount: number;
      readonly requestedTokens: number;
    };
    readonly forParticipant: string | null;
    readonly kind: "session_context";
    readonly latestSummary: null;
    readonly recentEventRange: {
      readonly endSeq: number | null;
      readonly startSeq: number | null;
    };
    readonly recentEvents: readonly {
      readonly seq: number;
      readonly type: string;
    }[];
    readonly recentTerminalTasks: readonly {
      readonly kind: string;
      readonly taskId: string;
    }[];
    readonly sessionId: string;
    readonly taskContracts: TaskContractsResponse["taskContracts"];
  };
}

interface ControlLeaseSnapshotsResponse extends JsonResponse {
  readonly controlLeases: readonly ControlLeaseSnapshot[];
}

interface ParticipantRuntimeSnapshotsResponse extends JsonResponse {
  readonly participants: readonly ParticipantRuntimeSnapshot[];
}

interface TasksResponse extends JsonResponse {
  readonly tasks: readonly {
    readonly cancelledAt: string | null;
    readonly claimExpiresAt: string | null;
    readonly claimedBy: string | null;
    readonly completedAt: string | null;
    readonly result: Record<string, unknown> | null;
    readonly taskId: string;
  }[];
}

interface TaskSnapshotsResponse extends JsonResponse {
  readonly tasks: readonly {
    readonly approvals: readonly {
      readonly approvalEventId: string;
      readonly decidedAt: string;
      readonly decidedByParticipantId: string;
      readonly decision: "approved" | "rejected";
      readonly reason: Record<string, unknown>;
      readonly targetKey: string;
      readonly taskId: string;
    }[];
    readonly completedAt: string | null;
    readonly releasedAt: string | null;
    readonly status:
      | "cancelled"
      | "claim_active"
      | "claim_cleared"
      | "claim_expired"
      | "completed"
      | "failed"
      | "unclaimed";
    readonly taskId: string;
  }[];
}

interface SessionDebugSummaryResponse extends JsonResponse {
  readonly summary: SessionDebugSummary;
}

interface ServerDebugResponse extends JsonResponse {
  readonly server: AppServerDebugInfo;
}

interface SessionListResponse extends JsonResponse {
  readonly sessions: readonly {
    readonly activeTaskCount: number;
    readonly activity?: "idle" | "queued" | "running" | "settled";
    readonly archived?: boolean;
    readonly bindings: readonly {
      readonly externalId: string;
      readonly provider: string;
    }[];
    readonly branch?: string | null;
    readonly createdAt: string;
    readonly cwd?: string | null;
    readonly deleted?: boolean;
    readonly eventCount: number;
    readonly forkedFrom?: {
      readonly forkSeq: number;
      readonly parentSessionId: string;
    } | null;
    readonly git?: Record<string, unknown> | null;
    readonly host?: "live" | "none" | "stale";
    readonly lastEventAt: string | null;
    readonly participantCount: number;
    readonly project?: string | null;
    readonly sessionId: string;
    readonly tangentOf?: Record<string, unknown> | null;
    readonly taskCount: number;
    readonly title?: string;
    readonly updatedAt?: string;
    readonly workspace?: string | null;
  }[];
}

interface PermanentDeleteResponse extends JsonResponse {
  readonly detail?: string;
  readonly ok: boolean;
  readonly reason?:
    | "failed"
    | "not-archived"
    | "not-found"
    | "presence_scope_insufficient"
    | "protected";
  readonly sessionId?: string;
}

e2e("tether e2e", () => {
  const databaseName = `tether_e2e_${randomUUID().replaceAll("-", "_")}`;
  const databaseUrl = buildDatabaseUrl(databaseName);
  let app: AppServer | null = null;
  let baseUrl = "";
  let pool: DatabasePool | null = null;

  beforeAll(async () => {
    await createDatabase(databaseName);
    pool = createPool(databaseUrl);
    await migrate(pool);
    app = createAppServer(pool, {
      auth: e2eAuthOptions,
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 1_000,
      },
      taskClaimSweeper: { intervalMs: 50 },
    });
    const port = await findOpenPort();
    await app.listen(port);
    baseUrl = `http://127.0.0.1:${port}`;
  }, 30_000);

  afterAll(async () => {
    await app?.close();
    await pool?.end();
    await dropDatabase(databaseName);
  }, 30_000);

  it("applies migrations to a fresh database", async () => {
    const response = await request<SessionResponse>("/sessions", {
      body: {},
      method: "POST",
    });

    expect(response.session.sessionId).toMatch(/^sess_/u);
  });

  it("rejects session-scoped writes for unknown sessions without creating phantom rows", async () => {
    const missingSessionId = `sess_missing_${randomUUID()}`;
    const before = await request<SessionListResponse>("/sessions");
    const publish = await requestStatus(`/sessions/${missingSessionId}/events`, {
      body: {
        payload: { text: "do not create a session" },
        producerId: "part_missing_session",
        type: "user.message",
      },
      method: "POST",
    });
    const registration = await requestStatus(`/sessions/${missingSessionId}/participants`, {
      body: {
        displayName: "Missing Session Participant",
        instanceId: "inst_missing_session",
        participantId: "part_missing_session",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    const after = await request<SessionListResponse>("/sessions");

    expect(publish.status).toBe(404);
    expect(publish.body).toMatchObject({
      reason: "session_not_found",
      sessionId: missingSessionId,
    });
    expect(registration.status).toBe(404);
    expect(registration.body).toMatchObject({
      reason: "session_not_found",
      sessionId: missingSessionId,
    });
    expect(after.sessions.map((session) => session.sessionId).sort()).toEqual(
      before.sessions.map((session) => session.sessionId).sort(),
    );
  });

  it("runs the control-lease current-row migration after legacy baselining", async () => {
    const legacyDatabaseName = `tether_e2e_lease_migration_${randomUUID().replaceAll("-", "_")}`;
    const legacyDatabase = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(legacyDatabase);
      await seedDuplicateCurrentControlLeases(legacyDatabase);

      await migrate(legacyDatabase);

      const [columns, indexes, duplicateGroups, supersededRows] = await Promise.all([
        legacyDatabase.pool.query<{ readonly count: number }>(
          `
            SELECT count(*)::int AS count
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'participant_control_leases'
              AND column_name = 'superseded_at'
          `,
        ),
        legacyDatabase.pool.query<{ readonly count: number }>(
          `
            SELECT count(*)::int AS count
            FROM pg_indexes
            WHERE tablename = 'participant_control_leases'
              AND indexname = 'participant_control_leases_current_unique'
          `,
        ),
        legacyDatabase.pool.query<{ readonly count: number }>(
          `
            SELECT count(*)::int AS count
            FROM (
              SELECT session_id, participant_id
              FROM participant_control_leases
              WHERE released_at IS NULL
                AND superseded_at IS NULL
              GROUP BY session_id, participant_id
              HAVING count(*) > 1
            ) duplicates
          `,
        ),
        legacyDatabase.pool.query<{ readonly count: number }>(
          `
            SELECT count(*)::int AS count
            FROM participant_control_leases
            WHERE session_id = 'sess_duplicate_lease_migration'
              AND participant_id = 'part_duplicate_lease_migration'
              AND superseded_at IS NOT NULL
          `,
        ),
      ]);

      expect(columns.rows[0]?.count).toBe(1);
      expect(indexes.rows[0]?.count).toBe(1);
      expect(duplicateGroups.rows[0]?.count).toBe(0);
      expect(supersededRows.rows[0]?.count).toBe(1);
    } finally {
      await legacyDatabase.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("baselines a migration 0003 legacy prefix and migrates through current head", async () => {
    const legacyDatabaseName = `tether_e2e_prefix_0003_${randomUUID().replaceAll("-", "_")}`;
    const legacyDatabase = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0003(legacyDatabase);

      await migrate(legacyDatabase);

      const columns = await legacyDatabase.pool.query<{
        readonly count: number;
      }>(
        `
          SELECT count(*)::int AS count
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND (
              (table_name = 'sessions' AND column_name = 'archived_at')
              OR (table_name = 'tasks' AND column_name IN (
                'claim_expired_at',
                'claim_expired_by',
                'input',
                'released_by'
              ))
            )
        `,
      );
      const migrationRows = await legacyDatabase.pool.query<{
        readonly count: number;
      }>(`SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`);

      expect(columns.rows[0]?.count).toBe(4);
      expect(migrationRows.rows[0]?.count).toBeGreaterThanOrEqual(10);
    } finally {
      await legacyDatabase.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it.each(
    generatedMigrationNames.map((_, prefixIndex) => prefixIndex),
  )("baselines journal-less migration prefix %i through current head", async (prefixIndex) => {
    const legacyDatabaseName = `tether_e2e_prefix_${String(prefixIndex).padStart(4, "0")}_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationPrefix(database, prefixIndex);

      await migrate(database);

      const expectedMigrations = readMigrationFiles({
        migrationsFolder: "drizzle",
      });
      const journal = await database.pool.query<{
        readonly createdAt: string;
        readonly hash: string;
      }>(
        `
            SELECT created_at::text AS "createdAt", hash
            FROM drizzle.__drizzle_migrations
            ORDER BY id
          `,
      );
      expect(journal.rows).toEqual(
        expectedMigrations.map((migration) => ({
          createdAt: String(migration.folderMillis),
          hash: migration.hash,
        })),
      );
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rejects a partial migration 0000 schema before journal or application mutation", async () => {
    const legacyDatabaseName = `tether_e2e_partial_0000_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await database.pool.query(`
        CREATE TABLE sessions (
          session_id text PRIMARY KEY NOT NULL
        )
      `);

      await expect(migrate(database)).rejects.toMatchObject({
        name: "DatabaseMigrationError",
        reason: "unsupported_schema",
        recognizedPrefix: null,
      });

      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      const applicationMutation = await database.pool.query<{
        readonly count: number;
      }>(
        `
          SELECT count(*)::int AS count
          FROM pg_class table_record
          JOIN pg_namespace namespace_record ON namespace_record.oid = table_record.relnamespace
          WHERE namespace_record.nspname = 'public'
            AND table_record.relname IN (
              'participants',
              'session_event_sequences',
              'session_events',
              'tasks'
            )
        `,
      );
      expect(journal.rows[0]?.count).toBe(0);
      expect(applicationMutation.rows[0]?.count).toBe(0);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("reports an unsupported schema through one structured real-server failure", async () => {
    const databaseName = `tether_e2e_server_unsupported_${randomUUID().replaceAll("-", "_")}`;
    const databaseUrl = buildDatabaseUrl(databaseName);
    const database = createPool(databaseUrl);
    try {
      await createDatabase(databaseName);
      await database.pool.query(`CREATE TABLE sessions (session_id text PRIMARY KEY NOT NULL)`);

      const result = await runServerProcess(databaseUrl);
      const migrationEvents = parseStructuredLogEntries(result.stderr).filter(
        (entry) => entry.event === "database.migration_failed",
      );

      expect(result.exitCode).not.toBe(0);
      expect(result.signal).toBeNull();
      expect(migrationEvents).toEqual([
        {
          details: {
            expectedFacts: ["migration_0000_complete=true"],
            journalHead: null,
            observedFacts: ["migration_0000_complete=false", "known_tether_table_count=1"],
            reason: "unsupported_schema",
            recognizedPrefix: null,
          },
          event: "database.migration_failed",
        },
      ]);
      expect(result.stderr).not.toContain(databaseUrl);
      expect(result.stderr).not.toContain("e2e-local-postgres-password");
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it("reports an invalid journal through the same typed real-server projection", async () => {
    const databaseName = `tether_e2e_server_invalid_journal_${randomUUID().replaceAll("-", "_")}`;
    const databaseUrl = buildDatabaseUrl(databaseName);
    const database = createPool(databaseUrl);
    try {
      await createDatabase(databaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);
      const firstMigration = readMigrationFiles({
        migrationsFolder: "drizzle",
      })[0];
      if (firstMigration === undefined) {
        throw new Error("Expected at least one generated migration");
      }
      const rawHashMarker = "raw-invalid-journal-hash-marker";
      await database.pool.query(
        `INSERT INTO drizzle.__drizzle_migrations (hash, created_at) VALUES ($1, $2)`,
        [rawHashMarker, firstMigration.folderMillis],
      );

      let expectedDetails: ReturnType<typeof projectDatabaseMigrationFailure> | null = null;
      try {
        await migrate(database);
      } catch (error) {
        if (!(error instanceof DatabaseMigrationError)) {
          throw error;
        }
        expectedDetails = projectDatabaseMigrationFailure(error);
      }
      if (expectedDetails === null) {
        throw new Error("Expected invalid journal migration to fail");
      }

      const result = await runServerProcess(databaseUrl);
      const migrationEvents = parseStructuredLogEntries(result.stderr).filter(
        (entry) => entry.event === "database.migration_failed",
      );

      expect(result.exitCode).not.toBe(0);
      expect(result.signal).toBeNull();
      expect(migrationEvents).toEqual([
        {
          details: expectedDetails,
          event: "database.migration_failed",
        },
      ]);
      expect(expectedDetails).toEqual({
        expectedFacts: ["journal_position=8", "known_migration_exists=true"],
        journalHead: {
          hashMatchesKnownMigration: false,
          position: 8,
          timestamp: String(firstMigration.folderMillis),
        },
        observedFacts: ["journal_row_count=9", "timestamp_matches=false", "hash_matches=false"],
        reason: "invalid_journal",
        recognizedPrefix: 7,
      });
      expect(result.stderr).not.toContain(rawHashMarker);
      expect(result.stderr).not.toContain("DatabaseMigrationError");
      expect(result.stderr.match(/database\.migration_failed/gu)).toHaveLength(1);
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it("preserves generic startup failure and successful startup logging", async () => {
    const databaseName = `tether_e2e_server_logging_${randomUUID().replaceAll("-", "_")}`;
    const databaseUrl = buildDatabaseUrl(databaseName);
    try {
      await createDatabase(databaseName);

      const genericFailure = await runServerProcess(databaseUrl, {
        env: { AUTH_MODE: "required", AUTH_SIGNING_SECRET: "" },
      });
      expect(genericFailure.exitCode).not.toBe(0);
      expect(genericFailure.stderr).toContain(
        "AUTH_SIGNING_SECRET is required when AUTH_MODE=required",
      );
      expect(genericFailure.stderr).not.toContain("database.migration_failed");

      const successfulStartup = await runServerProcess(databaseUrl, {
        shutdownAfterStdout: "tether listening on :0",
      });
      expect(successfulStartup).toMatchObject({ exitCode: 0, signal: null });
      expect(successfulStartup.stdout).toContain("tether listening on :0");
      expect(successfulStartup.stderr).not.toContain("database.migration_failed");
    } finally {
      await dropDatabase(databaseName);
    }
  }, 20_000);

  it("migrates a fresh database that contains an unrelated public table", async () => {
    const databaseName = `tether_e2e_unrelated_table_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(databaseName));
    try {
      await createDatabase(databaseName);
      await database.pool.query(`CREATE TABLE operator_scratchpad (note text NOT NULL)`);

      await migrate(database);

      const unrelatedTable = await database.pool.query<{
        readonly exists: boolean;
      }>(`SELECT to_regclass('public.operator_scratchpad') IS NOT NULL AS exists`);
      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      expect(unrelatedTable.rows[0]?.exists).toBe(true);
      expect(journal.rows[0]?.count).toBe(
        readMigrationFiles({ migrationsFolder: "drizzle" }).length,
      );
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it("accepts a current journal-less schema with an additive unrelated column", async () => {
    const databaseName = `tether_e2e_additive_schema_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(databaseName));
    try {
      await createDatabase(databaseName);
      await applyLegacyMigrationPrefix(database, generatedMigrationNames.length - 1);
      await database.pool.query(`ALTER TABLE tasks ADD COLUMN operator_annotation text`);

      await migrate(database);

      const additiveColumn = await database.pool.query<{
        readonly exists: boolean;
      }>(
        `
          SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'tasks'
              AND column_name = 'operator_annotation'
          ) AS exists
        `,
      );
      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      expect(additiveColumn.rows[0]?.exists).toBe(true);
      expect(journal.rows[0]?.count).toBe(
        readMigrationFiles({ migrationsFolder: "drizzle" }).length,
      );
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it("leaves the exact journal and application schema unchanged on second startup", async () => {
    const databaseName = `tether_e2e_idempotent_migration_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(databaseName));
    try {
      await createDatabase(databaseName);
      await migrate(database);
      const journalBefore = await readMigrationJournal(database);
      const schemaBefore = await readPublicSchemaFacts(database);

      await migrate(database);

      expect(await readMigrationJournal(database)).toEqual(journalBefore);
      expect(await readPublicSchemaFacts(database)).toEqual(schemaBefore);
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it("rejects a same-named index with the wrong structural definition", async () => {
    const legacyDatabaseName = `tether_e2e_wrong_index_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationPrefix(database, 5);
      await database.pool.query(`CREATE INDEX tasks_claim_expiry_idx ON tasks (created_at)`);

      await expect(migrate(database)).rejects.toMatchObject({
        name: "DatabaseMigrationError",
        reason: "unsupported_schema",
        recognizedPrefix: 5,
      });

      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      const laterApplicationDdl = await database.pool.query<{
        readonly exists: boolean;
      }>(`SELECT to_regclass('public.task_approvals') IS NOT NULL AS exists`);
      expect(journal.rows[0]?.count).toBe(0);
      expect(laterApplicationDdl.rows[0]?.exists).toBe(false);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("serializes concurrent migration starts against one legacy database", async () => {
    const legacyDatabaseName = `tether_e2e_concurrent_migration_${randomUUID().replaceAll("-", "_")}`;
    const firstDatabase = createPool(buildDatabaseUrl(legacyDatabaseName));
    const secondDatabase = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(firstDatabase);

      await withDiagnosticTimeout(
        Promise.all([migrate(firstDatabase), migrate(secondDatabase)]),
        5_000,
        "Concurrent migrations did not complete within 5 seconds",
      );

      const expectedMigrations = readMigrationFiles({
        migrationsFolder: "drizzle",
      });
      const journal = await firstDatabase.pool.query<{
        readonly createdAt: string;
        readonly hash: string;
      }>(
        `
          SELECT created_at::text AS "createdAt", hash
          FROM drizzle.__drizzle_migrations
          ORDER BY created_at, id
        `,
      );
      const duplicates = await firstDatabase.pool.query<{
        readonly count: number;
      }>(
        `
          SELECT count(*)::int AS count
          FROM (
            SELECT hash, created_at
            FROM drizzle.__drizzle_migrations
            GROUP BY hash, created_at
            HAVING count(*) > 1
          ) duplicate_journal_rows
        `,
      );

      expect(journal.rows).toEqual(
        expectedMigrations.map((migration) => ({
          createdAt: String(migration.folderMillis),
          hash: migration.hash,
        })),
      );
      expect(duplicates.rows[0]?.count).toBe(0);
    } finally {
      await Promise.allSettled([firstDatabase.end(), secondDatabase.end()]);
      await dropDatabase(legacyDatabaseName);
    }
  }, 15_000);

  it("serializes concurrent migration starts when each pool has one connection", async () => {
    const legacyDatabaseName = `tether_e2e_single_connection_migration_${randomUUID().replaceAll("-", "_")}`;
    const firstDatabase = createPool(buildDatabaseUrl(legacyDatabaseName), {
      max: 1,
    });
    const secondDatabase = createPool(buildDatabaseUrl(legacyDatabaseName), {
      max: 1,
    });
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(firstDatabase);

      await withDiagnosticTimeout(
        Promise.all([migrate(firstDatabase), migrate(secondDatabase)]),
        5_000,
        "Single-connection concurrent migrations did not complete within 5 seconds",
      );

      const expectedMigrations = readMigrationFiles({
        migrationsFolder: "drizzle",
      });
      const journal = await firstDatabase.pool.query<{
        readonly createdAt: string;
        readonly hash: string;
      }>(
        `
          SELECT created_at::text AS "createdAt", hash
          FROM drizzle.__drizzle_migrations
          ORDER BY created_at, id
        `,
      );

      expect(journal.rows).toEqual(
        expectedMigrations.map((migration) => ({
          createdAt: String(migration.folderMillis),
          hash: migration.hash,
        })),
      );
    } finally {
      await Promise.allSettled([firstDatabase.end(), secondDatabase.end()]);
      await dropDatabase(legacyDatabaseName);
    }
  }, 15_000);

  it("releases failed migration ownership without replacing the original cause", async () => {
    const legacyDatabaseName = `tether_e2e_failed_migration_cleanup_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName), {
      max: 1,
    });
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      let migrationFailed = false;
      const failingDatabase = wrapPoolQueries(database, async (query, values, next) => {
        const text = typeof query === "string" ? query : query.text;
        if (
          !migrationFailed &&
          /ALTER TABLE\s+"participant_control_leases"\s+ADD COLUMN\s+"superseded_at"/iu.test(text)
        ) {
          migrationFailed = true;
          throw new Error("injected migration execution failure");
        }
        if (migrationFailed && /pg_advisory_unlock/iu.test(text)) {
          throw new Error("injected advisory unlock failure");
        }
        return next(query, values);
      });

      await expect(migrate(failingDatabase)).rejects.toMatchObject({
        cause: expect.objectContaining({
          message: "injected migration execution failure",
        }),
      });
      await withDiagnosticTimeout(
        migrate(database),
        5_000,
        "Migration retry did not complete after ownership cleanup",
      );

      const expectedMigrations = readMigrationFiles({
        migrationsFolder: "drizzle",
      });
      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      expect(journal.rows[0]?.count).toBe(expectedMigrations.length);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  }, 15_000);

  it("applies only later migrations after a valid partial journal prefix", async () => {
    const legacyDatabaseName = `tether_e2e_partial_journal_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);

      await migrate(database);

      const expectedMigrations = readMigrationFiles({
        migrationsFolder: "drizzle",
      });
      const journal = await database.pool.query<{
        readonly createdAt: string;
        readonly hash: string;
      }>(
        `
          SELECT created_at::text AS "createdAt", hash
          FROM drizzle.__drizzle_migrations
          ORDER BY created_at, id
        `,
      );
      expect(journal.rows).toEqual(
        expectedMigrations.map((migration) => ({
          createdAt: String(migration.folderMillis),
          hash: migration.hash,
        })),
      );
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rejects duplicate journal rows before applying later migrations", async () => {
    const legacyDatabaseName = `tether_e2e_duplicate_journal_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);
      const firstMigration = readMigrationFiles({
        migrationsFolder: "drizzle",
      })[0];
      if (firstMigration === undefined) {
        throw new Error("Expected at least one generated migration");
      }
      await database.pool.query(
        `INSERT INTO drizzle.__drizzle_migrations (hash, created_at) VALUES ($1, $2)`,
        [firstMigration.hash, firstMigration.folderMillis],
      );

      await expectInvalidJournalBeforeApplicationDdl(database);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rejects gapped journal rows before applying later migrations", async () => {
    const legacyDatabaseName = `tether_e2e_gapped_journal_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);
      const migrations = readMigrationFiles({ migrationsFolder: "drizzle" });
      const omittedMigration = migrations[3];
      if (omittedMigration === undefined) {
        throw new Error("Expected generated migration 0003");
      }
      await database.pool.query(`DELETE FROM drizzle.__drizzle_migrations WHERE created_at = $1`, [
        omittedMigration.folderMillis,
      ]);

      await expectInvalidJournalBeforeApplicationDdl(database);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rejects reordered journal rows before applying later migrations", async () => {
    const legacyDatabaseName = `tether_e2e_reordered_journal_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);
      await database.pool.query(`
        UPDATE drizzle.__drizzle_migrations
        SET id = -id
        WHERE id IN (3, 4);

        UPDATE drizzle.__drizzle_migrations
        SET id = CASE id WHEN -3 THEN 4 WHEN -4 THEN 3 END
        WHERE id IN (-3, -4);
      `);

      await expectInvalidJournalBeforeApplicationDdl(database);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rejects a known journal timestamp with a mismatched hash", async () => {
    const legacyDatabaseName = `tether_e2e_hash_mismatch_journal_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);
      const migration = readMigrationFiles({ migrationsFolder: "drizzle" })[4];
      if (migration === undefined) {
        throw new Error("Expected generated migration 0004");
      }
      await database.pool.query(
        `UPDATE drizzle.__drizzle_migrations SET hash = $1 WHERE created_at = $2`,
        ["mismatched-known-migration-hash", migration.folderMillis],
      );

      await expectInvalidJournalBeforeApplicationDdl(database);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rejects a future unknown journal row before applying later migrations", async () => {
    const legacyDatabaseName = `tether_e2e_future_journal_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      await seedMigrationJournalPrefix(database, 8);
      const migrations = readMigrationFiles({ migrationsFolder: "drizzle" });
      const futureTimestamp =
        Math.max(...migrations.map((migration) => migration.folderMillis)) + 1;
      await database.pool.query(
        `INSERT INTO drizzle.__drizzle_migrations (hash, created_at) VALUES ($1, $2)`,
        ["unknown-future-migration-hash", futureTimestamp],
      );

      await expectInvalidJournalBeforeApplicationDdl(database);
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("rolls back failed legacy journal seeding and permits a clean retry", async () => {
    const legacyDatabaseName = `tether_e2e_seed_rollback_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(legacyDatabaseName), {
      max: 1,
    });
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0007(database);
      const queryTrace: string[] = [];
      let seedInsertCount = 0;
      const failingDatabase = wrapPoolQueries(database, async (query, values, next) => {
        const text = typeof query === "string" ? query : query.text;
        const normalized = text.trim().replaceAll(/\s+/gu, " ");
        if (
          normalized === "BEGIN" ||
          normalized === "COMMIT" ||
          normalized === "ROLLBACK" ||
          /INSERT INTO drizzle\.__drizzle_migrations/iu.test(normalized)
        ) {
          queryTrace.push(normalized);
        }
        if (/INSERT INTO drizzle\.__drizzle_migrations/iu.test(normalized)) {
          seedInsertCount += 1;
          if (seedInsertCount === 3) {
            throw new Error("injected legacy journal seed failure");
          }
        }
        return next(query, values);
      });

      await expect(migrate(failingDatabase)).rejects.toThrow(
        "injected legacy journal seed failure",
      );
      const rowsAfterFailure = await database.pool.query<{
        readonly count: number;
      }>(`SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`);
      expect(rowsAfterFailure.rows[0]?.count).toBe(0);
      expect(queryTrace[0]).toBe("BEGIN");
      expect(queryTrace.at(-1)).toBe("ROLLBACK");
      expect(queryTrace.filter((query) => /INSERT INTO/iu.test(query))).toHaveLength(3);
      expect(queryTrace).not.toContain("COMMIT");

      await withDiagnosticTimeout(
        migrate(database),
        5_000,
        "Migration retry did not complete after seed rollback",
      );
      const expectedMigrations = readMigrationFiles({
        migrationsFolder: "drizzle",
      });
      const journal = await database.pool.query<{
        readonly createdAt: string;
        readonly hash: string;
      }>(
        `
          SELECT created_at::text AS "createdAt", hash
          FROM drizzle.__drizzle_migrations
          ORDER BY id
        `,
      );
      expect(journal.rows).toEqual(
        expectedMigrations.map((migration) => ({
          createdAt: String(migration.folderMillis),
          hash: migration.hash,
        })),
      );
    } finally {
      await database.end();
      await dropDatabase(legacyDatabaseName);
    }
  }, 15_000);

  it("rejects non-contiguous legacy schemas before mutating application tables", async () => {
    const legacyDatabaseName = `tether_e2e_non_contiguous_${randomUUID().replaceAll("-", "_")}`;
    const legacyDatabase = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0003(legacyDatabase);
      await legacyDatabase.pool.query(`
        CREATE TABLE client_session_bindings (
          archived_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          external_id text NOT NULL,
          last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          provider text NOT NULL,
          session_id text NOT NULL,
          PRIMARY KEY (provider, external_id)
        )
      `);
      await legacyDatabase.pool.query(
        `CREATE INDEX tasks_claim_expiry_idx ON tasks (claim_expires_at)`,
      );

      await expect(migrate(legacyDatabase)).rejects.toMatchObject({
        expectedFacts: ["contiguous_migration_prefix=true"],
        journalHead: null,
        name: "DatabaseMigrationError",
        observedFacts: expect.arrayContaining([
          "migration_0005_represented=false",
          "migration_0006_represented=true",
        ]),
        reason: "unsupported_schema",
        recognizedPrefix: 4,
      });

      const mutatedColumns = await legacyDatabase.pool.query<{
        readonly count: number;
      }>(
        `
          SELECT count(*)::int AS count
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'tasks'
            AND column_name IN ('input', 'claim_expired_at', 'claim_expired_by', 'released_by')
        `,
      );
      const migrationRows = await legacyDatabase.pool.query<{
        readonly count: number;
      }>(`SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`);

      expect(mutatedColumns.rows[0]?.count).toBe(0);
      expect(migrationRows.rows[0]?.count).toBe(0);
    } finally {
      await legacyDatabase.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("requires REST auth while leaving health open", async () => {
    await expect(fetch(`${baseUrl}/health`)).resolves.toMatchObject({
      status: 200,
    });

    await expect(requestFrom(baseUrl, "/sessions", { authToken: null })).rejects.toThrow("401");
  });

  it("enforces REST role and session scope", async () => {
    const session = await createSession();
    const otherSession = await createSession();

    await expect(
      request(`/sessions/${session.sessionId}/tasks`, {
        authToken: mintE2eToken({
          participantId: "part_observer_denied",
          role: "observer",
          sessionId: session.sessionId,
        }),
        body: { kind: "text", objective: "observer should not create this" },
        method: "POST",
      }),
    ).rejects.toThrow("403");

    await expect(
      request(`/sessions/${session.sessionId}/events?after=0`, {
        authToken: mintE2eToken({
          participantId: "part_wrong_scope",
          role: "observer",
          sessionId: otherSession.sessionId,
        }),
      }),
    ).rejects.toThrow("403");
  });

  it("rejects oversized authenticated JSON bodies with a stable 413 response", async () => {
    const limitedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: { ...defaultResourceLimits, httpMaxBodyBytes: 32 },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await limitedApp.listen(port);
    const limitedUrl = `http://127.0.0.1:${port}`;
    try {
      const response = await requestStatusFrom(limitedUrl, "/sessions", {
        body: { sessionId: `sess_${"x".repeat(64)}` },
        method: "POST",
      });

      expect(response.status).toBe(413);
      expect(response.body).toMatchObject({
        error: "Payload Too Large",
        maxBytes: 32,
        reason: "body_too_large",
      });
    } finally {
      await limitedApp.close();
    }
  });

  it("enforces JSON body limits when auth is disabled", async () => {
    const disabledApp = createAppServer(currentPool(), {
      auth: {
        activeKid: testAuthSigningKid,
        mode: "disabled",
        secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      },
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: { ...defaultResourceLimits, httpMaxBodyBytes: 32 },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await disabledApp.listen(port);
    const disabledUrl = `http://127.0.0.1:${port}`;
    try {
      const response = await requestStatusFrom(disabledUrl, "/sessions", {
        authToken: null,
        body: { sessionId: `sess_${"x".repeat(64)}` },
        method: "POST",
      });

      expect(response.status).toBe(413);
      expect(response.body).toMatchObject({
        error: "Payload Too Large",
        maxBytes: 32,
        reason: "body_too_large",
      });
    } finally {
      await disabledApp.close();
    }
  });

  it("returns bounded event pages with pagination metadata", async () => {
    const limitedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: {
        ...defaultResourceLimits,
        eventListDefaultLimit: 2,
        eventListMaxLimit: 3,
      },
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await limitedApp.listen(port);
    const limitedUrl = `http://127.0.0.1:${port}`;
    try {
      const session = (
        await requestFrom<SessionResponse>(limitedUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      for (const index of [1, 2, 3, 4]) {
        await requestFrom(limitedUrl, `/sessions/${session.sessionId}/events`, {
          body: {
            payload: { index },
            producerId: "pagination-e2e",
            type: "user.message",
          },
          method: "POST",
        });
      }

      const defaultPage = await requestFrom<EventsResponse>(
        limitedUrl,
        `/sessions/${session.sessionId}/events?after=0`,
      );
      const clampedPage = await requestFrom<EventsResponse>(
        limitedUrl,
        `/sessions/${session.sessionId}/events?after=0&limit=999`,
      );

      expect(defaultPage.events).toHaveLength(2);
      expect(defaultPage.pagination).toMatchObject({
        afterSeq: 0,
        hasMore: true,
        limit: 2,
        nextAfterSeq: defaultPage.events.at(-1)?.seq,
        returned: 2,
      });
      expect(clampedPage.events).toHaveLength(3);
      expect(clampedPage.pagination).toMatchObject({
        hasMore: true,
        limit: 3,
        returned: 3,
      });
    } finally {
      await limitedApp.close();
    }
  });

  it("returns Host-presence inventory fields without duplicating public ensure events", async () => {
    const sessionId = `sess_host_presence_inventory_${randomUUID()}`;
    const firstEnsure = await request<SessionResponse>("/sessions", {
      body: { sessionId },
      method: "POST",
    });
    const secondEnsure = await request<SessionResponse>("/sessions", {
      body: { sessionId },
      method: "POST",
    });
    await request(`/sessions/${sessionId}/events`, {
      body: {
        payload: { text: "Build the Host-presence projection" },
        producerId: "viewer-user",
        type: "user.message",
      },
      method: "POST",
    });
    await request(`/sessions/${sessionId}/events`, {
      body: {
        payload: {
          branch: "main",
          cwd: "/workspace/tether/apps/tether",
          git: { clean: true },
          workspace: "/workspace/tether",
        },
        producerId: "host-runtime",
        type: "host.online",
      },
      method: "POST",
    });

    const events = await request<EventsResponse>(`/sessions/${sessionId}/events?after=0`);
    const inventory = await request<SessionListResponse>("/sessions");
    const session = inventory.sessions.find((candidate) => candidate.sessionId === sessionId);

    expect(firstEnsure.session.sessionId).toBe(sessionId);
    expect(secondEnsure.session.sessionId).toBe(sessionId);
    expect(events.events.map((event) => event.seq)).toEqual([1, 2]);
    expect(countSessionCreatedEvents(events.events)).toBe(0);
    expect(session).toMatchObject({
      activity: "idle",
      archived: false,
      branch: "main",
      cwd: "/workspace/tether/apps/tether",
      deleted: false,
      eventCount: 2,
      forkedFrom: null,
      git: { clean: true },
      host: "stale",
      project: "tether",
      sessionId,
      tangentOf: null,
      title: "Build the Host-presence projection",
      workspace: "/workspace/tether",
    });
    expect(session?.updatedAt).toEqual(expect.any(String));
  });

  it("permanently deletes only archived inactive Host-presence sessions", async () => {
    const session = await createSession();
    const deleteAuthToken = mintE2eToken({
      participantId: "part_delete_admin_e2e",
      role: "admin",
      sessionId: "*",
    });
    const notArchived = await requestStatus<PermanentDeleteResponse>(
      `/sessions/${session.sessionId}/delete`,
      { authToken: deleteAuthToken, method: "POST" },
    );
    await request(`/sessions/${session.sessionId}/events`, {
      body: {
        payload: { archived: true },
        producerId: "viewer-user",
        type: "session.archived",
      },
      method: "POST",
    });
    const deleted = await request<PermanentDeleteResponse>(
      `/sessions/${session.sessionId}/delete`,
      {
        authToken: deleteAuthToken,
        method: "POST",
      },
    );
    const inventory = await request<SessionListResponse>("/sessions");

    expect(notArchived.status).toBe(409);
    expect(notArchived.body).toMatchObject({
      ok: false,
      reason: "not-archived",
    });
    expect(deleted).toEqual({ ok: true, sessionId: session.sessionId });
    expect(inventory.sessions.some((candidate) => candidate.sessionId === session.sessionId)).toBe(
      false,
    );
  });

  it("applies allowlisted browser CORS headers and rejects denied origins", async () => {
    const corsApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      cors: { allowedOrigins: ["https://app.local"] },
      eventFanout: { catchUpPollIntervalMs: 0 },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await corsApp.listen(port);
    const corsUrl = `http://127.0.0.1:${port}`;
    try {
      const allowed = await fetch(`${corsUrl}/sessions`, {
        headers: {
          "access-control-request-headers": "authorization, content-type",
          "access-control-request-method": "GET",
          origin: "https://app.local",
        },
        method: "OPTIONS",
      });
      const denied = await fetch(`${corsUrl}/sessions`, {
        headers: {
          "access-control-request-method": "GET",
          origin: "https://evil.local",
        },
        method: "OPTIONS",
      });
      const get = await fetch(`${corsUrl}/sessions`, {
        headers: {
          authorization: `Bearer ${mintE2eToken({
            participantId: "part_cors_e2e",
            role: "admin",
            sessionId: "*",
          })}`,
          origin: "https://app.local",
        },
      });

      expect(allowed.status).toBe(204);
      expect(allowed.headers.get("access-control-allow-origin")).toBe("https://app.local");
      expect(allowed.headers.get("access-control-allow-credentials")).toBe("true");
      expect(allowed.headers.get("access-control-allow-methods")).toContain("POST");
      expect(denied.status).toBe(403);
      expect(get.status).toBe(200);
      expect(get.headers.get("access-control-allow-origin")).toBe("https://app.local");
    } finally {
      await corsApp.close();
    }
  });

  it("requires WebSocket access tokens and binds identity to the token", async () => {
    const session = await createSession();
    const otherSession = await createSession();
    const anonymousSocket = new WebSocket(
      `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
    );
    await expect(waitForSocketCloseCode(anonymousSocket)).resolves.toBe(1008);

    const wrongScopeUrl = new URL(
      `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
    );
    wrongScopeUrl.searchParams.set(
      "access_token",
      mintE2eToken({
        participantId: "part_ws_wrong_scope",
        role: "observer",
        sessionId: otherSession.sessionId,
      }),
    );
    const wrongScopeSocket = new WebSocket(wrongScopeUrl);
    await expect(waitForSocketCloseCode(wrongScopeSocket)).resolves.toBe(1008);

    const tokenParticipantId = "part_ws_token_identity";
    const identityMismatchUrl = new URL(
      `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=part_ws_attacker&instanceId=inst_ws_attacker&runtimeKind=codex`,
    );
    identityMismatchUrl.searchParams.set(
      "access_token",
      mintE2eToken({
        participantId: tokenParticipantId,
        role: "participant",
        sessionId: session.sessionId,
      }),
    );
    const identityMismatchSocket = new WebSocket(identityMismatchUrl);
    await expect(waitForSocketCloseCode(identityMismatchSocket)).resolves.toBe(1008);

    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=${tokenParticipantId}&instanceId=inst_ws_token_identity`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));
    socket.close();
    await waitForSocketClose(socket);

    const participants = await request<ParticipantsResponse>(
      `/sessions/${session.sessionId}/participants`,
    );
    expect(participants.participants).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          participantId: tokenParticipantId,
          runtimeKind: "generic_agent",
        }),
      ]),
    );
  });

  it("decodes WebSocket session ids consistently and keeps Host-presence streams passive", async () => {
    const sessionId = `sess_encoded/${randomUUID()}`;
    await request<SessionResponse>("/sessions", {
      body: { sessionId },
      method: "POST",
    });
    await request(`/sessions/${encodeURIComponent(sessionId)}/events`, {
      body: {
        payload: { text: "encoded session event" },
        producerId: "viewer-user",
        type: "user.message",
      },
      method: "POST",
    });
    const viewer = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${encodeURIComponent(
          sessionId,
        )}/stream?after=0&runtimeKind=viewer`,
      ),
    );
    const host = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${encodeURIComponent(
          sessionId,
        )}/stream?after=0&runtimeKind=host&participantId=part_host_presence&instanceId=inst_host_presence&displayName=Host%20Presence`,
      ),
    );
    const viewerMessages: unknown[] = [];
    const hostMessages: unknown[] = [];
    viewer.on("message", (data) => {
      viewerMessages.push(JSON.parse(String(data)) as unknown);
    });
    host.on("message", (data) => {
      hostMessages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(viewer);
    await waitForSocketOpen(host);
    await waitFor(() =>
      viewerMessages.some(
        (message) => isEventEnvelope(message) && message.event.type === "user.message",
      ),
    );
    await waitFor(() =>
      viewerMessages.some(
        (message) =>
          isPresenceEnvelope(message) &&
          message.hosts.some((presence) => presence.instanceId === "inst_host_presence"),
      ),
    );

    const eventsWhileConnected = await request<EventsResponse>(
      `/sessions/${encodeURIComponent(sessionId)}/events?after=0`,
    );
    const participantsWhileConnected = await request<ParticipantsResponse>(
      `/sessions/${encodeURIComponent(sessionId)}/participants`,
    );
    const debugWhileConnected = await request<ServerDebugResponse>("/debug/server");

    expect(eventsWhileConnected.events.map((event) => event.type)).toEqual(["user.message"]);
    expect(participantsWhileConnected.participants).toEqual([]);
    expect(debugWhileConnected.server.hostPresence.passiveSocketCount).toBe(2);
    expect(
      debugWhileConnected.server.hostPresence.nativeParticipantControlSocketCount,
    ).toBeGreaterThanOrEqual(0);

    host.close();
    await waitForSocketClose(host);
    await waitFor(() =>
      viewerMessages.some((message) => isPresenceEnvelope(message) && message.hosts.length === 0),
    );
    viewer.close();
    await waitForSocketClose(viewer);
    expect(hostMessages.some(isReplayCompleteEnvelope)).toBe(true);
  });

  it("streams full events to passive observers without acquiring a control lease", async () => {
    const sessionId = `sess_observer_${randomUUID()}`;
    await request<SessionResponse>("/sessions", {
      body: { sessionId },
      method: "POST",
    });
    await request(`/sessions/${sessionId}/events`, {
      body: {
        payload: { text: "replayed observer event" },
        producerId: "observer-user",
        type: "user.message",
      },
      method: "POST",
    });

    // Two observers share one token identity. A control participant would
    // collide on the second connection; passive observers hold no lease, so
    // both stream the full history and follow live events without conflict.
    const observerUrl = authenticatedWebSocketUrl(
      `${baseUrl.replace("http:", "ws:")}/sessions/${sessionId}/stream?after=0&runtimeKind=observer`,
    );
    const first = new WebSocket(observerUrl);
    const second = new WebSocket(observerUrl);
    const firstMessages: unknown[] = [];
    const secondMessages: unknown[] = [];
    first.on("message", (data) => firstMessages.push(JSON.parse(String(data)) as unknown));
    second.on("message", (data) => secondMessages.push(JSON.parse(String(data)) as unknown));
    await waitForSocketOpen(first);
    await waitForSocketOpen(second);
    await waitFor(
      () =>
        firstMessages.some(isReplayCompleteEnvelope) &&
        secondMessages.some(isReplayCompleteEnvelope),
    );

    await request(`/sessions/${sessionId}/events`, {
      body: {
        payload: { text: "live observer event" },
        producerId: "observer-user",
        type: "user.message",
      },
      method: "POST",
    });
    await waitFor(
      () =>
        firstMessages.filter(isEventEnvelope).length >= 2 &&
        secondMessages.filter(isEventEnvelope).length >= 2,
    );

    const leaseRows = await currentPool().pool.query<{
      readonly count: number;
    }>(
      `
        SELECT count(*)::int AS count
        FROM participant_control_leases
        WHERE session_id = $1
      `,
      [sessionId],
    );
    const participants = await request<ParticipantsResponse>(`/sessions/${sessionId}/participants`);

    // No control lease row, no durable participant, and no read-only rejection
    // or presence frame: the observer is a pure passive full-event reader.
    expect(leaseRows.rows[0]?.count).toBe(0);
    expect(participants.participants).toEqual([]);
    expect(firstMessages.some(isErrorEnvelope)).toBe(false);
    expect(secondMessages.some(isErrorEnvelope)).toBe(false);
    expect(firstMessages.some(isPresenceEnvelope)).toBe(false);
    expect(firstMessages.filter(isEventEnvelope).map((message) => message.event.seq)).toEqual([
      1, 2,
    ]);

    // A control command over a passive observer is rejected without touching the
    // control path.
    first.send(
      JSON.stringify({
        op: "task.claim",
        requestId: "req_observer",
        taskId: "task_x",
      }),
    );
    await waitFor(() =>
      firstMessages.some((message) => isErrorEnvelope(message) && /read-only/u.test(message.error)),
    );

    first.close();
    second.close();
    await waitForSocketClose(first);
    await waitForSocketClose(second);
  });

  it("closes oversized WebSocket frames before app command handling", async () => {
    const limitedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: { ...defaultResourceLimits, wsMaxPayloadBytes: 64 },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await limitedApp.listen(port);
    const limitedUrl = `http://127.0.0.1:${port}`;
    let socket: WebSocket | null = null;
    try {
      const session = (
        await requestFrom<SessionResponse>(limitedUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      socket = new WebSocket(
        authenticatedWebSocketUrl(
          `${limitedUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
        ),
      );
      const messages: unknown[] = [];
      socket.on("message", (data) => {
        messages.push(JSON.parse(String(data)) as unknown);
      });
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));

      const closeCode = waitForSocketCloseCode(socket);
      socket.send(
        JSON.stringify({
          op: webSocketOperation.publish,
          payload: { text: "x".repeat(256) },
          producerId: "oversized-ws-e2e",
          type: sessionEventType.userMessage,
        }),
      );

      await expect(closeCode).resolves.toBe(1009);
      const events = await requestFrom<EventsResponse>(
        limitedUrl,
        `/sessions/${session.sessionId}/events?after=0`,
      );
      expect(events.events.some((event) => event.producerId === "oversized-ws-e2e")).toBe(false);
    } finally {
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      await limitedApp.close();
    }
  });

  it("rate-limits WebSocket client messages per connection", async () => {
    const limitedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: {
        ...defaultResourceLimits,
        wsMessageRateLimit: 1,
        wsMessageRateWindowMs: 10_000,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await limitedApp.listen(port);
    const limitedUrl = `http://127.0.0.1:${port}`;
    let socket: WebSocket | null = null;
    try {
      const session = (
        await requestFrom<SessionResponse>(limitedUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      socket = new WebSocket(
        authenticatedWebSocketUrl(
          `${limitedUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
        ),
      );
      const messages: unknown[] = [];
      socket.on("message", (data) => {
        messages.push(JSON.parse(String(data)) as unknown);
      });
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));

      const closeCode = waitForSocketCloseCode(socket);
      socket.send(JSON.stringify({ op: "unsupported.one" }));
      socket.send(JSON.stringify({ op: "unsupported.two" }));

      await waitFor(() =>
        messages.some(
          (message) =>
            typeof message === "object" &&
            message !== null &&
            "reason" in message &&
            message.reason === "rate_limited",
        ),
      );
      await expect(closeCode).resolves.toBe(1008);
    } finally {
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      await limitedApp.close();
    }
  });

  it("rejects stale WebSocket replay windows without sending full history", async () => {
    const limitedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: { ...defaultResourceLimits, wsReplayMaxEvents: 1 },
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await limitedApp.listen(port);
    const limitedUrl = `http://127.0.0.1:${port}`;
    let socket: WebSocket | null = null;
    try {
      const session = (
        await requestFrom<SessionResponse>(limitedUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      await requestFrom(limitedUrl, `/sessions/${session.sessionId}/events`, {
        body: {
          payload: { text: "second event" },
          producerId: "replay-window-e2e",
          type: sessionEventType.userMessage,
        },
        method: "POST",
      });
      await requestFrom(limitedUrl, `/sessions/${session.sessionId}/events`, {
        body: {
          payload: { text: "third event" },
          producerId: "replay-window-e2e",
          type: sessionEventType.userMessage,
        },
        method: "POST",
      });
      socket = new WebSocket(
        authenticatedWebSocketUrl(
          `${limitedUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
        ),
      );
      const messages: unknown[] = [];
      socket.on("message", (data) => {
        messages.push(JSON.parse(String(data)) as unknown);
      });

      await expect(waitForSocketCloseCode(socket)).resolves.toBe(1013);
      expect(messages.filter(isEventEnvelope)).toHaveLength(0);
      expect(messages).toContainEqual(
        expect.objectContaining({
          op: "error",
          reason: "replay_window_exceeded",
        }),
      );
    } finally {
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      await limitedApp.close();
    }
  });

  it("rejects reserved producers and server-owned event types through public REST publish", async () => {
    const session = await createSession();
    const reservedEventId = `evt_rest_reserved_${randomUUID()}`;
    const reserved = await requestStatus(`/sessions/${session.sessionId}/events`, {
      authToken: mintE2eToken({
        participantId: systemProducerId,
        role: "participant",
        sessionId: session.sessionId,
      }),
      body: {
        eventId: reservedEventId,
        payload: { text: "forged system producer" },
        producerId: systemProducerId,
        type: sessionEventType.userMessage,
      },
      method: "POST",
    });

    expect(reserved.status).toBe(403);
    expect(reserved.body).toMatchObject({
      error: "Forbidden",
      reason: clientPublishDenyReason.ReservedProducer,
    });

    const deniedEventIds = [reservedEventId];
    for (const type of [
      sessionEventType.approvalRecorded,
      sessionEventType.controlCancel,
      sessionEventType.taskCompleted,
    ] as const) {
      const eventId = `evt_rest_server_type_${type}_${randomUUID()}`;
      deniedEventIds.push(eventId);
      const denied = await requestStatus(`/sessions/${session.sessionId}/events`, {
        body: {
          eventId,
          payload: { text: "forged lifecycle event" },
          producerId: `part_rest_policy_${type}`,
          type,
        },
        method: "POST",
      });
      expect(denied.status).toBe(403);
      expect(denied.body).toMatchObject({
        error: "Forbidden",
        reason: clientPublishDenyReason.ServerEventType,
      });
    }

    for (const type of [
      sessionEventType.agentOutput,
      sessionEventType.taskProgress,
      sessionEventType.userMessage,
      "custom.client.event",
    ] as const) {
      await request(`/sessions/${session.sessionId}/events`, {
        body: {
          eventId: `evt_rest_allowed_${type}_${randomUUID()}`,
          payload: { text: `allowed ${type}` },
          producerId: `part_rest_allowed_${type}`,
          type,
        },
        method: "POST",
      });
    }

    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    for (const deniedEventId of deniedEventIds) {
      expect(events.events.map((event) => event.eventId)).not.toContain(deniedEventId);
    }
    expect(events.events.map((event) => event.type)).toEqual(
      expect.arrayContaining([
        sessionEventType.agentOutput,
        sessionEventType.taskProgress,
        sessionEventType.userMessage,
        "custom.client.event",
      ]),
    );
  });

  it("replays REST publish retries for caller-supplied event ids", async () => {
    const session = await createSession();
    const eventId = `evt_rest_retry_${randomUUID()}`;
    const body = {
      eventId,
      payload: { text: "retryable publish" },
      producerId: "part_rest_retry",
      type: sessionEventType.userMessage,
    };

    const created = await requestStatus<PublishedEventResponse>(
      `/sessions/${session.sessionId}/events`,
      { body, method: "POST" },
    );
    const replayed = await requestStatus<PublishedEventResponse>(
      `/sessions/${session.sessionId}/events`,
      { body, method: "POST" },
    );

    expect(created.status).toBe(201);
    expect(created.body.status).toBe("created");
    expect(created.body.event.eventId).toBe(eventId);
    expect(replayed.status).toBe(200);
    expect(replayed.body.status).toBe("replayed");
    expect(replayed.body.event).toMatchObject({
      eventId,
      payload: body.payload,
      producerId: body.producerId,
      seq: created.body.event.seq,
      sessionId: session.sessionId,
      type: body.type,
    });

    const conflict = await requestStatus(`/sessions/${session.sessionId}/events`, {
      body: { ...body, payload: { text: "different publish" } },
      method: "POST",
    });
    expect(conflict.status).toBe(409);
    expect(conflict.body).toMatchObject({
      error: "Event id conflict",
      eventId,
      reason: "event_id_conflict",
    });
    expect(JSON.stringify(conflict.body)).not.toContain("retryable publish");

    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    expect(events.events.filter((event) => event.eventId === eventId)).toHaveLength(1);

    const concurrentEventId = `evt_rest_concurrent_${randomUUID()}`;
    const concurrentBody = {
      ...body,
      eventId: concurrentEventId,
      payload: { text: "concurrent retryable publish" },
    };
    const concurrentResults = await Promise.all([
      requestStatus<PublishedEventResponse>(`/sessions/${session.sessionId}/events`, {
        body: concurrentBody,
        method: "POST",
      }),
      requestStatus<PublishedEventResponse>(`/sessions/${session.sessionId}/events`, {
        body: concurrentBody,
        method: "POST",
      }),
    ]);
    expect(concurrentResults.map((result) => result.status).sort()).toEqual([200, 201]);
    expect(concurrentResults.map((result) => result.body.status).sort()).toEqual([
      "created",
      "replayed",
    ]);
    const eventsAfterConcurrent = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    expect(
      eventsAfterConcurrent.events.filter((event) => event.eventId === concurrentEventId),
    ).toHaveLength(1);
  });

  it("appends and lists event sequences above the Postgres int4 range", async () => {
    const session = await createSession();
    await currentPool().pool.query(
      `
        UPDATE session_event_sequences
        SET next_seq = $1
        WHERE session_id = $2
      `,
      [2_147_483_648, session.sessionId],
    );

    const created = await request<PublishedEventResponse>(`/sessions/${session.sessionId}/events`, {
      body: {
        eventId: `evt_post_int4_${randomUUID()}`,
        payload: { text: "post int4 publish" },
        producerId: "part_post_int4",
        type: sessionEventType.userMessage,
      },
      method: "POST",
    });
    const events = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=2147483647`,
    );

    expect(created.event.seq).toBe(2_147_483_648);
    expect(events.events.map((event) => event.seq)).toContain(2_147_483_648);
    expect(events.pagination.afterSeq).toBe(2_147_483_647);
  });

  it("rejects event sequence allocation beyond the safe numeric cursor cutoff", async () => {
    const session = await createSession();
    const eventId = `evt_sequence_overflow_${randomUUID()}`;
    await currentPool().pool.query(
      `
        UPDATE session_event_sequences
        SET next_seq = $1::bigint
        WHERE session_id = $2
      `,
      [(Number.MAX_SAFE_INTEGER + 1).toString(), session.sessionId],
    );

    const overflow = await requestStatus(`/sessions/${session.sessionId}/events`, {
      body: {
        eventId,
        payload: { text: "unsafe sequence" },
        producerId: "part_sequence_overflow",
        type: sessionEventType.userMessage,
      },
      method: "POST",
    });
    const persisted = await currentPool().pool.query<{
      readonly count: number;
    }>(
      `
        SELECT count(*)::int AS count
        FROM session_events
        WHERE session_id = $1
          AND event_id = $2
      `,
      [session.sessionId, eventId],
    );

    expect(overflow.status).toBe(500);
    expect(overflow.text).toContain(session.sessionId);
    expect(overflow.text).toContain((Number.MAX_SAFE_INTEGER + 1).toString());
    expect(persisted.rows[0]?.count).toBe(0);
  });

  it("replays REST task create retries for caller-supplied task ids", async () => {
    const session = await createSession();
    const taskId = `task_rest_retry_${randomUUID()}`;
    const body = {
      input: { priority: "high" },
      kind: "software_dev",
      objective: "Retry exactly once",
      taskId,
    };

    const created = await requestStatus<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body,
      method: "POST",
    });
    const replayed = await requestStatus<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body,
      method: "POST",
    });

    expect(created.status).toBe(201);
    expect(created.body.status).toBe("created");
    expect(created.body.task.taskId).toBe(taskId);
    expect(replayed.status).toBe(200);
    expect(replayed.body.status).toBe("replayed");
    expect(replayed.body.task).toEqual(created.body.task);

    const conflict = await requestStatus(`/sessions/${session.sessionId}/tasks`, {
      body: { ...body, objective: "Different work" },
      method: "POST",
    });
    expect(conflict.status).toBe(409);
    expect(conflict.body).toMatchObject({
      error: "Task id conflict",
      reason: "task_id_conflict",
      taskId,
    });
    expect(conflict.body.conflictingFields).toEqual(["objective"]);
    expect(JSON.stringify(conflict.body)).not.toContain("Retry exactly once");
    expect(JSON.stringify(conflict.body)).not.toContain("Different work");

    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    expect(
      events.events.filter(
        (event) =>
          event.type === sessionEventType.taskCreated && taskIdFromEventPayload(event) === taskId,
      ),
    ).toHaveLength(1);
  });

  it("summarizes idempotent replay and conflicts without route errors or raw content", async () => {
    const serviceLogs: StructuredLogEntry[] = [];
    const routeErrors: string[] = [];
    const observedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      httpRouteErrors: {
        logger: {
          error: (event) => {
            routeErrors.push(event);
          },
        },
      },
      sessionService: {
        controlEpochEnforcement: false,
        observability: {
          boundaryLogsEnabled: true,
          logger: { log: (entry) => serviceLogs.push(entry) },
          moduleName: "SessionService",
        },
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await findOpenPort();
    await observedApp.listen(port);
    const observedUrl = `http://127.0.0.1:${port}`;
    try {
      const session = (
        await requestFrom<SessionResponse>(observedUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      const eventBody = {
        eventId: `evt_observed_retry_${randomUUID()}`,
        payload: { text: "observability secret event content" },
        producerId: "part_observed_retry",
        type: sessionEventType.userMessage,
      };
      await requestStatusFrom<PublishedEventResponse>(
        observedUrl,
        `/sessions/${session.sessionId}/events`,
        { body: eventBody, method: "POST" },
      );
      await requestStatusFrom<PublishedEventResponse>(
        observedUrl,
        `/sessions/${session.sessionId}/events`,
        { body: eventBody, method: "POST" },
      );
      await requestStatusFrom(observedUrl, `/sessions/${session.sessionId}/events`, {
        body: {
          ...eventBody,
          payload: { text: "different secret event content" },
        },
        method: "POST",
      });

      const taskBody = {
        input: { secret: "observability secret task input" },
        kind: "software_dev",
        objective: "Observed retry",
        taskId: `task_observed_retry_${randomUUID()}`,
      };
      await requestStatusFrom<TaskResponse>(observedUrl, `/sessions/${session.sessionId}/tasks`, {
        body: taskBody,
        method: "POST",
      });
      await requestStatusFrom<TaskResponse>(observedUrl, `/sessions/${session.sessionId}/tasks`, {
        body: taskBody,
        method: "POST",
      });
      await requestStatusFrom(observedUrl, `/sessions/${session.sessionId}/tasks`, {
        body: {
          ...taskBody,
          input: { secret: "different secret task input" },
        },
        method: "POST",
      });
    } finally {
      await observedApp.close();
    }

    expect(routeErrors).toEqual([]);
    expect(serviceLogs).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          eventCount: 0,
          replayReason: "event_id_replay",
          status: "replayed",
        }),
        message: "boundary.exit",
        operation: "publishRestEvent",
      }),
    );
    expect(serviceLogs).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          conflictReason: "event_id_conflict",
          eventCount: 0,
          status: "conflict",
        }),
        message: "boundary.exit",
        operation: "publishRestEvent",
      }),
    );
    expect(serviceLogs).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          eventCount: 0,
          replayReason: "task_id_replay",
          status: "replayed",
        }),
        message: "boundary.exit",
        operation: "createTask",
      }),
    );
    expect(serviceLogs).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          conflictReason: "task_id_conflict",
          eventCount: 0,
          status: "conflict",
        }),
        message: "boundary.exit",
        operation: "createTask",
      }),
    );
    const encodedLogs = JSON.stringify(serviceLogs);
    expect(encodedLogs).not.toContain("observability secret event content");
    expect(encodedLogs).not.toContain("observability secret task input");
  });

  it("rejects reserved producers and server-owned event types through public WebSocket publish", async () => {
    const session = await createSession();
    const reservedSocket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=${systemProducerId}&instanceId=inst_ws_reserved_policy&runtimeKind=codex`,
      ),
    );
    const reservedMessages: unknown[] = [];
    reservedSocket.on("message", (data) => {
      reservedMessages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(reservedSocket);
    await waitFor(() => reservedMessages.some(isReplayCompleteEnvelope));

    const reservedRequestId = `req_ws_reserved_${randomUUID()}`;
    const reservedEventId = `evt_ws_reserved_${randomUUID()}`;
    reservedSocket.send(
      JSON.stringify({
        eventId: reservedEventId,
        op: webSocketOperation.publish,
        payload: { text: "forged system producer" },
        producerId: systemProducerId,
        requestId: reservedRequestId,
        type: sessionEventType.userMessage,
      }),
    );
    await waitFor(() =>
      reservedMessages.some((message) =>
        isErrorEnvelopeWithReason(
          message,
          reservedRequestId,
          clientPublishDenyReason.ReservedProducer,
        ),
      ),
    );
    reservedSocket.close();
    await waitForSocketClose(reservedSocket);

    const producerId = "part_ws_publish_policy";
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=${producerId}&instanceId=inst_ws_publish_policy&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));

    const deniedEventIds = [reservedEventId];
    for (const type of [
      sessionEventType.approvalRecorded,
      sessionEventType.controlCancel,
      sessionEventType.taskCompleted,
    ] as const) {
      const eventId = `evt_ws_server_type_${type}_${randomUUID()}`;
      deniedEventIds.push(eventId);
      const requestId = `req_ws_server_type_${type}_${randomUUID()}`;
      socket.send(
        JSON.stringify({
          eventId,
          op: webSocketOperation.publish,
          payload: { text: "forged lifecycle event" },
          producerId,
          requestId,
          type,
        }),
      );
      await waitFor(() =>
        messages.some((message) =>
          isErrorEnvelopeWithReason(message, requestId, clientPublishDenyReason.ServerEventType),
        ),
      );
    }

    const allowedEventId = `evt_ws_allowed_${randomUUID()}`;
    const allowedRequestId = `req_ws_allowed_${randomUUID()}`;
    socket.send(
      JSON.stringify({
        eventId: allowedEventId,
        op: webSocketOperation.publish,
        payload: { text: "allowed after denials" },
        producerId,
        requestId: allowedRequestId,
        type: sessionEventType.userMessage,
      }),
    );
    await waitFor(() =>
      messages.some(
        (message) =>
          isCommandResultEnvelope(message) &&
          message.requestId === allowedRequestId &&
          message.event?.eventId === allowedEventId,
      ),
    );
    socket.close();
    await waitForSocketClose(socket);

    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    for (const deniedEventId of deniedEventIds) {
      expect(events.events.map((event) => event.eventId)).not.toContain(deniedEventId);
    }
    expect(events.events).toContainEqual(
      expect.objectContaining({
        eventId: allowedEventId,
        producerId,
        type: sessionEventType.userMessage,
      }),
    );
  });

  it("returns correlated errors for invalid WebSocket command bodies", async () => {
    const session = await createSession();
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=part_ws_command_parse&instanceId=inst_ws_command_parse&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));

    const requestId = `req_ws_command_parse_${randomUUID()}`;
    socket.send(
      JSON.stringify({
        op: webSocketOperation.taskClaim,
        requestId,
      }),
    );
    await waitFor(() =>
      messages.some(
        (message) =>
          isErrorEnvelope(message) &&
          message.requestId === requestId &&
          message.command === webSocketOperation.taskClaim,
      ),
    );

    socket.close();
    await waitForSocketClose(socket);
  });

  it("returns correlated errors for WebSocket command service failures", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "text", objective: "Trigger correlated claim failure" },
      method: "POST",
    });
    const failingApp = await createFailingClaimAppServer("Injected claim failure");
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${failingApp.baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=part_ws_command_service&instanceId=inst_ws_command_service&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });

    try {
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));
      const requestId = `req_ws_command_service_${randomUUID()}`;
      socket.send(
        JSON.stringify({
          op: webSocketOperation.taskClaim,
          requestId,
          taskId: task.task.taskId,
        }),
      );

      await waitFor(() =>
        messages.some(
          (message) =>
            isErrorEnvelope(message) &&
            message.requestId === requestId &&
            message.command === webSocketOperation.taskClaim &&
            message.taskId === task.task.taskId,
        ),
      );
    } finally {
      if (socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      await failingApp.app.close();
    }
  });

  it("exits once-mode runtime after a correlated WebSocket command claim failure", async () => {
    const session = await createSession();
    await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "text", objective: "Do not wedge once-mode runtime" },
      method: "POST",
    });
    const failingApp = await createFailingClaimAppServer("Injected runtime claim failure");
    const client = await ParticipantRuntimeClient.connect({
      afterSeq: 0,
      authToken: mintE2eToken({
        participantId: "part_ws_command_once_runtime",
        role: "participant",
        sessionId: session.sessionId,
      }),
      capabilities: { workKinds: ["text"] },
      commandTimeoutMs: 500,
      displayName: "Command Once Runtime E2E",
      instanceId: "inst_ws_command_once_runtime",
      participantId: "part_ws_command_once_runtime",
      runtimeKind: "codex",
      serviceUrl: failingApp.baseUrl,
      sessionId: session.sessionId,
    });

    try {
      await Promise.race([
        client.runClaimableTasks({
          claimRefreshMs: 50,
          executor: async () => ({ result: { shouldNotRun: true } }),
          once: true,
          shouldClaimTask: (task) => task.kind === "text",
        }),
        sleep(1_000).then(() => {
          throw new Error("Once-mode runtime did not exit after correlated claim failure");
        }),
      ]);
      expect(client.debugInfo().pendingCommandCount).toBe(0);
    } finally {
      client.close();
      await client.waitForClose().catch(() => undefined);
      await failingApp.app.close();
    }
  });

  it("replays WebSocket publish retries for caller-supplied event ids", async () => {
    const session = await createSession();
    const producerId = "part_ws_retry";
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=${producerId}&instanceId=inst_ws_retry&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));

    const eventId = `evt_ws_retry_${randomUUID()}`;
    const firstRequestId = `req_ws_retry_first_${randomUUID()}`;
    const retryRequestId = `req_ws_retry_second_${randomUUID()}`;
    const publishBody = {
      eventId,
      op: webSocketOperation.publish,
      payload: { text: "retryable websocket publish" },
      producerId,
      type: sessionEventType.userMessage,
    };

    socket.send(JSON.stringify({ ...publishBody, requestId: firstRequestId }));
    await waitFor(() =>
      messages.some(
        (message) =>
          isCommandResultEnvelope(message) &&
          message.requestId === firstRequestId &&
          message.status === "created" &&
          message.event?.eventId === eventId,
      ),
    );
    const created = messages.find(
      (message) => isCommandResultEnvelope(message) && message.requestId === firstRequestId,
    );
    expect(created).toMatchObject({
      event: { eventId },
      requestId: firstRequestId,
      status: "created",
    });
    const createdSeq = isRecord(created) && isRecord(created.event) ? created.event.seq : null;

    socket.send(JSON.stringify({ ...publishBody, requestId: retryRequestId }));
    await waitFor(() =>
      messages.some(
        (message) =>
          isCommandResultEnvelope(message) &&
          message.requestId === retryRequestId &&
          message.status === "replayed" &&
          message.event?.eventId === eventId,
      ),
    );
    expect(
      messages.find(
        (message) => isCommandResultEnvelope(message) && message.requestId === retryRequestId,
      ),
    ).toMatchObject({
      event: { eventId, seq: createdSeq },
      requestId: retryRequestId,
      status: "replayed",
    });

    const conflictRequestId = `req_ws_retry_conflict_${randomUUID()}`;
    socket.send(
      JSON.stringify({
        ...publishBody,
        payload: { text: "different websocket publish" },
        requestId: conflictRequestId,
      }),
    );
    await waitFor(() =>
      messages.some((message) =>
        isErrorEnvelopeWithReason(message, conflictRequestId, "event_id_conflict"),
      ),
    );

    socket.close();
    await waitForSocketClose(socket);

    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    expect(events.events.filter((event) => event.eventId === eventId)).toHaveLength(1);
  });

  it("keeps public publish producer and type denials active when auth is disabled", async () => {
    const disabledApp = createAppServer(currentPool(), {
      auth: {
        activeKid: testAuthSigningKid,
        mode: "disabled",
        secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      },
    });
    const port = await findOpenPort();
    await disabledApp.listen(port);
    const disabledUrl = `http://127.0.0.1:${port}`;
    try {
      const sessionResponse = await requestStatusFrom<SessionResponse>(disabledUrl, "/sessions", {
        authToken: null,
        body: {},
        method: "POST",
      });
      const session = sessionResponse.body.session;
      const reserved = await requestStatusFrom(
        disabledUrl,
        `/sessions/${session.sessionId}/events`,
        {
          authToken: null,
          body: {
            payload: { text: "forged disabled system producer" },
            producerId: systemProducerId,
            type: sessionEventType.userMessage,
          },
          method: "POST",
        },
      );
      const serverType = await requestStatusFrom(
        disabledUrl,
        `/sessions/${session.sessionId}/events`,
        {
          authToken: null,
          body: {
            payload: { text: "forged disabled lifecycle event" },
            producerId: "part_disabled_publish_policy",
            type: sessionEventType.taskCompleted,
          },
          method: "POST",
        },
      );

      expect(reserved.status).toBe(403);
      expect(reserved.body).toMatchObject({
        error: "Forbidden",
        reason: clientPublishDenyReason.ReservedProducer,
      });
      expect(serverType.status).toBe(403);
      expect(serverType.body).toMatchObject({
        error: "Forbidden",
        reason: clientPublishDenyReason.ServerEventType,
      });
    } finally {
      await disabledApp.close();
    }
  });

  it("persists client session bindings for external conversations", async () => {
    const first = await request<ClientSessionBindingResponse>("/client-bindings/session", {
      body: {
        externalId: "external-chat-1",
        provider: "external-chat",
      },
      method: "POST",
    });
    const second = await request<ClientSessionBindingResponse>("/client-bindings/session", {
      body: {
        externalId: "external-chat-1",
        provider: "external-chat",
      },
      method: "POST",
    });
    const bindings = await request<ClientBindingsResponse>(
      "/client-bindings?provider=external-chat",
    );
    const archived = await request<ArchivedClientSessionBindingResponse>(
      "/client-bindings/external-chat/external-chat-1",
      {
        method: "DELETE",
      },
    );
    const afterArchive = await request<ClientBindingsResponse>(
      "/client-bindings?provider=external-chat",
    );

    expect(first.created).toBe(true);
    expect(first.binding).toMatchObject({
      externalId: "external-chat-1",
      provider: "external-chat",
      sessionId: first.session.sessionId,
    });
    expect(second.created).toBe(false);
    expect(second.session.sessionId).toBe(first.session.sessionId);
    expect(bindings.bindings).toContainEqual(
      expect.objectContaining({
        externalId: "external-chat-1",
        provider: "external-chat",
        sessionId: first.session.sessionId,
      }),
    );
    expect(archived.binding.sessionId).toBe(first.session.sessionId);
    expect(afterArchive.bindings).not.toContainEqual(
      expect.objectContaining({
        externalId: "external-chat-1",
        provider: "external-chat",
      }),
    );
  });

  it("rebounds archived client bindings to a newly selected session", async () => {
    const externalId = `external-rebound-${randomUUID()}`;
    const before = await request<SessionListResponse>("/sessions");
    const first = await requestStatus<ClientSessionBindingResponse>("/client-bindings/session", {
      body: { externalId, provider: "external-chat" },
      method: "POST",
    });
    await request<ArchivedClientSessionBindingResponse>(
      `/client-bindings/external-chat/${encodeURIComponent(externalId)}`,
      { method: "DELETE" },
    );
    const rebound = await requestStatus<ClientSessionBindingResponse>("/client-bindings/session", {
      body: { externalId, provider: "external-chat" },
      method: "POST",
    });
    const bindings = await request<ClientBindingsResponse>(
      "/client-bindings?provider=external-chat",
    );
    const sessions = await request<SessionListResponse>("/sessions");
    const oldEvents = await request<EventsResponse>(
      `/sessions/${first.body.session.sessionId}/events?after=0`,
    );
    const reboundEvents = await request<EventsResponse>(
      `/sessions/${rebound.body.session.sessionId}/events?after=0`,
    );
    const previousSessionIds = new Set(before.sessions.map((session) => session.sessionId));
    const addedSessionIds = sessions.sessions
      .map((session) => session.sessionId)
      .filter((sessionId) => !previousSessionIds.has(sessionId))
      .sort();

    expect(first.status).toBe(201);
    expect(first.body.created).toBe(true);
    expect(rebound.status).toBe(200);
    expect(rebound.body.created).toBe(false);
    expect(rebound.body.session.sessionId).not.toBe(first.body.session.sessionId);
    expect(rebound.body.binding).toMatchObject({
      externalId,
      provider: "external-chat",
      sessionId: rebound.body.session.sessionId,
    });
    expect(bindings.bindings).toContainEqual(
      expect.objectContaining({
        externalId,
        provider: "external-chat",
        sessionId: rebound.body.session.sessionId,
      }),
    );
    expect(addedSessionIds).toEqual(
      [first.body.session.sessionId, rebound.body.session.sessionId].sort(),
    );
    expect(countSessionCreatedEvents(oldEvents.events)).toBe(1);
    expect(countSessionCreatedEvents(reboundEvents.events)).toBe(1);
  });

  it("keeps client binding session.created emission idempotent for explicit sessions", async () => {
    const absentExternalId = `external-explicit-${randomUUID()}`;
    const explicitSession = await createSession();
    const absent = await requestStatus<ClientSessionBindingResponse>("/client-bindings/session", {
      body: {
        externalId: absentExternalId,
        provider: "external-chat",
        sessionId: explicitSession.sessionId,
      },
      method: "POST",
    });
    const absentEvents = await request<EventsResponse>(
      `/sessions/${explicitSession.sessionId}/events?after=0`,
    );

    const reboundExternalId = `external-explicit-rebound-${randomUUID()}`;
    await request<ClientSessionBindingResponse>("/client-bindings/session", {
      body: { externalId: reboundExternalId, provider: "external-chat" },
      method: "POST",
    });
    await request<ArchivedClientSessionBindingResponse>(
      `/client-bindings/external-chat/${encodeURIComponent(reboundExternalId)}`,
      { method: "DELETE" },
    );
    const reboundSession = await createSession();
    const rebound = await requestStatus<ClientSessionBindingResponse>("/client-bindings/session", {
      body: {
        externalId: reboundExternalId,
        provider: "external-chat",
        sessionId: reboundSession.sessionId,
      },
      method: "POST",
    });
    const reboundEvents = await request<EventsResponse>(
      `/sessions/${reboundSession.sessionId}/events?after=0`,
    );

    expect(absent.status).toBe(201);
    expect(absent.body.created).toBe(true);
    expect(absent.body.session.sessionId).toBe(explicitSession.sessionId);
    expect(countSessionCreatedEvents(absentEvents.events)).toBe(0);
    expect(rebound.status).toBe(200);
    expect(rebound.body.created).toBe(false);
    expect(rebound.body.session.sessionId).toBe(reboundSession.sessionId);
    expect(countSessionCreatedEvents(reboundEvents.events)).toBe(0);
  });

  it("classifies archived binding rebound inside the persistence mutation", async () => {
    const externalId = `external-store-rebound-${randomUUID()}`;
    const explicitSessionId = `sess_explicit_${randomUUID()}`;
    const inserted = await upsertClientSessionBinding(currentPool(), {
      externalId,
      provider: "external-chat",
    });
    await archiveClientSessionBinding(currentPool(), {
      externalId,
      provider: "external-chat",
    });
    await currentPool().pool.query(
      `
        UPDATE client_session_bindings
        SET last_seen_at = '2000-01-01T00:00:00Z'::timestamptz
        WHERE provider = $1
          AND external_id = $2
      `,
      ["external-chat", externalId],
    );
    await createDbSession(currentPool(), explicitSessionId);
    const rebound = await upsertClientSessionBinding(currentPool(), {
      externalId,
      provider: "external-chat",
      sessionId: explicitSessionId,
    });

    expect(inserted.created).toBe(true);
    expect(inserted.status).toBe("inserted");
    expect(rebound.created).toBe(false);
    expect(rebound.status).toBe("rebound");
    expect(rebound.binding.archivedAt).toBeNull();
    expect(rebound.binding.sessionId).toBe(explicitSessionId);
    expect(new Date(rebound.binding.lastSeenAt).getTime()).toBeGreaterThan(
      Date.parse("2000-01-01T00:00:00Z"),
    );
  });

  it("serializes concurrent first-time client binding resolves", async () => {
    const externalId = `external-concurrent-${randomUUID()}`;
    const before = await request<SessionListResponse>("/sessions");
    const results = await Promise.all([
      requestStatus<ClientSessionBindingResponse>("/client-bindings/session", {
        body: { externalId, provider: "external-chat" },
        method: "POST",
      }),
      requestStatus<ClientSessionBindingResponse>("/client-bindings/session", {
        body: { externalId, provider: "external-chat" },
        method: "POST",
      }),
    ]);
    const selectedSessionIds = new Set(results.map((result) => result.body.session.sessionId));
    expect(selectedSessionIds.size).toBe(1);
    const selectedSessionId = results[0]?.body.session.sessionId;
    if (selectedSessionId === undefined) {
      throw new Error("Concurrent resolve did not return a selected session");
    }
    const sessions = await request<SessionListResponse>("/sessions");
    const selectedEvents = await request<EventsResponse>(
      `/sessions/${selectedSessionId}/events?after=0`,
    );
    const previousSessionIds = new Set(before.sessions.map((session) => session.sessionId));
    const addedSessionIds = sessions.sessions
      .map((session) => session.sessionId)
      .filter((sessionId) => !previousSessionIds.has(sessionId));

    expect(results.filter((result) => result.status === 201)).toHaveLength(1);
    expect(results.filter((result) => result.body.created)).toHaveLength(1);
    expect(addedSessionIds).toEqual([selectedSessionId]);
    expect(countSessionCreatedEvents(selectedEvents.events)).toBe(1);
  });

  it("lists sessions with aggregate activity counts and bindings", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["text"] },
        displayName: "Session List Participant",
        instanceId: "inst_session_list",
        participantId: "part_session_list",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "text", objective: "Appear in the session list" },
      method: "POST",
    });
    const externalId = `session-list-${randomUUID()}`;
    const bound = await request<ClientSessionBindingResponse>("/client-bindings/session", {
      body: { externalId, provider: "external-chat" },
      method: "POST",
    });

    const { sessions } = await request<SessionListResponse>("/sessions");
    const listed = sessions.find((entry) => entry.sessionId === session.sessionId);
    const listedBound = sessions.find((entry) => entry.sessionId === bound.session.sessionId);

    expect(listed?.participantCount).toBeGreaterThanOrEqual(1);
    expect(listed?.taskCount).toBe(1);
    expect(listed?.activeTaskCount).toBe(1);
    expect(listed?.eventCount).toBeGreaterThanOrEqual(1);
    expect(listed?.lastEventAt).not.toBeNull();
    expect(listedBound?.bindings).toContainEqual({
      externalId,
      provider: "external-chat",
    });
  });

  it("runs the REST task lifecycle", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["software_dev"] },
        displayName: "Codex E2E",
        instanceId: "inst_codex_e2e",
        participantId: "part_codex_e2e",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Complete the e2e task" },
      method: "POST",
    });

    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: { instanceId: "inst_codex_e2e", participantId: "part_codex_e2e" },
      method: "POST",
    });
    const completion = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          instanceId: "inst_codex_e2e",
          participantId: "part_codex_e2e",
          result: { summary: "done" },
        },
        method: "POST",
      },
    );

    expect(completion.task.completedAt).not.toBeNull();
    expect(completion.task.result).toEqual({ summary: "done" });
  });

  it("classifies concurrent same participant REST retries once", async () => {
    const session = await createSession();
    const participantId = `part_registration_race_${randomUUID()}`;
    const body = {
      capabilities: { workKinds: ["software_dev"] },
      displayName: "Registration Race",
      instanceId: `inst_registration_race_${randomUUID()}`,
      participantId,
      runtimeKind: "codex",
    };

    const registrations = await Promise.all([
      request<ParticipantRegistrationResponse>(`/sessions/${session.sessionId}/participants`, {
        body,
        method: "POST",
      }),
      request<ParticipantRegistrationResponse>(`/sessions/${session.sessionId}/participants`, {
        body,
        method: "POST",
      }),
    ]);
    const refresh = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      { body, method: "POST" },
    );
    const update = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      {
        body: {
          ...body,
          capabilities: { workKinds: ["software_dev"], updated: true },
          displayName: "Registration Race Updated",
        },
        method: "POST",
      },
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const joinedEvents = participantEvents(events.events, "participant.joined", participantId);
    const updatedEvents = participantEvents(events.events, "participant.updated", participantId);
    const participants = await request<ParticipantsResponse>(
      `/sessions/${session.sessionId}/participants`,
    );
    const snapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));
    socket.close();
    await waitForSocketClose(socket);

    expect(registrations.map((registration) => registration.registrationStatus).sort()).toEqual([
      "joined",
      "refreshed",
    ]);
    expect(refresh.registrationStatus).toBe("refreshed");
    expect(update.registrationStatus).toBe("updated");
    expect(joinedEvents).toHaveLength(1);
    expect(updatedEvents).toHaveLength(1);
    expect(updatedEvents[0]?.payload.previousParticipant).toMatchObject({
      displayName: "Registration Race",
      participantId,
    });
    expect(
      participants.participants.filter(
        (participant) => participant.participantId === participantId,
      ),
    ).toHaveLength(1);
    expect(
      snapshots.participants.filter((snapshot) => snapshot.participantId === participantId),
    ).toHaveLength(1);
    expect(
      messages
        .filter(isEventEnvelope)
        .map((message) => message.event.type)
        .filter((type) => type === "participant.joined" || type === "participant.updated"),
    ).toEqual(["participant.joined", "participant.updated"]);
  });

  it("serializes store participant registration classification", async () => {
    const sessionId = `sess_store_registration_race_${randomUUID()}`;
    const participantId = `part_store_registration_race_${randomUUID()}`;
    const participantAdvisoryLockQuery =
      "SELECT pg_advisory_xact_lock(hashtext($1::text), hashtext($2::text))";
    const coordinator = createPostgresConcurrencyCoordinator(currentPool(), {
      actors: ["registration-a", "registration-b"],
      barrierTimeoutMs: 2_000,
      phases: [
        {
          actors: ["registration-a", "registration-b"],
          name: "participant-advisory-lock-ready",
          position: "before",
          query: { class: "participant-advisory-lock", text: participantAdvisoryLockQuery },
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 2_000, statementTimeoutMs: 5_000 },
    });
    const baseRegistration = {
      capabilities: {
        contracts: [{ taskKind: "software_dev" }],
        workKinds: ["software_dev"],
      },
      displayName: "Store Registration Race",
      participantId,
      runtimeKind: "codex",
      sessionId,
    };

    await createDbSession(currentPool(), sessionId);
    const registrations = await coordinator.run(async ({ databaseFor }) =>
      Promise.all([
        upsertParticipant(databaseFor("registration-a"), baseRegistration),
        upsertParticipant(databaseFor("registration-b"), baseRegistration),
      ]),
    );
    const visibleUpdate = await upsertParticipant(currentPool(), {
      ...baseRegistration,
      capabilities: { workKinds: ["software_dev"], z: true, a: true },
      displayName: "Store Registration Updated",
    });
    const refreshed = await upsertParticipant(currentPool(), {
      ...baseRegistration,
      capabilities: { a: true, workKinds: ["software_dev"], z: true },
      displayName: "Store Registration Updated",
    });

    expect(registrations.filter((registration) => registration.status === "joined")).toHaveLength(
      1,
    );
    expect(
      registrations.filter((registration) => registration.status === "refreshed"),
    ).toHaveLength(1);
    expect(visibleUpdate.status).toBe("updated");
    if (visibleUpdate.status !== "updated") {
      throw new Error("Visible participant update did not return the previous snapshot");
    }
    expect(visibleUpdate.previousParticipant.displayName).toBe("Store Registration Race");
    expect(refreshed.status).toBe("refreshed");
    expect(refreshed.participant.joinedAt).toBe(visibleUpdate.participant.joinedAt);
    expect(Date.parse(refreshed.participant.lastSeenAt)).toBeGreaterThanOrEqual(
      Date.parse(visibleUpdate.participant.lastSeenAt),
    );
  });

  it("coordinates exact named query phases on independent actor clients", async () => {
    const probeQuery = "SELECT pg_backend_pid()::int AS pid";
    const database = createPool(databaseUrl);
    onTestFinished(async () => database.end());
    const coordinator = createPostgresConcurrencyCoordinator(database, {
      actors: ["probe-a", "probe-b"],
      barrierTimeoutMs: 2_000,
      phases: [
        {
          actors: ["probe-a", "probe-b"],
          name: "probe-ready",
          position: "before",
          query: { class: "backend-probe", text: probeQuery },
        },
        {
          actors: ["probe-a", "probe-b"],
          name: "probe-finished",
          position: "after",
          query: { class: "backend-probe", text: probeQuery },
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 1_000, statementTimeoutMs: 3_000 },
    });

    const pids = await coordinator.run(async ({ databaseFor }) =>
      Promise.all(
        (["probe-a", "probe-b"] as const).map(async (actor) => {
          const client = await databaseFor(actor).pool.connect();
          try {
            await client.query("BEGIN");
            await client.query(`${probeQuery} `);
            const result = await client.query<{ readonly pid: number }>(probeQuery);
            await client.query("COMMIT");
            return result.rows[0]?.pid;
          } finally {
            client.release();
          }
        }),
      ),
    );

    expect(new Set(pids).size).toBe(2);
    const phaseEvents = coordinator.snapshot().phaseEvents;
    expect(phaseEvents).toHaveLength(4);
    expect(phaseEvents).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ actor: "probe-a", name: "probe-ready", position: "before" }),
        expect.objectContaining({ actor: "probe-b", name: "probe-ready", position: "before" }),
        expect.objectContaining({ actor: "probe-a", name: "probe-finished", position: "after" }),
        expect.objectContaining({ actor: "probe-b", name: "probe-finished", position: "after" }),
      ]),
    );
  });

  it("cancels lock waiters and releases every actor after an assertion failure", async () => {
    const advisoryLockQuery = "SELECT pg_advisory_xact_lock(hashtext($1::text))";
    const secretLockValue = "secret-lock-value-plan-34";
    const database = createPool(databaseUrl);
    onTestFinished(async () => database.end());
    const checkedOutBefore = database.pool.totalCount - database.pool.idleCount;
    const coordinator = createPostgresConcurrencyCoordinator(database, {
      actors: ["lock-holder", "lock-waiter"],
      barrierTimeoutMs: 2_000,
      phases: [
        {
          actors: ["lock-waiter"],
          name: "waiting-for-advisory-lock",
          position: "before",
          query: { class: "advisory-lock", text: advisoryLockQuery },
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 5_000 },
    });
    let blockedQuery: Promise<unknown> | null = null;

    await expect(
      coordinator.run(async ({ databaseFor, waitForLockWait }) => {
        const holder = await databaseFor("lock-holder").pool.connect();
        const waiter = await databaseFor("lock-waiter").pool.connect();
        try {
          await holder.query("BEGIN");
          await waiter.query("BEGIN");
          const [lockTimeout, statementTimeout] = await Promise.all([
            waiter.query<{ readonly lockTimeout: string }>(
              `SELECT current_setting('lock_timeout') AS "lockTimeout"`,
            ),
            waiter.query<{ readonly statementTimeout: string }>(
              `SELECT current_setting('statement_timeout') AS "statementTimeout"`,
            ),
          ]);
          expect(lockTimeout.rows[0]?.lockTimeout).toBe("5s");
          expect(statementTimeout.rows[0]?.statementTimeout).toBe("5s");
          await holder.query(advisoryLockQuery, [secretLockValue]);
          blockedQuery = waiter
            .query(advisoryLockQuery, [secretLockValue])
            .catch((error: unknown) => error);

          const lockWait = await waitForLockWait("lock-waiter");
          expect(lockWait.blocked).toBe(true);
          throw new Error("injected assertion failure");
        } finally {
          holder.release();
          waiter.release();
        }
      }),
    ).rejects.toThrow("injected assertion failure");
    await blockedQuery;

    const snapshot = coordinator.snapshot();
    const waiter = snapshot.actors.find((actor) => actor.actor === "lock-waiter");
    expect(waiter).toMatchObject({
      actor: "lock-waiter",
      phase: {
        name: "waiting-for-advisory-lock",
        position: "before",
        queryClass: "advisory-lock",
      },
    });
    expect(waiter?.backendPid).toEqual(expect.any(Number));
    expect(waiter?.lockWait).toMatchObject({ blocked: true, waitEventType: "Lock" });
    expect(snapshot.cleanup).toMatchObject({
      blockedActorsAfterRollback: 0,
      cancelledActors: 2,
      checkedOutAfter: checkedOutBefore,
      releasedActors: 2,
      rolledBackActors: 2,
    });
    expect(JSON.stringify(snapshot)).not.toContain(secretLockValue);
    expect(database.pool.totalCount - database.pool.idleCount).toBe(checkedOutBefore);
    expect(database.pool.waitingCount).toBe(0);
    const actorPids = snapshot.actors.map((actor) => actor.backendPid);
    const blockedActors = await database.pool.query<{ readonly count: number }>(
      `
        SELECT count(*)::int AS count
        FROM pg_stat_activity
        WHERE pid = ANY($1::int[])
          AND wait_event_type = 'Lock'
      `,
      [actorPids],
    );
    expect(blockedActors.rows[0]?.count).toBe(0);
  });

  it("reports bounded redacted diagnostics and cleans up after a barrier timeout", async () => {
    const probeQuery = "SELECT $1::text AS value";
    const secretValue = "secret-timeout-value-plan-34";
    const database = createPool(databaseUrl);
    onTestFinished(async () => database.end());
    const checkedOutBefore = database.pool.totalCount - database.pool.idleCount;
    const coordinator = createPostgresConcurrencyCoordinator(database, {
      actors: ["timeout-a", "timeout-b"],
      barrierTimeoutMs: 100,
      phases: [
        {
          actors: ["timeout-a", "timeout-b"],
          name: "both-probes-ready",
          position: "before",
          query: { class: "timeout-probe", text: probeQuery },
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 1_000, statementTimeoutMs: 2_000 },
    });

    const failure = await coordinator
      .run(async ({ databaseFor }) => {
        const client = await databaseFor("timeout-a").pool.connect();
        try {
          await client.query("BEGIN");
          await client.query(probeQuery, [secretValue]);
        } finally {
          client.release();
        }
      })
      .catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(Error);
    const message = failure instanceof Error ? failure.message : String(failure);
    expect(message).toContain("PostgreSQL concurrency barrier timed out");
    expect(message).toContain('"actor":"timeout-a"');
    expect(message).toContain('"name":"both-probes-ready"');
    expect(message).toContain('"position":"before"');
    expect(message).toContain('"queryClass":"timeout-probe"');
    expect(message).toContain('"backendPid":');
    expect(message).toContain('"lockWait":');
    expect(message).not.toContain(secretValue);
    expect(coordinator.snapshot().cleanup).toMatchObject({
      blockedActorsAfterRollback: 0,
      cancelledActors: 2,
      checkedOutAfter: checkedOutBefore,
      releasedActors: 2,
      rolledBackActors: 2,
    });
    expect(database.pool.totalCount - database.pool.idleCount).toBe(checkedOutBefore);
    expect(database.pool.waitingCount).toBe(0);
  });

  it("reports cleanup failures after attempting every actor release", async () => {
    const database = createPool(databaseUrl);
    onTestFinished(async () => database.end());
    const coordinator = createPostgresConcurrencyCoordinator(database, {
      actors: ["terminated-actor", "terminator"],
      barrierTimeoutMs: 1_000,
      phases: [],
      transactionTimeouts: { lockTimeoutMs: 1_000, statementTimeoutMs: 2_000 },
    });

    const failure = await coordinator
      .run(async ({ databaseFor }) => {
        const terminatedActor = await databaseFor("terminated-actor").pool.connect();
        const terminator = await databaseFor("terminator").pool.connect();
        try {
          await terminatedActor.query("BEGIN");
          await terminator.query("BEGIN");
          const pidResult = await terminatedActor.query<{ readonly pid: number }>(
            "SELECT pg_backend_pid()::int AS pid",
          );
          const terminatedPid = pidResult.rows[0]?.pid;
          if (terminatedPid === undefined) {
            throw new Error("Missing terminated actor backend PID");
          }
          await terminator.query("SELECT pg_terminate_backend($1)", [terminatedPid]);
          await terminatedActor.query("SELECT 1").catch(() => undefined);
        } finally {
          terminatedActor.release();
          terminator.release();
        }
      })
      .catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(PostgresConcurrencyCleanupError);
    const snapshot = coordinator.snapshot();
    expect(snapshot.cleanup?.failures).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ actor: "terminated-actor", operation: "rollback" }),
      ]),
    );
    expect(snapshot.cleanup).toMatchObject({
      blockedActorsAfterRollback: 0,
      checkedOutAfter: 0,
      releaseAttempts: 2,
      rollbackAttempts: 2,
    });
    expect(snapshot.cleanup?.releasedActors).toBeGreaterThanOrEqual(1);
    expect(database.pool.totalCount - database.pool.idleCount).toBe(0);
    expect(database.pool.waitingCount).toBe(0);
  });

  it("commits an epoch-N REST claim refresh before replacement installs epoch N+1", async () => {
    const { claimed, controlEpoch, instanceId, participantId, session, task } =
      await prepareControlEpochRaceFixture("mutation_first");
    const replacementAcquisitionId = `acq_epoch_n_plus_one_${randomUUID()}`;
    const coordinator = createPostgresConcurrencyCoordinator(currentPool(), {
      actors: ["mutation", "supersession"],
      barrierTimeoutMs: 5_000,
      phases: [
        {
          actors: ["mutation"],
          name: "epoch-n-fence-held",
          position: "after",
          query: { class: "current-control-lease-fence", text: currentControlLeaseFenceQuery },
          release: "manual",
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
    });
    const result = await coordinator.run(
      async ({ databaseFor, releasePhase, waitForLockWait, waitForPhase }) => {
        const mutationApp = createAppServer(databaseFor("mutation"), {
          auth: e2eAuthOptions,
          eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
          sessionService: {
            controlEpochEnforcement: true,
            taskClaimLeaseTtlMs: 120_000,
            wsControlLeaseTtlMs: 60_000,
          },
          taskClaimSweeper: { intervalMs: 0 },
        });
        const supersessionApp = createAppServer(databaseFor("supersession"), {
          auth: e2eAuthOptions,
          eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
          sessionService: {
            controlEpochEnforcement: true,
            taskClaimLeaseTtlMs: 60_000,
            wsControlLeaseTtlMs: 60_000,
          },
          taskClaimSweeper: { intervalMs: 0 },
        });
        const [mutationPort, supersessionPort] = await Promise.all([
          findOpenPort(),
          findOpenPort(),
        ]);
        await Promise.all([
          mutationApp.listen(mutationPort),
          supersessionApp.listen(supersessionPort),
        ]);
        const mutationBaseUrl = `http://127.0.0.1:${mutationPort}`;
        const supersessionBaseUrl = `http://127.0.0.1:${supersessionPort}`;

        try {
          const mutation = requestStatusFrom<TaskResponse>(
            mutationBaseUrl,
            `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim/refresh`,
            {
              body: { controlEpoch, instanceId, participantId },
              method: "POST",
            },
          );
          await waitForPhase("epoch-n-fence-held");
          const supersession = requestStatusFrom<ParticipantRegistrationResponse>(
            supersessionBaseUrl,
            `/sessions/${session.sessionId}/participants`,
            {
              body: {
                acquisitionId: replacementAcquisitionId,
                controlChannel: "rest",
                displayName: "Epoch mutation-first participant",
                instanceId,
                participantId,
                runtimeKind: "generic_agent",
              },
              method: "POST",
            },
          );
          const lockWait = await waitForLockWait("supersession");
          releasePhase("epoch-n-fence-held");
          const [mutationResponse, supersessionResponse] = await Promise.all([
            mutation,
            supersession,
          ]);
          return { lockWait, mutationResponse, supersessionResponse };
        } finally {
          await Promise.all([mutationApp.close(), supersessionApp.close()]);
        }
      },
    );
    const durableTask = await getTask(currentPool(), {
      sessionId: session.sessionId,
      taskId: task.task.taskId,
    });
    const currentEpochRows = await currentPool().pool.query<{ readonly epoch: unknown }>(
      `
        SELECT epoch
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND released_at IS NULL
          AND superseded_at IS NULL
      `,
      [session.sessionId, participantId],
    );

    expect(result.lockWait).toMatchObject({ blocked: true, waitEventType: "Lock" });
    expect(result.mutationResponse.status).toBe(200);
    expect(result.mutationResponse.body.task.claimExpiresAt).not.toBe(claimed.task.claimExpiresAt);
    expect(durableTask?.claimExpiresAt).toBe(result.mutationResponse.body.task.claimExpiresAt);
    expect(result.supersessionResponse.status).toBe(201);
    expect(result.supersessionResponse.body).toMatchObject({
      acquisitionStatus: "superseded",
      controlEpoch: controlEpoch + 1,
    });
    expect(Number(currentEpochRows.rows[0]?.epoch)).toBe(controlEpoch + 1);
  }, 30_000);

  it("rejects an epoch-N REST claim refresh after replacement installs epoch N+1", async () => {
    const { claimed, controlEpoch, instanceId, participantId, session, task } =
      await prepareControlEpochRaceFixture("supersession_first");
    const replacementAcquisitionId = `acq_epoch_n_plus_one_${randomUUID()}`;
    const coordinator = createPostgresConcurrencyCoordinator(currentPool(), {
      actors: ["mutation", "supersession"],
      barrierTimeoutMs: 5_000,
      phases: [
        {
          actors: ["mutation"],
          name: "epoch-n-before-fence",
          position: "before",
          query: { class: "current-control-lease-fence", text: currentControlLeaseFenceQuery },
          release: "manual",
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
    });

    const result = await coordinator.run(async ({ databaseFor, releasePhase, waitForPhase }) => {
      const mutationApp = createControlEpochRaceApp(databaseFor("mutation"), 120_000);
      const supersessionApp = createControlEpochRaceApp(databaseFor("supersession"), 60_000);
      const [mutationPort, supersessionPort] = await Promise.all([findOpenPort(), findOpenPort()]);
      await Promise.all([
        mutationApp.listen(mutationPort),
        supersessionApp.listen(supersessionPort),
      ]);
      const mutationBaseUrl = `http://127.0.0.1:${mutationPort}`;
      const supersessionBaseUrl = `http://127.0.0.1:${supersessionPort}`;

      try {
        const mutation = requestStatusFrom<TaskResponse>(
          mutationBaseUrl,
          `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim/refresh`,
          {
            body: { controlEpoch, instanceId, participantId },
            method: "POST",
          },
        );
        await waitForPhase("epoch-n-before-fence");
        const supersessionResponse = await requestStatusFrom<ParticipantRegistrationResponse>(
          supersessionBaseUrl,
          `/sessions/${session.sessionId}/participants`,
          {
            body: {
              acquisitionId: replacementAcquisitionId,
              controlChannel: "rest",
              displayName: "Epoch supersession-first participant",
              instanceId,
              participantId,
              runtimeKind: "generic_agent",
            },
            method: "POST",
          },
        );
        const currentBeforeStaleMutation = await currentPool().pool.query<{
          readonly claimExpiresAt: Date | null;
          readonly epoch: unknown;
        }>(
          `
              SELECT
                lease.epoch,
                task.claim_expires_at AS "claimExpiresAt"
              FROM participant_control_leases AS lease
              CROSS JOIN tasks AS task
              WHERE lease.session_id = $1
                AND lease.participant_id = $2
                AND lease.released_at IS NULL
                AND lease.superseded_at IS NULL
                AND task.session_id = $1
                AND task.task_id = $3
            `,
          [session.sessionId, participantId, task.task.taskId],
        );
        releasePhase("epoch-n-before-fence");
        return {
          currentBeforeStaleMutation: currentBeforeStaleMutation.rows[0],
          mutationResponse: await mutation,
          supersessionResponse,
        };
      } finally {
        await Promise.all([mutationApp.close(), supersessionApp.close()]);
      }
    });
    const durableTask = await getTask(currentPool(), {
      sessionId: session.sessionId,
      taskId: task.task.taskId,
    });
    const currentEpochRows = await currentPool().pool.query<{ readonly epoch: unknown }>(
      `
        SELECT epoch
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND released_at IS NULL
          AND superseded_at IS NULL
      `,
      [session.sessionId, participantId],
    );

    expect(result.supersessionResponse.status).toBe(201);
    expect(result.supersessionResponse.body).toMatchObject({
      acquisitionStatus: "superseded",
      controlEpoch: controlEpoch + 1,
    });
    expect(Number(result.currentBeforeStaleMutation?.epoch)).toBe(controlEpoch + 1);
    expect(result.currentBeforeStaleMutation?.claimExpiresAt?.toISOString()).toBe(
      claimed.task.claimExpiresAt,
    );
    expect(durableTask?.claimExpiresAt).toBe(claimed.task.claimExpiresAt);
    expect(result.mutationResponse.status).toBe(409);
    expect(result.mutationResponse.body).toMatchObject({
      code: "CONTROL_EPOCH_STALE",
      currentEpoch: controlEpoch + 1,
    });
    expect(Number(currentEpochRows.rows[0]?.epoch)).toBe(controlEpoch + 1);
    expect(coordinator.snapshot().phaseEvents).toEqual([
      expect.objectContaining({
        actor: "mutation",
        name: "epoch-n-before-fence",
        position: "before",
        queryClass: "current-control-lease-fence",
      }),
    ]);
  }, 30_000);

  it("allows exactly one of two synchronized REST claimants to claim one task", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Claim this task exactly once" },
      method: "POST",
    });
    const claimants = [
      {
        actor: "claimant-a",
        displayName: "Synchronized claimant A",
        instanceId: `inst_claimant_a_${randomUUID()}`,
        participantId: `part_claimant_a_${randomUUID()}`,
      },
      {
        actor: "claimant-b",
        displayName: "Synchronized claimant B",
        instanceId: `inst_claimant_b_${randomUUID()}`,
        participantId: `part_claimant_b_${randomUUID()}`,
      },
    ] as const;
    await Promise.all(
      claimants.map((claimant) =>
        request(`/sessions/${session.sessionId}/participants`, {
          body: {
            capabilities: { workKinds: ["software_dev"] },
            displayName: claimant.displayName,
            instanceId: claimant.instanceId,
            participantId: claimant.participantId,
            runtimeKind: "codex",
          },
          method: "POST",
        }),
      ),
    );
    const coordinator = createPostgresConcurrencyCoordinator(currentPool(), {
      actors: ["claimant-a", "claimant-b"],
      barrierTimeoutMs: 5_000,
      phases: [
        {
          actors: ["claimant-a", "claimant-b"],
          name: "both-task-claim-transactions-open",
          position: "after",
          query: { class: "task-claim-transaction-start", text: "BEGIN" },
          release: "manual",
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
    });

    const responses = await coordinator.run(async ({ databaseFor, releasePhase, waitForPhase }) => {
      const claimantAApp = createTaskClaimRaceApp(databaseFor("claimant-a"));
      const claimantBApp = createTaskClaimRaceApp(databaseFor("claimant-b"));
      const [claimantAPort, claimantBPort] = await Promise.all([findOpenPort(), findOpenPort()]);
      let claimantAStarted = false;
      let claimantBStarted = false;
      try {
        await claimantAApp.listen(claimantAPort);
        claimantAStarted = true;
        await claimantBApp.listen(claimantBPort);
        claimantBStarted = true;
        const requests = [
          requestStatusFrom(
            `http://127.0.0.1:${claimantAPort}`,
            `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
            {
              body: {
                instanceId: claimants[0].instanceId,
                participantId: claimants[0].participantId,
              },
              method: "POST",
            },
          ),
          requestStatusFrom(
            `http://127.0.0.1:${claimantBPort}`,
            `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
            {
              body: {
                instanceId: claimants[1].instanceId,
                participantId: claimants[1].participantId,
              },
              method: "POST",
            },
          ),
        ] as const;
        try {
          await waitForPhase("both-task-claim-transactions-open");
          releasePhase("both-task-claim-transactions-open");
          return await Promise.all(requests);
        } catch (error) {
          await Promise.allSettled(requests);
          throw error;
        }
      } finally {
        await Promise.all([
          ...(claimantAStarted ? [claimantAApp.close()] : []),
          ...(claimantBStarted ? [claimantBApp.close()] : []),
        ]);
      }
    });
    const durableClaims = await currentPool().pool.query<{
      readonly claimExpiresAt: Date;
      readonly claimedAt: Date;
      readonly claimedBy: string;
    }>(
      `
        SELECT
          claim_expires_at AS "claimExpiresAt",
          claimed_at AS "claimedAt",
          claimed_by AS "claimedBy"
        FROM tasks
        WHERE session_id = $1
          AND task_id = $2
          AND claimed_at IS NOT NULL
          AND claimed_by IS NOT NULL
          AND claim_expires_at > now()
          AND completed_at IS NULL
          AND failed_at IS NULL
          AND cancelled_at IS NULL
      `,
      [session.sessionId, task.task.taskId],
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const claimedEvents = events.events.filter(
      (event) =>
        event.type === "task.claimed" && taskIdFromEventPayload(event) === task.task.taskId,
    );
    const winningClaimant = claimants.find((_claimant, index) => responses[index]?.status === 200);
    const claimedEvent = claimedEvents[0];

    expect(responses.map((response) => response.status).sort()).toEqual([200, 409]);
    expect(responses.find((response) => response.status === 409)?.body).toEqual({
      error: "Task is already claimed or terminal",
    });
    expect(winningClaimant).toBeDefined();
    expect(durableClaims.rows).toHaveLength(1);
    expect(durableClaims.rows[0]?.claimedBy).toBe(winningClaimant?.participantId);
    expect(claimedEvents).toHaveLength(1);
    expect(claimedEvent).toBeDefined();
    if (claimedEvent === undefined) {
      throw new Error(`Missing task.claimed event for ${task.task.taskId}`);
    }
    expect(readEventTaskPayload(claimedEvent)).toMatchObject({
      claimedBy: winningClaimant?.participantId,
      sessionId: session.sessionId,
      taskId: task.task.taskId,
    });
    const phaseEvents = coordinator.snapshot().phaseEvents;
    expect(phaseEvents).toHaveLength(2);
    expect(phaseEvents).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          actor: "claimant-a",
          name: "both-task-claim-transactions-open",
          position: "after",
          queryClass: "task-claim-transaction-start",
        }),
        expect.objectContaining({
          actor: "claimant-b",
          name: "both-task-claim-transactions-open",
          position: "after",
          queryClass: "task-claim-transaction-start",
        }),
      ]),
    );
    const cleanup = coordinator.snapshot().cleanup;
    expect(cleanup).toMatchObject({
      blockedActorsAfterRollback: 0,
      releasedActors: 2,
      rolledBackActors: 2,
    });
  }, 30_000);

  it("keeps concurrent event sequence visibility ordered through allocator commit", async () => {
    const sessionId = `sess_sequence_commit_${randomUUID()}`;
    const publisherAInput = {
      eventId: `evt_sequence_commit_a_${randomUUID()}`,
      payload: { outcome: "commit", publisher: "a" },
      producerId: "part_sequence_commit_a",
      sessionId,
      type: "test.sequence.commit",
    } as const;
    const publisherBInput = {
      eventId: `evt_sequence_commit_b_${randomUUID()}`,
      payload: { outcome: "commit", publisher: "b" },
      producerId: "part_sequence_commit_b",
      sessionId,
      type: "test.sequence.commit",
    } as const;
    await createDbSession(currentPool(), sessionId);
    const sequenceBefore = await readNextEventSequence(currentPool(), sessionId);
    const coordinator = createPostgresConcurrencyCoordinator(currentPool(), {
      actors: ["publisher-a", "publisher-b"],
      barrierTimeoutMs: 5_000,
      phases: [
        {
          actors: ["publisher-a"],
          name: "publisher-a-sequence-allocated",
          position: "after",
          query: { class: "event-sequence-allocation", text: eventSequenceAllocatorQuery },
          release: "manual",
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
    });

    const result = await coordinator.run(
      async ({ databaseFor, releasePhase, waitForLockWait, waitForPhase }) => {
        let publisherA: Promise<AsyncOutcome<SessionEvent>> | null = null;
        let publisherB: Promise<AsyncOutcome<SessionEvent>> | null = null;
        let phaseReleased = false;
        try {
          publisherA = observeAsyncOutcome(
            appendEvent(databaseFor("publisher-a"), publisherAInput, {
              sourceId: "src_sequence_commit_a",
            }),
          );
          await waitForPhase("publisher-a-sequence-allocated");
          publisherB = observeAsyncOutcome(
            appendEvent(databaseFor("publisher-b"), publisherBInput, {
              sourceId: "src_sequence_commit_b",
            }),
          );
          const lockWait = await waitForLockWait("publisher-b");
          const eventsBeforeRelease = await listEvents(
            currentPool(),
            sessionId,
            sequenceBefore - 1,
          );

          expect(eventsBeforeRelease).toEqual([]);
          releasePhase("publisher-a-sequence-allocated");
          phaseReleased = true;
          const [eventA, eventB] = await Promise.all([publisherA, publisherB]);
          return {
            eventA: requireFulfilledOutcome(eventA),
            eventB: requireFulfilledOutcome(eventB),
            lockWait,
          };
        } catch (error) {
          await settleEventPublishersAfterFailure({
            phaseName: "publisher-a-sequence-allocated",
            phaseReleased,
            publishers: [publisherA, publisherB],
            releasePhase,
          });
          throw error;
        }
      },
    );
    const committedEvents = await listEvents(currentPool(), sessionId, sequenceBefore - 1);

    expect(result.lockWait).toMatchObject({ blocked: true, waitEventType: "Lock" });
    expect([result.eventA.seq, result.eventB.seq]).toEqual([sequenceBefore, sequenceBefore + 1]);
    expect(committedEvents).toHaveLength(2);
    expect(committedEvents).toEqual([
      expect.objectContaining({
        eventId: publisherAInput.eventId,
        payload: publisherAInput.payload,
        seq: sequenceBefore,
      }),
      expect.objectContaining({
        eventId: publisherBInput.eventId,
        payload: publisherBInput.payload,
        seq: sequenceBefore + 1,
      }),
    ]);
    expect(await readNextEventSequence(currentPool(), sessionId)).toBe(sequenceBefore + 2);
  }, 15_000);

  it("reuses a rolled-back event allocation without leaving a durable gap", async () => {
    const sessionId = `sess_sequence_rollback_${randomUUID()}`;
    const publisherAInput = {
      eventId: `evt_sequence_rollback_a_${randomUUID()}`,
      payload: { outcome: "rollback", publisher: "a" },
      producerId: "part_sequence_rollback_a",
      sessionId,
      type: "test.sequence.rollback",
    } as const;
    const publisherBInput = {
      eventId: `evt_sequence_rollback_b_${randomUUID()}`,
      payload: { outcome: "commit", publisher: "b" },
      producerId: "part_sequence_rollback_b",
      sessionId,
      type: "test.sequence.rollback",
    } as const;
    await createDbSession(currentPool(), sessionId);
    const sequenceBefore = await readNextEventSequence(currentPool(), sessionId);
    const coordinator = createPostgresConcurrencyCoordinator(currentPool(), {
      actors: ["publisher-a", "publisher-b"],
      barrierTimeoutMs: 5_000,
      phases: [
        {
          actors: ["publisher-a"],
          name: "publisher-a-rollback-sequence-allocated",
          position: "after",
          query: { class: "event-sequence-allocation", text: eventSequenceAllocatorQuery },
          release: "manual",
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
    });

    const result = await coordinator.run(
      async ({ databaseFor, releasePhase, waitForLockWait, waitForPhase }) => {
        let publisherA: Promise<AsyncOutcome<SessionEvent>> | null = null;
        let publisherB: Promise<AsyncOutcome<SessionEvent>> | null = null;
        let phaseReleased = false;
        try {
          const failingPublisherA = await createEventInsertFailingDatabase(
            databaseFor("publisher-a"),
          );
          publisherA = observeAsyncOutcome(
            appendEvent(failingPublisherA, publisherAInput, {
              sourceId: "src_sequence_rollback_a",
            }),
          );
          await waitForPhase("publisher-a-rollback-sequence-allocated");
          publisherB = observeAsyncOutcome(
            appendEvent(databaseFor("publisher-b"), publisherBInput, {
              sourceId: "src_sequence_rollback_b",
            }),
          );
          const lockWait = await waitForLockWait("publisher-b");
          releasePhase("publisher-a-rollback-sequence-allocated");
          phaseReleased = true;
          const [eventA, eventB] = await Promise.all([publisherA, publisherB]);
          return { eventA, eventB, lockWait };
        } catch (error) {
          await settleEventPublishersAfterFailure({
            phaseName: "publisher-a-rollback-sequence-allocated",
            phaseReleased,
            publishers: [publisherA, publisherB],
            releasePhase,
          });
          throw error;
        }
      },
    );
    const eventB = requireFulfilledOutcome(result.eventB);
    const committedEvents = await listEvents(currentPool(), sessionId, sequenceBefore - 1);

    expect(result.lockWait).toMatchObject({ blocked: true, waitEventType: "Lock" });
    expect(result.eventA).toMatchObject({
      reason: expect.objectContaining({ message: "injected session event insert failure" }),
      status: "rejected",
    });
    expect(eventB).toMatchObject({
      eventId: publisherBInput.eventId,
      payload: publisherBInput.payload,
      seq: sequenceBefore,
    });
    expect(committedEvents).toEqual([
      expect.objectContaining({
        eventId: publisherBInput.eventId,
        payload: publisherBInput.payload,
        seq: sequenceBefore,
      }),
    ]);
    expect(committedEvents.map((event) => event.eventId)).not.toContain(publisherAInput.eventId);
    expect(await readNextEventSequence(currentPool(), sessionId)).toBe(sequenceBefore + 1);
  }, 15_000);

  it("rolls back participant registration when composed event insert fails", async () => {
    const session = await createSession();
    const participantId = `part_registration_rollback_${randomUUID()}`;
    const failingDatabase = await createEventInsertFailingDatabase(currentPool());
    const body = {
      capabilities: { workKinds: ["software_dev"] },
      displayName: "Registration Rollback",
      participantId,
      runtimeKind: "codex",
    };
    await expect(
      upsertParticipantWithEvent(failingDatabase, {
        ...body,
        eventSourceId: "src_registration_rollback",
        sessionId: session.sessionId,
      }),
    ).rejects.toThrow("injected session event insert failure");

    await expect(listParticipants(currentPool(), session.sessionId)).resolves.toEqual([]);
    const preRetrySnapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    const retry = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      {
        body: {
          ...body,
          instanceId: `inst_registration_rollback_retry_${randomUUID()}`,
        },
        method: "POST",
      },
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const rollbackSnapshot = preRetrySnapshots.participants.find(
      (snapshot) => snapshot.participantId === participantId,
    );

    expect(rollbackSnapshot).toBeUndefined();
    expect(retry.registrationStatus).toBe("joined");
    expect(participantEvents(events.events, "participant.joined", participantId)).toHaveLength(1);
  }, 15_000);

  it("keeps task lifecycle rows and events consistent across transitions", async () => {
    const session = await createSession();
    const sessionId = session.sessionId;
    const controller = {
      instanceId: "inst_task_atomicity",
      participantId: "part_task_atomicity",
    };
    await request(`/sessions/${sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["software_dev"] },
        displayName: "Task Atomicity",
        ...controller,
        runtimeKind: "codex",
      },
      method: "POST",
    });

    const completed = await createClaimedTask(sessionId, "complete this task", controller);
    const completion = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${completed.task.taskId}/complete`,
      {
        body: { ...controller, result: { summary: "done" } },
        method: "POST",
      },
    );
    const failed = await createClaimedTask(sessionId, "fail this task", controller);
    const failure = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${failed.task.taskId}/fail`,
      {
        body: { ...controller, failure: { reason: "expected" } },
        method: "POST",
      },
    );
    const released = await createClaimedTask(sessionId, "release this task", controller);
    const release = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${released.task.taskId}/release`,
      {
        body: controller,
        method: "POST",
      },
    );
    const cancelled = await request<TaskResponse>(`/sessions/${sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "cancel this task" },
      method: "POST",
    });
    const cancellationReason = { message: "obsolete" };
    const cancellation = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${cancelled.task.taskId}/cancel`,
      {
        body: { ...controller, reason: cancellationReason },
        method: "POST",
      },
    );

    await expect(
      request(`/sessions/${sessionId}/tasks/${completed.task.taskId}/claim`, {
        body: controller,
        method: "POST",
      }),
    ).rejects.toThrow("409");
    await expect(
      request(`/sessions/${sessionId}/tasks/${cancelled.task.taskId}/claim`, {
        body: controller,
        method: "POST",
      }),
    ).rejects.toThrow("409");
    const events = await request<EventsResponse>(`/sessions/${sessionId}/events?after=0`);

    expect(events.events.filter((event) => event.type === "task.created")).toHaveLength(4);
    expect(events.events.filter((event) => event.type === "task.claimed")).toHaveLength(3);
    expect(events.events.filter((event) => event.type === "task.completed")).toHaveLength(1);
    expect(events.events.filter((event) => event.type === "task.failed")).toHaveLength(1);
    expect(events.events.filter((event) => event.type === "task.released")).toHaveLength(1);
    expect(events.events.filter((event) => event.type === "control.cancel")).toHaveLength(1);
    expectTaskEventPayload(
      events.events,
      "task.completed",
      completion.task.taskId,
      completion.task,
    );
    expectTaskEventPayload(events.events, "task.failed", failure.task.taskId, failure.task);
    expectTaskEventPayload(events.events, "task.released", release.task.taskId, release.task);
    const cancelEvent = expectTaskEventPayload(
      events.events,
      "control.cancel",
      cancellation.task.taskId,
      cancellation.task,
    );
    expect(cancelEvent.payload.participantId).toBe(controller.participantId);
    expect(cancelEvent.payload.reason).toEqual(cancellationReason);
    expectTaskEventConsistency(events.events, [
      completion.task,
      failure.task,
      release.task,
      cancellation.task,
    ]);
  });

  it("lets a connected participant claim explicitly released work without reconnecting", async () => {
    const session = await createSession();
    const controller = {
      instanceId: "inst_release_controller_e2e",
      participantId: "part_release_controller_e2e",
    };
    const claimed = await createClaimedTask(
      session.sessionId,
      "release to live runtime",
      controller,
    );
    const preReleaseEvents = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    const afterSeq = Math.max(...preReleaseEvents.events.map((event) => event.seq));
    const client = await ParticipantRuntimeClient.connect({
      afterSeq,
      authToken: mintE2eToken({
        participantId: "part_release_runtime_e2e",
        role: "participant",
        sessionId: session.sessionId,
      }),
      capabilities: { workKinds: ["software_dev"] },
      displayName: "Release Runtime E2E",
      instanceId: "inst_release_runtime_e2e",
      participantId: "part_release_runtime_e2e",
      runtimeKind: "codex",
      serviceUrl: baseUrl,
      sessionId: session.sessionId,
    });
    const loop = client.runClaimableTasks({
      claimRefreshMs: 50,
      executor: async () => ({ result: { pickedUpReleasedTask: true } }),
      once: false,
      shouldClaimTask: (task) => task.kind === "software_dev",
    });

    try {
      await waitForAsync(async () => client.debugInfo().eventHandlerCount === 1);
      await request<TaskResponse>(
        `/sessions/${session.sessionId}/tasks/${claimed.task.taskId}/release`,
        {
          body: controller,
          method: "POST",
        },
      );
      await waitForAsync(async () => {
        const tasks = await request<TasksResponse>(
          `/sessions/${session.sessionId}/tasks?status=all`,
        );
        return tasks.tasks.some(
          (task) =>
            task.taskId === claimed.task.taskId &&
            task.completedAt !== null &&
            task.result?.pickedUpReleasedTask === true,
        );
      });
    } finally {
      client.close();
      await loop;
    }

    const tasks = await request<TasksResponse>(`/sessions/${session.sessionId}/tasks?status=all`);
    const completedTask = tasks.tasks.find((task) => task.taskId === claimed.task.taskId);
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const eventTypes = events.events.map((event) => event.type);

    expect(completedTask?.claimedBy).toBe("part_release_runtime_e2e");
    expect(completedTask?.result).toEqual({ pickedUpReleasedTask: true });
    expect(eventTypes).toContain("task.released");
    expect(eventTypes).toContain("task.completed");
    expect(eventTypes).not.toContain("task.claim_expired");
  });

  it("rolls back task creation when composed event insert fails", async () => {
    const session = await createSession();
    const taskId = `task_atomic_rollback_${randomUUID()}`;
    const failingDatabase = await createEventInsertFailingDatabase(currentPool());

    await expect(
      createTaskWithEvent(failingDatabase, {
        eventSourceId: "src_e2e_atomicity",
        kind: "software_dev",
        objective: "rollback this task",
        sessionId: session.sessionId,
        taskId,
      }),
    ).rejects.toThrow("injected session event insert failure");

    await expect(
      getTask(currentPool(), { sessionId: session.sessionId, taskId }),
    ).resolves.toBeNull();
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    expect(
      events.events.some(
        (event) => event.type === "task.created" && taskIdFromEventPayload(event) === taskId,
      ),
    ).toBe(false);
  });

  it("keeps task completion retryable when composed event insert fails", async () => {
    const session = await createSession();
    const taskId = `task_atomic_complete_${randomUUID()}`;
    await createTaskWithEvent(currentPool(), {
      eventSourceId: "src_e2e_atomicity",
      kind: "software_dev",
      objective: "complete after rollback",
      sessionId: session.sessionId,
      taskId,
    });
    await request(`/sessions/${session.sessionId}/tasks/${taskId}/claim`, {
      body: { instanceId: "inst_retryable", participantId: "part_retryable" },
      method: "POST",
    });
    const failingDatabase = await createEventInsertFailingDatabase(currentPool());

    await expect(
      completeTaskWithEvent(failingDatabase, {
        eventSourceId: "src_e2e_atomicity",
        participantId: "part_retryable",
        result: { summary: "rolled back" },
        sessionId: session.sessionId,
        taskId,
      }),
    ).rejects.toThrow("injected session event insert failure");

    const afterFailure = await getTask(currentPool(), {
      sessionId: session.sessionId,
      taskId,
    });
    expect(afterFailure?.completedAt).toBeNull();
    await expect(
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${taskId}/complete`, {
        body: {
          instanceId: "inst_retryable",
          participantId: "part_retryable",
          result: { summary: "retried" },
        },
        method: "POST",
      }),
    ).resolves.toMatchObject({
      task: { completedAt: expect.any(String) as string },
    });
  });

  it("orders concurrent terminal task transitions by committed event state", async () => {
    const session = await createSession();
    const controller = {
      instanceId: "inst_concurrent_terminal",
      participantId: "part_concurrent_terminal",
    };
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "race terminal transitions" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: controller,
      method: "POST",
    });

    const [completeResult, cancelResult] = await Promise.allSettled([
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`, {
        body: { ...controller, result: { summary: "completed first" } },
        method: "POST",
      }),
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/cancel`, {
        body: { ...controller, reason: { message: "cancelled first" } },
        method: "POST",
      }),
    ]);
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const terminalEvents = events.events.filter(
      (event) =>
        taskIdFromEventPayload(event) === task.task.taskId &&
        (event.type === "control.cancel" || event.type === "task.completed"),
    );

    expect(
      [completeResult.status, cancelResult.status].filter((status) => status === "fulfilled"),
    ).toHaveLength(1);
    expect(terminalEvents).toHaveLength(1);
    const terminalEvent = terminalEvents[0];
    expect(terminalEvent?.seq).toBeGreaterThan(
      events.events.find(
        (event) =>
          event.type === "task.claimed" && taskIdFromEventPayload(event) === task.task.taskId,
      )?.seq ?? 0,
    );
    if (completeResult.status === "fulfilled") {
      expect(terminalEvent?.type).toBe("task.completed");
      expectTaskEventPayload(
        events.events,
        "task.completed",
        task.task.taskId,
        completeResult.value.task,
      );
    } else if (cancelResult.status === "fulfilled") {
      expect(terminalEvent?.type).toBe("control.cancel");
      expectTaskEventPayload(
        events.events,
        "control.cancel",
        task.task.taskId,
        cancelResult.value.task,
      );
    }
  });

  it("records approval intent as an event without mutating task state", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["generic_approval_request"] },
        displayName: "Generic Approval Agent E2E",
        instanceId: "inst_generic_agent_e2e",
        participantId: "part_generic_agent_e2e",
        runtimeKind: "generic_agent",
      },
      method: "POST",
    });
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "generic_approval_request",
        objective: "generic approval",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: {
        instanceId: "inst_generic_agent_e2e",
        participantId: "part_generic_agent_e2e",
      },
      method: "POST",
    });
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          instanceId: "inst_generic_agent_e2e",
          participantId: "part_generic_agent_e2e",
          result: createGenericApprovalResult(),
        },
        method: "POST",
      },
    );

    const approval = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_external_bridge_e2e",
          participantId: "part_external_bridge_e2e",
          reason: { source: "external-chat" },
        },
        method: "POST",
      },
    );
    const taskAfterApproval = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}`,
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);

    expect(approval).toMatchObject({
      decision: "approved",
      event: { type: "approval.recorded" },
      status: "recorded",
      task: {
        result: {
          dryRun: genericDryRunApprovalFixture(),
          itemRecommendations: [
            {
              action: "approve",
              category: "generic",
              evidence: [],
              manualActionReason: null,
              itemId: "item-1",
              reason: "matches the request",
              requiresApproval: false,
              risk: "low",
              title: "Request Item",
            },
          ],
          summary: "Prepared a plan",
        },
        taskId: task.task.taskId,
      },
    });
    expect(taskAfterApproval.task).toMatchObject({
      result: {
        dryRun: genericDryRunApprovalFixture(),
        itemRecommendations: [
          {
            action: "approve",
            category: "generic",
            evidence: [],
            manualActionReason: null,
            itemId: "item-1",
            reason: "matches the request",
            requiresApproval: false,
            risk: "low",
            title: "Request Item",
          },
        ],
        summary: "Prepared a plan",
      },
      taskId: task.task.taskId,
    });
    expect(events.events.map((event) => event.type)).toContain("approval.recorded");
  });

  it("records approval intent for a media series removal dry run", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["generic_remove_item"] },
        displayName: "Generic Agent E2E",
        instanceId: "inst_generic_agent_e2e",
        participantId: "part_generic_agent_e2e",
        runtimeKind: "generic_agent",
      },
      method: "POST",
    });
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        input: { seriesTitle: "Marvel's The Punisher" },
        kind: "generic_remove_item",
        objective: "remove the mistakenly added Punisher series",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: {
        instanceId: "inst_generic_agent_e2e",
        participantId: "part_generic_agent_e2e",
      },
      method: "POST",
    });
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          instanceId: "inst_generic_agent_e2e",
          participantId: "part_generic_agent_e2e",
          result: {
            authorization: {
              authorization: "needs_approval",
              dryRun: false,
              reason: "Action changes media service state and requires explicit approval.",
            },
            dryRun: {
              approvalSummary: ["title: Marvel's The Punisher", "tvdbId: 331980", "series id: 110"],
              authorization: "needs_approval",
              request: {
                addImportListExclusion: false,
                deleteFiles: true,
                path: "/tv/Marvel's The Punisher",
                seriesId: 110,
                title: "Marvel's The Punisher",
                torrentHashes: [],
                tvdbId: 331980,
                year: 2017,
              },
              target: "sonarr_series",
              torrents: [],
            },
            kind: "generic_remove_item",
            readOnly: false,
          },
        },
        method: "POST",
      },
    );

    const approval = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_external_generic_e2e",
          participantId: "part_external_generic_e2e",
          reason: { source: "external-chat" },
        },
        method: "POST",
      },
    );

    expect(approval).toMatchObject({
      decision: "approved",
      event: { type: "approval.recorded" },
      status: "recorded",
      task: { taskId: task.task.taskId },
    });
  });

  it("ignores duplicate task approval decisions without appending another event", async () => {
    const session = await createSession();
    const task = await createCompletedGenericApprovalTask(session.sessionId, "duplicate");

    await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_external_duplicate_e2e",
          participantId: "part_external_duplicate_e2e",
        },
        method: "POST",
      },
    );
    const sameDecisionDuplicate = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_external_duplicate_e2e",
          participantId: "part_external_duplicate_e2e",
        },
        method: "POST",
      },
    );
    const contradictoryDuplicate = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "rejected",
          instanceId: "inst_external_duplicate_e2e",
          participantId: "part_external_duplicate_e2e",
        },
        method: "POST",
      },
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);

    expect(sameDecisionDuplicate).toMatchObject({
      decision: "approved",
      existingDecision: "approved",
      ignoredReason: "already_approved",
      status: "ignored",
      task: { taskId: task.task.taskId },
    });
    expect(contradictoryDuplicate).toMatchObject({
      decision: "rejected",
      existingDecision: "approved",
      ignoredReason: "already_approved",
      status: "ignored",
      task: { taskId: task.task.taskId },
    });
    expect(events.events.filter((event) => event.type === "approval.recorded")).toHaveLength(1);
  });

  it("records only one approval for concurrent REST approval requests", async () => {
    const session = await createSession();
    const task = await createCompletedGenericApprovalTask(session.sessionId, "concurrent-rest");

    const [approved, rejected] = await Promise.all([
      request<TaskApprovalResponse>(
        `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
        {
          body: {
            decision: "approved",
            instanceId: "inst_external_concurrent_e2e",
            participantId: "part_external_concurrent_e2e",
          },
          method: "POST",
        },
      ),
      request<TaskApprovalResponse>(
        `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
        {
          body: {
            decision: "rejected",
            instanceId: "inst_external_concurrent_e2e",
            participantId: "part_external_concurrent_e2e",
          },
          method: "POST",
        },
      ),
    ]);
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const approvals = await listTaskApprovals(currentPool(), {
      sessionId: session.sessionId,
      taskId: task.task.taskId,
    });

    expect([approved.status, rejected.status].sort()).toEqual(["ignored", "recorded"]);
    expect(events.events.filter((event) => event.type === "approval.recorded")).toHaveLength(1);
    expect(approvals).toHaveLength(1);
    expect(approvals[0]?.targetKey).toBe("task");
  });

  it("records only one approval for concurrent store-level approval calls", async () => {
    const session = await createSession();
    const task = await createCompletedGenericApprovalTask(session.sessionId, "concurrent-store");

    const [first, second] = await Promise.all([
      recordTaskApproval(currentPool(), {
        decision: "approved",
        eventSourceId: "src_store_approval_test",
        participantId: "part_store_approval_test",
        reason: {},
        sessionId: session.sessionId,
        taskId: task.task.taskId,
      }),
      recordTaskApproval(currentPool(), {
        decision: "approved",
        eventSourceId: "src_store_approval_test",
        participantId: "part_store_approval_test",
        reason: {},
        sessionId: session.sessionId,
        taskId: task.task.taskId,
      }),
    ]);
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
    const approvals = await listTaskApprovals(currentPool(), {
      sessionId: session.sessionId,
      taskId: task.task.taskId,
    });

    expect([first?.status, second?.status].sort()).toEqual(["ignored", "recorded"]);
    expect(events.events.filter((event) => event.type === "approval.recorded")).toHaveLength(1);
    expect(approvals).toHaveLength(1);
  });

  it("backfills historic approval events into approval rows during migration", async () => {
    const legacyDatabaseName = `tether_e2e_legacy_${randomUUID().replaceAll("-", "_")}`;
    const legacyDatabase = createPool(buildDatabaseUrl(legacyDatabaseName));
    try {
      await createDatabase(legacyDatabaseName);
      await applyLegacyMigrationsThrough0006(legacyDatabase);
      await seedLegacyApprovalEvent(legacyDatabase);

      await migrate(legacyDatabase);

      const duplicate = await recordTaskApproval(legacyDatabase, {
        decision: "rejected",
        eventSourceId: "src_legacy_backfill_test",
        participantId: "part_legacy_backfill_test",
        reason: {},
        sessionId: "sess_legacy_backfill",
        taskId: "task_legacy_backfill",
      });
      const approvals = await listTaskApprovals(legacyDatabase, {
        sessionId: "sess_legacy_backfill",
        taskId: "task_legacy_backfill",
      });
      const eventCount = await legacyDatabase.pool.query<{
        readonly count: number;
      }>(
        `
          SELECT count(*)::int AS count
          FROM session_events
          WHERE session_id = $1
            AND type = 'approval.recorded'
        `,
        ["sess_legacy_backfill"],
      );

      expect(duplicate).toMatchObject({
        existingDecision: "approved",
        status: "ignored",
      });
      expect(approvals).toHaveLength(1);
      expect(approvals[0]).toMatchObject({
        approvalEventId: "evt_legacy_approval",
        decision: "approved",
        targetKey: "task",
      });
      expect(eventCount.rows[0]?.count).toBe(1);
    } finally {
      await legacyDatabase.end();
      await dropDatabase(legacyDatabaseName);
    }
  });

  it("records separate approval decisions for separate email recommendation targets", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "generic_approval_request",
        objective: "generic approval",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: {
        instanceId: "inst_approval_target_e2e",
        participantId: "part_approval_target_e2e",
      },
      method: "POST",
    });
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          instanceId: "inst_approval_target_e2e",
          participantId: "part_approval_target_e2e",
          result: {
            ...createGenericApprovalResult(),
            actions: ["Approve Request Item", "Approve Follow-up Item"],
            inspectedItems: ["item-1", "item-2"],
            itemRecommendations: [
              {
                action: "approve",
                authorization: "needs_approval",
                authorizationReason: "e2e approval fixture",
                autonomousEligibility: "requires_approval",
                autonomousReason: "e2e approval fixture",
                category: "generic",
                evidence: [],
                manualActionReason: null,
                itemId: "item-1",
                reason: "matches the request",
                requiresApproval: true,
                risk: "low",
                title: "Request Item",
              },
              {
                action: "approve",
                authorization: "needs_approval",
                authorizationReason: "e2e approval fixture",
                autonomousEligibility: "requires_approval",
                autonomousReason: "e2e approval fixture",
                category: "generic",
                evidence: [],
                manualActionReason: null,
                itemId: "item-2",
                reason: "matches the follow-up request",
                requiresApproval: true,
                risk: "low",
                title: "Follow-up Item",
              },
            ],
            plannerOutputRecommendationCount: 2,
            selectedItems: ["Request Item", "Follow-up Item"],
            titles: ["Request Item", "Follow-up Item"],
          },
        },
        method: "POST",
      },
    );

    const firstApproval = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_external_target_e2e",
          participantId: "part_external_target_e2e",
          reason: { approvalTarget: { action: "keep", key: "message-1" } },
        },
        method: "POST",
      },
    );
    const secondApproval = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_external_target_e2e",
          participantId: "part_external_target_e2e",
          reason: { approvalTarget: { action: "keep", key: "message-2" } },
        },
        method: "POST",
      },
    );
    const duplicateFirstApproval = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: {
          decision: "rejected",
          instanceId: "inst_external_target_e2e",
          participantId: "part_external_target_e2e",
          reason: { approvalTarget: { action: "keep", key: "message-1" } },
        },
        method: "POST",
      },
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);

    expect(firstApproval.status).toBe("recorded");
    expect(secondApproval.status).toBe("recorded");
    expect(duplicateFirstApproval).toMatchObject({
      decision: "rejected",
      existingDecision: "approved",
      ignoredReason: "already_approved",
      status: "ignored",
    });
    expect(events.events.filter((event) => event.type === "approval.recorded")).toHaveLength(2);
  });

  it("rejects approval for tasks without a completed dry-run organization plan", async () => {
    const session = await createSession();
    const uncompletedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "generic_approval_request",
        objective: "generic approval",
      },
      method: "POST",
    });
    const unsupportedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "generic approval" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${unsupportedTask.task.taskId}/claim`, {
      body: {
        instanceId: "inst_software_invalid_e2e",
        participantId: "part_software_invalid_e2e",
      },
      method: "POST",
    });
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${unsupportedTask.task.taskId}/complete`,
      {
        body: {
          instanceId: "inst_software_invalid_e2e",
          participantId: "part_software_invalid_e2e",
          result: { dryRun: true, organizationRecommendations: [] },
        },
        method: "POST",
      },
    );
    const invalidPlanTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "generic_approval_request",
        objective: "generic approval",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${invalidPlanTask.task.taskId}/claim`, {
      body: {
        instanceId: "inst_email_invalid_e2e",
        participantId: "part_email_invalid_e2e",
      },
      method: "POST",
    });
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${invalidPlanTask.task.taskId}/complete`,
      {
        body: {
          instanceId: "inst_email_invalid_e2e",
          participantId: "part_email_invalid_e2e",
          result: { dryRun: true, organizationRecommendations: [] },
        },
        method: "POST",
      },
    );

    await expect(
      request(`/sessions/${session.sessionId}/tasks/${uncompletedTask.task.taskId}/approval`, {
        body: {
          decision: "approved",
          instanceId: "inst_external_invalid_e2e",
          participantId: "part_external_invalid_e2e",
        },
        method: "POST",
      }),
    ).rejects.toThrow("task_not_completed");
    await expect(
      request(`/sessions/${session.sessionId}/tasks/${unsupportedTask.task.taskId}/approval`, {
        body: {
          decision: "approved",
          instanceId: "inst_external_invalid_e2e",
          participantId: "part_external_invalid_e2e",
        },
        method: "POST",
      }),
    ).rejects.toThrow("unsupported_task_kind");
    await expect(
      request(`/sessions/${session.sessionId}/tasks/${invalidPlanTask.task.taskId}/approval`, {
        body: {
          decision: "approved",
          instanceId: "inst_external_invalid_e2e",
          participantId: "part_external_invalid_e2e",
        },
        method: "POST",
      }),
    ).rejects.toThrow("unsupported_task_kind");
  });

  it("rejects approval when no domain validator is registered for the task kind", async () => {
    const unvalidatedApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      sessionService: {
        approvalValidators: [],
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 1_000,
      },
    });
    const port = await findOpenPort();
    await unvalidatedApp.listen(port);
    const unvalidatedOrigin = `http://127.0.0.1:${port}`;
    try {
      const session = await requestFrom<SessionResponse>(unvalidatedOrigin, "/sessions", {
        body: {},
        method: "POST",
      });
      const sessionId = session.session.sessionId;
      const task = await requestFrom<TaskResponse>(
        unvalidatedOrigin,
        `/sessions/${sessionId}/tasks`,
        {
          body: {
            kind: "generic_approval_request",
            objective: "generic approval",
          },
          method: "POST",
        },
      );
      await requestFrom(
        unvalidatedOrigin,
        `/sessions/${sessionId}/tasks/${task.task.taskId}/claim`,
        {
          body: {
            instanceId: "inst_email_unvalidated_e2e",
            participantId: "part_email_unvalidated_e2e",
          },
          method: "POST",
        },
      );
      await requestFrom<TaskResponse>(
        unvalidatedOrigin,
        `/sessions/${sessionId}/tasks/${task.task.taskId}/complete`,
        {
          body: {
            instanceId: "inst_email_unvalidated_e2e",
            participantId: "part_email_unvalidated_e2e",
            result: {
              dryRun: true,
              organizationRecommendations: [],
            },
          },
          method: "POST",
        },
      );

      await expect(
        requestFrom(
          unvalidatedOrigin,
          `/sessions/${sessionId}/tasks/${task.task.taskId}/approval`,
          {
            body: {
              decision: "approved",
              instanceId: "inst_external_unvalidated_e2e",
              participantId: "part_external_unvalidated_e2e",
            },
            method: "POST",
          },
        ),
      ).rejects.toThrow("unsupported_task_kind");
    } finally {
      await unvalidatedApp.close();
    }
  });

  it("defaults task lists to active tasks and keeps history opt-in", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["software_dev"] },
        displayName: "Codex Filter E2E",
        instanceId: "inst_codex_filter_e2e",
        participantId: "part_codex_filter_e2e",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    const activeTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        input: { priority: "normal", target: { path: "src/index.ts" } },
        kind: "software_dev",
        objective: "Stay visible",
      },
      method: "POST",
    });
    const completedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Move to history" },
      method: "POST",
    });
    const cancelledTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Cancel into history" },
      method: "POST",
    });

    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/claim`, {
      body: {
        instanceId: "inst_codex_filter_e2e",
        participantId: "part_codex_filter_e2e",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/complete`, {
      body: {
        instanceId: "inst_codex_filter_e2e",
        participantId: "part_codex_filter_e2e",
        result: { summary: "done" },
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${cancelledTask.task.taskId}/cancel`, {
      body: {
        participantId: "part_filter_canceller_e2e",
        reason: { message: "not needed" },
      },
      method: "POST",
    });

    const activeTasks = await request<TasksResponse>(`/sessions/${session.sessionId}/tasks`);
    const allTasks = await request<TasksResponse>(
      `/sessions/${session.sessionId}/tasks?status=all`,
    );
    const terminalTasks = await request<TasksResponse>(
      `/sessions/${session.sessionId}/tasks?status=terminal`,
    );
    const completedTaskRead = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}`,
    );

    expect(activeTasks.tasks.map((task) => task.taskId)).toEqual([activeTask.task.taskId]);
    expect(activeTask.task.input).toEqual({
      priority: "normal",
      target: { path: "src/index.ts" },
    });
    expect(allTasks.tasks.map((task) => task.taskId)).toEqual([
      cancelledTask.task.taskId,
      completedTask.task.taskId,
      activeTask.task.taskId,
    ]);
    expect(terminalTasks.tasks.map((task) => task.taskId)).toEqual([
      cancelledTask.task.taskId,
      completedTask.task.taskId,
    ]);
    expect(completedTaskRead.task).toMatchObject({
      completedAt: expect.any(String) as string,
      result: { summary: "done" },
      taskId: completedTask.task.taskId,
    });
  });

  it("requires parallel runtimes to use distinct participant identities", async () => {
    const session = await createSession();
    const firstRegistration = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      {
        body: {
          capabilities: { workKinds: ["software_dev"] },
          displayName: "Codex Worker 1",
          instanceId: "inst_codex_pool_1",
          participantId: "part_codex_pool_1",
          runtimeKind: "codex",
        },
        method: "POST",
      },
    );
    const refresh = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      {
        body: {
          capabilities: { workKinds: ["software_dev"] },
          displayName: "Codex Worker 1",
          instanceId: "inst_codex_pool_1",
          participantId: "part_codex_pool_1",
          runtimeKind: "codex",
        },
        method: "POST",
      },
    );

    await expect(
      request(`/sessions/${session.sessionId}/participants`, {
        body: {
          capabilities: { workKinds: ["software_dev"] },
          displayName: "Codex Worker 1 duplicate",
          instanceId: "inst_codex_pool_2",
          participantId: "part_codex_pool_1",
          runtimeKind: "codex",
        },
        method: "POST",
      }),
    ).rejects.toThrow("409");
    const secondRegistration = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      {
        body: {
          capabilities: { workKinds: ["software_dev"] },
          displayName: "Codex Worker 2",
          instanceId: "inst_codex_pool_2",
          participantId: "part_codex_pool_2",
          runtimeKind: "codex",
        },
        method: "POST",
      },
    );

    expect(firstRegistration.registrationStatus).toBe("joined");
    expect(refresh.registrationStatus).toBe("refreshed");
    expect(secondRegistration.registrationStatus).toBe("joined");
  });

  it("discovers participant task contracts through a common session endpoint", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: {
          contracts: [
            {
              approval: "required_for_mutation",
              description: "Prepare a generic approval dry run.",
              inputSchemaRef: "generic-task-contract:generic_approval_request:v1:input",
              participantRuntimeKind: "generic_agent",
              readOnlyByDefault: true,
              resultSchemaRef: "generic-task-contract:generic_approval_request:v1:result",
              taskKind: "generic_approval_request",
              title: "Generic approval",
              version: "1",
            },
            { taskKind: "" },
          ],
          workKinds: ["generic_approval_request"],
        },
        displayName: "Generic Approval Agent E2E",
        instanceId: "inst_contract_email_e2e",
        participantId: "part_contract_approval_e2e",
        runtimeKind: "generic_agent",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: {
          contracts: [
            {
              approval: "required_for_mutation",
              description: "Prepare a generic reset dry run.",
              inputSchemaRef: "task-contract:generic_reset:v1:input",
              participantRuntimeKind: "generic_agent",
              readOnlyByDefault: true,
              resultSchemaRef: "task-contract:generic_reset:v1:result",
              taskKind: "generic_reset",
              title: "Generic reset",
              version: "1",
            },
          ],
          workKinds: ["generic_reset"],
        },
        displayName: "Generic Agent E2E",
        instanceId: "inst_contract_generic_e2e",
        participantId: "part_contract_generic_e2e",
        runtimeKind: "generic_agent",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: {
          contracts: [
            {
              approval: "none",
              description: "Run a typed test task.",
              inputJsonSchema: {
                additionalProperties: false,
                properties: {
                  query: { type: "string" },
                },
                required: ["query"],
                type: "object",
              },
              inputSchemaRef: "typed-task-contract:typed_lookup:v1:input",
              participantRuntimeKind: "typed_agent",
              readOnlyByDefault: true,
              resultJsonSchema: {
                additionalProperties: false,
                properties: {
                  summary: { type: "string" },
                },
                required: ["summary"],
                type: "object",
              },
              resultSchemaRef: "typed-task-contract:typed_lookup:v1:result",
              taskKind: "typed_lookup",
              title: "Typed lookup",
              version: "1",
            },
          ],
          workKinds: ["typed_lookup"],
        },
        displayName: "Typed Agent E2E",
        instanceId: "inst_contract_typed_e2e",
        participantId: "part_contract_typed_e2e",
        runtimeKind: "typed_agent",
      },
      method: "POST",
    });

    const contracts = await request<TaskContractsResponse>(
      `/sessions/${session.sessionId}/task-contracts`,
    );
    const typedContract = await request<TaskContractResponse>(
      `/sessions/${session.sessionId}/task-contracts/typed_lookup`,
    );
    const strictMediaTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "generic_reset",
        objective: "run generic reset through advertised contract",
        requireContract: true,
      },
      method: "POST",
    });
    const strictTypedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        input: { query: "Dark" },
        kind: "typed_lookup",
        objective: "typed lookup",
        requireContract: true,
      },
      method: "POST",
    });
    const looseTypoTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "generic_resetv",
        objective: "legacy loose task creation",
      },
      method: "POST",
    });
    const strictTypedTaskInspection = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${strictTypedTask.task.taskId}?include=contract`,
    );

    expect(contracts.taskContracts).toEqual([
      {
        approval: "required_for_mutation",
        description: "Prepare a generic approval dry run.",
        displayName: "Generic Approval Agent E2E",
        inputSchemaRef: "generic-task-contract:generic_approval_request:v1:input",
        participantId: "part_contract_approval_e2e",
        participantRuntimeKind: "generic_agent",
        readOnlyByDefault: true,
        resultSchemaRef: "generic-task-contract:generic_approval_request:v1:result",
        runtimeKind: "generic_agent",
        sessionId: session.sessionId,
        taskKind: "generic_approval_request",
        title: "Generic approval",
        version: "1",
      },
      {
        approval: "required_for_mutation",
        description: "Prepare a generic reset dry run.",
        displayName: "Generic Agent E2E",
        inputSchemaRef: "task-contract:generic_reset:v1:input",
        participantId: "part_contract_generic_e2e",
        participantRuntimeKind: "generic_agent",
        readOnlyByDefault: true,
        resultSchemaRef: "task-contract:generic_reset:v1:result",
        runtimeKind: "generic_agent",
        sessionId: session.sessionId,
        taskKind: "generic_reset",
        title: "Generic reset",
        version: "1",
      },
      {
        approval: "none",
        description: "Run a typed test task.",
        displayName: "Typed Agent E2E",
        inputJsonSchema: {
          additionalProperties: false,
          properties: {
            query: { type: "string" },
          },
          required: ["query"],
          type: "object",
        },
        inputSchemaRef: "typed-task-contract:typed_lookup:v1:input",
        participantId: "part_contract_typed_e2e",
        participantRuntimeKind: "typed_agent",
        readOnlyByDefault: true,
        resultJsonSchema: {
          additionalProperties: false,
          properties: {
            summary: { type: "string" },
          },
          required: ["summary"],
          type: "object",
        },
        resultSchemaRef: "typed-task-contract:typed_lookup:v1:result",
        runtimeKind: "typed_agent",
        sessionId: session.sessionId,
        taskKind: "typed_lookup",
        title: "Typed lookup",
        version: "1",
      },
    ]);
    expect(typedContract.taskContract).toEqual(contracts.taskContracts[2]);
    expect(typedContract.taskContracts).toEqual([contracts.taskContracts[2]]);
    expect(strictMediaTask.task).toMatchObject({
      kind: "generic_reset",
      taskId: expect.any(String) as string,
    });
    expect(strictTypedTask.task).toMatchObject({
      input: { query: "Dark" },
      kind: "typed_lookup",
      taskId: expect.any(String) as string,
    });
    expect(strictTypedTaskInspection).toMatchObject({
      contract: {
        inputJsonSchema: {
          required: ["query"],
          type: "object",
        },
        resultJsonSchema: {
          required: ["summary"],
          type: "object",
        },
        taskKind: "typed_lookup",
      },
      task: {
        kind: "typed_lookup",
        taskId: strictTypedTask.task.taskId,
      },
    });
    expect(looseTypoTask.task).toMatchObject({
      kind: "generic_resetv",
      taskId: expect.any(String) as string,
    });
    await expect(
      request(`/sessions/${session.sessionId}/tasks`, {
        body: {
          kind: "generic_resetv",
          objective: "strict typo should fail",
          requireContract: true,
        },
        method: "POST",
      }),
    ).rejects.toThrow("409");
    await expect(
      request(`/sessions/${session.sessionId}/tasks`, {
        body: {
          input: { extra: true },
          kind: "typed_lookup",
          objective: "strict invalid input should fail",
          requireContract: true,
        },
        method: "POST",
      }),
    ).rejects.toThrow("400");
  });

  it("builds deterministic bounded session context views", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: {
          contracts: [
            {
              approval: "none",
              description: "Report generic agent status.",
              inputSchemaRef: "task-contract:generic_status:v1:input",
              participantRuntimeKind: "generic_agent",
              readOnlyByDefault: true,
              resultSchemaRef: "task-contract:generic_status:v1:result",
              taskKind: "generic_status",
              title: "Generic agent status",
              version: "1",
            },
          ],
          workKinds: ["generic_status"],
        },
        displayName: "Generic Agent Context E2E",
        instanceId: "inst_context_generic_e2e",
        participantId: "part_context_generic_e2e",
        runtimeKind: "generic_agent",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/events`, {
      body: {
        payload: { text: "older user request" },
        producerId: "external-client",
        type: "user.message",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/events`, {
      body: {
        payload: { text: "newest user request" },
        producerId: "external-client",
        type: "user.message",
      },
      method: "POST",
    });
    const activeTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "coordinate_request", objective: "generic status" },
      method: "POST",
    });
    const terminalTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "generic_status", objective: "status" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${terminalTask.task.taskId}/claim`, {
      body: {
        instanceId: "inst_context_generic_e2e",
        participantId: "part_context_generic_e2e",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${terminalTask.task.taskId}/complete`, {
      body: {
        instanceId: "inst_context_generic_e2e",
        participantId: "part_context_generic_e2e",
        result: { kind: "generic_status", readOnly: true },
      },
      method: "POST",
    });

    const context = await request<SessionContextResponse>(
      `/sessions/${session.sessionId}/context?forParticipant=part_coordinator&budgetTokens=1000`,
    );

    expect(context.context).toMatchObject({
      activeTasks: [
        {
          kind: "coordinate_request",
          taskId: activeTask.task.taskId,
        },
      ],
      budget: {
        estimatedTokens: expect.any(Number) as number,
        omittedEventCount: expect.any(Number) as number,
        requestedTokens: 1_000,
      },
      forParticipant: "part_coordinator",
      kind: "session_context",
      latestSummary: null,
      recentEventRange: {
        endSeq: expect.any(Number) as number,
        startSeq: expect.any(Number) as number,
      },
      recentTerminalTasks: [
        {
          kind: "generic_status",
          taskId: terminalTask.task.taskId,
        },
      ],
      sessionId: session.sessionId,
      taskContracts: [
        expect.objectContaining({
          participantId: "part_context_generic_e2e",
          taskKind: "generic_status",
        }),
      ],
    });
    expect(context.context.recentEvents.length).toBeGreaterThan(0);
    expect(context.context.recentEvents.at(-1)?.type).toBe("task.completed");
    expect(context.context.budget.estimatedTokens).toBeGreaterThan(0);
  });

  it("lets a new runtime take over a participant after the active control lease expires", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["software_dev"] },
        displayName: "Expiring Codex",
        instanceId: "inst_expiring_codex_1",
        participantId: "part_expiring_codex",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    await currentPool().pool.query(
      `
        UPDATE participant_control_leases
        SET lease_expires_at = now() - interval '1 millisecond'
        WHERE session_id = $1
          AND participant_id = $2
      `,
      [session.sessionId, "part_expiring_codex"],
    );
    const takeover = await request<ParticipantRegistrationResponse>(
      `/sessions/${session.sessionId}/participants`,
      {
        body: {
          capabilities: { workKinds: ["software_dev"] },
          displayName: "Expiring Codex",
          instanceId: "inst_expiring_codex_2",
          participantId: "part_expiring_codex",
          runtimeKind: "codex",
        },
        method: "POST",
      },
    );

    expect(takeover.registrationStatus).toBe("refreshed");
    const snapshots = await request<ControlLeaseSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/control-leases`,
    );
    const oldLease = snapshots.controlLeases.find(
      (lease) =>
        lease.participantId === "part_expiring_codex" &&
        lease.instanceId === "inst_expiring_codex_1",
    );
    const newLease = snapshots.controlLeases.find(
      (lease) =>
        lease.participantId === "part_expiring_codex" &&
        lease.instanceId === "inst_expiring_codex_2",
    );
    expect(oldLease).toMatchObject({ status: "superseded" });
    expect(oldLease?.supersededAt).not.toBeNull();
    expect(newLease).toMatchObject({ status: "active" });
  });

  it("keeps control lease claim and refresh expiry on the DB clock when the app clock is behind", async () => {
    const session = await createSession();
    await withSkewedAppClock(-10_000, () =>
      request(`/sessions/${session.sessionId}/participants`, {
        body: {
          displayName: "Skewed Control",
          instanceId: "inst_skewed_control",
          participantId: "part_skewed_control",
          runtimeKind: "codex",
        },
        method: "POST",
      }),
    );
    let remainingMs = await readControlLeaseRemainingMs(
      session.sessionId,
      "part_skewed_control",
      "inst_skewed_control",
    );
    let snapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    let runtime = snapshots.participants.find(
      (participant) => participant.participantId === "part_skewed_control",
    );
    expect(remainingMs).toBeGreaterThan(0);
    expect(runtime?.currentControlLease).toMatchObject({ status: "active" });

    await withSkewedAppClock(-10_000, () =>
      request(`/sessions/${session.sessionId}/participants`, {
        body: {
          displayName: "Skewed Control",
          instanceId: "inst_skewed_control",
          participantId: "part_skewed_control",
          runtimeKind: "codex",
        },
        method: "POST",
      }),
    );
    remainingMs = await readControlLeaseRemainingMs(
      session.sessionId,
      "part_skewed_control",
      "inst_skewed_control",
    );
    snapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    runtime = snapshots.participants.find(
      (participant) => participant.participantId === "part_skewed_control",
    );

    expect(remainingMs).toBeGreaterThan(0);
    expect(runtime?.currentControlLease).toMatchObject({ status: "active" });
  });

  it("enforces one current control lease per participant in the database", async () => {
    const session = await createSession();
    await currentPool().pool.query(
      `
        INSERT INTO participant_control_leases (
          control_channel,
          instance_id,
          lease_expires_at,
          participant_id,
          session_id
        )
        VALUES ('rest', 'inst_unique_control_1', now() + interval '1 minute', $1, $2)
      `,
      ["part_unique_control", session.sessionId],
    );

    await expect(
      currentPool().pool.query(
        `
          INSERT INTO participant_control_leases (
            control_channel,
            instance_id,
            lease_expires_at,
            participant_id,
            session_id
          )
          VALUES ('rest', 'inst_unique_control_2', now() + interval '1 minute', $1, $2)
        `,
        ["part_unique_control", session.sessionId],
      ),
    ).rejects.toMatchObject({ code: "23505" });
  });

  it("replays one Acquisition ID across replicas without duplicate lease or registration effects", async () => {
    const database = currentPool();
    const sessionId = `sess_rest_acquisition_${randomUUID()}`;
    const participantId = "part_rest_acquisition";
    await createDbSession(database, sessionId);
    const acquisitionInput = {
      acquisitionId: "acq_shared_retry",
      capabilities: {},
      displayName: "REST acquisition participant",
      eventSourceId: "e2e-rest-acquisition",
      instanceId: "inst_rest_acquisition",
      leaseTtlMs: 60_000,
      participantId,
      runtimeKind: "generic_agent",
      sessionId,
    } as const;

    const [first, second] = await Promise.all([
      acquireRestParticipantControl(database, acquisitionInput),
      acquireRestParticipantControl(database, acquisitionInput),
    ]);

    expect([first.status, second.status].sort()).toEqual(["claimed", "replayed"]);
    const generations = await database.pool.query<{ readonly count: number }>(
      `
        SELECT count(*)::int AS count
        FROM participant_control_leases
        WHERE session_id = $1 AND participant_id = $2
      `,
      [sessionId, participantId],
    );
    const registrationEvents = await database.pool.query<{
      readonly count: number;
    }>(
      `
        SELECT count(*)::int AS count
        FROM session_events
        WHERE session_id = $1 AND type = 'participant.joined'
      `,
      [sessionId],
    );
    expect(generations.rows[0]?.count).toBe(1);
    expect(registrationEvents.rows[0]?.count).toBe(1);

    const replacement = await acquireRestParticipantControl(database, {
      ...acquisitionInput,
      acquisitionId: "acq_replacement",
    });
    expect(replacement.status).toBe("superseded");
    const staleReplay = await acquireRestParticipantControl(database, acquisitionInput);
    expect(staleReplay).toEqual({ status: "acquisition_stale" });
    if (replacement.status === "conflict" || replacement.status === "acquisition_stale") {
      throw new Error("Expected replacement acquisition context");
    }
    await releaseControlLease(database, {
      controlChannel: "rest",
      controlEpoch:
        first.status === "conflict" || first.status === "acquisition_stale" ? 0 : first.lease.epoch,
      instanceId: acquisitionInput.instanceId,
      participantId,
      sessionId,
    });
    const current = await database.pool.query<{ readonly epoch: number }>(
      `
        SELECT epoch
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND released_at IS NULL
          AND superseded_at IS NULL
      `,
      [sessionId, participantId],
    );
    expect(Number(current.rows[0]?.epoch)).toBe(replacement.lease.epoch);
  });

  it("keeps mixed-mode missing-epoch behavior explicit without compatibility lease writes", async () => {
    const database = currentPool();
    const sessionId = `sess_rest_mixed_mode_${randomUUID()}`;
    await createDbSession(database, sessionId);
    const compatibility = createSessionServiceEffect(database, {
      controlEpochEnforcement: false,
    });
    const enforced = createSessionServiceEffect(database, {
      controlEpochEnforcement: true,
    });
    const publishInput = {
      eventId: undefined,
      instanceId: undefined,
      payload: {},
      producerId: "part_mixed_mode",
      sessionId,
      type: "client.message",
    } as const;

    const accepted = await Effect.runPromise(compatibility.publishRestEvent(publishInput));
    expect(accepted.status).toBe("created");
    const leases = await database.pool.query<{ readonly count: number }>(
      `
        SELECT count(*)::int AS count
        FROM participant_control_leases
        WHERE session_id = $1
      `,
      [sessionId],
    );
    expect(leases.rows[0]?.count).toBe(0);

    const required = await Effect.runPromise(enforced.publishRestEvent(publishInput));
    expect(required).toEqual({ status: "control_epoch_required" });
    const stale = await Effect.runPromise(
      compatibility.publishRestEvent({
        ...publishInput,
        controlEpoch: 1,
        instanceId: "inst_mixed_mode",
      }),
    );
    expect(stale).toEqual({
      currentEpoch: null,
      status: "control_epoch_stale",
    });
    expect(compatibility.debugInfo().restControl).toMatchObject({
      counts: {
        "session.events.append": {
          epoch_stale: 1,
          unfenced_accepted: 1,
        },
      },
    });
    expect(enforced.debugInfo().restControl).toMatchObject({
      counts: {
        "session.events.append": {
          epoch_required: 1,
        },
      },
    });
  });

  it("returns the distinct required error when REST release omits its epoch", async () => {
    const session = await createSession();
    const participantId = `part_release_required_${randomUUID()}`;
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Release required participant",
        instanceId: "inst_release_required",
        participantId,
        runtimeKind: "generic_agent",
      },
      method: "POST",
    });

    const response = await requestStatus(
      `/sessions/${session.sessionId}/participants/${participantId}/control/release`,
      {
        body: { instanceId: "inst_release_required" },
        method: "POST",
      },
    );

    expect(response.status).toBe(428);
    expect(response.body).toMatchObject({ code: "CONTROL_EPOCH_REQUIRED" });
  });

  it("clears superseded_at when a previously superseded instance reclaims control", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Reclaiming Runtime",
        instanceId: "inst_reclaim_a",
        participantId: "part_reclaim",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    await expireControlLease(session.sessionId, "part_reclaim", "inst_reclaim_a");
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Reclaiming Runtime",
        instanceId: "inst_reclaim_b",
        participantId: "part_reclaim",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    await expireControlLease(session.sessionId, "part_reclaim", "inst_reclaim_b");
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Reclaiming Runtime",
        instanceId: "inst_reclaim_a",
        participantId: "part_reclaim",
        runtimeKind: "codex",
      },
      method: "POST",
    });

    const currentRows = await currentPool().pool.query<{
      readonly count: number;
      readonly releasedAt: string | null;
      readonly supersededAt: string | null;
    }>(
      `
        SELECT
          count(*)::int AS count,
          max(released_at::text) AS "releasedAt",
          max(superseded_at::text) AS "supersededAt"
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND instance_id = $3
          AND released_at IS NULL
          AND superseded_at IS NULL
      `,
      [session.sessionId, "part_reclaim", "inst_reclaim_a"],
    );
    const snapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    const runtime = snapshots.participants.find(
      (participant) => participant.participantId === "part_reclaim",
    );

    expect(currentRows.rows[0]).toMatchObject({
      count: 1,
      releasedAt: null,
      supersededAt: null,
    });
    expect(runtime?.currentControlLease).toMatchObject({
      instanceId: "inst_reclaim_a",
      status: "active",
    });
  });

  it("lets a new runtime take over a participant after WebSocket disconnect releases control", async () => {
    const session = await createSession();
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=part_ws_released&instanceId=inst_ws_released_1&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));
    socket.close();
    await waitForSocketClose(socket);
    const takeover = await waitForAsyncValue(() =>
      request<ParticipantRegistrationResponse>(`/sessions/${session.sessionId}/participants`, {
        body: {
          displayName: "part_ws_released",
          instanceId: "inst_ws_released_2",
          participantId: "part_ws_released",
          runtimeKind: "codex",
        },
        method: "POST",
      }),
    );

    expect(takeover.registrationStatus).toBe("refreshed");
  });

  it("refreshes WebSocket control leases while sockets stay open", async () => {
    const session = await createSession();
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=part_ws_refreshed&instanceId=inst_ws_refreshed_1&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));
    try {
      await sleep(650);

      await expect(
        request(`/sessions/${session.sessionId}/participants`, {
          body: {
            displayName: "part_ws_refreshed",
            instanceId: "inst_ws_refreshed_2",
            participantId: "part_ws_refreshed",
            runtimeKind: "codex",
          },
          method: "POST",
        }),
      ).rejects.toThrow("409");
    } finally {
      socket.close();
      await waitForSocketClose(socket);
    }
    const takeover = await waitForAsyncValue(() =>
      request<ParticipantRegistrationResponse>(`/sessions/${session.sessionId}/participants`, {
        body: {
          displayName: "part_ws_refreshed",
          instanceId: "inst_ws_refreshed_2",
          participantId: "part_ws_refreshed",
          runtimeKind: "codex",
        },
        method: "POST",
      }),
    );

    expect(takeover.registrationStatus).toBe("refreshed");
  });

  it("exposes read-only control lease snapshots", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Debug REST",
        instanceId: "inst_debug_rest",
        participantId: "part_debug_rest",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    const eventsBeforeDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    let snapshots = await request<ControlLeaseSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/control-leases`,
    );
    const restLease = snapshots.controlLeases.find(
      (lease) => lease.participantId === "part_debug_rest",
    );
    const eventsAfterDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );

    expect(restLease).toMatchObject({
      controlChannel: "rest",
      instanceId: "inst_debug_rest",
      status: "active",
    });
    expect(restLease?.releasedAt).toBeNull();
    expect(eventsAfterDebug.events).toHaveLength(eventsBeforeDebug.events.length);

    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&participantId=part_debug_ws&instanceId=inst_debug_ws&runtimeKind=codex`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));
    try {
      snapshots = await request<ControlLeaseSnapshotsResponse>(
        `/sessions/${session.sessionId}/debug/control-leases`,
      );
      const wsLease = snapshots.controlLeases.find(
        (lease) => lease.participantId === "part_debug_ws",
      );
      expect(wsLease).toMatchObject({
        controlChannel: "ws",
        instanceId: "inst_debug_ws",
        status: "active",
      });
    } finally {
      socket.close();
      await waitForSocketClose(socket);
    }
    await waitForAsync(async () => {
      const releasedSnapshots = await request<ControlLeaseSnapshotsResponse>(
        `/sessions/${session.sessionId}/debug/control-leases`,
      );
      const releasedLease = releasedSnapshots.controlLeases.find(
        (lease) => lease.participantId === "part_debug_ws",
      );
      return releasedLease?.status === "released" && releasedLease.releasedAt !== null;
    });

    await currentPool().pool.query(
      `
        UPDATE participant_control_leases
        SET lease_expires_at = now() - interval '1 millisecond'
        WHERE session_id = $1
          AND participant_id = $2
      `,
      [session.sessionId, "part_debug_rest"],
    );
    snapshots = await request<ControlLeaseSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/control-leases`,
    );
    const expiredLease = snapshots.controlLeases.find(
      (lease) => lease.participantId === "part_debug_rest",
    );

    expect(expiredLease).toMatchObject({
      controlChannel: "rest",
      instanceId: "inst_debug_rest",
      status: "expired",
    });
    expect(expiredLease?.releasedAt).toBeNull();

    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Debug REST takeover",
        instanceId: "inst_debug_rest_2",
        participantId: "part_debug_rest",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    snapshots = await request<ControlLeaseSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/control-leases`,
    );
    const supersededLease = snapshots.controlLeases.find(
      (lease) =>
        lease.participantId === "part_debug_rest" && lease.instanceId === "inst_debug_rest",
    );
    expect(supersededLease).toMatchObject({ status: "superseded" });
    expect(supersededLease?.supersededAt).not.toBeNull();
  });

  it("exposes participant runtime snapshots that join presence and control state", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Debug Participant",
        instanceId: "inst_runtime_debug",
        participantId: "part_runtime_debug",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    const eventsBeforeDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    let snapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    const activeSnapshot = snapshots.participants.find(
      (participant) => participant.participantId === "part_runtime_debug",
    );
    const eventsAfterDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );

    expect(activeSnapshot).toMatchObject({
      currentControlLease: {
        controlChannel: "rest",
        instanceId: "inst_runtime_debug",
        status: "active",
      },
      participant: {
        runtimeKind: "codex",
      },
      registered: true,
      status: "registered_control_active",
    });
    expect(activeSnapshot?.participant?.lastSeenAt).not.toBeNull();
    expect(activeSnapshot?.currentControlLease?.lastSeenAt).not.toBeNull();
    expect(eventsAfterDebug.events).toHaveLength(eventsBeforeDebug.events.length);

    await currentPool().pool.query(
      `
        UPDATE participant_control_leases
        SET lease_expires_at = now() - interval '1 millisecond'
        WHERE session_id = $1
          AND participant_id = $2
      `,
      [session.sessionId, "part_runtime_debug"],
    );
    snapshots = await request<ParticipantRuntimeSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/participants`,
    );
    const inactiveSnapshot = snapshots.participants.find(
      (participant) => participant.participantId === "part_runtime_debug",
    );

    expect(inactiveSnapshot?.currentControlLease).toBeNull();
    expect(inactiveSnapshot?.latestControlLease).toMatchObject({
      instanceId: "inst_runtime_debug",
      status: "expired",
    });
    expect(inactiveSnapshot?.status).toBe("registered_control_inactive");
  });

  it("exposes read-only task snapshots with derived lifecycle status", async () => {
    const session = await createSession();
    const releasedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Release this task" },
      method: "POST",
    });
    const eventsBeforeDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    let snapshots = await request<TaskSnapshotsResponse>(
      `/sessions/${session.sessionId}/debug/tasks`,
    );
    const unclaimedSnapshot = snapshots.tasks.find(
      (task) => task.taskId === releasedTask.task.taskId,
    );
    const eventsAfterDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );

    expect(unclaimedSnapshot?.status).toBe("unclaimed");
    expect(eventsAfterDebug.events).toHaveLength(eventsBeforeDebug.events.length);

    await request(`/sessions/${session.sessionId}/tasks/${releasedTask.task.taskId}/claim`, {
      body: {
        instanceId: "inst_task_snapshot",
        participantId: "part_task_snapshot",
      },
      method: "POST",
    });
    snapshots = await request<TaskSnapshotsResponse>(`/sessions/${session.sessionId}/debug/tasks`);
    const activeSnapshot = snapshots.tasks.find((task) => task.taskId === releasedTask.task.taskId);
    expect(activeSnapshot?.status).toBe("claim_active");

    await request(`/sessions/${session.sessionId}/tasks/${releasedTask.task.taskId}/release`, {
      body: {
        instanceId: "inst_task_snapshot",
        participantId: "part_task_snapshot",
      },
      method: "POST",
    });
    snapshots = await request<TaskSnapshotsResponse>(`/sessions/${session.sessionId}/debug/tasks`);
    const clearedSnapshot = snapshots.tasks.find(
      (task) => task.taskId === releasedTask.task.taskId,
    );
    expect(clearedSnapshot?.status).toBe("claim_cleared");
    expect(clearedSnapshot?.releasedAt).not.toBeNull();

    const completedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Complete this task" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/claim`, {
      body: {
        instanceId: "inst_task_snapshot",
        participantId: "part_task_snapshot",
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/complete`, {
      body: {
        instanceId: "inst_task_snapshot",
        participantId: "part_task_snapshot",
        result: { summary: "complete" },
      },
      method: "POST",
    });
    snapshots = await request<TaskSnapshotsResponse>(`/sessions/${session.sessionId}/debug/tasks`);
    const completedSnapshot = snapshots.tasks.find(
      (task) => task.taskId === completedTask.task.taskId,
    );

    expect(completedSnapshot?.status).toBe("completed");
    expect(completedSnapshot?.completedAt).not.toBeNull();

    const approvalTask = await createCompletedGenericApprovalTask(session.sessionId, "debug");
    const approval = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${approvalTask.task.taskId}/approval`,
      {
        body: {
          decision: "approved",
          instanceId: "inst_task_snapshot_approval",
          participantId: "part_task_snapshot_approval",
          reason: { approvalTarget: { action: "keep", key: "message-1" } },
        },
        method: "POST",
      },
    );
    snapshots = await request<TaskSnapshotsResponse>(`/sessions/${session.sessionId}/debug/tasks`);
    const approvalSnapshot = snapshots.tasks.find(
      (task) => task.taskId === approvalTask.task.taskId,
    );

    expect(approval.status).toBe("recorded");
    expect(approvalSnapshot?.approvals).toEqual([
      expect.objectContaining({
        approvalEventId: approval.event?.eventId,
        decidedByParticipantId: "part_task_snapshot_approval",
        decision: "approved",
        targetKey: "approvalTarget:keep:message-1",
        taskId: approvalTask.task.taskId,
      }),
    ]);
  });

  it("exposes an aggregate session debug summary", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        displayName: "Summary Participant",
        instanceId: "inst_summary",
        participantId: "part_summary",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Keep this task claimable" },
      method: "POST",
    });
    const activeTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Keep this task claimed" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${activeTask.task.taskId}/claim`, {
      body: { instanceId: "inst_summary", participantId: "part_summary" },
      method: "POST",
    });
    const completedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Complete this summary task" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/claim`, {
      body: { instanceId: "inst_summary", participantId: "part_summary" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/complete`, {
      body: {
        instanceId: "inst_summary",
        participantId: "part_summary",
        result: { summary: "complete" },
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${activeTask.task.taskId}/claim/refresh`, {
      body: { instanceId: "inst_summary", participantId: "part_summary" },
      method: "POST",
    });
    const eventsBeforeDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    const summary = await request<SessionDebugSummaryResponse>(
      `/sessions/${session.sessionId}/debug/summary`,
    );
    const eventsAfterDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );

    expect(summary.summary.participants).toMatchObject({
      activeControl: 1,
      registered: 1,
      total: 1,
    });
    expect(summary.summary.controlLeases).toEqual({
      active: 1,
      expired: 0,
      released: 0,
      superseded: 0,
      total: 1,
    });
    expect(summary.summary.tasks).toMatchObject({
      activeClaims: 1,
      claimable: 1,
      completed: 1,
      terminal: 1,
      total: 3,
      unclaimed: 1,
    });
    expect(eventsAfterDebug.events).toHaveLength(eventsBeforeDebug.events.length);
  });

  it("exposes read-only process-local server debug info", async () => {
    const session = await createSession();
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));

    const eventsBeforeDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    const debug = await request<ServerDebugResponse>("/debug/server");
    const eventsAfterDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );

    expect(debug.server.eventFanout.connected).toBe(true);
    expect(debug.server.eventFanout.listenerState).toBe("connected");
    expect(debug.server.eventFanout.listenerErrorCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.reconnectAttemptCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.reconnectSuccessCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.lastListenerError).toBeNull();
    expect(debug.server.eventFanout.lastDisconnectedAt).toBeNull();
    expect(debug.server.eventFanout.lastConnectedAt).toEqual(expect.any(String));
    expect(debug.server.eventFanout.lastReconnectDelayMs).toBeNull();
    expect(debug.server.eventFanout.fanoutCursorSessionCount).toBe(0);
    expect(debug.server.eventFanout.sessionCursorCount).toBeGreaterThanOrEqual(1);
    expect(debug.server.eventFanout.catchUpPollIntervalMs).toBe(1_000);
    expect(debug.server.eventFanout.notificationCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.broadcastCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.catchUpPollCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.catchUpBatchCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.eventFanout.catchUpEventCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.hub.sessionCount).toBeGreaterThanOrEqual(1);
    expect(debug.server.hub.socketCount).toBeGreaterThanOrEqual(1);
    expect(debug.server.hub.backpressureCloseCount).toBeGreaterThanOrEqual(0);
    expect(debug.server.resourceLimits.limits).toEqual(defaultResourceLimits);
    expect(debug.server.resourceLimits.counters).toMatchObject({
      bodyTooLargeCount: expect.any(Number) as number,
      replayWindowExceededCount: expect.any(Number) as number,
      wsPayloadTooLargeCount: expect.any(Number) as number,
      wsRateLimitedCount: expect.any(Number) as number,
    });
    expect(debug.server.service.eventSourceId).toMatch(/^src_/u);
    expect(debug.server.service.restControlLeaseTtlMs).toBeGreaterThan(0);
    expect(debug.server.service.taskClaimLeaseTtlMs).toBe(200);
    expect(debug.server.service.wsControlLeaseTtlMs).toBe(1_000);
    expect(debug.server.taskClaimSweeper.enabled).toBe(true);
    expect(debug.server.taskClaimSweeper.intervalMs).toBe(50);
    expect(typeof debug.server.taskClaimSweeper.running).toBe("boolean");
    expect(typeof debug.server.taskClaimSweeper.scheduled).toBe("boolean");
    expect(eventsAfterDebug.events).toHaveLength(eventsBeforeDebug.events.length);

    socket.close();
    await waitForSocketClose(socket);
  });

  it("cancels tasks and rejects later claims", async () => {
    const session = await createSession();
    await request(`/sessions/${session.sessionId}/participants`, {
      body: {
        capabilities: { workKinds: ["software_dev"] },
        displayName: "Codex Cancellation E2E",
        instanceId: "inst_codex_cancel_e2e",
        participantId: "part_codex_cancel_e2e",
        runtimeKind: "codex",
      },
      method: "POST",
    });
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Cancel the e2e task" },
      method: "POST",
    });

    const cancellation = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/cancel`,
      {
        body: {
          instanceId: "inst_codex_cancel_e2e",
          participantId: "part_codex_cancel_e2e",
          reason: { message: "obsolete" },
        },
        method: "POST",
      },
    );

    await expect(
      request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
        body: {
          instanceId: "inst_codex_cancel_e2e",
          participantId: "part_codex_cancel_e2e",
        },
        method: "POST",
      }),
    ).rejects.toThrow("409");
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);

    expect(cancellation.task.cancelledAt).not.toBeNull();
    expect(events.events.map((event) => event.type)).toContain("control.cancel");
  });

  it("lets another participant reclaim expired task claims", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Recover this expired claim" },
      method: "POST",
    });
    const firstClaim = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_first_claimant",
          participantId: "part_first_claimant",
        },
        method: "POST",
      },
    );

    await waitForAsync(async () => {
      const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
      return events.events.some((event) => event.type === "task.claim_expired");
    });
    await expect(
      request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`, {
        body: {
          instanceId: "inst_first_claimant",
          participantId: "part_first_claimant",
          result: { summary: "too late" },
        },
        method: "POST",
      }),
    ).rejects.toThrow("409");
    const secondClaim = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_second_claimant",
          participantId: "part_second_claimant",
        },
        method: "POST",
      },
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);

    expect(firstClaim.task.claimExpiresAt).not.toBeNull();
    expect(secondClaim.task.claimedBy).toBe("part_second_claimant");
    expect(events.events.filter((event) => event.type === "task.claimed")).toHaveLength(2);
    expect(events.events.map((event) => event.type)).toContain("task.claim_expired");
    expectTaskClaimExpiredConsistency(
      events.events,
      task.task.taskId,
      "part_first_claimant",
      secondClaim.task.sessionId,
    );
  });

  it("expires concurrent task claim batches without duplicate or missing claim-expired events", async () => {
    const expirationDatabaseName = `tether_e2e_expiration_${randomUUID().replaceAll("-", "_")}`;
    await createDatabase(expirationDatabaseName);
    const expirationPool = createPool(buildDatabaseUrl(expirationDatabaseName));
    const batch = [
      {
        sessionId: `sess_expiration_c_${randomUUID()}`,
        taskId: `task_expiration_c_${randomUUID()}`,
      },
      {
        sessionId: `sess_expiration_a_${randomUUID()}`,
        taskId: `task_expiration_b_${randomUUID()}`,
      },
      {
        sessionId: `sess_expiration_a_${randomUUID()}`,
        taskId: `task_expiration_a_${randomUUID()}`,
      },
      {
        sessionId: `sess_expiration_b_${randomUUID()}`,
        taskId: `task_expiration_a_${randomUUID()}`,
      },
    ] as const;
    try {
      await migrate(expirationPool);
      for (const item of batch) {
        await createDbClaimedTask(item.sessionId, item.taskId, expirationPool);
      }
      await expireClaimsNow(batch, expirationPool);

      const singleBatchEvents = await expireTaskClaims(expirationPool, {
        batchSize: batch.length,
        sourceId: "src_expiration_order_e2e",
      });
      expect(
        singleBatchEvents.map((event) => `${event.sessionId}:${taskIdFromEventPayload(event)}`),
      ).toEqual(
        [...batch]
          .sort((left, right) => {
            const sessionOrder = left.sessionId.localeCompare(right.sessionId);
            return sessionOrder === 0 ? left.taskId.localeCompare(right.taskId) : sessionOrder;
          })
          .map((item) => `${item.sessionId}:${item.taskId}`),
      );

      const concurrentBatch = [
        {
          sessionId: `sess_expiration_concurrent_a_${randomUUID()}`,
          taskId: `task_expiration_concurrent_1_${randomUUID()}`,
        },
        {
          sessionId: `sess_expiration_concurrent_b_${randomUUID()}`,
          taskId: `task_expiration_concurrent_2_${randomUUID()}`,
        },
        {
          sessionId: `sess_expiration_concurrent_c_${randomUUID()}`,
          taskId: `task_expiration_concurrent_3_${randomUUID()}`,
        },
      ] as const;
      for (const item of concurrentBatch) {
        await createDbClaimedTask(item.sessionId, item.taskId, expirationPool);
      }
      await expireClaimsNow(concurrentBatch, expirationPool);

      const concurrentResults = await Promise.all([
        expireTaskClaims(expirationPool, {
          batchSize: concurrentBatch.length,
          sourceId: "src_expiration_concurrent_a_e2e",
        }),
        expireTaskClaims(expirationPool, {
          batchSize: concurrentBatch.length,
          sourceId: "src_expiration_concurrent_b_e2e",
        }),
      ]);
      const expiredEvents = concurrentResults.flat();
      const expiredKeys = expiredEvents.map(
        (event) => `${event.sessionId}:${taskIdFromEventPayload(event)}`,
      );

      expect(new Set(expiredKeys).size).toBe(expiredKeys.length);
      expect(expiredKeys.sort()).toEqual(
        concurrentBatch.map((item) => `${item.sessionId}:${item.taskId}`).sort(),
      );
      for (const item of concurrentBatch) {
        const task = await getTask(expirationPool, item);
        expect(task).toMatchObject({
          claimExpiredAt: expect.any(String),
          claimExpiredBy: `part_expiration_${item.taskId}`,
          claimExpiresAt: null,
          claimedAt: null,
          claimedBy: null,
          releasedAt: null,
          releasedBy: null,
        });
      }
    } finally {
      await expirationPool.end();
      await dropDatabase(expirationDatabaseName);
    }
  });

  it("keeps task claim expiry on the DB clock when the app clock is behind", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Claim under skew" },
      method: "POST",
    });

    const claimed = await withSkewedAppClock(-10_000, () =>
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
        body: {
          instanceId: "inst_skewed_claim",
          participantId: "part_skewed_claim",
        },
        method: "POST",
      }),
    );
    const remainingMs = await readTaskClaimRemainingMs(session.sessionId, task.task.taskId);
    const completed = await withSkewedAppClock(-10_000, () =>
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`, {
        body: {
          instanceId: "inst_skewed_claim",
          participantId: "part_skewed_claim",
          result: { summary: "completed before DB TTL elapsed" },
        },
        method: "POST",
      }),
    );

    expect(claimed.task.claimExpiresAt).not.toBeNull();
    expect(remainingMs).toBeGreaterThan(0);
    expect(completed.task.completedAt).not.toBeNull();
  });

  it("keeps task claim refresh expiry on the DB clock when the app clock is behind", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Refresh under skew" },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: {
        instanceId: "inst_skewed_refresh",
        participantId: "part_skewed_refresh",
      },
      method: "POST",
    });

    const refreshed = await withSkewedAppClock(-10_000, () =>
      request<TaskResponse>(
        `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim/refresh`,
        {
          body: {
            instanceId: "inst_skewed_refresh",
            participantId: "part_skewed_refresh",
          },
          method: "POST",
        },
      ),
    );
    const remainingMs = await readTaskClaimRemainingMs(session.sessionId, task.task.taskId);
    const completed = await withSkewedAppClock(-10_000, () =>
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`, {
        body: {
          instanceId: "inst_skewed_refresh",
          participantId: "part_skewed_refresh",
          result: { summary: "refreshed before DB TTL elapsed" },
        },
        method: "POST",
      }),
    );

    expect(refreshed.task.claimExpiresAt).not.toBeNull();
    expect(remainingMs).toBeGreaterThan(0);
    expect(completed.task.completedAt).not.toBeNull();
  });

  it("broadcasts task claim-expired events from the scheduler", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "software_dev",
        objective: "Broadcast this expired claim",
      },
      method: "POST",
    });
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));

    await request(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`, {
      body: {
        instanceId: "inst_stale_claimant",
        participantId: "part_stale_claimant",
      },
      method: "POST",
    });

    await waitFor(() => messages.some(isTaskClaimExpiredEnvelope));
    const claim = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_recovery_claimant",
          participantId: "part_recovery_claimant",
        },
        method: "POST",
      },
    );

    expect(claim.task.claimedBy).toBe("part_recovery_claimant");
    socket.close();
    await waitForSocketClose(socket);
  });

  it("broadcasts live WebSocket events", async () => {
    const session = await createSession();
    const socket = new WebSocket(
      authenticatedWebSocketUrl(
        `${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
      ),
    );
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isReplayCompleteEnvelope));

    await request(`/sessions/${session.sessionId}/events`, {
      body: {
        payload: { text: "broadcast" },
        producerId: "e2e",
        type: "user.message",
      },
      method: "POST",
    });

    await waitFor(() =>
      messages.some((message) => isEventEnvelope(message) && message.event.type === "user.message"),
    );
    socket.close();
    await waitForSocketClose(socket);
  });

  it("reconnects participant runtime clients from the last observed sequence", async () => {
    const session = await createSession();
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "text", objective: "Replay once after reconnect" },
      method: "POST",
    });
    const client = await ParticipantRuntimeClient.connect({
      afterSeq: 0,
      authToken: mintE2eToken({
        participantId: "part_runtime_client_e2e",
        role: "participant",
        sessionId: session.sessionId,
      }),
      capabilities: { workKinds: ["text"] },
      displayName: "Runtime Client E2E",
      instanceId: "inst_runtime_client_e2e",
      participantId: "part_runtime_client_e2e",
      runtimeKind: "codex",
      serviceUrl: baseUrl,
      sessionId: session.sessionId,
    });
    const firstReplayEvents: SessionEvent[] = [];
    client.onEvent((event) => {
      firstReplayEvents.push(event);
    });
    await client.waitForReplayComplete();

    const reconnected = await client.reconnect();
    const reconnectedEvents: SessionEvent[] = [];
    reconnected.onEvent((event) => {
      reconnectedEvents.push(event);
    });
    await reconnected.waitForReplayComplete();
    await request(`/sessions/${session.sessionId}/events`, {
      body: {
        payload: { text: "after reconnect" },
        producerId: "runtime-client-e2e",
        type: "user.message",
      },
      method: "POST",
    });
    await waitFor(() =>
      reconnectedEvents.some(
        (event) => event.type === "user.message" && event.producerId === "runtime-client-e2e",
      ),
    );

    expect(firstReplayEvents.map((event) => event.type)).toContain("task.created");
    expect(
      reconnectedEvents.some(
        (event) =>
          event.type === "task.created" &&
          "task" in event.payload &&
          typeof event.payload.task === "object" &&
          event.payload.task !== null &&
          "taskId" in event.payload.task &&
          event.payload.task.taskId === task.task.taskId,
      ),
    ).toBe(false);

    reconnected.close();
    await reconnected.waitForClose();
  });

  it("automatically reconnects participant runtime clients after transport loss", async () => {
    const session = await createSession();
    const client = await ParticipantRuntimeClient.connect({
      afterSeq: 0,
      authToken: mintE2eToken({
        participantId: "part_auto_reconnect_runtime_client_e2e",
        role: "participant",
        sessionId: session.sessionId,
      }),
      capabilities: { workKinds: ["text"] },
      displayName: "Auto Reconnect Runtime Client E2E",
      instanceId: "inst_auto_reconnect_runtime_client_e2e",
      participantId: "part_auto_reconnect_runtime_client_e2e",
      reconnect: { baseDelayMs: 100, maxDelayMs: 100 },
      runtimeKind: "codex",
      serviceUrl: baseUrl,
      sessionId: session.sessionId,
    });
    const observedEvents: SessionEvent[] = [];
    client.onEvent((event) => {
      observedEvents.push(event);
    });
    const taskLoop = client.runClaimableTasks({
      claimRefreshMs: 50,
      executor: async () => ({ result: { output: "unused" } }),
      once: false,
      shouldClaimTask: () => false,
    });
    taskLoop.catch((error: unknown) => {
      console.error(error);
    });
    await client.waitForReplayComplete();

    client.disconnect();
    await waitFor(() => client.debugInfo().socketReadyState === WebSocket.CLOSED);
    await request(`/sessions/${session.sessionId}/events`, {
      body: {
        payload: { text: "missed during reconnect" },
        producerId: "runtime-client-auto-reconnect-e2e",
        type: "user.message",
      },
      method: "POST",
    });
    await waitFor(() =>
      observedEvents.some(
        (event) =>
          event.type === "user.message" && event.producerId === "runtime-client-auto-reconnect-e2e",
      ),
    );

    expect(client.debugInfo().reconnectSuccessCount).toBeGreaterThan(0);

    client.close();
    await taskLoop;
  });

  it("rejects permanent delete when remote Host Presence is outside replica scope", async () => {
    const replicaA = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      runtimeTopology: "multi",
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const replicaB = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      runtimeTopology: "multi",
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    let replicaAStarted = false;
    let replicaBStarted = false;
    let host: WebSocket | null = null;
    try {
      const portA = await findOpenPort();
      const portB = await findOpenPort();
      await replicaA.listen(portA);
      replicaAStarted = true;
      await replicaB.listen(portB);
      replicaBStarted = true;
      const replicaAUrl = `http://127.0.0.1:${portA}`;
      const replicaBUrl = `http://127.0.0.1:${portB}`;
      const session = (
        await requestFrom<SessionResponse>(replicaBUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      await requestFrom(replicaBUrl, `/sessions/${session.sessionId}/events`, {
        body: {
          payload: { archived: true },
          producerId: "replica-delete-e2e",
          type: "session.archived",
        },
        method: "POST",
      });
      host = new WebSocket(
        authenticatedWebSocketUrl(
          `${replicaAUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&runtimeKind=host&participantId=part_remote_host&instanceId=inst_remote_host&displayName=Remote%20Host`,
        ),
      );
      await waitForSocketOpen(host);
      await waitFor(() => replicaA.debugInfo().hostPresence.passiveSocketCount === 1);

      const response = await requestStatusFrom<PermanentDeleteResponse>(
        replicaBUrl,
        `/sessions/${session.sessionId}/delete`,
        {
          authToken: mintE2eToken({
            participantId: "part_replica_delete_admin",
            role: "admin",
            sessionId: "*",
          }),
          method: "POST",
        },
      );

      expect(replicaB.debugInfo().hostPresence.passiveSocketCount).toBe(0);
      expect(response.status).toBe(409);
      expect(response.body).toMatchObject({
        ok: false,
        reason: "presence_scope_insufficient",
      });
    } finally {
      if (host && host.readyState !== WebSocket.CLOSED) {
        host.close();
        await waitForSocketClose(host);
      }
      if (replicaBStarted) {
        await replicaB.close();
      }
      if (replicaAStarted) {
        await replicaA.close();
      }
    }
  });

  it("fans out committed events across app replicas", async () => {
    const replicaA = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 200,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const replicaB = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 200,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    let replicaAStarted = false;
    let replicaBStarted = false;
    let socketA: WebSocket | null = null;
    let socketB: WebSocket | null = null;
    try {
      const portA = await findOpenPort();
      const portB = await findOpenPort();
      await replicaA.listen(portA);
      replicaAStarted = true;
      await replicaB.listen(portB);
      replicaBStarted = true;
      const replicaAUrl = `http://127.0.0.1:${portA}`;
      const replicaBUrl = `http://127.0.0.1:${portB}`;
      const session = (
        await requestFrom<SessionResponse>(replicaBUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      await currentPool().pool.query(
        `
          UPDATE session_event_sequences
          SET next_seq = $1
          WHERE session_id = $2
        `,
        [2_147_483_648, session.sessionId],
      );
      socketA = new WebSocket(
        authenticatedWebSocketUrl(
          `${replicaAUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=2147483647`,
        ),
      );
      socketB = new WebSocket(
        authenticatedWebSocketUrl(
          `${replicaBUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=2147483647&runtimeKind=observer`,
        ),
      );
      const messagesA: unknown[] = [];
      const messagesB: unknown[] = [];
      socketA.on("message", (data) => {
        messagesA.push(JSON.parse(String(data)) as unknown);
      });
      socketB.on("message", (data) => {
        messagesB.push(JSON.parse(String(data)) as unknown);
      });
      await Promise.all([waitForSocketOpen(socketA), waitForSocketOpen(socketB)]);
      await waitFor(
        () => messagesA.some(isReplayCompleteEnvelope) && messagesB.some(isReplayCompleteEnvelope),
      );

      await requestFrom(replicaBUrl, `/sessions/${session.sessionId}/events`, {
        body: {
          payload: { text: "replica fanout" },
          producerId: "replica-b",
          type: "user.message",
        },
        method: "POST",
      });

      await waitFor(() =>
        [messagesA, messagesB].every((messages) =>
          messages.some(
            (message) =>
              isEventEnvelope(message) &&
              message.event.type === "user.message" &&
              message.event.producerId === "replica-b" &&
              message.event.seq === 2_147_483_648,
          ),
        ),
      );
    } finally {
      for (const socket of [socketA, socketB]) {
        if (socket && socket.readyState !== WebSocket.CLOSED) {
          socket.close();
          await waitForSocketClose(socket);
        }
      }
      if (replicaBStarted) {
        await replicaB.close();
      }
      if (replicaAStarted) {
        await replicaA.close();
      }
    }
  });

  it("catches up missed cross-replica events from the durable log", async () => {
    const replicaA = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 50, listenEnabled: false },
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 200,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const replicaB = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 200,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    let replicaAStarted = false;
    let replicaBStarted = false;
    let socket: WebSocket | null = null;
    try {
      const portA = await findOpenPort();
      const portB = await findOpenPort();
      await replicaA.listen(portA);
      replicaAStarted = true;
      await replicaB.listen(portB);
      replicaBStarted = true;
      const replicaAUrl = `http://127.0.0.1:${portA}`;
      const replicaBUrl = `http://127.0.0.1:${portB}`;
      const session = (
        await requestFrom<SessionResponse>(replicaBUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      socket = new WebSocket(
        authenticatedWebSocketUrl(
          `${replicaAUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0`,
        ),
      );
      const messages: unknown[] = [];
      socket.on("message", (data) => {
        messages.push(JSON.parse(String(data)) as unknown);
      });
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));

      await requestFrom(replicaBUrl, `/sessions/${session.sessionId}/events`, {
        body: {
          payload: { text: "catch-up fanout" },
          producerId: "replica-b-catch-up",
          type: "user.message",
        },
        method: "POST",
      });

      await waitFor(() =>
        messages.some(
          (message) =>
            isEventEnvelope(message) &&
            message.event.type === "user.message" &&
            message.event.producerId === "replica-b-catch-up",
        ),
      );
      expect(replicaA.debugInfo().eventFanout.catchUpPollCount).toBeGreaterThan(0);
    } finally {
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      if (replicaBStarted) {
        await replicaB.close();
      }
      if (replicaAStarted) {
        await replicaA.close();
      }
    }
  });

  it("fails readiness on fanout catch-up failure and recovers after durable repair", async () => {
    let failCatchUp = false;
    const durableService = createSessionServiceEffect(currentPool(), {
      controlEpochEnforcement: false,
      taskClaimLeaseTtlMs: 200,
      wsControlLeaseTtlMs: 200,
    });
    const replicaA = createAppServerWithSessionService(
      currentPool(),
      {
        ...durableService,
        listEvents: (sessionId, afterSeq, options) =>
          failCatchUp
            ? Effect.die(new Error("simulated fanout catch-up failure"))
            : durableService.listEvents(sessionId, afterSeq, options),
      },
      {
        auth: e2eAuthOptions,
        eventFanout: { catchUpPollIntervalMs: 10, listenEnabled: false },
        readiness: { fanoutStaleAfterMs: 1 },
        runtimeTopology: "multi",
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    const replicaB = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0 },
      runtimeTopology: "multi",
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    let replicaAStarted = false;
    let replicaBStarted = false;
    let socket: WebSocket | null = null;
    try {
      const portA = await findOpenPort();
      const portB = await findOpenPort();
      await replicaA.listen(portA);
      replicaAStarted = true;
      await replicaB.listen(portB);
      replicaBStarted = true;
      const replicaAUrl = `http://127.0.0.1:${portA}`;
      const replicaBUrl = `http://127.0.0.1:${portB}`;
      const session = (
        await requestFrom<SessionResponse>(replicaBUrl, "/sessions", {
          body: {},
          method: "POST",
        })
      ).session;
      socket = new WebSocket(
        authenticatedWebSocketUrl(
          `${replicaAUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream?after=0&runtimeKind=observer`,
        ),
      );
      const messages: unknown[] = [];
      socket.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));

      failCatchUp = true;
      await waitFor(
        () =>
          replicaA.debugInfo().eventFanout.catchUpFailureCount > 0 &&
          (replicaA.debugInfo().eventFanout.sessionLag[0]?.lagAgeMs ?? 0) > 1,
      );
      const failedReadiness = await fetch(`${replicaAUrl}/ready`);

      expect(failedReadiness.status).toBe(503);
      expect(await failedReadiness.json()).toMatchObject({
        ready: false,
        reason: "fanout_catchup_stale",
      });

      failCatchUp = false;
      await waitFor(() => replicaA.debugInfo().eventFanout.catchUpRecoveryCount > 0);
      const recoveredReadiness = await fetch(`${replicaAUrl}/ready`);

      expect(recoveredReadiness.status).toBe(200);
      expect(await recoveredReadiness.json()).toMatchObject({ ready: true });
    } finally {
      if (socket && socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      if (replicaBStarted) {
        await replicaB.close();
      }
      if (replicaAStarted) {
        await replicaA.close();
      }
    }
  });

  /**
   * Creates a session through the public HTTP API for e2e setup.
   */
  async function createSession(): Promise<SessionResponse["session"]> {
    const response = await request<SessionResponse>("/sessions", {
      body: {},
      method: "POST",
    });
    return response.session;
  }

  /**
   * Returns the initialized test database pool.
   */
  function currentPool(): DatabasePool {
    if (!pool) {
      throw new Error("Database pool is not initialized");
    }
    return pool;
  }

  /** Reads the durable allocator cursor for one session as a safe event sequence. */
  async function readNextEventSequence(database: DatabasePool, sessionId: string): Promise<number> {
    const rows = await database.pool.query<{ readonly nextSeq: string }>(
      `
        SELECT next_seq::text AS "nextSeq"
        FROM session_event_sequences
        WHERE session_id = $1
      `,
      [sessionId],
    );
    const nextSeq = Number(rows.rows[0]?.nextSeq);
    if (!Number.isSafeInteger(nextSeq) || nextSeq <= 0) {
      throw new Error(`Invalid next event sequence for ${sessionId}`);
    }
    return nextSeq;
  }

  /** Creates one public REST lease and claimed task for a Control Epoch race. */
  async function prepareControlEpochRaceFixture(label: string): Promise<{
    readonly claimed: TaskResponse;
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly session: SessionResponse["session"];
    readonly task: TaskResponse;
  }> {
    const session = await createSession();
    const participantId = `part_epoch_${label}_${randomUUID()}`;
    const instanceId = `inst_epoch_${label}_${randomUUID()}`;
    const setupApp = createControlEpochRaceApp(currentPool(), 60_000);
    const setupPort = await findOpenPort();
    await setupApp.listen(setupPort);
    const setupBaseUrl = `http://127.0.0.1:${setupPort}`;
    try {
      const acquisition = await requestFrom<ParticipantRegistrationResponse>(
        setupBaseUrl,
        `/sessions/${session.sessionId}/participants`,
        {
          body: {
            acquisitionId: `acq_epoch_n_${randomUUID()}`,
            controlChannel: "rest",
            displayName: `Epoch ${label} participant`,
            instanceId,
            participantId,
            runtimeKind: "generic_agent",
          },
          method: "POST",
        },
      );
      const controlEpoch = acquisition.controlEpoch;
      if (controlEpoch === undefined) {
        throw new Error("Initial REST acquisition did not return a Control Epoch");
      }
      const task = await requestFrom<TaskResponse>(
        setupBaseUrl,
        `/sessions/${session.sessionId}/tasks`,
        {
          body: { kind: "software_dev", objective: "Refresh under epoch serialization" },
          method: "POST",
        },
      );
      const claimed = await requestFrom<TaskResponse>(
        setupBaseUrl,
        `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
        {
          body: { controlEpoch, instanceId, participantId },
          method: "POST",
        },
      );
      return { claimed, controlEpoch, instanceId, participantId, session, task };
    } finally {
      await setupApp.close();
    }
  }

  /** Builds a scoped enforced REST server without fanout or sweep background work. */
  function createControlEpochRaceApp(
    database: DatabasePool,
    taskClaimLeaseTtlMs: number,
  ): AppServer {
    return createAppServer(database, {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      sessionService: {
        controlEpochEnforcement: true,
        taskClaimLeaseTtlMs,
        wsControlLeaseTtlMs: 60_000,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
  }

  /** Builds one scoped claimant server without fanout or background sweep work. */
  function createTaskClaimRaceApp(database: DatabasePool): AppServer {
    return createAppServer(database, {
      auth: e2eAuthOptions,
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 60_000,
        wsControlLeaseTtlMs: 60_000,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
  }

  /**
   * Starts a scoped app-server replica whose claim command fails after command
   * parsing, preserving production gateway behavior.
   */
  async function createFailingClaimAppServer(errorMessage: string): Promise<{
    readonly app: AppServer;
    readonly baseUrl: string;
  }> {
    const service = createSessionServiceEffect(currentPool(), {
      taskClaimLeaseTtlMs: 200,
      wsControlLeaseTtlMs: 1_000,
    });
    const app = createAppServerWithSessionService(
      currentPool(),
      {
        ...service,
        claimTask: () => Effect.fail(new Error(errorMessage) as never),
      },
      {
        auth: e2eAuthOptions,
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    const port = await findOpenPort();
    await app.listen(port);
    return { app, baseUrl: `http://127.0.0.1:${port}` };
  }

  /** Creates a claimed task whose lease can be elapsed later in one batch. */
  async function createDbClaimedTask(
    sessionId: string,
    taskId: string,
    targetPool: DatabasePool = currentPool(),
  ): Promise<void> {
    await createDbSession(targetPool, sessionId);
    await createTaskWithEvent(targetPool, {
      eventSourceId: "src_expiration_setup_e2e",
      kind: "software_dev",
      objective: "Expire this claim",
      sessionId,
      taskId,
    });
    await claimTaskWithEvent(targetPool, {
      claimLeaseTtlMs: 60_000,
      eventSourceId: "src_expiration_setup_e2e",
      participantId: `part_expiration_${taskId}`,
      sessionId,
      taskId,
    });
  }

  /** Marks prepared task claims expired in one DB write to avoid sweeper races. */
  async function expireClaimsNow(
    claims: readonly { readonly sessionId: string; readonly taskId: string }[],
    targetPool: DatabasePool = currentPool(),
  ): Promise<void> {
    const pairs = claims
      .map((_claim, index) => `($${index * 2 + 1}::text, $${index * 2 + 2}::text)`)
      .join(", ");
    const values = claims.flatMap((claim) => [claim.sessionId, claim.taskId]);
    await targetPool.pool.query(
      `
        UPDATE tasks
        SET claim_expires_at = now() - interval '1 millisecond'
        FROM (VALUES ${pairs}) AS expired(session_id, task_id)
        WHERE tasks.session_id = expired.session_id
          AND tasks.task_id = expired.task_id
      `,
      values,
    );
  }

  /** Runs an operation while the app process clock is skewed by the given delta. */
  async function withSkewedAppClock<TValue>(
    deltaMs: number,
    operation: () => Promise<TValue>,
  ): Promise<TValue> {
    const realDateNow = Date.now;
    Date.now = () => realDateNow() + deltaMs;
    try {
      return await operation();
    } finally {
      Date.now = realDateNow;
    }
  }

  /** Reads task-claim lease time remaining relative to the database clock. */
  async function readTaskClaimRemainingMs(sessionId: string, taskId: string): Promise<number> {
    const rows = await currentPool().pool.query<{
      readonly remainingMs: string | number | null;
    }>(
      `
        SELECT EXTRACT(EPOCH FROM (claim_expires_at - now())) * 1000 AS "remainingMs"
        FROM tasks
        WHERE session_id = $1
          AND task_id = $2
      `,
      [sessionId, taskId],
    );
    return Number(rows.rows[0]?.remainingMs ?? Number.NaN);
  }

  /** Reads control-lease time remaining relative to the database clock. */
  async function readControlLeaseRemainingMs(
    sessionId: string,
    participantId: string,
    instanceId: string,
  ): Promise<number> {
    const rows = await currentPool().pool.query<{
      readonly remainingMs: string | number | null;
    }>(
      `
        SELECT EXTRACT(EPOCH FROM (lease_expires_at - now())) * 1000 AS "remainingMs"
        FROM participant_control_leases
        WHERE session_id = $1
          AND participant_id = $2
          AND instance_id = $3
      `,
      [sessionId, participantId, instanceId],
    );
    return Number(rows.rows[0]?.remainingMs ?? Number.NaN);
  }

  /** Forces one control lease to expire relative to the database clock. */
  async function expireControlLease(
    sessionId: string,
    participantId: string,
    instanceId: string,
  ): Promise<void> {
    await currentPool().pool.query(
      `
        UPDATE participant_control_leases
        SET lease_expires_at = now() - interval '1 millisecond'
        WHERE session_id = $1
          AND participant_id = $2
          AND instance_id = $3
      `,
      [sessionId, participantId, instanceId],
    );
  }

  /** Creates and claims a task through the public REST lifecycle. */
  async function createClaimedTask(
    sessionId: string,
    objective: string,
    controller: { readonly instanceId: string; readonly participantId: string },
  ): Promise<TaskResponse> {
    const task = await request<TaskResponse>(`/sessions/${sessionId}/tasks`, {
      body: { kind: "software_dev", objective },
      method: "POST",
    });
    await request(`/sessions/${sessionId}/tasks/${task.task.taskId}/claim`, {
      body: controller,
      method: "POST",
    });
    return task;
  }

  /** Creates a completed email task that can receive approval decisions. */
  async function createCompletedGenericApprovalTask(
    sessionId: string,
    idSuffix: string,
  ): Promise<TaskResponse> {
    const task = await request<TaskResponse>(`/sessions/${sessionId}/tasks`, {
      body: {
        kind: "generic_approval_request",
        objective: `generic approval ${idSuffix}`,
      },
      method: "POST",
    });
    const controller = {
      instanceId: `inst_email_${idSuffix.replaceAll("-", "_")}_e2e`,
      participantId: `part_email_${idSuffix.replaceAll("-", "_")}_e2e`,
    };
    await request(`/sessions/${sessionId}/tasks/${task.task.taskId}/claim`, {
      body: controller,
      method: "POST",
    });
    await request<TaskResponse>(`/sessions/${sessionId}/tasks/${task.task.taskId}/complete`, {
      body: {
        ...controller,
        result: createGenericApprovalResult(),
      },
      method: "POST",
    });
    return task;
  }

  /** Applies the earliest task-claim-expiry migration prefix. */
  async function applyLegacyMigrationsThrough0003(database: DatabasePool): Promise<void> {
    await applyLegacyMigrationPrefix(database, 3);
  }

  /** Applies the pre-approval-table migration set for backfill cutover tests. */
  async function applyLegacyMigrationsThrough0006(database: DatabasePool): Promise<void> {
    await applyLegacyMigrationPrefix(database, 6);
  }

  /** Applies migrations through the last pre-control-lease-current-index schema. */
  async function applyLegacyMigrationsThrough0007(database: DatabasePool): Promise<void> {
    await applyLegacyMigrationPrefix(database, 7);
  }

  /** Applies one complete journal-less generated migration prefix. */
  async function applyLegacyMigrationPrefix(
    database: DatabasePool,
    prefixIndex: number,
  ): Promise<void> {
    await applyLegacyMigrations(database, generatedMigrationNames.slice(0, prefixIndex + 1));
  }

  /** Applies a list of generated migration SQL files to a legacy test database. */
  async function applyLegacyMigrations(
    database: DatabasePool,
    migrationNames: readonly string[],
  ): Promise<void> {
    for (const migrationName of migrationNames) {
      const migrationSql = await readFile(
        new URL(`../drizzle/${migrationName}`, import.meta.url),
        "utf8",
      );
      await database.pool.query(migrationSql);
    }
  }

  /** Seeds an exact ordered prefix of Drizzle's generated migration journal. */
  async function seedMigrationJournalPrefix(
    database: DatabasePool,
    prefixLength: number,
  ): Promise<void> {
    await database.pool.query(`
      CREATE SCHEMA IF NOT EXISTS drizzle;
      CREATE TABLE drizzle.__drizzle_migrations (
        id SERIAL PRIMARY KEY,
        hash text NOT NULL,
        created_at bigint
      )
    `);
    const migrations = readMigrationFiles({
      migrationsFolder: "drizzle",
    }).slice(0, prefixLength);
    for (const migration of migrations) {
      await database.pool.query(
        `INSERT INTO drizzle.__drizzle_migrations (hash, created_at) VALUES ($1, $2)`,
        [migration.hash, migration.folderMillis],
      );
    }
  }

  /** Reads the complete migration journal in insertion order. */
  async function readMigrationJournal(
    database: DatabasePool,
  ): Promise<readonly { readonly createdAt: string; readonly hash: string }[]> {
    const result = await database.pool.query<{
      readonly createdAt: string;
      readonly hash: string;
    }>(
      `
        SELECT created_at::text AS "createdAt", hash
        FROM drizzle.__drizzle_migrations
        ORDER BY id
      `,
    );
    return result.rows;
  }

  /** Reads deterministic public application-schema facts for idempotency comparison. */
  async function readPublicSchemaFacts(
    database: DatabasePool,
  ): Promise<readonly Record<string, unknown>[]> {
    const result = await database.pool.query<Record<string, unknown>>(
      `
        SELECT
          column_default AS "columnDefault",
          column_name AS "columnName",
          data_type AS "dataType",
          is_nullable AS "isNullable",
          table_name AS "tableName"
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
      `,
    );
    return result.rows;
  }

  /** Runs the real server entry point and captures its terminal startup result. */
  async function runServerProcess(
    databaseUrl: string,
    options: RunServerProcessOptions = {},
  ): Promise<ServerProcessResult> {
    const child = spawn(process.execPath, ["--import", "tsx", "src/server.ts"], {
      cwd: fileURLToPath(new URL("..", import.meta.url)),
      env: {
        ...process.env,
        AUTH_MODE: "disabled",
        DATABASE_URL: databaseUrl,
        PORT: "0",
        RUNTIME_TOPOLOGY: "single",
        ...options.env,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    let stdout = "";
    child.stderr.setEncoding("utf8");
    child.stdout.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      stderr += chunk;
    });
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
      if (
        options.shutdownAfterStdout !== undefined &&
        stdout.includes(options.shutdownAfterStdout)
      ) {
        child.kill("SIGTERM");
      }
    });

    const result = await new Promise<ServerProcessResult>((resolve, reject) => {
      const timeout = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new Error("Real server process did not terminate after startup failure"));
      }, 10_000);
      child.once("error", (error) => {
        clearTimeout(timeout);
        reject(error);
      });
      child.once("close", (exitCode, signal) => {
        clearTimeout(timeout);
        resolve({ exitCode, signal, stderr, stdout });
      });
    });
    return result;
  }

  /** Parses only complete JSON object lines from captured structured stderr. */
  function parseStructuredLogEntries(stderr: string): readonly Record<string, unknown>[] {
    const entries: Record<string, unknown>[] = [];
    for (const line of stderr.split("\n")) {
      try {
        const value = JSON.parse(line) as unknown;
        if (isRecord(value)) {
          entries.push(value);
        }
      } catch {}
    }
    return entries;
  }

  /** Asserts a malformed journal fails closed before migration 0008 application DDL. */
  async function expectInvalidJournalBeforeApplicationDdl(database: DatabasePool): Promise<void> {
    await expect(migrate(database)).rejects.toMatchObject({
      name: "DatabaseMigrationError",
      reason: "invalid_journal",
    });

    const applicationDdl = await database.pool.query<{
      readonly exists: boolean;
    }>(
      `
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = 'participant_control_leases'
            AND column_name = 'superseded_at'
        ) AS exists
      `,
    );
    expect(applicationDdl.rows[0]?.exists).toBe(false);
  }

  /** Seeds duplicate current leases that predate the partial unique index. */
  async function seedDuplicateCurrentControlLeases(database: DatabasePool): Promise<void> {
    await database.pool.query(
      `
        INSERT INTO sessions (session_id)
        VALUES ('sess_duplicate_lease_migration');
      `,
    );
    await database.pool.query(
      `
        INSERT INTO participant_control_leases (
          claimed_at,
          control_channel,
          instance_id,
          lease_expires_at,
          participant_id,
          session_id
        )
        VALUES
          (
            now() - interval '2 minutes',
            'rest',
            'inst_duplicate_lease_old',
            now() + interval '1 minute',
            'part_duplicate_lease_migration',
            'sess_duplicate_lease_migration'
          ),
          (
            now() - interval '1 minute',
            'rest',
            'inst_duplicate_lease_winner',
            now() + interval '2 minutes',
            'part_duplicate_lease_migration',
            'sess_duplicate_lease_migration'
          );
      `,
    );
  }

  /** Seeds a legacy database with an approval event before `task_approvals` exists. */
  async function seedLegacyApprovalEvent(database: DatabasePool): Promise<void> {
    await database.pool.query(
      `
        INSERT INTO sessions (session_id)
        VALUES ($1);
      `,
      ["sess_legacy_backfill"],
    );
    await database.pool.query(
      `
        INSERT INTO session_event_sequences (session_id, next_seq)
        VALUES ($1, 2);
      `,
      ["sess_legacy_backfill"],
    );
    await database.pool.query(
      `
        INSERT INTO tasks (
          completed_at,
          kind,
          objective,
          result,
          session_id,
          task_id
        )
        VALUES (now(), $1, $2, $3::jsonb, $4, $5);
      `,
      [
        "generic_approval_request",
        "legacy backfill approval",
        JSON.stringify(createGenericApprovalResult()),
        "sess_legacy_backfill",
        "task_legacy_backfill",
      ],
    );
    await database.pool.query(
      `
        INSERT INTO session_events (
          event_id,
          payload,
          producer_id,
          seq,
          session_id,
          type
        )
        VALUES ($1, $2::jsonb, $3, 1, $4, 'approval.recorded');
      `,
      [
        "evt_legacy_approval",
        JSON.stringify({
          decision: "approved",
          participantId: "part_legacy_backfill_original",
          reason: { approvalTarget: { action: "keep", key: "message-1" } },
          task: {
            ...createGenericApprovalTaskRecord("sess_legacy_backfill", "task_legacy_backfill"),
            completedAt: new Date().toISOString(),
            result: createGenericApprovalResult(),
          },
        }),
        systemProducerId,
        "sess_legacy_backfill",
      ],
    );
  }

  /**
   * Sends an HTTP request to the e2e app and parses successful JSON responses.
   */
  async function request<TResponse extends JsonResponse>(
    path: string,
    init: E2eRequestInit = {},
  ): Promise<TResponse> {
    return requestFrom(baseUrl, path, init);
  }

  /**
   * Sends an HTTP request to the e2e app and returns status plus parsed JSON.
   */
  async function requestStatus<TResponse extends JsonResponse = JsonResponse>(
    path: string,
    init: E2eRequestInit = {},
  ): Promise<RawJsonResponse<TResponse>> {
    return requestStatusFrom(baseUrl, path, init);
  }

  /**
   * Sends an HTTP request to a specific e2e app origin and parses successful
   * JSON responses.
   */
  async function requestFrom<TResponse extends JsonResponse>(
    origin: string,
    path: string,
    init: E2eRequestInit = {},
  ): Promise<TResponse> {
    const raw = await requestStatusFrom<TResponse>(origin, path, init);
    if (raw.status < 200 || raw.status >= 300) {
      throw new Error(`${init.method ?? "GET"} ${path} failed ${raw.status}: ${raw.text}`);
    }
    return raw.body;
  }

  /**
   * Sends an HTTP request to a specific e2e app origin and keeps non-2xx JSON.
   */
  async function requestStatusFrom<TResponse extends JsonResponse = JsonResponse>(
    origin: string,
    path: string,
    init: E2eRequestInit = {},
  ): Promise<RawJsonResponse<TResponse>> {
    const authToken =
      init.authToken === null ? null : (init.authToken ?? mintAuthTokenForRequest(path, init));
    const response = await fetch(`${origin}${path}`, {
      ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
      headers: {
        "content-type": "application/json",
        ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
      },
      method: init.method ?? "GET",
    });
    const text = await response.text();
    return {
      body: parseJsonResponseBody<TResponse>(text),
      status: response.status,
      text,
    };
  }

  /** Mints a scoped token for one e2e REST request. */
  function mintAuthTokenForRequest(path: string, init: E2eRequestInit): string {
    const method = init.method ?? "GET";
    const sessionId = extractSessionId(path);
    if (path === "/debug/server" || path === "/" || path === "/ui") {
      return mintE2eToken({
        participantId: "part_e2e_admin",
        role: "admin",
        sessionId: "*",
      });
    }
    if (path.startsWith("/client-bindings") || (method === "POST" && path === "/sessions")) {
      return mintE2eToken({
        participantId: "part_e2e_service",
        role: "admin",
        sessionId: "*",
      });
    }
    if (!sessionId) {
      return mintE2eToken({
        participantId: "part_e2e_service",
        role: "admin",
        sessionId: "*",
      });
    }
    if (path.includes("/debug/")) {
      return mintE2eToken({
        participantId: "part_e2e_admin",
        role: "admin",
        sessionId,
      });
    }
    if (method === "GET") {
      return mintE2eToken({
        participantId: "part_e2e_observer",
        role: "observer",
        sessionId,
      });
    }
    return mintE2eToken({
      participantId: readParticipantIdForRequest(path, init.body) ?? "part_e2e_controller",
      role: "participant",
      sessionId,
    });
  }
});

interface E2eRequestInit {
  readonly authToken?: string | null;
  readonly body?: unknown;
  readonly method?: string;
}

/** Counts canonical session creation lifecycle events in a fetched event page. */
function countSessionCreatedEvents(events: readonly SessionEvent[]): number {
  return events.filter((event) => event.type === sessionEventType.sessionCreated).length;
}

/** Finds one lifecycle event and verifies its embedded task mirrors the row. */
function expectTaskEventPayload(
  events: readonly SessionEvent[],
  type: string,
  taskId: string,
  task: TaskResponse["task"],
): SessionEvent {
  const event = events.find(
    (candidate) => candidate.type === type && taskIdFromEventPayload(candidate) === taskId,
  );
  expect(event).toBeDefined();
  if (!event) {
    throw new Error(`Missing ${type} event for ${taskId}`);
  }
  const payloadTask = readEventTaskPayload(event);
  expect(payloadTask).toMatchObject({
    cancelledAt: task.cancelledAt,
    claimExpiresAt: task.claimExpiresAt,
    claimedAt: task.claimedAt,
    claimedBy: task.claimedBy,
    completedAt: task.completedAt,
    failedAt: task.failedAt,
    failure: task.failure,
    releasedAt: task.releasedAt,
    result: task.result,
    sessionId: task.sessionId,
    taskId: task.taskId,
  });
  return event;
}

/** Verifies every task row has the lifecycle events needed for replay. */
function expectTaskEventConsistency(
  events: readonly SessionEvent[],
  tasks: readonly TaskResponse["task"][],
): void {
  for (const task of tasks) {
    expect(
      events.some(
        (event) => event.type === "task.created" && taskIdFromEventPayload(event) === task.taskId,
      ),
    ).toBe(true);
    if (task.completedAt !== null) {
      expectTaskEventPayload(events, "task.completed", task.taskId, task);
    }
    if (task.failedAt !== null) {
      expectTaskEventPayload(events, "task.failed", task.taskId, task);
    }
    if (task.cancelledAt !== null) {
      expectTaskEventPayload(events, "control.cancel", task.taskId, task);
    }
    if (task.releasedAt !== null) {
      expectTaskEventPayload(events, "task.released", task.taskId, task);
    }
  }
}

/** Filters participant lifecycle events for one participant identity. */
function participantEvents(
  events: readonly SessionEvent[],
  type: "participant.joined" | "participant.updated",
  participantId: string,
): SessionEvent[] {
  return events.filter((event) => {
    if (event.type !== type) {
      return false;
    }
    const participant = event.payload.participant;
    return isRecord(participant) && participant.participantId === participantId;
  });
}

/** Verifies claim-expiration events identify the cleared task and claimant. */
function expectTaskClaimExpiredConsistency(
  events: readonly SessionEvent[],
  taskId: string,
  previousClaimedBy: string,
  sessionId: string,
): void {
  const event = events.find(
    (candidate) =>
      candidate.type === "task.claim_expired" && taskIdFromEventPayload(candidate) === taskId,
  );
  expect(event).toBeDefined();
  if (!event) {
    throw new Error(`Missing task.claim_expired event for ${taskId}`);
  }
  expect(event.payload.previousClaimedBy).toBe(previousClaimedBy);
  expect(readEventTaskPayload(event)).toMatchObject({
    claimExpiredAt: expect.any(String) as string,
    claimExpiredBy: previousClaimedBy,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    releasedAt: null,
    releasedBy: null,
    sessionId,
    taskId,
  });
}

/** Reads the task id from lifecycle event payloads without assuming event type. */
function taskIdFromEventPayload(event: SessionEvent): string | null {
  const task = event.payload.task;
  if (!isRecord(task)) {
    return null;
  }
  return typeof task.taskId === "string" ? task.taskId : null;
}

/** Reads the embedded task object from a lifecycle event payload. */
function readEventTaskPayload(event: SessionEvent): Record<string, unknown> {
  const task = event.payload.task;
  if (!isRecord(task)) {
    throw new Error(`Event ${event.eventId} has no task payload`);
  }
  return task;
}

/** Creates a test-only pool wrapper that fails the event row insert. */
async function createEventInsertFailingDatabase(database: DatabasePool): Promise<DatabasePool> {
  return wrapPoolQueries(database, async (query, values, next) => {
    const text = typeof query === "string" ? query : query.text;
    if (/INSERT\s+INTO\s+session_events/iu.test(text)) {
      throw new Error("injected session event insert failure");
    }
    return next(query, values);
  });
}

/** Parses an HTTP JSON response body, treating empty bodies as empty objects. */
function parseJsonResponseBody<TResponse extends JsonResponse>(text: string): TResponse {
  return (text.trim() ? JSON.parse(text) : {}) as TResponse;
}

/** Mints one e2e token with the shared test signing key. */
function mintE2eToken(input: {
  readonly participantId: string;
  readonly role: AuthRole;
  readonly sessionId: string;
}): string {
  return mintTestAuthToken(input);
}

/** Adds a scoped e2e access token to a WebSocket stream URL. */
function authenticatedWebSocketUrl(rawUrl: string): string {
  const url = new URL(rawUrl);
  const sessionId = extractSessionId(url.pathname);
  if (!sessionId) {
    throw new Error("WebSocket URL is missing a session id");
  }
  const participantId = url.searchParams.get("participantId");
  url.searchParams.set(
    "access_token",
    mintE2eToken({
      participantId: participantId ?? "part_e2e_ws_observer",
      role: participantId ? "participant" : "observer",
      sessionId,
    }),
  );
  return url.toString();
}

/** Extracts the durable session id from a session-scoped REST or WS path. */
function extractSessionId(path: string): string | null {
  const match = path.match(/^\/sessions\/([^/?]+)/u);
  return match?.[1] ? decodeURIComponent(match[1]) : null;
}

/** Reads the participant identity carried by a REST mutation body or route path. */
function readParticipantIdForRequest(path: string, body: unknown): string | null {
  const participantRouteMatch = path.match(
    /^\/sessions\/[^/]+\/participants\/([^/]+)\/(?:heartbeat|control\/release)$/u,
  );
  if (participantRouteMatch?.[1]) {
    return decodeURIComponent(participantRouteMatch[1]);
  }
  if (!isRecord(body)) {
    return null;
  }
  if (typeof body.participantId === "string") {
    return body.participantId;
  }
  if (typeof body.producerId === "string") {
    return body.producerId;
  }
  return null;
}

/**
 * Creates an isolated Postgres database for one e2e run.
 */
async function createDatabase(databaseName: string): Promise<void> {
  const adminPool = new pg.Pool({ connectionString: adminDatabaseUrl });
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
  } finally {
    await adminPool.end();
  }
}

/**
 * Builds a database URL that points at the isolated e2e database.
 */
function buildDatabaseUrl(databaseName: string): string {
  const url = new URL(adminDatabaseUrl);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

/**
 * Drops the isolated e2e database after terminating open connections.
 */
async function dropDatabase(databaseName: string): Promise<void> {
  const adminPool = new pg.Pool({ connectionString: adminDatabaseUrl });
  try {
    await adminPool.query(
      `
      SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = $1
    `,
      [databaseName],
    );
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)}`);
  } finally {
    await adminPool.end();
  }
}

/**
 * Reserves and releases a port using the same wildcard bind as the e2e app server.
 */
async function findOpenPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, resolve);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
  if (typeof address !== "object" || address === null) {
    throw new Error("Failed to allocate e2e port");
  }
  return address.port;
}

/**
 * Checks whether a parsed WebSocket message is an event envelope.
 */
function isEventEnvelope(value: unknown): value is {
  readonly event: {
    readonly producerId: string;
    readonly seq: number;
    readonly type: string;
  };
  readonly op: "event";
} {
  return (
    typeof value === "object" &&
    value !== null &&
    "op" in value &&
    value.op === "event" &&
    "event" in value &&
    typeof value.event === "object" &&
    value.event !== null &&
    "producerId" in value.event &&
    typeof value.event.producerId === "string" &&
    "seq" in value.event &&
    typeof value.event.seq === "number" &&
    "type" in value.event &&
    typeof value.event.type === "string"
  );
}

/**
 * Checks whether a parsed WebSocket message marks replay completion.
 */
function isReplayCompleteEnvelope(value: unknown): value is { readonly op: "replay.complete" } {
  return (
    typeof value === "object" && value !== null && "op" in value && value.op === "replay.complete"
  );
}

/** Checks whether a parsed WebSocket message is a Host-presence presence frame. */
function isPresenceEnvelope(value: unknown): value is {
  readonly hosts: readonly { readonly instanceId: string }[];
  readonly op: "presence";
} {
  return (
    isRecord(value) &&
    value.op === "presence" &&
    Array.isArray(value.hosts) &&
    value.hosts.every((host) => isRecord(host) && typeof host.instanceId === "string")
  );
}

/**
 * Checks whether a parsed WebSocket message is a command result envelope.
 */
function isCommandResultEnvelope(value: unknown): value is {
  readonly event?: { readonly eventId: string };
  readonly op: "command.result";
  readonly requestId?: string;
  readonly status?: "created" | "replayed";
} {
  return isRecord(value) && value.op === webSocketOperation.commandResult;
}

/**
 * Checks whether a parsed WebSocket message is an error envelope.
 */
function isErrorEnvelope(value: unknown): value is {
  readonly command?: string;
  readonly error: string;
  readonly op: "error";
  readonly requestId?: string;
  readonly taskId?: string;
} {
  return (
    isRecord(value) && value.op === webSocketOperation.error && typeof value.error === "string"
  );
}

/**
 * Checks whether a parsed WebSocket message is a publish policy error.
 */
function isErrorEnvelopeWithReason(
  value: unknown,
  requestId: string,
  reason: string,
): value is {
  readonly error: string;
  readonly op: "error";
  readonly reason: string;
  readonly requestId: string;
} {
  return (
    isRecord(value) &&
    value.op === webSocketOperation.error &&
    value.requestId === requestId &&
    value.reason === reason &&
    typeof value.error === "string"
  );
}

/**
 * Checks whether a parsed WebSocket message is a task claim-expired event.
 */
function isTaskClaimExpiredEnvelope(value: unknown): value is {
  readonly event: { readonly type: "task.claim_expired" };
  readonly op: "event";
} {
  return isEventEnvelope(value) && value.event.type === "task.claim_expired";
}

/**
 * Quotes a Postgres identifier for database create/drop commands.
 */
function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

/** Builds a complete generic result fixture accepted by approval gates. */
function createGenericApprovalResult(): Record<string, unknown> {
  return {
    actions: ["Approve Request"],
    compatibilityNormalizationCount: 0,
    decisionType: "review_items",
    dryRun: genericDryRunApprovalFixture(),
    inspectedItemCount: 1,
    inspectedItems: ["item-1"],
    intent: "review",
    itemCount: 1,
    itemRecommendations: [
      {
        action: "approve",
        category: "generic",
        evidence: [],
        manualActionReason: null,
        itemId: "item-1",
        reason: "matches the request",
        requiresApproval: false,
        risk: "low",
        title: "Request Item",
      },
    ],
    matchedKeywords: ["request"],
    planner: "deterministic",
    plannerInputItemCount: 1,
    plannerModel: null,
    plannerOutputRecommendationCount: 1,
    plannerValidation: "accepted",
    plannerWarnings: [],
    queryKeywords: ["request"],
    secondPassAdjustmentCount: 0,
    secondPassVerificationCount: 0,
    selectedItems: ["Request Item"],
    titles: ["Request Item"],
    summary: "Prepared a plan",
    kind: "generic_approval_request",
  };
}

/** Builds the generic dry-run approval envelope accepted by core defaults. */
function genericDryRunApprovalFixture(): Record<string, unknown> {
  return {
    approvalSummary: ["Approve Request", "item: item-1"],
    authorization: "needs_approval",
    request: {
      action: "approve",
      key: "item-1",
    },
    target: "item-1",
  };
}

/** Builds a task payload shape for seeded approval events. */
function createGenericApprovalTaskRecord(sessionId: string, taskId: string): TaskResponse["task"] {
  const now = new Date().toISOString();
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: now,
    createdAt: now,
    failedAt: null,
    failure: null,
    input: null,
    kind: "generic_approval_request",
    objective: "legacy backfill approval",
    releasedAt: null,
    releasedBy: null,
    result: createGenericApprovalResult(),
    sessionId,
    taskId,
  };
}

/** Checks whether a value is a non-null object record. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** Observes both promise outcomes immediately so delayed cleanup cannot leak rejections. */
async function observeAsyncOutcome<TValue>(
  operation: Promise<TValue>,
): Promise<AsyncOutcome<TValue>> {
  try {
    return { status: "fulfilled", value: await operation };
  } catch (reason) {
    return { reason, status: "rejected" };
  }
}

/** Returns a fulfilled test operation or rethrows its already-observed failure. */
function requireFulfilledOutcome<TValue>(outcome: AsyncOutcome<TValue>): TValue {
  if (outcome.status === "rejected") {
    throw outcome.reason;
  }
  return outcome.value;
}

/** Releases a failed manual phase and waits for every publisher to settle. */
async function settleEventPublishersAfterFailure(input: {
  readonly phaseName: string;
  readonly phaseReleased: boolean;
  readonly publishers: readonly (Promise<AsyncOutcome<SessionEvent>> | null)[];
  readonly releasePhase: (name: string) => void;
}): Promise<void> {
  if (!input.phaseReleased) {
    try {
      input.releasePhase(input.phaseName);
    } catch {}
  }
  await Promise.all(
    input.publishers.filter(
      (publisher): publisher is Promise<AsyncOutcome<SessionEvent>> => publisher !== null,
    ),
  );
}

/** Rejects with a diagnostic when an asynchronous E2E operation stops making progress. */
async function withDiagnosticTimeout<TValue>(
  operation: Promise<TValue>,
  timeoutMs: number,
  message: string,
): Promise<TValue> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

/**
 * Polls a synchronous condition until it passes or the e2e timeout elapses.
 */
async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 2_000) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for e2e condition");
}

/**
 * Polls an async condition until it passes or the e2e timeout elapses.
 */
async function waitForAsync(predicate: () => Promise<boolean>): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 2_000) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("Timed out waiting for async e2e condition");
}

/**
 * Retries an async load until it succeeds or rethrows the last error on timeout.
 */
async function waitForAsyncValue<TValue>(load: () => Promise<TValue>): Promise<TValue> {
  const startedAt = Date.now();
  let lastError: unknown = null;
  while (Date.now() - startedAt < 2_000) {
    try {
      return await load();
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Timed out waiting for async value");
}

/**
 * Waits for a fixed duration in timing-sensitive e2e checks.
 */
async function sleep(durationMs: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, durationMs));
}

/**
 * Resolves once a WebSocket is open.
 */
async function waitForSocketOpen(socket: WebSocket): Promise<void> {
  if (socket.readyState === WebSocket.OPEN) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
}

/**
 * Resolves once a WebSocket is closed.
 */
async function waitForSocketClose(socket: WebSocket): Promise<void> {
  if (socket.readyState === WebSocket.CLOSED) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    socket.once("close", resolve);
    socket.once("error", reject);
  });
}

/**
 * Resolves with a WebSocket close code.
 */
async function waitForSocketCloseCode(socket: WebSocket): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    socket.once("close", (code) => resolve(code));
    socket.once("error", reject);
  });
}
