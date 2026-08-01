import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createConnection } from "node:net";
import { fileURLToPath } from "node:url";

import { readMigrationFiles } from "drizzle-orm/migrator";
import { Effect } from "effect";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it, onTestFinished } from "vitest";
import WebSocket from "ws";
import type { ParticipantTaskExecutorContext } from "@dungle-scrubs/tether-client";
import {
  createSessionSummaryExecutor,
  OllamaClient,
  TetherApiClient,
} from "@dungle-scrubs/session-summary-worker";
import type {
  SessionScalabilityDebugRecord,
  SessionSummaryContent,
  SessionSummaryGenerationJob,
  SessionSummaryOllamaIdentity,
  OperatorGrantScope,
  BrowserOperatorCommandResponse,
  TaskRecord,
} from "@dungle-scrubs/tether-protocol";
import {
  mintTestAuthToken,
  testAuthSigningKid,
  testAuthSigningSecret,
} from "../src/auth/test-tokens.js";
import type { AuthRole } from "../src/auth/token.js";
import { createAuthPersistenceStores } from "../src/auth/db-grant-stores.js";
import { runBootstrapAdminCli } from "../src/auth/bootstrap-cli.js";
import { executeBrowserPairingCli } from "../src/auth/browser-pairing-cli.js";
import { createBrowserPairingLifecycle } from "../src/auth/browser-pairing.js";
import { createBrowserPairingStore } from "../src/auth/browser-pairing-stores.js";
import { hashAuthTicket } from "../src/auth/ticket-lifecycle.js";
import type {
  AuthGrantAuditMetadata,
  AuthGrantMetadata,
  AuthTicketAdmissionMetadata,
} from "../src/auth/grant-stores.js";
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
  createOperatorCommandTaskWithEvent,
  createSession as createDbSession,
  createPool,
  createTaskWithEvent,
  expireTaskClaims,
  failTaskWithEvent,
  getTask,
  listContextEventSuffix,
  listEvents,
  listParticipants,
  listTaskApprovals,
  migrate,
  recordTaskApproval,
  refreshTaskClaim,
  releaseControlLease,
  releaseTaskWithEvent,
  taskClaimLockQuery,
  upsertClientSessionBinding,
  upsertParticipant,
  upsertParticipantWithEvent,
} from "../src/db.js";
import {
  backfillSessionProjection,
  type SessionProjectionTransaction,
  verifySessionProjection,
} from "../src/db-session-projections.js";
import type { AppServer, AppServerDebugInfo } from "../src/http.js";
import { createAppServer, createAppServerWithSessionService } from "../src/http.js";
import type { StructuredLogEntry } from "../src/observability.js";
import { sessionEventType, systemProducerId, webSocketOperation } from "../src/protocol.js";
import { defaultResourceLimits } from "../src/resource-limits.js";
import { clientPublishDenyReason } from "../src/session-event-publish-policy.js";
import { createSessionServiceEffect } from "../src/session-service.js";
import { createSessionSummaryGenerationService } from "../src/session-summary-service.js";
import { createSessionSummaryStore } from "../src/session-summary-store.js";
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
const projectionBenchmark = process.env.E2E_PROJECTION_BENCHMARK === "true" ? it : it.skip;
const adminDatabaseUrl = readRequiredE2eAdminDatabaseUrl();
const e2eAuthOptions = {
  activeKid: testAuthSigningKid,
  // E2e fixtures authenticate with legacy stateless test tokens, which
  // required mode rejects unless the migration escape hatch is enabled.
  allowLegacyTokens: true,
  issuer: "https://auth.e2e.tether.local",
  mode: "required",
  preEnforcementGrantIssuanceEnabled: true,
  secrets: { [testAuthSigningKid]: testAuthSigningSecret },
} as const;
const summaryWorkerOllamaIdentity = {
  contextSize: 32_768,
  model: "evaluated-e2e-model",
  quantization: "Q4_K_M",
  revision: `sha256:${"a".repeat(64)}`,
  thinkingMode: "disabled",
} as const satisfies SessionSummaryOllamaIdentity;

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
  "0014_flippant_caretaker.sql",
  "0015_conscious_toad.sql",
  "0016_daffy_surge.sql",
  "0017_skinny_lockheed.sql",
  "0018_complex_elektra.sql",
  "0019_hesitant_jazinda.sql",
  "0020_busy_maria_hill.sql",
  "0021_warm_doctor_octopus.sql",
  "0022_lonely_infant_terrible.sql",
  "0023_nosy_robbie_robertson.sql",
  "0024_eminent_lizard.sql",
  "0025_strange_dakota_north.sql",
] as const;
const authFoundationMigrationIndex = 16;
const preAuthFoundationMigrationIndex = authFoundationMigrationIndex - 1;

interface JsonResponse {
  readonly [key: string]: unknown;
}

interface AuthGrantPublicResponse {
  readonly audience: "tether-rest";
  readonly expiresAt: string;
  readonly issuedAt: string;
  readonly issuer: string;
  readonly jti: string;
  readonly kid: string;
  readonly revokedAt: string | null;
  readonly role: "admin" | "observer" | "participant";
  readonly sessionScope: string;
  readonly subject: string;
}

interface AuthGrantCreateResponse extends JsonResponse {
  readonly bearer: string;
  readonly grant: AuthGrantPublicResponse;
}

interface AuthGrantReadResponse extends JsonResponse {
  readonly grant: AuthGrantPublicResponse;
}

interface AuthGrantListResponse extends JsonResponse {
  readonly grants: readonly AuthGrantPublicResponse[];
}

interface AuthTicketCreateResponse extends JsonResponse {
  readonly expiresAt: string;
  readonly ticket: string;
}

interface BrowserPairingCreateResponse extends JsonResponse {
  readonly exchangeSecret: string;
  readonly request: {
    readonly requestId: string;
  };
}

interface BrowserPairingExchangeResponse extends JsonResponse {
  readonly csrfToken: string;
  readonly grantJti: string;
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
  readonly headers: Headers;
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
    readonly claimId: string | null;
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

/** Narrows a claimed task's server-issued claim id to a required string. */
function requireClaimId(task: { readonly claimId: string | null }): string {
  if (task.claimId === null) {
    throw new Error("Expected a server-issued claim id on a claimed task");
  }
  return task.claimId;
}

interface TaskApprovalResponse extends JsonResponse {
  readonly approval: {
    readonly approvalEventId: string;
    readonly decidedAt: string;
    readonly decidedByParticipantId: string;
    readonly decision: "approved" | "rejected";
    readonly reason: Record<string, unknown>;
    readonly sessionId: string;
    readonly targetKey: string;
    readonly taskId: string;
  };
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
    readonly latestSummary: null | {
      readonly budgetClass: string;
      readonly content: { readonly headline: string };
      readonly coversSeqFrom: number;
      readonly coversSeqTo: number;
      readonly summaryId: string;
    };
    readonly mode: "raw_only" | "summary_with_raw_tail";
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

interface SessionScalabilityDebugResponse extends JsonResponse {
  readonly scalability: SessionScalabilityDebugRecord;
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

interface Deferred<TValue> {
  readonly promise: Promise<TValue>;
  readonly resolve: (value: TValue | PromiseLike<TValue>) => void;
}

/** Pauses one production backfill immediately before its PostgreSQL CAS. */
class PausedProjectionBackfillClient implements SessionProjectionTransaction {
  readonly candidateReady = createDeferred<void>();
  private readonly resumeSignal = createDeferred<void>();
  private resumed = false;

  constructor(private readonly client: pg.PoolClient) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("INSERT INTO session_projections") && sql.includes("$17::boolean")) {
      this.candidateReady.resolve();
      await this.resumeSignal.promise;
    }
    const result = await this.client.query<TRow>(sql, values ? [...values] : undefined);
    return { rows: result.rows };
  }

  resume(): void {
    if (!this.resumed) {
      this.resumed = true;
      this.resumeSignal.resolve();
    }
  }
}

/** Measures bounded backfill queries and can pause before a chosen CAS. */
class MeasuredProjectionBackfillClient implements SessionProjectionTransaction {
  readonly candidateReady = createDeferred<void>();
  readonly queryDurationsMs: number[] = [];
  private readonly resumeSignal = createDeferred<void>();
  private paused = false;
  private resumed = false;

  constructor(
    private readonly client: SessionProjectionTransaction,
    private readonly pauseAtCoverage: number | null = null,
  ) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ): Promise<{ readonly rows: TRow[] }> {
    const isBackfillCas =
      sql.includes("INSERT INTO session_projections") && sql.includes("$17::boolean");
    const candidateCoverage = isBackfillCas ? Number(values?.[4]) : null;
    if (
      !this.paused &&
      this.pauseAtCoverage !== null &&
      candidateCoverage !== null &&
      candidateCoverage >= this.pauseAtCoverage
    ) {
      this.paused = true;
      this.candidateReady.resolve();
      await this.resumeSignal.promise;
    }
    const startedAt = performance.now();
    const result = await this.client.query<TRow>(sql, values);
    if (sql.includes("FROM session_events") || isBackfillCas) {
      this.queryDurationsMs.push(performance.now() - startedAt);
    }
    return result;
  }

  resume(): void {
    if (!this.resumed) {
      this.resumed = true;
      this.resumeSignal.resolve();
    }
  }
}

function createDeferred<TValue>(): Deferred<TValue> {
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
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
    const port = await app.listen(0);
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
    const scalabilityTables = await currentPool().pool.query<{ readonly tableName: string }>(
      `
        SELECT table_name AS "tableName"
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ('session_projections', 'session_summaries')
        ORDER BY table_name
      `,
    );
    expect(scalabilityTables.rows.map((row) => row.tableName)).toEqual([
      "session_projections",
      "session_summaries",
    ]);
  });

  it("serializes competing Session Summary publications to exactly one active row", async () => {
    const sessionId = `sess_summary_publication_race_${randomUUID()}`;
    await createDbSession(currentPool(), sessionId);
    await appendEvent(
      currentPool(),
      {
        eventId: `evt_summary_publication_race_${randomUUID()}`,
        payload: { text: "source event" },
        producerId: "summary-publication-race-e2e",
        sessionId,
        type: "user.message",
      },
      { sourceId: "src_summary_publication_race_e2e" },
    );
    const summaryIds = [
      `summary_publication_race_a_${randomUUID()}`,
      `summary_publication_race_b_${randomUUID()}`,
    ] as const;
    for (const [index, summaryId] of summaryIds.entries()) {
      await currentPool().pool.query(
        `
          INSERT INTO session_summaries (
            budget_class, content, covers_seq_from, covers_seq_to,
            generation_task_id, integrity_algorithm, integrity_hash,
            ollama_context_size, ollama_model, ollama_quantization,
            ollama_revision, ollama_thinking_mode, output_schema_version,
            producer_id, producer_version, prompt_version, session_id,
            source_event_count, source_first_event_id, source_last_event_id,
            source_range_hash, summary_id, validated_at
          )
          VALUES (
            'standard', $1::jsonb, 1, 1, $2, 'sha256', $3,
            32768, 'qwen3:8b', 'Q4_K_M', 'sha256:model-revision',
            'enabled', 'session-summary.v1', 'session-summary-worker',
            '1.0.0', 'session-summary-prompt.v1', $4, 1, $5, $5, $3,
            $6, clock_timestamp()
          )
        `,
        [
          JSON.stringify({
            facts: [],
            headline: `Candidate ${index + 1}`,
            narrative: "Competing publication candidate.",
            openQuestions: [],
          }),
          `task_summary_publication_race_${index}_${randomUUID()}`,
          String(index + 1).repeat(64),
          sessionId,
          `evt_summary_publication_race_source_${index}`,
          summaryId,
        ],
      );
    }
    const replicaAStore = createSessionSummaryStore(currentPool().pool);
    const replicaBStore = createSessionSummaryStore(currentPool().pool);

    const publications = await Promise.allSettled([
      replicaAStore.publishCandidate(summaryIds[0]),
      replicaBStore.publishCandidate(summaryIds[1]),
    ]);
    const active = await currentPool().pool.query<{
      readonly count: number;
      readonly summaryId: string;
    }>(
      `
        SELECT count(*)::int AS count, min(summary_id) AS "summaryId"
        FROM session_summaries
        WHERE session_id = $1
          AND budget_class = 'standard'
          AND published_at IS NOT NULL
          AND superseded_at IS NULL
      `,
      [sessionId],
    );

    expect(publications.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect(publications.filter((result) => result.status === "rejected")).toHaveLength(1);
    expect(active.rows[0]?.count).toBe(1);
    expect(summaryIds).toContain(active.rows[0]?.summaryId);
  });

  it("atomically invalidates failed browser pairings and admits only one stolen-code racer", async () => {
    const store = createBrowserPairingStore(currentPool());
    const currentTime = { value: new Date("2026-08-01T02:00:00.000Z") };
    const createLifecycle = (exchangeSecret: string, csrfToken: string) =>
      createBrowserPairingLifecycle({
        activeKid: testAuthSigningKid,
        issuer: e2eAuthOptions.issuer,
        now: () => currentTime.value,
        randomCsrfToken: () => csrfToken,
        randomExchangeSecret: () => exchangeSecret,
        randomPhrase: () => "amber cedar orbit",
        secrets: e2eAuthOptions.secrets,
        store,
      });
    const scope: OperatorGrantScope = {
      actions: ["archive"],
      commands: ["scan"],
      permissions: ["approval.submit", "session.read"],
      scopeKeys: ["account-primary:inbox"],
      sessionIds: [`sess_browser_pairing_${randomUUID()}`],
      targetKinds: ["message"],
    };
    const nonce = "N".repeat(22);
    const failedSecret = "F".repeat(43);
    const failedLifecycle = createLifecycle(failedSecret, "C".repeat(43));
    const failedRequest = await failedLifecycle.create({
      operatorSubject: "operator@example.test",
      origin: "https://hub.example.test",
      publicNonce: nonce,
      requestedScope: scope,
      sourceAddress: "source-a",
    });
    await failedLifecycle.confirm(failedRequest.request.requestId, "admin@example.test");

    for (let attempt = 0; attempt < 5; attempt += 1) {
      const input = {
        exchangeSecret: "X".repeat(43),
        publicNonce: attempt === 0 ? "Q".repeat(22) : nonce,
      };
      await expect(
        failedLifecycle.exchange(failedRequest.request.requestId, input, {
          origin: "https://hub.example.test",
          sourceAddress: "source-a",
        }),
      ).rejects.toEqual(
        expect.objectContaining({
          code: attempt === 0 ? "pairing_nonce_mismatch" : "pairing_secret_invalid",
        }),
      );
    }
    await expect(
      failedLifecycle.exchange(
        failedRequest.request.requestId,
        {
          exchangeSecret: failedSecret,
          publicNonce: nonce,
        },
        { origin: "https://hub.example.test", sourceAddress: "source-a" },
      ),
    ).rejects.toEqual(expect.objectContaining({ code: "pairing_invalidated" }));
    const failedStored = await store.inspect(failedRequest.request.requestId);
    expect(failedStored).toMatchObject({ failedAttempts: 5 });
    expect(failedStored?.invalidatedAt).not.toBeNull();
    expect(JSON.stringify(failedStored)).not.toContain(failedSecret);

    const raceSecret = "R".repeat(43);
    const raceLifecycle = createLifecycle(raceSecret, "S".repeat(43));
    const raceRequest = await raceLifecycle.create({
      operatorSubject: "operator@example.test",
      origin: "https://hub.example.test",
      publicNonce: nonce,
      requestedScope: scope,
      sourceAddress: "source-b",
    });
    await raceLifecycle.confirm(raceRequest.request.requestId, "admin@example.test");
    const races = await Promise.allSettled([
      raceLifecycle.exchange(
        raceRequest.request.requestId,
        { exchangeSecret: raceSecret, publicNonce: nonce },
        { origin: "https://hub.example.test", sourceAddress: "source-b" },
      ),
      raceLifecycle.exchange(
        raceRequest.request.requestId,
        { exchangeSecret: raceSecret, publicNonce: nonce },
        { origin: "https://hub.example.test", sourceAddress: "source-b" },
      ),
    ]);
    expect(races.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    const rejected = races.find((result) => result.status === "rejected");
    expect(rejected).toMatchObject({
      reason: expect.objectContaining({ code: "pairing_already_exchanged" }),
      status: "rejected",
    });
    const grants = await currentPool().pool.query<{ readonly count: number }>(
      `SELECT count(*)::int AS count FROM auth_grants WHERE subject = $1`,
      ["operator@example.test"],
    );
    expect(grants.rows[0]?.count).toBe(1);

    const expirySecret = "Y".repeat(43);
    const expiryLifecycle = createLifecycle(expirySecret, "D".repeat(43));
    const expiryRequest = await expiryLifecycle.create({
      operatorSubject: "expired-operator@example.test",
      origin: "https://hub.example.test",
      publicNonce: nonce,
      requestedScope: scope,
      sourceAddress: "source-c",
    });
    await expiryLifecycle.confirm(expiryRequest.request.requestId, "admin@example.test");
    currentTime.value = new Date("2026-08-01T02:11:00.000Z");
    await expect(
      expiryLifecycle.exchange(
        expiryRequest.request.requestId,
        { exchangeSecret: expirySecret, publicNonce: nonce },
        { origin: "https://hub.example.test", sourceAddress: "source-c" },
      ),
    ).rejects.toEqual(expect.objectContaining({ code: "pairing_expired" }));

    const rateLifecycle = createLifecycle("L".repeat(43), "K".repeat(43));
    const creations = await Promise.allSettled(
      Array.from({ length: 6 }, () =>
        rateLifecycle.create({
          operatorSubject: "rate-limited-operator@example.test",
          origin: "https://hub.example.test",
          publicNonce: nonce,
          requestedScope: scope,
          sourceAddress: "source-d",
        }),
      ),
    );
    expect(creations.filter((result) => result.status === "fulfilled")).toHaveLength(5);
    expect(creations.filter((result) => result.status === "rejected")).toEqual([
      expect.objectContaining({
        reason: expect.objectContaining({ code: "pairing_rate_limited" }),
        status: "rejected",
      }),
    ]);

    const exchangeSource = "shared-exchange-source";
    const expiredFailureId = `pairfail_${randomUUID()}`;
    await currentPool().pool.query(
      `INSERT INTO browser_pairing_exchange_failures (created_at, failure_id, source_address_hash)
       VALUES ($1, $2, $3)`,
      [new Date(currentTime.value.getTime() - 11 * 60 * 1_000), expiredFailureId, "a".repeat(64)],
    );
    const aggregateRequests = await Promise.all(
      Array.from({ length: 6 }, async (_, requestIndex) => {
        const lifecycle = createLifecycle(
          String(requestIndex).padStart(1, "0").repeat(43),
          String(requestIndex + 1).repeat(43),
        );
        const created = await lifecycle.create({
          operatorSubject: `aggregate-${requestIndex}@example.test`,
          origin: "https://hub.example.test",
          publicNonce: nonce,
          requestedScope: scope,
          sourceAddress: `creation-source-${requestIndex}`,
        });
        await lifecycle.confirm(created.request.requestId, "admin@example.test");
        return { created, lifecycle };
      }),
    );
    for (const { created, lifecycle } of aggregateRequests.slice(0, 5)) {
      for (let attempt = 0; attempt < 4; attempt += 1) {
        await expect(
          lifecycle.exchange(
            created.request.requestId,
            { exchangeSecret: "Z".repeat(43), publicNonce: nonce },
            { origin: "https://hub.example.test", sourceAddress: exchangeSource },
          ),
        ).rejects.toEqual(expect.objectContaining({ code: "pairing_secret_invalid" }));
      }
    }
    const rateLimitedExchange = aggregateRequests[5];
    if (rateLimitedExchange === undefined) throw new Error("aggregate pairing fixture missing");
    await expect(
      rateLimitedExchange.lifecycle.exchange(
        rateLimitedExchange.created.request.requestId,
        { exchangeSecret: "Z".repeat(43), publicNonce: nonce },
        { origin: "https://hub.example.test", sourceAddress: exchangeSource },
      ),
    ).rejects.toEqual(expect.objectContaining({ code: "pairing_rate_limited" }));
    const retainedExpiredFailure = await currentPool().pool.query(
      `SELECT failure_id FROM browser_pairing_exchange_failures WHERE failure_id = $1`,
      [expiredFailureId],
    );
    expect(retainedExpiredFailure.rowCount).toBe(0);

    const unknownRequestSource = "unknown-request-source";
    const unknownRequestLifecycle = aggregateRequests[0]?.lifecycle;
    if (unknownRequestLifecycle === undefined) throw new Error("unknown request fixture missing");
    for (let attempt = 0; attempt < 20; attempt += 1) {
      await expect(
        unknownRequestLifecycle.exchange(
          `pair_missing_${attempt}`,
          { exchangeSecret: "Z".repeat(43), publicNonce: nonce },
          { origin: "https://hub.example.test", sourceAddress: unknownRequestSource },
        ),
      ).rejects.toEqual(expect.objectContaining({ code: "pairing_not_found" }));
    }
    await expect(
      unknownRequestLifecycle.exchange(
        "pair_missing_rate_limited",
        { exchangeSecret: "Z".repeat(43), publicNonce: nonce },
        { origin: "https://hub.example.test", sourceAddress: unknownRequestSource },
      ),
    ).rejects.toEqual(expect.objectContaining({ code: "pairing_rate_limited" }));
  });

  it("creates and confirms a browser pairing through the loopback admin CLI", async () => {
    const scope: OperatorGrantScope = {
      actions: ["archive"],
      commands: ["scan"],
      permissions: ["session.read"],
      scopeKeys: ["account-primary:inbox"],
      sessionIds: [`sess_cli_pairing_${randomUUID()}`],
      targetKinds: ["message"],
    };
    const created = JSON.parse(
      await executeBrowserPairingCli({
        command: "create",
        databaseUrl,
        operatorSubject: "cli-operator@example.test",
        origin: "https://hub.example.test",
        publicNonce: "N".repeat(22),
        requestedScope: scope,
      }),
    ) as {
      readonly request: {
        readonly requestId: string;
        readonly verificationPhrase: string;
      };
      readonly status: "created";
    };
    expect(created.status).toBe("created");

    const confirmed = JSON.parse(
      await executeBrowserPairingCli({
        actorSubject: "cli-admin@example.test",
        command: "confirm",
        databaseUrl,
        requestId: created.request.requestId,
        verificationPhrase: created.request.verificationPhrase,
      }),
    ) as { readonly status: string };

    expect(confirmed.status).toBe("confirmed");
    await expect(
      createBrowserPairingStore(currentPool()).inspect(created.request.requestId),
    ).resolves.toMatchObject({
      confirmedBySubject: "cli-admin@example.test",
    });
  });

  it("pairs a browser into dedicated operator routes and revokes it independently", async () => {
    const sessionId = `sess_browser_operator_${randomUUID()}`;
    await createDbSession(currentPool(), sessionId);
    const origin = "https://hub.example.test";
    const operatorApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      cors: { allowedOrigins: [origin] },
      eventFanout: { catchUpPollIntervalMs: 0 },
      resourceLimits: { ...defaultResourceLimits, restEventListMaxBytes: 4_096 },
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await operatorApp.listen(0);
    const operatorUrl = `http://127.0.0.1:${port}`;
    let operatorSocket: WebSocket | null = null;
    try {
      const nonce = "B".repeat(22);
      const scope: OperatorGrantScope = {
        actions: ["archive"],
        commands: ["authority-revoke", "backlog-preview", "scan"],
        permissions: [
          "approval.submit",
          "authority.revoke",
          "backlog-preview.request",
          "browser-session.read",
          "browser-session.revoke",
          "scan.request",
          "session.read",
          "websocket.connect",
        ],
        scopeKeys: ["account-primary:inbox"],
        sessionIds: [sessionId],
        targetKinds: ["message"],
      };
      const created = await requestStatusFrom<BrowserPairingCreateResponse>(
        operatorUrl,
        "/browser/pairing-requests",
        {
          authToken: null,
          body: {
            operatorSubject: "operator@example.test",
            publicNonce: nonce,
            requestedScope: scope,
          },
          headers: { origin },
          method: "POST",
        },
      );
      expect(created.status).toBe(201);
      expect(created.headers.get("cache-control")).toBe("no-store");
      await createBrowserPairingStore(currentPool()).confirm({
        actorSubject: "admin@example.test",
        confirmedAt: new Date(),
        requestId: created.body.request.requestId,
      });
      const exchanged = await requestStatusFrom<BrowserPairingExchangeResponse>(
        operatorUrl,
        `/browser/pairing-requests/${created.body.request.requestId}/exchange`,
        {
          authToken: null,
          body: { exchangeSecret: created.body.exchangeSecret, publicNonce: nonce },
          headers: { origin },
          method: "POST",
        },
      );
      const setCookie = exchanged.headers.get("set-cookie");
      expect(exchanged.status).toBe(200);
      expect(setCookie).toContain("__Host-Http-tether-operator=");
      expect(setCookie).toContain("Path=/");
      expect(setCookie).toContain("Secure");
      expect(setCookie).toContain("HttpOnly");
      expect(setCookie).toContain("SameSite=Strict");
      const cookie = setCookie?.split(";", 1)[0];
      expect(cookie).toBeTruthy();
      if (!cookie) throw new Error("browser operator cookie missing");
      const headers = { cookie, origin };
      const self = await requestStatusFrom(operatorUrl, "/operator/browser-session", {
        authToken: null,
        headers,
      });
      expect(self).toMatchObject({
        body: { grantJti: exchanged.body.grantJti, status: "active" },
        status: 200,
      });
      expect(self.headers.get("cache-control")).toBe("no-store");
      const sameOriginSelf = await requestStatusFrom(operatorUrl, "/operator/browser-session", {
        authToken: null,
        headers: { cookie },
      });
      expect(sameOriginSelf.status).toBe(200);
      const generic = await requestStatusFrom(operatorUrl, "/sessions", {
        authToken: null,
        headers,
      });
      expect(generic).toMatchObject({ status: 401 });
      const wrongSession = await requestStatusFrom(
        operatorUrl,
        "/operator/sessions/sess_other/snapshot",
        { authToken: null, headers },
      );
      expect(wrongSession).toMatchObject({
        body: { reason: "operator_session_denied" },
        status: 403,
      });
      const command = await requestStatusFrom<BrowserOperatorCommandResponse>(
        operatorUrl,
        `/operator/sessions/${sessionId}/commands`,
        {
          authToken: null,
          body: { command: "scan", scopeKey: "account-primary:inbox" },
          headers: { ...headers, "x-tether-csrf": exchanged.body.csrfToken },
          method: "POST",
        },
      );
      expect(command.status).toBe(201);
      const persistedCommand = await currentPool().pool.query<{
        readonly operatorCommandKey: string | null;
        readonly operatorGrantJti: string | null;
      }>(
        `SELECT operator_command_key AS "operatorCommandKey", operator_grant_jti AS "operatorGrantJti"
         FROM tasks WHERE session_id = $1 AND task_id = $2`,
        [sessionId, command.body.task.taskId],
      );
      expect(persistedCommand.rows[0]).toEqual({
        operatorCommandKey: expect.stringMatching(/^[0-9a-f]{64}$/u),
        operatorGrantJti: exchanged.body.grantJti,
      });
      const forgedOperatorTask = await requestStatusFrom(
        operatorUrl,
        `/sessions/${sessionId}/tasks`,
        {
          body: { kind: "operator.scan", objective: "forged operator command" },
          method: "POST",
        },
      );
      expect(forgedOperatorTask.status).toBe(400);
      const duplicateCommand = await requestStatusFrom<BrowserOperatorCommandResponse>(
        operatorUrl,
        `/operator/sessions/${sessionId}/commands`,
        {
          authToken: null,
          body: { command: "scan", scopeKey: "account-primary:inbox" },
          headers: { ...headers, "x-tether-csrf": exchanged.body.csrfToken },
          method: "POST",
        },
      );
      expect(duplicateCommand).toMatchObject({
        body: { status: "replayed", task: { taskId: command.body.task.taskId } },
        status: 200,
      });
      const snapshot = await requestStatusFrom(
        operatorUrl,
        `/operator/sessions/${sessionId}/snapshot`,
        { authToken: null, headers },
      );
      expect(snapshot).toMatchObject({
        body: {
          sessionId,
          truncated: { events: false, participants: false, tasks: false },
        },
        status: 200,
      });
      expect(Buffer.byteLength(snapshot.text)).toBeLessThanOrEqual(4_096);
      const csrfDenied = await requestStatusFrom(
        operatorUrl,
        `/operator/sessions/${sessionId}/commands`,
        {
          authToken: null,
          body: { command: "scan", scopeKey: "account-primary:inbox" },
          headers: { ...headers, "x-tether-csrf": "Z".repeat(43) },
          method: "POST",
        },
      );
      expect(csrfDenied).toMatchObject({
        body: { reason: "operator_csrf_denied" },
        status: 403,
      });
      const target = {
        action: "archive",
        digest: "digest_browser_operator",
        scopeKey: "account-primary:inbox",
        targetId: "message_browser_operator",
        targetKind: "message",
        targetRevision: "revision_browser_operator",
      };
      const task = await request<TaskResponse>(`/sessions/${sessionId}/tasks`, {
        body: { kind: "opaque_manifest_review", objective: "review browser target" },
        method: "POST",
      });
      const controller = {
        instanceId: `inst_browser_operator_${randomUUID()}`,
        participantId: `part_browser_operator_${randomUUID()}`,
      };
      const claimed = await request<TaskResponse>(
        `/sessions/${sessionId}/tasks/${task.task.taskId}/claim`,
        { body: controller, method: "POST" },
      );
      await request<TaskResponse>(`/sessions/${sessionId}/tasks/${task.task.taskId}/complete`, {
        body: {
          ...controller,
          claimId: requireClaimId(claimed.task),
          result: { targetManifest: [target] },
        },
        method: "POST",
      });
      const approvalPath = `/operator/sessions/${sessionId}/tasks/${task.task.taskId}/approval`;
      const operatorMutationHeaders = {
        ...headers,
        "x-tether-csrf": exchanged.body.csrfToken,
      };
      const scopeDenied = await requestStatusFrom(operatorUrl, approvalPath, {
        authToken: null,
        body: { decision: "approved", reason: {}, target: { ...target, scopeKey: "other" } },
        headers: operatorMutationHeaders,
        method: "POST",
      });
      expect(scopeDenied).toMatchObject({
        body: { reason: "operator_scope_key_denied" },
        status: 403,
      });
      const actionDenied = await requestStatusFrom(operatorUrl, approvalPath, {
        authToken: null,
        body: { decision: "approved", reason: {}, target: { ...target, action: "trash" } },
        headers: operatorMutationHeaders,
        method: "POST",
      });
      expect(actionDenied).toMatchObject({
        body: { reason: "operator_action_denied" },
        status: 403,
      });
      const manifestDenied = await requestStatusFrom(operatorUrl, approvalPath, {
        authToken: null,
        body: {
          decision: "approved",
          reason: {},
          target: { ...target, targetId: "message_guessed" },
        },
        headers: operatorMutationHeaders,
        method: "POST",
      });
      expect(manifestDenied).toMatchObject({
        body: { rejectionReason: "target_absent" },
        status: 409,
      });
      const approval = await requestStatusFrom(operatorUrl, approvalPath, {
        authToken: null,
        body: { decision: "approved", reason: {}, target },
        headers: operatorMutationHeaders,
        method: "POST",
      });
      const duplicate = await requestStatusFrom(operatorUrl, approvalPath, {
        authToken: null,
        body: { decision: "rejected", reason: {}, target },
        headers: operatorMutationHeaders,
        method: "POST",
      });
      expect(approval).toMatchObject({
        body: {
          approval: { decidedByParticipantId: "operator@example.test", decision: "approved" },
          status: "recorded",
        },
        status: 200,
      });
      expect(duplicate).toMatchObject({
        body: {
          approval: { decidedByParticipantId: "operator@example.test", decision: "approved" },
          existingDecision: "approved",
          status: "ignored",
        },
        status: 200,
      });
      const deniedOrigin = await requestStatusFrom(operatorUrl, "/operator/browser-session", {
        authToken: null,
        headers: { cookie, origin: "https://evil.example.test" },
      });
      expect(deniedOrigin).toMatchObject({
        body: { reason: "operator_origin_denied" },
        status: 403,
      });
      const websocketTicket = await requestStatusFrom<AuthTicketCreateResponse>(
        operatorUrl,
        "/operator/websocket-ticket",
        {
          authToken: null,
          headers: { ...headers, "x-tether-csrf": exchanged.body.csrfToken },
          method: "POST",
        },
      );
      expect(websocketTicket.status).toBe(201);
      expect(websocketTicket.headers.get("cache-control")).toBe("no-store");
      const operatorStreamUrl = new URL(
        `${operatorUrl.replace("http:", "ws:")}/sessions/${sessionId}/stream`,
      );
      operatorStreamUrl.searchParams.set("after", "0");
      operatorStreamUrl.searchParams.set("runtimeKind", "observer");
      operatorStreamUrl.searchParams.set("ticket", websocketTicket.body.ticket);
      operatorSocket = new WebSocket(operatorStreamUrl, { origin });
      const operatorMessages: unknown[] = [];
      operatorSocket.on("message", (data) => {
        operatorMessages.push(JSON.parse(String(data)) as unknown);
      });
      await waitForSocketOpen(operatorSocket);
      await waitFor(() => operatorMessages.some(isReplayCompleteEnvelope));
      const operatorSocketClose = waitForSocketCloseDetails(operatorSocket);
      const revoked = await requestStatusFrom(operatorUrl, "/operator/browser-session/revoke", {
        authToken: null,
        headers: { ...headers, "x-tether-csrf": exchanged.body.csrfToken },
        method: "POST",
      });
      expect(revoked).toMatchObject({ body: { status: "revoked" }, status: 200 });
      const revokedCommandTaskId = `task_revoked_operator_${randomUUID()}`;
      await expect(
        createOperatorCommandTaskWithEvent(currentPool(), {
          authority: {
            grantJti: exchanged.body.grantJti,
            request: {
              command: "scan",
              scopeKey: "account-primary:inbox",
              sessionId,
            },
          },
          eventSourceId: `evt_revoked_operator_${randomUUID()}`,
          taskId: revokedCommandTaskId,
        }),
      ).rejects.toMatchObject({
        name: "OperatorCommandAdmissionError",
        reason: "operator_grant_revoked",
      });
      await expect(
        getTask(currentPool(), { sessionId, taskId: revokedCommandTaskId }),
      ).resolves.toBeNull();
      await expect(operatorSocketClose).resolves.toEqual({
        code: 1008,
        reason: "auth_grant_revoked",
      });
      const afterRevocation = await requestStatusFrom(operatorUrl, "/operator/browser-session", {
        authToken: null,
        headers,
      });
      expect(afterRevocation).toMatchObject({
        body: { reason: "operator_auth_denied" },
        status: 401,
      });
      const rePair = await requestStatusFrom<BrowserPairingCreateResponse>(
        operatorUrl,
        "/browser/pairing-requests",
        {
          authToken: null,
          body: {
            operatorSubject: "operator@example.test",
            publicNonce: "P".repeat(22),
            requestedScope: scope,
          },
          headers: { origin },
          method: "POST",
        },
      );
      expect(rePair).toMatchObject({ body: { status: "created" }, status: 201 });
      expect(rePair.body.request.requestId).not.toBe(created.body.request.requestId);
    } finally {
      if (operatorSocket && operatorSocket.readyState !== WebSocket.CLOSED) {
        operatorSocket.close();
        await waitForSocketClose(operatorSocket);
      }
      await operatorApp.close();
    }
  });

  it("persists grant authority, bounded audit state, and hashed tickets through narrow stores", async () => {
    const stores = createAuthPersistenceStores(currentPool());
    const issuedAt = new Date("2026-01-01T00:00:00.000Z");
    const expiresAt = new Date("2026-01-02T00:00:00.000Z");
    const jti = `grant_${randomUUID()}`;
    const ticketHash = "a".repeat(64);
    const auditId = `audit_${randomUUID()}`;

    await stores.createGrantWithAudit({
      audit: {
        action: "grant.created",
        actorSubject: "part_e2e_store",
        auditId,
        metadata: { requestId: "req_e2e_create" },
        occurredAt: issuedAt,
        reasonCode: "bootstrap",
      },
      grant: {
        audience: "tether-rest",
        expiresAt,
        issuedAt,
        issuer: "https://auth.e2e.tether.local",
        jti,
        kid: testAuthSigningKid,
        metadata: { requestId: "req_e2e_create", source: "bootstrap" },
        revokedAt: null,
        role: "admin",
        sessionScope: "*",
        subject: "part_e2e_store",
      },
    });
    await stores.tickets.create({
      admissionMetadata: {
        remoteAddressHash: null,
        replicaId: "replica_e2e",
        transport: "websocket",
      },
      audience: "tether-websocket",
      consumedAt: null,
      createdAt: issuedAt,
      expiresAt: new Date("2026-01-01T00:00:30.000Z"),
      parentGrantJti: jti,
      ticketHash,
    });

    await expect(stores.grants.findByJti(jti)).resolves.toMatchObject({
      audience: "tether-rest",
      jti,
      revokedAt: null,
      subject: "part_e2e_store",
    });
    await expect(stores.audits.listForGrant(jti, 10)).resolves.toEqual([
      expect.objectContaining({ action: "grant.created", grantJti: jti }),
    ]);
    await expect(stores.tickets.findByHash(ticketHash)).resolves.toMatchObject({
      parentGrantJti: jti,
      ticketHash,
    });

    const rolledBackJti = `grant_${randomUUID()}`;
    await expect(
      stores.createGrantWithAudit({
        audit: {
          action: "grant.created",
          actorSubject: "part_e2e_store",
          auditId,
          metadata: { requestId: "req_e2e_rollback" },
          occurredAt: issuedAt,
          reasonCode: "bootstrap",
        },
        grant: {
          audience: "tether-rest",
          expiresAt,
          issuedAt,
          issuer: "https://auth.e2e.tether.local",
          jti: rolledBackJti,
          kid: testAuthSigningKid,
          metadata: { requestId: "req_e2e_rollback", source: "bootstrap" },
          revokedAt: null,
          role: "observer",
          sessionScope: "*",
          subject: "part_e2e_rollback",
        },
      }),
    ).rejects.toThrow("auth_grant_create_failed");
    await expect(stores.grants.findByJti(rolledBackJti)).resolves.toBeNull();

    const revokeRollbackJti = `grant_${randomUUID()}`;
    await stores.createGrantWithAudit({
      audit: {
        action: "grant.created",
        actorSubject: "part_e2e_store",
        auditId: `audit_${randomUUID()}`,
        metadata: { requestId: "req_e2e_revoke_rollback_create" },
        occurredAt: issuedAt,
        reasonCode: "bootstrap",
      },
      grant: {
        audience: "tether-rest",
        expiresAt,
        issuedAt,
        issuer: "https://auth.e2e.tether.local",
        jti: revokeRollbackJti,
        kid: testAuthSigningKid,
        metadata: {
          requestId: "req_e2e_revoke_rollback_create",
          source: "bootstrap",
        },
        revokedAt: null,
        role: "observer",
        sessionScope: "*",
        subject: "part_e2e_revoke_rollback",
      },
    });
    await expect(
      stores.revokeGrantWithAudit({
        audit: {
          action: "grant.revoked",
          actorSubject: "part_e2e_store",
          auditId,
          metadata: { requestId: "req_e2e_revoke_rollback" },
          occurredAt: new Date("2026-01-01T00:01:00.000Z"),
          reasonCode: "operator-request",
        },
        jti: revokeRollbackJti,
        revokedAt: new Date("2026-01-01T00:01:00.000Z"),
      }),
    ).rejects.toThrow("auth_grant_revoke_failed");
    await expect(stores.grants.findByJti(revokeRollbackJti)).resolves.toMatchObject({
      revokedAt: null,
    });
    await expect(stores.audits.listForGrant(revokeRollbackJti, 10)).resolves.toHaveLength(1);

    const overlongGrantJti = `grant_${randomUUID()}`;
    await expect(
      stores.createGrantWithAudit({
        audit: {
          action: "grant.created",
          actorSubject: "part_e2e_store",
          auditId: `audit_${randomUUID()}`,
          metadata: { requestId: "req_e2e_overlong" },
          occurredAt: issuedAt,
          reasonCode: "bootstrap",
        },
        grant: {
          audience: "tether-rest",
          expiresAt: new Date("2026-01-08T00:00:00.001Z"),
          issuedAt,
          issuer: "https://auth.e2e.tether.local",
          jti: overlongGrantJti,
          kid: testAuthSigningKid,
          metadata: { requestId: "req_e2e_overlong", source: "bootstrap" },
          revokedAt: null,
          role: "observer",
          sessionScope: "*",
          subject: "part_e2e_overlong",
        },
      }),
    ).rejects.toThrow("auth_grant_create_failed");
    await expect(stores.grants.findByJti(overlongGrantJti)).resolves.toBeNull();

    const credentialMarker = "tgr2.secret_payload.secret_signature";
    const unsafeGrantMetadata = {
      bearer: credentialMarker,
      requestId: null,
      source: "bootstrap",
    } as unknown as AuthGrantMetadata;
    const rejectedMetadataWrite = stores.createGrantWithAudit({
      audit: {
        action: "grant.created",
        actorSubject: "part_e2e_store",
        auditId: `audit_${randomUUID()}`,
        metadata: { requestId: null },
        occurredAt: issuedAt,
        reasonCode: "bootstrap",
      },
      grant: {
        audience: "tether-rest",
        expiresAt,
        issuedAt,
        issuer: "https://auth.e2e.tether.local",
        jti: `grant_${randomUUID()}`,
        kid: testAuthSigningKid,
        metadata: unsafeGrantMetadata,
        revokedAt: null,
        role: "observer",
        sessionScope: "*",
        subject: "part_e2e_rejected_metadata",
      },
    });
    await expect(rejectedMetadataWrite).rejects.toThrow("auth_metadata_invalid");
    await rejectedMetadataWrite.catch((error: unknown) => {
      expect(String(error)).not.toContain(credentialMarker);
      expect(String(error)).not.toContain("part_e2e_rejected_metadata");
    });

    const unsafeAuditMetadata = {
      authorization: credentialMarker,
      requestId: null,
    } as unknown as AuthGrantAuditMetadata;
    await expect(
      stores.createGrantWithAudit({
        audit: {
          action: "grant.created",
          actorSubject: "part_e2e_store",
          auditId: `audit_${randomUUID()}`,
          metadata: unsafeAuditMetadata,
          occurredAt: issuedAt,
          reasonCode: "bootstrap",
        },
        grant: {
          audience: "tether-rest",
          expiresAt,
          issuedAt,
          issuer: "https://auth.e2e.tether.local",
          jti: `grant_${randomUUID()}`,
          kid: testAuthSigningKid,
          metadata: { requestId: null, source: "bootstrap" },
          revokedAt: null,
          role: "observer",
          sessionScope: "*",
          subject: "part_e2e_rejected_audit_metadata",
        },
      }),
    ).rejects.toThrow("auth_metadata_invalid");

    const unsafeAdmissionMetadata = {
      remoteAddressHash: null,
      replicaId: "replica_e2e",
      ticket: credentialMarker,
      transport: "websocket",
    } as unknown as AuthTicketAdmissionMetadata;
    await expect(
      stores.tickets.create({
        admissionMetadata: unsafeAdmissionMetadata,
        audience: "tether-websocket",
        consumedAt: null,
        createdAt: issuedAt,
        expiresAt: new Date("2026-01-01T00:00:30.000Z"),
        parentGrantJti: jti,
        ticketHash: "b".repeat(64),
      }),
    ).rejects.toThrow("auth_metadata_invalid");

    const overlongTicketHash = "d".repeat(64);
    await expect(
      stores.tickets.create({
        admissionMetadata: {
          remoteAddressHash: null,
          replicaId: "replica_e2e",
          transport: "websocket",
        },
        audience: "tether-websocket",
        consumedAt: null,
        createdAt: issuedAt,
        expiresAt: new Date("2026-01-01T00:00:30.001Z"),
        parentGrantJti: jti,
        ticketHash: overlongTicketHash,
      }),
    ).rejects.toThrow("auth_ticket_create_failed");
    await expect(stores.tickets.findByHash(overlongTicketHash)).resolves.toBeNull();

    const revokedAt = new Date("2026-01-01T00:01:00.000Z");
    await expect(
      stores.revokeGrantWithAudit({
        audit: {
          action: "grant.revoked",
          actorSubject: "part_e2e_store",
          auditId: `audit_${randomUUID()}`,
          metadata: { requestId: "req_e2e_revoke" },
          occurredAt: revokedAt,
          reasonCode: "operator-request",
        },
        jti,
        revokedAt,
      }),
    ).resolves.toMatchObject({ status: "revoked" });
    await expect(
      stores.revokeGrantWithAudit({
        audit: {
          action: "grant.revoked",
          actorSubject: "part_e2e_store",
          auditId: `audit_${randomUUID()}`,
          metadata: { requestId: "req_e2e_revoke_retry" },
          occurredAt: revokedAt,
          reasonCode: "operator-request",
        },
        jti,
        revokedAt,
      }),
    ).resolves.toMatchObject({ status: "already_revoked" });
    await expect(stores.audits.listForGrant(jti, 10)).resolves.toHaveLength(2);

    const authColumns = await currentPool().pool.query<{
      readonly columnName: string;
    }>(`
      SELECT column_name AS "columnName"
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name IN ('auth_grants', 'auth_grant_audit_events', 'auth_tickets')
    `);
    const authColumnNames = authColumns.rows.map((row) => row.columnName);
    for (const forbiddenColumnName of ["bearer", "token", "ticket"]) {
      expect(authColumnNames).not.toContain(forbiddenColumnName);
    }

    await expect(
      currentPool().pool.query(
        `
          INSERT INTO auth_grants (
            audience, expires_at, issued_at, issuer, jti, kid, metadata,
            revoked_at, role, session_scope, subject
          ) VALUES (
            'tether-rest', $1, $2, 'https://auth.e2e.tether.local', $3,
            $4, $5::jsonb, NULL, 'observer', '*', 'part_invalid_metadata'
          )
        `,
        [
          expiresAt,
          issuedAt,
          `grant_${randomUUID()}`,
          testAuthSigningKid,
          JSON.stringify({ requestId: null, source: null }),
        ],
      ),
    ).rejects.toThrow();

    await expect(
      currentPool().pool.query(
        `
          INSERT INTO auth_grants (
            audience, expires_at, issued_at, issuer, jti, kid, metadata,
            revoked_at, role, session_scope, subject
          ) VALUES (
            'tether-rest', $1, $2, 'https://auth.e2e.tether.local', $3,
            $4, $5::jsonb, NULL, 'observer', '*', 'part_overlong_database_grant'
          )
        `,
        [
          new Date("2026-01-08T00:00:00.001Z"),
          issuedAt,
          `grant_${randomUUID()}`,
          testAuthSigningKid,
          JSON.stringify({ requestId: null, source: "bootstrap" }),
        ],
      ),
    ).rejects.toThrow();

    await expect(
      currentPool().pool.query(
        `
          INSERT INTO auth_tickets (
            admission_metadata, audience, consumed_at, created_at, expires_at,
            parent_grant_jti, ticket_hash
          ) VALUES (
            $1::jsonb, 'tether-websocket', NULL, $3, $2, $4, $5
          )
        `,
        [
          JSON.stringify({
            remoteAddressHash: null,
            replicaId: null,
            transport: "websocket",
          }),
          new Date("2026-01-01T00:00:30.000Z"),
          issuedAt,
          jti,
          "c".repeat(64),
        ],
      ),
    ).rejects.toThrow();

    await expect(
      currentPool().pool.query(
        `
          INSERT INTO auth_tickets (
            admission_metadata, audience, consumed_at, created_at, expires_at,
            parent_grant_jti, ticket_hash
          ) VALUES (
            $1::jsonb, 'tether-websocket', NULL, $2, $3, $4, $5
          )
        `,
        [
          JSON.stringify({
            remoteAddressHash: null,
            replicaId: "replica_e2e",
            transport: "websocket",
          }),
          issuedAt,
          new Date("2026-01-01T00:00:30.001Z"),
          jti,
          "e".repeat(64),
        ],
      ),
    ).rejects.toThrow();
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
      const scalabilityTables = await legacyDatabase.pool.query<{ readonly count: number }>(
        `
          SELECT count(*)::int AS count
          FROM information_schema.tables
          WHERE table_schema = 'public'
            AND table_name IN ('session_projections', 'session_summaries')
        `,
      );

      expect(columns.rows[0]?.count).toBe(4);
      expect(migrationRows.rows[0]?.count).toBeGreaterThanOrEqual(10);
      expect(scalabilityTables.rows[0]?.count).toBe(2);
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

  it("rejects a journal-less auth schema with a weakened lifetime constraint", async () => {
    const databaseName = `tether_e2e_weakened_auth_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(databaseName));
    try {
      await createDatabase(databaseName);
      await applyLegacyMigrationPrefix(database, authFoundationMigrationIndex);
      await database.pool.query(`
        ALTER TABLE auth_grants DROP CONSTRAINT auth_grants_lifetime_check;
        ALTER TABLE auth_grants ADD CONSTRAINT auth_grants_lifetime_check
          CHECK (expires_at > issued_at);
      `);

      await expect(migrate(database)).rejects.toMatchObject({
        name: "DatabaseMigrationError",
        reason: "unsupported_schema",
        recognizedPrefix: preAuthFoundationMigrationIndex,
      });

      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      expect(journal.rows[0]?.count).toBe(0);
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it("rejects a partial journal-less auth migration before generated DDL", async () => {
    const databaseName = `tether_e2e_partial_auth_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(databaseName));
    try {
      await createDatabase(databaseName);
      await applyLegacyMigrationPrefix(database, preAuthFoundationMigrationIndex);
      await database.pool.query(`CREATE TABLE auth_grants (jti text PRIMARY KEY NOT NULL)`);

      await expect(migrate(database)).rejects.toMatchObject({
        name: "DatabaseMigrationError",
        reason: "unsupported_schema",
        recognizedPrefix: preAuthFoundationMigrationIndex,
      });

      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      const laterAuthTable = await database.pool.query<{
        readonly exists: boolean;
      }>(`SELECT to_regclass('public.auth_tickets') IS NOT NULL AS exists`);
      expect(journal.rows[0]?.count).toBe(0);
      expect(laterAuthTable.rows[0]?.exists).toBe(false);
    } finally {
      await database.end();
      await dropDatabase(databaseName);
    }
  });

  it.each([
    {
      label: "removed nullability",
      mutationSql: "ALTER TABLE auth_grants ALTER COLUMN subject DROP NOT NULL",
    },
    {
      label: "changed column type",
      mutationSql: "ALTER TABLE auth_grants ALTER COLUMN issuer TYPE varchar(512)",
    },
    {
      label: "changed column default",
      mutationSql: "ALTER TABLE auth_tickets ALTER COLUMN created_at SET DEFAULT now()",
    },
    {
      label: "credential-bearing extra column",
      mutationSql:
        "ALTER TABLE auth_grants ADD COLUMN bearer text NOT NULL DEFAULT 'tgr2.secret.signature'",
    },
    {
      label: "weakened constraint suffix",
      mutationSql: `
        ALTER TABLE auth_grants DROP CONSTRAINT auth_grants_lifetime_check;
        ALTER TABLE auth_grants ADD CONSTRAINT auth_grants_lifetime_check
          CHECK (expires_at <= issued_at + interval '7 days' OR true);
      `,
    },
    {
      label: "unvalidated constraint",
      mutationSql: `
        ALTER TABLE auth_tickets DROP CONSTRAINT auth_tickets_hash_check;
        ALTER TABLE auth_tickets ADD CONSTRAINT auth_tickets_hash_check
          CHECK (ticket_hash ~ '^[0-9a-f]{64}$') NOT VALID;
      `,
    },
    {
      label: "altered foreign-key action",
      mutationSql: `
        ALTER TABLE auth_tickets
          DROP CONSTRAINT auth_tickets_parent_grant_jti_auth_grants_jti_fk;
        ALTER TABLE auth_tickets
          ADD CONSTRAINT auth_tickets_parent_grant_jti_auth_grants_jti_fk
          FOREIGN KEY (parent_grant_jti) REFERENCES auth_grants(jti) ON DELETE CASCADE;
      `,
    },
  ])("rejects journal-less auth schema with $label", async ({ label, mutationSql }) => {
    const databaseName = `tether_e2e_auth_fact_${label.replaceAll(/[^a-z]+/gu, "_")}_${randomUUID().replaceAll("-", "_")}`;
    const database = createPool(buildDatabaseUrl(databaseName));
    try {
      await createDatabase(databaseName);
      await applyLegacyMigrationPrefix(database, authFoundationMigrationIndex);
      await database.pool.query(mutationSql);

      await expect(migrate(database)).rejects.toMatchObject({
        name: "DatabaseMigrationError",
        reason: "unsupported_schema",
        recognizedPrefix: preAuthFoundationMigrationIndex,
      });
      const journal = await database.pool.query<{ readonly count: number }>(
        `SELECT count(*)::int AS count FROM drizzle.__drizzle_migrations`,
      );
      expect(journal.rows[0]?.count).toBe(0);
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

  it("reproduces that a current legacy bearer has no server-side revocation lifecycle", async () => {
    const authToken = mintE2eToken({
      participantId: "part_e2e_non_revocable",
      role: "admin",
      sessionId: "*",
    });

    await expect(
      requestFrom<SessionListResponse>(baseUrl, "/sessions", { authToken }),
    ).resolves.toEqual(expect.objectContaining({ sessions: expect.any(Array) }));

    const attemptedRevocation = await requestStatusFrom(baseUrl, "/auth/grants/revoke", {
      authToken,
      body: {},
      method: "POST",
    });
    expect(attemptedRevocation.status).toBe(404);

    await expect(
      requestFrom<SessionListResponse>(baseUrl, "/sessions", { authToken }),
    ).resolves.toEqual(expect.objectContaining({ sessions: expect.any(Array) }));
  });

  it("creates, inspects, lists, and idempotently revokes a durable auth grant", async () => {
    const created = await requestFrom<AuthGrantCreateResponse>(baseUrl, "/auth/grants", {
      body: {
        role: "admin",
        sessionScope: "*",
        subject: "part_auth_lifecycle",
        ttlSeconds: 3_600,
      },
      method: "POST",
    });

    expect(created.bearer).toMatch(/^tgr2\./u);
    expect(created.grant).toMatchObject({
      audience: "tether-rest",
      revokedAt: null,
      role: "admin",
      sessionScope: "*",
      subject: "part_auth_lifecycle",
    });
    expect(created.grant).not.toHaveProperty("metadata");

    await expect(
      requestFrom<SessionListResponse>(baseUrl, "/sessions", {
        authToken: created.bearer,
      }),
    ).resolves.toEqual(expect.objectContaining({ sessions: expect.any(Array) }));

    const ticket = await requestFrom<AuthTicketCreateResponse>(baseUrl, "/auth/tickets", {
      authToken: created.bearer,
      body: {},
      method: "POST",
    });
    expect(ticket.ticket).toMatch(/^[A-Za-z0-9_-]{43}$/u);
    expect(new Date(ticket.expiresAt).getTime() - Date.now()).toBeGreaterThan(25_000);
    const persistedTicket = await currentPool().pool.query<{
      readonly parentGrantJti: string;
      readonly rawTicketMatches: boolean;
      readonly ticketHash: string;
    }>(
      `
        SELECT
          parent_grant_jti AS "parentGrantJti",
          ticket_hash = $1 AS "rawTicketMatches",
          ticket_hash AS "ticketHash"
        FROM auth_tickets
        WHERE parent_grant_jti = $2
      `,
      [ticket.ticket, created.grant.jti],
    );
    expect(persistedTicket.rows).toEqual([
      {
        parentGrantJti: created.grant.jti,
        rawTicketMatches: false,
        ticketHash: expect.stringMatching(/^[0-9a-f]{64}$/u) as string,
      },
    ]);

    const inspected = await requestFrom<AuthGrantReadResponse>(
      baseUrl,
      `/auth/grants/${created.grant.jti}`,
    );
    expect(inspected.grant).toEqual(created.grant);
    expect(JSON.stringify(inspected)).not.toContain(created.bearer);
    expect(inspected.grant).not.toHaveProperty("signature");

    const listed = await requestFrom<AuthGrantListResponse>(baseUrl, "/auth/grants?limit=1");
    expect(listed.grants).toHaveLength(1);
    expect(listed.grants[0]).toEqual(created.grant);
    expect(JSON.stringify(listed)).not.toContain(created.bearer);

    const revoked = await requestStatusFrom<AuthGrantReadResponse & { readonly status: string }>(
      baseUrl,
      `/auth/grants/${created.grant.jti}/revoke`,
      {
        body: {},
        method: "POST",
      },
    );
    expect(revoked.status).toBe(200);
    expect(revoked.body.status).toBe("revoked");
    expect(revoked.body.grant.revokedAt).not.toBeNull();

    const repeated = await requestStatusFrom<AuthGrantReadResponse & { readonly status: string }>(
      baseUrl,
      `/auth/grants/${created.grant.jti}/revoke`,
      {
        body: {},
        method: "POST",
      },
    );
    expect(repeated.status).toBe(200);
    expect(repeated.body.status).toBe("already_revoked");
    expect(repeated.body.grant.revokedAt).toBe(revoked.body.grant.revokedAt);

    const deniedAfterRevocation = await requestStatusFrom(baseUrl, "/sessions", {
      authToken: created.bearer,
    });
    expect(deniedAfterRevocation.status).toBe(401);
    expect(deniedAfterRevocation.body).toEqual({
      error: "Unauthorized",
      reason: "auth_grant_revoked",
    });

    const audits = await createAuthPersistenceStores(currentPool()).audits.listForGrant(
      created.grant.jti,
      10,
    );
    expect(audits.map((audit) => audit.action).sort()).toEqual(["grant.created", "grant.revoked"]);
    expect(JSON.stringify(audits)).not.toContain(created.bearer);

    const missing = await requestStatusFrom(baseUrl, "/auth/grants/grant_missing");
    expect(missing.status).toBe(404);
    expect(missing.text).not.toContain(created.bearer);

    const invalidLimit = await requestStatusFrom(baseUrl, "/auth/grants?limit=101");
    expect(invalidLimit.status).toBe(400);
    expect(invalidLimit.text).not.toContain(created.bearer);

    const invalidJti = await requestStatusFrom(baseUrl, "/auth/grants/not-a-grant");
    expect(invalidJti.status).toBe(400);
    expect(invalidJti.text).not.toContain(created.bearer);
  });

  it("atomically consumes one ticket exactly once under a concurrent race", async () => {
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "observer",
        sessionScope: "*",
        subject: `part_ticket_race_${randomUUID()}`,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const minted = await request<AuthTicketCreateResponse>("/auth/tickets", {
      authToken: parent.bearer,
      body: {},
      method: "POST",
    });
    const store = createAuthPersistenceStores(currentPool()).tickets;

    const results = await Promise.all([
      store.consume(hashAuthTicket(minted.ticket)),
      store.consume(hashAuthTicket(minted.ticket)),
    ]);

    expect(results.filter((result) => result !== null)).toHaveLength(1);
    expect(results.find((result) => result !== null)).toMatchObject({
      parentGrantJti: parent.grant.jti,
      ticketHash: hashAuthTicket(minted.ticket),
    });
    await expect(store.findByHash(hashAuthTicket(minted.ticket))).resolves.toMatchObject({
      consumedAt: expect.any(Date) as Date,
    });
  });

  it("admits one of two replicas with one ticket and binds the winner to its parent grant", async () => {
    const session = await createSession();
    const participantId = `part_ticket_replica_${randomUUID()}`;
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const minted = await request<AuthTicketCreateResponse>("/auth/tickets", {
      authToken: parent.bearer,
      body: {},
      method: "POST",
    });
    const durableTicketStore = createAuthPersistenceStores(currentPool()).tickets;
    let consumeArrivals = 0;
    let releaseConsumes = (): void => undefined;
    const bothConsumesArrived = new Promise<void>((resolve) => {
      releaseConsumes = resolve;
    });
    const coordinatedTicketStore = {
      consume: async (ticketHash: string) => {
        consumeArrivals += 1;
        if (consumeArrivals === 2) releaseConsumes();
        await bothConsumesArrived;
        return durableTicketStore.consume(ticketHash);
      },
      create: durableTicketStore.create,
      findByHash: durableTicketStore.findByHash,
    };
    const replicas = [
      createAppServer(currentPool(), {
        auth: { ...e2eAuthOptions, ticketStore: coordinatedTicketStore },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      }),
      createAppServer(currentPool(), {
        auth: { ...e2eAuthOptions, ticketStore: coordinatedTicketStore },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      }),
    ] as const;
    const ports = await Promise.all(replicas.map((replica) => replica.listen(0)));
    const sockets = ports.map((port, index) => {
      const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
      url.searchParams.set("after", "0");
      url.searchParams.set("instanceId", `inst_ticket_replica_${index}_${randomUUID()}`);
      url.searchParams.set("participantId", participantId);
      url.searchParams.set("runtimeKind", "codex");
      url.searchParams.set("ticket", minted.ticket);
      return new WebSocket(url);
    });
    const messages = sockets.map((): unknown[] => []);
    sockets.forEach((socket, index) => {
      socket.on("message", (data) => messages[index]?.push(JSON.parse(String(data)) as unknown));
    });

    try {
      await Promise.all(sockets.map(waitForSocketOpen));
      await waitFor(() => messages.every((received) => received.length > 0));
      expect(messages.filter((received) => received.some(isReplayCompleteEnvelope))).toHaveLength(
        1,
      );
      expect(
        messages.filter((received) =>
          received.some((message) => isWebSocketErrorWithReason(message, "auth_ticket_consumed")),
        ),
      ).toHaveLength(1);
      const winnerIndex = messages.findIndex((received) => received.some(isReplayCompleteEnvelope));
      const winner = sockets[winnerIndex];
      const winnerMessages = messages[winnerIndex];
      if (winner === undefined || winnerMessages === undefined) {
        throw new Error("Ticket race did not produce one admitted socket");
      }

      await currentPool().pool.query(
        `UPDATE auth_tickets
         SET expires_at = GREATEST(consumed_at, created_at + INTERVAL '1 millisecond')
         WHERE ticket_hash = $1`,
        [hashAuthTicket(minted.ticket)],
      );
      const eventId = `evt_ticket_expired_after_admission_${randomUUID()}`;
      const requestId = `req_ticket_expired_after_admission_${randomUUID()}`;
      winner.send(
        JSON.stringify({
          eventId,
          op: webSocketOperation.publish,
          payload: { text: "ticket expiry does not close admitted socket" },
          producerId: participantId,
          requestId,
          type: sessionEventType.userMessage,
        }),
      );
      await waitFor(() =>
        winnerMessages.some(
          (message) => isCommandResultEnvelope(message) && message.requestId === requestId,
        ),
      );
      const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
      expect(events.events.map((event) => event.eventId)).toContain(eventId);
      expect(JSON.stringify(messages)).not.toContain(minted.ticket);
    } finally {
      releaseConsumes();
      for (const socket of sockets) {
        if (socket.readyState !== WebSocket.CLOSED) {
          socket.close();
          await waitForSocketClose(socket);
        }
      }
      await Promise.all(replicas.map((replica) => replica.close()));
    }
  });

  it("burns a consumed ticket when the transport fails before the upgrade response", async () => {
    const session = await createSession();
    const participantId = `part_ticket_transport_${randomUUID()}`;
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const minted = await request<AuthTicketCreateResponse>("/auth/tickets", {
      authToken: parent.bearer,
      body: {},
      method: "POST",
    });
    const url = new URL(`${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream`);
    url.searchParams.set("after", "0");
    url.searchParams.set("instanceId", `inst_ticket_transport_${randomUUID()}`);
    url.searchParams.set("participantId", participantId);
    url.searchParams.set("runtimeKind", "codex");
    url.searchParams.set("ticket", minted.ticket);

    const failedUpgrade = await sendInvalidWebSocketUpgrade(url);
    expect(failedUpgrade).toContain("HTTP/1.1 400");
    await expect(
      createAuthPersistenceStores(currentPool()).tickets.findByHash(hashAuthTicket(minted.ticket)),
    ).resolves.toMatchObject({ consumedAt: expect.any(Date) as Date });

    const replay = new WebSocket(url);
    const messages: unknown[] = [];
    replay.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));
    try {
      await waitForSocketOpen(replay);
      await waitFor(() =>
        messages.some((message) => isWebSocketErrorWithReason(message, "auth_ticket_consumed")),
      );
      expect(JSON.stringify(messages)).not.toContain(minted.ticket);
    } finally {
      if (replay.readyState !== WebSocket.CLOSED) {
        replay.close();
        await waitForSocketClose(replay);
      }
    }
  });

  it("admits Node WebSockets with an Authorization header and no credential query", async () => {
    const session = await createSession();
    const participantId = `part_ws_header_${randomUUID()}`;
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const url = new URL(`${baseUrl.replace("http:", "ws:")}/sessions/${session.sessionId}/stream`);
    url.searchParams.set("after", "0");
    url.searchParams.set("instanceId", `inst_ws_header_${randomUUID()}`);
    url.searchParams.set("participantId", participantId);
    url.searchParams.set("runtimeKind", "codex");
    expect(url.searchParams.has("access_token")).toBe(false);
    expect(url.searchParams.has("ticket")).toBe(false);
    expect(url.toString()).not.toContain(parent.bearer);

    const socket = new WebSocket(url, {
      headers: { authorization: `Bearer ${parent.bearer}` },
    });
    const messages: unknown[] = [];
    socket.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));
    try {
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));
      expect(JSON.stringify(messages)).not.toContain(parent.bearer);
    } finally {
      if (socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
    }
  });

  it("registers participant, observer, host, and viewer sockets in one auth registry", async () => {
    const session = await createSession();
    const participantId = `part_auth_registry_${randomUUID()}`;
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const registryApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      authRevocation: { listenEnabled: false, pollIntervalMs: 0 },
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      sessionService: { controlEpochEnforcement: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await registryApp.listen(0);
    const streamKinds = ["codex", "observer", "host", "viewer"] as const;
    const sockets = streamKinds.map((runtimeKind) => {
      const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
      url.searchParams.set("after", "0");
      url.searchParams.set("instanceId", `inst_auth_registry_${runtimeKind}_${randomUUID()}`);
      url.searchParams.set("participantId", participantId);
      url.searchParams.set("runtimeKind", runtimeKind);
      return new WebSocket(url, {
        headers: { authorization: `Bearer ${parent.bearer}` },
      });
    });
    const messages = sockets.map((): unknown[] => []);
    sockets.forEach((socket, index) => {
      socket.on("message", (data) => messages[index]?.push(JSON.parse(String(data)) as unknown));
    });

    try {
      await Promise.all(sockets.map(waitForSocketOpen));
      await waitFor(() => messages.every((received) => received.some(isReplayCompleteEnvelope)));
      expect(registryApp.debugInfo().authSockets).toMatchObject({
        grantCount: 1,
        socketCount: 4,
        socketsByStream: {
          host: 1,
          observer: 1,
          participant: 1,
          viewer: 1,
        },
        timerCount: 4,
      });
      expect(JSON.stringify(registryApp.debugInfo().authSockets)).not.toContain(parent.grant.jti);
    } finally {
      for (const socket of sockets) {
        if (socket.readyState !== WebSocket.CLOSED) {
          socket.close();
          await waitForSocketClose(socket);
        }
      }
      await waitFor(() => registryApp.debugInfo().authSockets.socketCount === 0);
      expect(registryApp.debugInfo().authSockets).toMatchObject({
        grantCount: 0,
        socketCount: 0,
        timerCount: 0,
      });
      await registryApp.close();
    }
  });

  it("closes matching sockets across two replicas within five seconds of revocation", async () => {
    const session = await createSession();
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "observer",
        sessionScope: session.sessionId,
        subject: `part_revocation_replica_${randomUUID()}`,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const replicas = [
      createAppServer(currentPool(), {
        auth: e2eAuthOptions,
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      }),
      createAppServer(currentPool(), {
        auth: e2eAuthOptions,
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      }),
    ] as const;
    const firstPort = await replicas[0].listen(0);
    const secondPort = await replicas[1].listen(0);
    const sockets = [firstPort, secondPort].map((port, index) => {
      const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
      url.searchParams.set("after", "0");
      url.searchParams.set("instanceId", `inst_revocation_replica_${index}_${randomUUID()}`);
      url.searchParams.set("runtimeKind", "observer");
      return new WebSocket(url, {
        headers: { authorization: `Bearer ${parent.bearer}` },
      });
    });
    const messages = sockets.map((): unknown[] => []);
    sockets.forEach((socket, index) => {
      socket.on("message", (data) => messages[index]?.push(JSON.parse(String(data)) as unknown));
    });

    try {
      await Promise.all(sockets.map(waitForSocketOpen));
      await waitFor(() => messages.every((received) => received.some(isReplayCompleteEnvelope)));
      const closeResults = sockets.map(waitForSocketCloseDetails);
      const revokedAt = Date.now();
      await request(`/auth/grants/${parent.grant.jti}/revoke`, {
        body: {},
        method: "POST",
      });
      const closed = await Promise.all(closeResults);

      expect(Date.now() - revokedAt).toBeLessThan(5_000);
      expect(closed).toEqual([
        { code: 1008, reason: "auth_grant_revoked" },
        { code: 1008, reason: "auth_grant_revoked" },
      ]);
      for (const replica of replicas) {
        expect(replica.debugInfo()).toMatchObject({
          authSockets: { closeCount: 1, socketCount: 0 },
        });
      }
    } finally {
      for (const socket of sockets) {
        if (socket.readyState !== WebSocket.CLOSED) socket.terminate();
      }
      await Promise.all(replicas.map((replica) => replica.close()));
    }
  });

  it("repairs a missed revocation notification through bounded PostgreSQL polling", async () => {
    const session = await createSession();
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "observer",
        sessionScope: session.sessionId,
        subject: `part_revocation_poll_${randomUUID()}`,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const pollingApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      authRevocation: { listenEnabled: false, pollIntervalMs: 100 },
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await pollingApp.listen(0);
    const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
    url.searchParams.set("after", "0");
    url.searchParams.set("runtimeKind", "observer");
    const socket = new WebSocket(url, {
      headers: { authorization: `Bearer ${parent.bearer}` },
    });
    const messages: unknown[] = [];
    socket.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));

    try {
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));
      const closedPromise = waitForSocketCloseDetails(socket);
      const revokedAt = Date.now();
      await request(`/auth/grants/${parent.grant.jti}/revoke`, {
        body: {},
        method: "POST",
      });

      await expect(closedPromise).resolves.toEqual({
        code: 1008,
        reason: "auth_grant_revoked",
      });
      expect(Date.now() - revokedAt).toBeLessThan(5_000);
      expect(pollingApp.debugInfo().authRevocation).toMatchObject({
        notificationCount: 0,
        pollCloseCount: 1,
        pollFailureCount: 0,
      });
    } finally {
      if (socket.readyState !== WebSocket.CLOSED) socket.terminate();
      await pollingApp.close();
    }
  });

  it("closes an admitted socket at parent grant expiry without late grace", async () => {
    const session = await createSession();
    const parent = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "observer",
        sessionScope: session.sessionId,
        subject: `part_expiry_socket_${randomUUID()}`,
        ttlSeconds: 2,
      },
      method: "POST",
    });
    const expiryApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      authRevocation: { listenEnabled: false, pollIntervalMs: 0 },
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await expiryApp.listen(0);
    const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
    url.searchParams.set("after", "0");
    url.searchParams.set("runtimeKind", "observer");
    const socket = new WebSocket(url, {
      headers: { authorization: `Bearer ${parent.bearer}` },
    });
    const messages: unknown[] = [];
    socket.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));

    try {
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));
      const closed = await waitForSocketCloseDetails(socket);
      const closedAt = Date.now();

      expect(closed).toEqual({ code: 1008, reason: "auth_grant_expired" });
      expect(closedAt).toBeGreaterThanOrEqual(new Date(parent.grant.expiresAt).getTime());
      expect(closedAt).toBeLessThan(new Date(parent.grant.expiresAt).getTime() + 500);
    } finally {
      if (socket.readyState !== WebSocket.CLOSED) socket.terminate();
      await expiryApp.close();
    }
  });

  it("reauthorizes established WebSocket commands against durable revocation", async () => {
    const session = await createSession();
    const participantId = `part_ws_grant_${randomUUID()}`;
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: {
        kind: "text",
        objective: "Remain unclaimed after grant revocation",
      },
      method: "POST",
    });
    const created = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    const delayedClosureApp = createAppServer(currentPool(), {
      auth: e2eAuthOptions,
      authRevocation: { listenEnabled: false, pollIntervalMs: 0 },
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 1_000,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const delayedClosurePort = await delayedClosureApp.listen(0);
    const url = new URL(
      `ws://127.0.0.1:${delayedClosurePort}/sessions/${session.sessionId}/stream`,
    );
    url.searchParams.set("access_token", created.bearer);
    url.searchParams.set("after", "0");
    url.searchParams.set("instanceId", `inst_ws_grant_${randomUUID()}`);
    url.searchParams.set("participantId", participantId);
    url.searchParams.set("runtimeKind", "codex");
    const socket = new WebSocket(url);
    const messages: unknown[] = [];
    socket.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));

    try {
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));

      const allowedEventId = `evt_ws_grant_allowed_${randomUUID()}`;
      const allowedRequestId = `req_ws_grant_allowed_${randomUUID()}`;
      socket.send(
        JSON.stringify({
          eventId: allowedEventId,
          op: webSocketOperation.publish,
          payload: { text: "authorized before revocation" },
          producerId: participantId,
          requestId: allowedRequestId,
          type: sessionEventType.userMessage,
        }),
      );
      await waitFor(() =>
        messages.some(
          (message) => isCommandResultEnvelope(message) && message.requestId === allowedRequestId,
        ),
      );

      await request(`/auth/grants/${created.grant.jti}/revoke`, {
        body: {},
        method: "POST",
      });
      const deniedEventId = `evt_ws_grant_denied_${randomUUID()}`;
      const deniedPublishRequestId = `req_ws_grant_denied_publish_${randomUUID()}`;
      const deniedTaskCommands = [
        webSocketOperation.taskClaim,
        webSocketOperation.taskCancel,
        webSocketOperation.taskRefresh,
        webSocketOperation.taskComplete,
        webSocketOperation.taskFail,
        webSocketOperation.taskRelease,
      ] as const;
      const deniedTaskRequests = deniedTaskCommands.map((op) => ({
        op,
        requestId: `req_ws_grant_denied_${op.replace(".", "_")}_${randomUUID()}`,
      }));
      socket.send(
        JSON.stringify({
          eventId: deniedEventId,
          op: webSocketOperation.publish,
          payload: { text: "must not persist after revocation" },
          producerId: participantId,
          requestId: deniedPublishRequestId,
          type: sessionEventType.userMessage,
        }),
      );
      for (const command of deniedTaskRequests) {
        socket.send(JSON.stringify({ ...command, taskId: task.task.taskId }));
      }
      await waitFor(
        () =>
          messages.some((message) =>
            isErrorEnvelopeWithReason(message, deniedPublishRequestId, "auth_grant_revoked"),
          ) &&
          deniedTaskRequests.every(({ op, requestId }) =>
            messages.some(
              (message) =>
                isErrorEnvelopeWithReason(message, requestId, "auth_grant_revoked") &&
                message.command === op,
            ),
          ),
      );

      const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
      const tasks = await request<TasksResponse>(`/sessions/${session.sessionId}/tasks`);
      expect(events.events.map((event) => event.eventId)).toContain(allowedEventId);
      expect(events.events.map((event) => event.eventId)).not.toContain(deniedEventId);
      expect(
        tasks.tasks.find((candidate) => candidate.taskId === task.task.taskId)?.claimedBy,
      ).toBe(null);
      const [, encodedPayload, encodedSignature] = created.bearer.split(".");
      const diagnostics = JSON.stringify(messages);
      expect(diagnostics).not.toContain(created.bearer);
      expect(diagnostics).not.toContain(encodedPayload);
      expect(diagnostics).not.toContain(encodedSignature);
    } finally {
      if (socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      await delayedClosureApp.close();
    }
  });

  it("does not positively cache established WebSocket grants and fails closed on store outage", async () => {
    const session = await createSession();
    const participantId = `part_ws_store_${randomUUID()}`;
    const created = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    let authReadCount = 0;
    let failAuthReads = false;
    const durableGrantStore = createAuthPersistenceStores(currentPool()).grants;
    const observedGrantStore = {
      findByJti: async (jti: string) => {
        authReadCount += 1;
        if (failAuthReads) throw new Error("injected auth store credential detail");
        return durableGrantStore.findByJti(jti);
      },
      list: durableGrantStore.list,
    };
    const authorityApp = createAppServer(currentPool(), {
      auth: { ...e2eAuthOptions, grantStore: observedGrantStore },
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      sessionService: {
        controlEpochEnforcement: false,
        taskClaimLeaseTtlMs: 200,
        wsControlLeaseTtlMs: 1_000,
      },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await authorityApp.listen(0);
    const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
    url.searchParams.set("access_token", created.bearer);
    url.searchParams.set("after", "0");
    url.searchParams.set("instanceId", `inst_ws_store_${randomUUID()}`);
    url.searchParams.set("participantId", participantId);
    url.searchParams.set("runtimeKind", "codex");
    const socket = new WebSocket(url);
    const messages: unknown[] = [];
    socket.on("message", (data) => messages.push(JSON.parse(String(data)) as unknown));

    try {
      await waitForSocketOpen(socket);
      await waitFor(() => messages.some(isReplayCompleteEnvelope));
      expect(authReadCount).toBe(1);

      const allowedEventId = `evt_ws_store_allowed_${randomUUID()}`;
      const allowedRequestId = `req_ws_store_allowed_${randomUUID()}`;
      socket.send(
        JSON.stringify({
          eventId: allowedEventId,
          op: webSocketOperation.publish,
          payload: { text: "requires a second grant read" },
          producerId: participantId,
          requestId: allowedRequestId,
          type: sessionEventType.userMessage,
        }),
      );
      await waitFor(() =>
        messages.some(
          (message) => isCommandResultEnvelope(message) && message.requestId === allowedRequestId,
        ),
      );
      expect(authReadCount).toBe(2);

      failAuthReads = true;
      const deniedEventId = `evt_ws_store_denied_${randomUUID()}`;
      const deniedRequestId = `req_ws_store_denied_${randomUUID()}`;
      socket.send(
        JSON.stringify({
          eventId: deniedEventId,
          op: webSocketOperation.publish,
          payload: { text: "must fail closed during outage" },
          producerId: participantId,
          requestId: deniedRequestId,
          type: sessionEventType.userMessage,
        }),
      );
      await waitFor(() =>
        messages.some((message) =>
          isErrorEnvelopeWithReason(message, deniedRequestId, "auth_store_unavailable"),
        ),
      );
      expect(authReadCount).toBe(3);

      const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);
      expect(events.events.map((event) => event.eventId)).toContain(allowedEventId);
      expect(events.events.map((event) => event.eventId)).not.toContain(deniedEventId);
    } finally {
      if (socket.readyState !== WebSocket.CLOSED) {
        socket.close();
        await waitForSocketClose(socket);
      }
      await authorityApp.close();
    }
  });

  it("absorbs an asynchronous upgrade rejection after the peer disconnects", async () => {
    const session = await createSession();
    const participantId = `part_ws_disconnect_${randomUUID()}`;
    const created = await request<AuthGrantCreateResponse>("/auth/grants", {
      body: {
        role: "participant",
        sessionScope: session.sessionId,
        subject: participantId,
        ttlSeconds: 3_600,
      },
      method: "POST",
    });
    let markAuthReadStarted = (): void => undefined;
    const authReadStarted = new Promise<void>((resolve) => {
      markAuthReadStarted = resolve;
    });
    let releaseAuthRead = (): void => undefined;
    const authReadRelease = new Promise<void>((resolve) => {
      releaseAuthRead = resolve;
    });
    const durableGrantStore = createAuthPersistenceStores(currentPool()).grants;
    const delayedFailureGrantStore = {
      findByJti: async (_jti: string) => {
        markAuthReadStarted();
        await authReadRelease;
        throw new Error("injected delayed auth read failure");
      },
      list: durableGrantStore.list,
    };
    const delayedFailureApp = createAppServer(currentPool(), {
      auth: { ...e2eAuthOptions, grantStore: delayedFailureGrantStore },
      eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
      taskClaimSweeper: { intervalMs: 0 },
    });
    const port = await delayedFailureApp.listen(0);
    const url = new URL(`ws://127.0.0.1:${port}/sessions/${session.sessionId}/stream`);
    url.searchParams.set("access_token", created.bearer);
    url.searchParams.set("participantId", participantId);
    const socket = new WebSocket(url);
    socket.on("error", () => undefined);
    const unhandledRejections: unknown[] = [];
    const observeUnhandled = (reason: unknown): void => {
      unhandledRejections.push(reason);
    };
    process.on("unhandledRejection", observeUnhandled);

    try {
      await authReadStarted;
      socket.terminate();
      releaseAuthRead();
      await sleep(50);
      expect(unhandledRejections).toEqual([]);
    } finally {
      process.off("unhandledRejection", observeUnhandled);
      releaseAuthRead();
      if (socket.readyState !== WebSocket.CLOSED) socket.terminate();
      await delayedFailureApp.close();
    }
  });

  it("authorizes grant lifecycle routes before revealing grant existence", async () => {
    const participant = mintE2eToken({
      participantId: "part_auth_denied",
      role: "participant",
      sessionId: "*",
    });
    const paths = [
      "/auth/grants/grant_known_only_to_admin",
      "/auth/grants/grant_missing/revoke",
    ] as const;
    for (const path of paths) {
      const response = await requestStatusFrom(baseUrl, path, {
        authToken: participant,
        ...(path.endsWith("/revoke") ? { body: {}, method: "POST" } : {}),
      });
      expect(response.status).toBe(403);
      expect(response.body).toEqual({ error: "Forbidden", reason: "role" });
      expect(response.text).not.toContain("grant_missing");
      expect(response.text).not.toContain("grant_known_only_to_admin");
    }
  });

  it("requires authentication before grant lifecycle routing", async () => {
    const response = await requestStatusFrom(baseUrl, "/auth/grants/grant_missing", {
      authToken: null,
    });
    expect(response.status).toBe(401);
    expect(response.body).toEqual({ error: "Unauthorized", reason: "missing" });
  });

  it("keeps provisional tgr2 issuance observably gated before persistence", async () => {
    const gatedApp = createAppServer(currentPool(), {
      auth: {
        activeKid: testAuthSigningKid,
        allowLegacyTokens: true,
        issuer: "https://auth.e2e.tether.local",
        mode: "required",
        secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      },
    });
    const port = await gatedApp.listen(0);
    onTestFinished(() => gatedApp.close());

    expect(gatedApp.debugInfo().auth.grantIssuanceEnabled).toBe(false);
    const subject = `admin_gated_${randomUUID()}`;
    const response = await requestStatusFrom(`http://127.0.0.1:${port}`, "/auth/grants", {
      authToken: mintE2eToken({
        participantId: "part_gate_admin",
        role: "admin",
        sessionId: "*",
      }),
      body: { role: "admin", sessionScope: "*", subject },
      method: "POST",
    });
    expect(response.status).toBe(503);
    expect(response.body).toEqual({
      error: "Authentication grant issuance unavailable",
      reason: "auth_grant_issuance_gated",
    });
    const persisted = await currentPool().pool.query<{
      readonly count: number;
    }>(`SELECT count(*)::int AS count FROM auth_grants WHERE subject = $1`, [subject]);
    expect(persisted.rows[0]?.count).toBe(0);
  });

  it("preserves explicit auth-disabled development access to the grant lifecycle", async () => {
    const disabledApp = createAppServer(currentPool(), {
      auth: {
        activeKid: testAuthSigningKid,
        issuer: "https://auth.e2e.tether.local",
        mode: "disabled",
        preEnforcementGrantIssuanceEnabled: true,
        secrets: { [testAuthSigningKid]: testAuthSigningSecret },
      },
    });
    const port = await disabledApp.listen(0);
    onTestFinished(() => disabledApp.close());

    const response = await requestStatusFrom<AuthGrantCreateResponse>(
      `http://127.0.0.1:${port}`,
      "/auth/grants",
      {
        authToken: null,
        body: {
          role: "admin",
          sessionScope: "*",
          subject: "bootstrap_disabled_mode",
        },
        method: "POST",
      },
    );
    expect(response.status).toBe(201);
    expect(response.body.bearer).toMatch(/^tgr2\./u);
  });

  it("bootstraps one audited administrator and writes its bearer once", async () => {
    const output: string[] = [];
    await runBootstrapAdminCli(
      ["--subject", "admin_bootstrap_e2e", "--ttl", "1h"],
      {
        AUTH_ISSUER: "https://auth.e2e.tether.local",
        AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED: "true",
        AUTH_SIGNING_KID: testAuthSigningKid,
        AUTH_SIGNING_SECRET: testAuthSigningSecret,
        DATABASE_URL: databaseUrl,
      },
      (value) => output.push(value),
    );

    expect(output).toHaveLength(1);
    expect(output[0]?.endsWith("\n")).toBe(true);
    const created = JSON.parse(output[0] ?? "") as AuthGrantCreateResponse;
    expect(created.bearer).toMatch(/^tgr2\./u);
    expect(created.grant).toMatchObject({
      role: "admin",
      sessionScope: "*",
      subject: "admin_bootstrap_e2e",
    });
    const stores = createAuthPersistenceStores(currentPool());
    await expect(stores.grants.findByJti(created.grant.jti)).resolves.toMatchObject({
      metadata: { source: "bootstrap" },
      subject: "admin_bootstrap_e2e",
    });
    const audits = await stores.audits.listForGrant(created.grant.jti, 10);
    expect(audits).toEqual([
      expect.objectContaining({
        action: "grant.created",
        reasonCode: "bootstrap",
      }),
    ]);
    expect(JSON.stringify(audits)).not.toContain(created.bearer);
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
    const port = await limitedApp.listen(0);
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
    const port = await disabledApp.listen(0);
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
    const port = await limitedApp.listen(0);
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

  it("keeps a live append when a stale projection backfill compare-and-set races it", async () => {
    const sessionId = `sess_projection_race_${randomUUID()}`;
    await createDbSession(currentPool(), sessionId);
    for (const seq of [1, 2, 3]) {
      await appendEvent(
        currentPool(),
        {
          eventId: `evt_projection_race_${randomUUID()}`,
          payload: { text: `prefix-${seq}` },
          producerId: "projection-race-e2e",
          sessionId,
          type: "client.observed",
        },
        { sourceId: "src_projection_race_e2e" },
      );
    }
    await currentPool().pool.query(`DELETE FROM session_projections WHERE session_id = $1`, [
      sessionId,
    ]);

    const rawClient = await currentPool().pool.connect();
    const pausedClient = new PausedProjectionBackfillClient(rawClient);
    try {
      const backfill = backfillSessionProjection(pausedClient, { batchSize: 2, sessionId });
      await pausedClient.candidateReady.promise;
      const live = await appendEvent(
        currentPool(),
        {
          eventId: `evt_projection_race_live_${randomUUID()}`,
          payload: { title: "Live append wins" },
          producerId: "projection-race-e2e",
          sessionId,
          type: "session.title",
        },
        { sourceId: "src_projection_race_e2e" },
      );
      pausedClient.resume();

      const result = await backfill;
      const stored = await currentPool().pool.query<{
        readonly coversSeqTo: string;
        readonly eventCount: string;
        readonly title: string | null;
      }>(
        `
          SELECT
            covers_seq_to AS "coversSeqTo",
            event_count AS "eventCount",
            title
          FROM session_projections
          WHERE session_id = $1
        `,
        [sessionId],
      );

      expect(live.seq).toBe(4);
      expect(result).toMatchObject({
        coversSeqTo: 2,
        currentCoversSeqTo: 4,
        outcome: "stale",
      });
      expect(stored.rows[0]).toEqual({
        coversSeqTo: "4",
        eventCount: "4",
        title: "Live append wins",
      });
    } finally {
      pausedClient.resume();
      rawClient.release();
    }
  });

  it("keeps inventory and permanent-delete eligibility correct above 10,000 events", async () => {
    const sessionId = `sess_projection_long_${randomUUID()}`;
    await createLongSessionFixture(currentPool(), sessionId);

    const backfilled = await backfillSessionProjection(currentPool().pool, {
      batchSize: 500,
      sessionId,
    });
    const firstInventory = await request<SessionListResponse>("/sessions");
    const firstSession = firstInventory.sessions.find(
      (candidate) => candidate.sessionId === sessionId,
    );
    const unchangedBackfill = await backfillSessionProjection(currentPool().pool, {
      batchSize: 500,
      sessionId,
    });
    const secondInventory = await request<SessionListResponse>("/sessions");
    const secondSession = secondInventory.sessions.find(
      (candidate) => candidate.sessionId === sessionId,
    );

    expect(backfilled).toMatchObject({
      coversSeqTo: 10_002,
      eventCount: 10_002,
      outcome: "written",
    });
    expect(firstSession).toMatchObject({
      archived: true,
      eventCount: 10_002,
      sessionId,
      title: "Title after ten thousand",
    });
    expect(unchangedBackfill).toMatchObject({
      coversSeqTo: 10_002,
      eventCount: 10_002,
      outcome: "unchanged",
    });
    expect(secondSession?.updatedAt).toBe(firstSession?.updatedAt);
    const verificationFixture = createProjectionVerificationFixture(sessionId);
    await expect(
      verifySessionProjection(currentPool().pool, verificationFixture.options),
    ).resolves.toMatchObject(verificationFixture.expected);

    const deleteAuthToken = mintE2eToken({
      participantId: "part_projection_long_delete",
      role: "admin",
      sessionId: "*",
    });
    const deleted = await requestStatus<PermanentDeleteResponse>(`/sessions/${sessionId}/delete`, {
      authToken: deleteAuthToken,
      method: "POST",
    });

    expect(deleted.status).toBe(200);
    expect(deleted.body).toEqual({ ok: true, sessionId });
  });

  projectionBenchmark(
    "meets the bounded online projection backfill performance gate",
    async () => {
      const batchSize = 10_000;
      const measurements: {
        readonly eventCount: number;
        readonly maxBatchQueryMs: number;
        readonly totalMs: number;
      }[] = [];
      let millionEventSessionId = "";
      for (const eventCount of [10_000, 100_000, 1_000_000]) {
        const sessionId = `sess_projection_benchmark_${eventCount}_${randomUUID()}`;
        millionEventSessionId = sessionId;
        await seedProjectionBenchmarkSession(sessionId, eventCount);
        const measured = new MeasuredProjectionBackfillClient(currentPool().pool);
        const startedAt = performance.now();
        const result = await backfillSessionProjection(measured, { batchSize, sessionId });
        const totalMs = performance.now() - startedAt;
        const verification = await verifySessionProjection(currentPool().pool, {
          batchSize,
          sessionId,
        });
        const maxBatchQueryMs = Math.max(...measured.queryDurationsMs);

        expect(result).toMatchObject({ coversSeqTo: eventCount, eventCount, outcome: "written" });
        expect(verification).toMatchObject({
          freshCoversSeqTo: eventCount,
          freshEventCount: eventCount,
          status: "current",
        });
        expect(maxBatchQueryMs).toBeLessThan(500);
        measurements.push({ eventCount, maxBatchQueryMs, totalMs });
      }

      const baselineSessionId = `sess_projection_benchmark_baseline_${randomUUID()}`;
      await createDbSession(currentPool(), baselineSessionId);
      const baselineLatencies = await measureProjectionAppendLatencies(baselineSessionId, 200);

      await currentPool().pool.query(`DELETE FROM session_projections WHERE session_id = $1`, [
        millionEventSessionId,
      ]);
      const racing = new MeasuredProjectionBackfillClient(currentPool().pool, 900_000);
      const backfill = backfillSessionProjection(racing, {
        batchSize,
        sessionId: millionEventSessionId,
      });
      await racing.candidateReady.promise;
      const activeAppendLatenciesPromise = measureProjectionAppendLatencies(
        millionEventSessionId,
        200,
      );
      racing.resume();
      const [raceResult, activeAppendLatencies] = await Promise.all([
        backfill,
        activeAppendLatenciesPromise,
      ]);
      const finalVerification = await verifySessionProjection(currentPool().pool, {
        batchSize,
        sessionId: millionEventSessionId,
      });
      const baselineP95Ms = percentile95(baselineLatencies);
      const activeP95Ms = percentile95(activeAppendLatencies);

      expect(raceResult.currentCoversSeqTo).toBeGreaterThanOrEqual(1_000_000);
      expect(finalVerification).toMatchObject({
        freshCoversSeqTo: 1_000_200,
        freshEventCount: 1_000_200,
        status: "current",
      });
      expect(Math.max(...racing.queryDurationsMs)).toBeLessThan(500);
      expect(activeP95Ms).toBeLessThanOrEqual(baselineP95Ms * 2);
      process.stdout.write(
        `${JSON.stringify({
          activeP95Ms,
          baselineP95Ms,
          measurements,
          raceOutcome: raceResult.outcome,
        })}\n`,
      );
    },
    180_000,
  );

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
    const port = await corsApp.listen(0);
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
    const port = await limitedApp.listen(0);
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
    const port = await limitedApp.listen(0);
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
    const port = await limitedApp.listen(0);
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
          limit: 1,
          op: "error",
          reason: "replay_window_exceeded",
        }),
      );
      const rawEvents = await requestFrom<EventsResponse>(
        limitedUrl,
        `/sessions/${session.sessionId}/events?after=0`,
      );
      expect(rawEvents.events).toHaveLength(2);
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
    const port = await observedApp.listen(0);
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
    const port = await disabledApp.listen(0);
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

    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: { instanceId: "inst_codex_e2e", participantId: "part_codex_e2e" },
        method: "POST",
      },
    );
    const completion = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          claimId: claimed.task.claimId,
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
          query: {
            class: "participant-advisory-lock",
            text: participantAdvisoryLockQuery,
          },
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
        expect.objectContaining({
          actor: "probe-a",
          name: "probe-ready",
          position: "before",
        }),
        expect.objectContaining({
          actor: "probe-b",
          name: "probe-ready",
          position: "before",
        }),
        expect.objectContaining({
          actor: "probe-a",
          name: "probe-finished",
          position: "after",
        }),
        expect.objectContaining({
          actor: "probe-b",
          name: "probe-finished",
          position: "after",
        }),
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
    expect(waiter?.lockWait).toMatchObject({
      blocked: true,
      waitEventType: "Lock",
    });
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
          const pidResult = await terminatedActor.query<{
            readonly pid: number;
          }>("SELECT pg_backend_pid()::int AS pid");
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
        expect.objectContaining({
          actor: "terminated-actor",
          operation: "rollback",
        }),
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
          query: {
            class: "current-control-lease-fence",
            text: currentControlLeaseFenceQuery,
          },
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
          mutationApp.listen(0),
          supersessionApp.listen(0),
        ]);
        const mutationBaseUrl = `http://127.0.0.1:${mutationPort}`;
        const supersessionBaseUrl = `http://127.0.0.1:${supersessionPort}`;

        try {
          const mutation = requestStatusFrom<TaskResponse>(
            mutationBaseUrl,
            `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim/refresh`,
            {
              body: { claimId: claimed.task.claimId, controlEpoch, instanceId, participantId },
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
    const currentEpochRows = await currentPool().pool.query<{
      readonly epoch: unknown;
    }>(
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

    expect(result.lockWait).toMatchObject({
      blocked: true,
      waitEventType: "Lock",
    });
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
          query: {
            class: "current-control-lease-fence",
            text: currentControlLeaseFenceQuery,
          },
          release: "manual",
        },
      ],
      transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
    });

    const result = await coordinator.run(async ({ databaseFor, releasePhase, waitForPhase }) => {
      const mutationApp = createControlEpochRaceApp(databaseFor("mutation"), 120_000);
      const supersessionApp = createControlEpochRaceApp(databaseFor("supersession"), 60_000);
      const [mutationPort, supersessionPort] = await Promise.all([
        mutationApp.listen(0),
        supersessionApp.listen(0),
      ]);
      const mutationBaseUrl = `http://127.0.0.1:${mutationPort}`;
      const supersessionBaseUrl = `http://127.0.0.1:${supersessionPort}`;

      try {
        const mutation = requestStatusFrom<TaskResponse>(
          mutationBaseUrl,
          `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim/refresh`,
          {
            body: { claimId: claimed.task.claimId, controlEpoch, instanceId, participantId },
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
    const currentEpochRows = await currentPool().pool.query<{
      readonly epoch: unknown;
    }>(
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
      body: {
        kind: "software_dev",
        objective: "Claim this task exactly once",
      },
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
      let claimantAStarted = false;
      let claimantBStarted = false;
      try {
        const claimantAPort = await claimantAApp.listen(0);
        claimantAStarted = true;
        const claimantBPort = await claimantBApp.listen(0);
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
          query: {
            class: "event-sequence-allocation",
            text: eventSequenceAllocatorQuery,
          },
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

    expect(result.lockWait).toMatchObject({
      blocked: true,
      waitEventType: "Lock",
    });
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
          query: {
            class: "event-sequence-allocation",
            text: eventSequenceAllocatorQuery,
          },
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

    expect(result.lockWait).toMatchObject({
      blocked: true,
      waitEventType: "Lock",
    });
    expect(result.eventA).toMatchObject({
      reason: expect.objectContaining({
        message: "injected session event insert failure",
      }),
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
        body: { ...controller, claimId: completed.task.claimId, result: { summary: "done" } },
        method: "POST",
      },
    );
    const failed = await createClaimedTask(sessionId, "fail this task", controller);
    const failure = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${failed.task.taskId}/fail`,
      {
        body: { ...controller, claimId: failed.task.claimId, failure: { reason: "expected" } },
        method: "POST",
      },
    );
    const released = await createClaimedTask(sessionId, "release this task", controller);
    const release = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${released.task.taskId}/release`,
      {
        body: { ...controller, claimId: released.task.claimId },
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
          body: { ...controller, claimId: claimed.task.claimId },
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
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${taskId}/claim`,
      {
        body: { instanceId: "inst_retryable", participantId: "part_retryable" },
        method: "POST",
      },
    );
    const failingDatabase = await createEventInsertFailingDatabase(currentPool());

    await expect(
      completeTaskWithEvent(failingDatabase, {
        claimId: requireClaimId(claimed.task),
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
          claimId: claimed.task.claimId,
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
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: controller,
        method: "POST",
      },
    );

    const [completeResult, cancelResult] = await Promise.allSettled([
      request<TaskResponse>(`/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`, {
        body: {
          ...controller,
          claimId: claimed.task.claimId,
          result: { summary: "completed first" },
        },
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
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_generic_agent_e2e",
          participantId: "part_generic_agent_e2e",
        },
        method: "POST",
      },
    );
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          claimId: claimed.task.claimId,
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
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_generic_agent_e2e",
          participantId: "part_generic_agent_e2e",
        },
        method: "POST",
      },
    );
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          claimId: claimed.task.claimId,
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

  it("atomically binds manifest targets and returns the canonical duplicate decision", async () => {
    const session = await createSession();
    const target = {
      action: "action_opaque_1",
      digest: "digest_opaque_1",
      scopeKey: "scope_opaque_1",
      targetId: "target_opaque_1",
      targetKind: "kind_opaque_1",
      targetRevision: "revision_opaque_1",
    };
    const task = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "opaque_manifest_review", objective: "review opaque targets" },
      method: "POST",
    });
    const controller = {
      instanceId: "inst_manifest_e2e",
      participantId: "part_manifest_e2e",
    };
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      { body: controller, method: "POST" },
    );
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          ...controller,
          claimId: requireClaimId(claimed.task),
          result: { targetManifest: [target] },
        },
        method: "POST",
      },
    );
    const targetless = await requestStatus(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: { decision: "approved", participantId: "operator_missing_target" },
        method: "POST",
      },
    );
    const first = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: { decision: "approved", participantId: "operator_first", target },
        method: "POST",
      },
    );
    const identical = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: { decision: "approved", participantId: "operator_second", target },
        method: "POST",
      },
    );
    const contradictory = await request<TaskApprovalResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/approval`,
      {
        body: { decision: "rejected", participantId: "operator_third", target },
        method: "POST",
      },
    );
    const events = await request<EventsResponse>(`/sessions/${session.sessionId}/events?after=0`);

    expect(targetless).toMatchObject({
      body: { rejectionReason: "target_required" },
      status: 409,
    });
    expect(first).toMatchObject({
      approval: {
        decidedByParticipantId: "operator_first",
        decision: "approved",
        targetKey: expect.stringMatching(/^approvalTarget:v2:sha256:[0-9a-f]{64}$/),
      },
      status: "recorded",
    });
    expect(identical).toMatchObject({ status: "ignored" });
    expect(contradictory).toMatchObject({
      decision: "rejected",
      existingDecision: "approved",
      status: "ignored",
    });
    expect(identical.approval).toEqual(first.approval);
    expect(contradictory.approval).toEqual(first.approval);
    expect(identical.task).toEqual(first.task);
    expect(contradictory.task).toEqual(first.task);
    const approvalEvents = events.events.filter((event) => event.type === "approval.recorded");
    expect(approvalEvents).toHaveLength(1);
    expect(approvalEvents[0]?.payload).toMatchObject({ approval: first.approval });
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
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_approval_target_e2e",
          participantId: "part_approval_target_e2e",
        },
        method: "POST",
      },
    );
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/complete`,
      {
        body: {
          claimId: claimed.task.claimId,
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
    const unsupportedClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${unsupportedTask.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_software_invalid_e2e",
          participantId: "part_software_invalid_e2e",
        },
        method: "POST",
      },
    );
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${unsupportedTask.task.taskId}/complete`,
      {
        body: {
          claimId: unsupportedClaimed.task.claimId,
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
    const invalidPlanClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${invalidPlanTask.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_email_invalid_e2e",
          participantId: "part_email_invalid_e2e",
        },
        method: "POST",
      },
    );
    await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${invalidPlanTask.task.taskId}/complete`,
      {
        body: {
          claimId: invalidPlanClaimed.task.claimId,
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
    const port = await unvalidatedApp.listen(0);
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
      const claimed = await requestFrom<TaskResponse>(
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
            claimId: claimed.task.claimId,
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

    const filterClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_codex_filter_e2e",
          participantId: "part_codex_filter_e2e",
        },
        method: "POST",
      },
    );
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/complete`, {
      body: {
        claimId: filterClaimed.task.claimId,
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
    const terminalClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${terminalTask.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_context_generic_e2e",
          participantId: "part_context_generic_e2e",
        },
        method: "POST",
      },
    );
    await request(`/sessions/${session.sessionId}/tasks/${terminalTask.task.taskId}/complete`, {
      body: {
        claimId: terminalClaimed.task.claimId,
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
      mode: "raw_only",
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

  it("runs the external worker through fake Ollama and publishes summary-backed context", async () => {
    const fixture = await createSummaryWorkerJobFixture("success");
    const executor = createSessionSummaryExecutor({
      evaluatedSelection: enabledSummaryWorkerSelection(),
      maxExecutionMs: 60_000,
      ollama: new OllamaClient({
        baseUrl: "http://fake-ollama.test",
        fetch: createFakeOllamaFetch({
          facts: [],
          headline: "Integrated worker summary",
          narrative: "The exact reserved range was summarized.",
          openQuestions: [],
        }),
        maxAttempts: 1,
        requestTimeoutMs: 1_000,
        retryBackoffMs: 1,
      }),
      tether: new TetherApiClient({
        authToken: fixture.authToken,
        baseUrl,
        pageSize: 100,
        requestTimeoutMs: 2_000,
      }),
    });

    const result = await executor(fixture.context);
    await backfillSessionProjection(currentPool().pool, {
      batchSize: 100,
      sessionId: fixture.job.sessionId,
    });
    const context = await request<SessionContextResponse>(
      `/sessions/${fixture.job.sessionId}/context?budgetTokens=8000`,
    );
    const exact = await request<EventsResponse>(
      `/sessions/${fixture.job.sessionId}/events?after=0&limit=100`,
    );
    const exactSuffix = exact.events.filter((event) => event.seq > fixture.job.range.to);

    expect(result.result).toEqual({ status: "candidate_submitted" });
    expect(context.context).toMatchObject({
      latestSummary: {
        content: { headline: "Integrated worker summary" },
        coversSeqFrom: fixture.job.range.from,
        coversSeqTo: fixture.job.range.to,
        summaryId: fixture.job.summaryId,
      },
      mode: "summary_with_raw_tail",
    });
    expect(context.context.recentEvents.map((event) => event.seq)).toEqual(
      exactSuffix.map((event) => event.seq),
    );
    expect(exactSuffix.some((event) => event.payload.text === "exact raw tail")).toBe(true);
  });

  it("keeps raw-only context and exact replay unchanged when fake Ollama fails", async () => {
    const fixture = await createSummaryWorkerJobFixture("failure");
    const before = await request<EventsResponse>(
      `/sessions/${fixture.job.sessionId}/events?after=0&limit=100`,
    );
    const executor = createSessionSummaryExecutor({
      evaluatedSelection: enabledSummaryWorkerSelection(),
      maxExecutionMs: 60_000,
      ollama: new OllamaClient({
        baseUrl: "http://fake-ollama.test",
        fetch: async () => {
          throw new Error("fake Ollama unavailable");
        },
        maxAttempts: 1,
        requestTimeoutMs: 100,
        retryBackoffMs: 1,
      }),
      tether: new TetherApiClient({
        authToken: fixture.authToken,
        baseUrl,
        pageSize: 100,
        requestTimeoutMs: 2_000,
      }),
    });

    await expect(executor(fixture.context)).rejects.toMatchObject({
      code: "generation_unavailable",
    });
    await backfillSessionProjection(currentPool().pool, {
      batchSize: 100,
      sessionId: fixture.job.sessionId,
    });
    const context = await request<SessionContextResponse>(
      `/sessions/${fixture.job.sessionId}/context?budgetTokens=8000`,
    );
    const after = await request<EventsResponse>(
      `/sessions/${fixture.job.sessionId}/events?after=0&limit=100`,
    );

    expect(context.context).toMatchObject({ latestSummary: null, mode: "raw_only" });
    expect(after.events).toEqual(before.events);
  });

  it("builds context from the active budget-class summary plus its exact raw suffix", async () => {
    const session = await createSession();
    for (const text of ["covered one", "covered two", "exact tail"]) {
      await request(`/sessions/${session.sessionId}/events`, {
        body: { payload: { text }, producerId: "external-client", type: "user.message" },
        method: "POST",
      });
    }
    const eventRows = await currentPool().pool.query<{
      readonly eventId: string;
      readonly seq: string;
    }>(
      `SELECT event_id AS "eventId", seq::text FROM session_events WHERE session_id = $1 ORDER BY seq`,
      [session.sessionId],
    );
    const covered = eventRows.rows.slice(0, -1);
    const suffix = eventRows.rows.at(-1);
    if (covered.length === 0 || suffix === undefined) {
      throw new Error("Expected covered events and one raw suffix event");
    }
    const activeSummaryId = `summary_context_active_${randomUUID()}`;
    await insertPublishedContextSummary({
      budgetClass: "8k",
      coversSeqFrom: Number(covered[0]?.seq),
      coversSeqTo: Number(covered.at(-1)?.seq),
      headline: "Superseded 8k summary",
      sessionId: session.sessionId,
      sourceEventCount: covered.length,
      sourceFirstEventId: covered[0]?.eventId ?? "missing",
      sourceLastEventId: covered.at(-1)?.eventId ?? "missing",
      summaryId: `summary_context_superseded_${randomUUID()}`,
      superseded: true,
    });
    await insertPublishedContextSummary({
      budgetClass: "8k",
      coversSeqFrom: Number(covered[0]?.seq),
      coversSeqTo: Number(covered.at(-1)?.seq),
      headline: "Selected 8k summary",
      sessionId: session.sessionId,
      sourceEventCount: covered.length,
      sourceFirstEventId: covered[0]?.eventId ?? "missing",
      sourceLastEventId: covered.at(-1)?.eventId ?? "missing",
      summaryId: activeSummaryId,
    });
    await insertPublishedContextSummary({
      budgetClass: "16k",
      coversSeqFrom: Number(covered[0]?.seq),
      coversSeqTo: Number(covered.at(-1)?.seq),
      headline: "Wrong budget summary",
      sessionId: session.sessionId,
      sourceEventCount: covered.length,
      sourceFirstEventId: covered[0]?.eventId ?? "missing",
      sourceLastEventId: covered.at(-1)?.eventId ?? "missing",
      summaryId: `summary_context_wrong_budget_${randomUUID()}`,
    });

    const context = await request<SessionContextResponse>(
      `/sessions/${session.sessionId}/context?budgetTokens=8000`,
    );
    const exactEvents = await request<EventsResponse>(`/sessions/${session.sessionId}/events`);

    expect(context.context).toMatchObject({
      budget: { omittedEventCount: 0, requestedTokens: 8_000 },
      latestSummary: {
        budgetClass: "8k",
        content: { headline: "Selected 8k summary" },
        coversSeqFrom: Number(covered[0]?.seq),
        coversSeqTo: Number(covered.at(-1)?.seq),
        summaryId: activeSummaryId,
      },
      mode: "summary_with_raw_tail",
      recentEventRange: { startSeq: Number(suffix.seq), endSeq: Number(suffix.seq) },
    });
    expect(context.context.recentEvents.map((event) => event.seq)).toEqual([Number(suffix.seq)]);
    expect(exactEvents.events.map((event) => event.seq)).toEqual(
      eventRows.rows.map((event) => Number(event.seq)),
    );
    expect(exactEvents.events.some((event) => event.type.includes("summary"))).toBe(false);
  });

  it("reads only the bounded newest context tail while retaining exact omitted accounting", async () => {
    const session = await createSession();
    const insertedCount = 10_001;
    await currentPool().pool.query(
      `
        INSERT INTO session_events (event_id, payload, producer_id, seq, session_id, type)
        SELECT $1 || generate_series::text, '{}'::jsonb, 'context-tail-e2e',
          generate_series + 1, $2, 'context.tail'
        FROM generate_series(1, $3)
      `,
      [`evt_context_tail_${randomUUID()}_`, session.sessionId, insertedCount],
    );
    await currentPool().pool.query(
      `
        UPDATE session_projections
        SET covers_seq_to = $2, event_count = $2
        WHERE session_id = $1
      `,
      [session.sessionId, insertedCount + 1],
    );

    const suffix = await listContextEventSuffix(currentPool(), session.sessionId, 0, 10_000);

    expect(suffix).toMatchObject({
      eligibleEventCount: insertedCount + 1,
      truncated: true,
    });
    expect(suffix.events).toHaveLength(10_000);
    expect(suffix.events[0]?.seq).toBe(3);
    expect(suffix.events.at(-1)?.seq).toBe(insertedCount + 1);
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

    const releasedClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${releasedTask.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_task_snapshot",
          participantId: "part_task_snapshot",
        },
        method: "POST",
      },
    );
    snapshots = await request<TaskSnapshotsResponse>(`/sessions/${session.sessionId}/debug/tasks`);
    const activeSnapshot = snapshots.tasks.find((task) => task.taskId === releasedTask.task.taskId);
    expect(activeSnapshot?.status).toBe("claim_active");

    await request(`/sessions/${session.sessionId}/tasks/${releasedTask.task.taskId}/release`, {
      body: {
        claimId: releasedClaimed.task.claimId,
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
    const completedClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_task_snapshot",
          participantId: "part_task_snapshot",
        },
        method: "POST",
      },
    );
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/complete`, {
      body: {
        claimId: completedClaimed.task.claimId,
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
    const activeClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${activeTask.task.taskId}/claim`,
      {
        body: { instanceId: "inst_summary", participantId: "part_summary" },
        method: "POST",
      },
    );
    const completedTask = await request<TaskResponse>(`/sessions/${session.sessionId}/tasks`, {
      body: { kind: "software_dev", objective: "Complete this summary task" },
      method: "POST",
    });
    const summaryCompletedClaimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/claim`,
      {
        body: { instanceId: "inst_summary", participantId: "part_summary" },
        method: "POST",
      },
    );
    await request(`/sessions/${session.sessionId}/tasks/${completedTask.task.taskId}/complete`, {
      body: {
        claimId: summaryCompletedClaimed.task.claimId,
        instanceId: "inst_summary",
        participantId: "part_summary",
        result: { summary: "complete" },
      },
      method: "POST",
    });
    await request(`/sessions/${session.sessionId}/tasks/${activeTask.task.taskId}/claim/refresh`, {
      body: {
        claimId: activeClaimed.task.claimId,
        instanceId: "inst_summary",
        participantId: "part_summary",
      },
      method: "POST",
    });
    const eventsBeforeDebug = await request<EventsResponse>(
      `/sessions/${session.sessionId}/events?after=0`,
    );
    const summary = await request<SessionDebugSummaryResponse>(
      `/sessions/${session.sessionId}/debug/summary`,
    );
    const scalability = await request<SessionScalabilityDebugResponse>(
      `/sessions/${session.sessionId}/debug/scalability`,
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
    expect(scalability.scalability).toMatchObject({
      context: { rawOnlyCount: expect.any(Number), summaryBackedCount: expect.any(Number) },
      projection: {
        activeReducerVersion: 1,
        coverage: { coversSeqTo: eventsBeforeDebug.events.length },
        current: true,
      },
      summary: {
        activeCandidate: null,
        publicationEnabled: false,
        rejectionCode: null,
        retentionEnabled: false,
      },
      worker: { ollamaStatus: "disabled", status: "disabled" },
    });
    expect(JSON.stringify(scalability)).not.toContain("Remain active");
    expect(JSON.stringify(scalability)).not.toContain("complete");
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
          claimId: firstClaim.task.claimId,
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

  // Milestone M4 (RFC D-008/D-009): a claim attempt on an ELAPSED claim atomically
  // expires and re-claims the task in one transaction, so the task never waits for
  // the background sweeper. These tests run on an isolated database with NO
  // background sweeper, so every reclaim is exercised deterministically.
  describe("atomic on-demand reclaim", () => {
    const reclaimDatabaseName = `tether_e2e_reclaim_${randomUUID().replaceAll("-", "_")}`;
    let reclaimPool: DatabasePool | null = null;

    beforeAll(async () => {
      await createDatabase(reclaimDatabaseName);
      reclaimPool = createPool(buildDatabaseUrl(reclaimDatabaseName));
      await migrate(reclaimPool);
    }, 30_000);

    afterAll(async () => {
      await reclaimPool?.end();
      await dropDatabase(reclaimDatabaseName);
    }, 30_000);

    /** Returns the isolated, sweeper-free reclaim database pool. */
    function pool(): DatabasePool {
      if (reclaimPool === null) {
        throw new Error("Reclaim database pool is not initialized");
      }
      return reclaimPool;
    }

    /** Forces one live claim to elapse relative to the database clock. */
    async function elapseClaim(sessionId: string, taskId: string): Promise<void> {
      await pool().pool.query(
        `
          UPDATE tasks
          SET claim_expires_at = now() - interval '1 second'
          WHERE session_id = $1 AND task_id = $2
        `,
        [sessionId, taskId],
      );
    }

    /** Counts committed events of one type for a single task. */
    async function countTaskEvents(
      sessionId: string,
      taskId: string,
      type: SessionEvent["type"],
    ): Promise<number> {
      const events = await listEvents(pool(), sessionId, 0);
      return events.filter(
        (event) => event.type === type && taskIdFromEventPayload(event) === taskId,
      ).length;
    }

    /** Seeds a task with an initial claim and then elapses that claim's lease. */
    async function prepareElapsedClaim(suffix: string): Promise<{
      readonly previousClaimId: string;
      readonly previousClaimedBy: string;
      readonly sessionId: string;
      readonly taskId: string;
    }> {
      const sessionId = `sess_reclaim_${suffix}_${randomUUID()}`;
      const taskId = `task_reclaim_${suffix}_${randomUUID()}`;
      const previousClaimedBy = `part_prev_${suffix}`;
      await createDbSession(pool(), sessionId);
      await createTaskWithEvent(pool(), {
        eventSourceId: "src_reclaim_setup_e2e",
        kind: "software_dev",
        objective: "Reclaim this elapsed claim",
        sessionId,
        taskId,
      });
      const firstClaim = await claimTaskWithEvent(pool(), {
        claimLeaseTtlMs: 60_000,
        eventSourceId: "src_reclaim_setup_e2e",
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      const previousClaimId = firstClaim?.task.claimId ?? null;
      if (firstClaim === null || previousClaimId === null) {
        throw new Error("Failed to seed the initial claim");
      }
      await elapseClaim(sessionId, taskId);
      return { previousClaimId, previousClaimedBy, sessionId, taskId };
    }

    it("atomically expires and re-claims an elapsed claim without the sweeper", async () => {
      const { previousClaimId, previousClaimedBy, sessionId, taskId } =
        await prepareElapsedClaim("solo");
      const reclaimer = "part_reclaimer_solo";

      const result = await claimTaskWithEvent(pool(), {
        claimLeaseTtlMs: 60_000,
        eventSourceId: "src_reclaim_solo_e2e",
        participantId: reclaimer,
        sessionId,
        taskId,
      });

      expect(result).not.toBeNull();
      if (result === null) {
        throw new Error("Reclaim returned null");
      }
      // The reclaim commits an ordered pair: claim_expired (old owner) then
      // claimed (new owner), with a strictly lower sequence for the expiry.
      expect(result.events.map((event) => event.type)).toEqual([
        "task.claim_expired",
        "task.claimed",
      ]);
      const [expiredEvent, claimedEvent] = result.events;
      expect(expiredEvent?.seq ?? 0).toBeLessThan(claimedEvent?.seq ?? 0);
      expect(expiredEvent?.payload.previousClaimedBy).toBe(previousClaimedBy);
      // A fresh server-issued Claim ID replaces the elapsed one.
      expect(result.task.claimId).not.toBe(previousClaimId);
      expect(result.task.claimId).not.toBeNull();
      expect(result.task.claimedBy).toBe(reclaimer);
      // No sweeper ran, yet the durable event log carries exactly one expiry and
      // the two claims (initial plus reclaim).
      expect(await countTaskEvents(sessionId, taskId, "task.claim_expired")).toBe(1);
      expect(await countTaskEvents(sessionId, taskId, "task.claimed")).toBe(2);
    });

    it("lets exactly one of two racing claimants reclaim an elapsed task", async () => {
      const { sessionId, taskId } = await prepareElapsedClaim("race");
      const coordinator = createPostgresConcurrencyCoordinator(pool(), {
        actors: ["winner", "loser"],
        barrierTimeoutMs: 5_000,
        phases: [
          {
            // The winner is held immediately after it has locked the task row via
            // FOR UPDATE, so the loser must block on the same row. This boundary
            // runs exactly once per claim, unlike the event-sequence allocator that
            // a reclaim visits twice.
            actors: ["winner"],
            name: "winner-holds-task-lock",
            position: "after",
            query: { class: "task-claim-lock", text: taskClaimLockQuery },
            release: "manual",
          },
        ],
        transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
      });

      const { loserResult, lockWait, winnerResult } = await coordinator.run(
        async ({ databaseFor, releasePhase, waitForLockWait, waitForPhase }) => {
          const winner = claimTaskWithEvent(databaseFor("winner"), {
            claimLeaseTtlMs: 60_000,
            eventSourceId: "src_reclaim_winner_e2e",
            participantId: "part_winner",
            sessionId,
            taskId,
          });
          await waitForPhase("winner-holds-task-lock");
          const loser = claimTaskWithEvent(databaseFor("loser"), {
            claimLeaseTtlMs: 60_000,
            eventSourceId: "src_reclaim_loser_e2e",
            participantId: "part_loser",
            sessionId,
            taskId,
          });
          const lockWait = await waitForLockWait("loser");
          releasePhase("winner-holds-task-lock");
          const [winnerResult, loserResult] = await Promise.all([winner, loser]);
          return { loserResult, lockWait, winnerResult };
        },
      );

      // The loser blocked on the task row FOR UPDATE the winner held.
      expect(lockWait).toMatchObject({ blocked: true, waitEventType: "Lock" });
      // Exactly one claimant won and performed the atomic reclaim.
      expect(winnerResult).not.toBeNull();
      expect(winnerResult?.events.map((event) => event.type)).toEqual([
        "task.claim_expired",
        "task.claimed",
      ]);
      expect(winnerResult?.task.claimedBy).toBe("part_winner");
      // The loser observed the fresh live claim and did NOT double-expire it.
      expect(loserResult).toBeNull();
      expect(await countTaskEvents(sessionId, taskId, "task.claim_expired")).toBe(1);
      expect(await countTaskEvents(sessionId, taskId, "task.claimed")).toBe(2);
      const durable = await getTask(pool(), { sessionId, taskId });
      expect(durable?.claimedBy).toBe("part_winner");
      expect(durable?.claimId).toBe(winnerResult?.task.claimId);
    });

    it("skips a reclaiming claimant's locked task during a concurrent sweep", async () => {
      const { sessionId, taskId } = await prepareElapsedClaim("sweeper");
      const coordinator = createPostgresConcurrencyCoordinator(pool(), {
        actors: ["claimant"],
        barrierTimeoutMs: 5_000,
        phases: [
          {
            // Hold the claimant right after it locks the task row (once per claim).
            actors: ["claimant"],
            name: "claimant-holds-task-lock",
            position: "after",
            query: { class: "task-claim-lock", text: taskClaimLockQuery },
            release: "manual",
          },
        ],
        transactionTimeouts: { lockTimeoutMs: 5_000, statementTimeoutMs: 10_000 },
      });

      const { claimResult, sweepEvents } = await coordinator.run(
        async ({ databaseFor, releasePhase, waitForPhase }) => {
          const claim = claimTaskWithEvent(databaseFor("claimant"), {
            claimLeaseTtlMs: 60_000,
            eventSourceId: "src_reclaim_claimant_e2e",
            participantId: "part_claimant",
            sessionId,
            taskId,
          });
          await waitForPhase("claimant-holds-task-lock");
          // The sweeper runs while the claimant holds the task row lock. Its
          // `FOR UPDATE SKIP LOCKED` skips the locked row rather than deadlocking.
          const sweepEvents = await expireTaskClaims(pool(), {
            batchSize: 10,
            sourceId: "src_reclaim_sweep_e2e",
          });
          releasePhase("claimant-holds-task-lock");
          const claimResult = await claim;
          return { claimResult, sweepEvents };
        },
      );

      // The sweeper skipped the locked task, so it emitted no expiry for it.
      expect(sweepEvents.filter((event) => taskIdFromEventPayload(event) === taskId)).toHaveLength(
        0,
      );
      // The claimant completed the atomic reclaim after the sweep finished.
      expect(claimResult?.events.map((event) => event.type)).toEqual([
        "task.claim_expired",
        "task.claimed",
      ]);
      expect(claimResult?.task.claimedBy).toBe("part_claimant");
      // No duplicate expiration: exactly one claim_expired for the task.
      expect(await countTaskEvents(sessionId, taskId, "task.claim_expired")).toBe(1);
    });

    it("fences stale claim generations after an atomic reclaim", async () => {
      const { previousClaimId, previousClaimedBy, sessionId, taskId } =
        await prepareElapsedClaim("fence");

      const reclaim = await claimTaskWithEvent(pool(), {
        claimLeaseTtlMs: 60_000,
        eventSourceId: "src_reclaim_fence_e2e",
        participantId: "part_new_owner",
        sessionId,
        taskId,
      });
      const newClaimId = reclaim?.task.claimId ?? null;
      expect(newClaimId).not.toBeNull();
      expect(newClaimId).not.toBe(previousClaimId);

      const eventCountBefore = (await listEvents(pool(), sessionId, 0)).length;
      // The stale generation (old owner + old Claim ID) cannot refresh, complete,
      // fail, or release the replacement claim.
      const refreshed = await refreshTaskClaim(pool(), {
        claimId: previousClaimId,
        claimLeaseTtlMs: 60_000,
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      expect(refreshed).toBeNull();
      const completed = await completeTaskWithEvent(pool(), {
        claimId: previousClaimId,
        eventSourceId: "src_reclaim_fence_e2e",
        participantId: previousClaimedBy,
        result: { summary: "stale complete" },
        sessionId,
        taskId,
      });
      expect(completed).toBeNull();
      const failed = await failTaskWithEvent(pool(), {
        claimId: previousClaimId,
        eventSourceId: "src_reclaim_fence_e2e",
        failure: { reason: "stale fail" },
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      expect(failed).toBeNull();
      const released = await releaseTaskWithEvent(pool(), {
        claimId: previousClaimId,
        eventSourceId: "src_reclaim_fence_e2e",
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      expect(released).toBeNull();

      // None of the stale mutations appended an event or displaced the owner.
      expect((await listEvents(pool(), sessionId, 0)).length).toBe(eventCountBefore);
      const durable = await getTask(pool(), { sessionId, taskId });
      expect(durable?.claimId).toBe(newClaimId);
      expect(durable?.claimedBy).toBe("part_new_owner");
      expect(durable?.completedAt).toBeNull();
      expect(durable?.failedAt).toBeNull();
    });

    it("fences a same-participant stale Claim ID after it reclaims its own elapsed task", async () => {
      const { previousClaimId, previousClaimedBy, sessionId, taskId } =
        await prepareElapsedClaim("self-fence");

      // The SAME participant reclaims its own elapsed task, minting a new Claim
      // ID. Because claimed_by is unchanged, only the Claim ID predicate can
      // reject the stale generation below.
      const reclaim = await claimTaskWithEvent(pool(), {
        claimLeaseTtlMs: 60_000,
        eventSourceId: "src_reclaim_self_fence_e2e",
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      const newClaimId = reclaim?.task.claimId ?? null;
      expect(newClaimId).not.toBeNull();
      expect(newClaimId).not.toBe(previousClaimId);
      expect(reclaim?.task.claimedBy).toBe(previousClaimedBy);

      const eventCountBefore = (await listEvents(pool(), sessionId, 0)).length;
      // Same participant, same still-live lease: the old Claim ID is the only
      // failing predicate, so these prove the Claim ID fence in isolation.
      const refreshed = await refreshTaskClaim(pool(), {
        claimId: previousClaimId,
        claimLeaseTtlMs: 60_000,
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      expect(refreshed).toBeNull();
      const completed = await completeTaskWithEvent(pool(), {
        claimId: previousClaimId,
        eventSourceId: "src_reclaim_self_fence_e2e",
        participantId: previousClaimedBy,
        result: { summary: "stale self complete" },
        sessionId,
        taskId,
      });
      expect(completed).toBeNull();
      const failed = await failTaskWithEvent(pool(), {
        claimId: previousClaimId,
        eventSourceId: "src_reclaim_self_fence_e2e",
        failure: { reason: "stale self fail" },
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      expect(failed).toBeNull();
      const released = await releaseTaskWithEvent(pool(), {
        claimId: previousClaimId,
        eventSourceId: "src_reclaim_self_fence_e2e",
        participantId: previousClaimedBy,
        sessionId,
        taskId,
      });
      expect(released).toBeNull();

      expect((await listEvents(pool(), sessionId, 0)).length).toBe(eventCountBefore);
      const durable = await getTask(pool(), { sessionId, taskId });
      expect(durable?.claimId).toBe(newClaimId);
      expect(durable?.claimedBy).toBe(previousClaimedBy);
      expect(durable?.completedAt).toBeNull();
      expect(durable?.failedAt).toBeNull();
      expect(durable?.releasedAt).toBeNull();
    });

    it("keeps a legacy claim without a Claim ID immutable until it is reclaimed", async () => {
      const sessionId = `sess_reclaim_legacy_${randomUUID()}`;
      const taskId = `task_reclaim_legacy_${randomUUID()}`;
      const legacyOwner = "part_legacy_owner";
      await createDbSession(pool(), sessionId);
      await createTaskWithEvent(pool(), {
        eventSourceId: "src_reclaim_legacy_e2e",
        kind: "software_dev",
        objective: "Legacy claim without a Claim ID",
        sessionId,
        taskId,
      });
      await claimTaskWithEvent(pool(), {
        claimLeaseTtlMs: 60_000,
        eventSourceId: "src_reclaim_legacy_e2e",
        participantId: legacyOwner,
        sessionId,
        taskId,
      });
      // Simulate a pre-Claim-ID migration row: an active claim whose claim_id is
      // NULL and whose lease has not yet elapsed.
      await pool().pool.query(
        `UPDATE tasks SET claim_id = NULL WHERE session_id = $1 AND task_id = $2`,
        [sessionId, taskId],
      );

      // Claim-owned mutations require the exact current Claim ID, so a NULL-id row
      // cannot be completed, failed, released, or refreshed.
      expect(
        await completeTaskWithEvent(pool(), {
          claimId: "claim_missing",
          eventSourceId: "src_reclaim_legacy_e2e",
          participantId: legacyOwner,
          result: { summary: "legacy complete" },
          sessionId,
          taskId,
        }),
      ).toBeNull();
      expect(
        await releaseTaskWithEvent(pool(), {
          claimId: "claim_missing",
          eventSourceId: "src_reclaim_legacy_e2e",
          participantId: legacyOwner,
          sessionId,
          taskId,
        }),
      ).toBeNull();
      expect(
        await refreshTaskClaim(pool(), {
          claimId: "claim_missing",
          claimLeaseTtlMs: 60_000,
          participantId: legacyOwner,
          sessionId,
          taskId,
        }),
      ).toBeNull();
      // The claim is still live, so another participant cannot win it yet.
      expect(
        await claimTaskWithEvent(pool(), {
          claimLeaseTtlMs: 60_000,
          eventSourceId: "src_reclaim_legacy_e2e",
          participantId: "part_early_challenger",
          sessionId,
          taskId,
        }),
      ).toBeNull();

      // Once the legacy claim elapses it can be atomically reclaimed.
      await elapseClaim(sessionId, taskId);
      const reclaim = await claimTaskWithEvent(pool(), {
        claimLeaseTtlMs: 60_000,
        eventSourceId: "src_reclaim_legacy_e2e",
        participantId: "part_reclaimer_legacy",
        sessionId,
        taskId,
      });
      expect(reclaim?.events.map((event) => event.type)).toEqual([
        "task.claim_expired",
        "task.claimed",
      ]);
      const [legacyExpired] = reclaim?.events ?? [];
      expect(legacyExpired?.payload.previousClaimedBy).toBe(legacyOwner);
      expect(reclaim?.task.claimedBy).toBe("part_reclaimer_legacy");
      expect(reclaim?.task.claimId).not.toBeNull();
    });
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
          claimId: claimed.task.claimId,
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
    const claimed = await request<TaskResponse>(
      `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: {
          instanceId: "inst_skewed_refresh",
          participantId: "part_skewed_refresh",
        },
        method: "POST",
      },
    );

    const refreshed = await withSkewedAppClock(-10_000, () =>
      request<TaskResponse>(
        `/sessions/${session.sessionId}/tasks/${task.task.taskId}/claim/refresh`,
        {
          body: {
            claimId: claimed.task.claimId,
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
          claimId: refreshed.task.claimId,
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
      const portA = await replicaA.listen(0);
      const portB = await replicaB.listen(0);
      replicaAStarted = true;
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
      const portA = await replicaA.listen(0);
      const portB = await replicaB.listen(0);
      replicaAStarted = true;
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
      const portA = await replicaA.listen(0);
      const portB = await replicaB.listen(0);
      replicaAStarted = true;
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
      const portA = await replicaA.listen(0);
      const portB = await replicaB.listen(0);
      replicaAStarted = true;
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

  /** Inserts one structurally valid active summary for context-read E2E setup. */
  async function insertPublishedContextSummary(input: {
    readonly budgetClass: string;
    readonly coversSeqFrom: number;
    readonly coversSeqTo: number;
    readonly headline: string;
    readonly sessionId: string;
    readonly sourceEventCount: number;
    readonly sourceFirstEventId: string;
    readonly sourceLastEventId: string;
    readonly summaryId: string;
    readonly superseded?: boolean;
  }): Promise<void> {
    await currentPool().pool.query(
      `
        INSERT INTO session_summaries (
          budget_class, content, covers_seq_from, covers_seq_to,
          generation_task_id, integrity_algorithm, integrity_hash,
          ollama_context_size, ollama_model, ollama_quantization,
          ollama_revision, ollama_thinking_mode, output_schema_version,
          producer_id, producer_version, prompt_version, published_at,
          session_id, source_event_count, source_first_event_id,
          source_last_event_id, source_range_hash, summary_id, superseded_at,
          validated_at
        )
        VALUES (
          $1, $2::jsonb, $3, $4, $5, 'sha256', $6,
          32768, 'local-model', 'q4_k_m', 'revision-1', 'low',
          'summary.v1', 'summary-worker', '1.0.0', 'prompt.v1',
          clock_timestamp(), $7, $8, $9, $10, $11, $12,
          CASE WHEN $13 THEN clock_timestamp() ELSE NULL END,
          clock_timestamp()
        )
      `,
      [
        input.budgetClass,
        JSON.stringify({
          facts: [],
          headline: input.headline,
          narrative: "Validated earlier durable context.",
          openQuestions: [],
        }),
        input.coversSeqFrom,
        input.coversSeqTo,
        `task_${input.summaryId}`,
        "a".repeat(64),
        input.sessionId,
        input.sourceEventCount,
        input.sourceFirstEventId,
        input.sourceLastEventId,
        "b".repeat(64),
        input.summaryId,
        input.superseded ?? false,
      ],
    );
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

  /** Creates a reserved and claimed real-Postgres job for the external worker E2E. */
  async function createSummaryWorkerJobFixture(label: string): Promise<{
    readonly authToken: string;
    readonly context: ParticipantTaskExecutorContext;
    readonly job: SessionSummaryGenerationJob;
  }> {
    const session = await createSession();
    for (const text of ["covered source one", "covered source two"]) {
      await appendEvent(
        currentPool(),
        {
          eventId: `evt_summary_worker_${label}_${randomUUID()}`,
          payload: { text },
          producerId: "summary-worker-e2e",
          sessionId: session.sessionId,
          type: "user.message",
        },
        { sourceId: "src_summary_worker_e2e" },
      );
    }
    const generation = createSessionSummaryGenerationService({
      store: createSessionSummaryStore(currentPool().pool),
    });
    const reservation = await generation.requestGeneration(
      createSummaryRangeFixture(session.sessionId),
    );
    if (reservation.status !== "created") {
      throw new Error(`Expected created Session Summary job, got ${reservation.status}`);
    }
    const participantId = `part_summary_worker_${label}_${randomUUID()}`;
    const instanceId = `inst_summary_worker_${label}_${randomUUID()}`;
    const acquisition = await acquireRestParticipantControl(currentPool(), {
      acquisitionId: `acq_summary_worker_${label}_${randomUUID()}`,
      capabilities: { workKinds: ["session_summary_generation"] },
      displayName: "Session Summary Worker E2E",
      eventSourceId: "src_summary_worker_control_e2e",
      instanceId,
      leaseTtlMs: 60_000,
      participantId,
      runtimeKind: "session_summary_worker",
      sessionId: session.sessionId,
    });
    if (acquisition.status === "conflict" || acquisition.status === "acquisition_stale") {
      throw new Error(`Failed to acquire worker control: ${acquisition.status}`);
    }
    const controlEpoch = acquisition.lease.epoch;
    const claimed = await claimTaskWithEvent(currentPool(), {
      claimLeaseTtlMs: 60_000,
      controlGuard: {
        controlChannel: "rest",
        controlEpoch,
        instanceId,
        participantId,
        sessionId: session.sessionId,
      },
      eventSourceId: "src_summary_worker_claim_e2e",
      participantId,
      sessionId: session.sessionId,
      taskId: reservation.job.taskId,
    });
    if (claimed === null) {
      throw new Error("Failed to claim reserved Session Summary job");
    }
    await appendEvent(
      currentPool(),
      {
        eventId: `evt_summary_worker_tail_${label}_${randomUUID()}`,
        payload: { text: "exact raw tail" },
        producerId: "summary-worker-e2e",
        sessionId: session.sessionId,
        type: "user.message",
      },
      { sourceId: "src_summary_worker_tail_e2e" },
    );
    return {
      authToken: mintE2eToken({ participantId, role: "participant", sessionId: session.sessionId }),
      context: {
        controlEpoch,
        instanceId,
        participantId,
        publishOutput: async () => undefined,
        publishProgress: async () => undefined,
        recentEvents: [],
        sessionId: session.sessionId,
        signal: new AbortController().signal,
        task: claimed.task satisfies TaskRecord,
      },
      job: reservation.job,
    };
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
    const setupPort = await setupApp.listen(0);
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
          body: {
            kind: "software_dev",
            objective: "Refresh under epoch serialization",
          },
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
      return {
        claimed,
        controlEpoch,
        instanceId,
        participantId,
        session,
        task,
      };
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

  /** Seeds a sanitized long event mix without exercising HTTP one row at a time. */
  async function seedProjectionBenchmarkSession(
    sessionId: string,
    eventCount: number,
  ): Promise<void> {
    await createDbSession(currentPool(), sessionId);
    await currentPool().pool.query(
      `
        INSERT INTO session_events (
          created_at,
          event_id,
          payload,
          producer_id,
          seq,
          session_id,
          type
        )
        SELECT
          clock_timestamp() + generated.seq * interval '1 microsecond',
          'evt_projection_benchmark_' || generated.seq || '_' || $1,
          CASE generated.seq % 10000
            WHEN 1 THEN jsonb_build_object('text', 'Sanitized benchmark message')
            WHEN 2 THEN jsonb_build_object('title', 'Sanitized benchmark title')
            WHEN 3 THEN jsonb_build_object(
              'branch',
              'main',
              'cwd',
              '/workspace/tether',
              'workspace',
              '/workspace/tether'
            )
            WHEN 4 THEN jsonb_build_object('archived', false)
            ELSE '{}'::jsonb
          END,
          'projection-benchmark-e2e',
          generated.seq,
          $1,
          CASE generated.seq % 10000
            WHEN 1 THEN 'user.message'
            WHEN 2 THEN 'session.title'
            WHEN 3 THEN 'host.online'
            WHEN 4 THEN 'session.archived'
            ELSE 'client.observed'
          END
        FROM generate_series(1, $2::int) AS generated(seq)
      `,
      [sessionId, eventCount],
    );
    await currentPool().pool.query(
      `
        UPDATE session_event_sequences
        SET next_seq = $2::bigint + 1
        WHERE session_id = $1
      `,
      [sessionId, eventCount],
    );
  }

  /** Measures sequential production append latency for one session. */
  async function measureProjectionAppendLatencies(
    sessionId: string,
    count: number,
  ): Promise<number[]> {
    const latencies: number[] = [];
    for (let index = 0; index < count; index += 1) {
      const startedAt = performance.now();
      await appendEvent(
        currentPool(),
        {
          eventId: `evt_projection_benchmark_live_${randomUUID()}`,
          payload: {},
          producerId: "projection-benchmark-e2e",
          sessionId,
          type: "client.observed",
        },
        { sourceId: "src_projection_benchmark_e2e" },
      );
      latencies.push(performance.now() - startedAt);
    }
    return latencies;
  }

  /** Returns the nearest-rank 95th percentile for a non-empty sample. */
  function percentile95(values: readonly number[]): number {
    if (values.length === 0) {
      throw new Error("Cannot calculate p95 for an empty sample");
    }
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.ceil(sorted.length * 0.95) - 1] ?? 0;
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
    const port = await app.listen(0);
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
    return request<TaskResponse>(`/sessions/${sessionId}/tasks/${task.task.taskId}/claim`, {
      body: controller,
      method: "POST",
    });
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
    const claimed = await request<TaskResponse>(
      `/sessions/${sessionId}/tasks/${task.task.taskId}/claim`,
      {
        body: controller,
        method: "POST",
      },
    );
    await request<TaskResponse>(`/sessions/${sessionId}/tasks/${task.task.taskId}/complete`, {
      body: {
        ...controller,
        claimId: claimed.task.claimId,
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
        ...init.headers,
      },
      method: init.method ?? "GET",
    });
    const text = await response.text();
    return {
      body: parseJsonResponseBody<TResponse>(text),
      headers: response.headers,
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
  readonly headers?: Readonly<Record<string, string>>;
  readonly method?: string;
}

/** Evaluated worker selection shared by success and failure integration fixtures. */
function enabledSummaryWorkerSelection() {
  return {
    candidate: {
      identity: summaryWorkerOllamaIdentity,
      outputSchemaVersion: "session-summary.v1",
      promptVersion: "session-summary.v1",
    },
    status: "enabled",
  } as const;
}

/** Creates one fake Ollama transport that returns protocol-valid structured content. */
function createFakeOllamaFetch(content: SessionSummaryContent): typeof globalThis.fetch {
  return async () => Response.json({ message: { content: JSON.stringify(content) } });
}

/** Builds the exact range-selection policy used by integrated worker jobs. */
function createSummaryRangeFixture(sessionId: string) {
  return {
    budgetClass: "8k",
    deadlineAt: new Date(Date.now() + 60_000),
    inputLimitBytes: 64_000,
    maxEventCount: 100,
    ollama: summaryWorkerOllamaIdentity,
    outputLimitBytes: 64_000,
    outputSchemaVersion: "session-summary.v1",
    producer: { id: "session-summary-worker", version: "e2e" },
    promptVersion: "session-summary.v1",
    sessionId,
  } as const;
}

/** Seeds a long raw-event session whose final events change inventory eligibility. */
async function createLongSessionFixture(database: DatabasePool, sessionId: string): Promise<void> {
  await createDbSession(database, sessionId);
  await database.pool.query(
    `
      INSERT INTO session_events (
        created_at,
        event_id,
        payload,
        producer_id,
        seq,
        session_id,
        type
      )
      SELECT
        clock_timestamp() + generated.seq * interval '1 millisecond',
        'evt_projection_long_' || generated.seq || '_' || $1,
        CASE
          WHEN generated.seq = 10001 THEN jsonb_build_object(
            'title',
            'Title after ten thousand'
          )
          WHEN generated.seq = 10002 THEN jsonb_build_object('archived', true)
          ELSE '{}'::jsonb
        END,
        'projection-long-e2e',
        generated.seq,
        $1,
        CASE
          WHEN generated.seq = 10001 THEN 'session.title'
          WHEN generated.seq = 10002 THEN 'session.archived'
          ELSE 'client.observed'
        END
      FROM generate_series(1, 10002) AS generated(seq)
    `,
    [sessionId],
  );
  await database.pool.query(
    `
      UPDATE session_event_sequences
      SET next_seq = 10003
      WHERE session_id = $1
    `,
    [sessionId],
  );
}

/** Builds the non-skipped verification contract for a long-session projection. */
function createProjectionVerificationFixture(sessionId: string) {
  return {
    expected: {
      differenceFields: [],
      freshCoversSeqTo: 10_002,
      freshEventCount: 10_002,
      status: "current",
    },
    options: { batchSize: 500, sessionId },
  } as const;
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
 * Sends an authenticated but structurally invalid WebSocket handshake and
 * returns the server response after the peer closes the transport.
 */
async function sendInvalidWebSocketUpgrade(url: URL): Promise<string> {
  const port = Number(url.port);
  if (!Number.isSafeInteger(port) || port <= 0) {
    throw new Error("Invalid WebSocket test port");
  }
  return new Promise<string>((resolve, reject) => {
    const chunks: Buffer[] = [];
    const socket = createConnection({ host: url.hostname, port });
    socket.once("error", reject);
    socket.on("data", (chunk: Buffer) => chunks.push(chunk));
    socket.once("close", () => resolve(Buffer.concat(chunks).toString("utf8")));
    socket.once("connect", () => {
      socket.write(
        [
          `GET ${url.pathname}${url.search} HTTP/1.1`,
          `Host: ${url.host}`,
          "Connection: Upgrade",
          "Upgrade: websocket",
          "Sec-WebSocket-Version: 13",
          "Sec-WebSocket-Key: invalid",
          "",
          "",
        ].join("\r\n"),
      );
    });
  });
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
 * Checks whether a parsed WebSocket message is an error with the expected reason.
 */
function isWebSocketErrorWithReason(value: unknown, reason: string): boolean {
  return (
    isRecord(value) &&
    value.op === webSocketOperation.error &&
    value.reason === reason &&
    typeof value.error === "string"
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
  readonly command?: string;
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
    claimId: null,
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

/** Resolves with the bounded close code and reason observed by a WebSocket peer. */
async function waitForSocketCloseDetails(
  socket: WebSocket,
): Promise<{ readonly code: number; readonly reason: string }> {
  return new Promise((resolve, reject) => {
    socket.once("close", (code, reason) => resolve({ code, reason: reason.toString("utf8") }));
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
