import type {
  SessionScalabilityDebugRecord,
  SessionScalabilityHealthWarning,
  SessionScalabilitySummaryHead,
} from "@dungle-scrubs/tether-protocol";

import {
  ModuleObservability,
  type ModuleObservabilityOptions,
  readModuleObservabilityOptions,
} from "./observability.js";
import { SESSION_PROJECTION_REDUCER_VERSION } from "./session-projection.js";
import { sessionEventRetentionConfiguration } from "./session-event-retention.js";
import { sessionSummaryPublicationBaseline } from "./session-summary-publication-config.js";
import {
  type SessionScalabilityRuntimeState,
  sessionScalabilityRuntimeState,
} from "./session-scalability-runtime-state.js";

interface DiagnosticQueryPool {
  readonly query: <TRow extends Record<string, unknown>>(
    sql: string,
    values?: unknown[],
  ) => Promise<{ readonly rows: TRow[] }>;
}

interface ProjectionDiagnosticRow extends Record<string, unknown> {
  readonly coversSeqTo: string | number;
  readonly eventCount: string | number;
  readonly reducerVersion: number;
}

interface SummaryDiagnosticRow extends Record<string, unknown> {
  readonly budgetClass: string;
  readonly coversSeqFrom: string | number;
  readonly coversSeqTo: string | number;
  readonly producerId: string;
  readonly producerVersion: string;
  readonly summaryId: string;
}

interface SummaryRejectionRow extends Record<string, unknown> {
  readonly rejectionCode: string | null;
}

interface ScalabilityHealthRow extends Record<string, unknown> {
  readonly invalidSummaryCount: string | number;
  readonly staleProjectionCount: string | number;
}

/**
 * Truthful current-cutoff health while A-005 keeps publication, the external
 * worker, and Ollama disabled. No dynamic model readiness is manufactured.
 */
export const sessionScalabilityBaselineWarnings = [
  "summary_publication_disabled",
  "summary_worker_disabled",
  "ollama_disabled",
  "retention_disabled",
] as const satisfies readonly SessionScalabilityHealthWarning[];

/** Stable failure codes for malformed content-free diagnostic rows. */
export type SessionScalabilityDiagnosticFailureCode =
  | "projection_head_invalid"
  | "summary_head_invalid";

/** Typed boundary error that never includes event or summary content. */
export class SessionScalabilityDiagnosticError extends Error {
  readonly code: SessionScalabilityDiagnosticFailureCode;

  constructor(code: SessionScalabilityDiagnosticFailureCode) {
    super(code);
    this.name = "SessionScalabilityDiagnosticError";
    this.code = code;
  }
}

/**
 * Central content-free operator projection for event-log scalability state.
 * The class intentionally selects metadata columns only.
 */
export class SessionScalabilityDiagnostics {
  readonly #observability: ModuleObservability;
  readonly #pool: DiagnosticQueryPool;
  readonly #runtimeState: SessionScalabilityRuntimeState;
  #rawOnlyCount = 0;
  #summaryBackedCount = 0;

  constructor(
    pool: DiagnosticQueryPool,
    observability?: ModuleObservabilityOptions,
    runtimeState: SessionScalabilityRuntimeState = sessionScalabilityRuntimeState,
  ) {
    this.#pool = pool;
    this.#observability = new ModuleObservability(
      observability ?? readModuleObservabilityOptions("SessionScalabilityDiagnostics"),
    );
    this.#runtimeState = runtimeState;
  }

  /** Records which bounded context path served one request. */
  recordContext(mode: "raw_only" | "summary_with_raw_tail"): void {
    if (mode === "raw_only") {
      this.#rawOnlyCount += 1;
    } else {
      this.#summaryBackedCount += 1;
    }
    this.#observability.debug("recordContext", "context.mode_recorded", {
      rawOnlyCount: this.#rawOnlyCount,
      summaryBackedCount: this.#summaryBackedCount,
    });
  }

  /** Reads safe projection, summary, context, worker, and health metadata. */
  async read(sessionId: string): Promise<SessionScalabilityDebugRecord> {
    return this.#observability.traceBoundary(
      "read",
      { sessionIdPresent: sessionId.length > 0 },
      async () => {
        const [projectionResult, summaryResult, activeCandidateResult, rejectionResult] =
          await Promise.all([
            this.#pool.query<ProjectionDiagnosticRow>(
              `SELECT
               covers_seq_to AS "coversSeqTo",
               event_count AS "eventCount",
               reducer_version AS "reducerVersion"
             FROM session_projections
             WHERE session_id = $1`,
              [sessionId],
            ),
            this.#pool.query<SummaryDiagnosticRow>(
              `SELECT
               budget_class AS "budgetClass",
               covers_seq_from AS "coversSeqFrom",
               covers_seq_to AS "coversSeqTo",
               producer_id AS "producerId",
               producer_version AS "producerVersion",
               summary_id AS "summaryId"
             FROM session_summaries
             WHERE session_id = $1
               AND published_at IS NOT NULL
               AND superseded_at IS NULL
             ORDER BY budget_class ASC
             LIMIT 32`,
              [sessionId],
            ),
            this.#pool.query<SummaryDiagnosticRow>(
              `SELECT
               budget_class AS "budgetClass",
               covers_seq_from AS "coversSeqFrom",
               covers_seq_to AS "coversSeqTo",
               producer_id AS "producerId",
               producer_version AS "producerVersion",
               summary_id AS "summaryId"
             FROM session_summaries
             WHERE session_id = $1
               AND published_at IS NULL
               AND quarantined_at IS NULL
               AND superseded_at IS NULL
             ORDER BY created_at DESC, summary_id DESC
             LIMIT 1`,
              [sessionId],
            ),
            this.#pool.query<SummaryRejectionRow>(
              `SELECT failure ->> 'code' AS "rejectionCode"
             FROM session_summaries
             WHERE session_id = $1
               AND quarantined_at IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1
                 FROM session_summaries AS newer
                 WHERE newer.session_id = session_summaries.session_id
                   AND newer.budget_class = session_summaries.budget_class
                   AND (newer.created_at, newer.summary_id) >
                       (session_summaries.created_at, session_summaries.summary_id)
               )
             ORDER BY quarantined_at DESC, summary_id DESC
             LIMIT 1`,
              [sessionId],
            ),
          ]);
        this.#observability.assertInvariant(
          projectionResult.rows.length <= 1,
          "read",
          "projection inventory returned more than one row",
          { projectionRowCount: projectionResult.rows.length },
        );
        this.#observability.assertInvariant(
          summaryResult.rows.length <= 32,
          "read",
          "summary inventory exceeded its bounded query",
          { summaryRowCount: summaryResult.rows.length },
        );
        const projection = parseProjection(projectionResult.rows[0]);
        const active = summaryResult.rows.map(parseSummaryHead);
        const activeCandidate = activeCandidateResult.rows[0]
          ? parseSummaryHead(activeCandidateResult.rows[0])
          : null;
        const rejectionCode = parseRejectionCode(rejectionResult.rows[0]?.rejectionCode ?? null);
        const current =
          projection !== null && projection.reducerVersion === SESSION_PROJECTION_REDUCER_VERSION;
        const runtime = this.#runtimeState.read(sessionId);
        const warnings = buildWarnings(current, rejectionCode !== null);
        const disabledReason =
          sessionSummaryPublicationBaseline.status === "disabled"
            ? sessionSummaryPublicationBaseline.reason
            : null;
        return {
          context: {
            rawOnlyCount: this.#rawOnlyCount,
            summaryBackedCount: this.#summaryBackedCount,
          },
          healthWarnings: warnings,
          projection: {
            activeReducerVersion: SESSION_PROJECTION_REDUCER_VERSION,
            coverage:
              projection === null
                ? null
                : {
                    coversSeqTo: projection.coversSeqTo,
                    eventCount: projection.eventCount,
                  },
            current,
            currentCount: current ? 1 : 0,
            enabled: true,
            latestBackfill: runtime.latestBackfill,
            latestVerification: runtime.latestVerification,
            staleCount: current ? 0 : 1,
          },
          retention: sessionEventRetentionConfiguration,
          sessionId,
          summary: {
            active,
            activeCandidate,
            disabledReason,
            publicationEnabled: sessionSummaryPublicationBaseline.status === "enabled",
            rejectionCode,
            retentionEnabled: false,
          },
          worker: {
            ollamaStatus: "disabled",
            reason: disabledReason,
            status: "disabled",
          },
        };
      },
      (record) => ({
        activeSummaryCount: record.summary.active.length,
        healthWarningCount: record.healthWarnings.length,
        projectionCurrent: record.projection.current,
      }),
      (error) => ({
        errorCode:
          error instanceof SessionScalabilityDiagnosticError
            ? error.code
            : "diagnostic_read_failed",
      }),
    );
  }

  /** Reads bounded aggregate warnings used by the unauthenticated health route. */
  async readHealthWarnings(): Promise<readonly SessionScalabilityHealthWarning[]> {
    const result = await this.#pool.query<ScalabilityHealthRow>(
      `SELECT
         count(*) FILTER (
           WHERE projection.session_id IS NULL
              OR projection.reducer_version <> $1
         ) AS "staleProjectionCount",
         (
           SELECT count(*)
           FROM session_summaries AS candidate
           WHERE candidate.quarantined_at IS NOT NULL
             AND NOT EXISTS (
               SELECT 1
               FROM session_summaries AS newer
               WHERE newer.session_id = candidate.session_id
                 AND newer.budget_class = candidate.budget_class
                 AND (newer.created_at, newer.summary_id) >
                     (candidate.created_at, candidate.summary_id)
             )
         ) AS "invalidSummaryCount"
       FROM sessions AS session
       LEFT JOIN session_projections AS projection
         ON projection.session_id = session.session_id`,
      [SESSION_PROJECTION_REDUCER_VERSION],
    );
    const row = result.rows[0];
    const staleProjectionCount = parseNonNegativeSafeInteger(row?.staleProjectionCount ?? 0);
    const invalidSummaryCount = parseNonNegativeSafeInteger(row?.invalidSummaryCount ?? 0);
    if (staleProjectionCount === null || invalidSummaryCount === null) {
      throw new SessionScalabilityDiagnosticError("projection_head_invalid");
    }
    return buildWarnings(staleProjectionCount === 0, invalidSummaryCount > 0);
  }

  /** Returns process-local boundary counters without database content. */
  debugInfo(): ReturnType<ModuleObservability["debugInfo"]> & {
    readonly rawOnlyCount: number;
    readonly summaryBackedCount: number;
  } {
    return {
      ...this.#observability.debugInfo(),
      rawOnlyCount: this.#rawOnlyCount,
      summaryBackedCount: this.#summaryBackedCount,
    };
  }
}

function parseProjection(
  row: ProjectionDiagnosticRow | undefined,
):
  | (ProjectionDiagnosticRow & { readonly coversSeqTo: number; readonly eventCount: number })
  | null {
  if (row === undefined) {
    return null;
  }
  const coversSeqTo = parseNonNegativeSafeInteger(row.coversSeqTo);
  const eventCount = parseNonNegativeSafeInteger(row.eventCount);
  if (
    coversSeqTo === null ||
    eventCount === null ||
    !Number.isSafeInteger(row.reducerVersion) ||
    row.reducerVersion < 0
  ) {
    throw new SessionScalabilityDiagnosticError("projection_head_invalid");
  }
  return { ...row, coversSeqTo, eventCount };
}

function parseSummaryHead(row: SummaryDiagnosticRow): SessionScalabilitySummaryHead {
  const coversSeqFrom = parseNonNegativeSafeInteger(row.coversSeqFrom);
  const coversSeqTo = parseNonNegativeSafeInteger(row.coversSeqTo);
  if (
    coversSeqFrom === null ||
    coversSeqTo === null ||
    coversSeqFrom > coversSeqTo ||
    row.budgetClass.length === 0 ||
    row.producerId.length === 0 ||
    row.producerVersion.length === 0 ||
    row.summaryId.length === 0
  ) {
    throw new SessionScalabilityDiagnosticError("summary_head_invalid");
  }
  return { ...row, coversSeqFrom, coversSeqTo };
}

function parseRejectionCode(value: string | null): string | null {
  if (value === null) {
    return null;
  }
  if (!/^[a-z][a-z0-9_]{0,63}$/u.test(value)) {
    throw new SessionScalabilityDiagnosticError("summary_head_invalid");
  }
  return value;
}

function parseNonNegativeSafeInteger(value: string | number): number | null {
  const parsed = typeof value === "number" ? value : Number.parseInt(value, 10);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function buildWarnings(
  currentProjection: boolean,
  summaryInvalid: boolean,
): readonly SessionScalabilityHealthWarning[] {
  return [
    ...(currentProjection ? [] : (["projection_stale"] as const)),
    ...(summaryInvalid ? (["summary_invalid"] as const) : []),
    ...sessionScalabilityBaselineWarnings,
  ];
}
