import { describe, expect, it } from "vitest";

import type { StructuredLogEntry } from "../src/observability.js";
import { TetherInvariantError } from "../src/observability.js";
import { SessionScalabilityDiagnostics } from "../src/session-scalability-diagnostics.js";
import { SessionScalabilityRuntimeState } from "../src/session-scalability-runtime-state.js";

describe("Session scalability diagnostics", () => {
  it("keeps scoped debug logging silent when its toggle is disabled", () => {
    const logs: StructuredLogEntry[] = [];
    const diagnostics = new SessionScalabilityDiagnostics(new DiagnosticPool(), {
      debugEnabled: false,
      logger: { log: (entry) => logs.push(entry) },
      moduleName: "SessionScalabilityDiagnosticsTest",
    });

    diagnostics.recordContext("raw_only");

    expect(logs).toEqual([]);
    expect(diagnostics.debugInfo()).toMatchObject({ debugEnabled: false, rawOnlyCount: 1 });
  });

  it("bounds process-local maintenance outcome inventory", () => {
    const state = new SessionScalabilityRuntimeState();
    for (let index = 0; index <= 1_024; index += 1) {
      state.recordBackfill(`sess_${index}`, {
        batchesRead: 1,
        malformedEventCount: 0,
        status: "written",
      });
    }

    expect(state.read("sess_0").latestBackfill).toEqual({ status: "not_observed" });
    expect(state.read("sess_1024").latestBackfill).toMatchObject({ status: "written" });
  });

  it("centralizes content-free current projection and disabled summary health", async () => {
    const diagnostics = new SessionScalabilityDiagnostics(new DiagnosticPool());
    diagnostics.recordContext("raw_only");
    diagnostics.recordContext("summary_with_raw_tail");

    const record = await diagnostics.read("sess_1");

    expect(record).toMatchObject({
      context: { rawOnlyCount: 1, summaryBackedCount: 1 },
      healthWarnings: [
        "summary_publication_disabled",
        "summary_worker_disabled",
        "ollama_disabled",
        "retention_disabled",
      ],
      projection: {
        activeReducerVersion: 1,
        coverage: { coversSeqTo: 42, eventCount: 42 },
        current: true,
        currentCount: 1,
        enabled: true,
        latestBackfill: { status: "not_observed" },
        latestVerification: { status: "not_observed" },
        staleCount: 0,
      },
      summary: {
        active: [{ budgetClass: "8k", coversSeqTo: 40, summaryId: "summary_1" }],
        activeCandidate: null,
        disabledReason: "hard_gates_not_passed",
        publicationEnabled: false,
        rejectionCode: null,
        retentionEnabled: false,
      },
      worker: {
        ollamaStatus: "disabled",
        reason: "hard_gates_not_passed",
        status: "disabled",
      },
    });
    expect(JSON.stringify(record)).not.toContain("summary narrative");
    expect(JSON.stringify(record)).not.toContain("event payload");
  });

  it("keeps debug and health in agreement for stale and rejected state", async () => {
    const state = new SessionScalabilityRuntimeState();
    state.recordBackfill("sess_1", {
      batchesRead: 3,
      malformedEventCount: 1,
      status: "stale",
    });
    state.recordVerification("sess_1", {
      batchesRead: 4,
      differenceCount: 2,
      malformedEventCount: 1,
      status: "mismatch",
    });
    const diagnostics = new SessionScalabilityDiagnostics(
      new DiagnosticPool({
        activeCandidates: [summaryHead("summary_candidate")],
        health: { invalidSummaryCount: "1", staleProjectionCount: "1" },
        projectionRows: [{ coversSeqTo: "41", eventCount: "42", reducerVersion: 0 }],
        rejections: [{ rejectionCode: "invalid_output" }],
      }),
      undefined,
      state,
    );

    const record = await diagnostics.read("sess_1");
    const healthWarnings = await diagnostics.readHealthWarnings();

    expect(record.projection).toMatchObject({
      current: false,
      latestBackfill: { batchesRead: 3, status: "stale" },
      latestVerification: { differenceCount: 2, status: "mismatch" },
      staleCount: 1,
    });
    expect(record.summary).toMatchObject({
      activeCandidate: { summaryId: "summary_candidate" },
      rejectionCode: "invalid_output",
    });
    expect(record.worker).toMatchObject({ ollamaStatus: "disabled", status: "disabled" });
    expect(record.healthWarnings).toEqual(healthWarnings);
    expect(healthWarnings).toEqual([
      "projection_stale",
      "summary_invalid",
      "summary_publication_disabled",
      "summary_worker_disabled",
      "ollama_disabled",
      "retention_disabled",
    ]);
  });

  it("clears per-session and aggregate invalid warnings after a newer candidate", async () => {
    const pool = new DiagnosticPool({
      activeCandidates: [summaryHead("summary_recovered")],
      health: { invalidSummaryCount: "0", staleProjectionCount: "0" },
      rejections: [],
    });
    const diagnostics = new SessionScalabilityDiagnostics(pool);

    const record = await diagnostics.read("sess_1");
    const healthWarnings = await diagnostics.readHealthWarnings();

    expect(record.summary).toMatchObject({
      activeCandidate: { summaryId: "summary_recovered" },
      rejectionCode: null,
    });
    expect(record.healthWarnings).toEqual(healthWarnings);
    expect(record.healthWarnings).not.toContain("summary_invalid");
    expect(pool.queries.find((query) => query.includes("rejectionCode"))).toContain("NOT EXISTS");
  });

  it("honors debug toggles and reports content-free invariant failures", async () => {
    const logs: StructuredLogEntry[] = [];
    const diagnostics = new SessionScalabilityDiagnostics(
      new DiagnosticPool({
        projectionRows: [
          { coversSeqTo: "1", eventCount: "1", reducerVersion: 1 },
          { coversSeqTo: "2", eventCount: "2", reducerVersion: 1 },
        ],
      }),
      {
        boundaryLogsEnabled: true,
        debugEnabled: true,
        logger: { log: (entry) => logs.push(entry) },
        moduleName: "SessionScalabilityDiagnosticsTest",
      },
    );
    diagnostics.recordContext("raw_only");

    await expect(diagnostics.read("sess_secret_identifier")).rejects.toBeInstanceOf(
      TetherInvariantError,
    );
    expect(diagnostics.debugInfo()).toMatchObject({
      boundaryCalls: 1,
      boundaryFailures: 1,
      debugEnabled: true,
      rawOnlyCount: 1,
    });
    const encoded = JSON.stringify(logs);
    expect(encoded).toContain("context.mode_recorded");
    expect(encoded).not.toContain("sess_secret_identifier");
    expect(encoded).not.toContain("summary narrative");
    expect(encoded).not.toContain("event payload");
  });
});

interface DiagnosticPoolOptions {
  readonly activeCandidates?: readonly Record<string, unknown>[];
  readonly health?: {
    readonly invalidSummaryCount: string | number;
    readonly staleProjectionCount: string | number;
  };
  readonly projectionRows?: readonly Record<string, unknown>[];
  readonly published?: readonly Record<string, unknown>[];
  readonly rejections?: readonly Record<string, unknown>[];
}

class DiagnosticPool {
  readonly #options: DiagnosticPoolOptions;
  readonly queries: string[] = [];

  constructor(options: DiagnosticPoolOptions = {}) {
    this.#options = options;
  }

  async query<TRow extends Record<string, unknown>>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql);
    if (sql.includes("FROM sessions AS session")) {
      return {
        rows: [
          this.#options.health ?? { invalidSummaryCount: "0", staleProjectionCount: "0" },
        ] as unknown as TRow[],
      };
    }
    if (sql.includes("FROM session_projections")) {
      return {
        rows: (this.#options.projectionRows ?? [
          { coversSeqTo: "42", eventCount: "42", reducerVersion: 1 },
        ]) as unknown as TRow[],
      };
    }
    if (sql.includes("quarantined_at IS NOT NULL")) {
      return { rows: (this.#options.rejections ?? []) as unknown as TRow[] };
    }
    if (sql.includes("published_at IS NULL")) {
      return { rows: (this.#options.activeCandidates ?? []) as unknown as TRow[] };
    }
    return {
      rows: (this.#options.published ?? [summaryHead("summary_1")]) as unknown as TRow[],
    };
  }
}

function summaryHead(summaryId: string): Record<string, unknown> {
  return {
    budgetClass: "8k",
    coversSeqFrom: "1",
    coversSeqTo: "40",
    producerId: "worker",
    producerVersion: "1.0.0",
    summaryId,
  };
}
