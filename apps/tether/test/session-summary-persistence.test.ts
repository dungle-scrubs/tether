import { readdir, readFile } from "node:fs/promises";

import { getTableConfig } from "drizzle-orm/pg-core";
import { deriveSessionSummaryCorrelationId } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import { sessionSummaries } from "../src/schema.js";
import {
  createSessionSummaryStore,
  type SessionSummaryStoreClient,
} from "../src/session-summary-store.js";

describe("Session Summary persistence", () => {
  it("defines the complete session_summaries schema and one-active-summary invariant", () => {
    const table = getTableConfig(sessionSummaries);

    expect(table.name).toBe("session_summaries");
    expect(table.columns.map((column) => column.name)).toEqual([
      "budget_class",
      "content",
      "covers_seq_from",
      "covers_seq_to",
      "created_at",
      "failure",
      "generation_task_id",
      "integrity_algorithm",
      "integrity_hash",
      "ollama_context_size",
      "ollama_model",
      "ollama_quantization",
      "ollama_revision",
      "ollama_thinking_mode",
      "output_schema_version",
      "producer_id",
      "producer_version",
      "prompt_version",
      "published_at",
      "quarantined_at",
      "session_id",
      "source_event_count",
      "source_first_event_id",
      "source_last_event_id",
      "source_range_hash",
      "summary_id",
      "superseded_at",
      "validated_at",
    ]);
    expect(table.columns.find((column) => column.name === "summary_id")?.primary).toBe(true);
    expect(table.indexes.map((index) => index.config.name)).toContain(
      "session_summaries_active_unique",
    );
    const activeIndex = table.indexes.find(
      (index) => index.config.name === "session_summaries_active_unique",
    );
    expect(activeIndex?.config.unique).toBe(true);
    expect(activeIndex?.config.where).toBeDefined();
  });

  it("generates the summary table and partial publication uniqueness invariant", async () => {
    const migrationDirectory = new URL("../drizzle/", import.meta.url);
    const migrationNames = (await readdir(migrationDirectory))
      .filter((name) => /^\d{4}_.+\.sql$/u.test(name))
      .sort();
    const summaryMigrationName = migrationNames.find((name) => name.startsWith("0015_"));

    expect(summaryMigrationName).toBeDefined();
    const migrationSql = await readFile(
      new URL(summaryMigrationName ?? "missing.sql", migrationDirectory),
      "utf8",
    );
    expect(migrationSql).toContain('CREATE TABLE "session_summaries"');
    expect(migrationSql).toContain('"content" jsonb');
    expect(migrationSql).toContain('"validated_at" timestamp with time zone');
    expect(migrationSql).toContain('"published_at" timestamp with time zone');
    expect(migrationSql).toContain('"quarantined_at" timestamp with time zone');
    expect(migrationSql).toContain('"superseded_at" timestamp with time zone');
    expect(migrationSql).toContain(
      'CREATE UNIQUE INDEX "session_summaries_active_unique" ON "session_summaries" USING btree ("session_id","budget_class") WHERE "session_summaries"."published_at" IS NOT NULL AND "session_summaries"."superseded_at" IS NULL',
    );
  });

  it("locks session and budget class before reserving an exact generation range", async () => {
    const client = new RangeSelectionClient();
    const store = createSessionSummaryStore(
      {
        connect: async () => client,
      },
      {
        createGenerationTask: async (transactionClient) => {
          await transactionClient.query("SELECT 'summary-task-created'");
        },
      },
    );

    const result = await store.selectAndReserveGeneration({
      budgetClass: "standard",
      deadlineAt: new Date("2026-07-17T00:05:00.000Z"),
      inputLimitBytes: 262_144,
      maxEventCount: 2,
      ollama: {
        contextSize: 32_768,
        model: "qwen3:8b",
        quantization: "Q4_K_M",
        revision: "sha256:model-revision",
        thinkingMode: "enabled",
      },
      outputLimitBytes: 32_768,
      outputSchemaVersion: "session-summary.v1",
      producer: { id: "session-summary-worker", version: "1.0.0" },
      promptVersion: "session-summary-prompt.v1",
      sessionId: "sess_1",
      summaryId: "summary_1",
      taskId: "task_summary_1",
    });

    expect(result.status).toBe("reserved");
    if (result.status !== "reserved") {
      throw new Error("expected a reserved generation job");
    }
    expect(result.job.range).toEqual({ from: 1, to: 2 });
    expect(result.job.source).toMatchObject({
      eventCount: 2,
      firstEventId: "evt_1",
      lastEventId: "evt_2",
    });
    const lockIndex = client.queries.findIndex((query) => query.includes("pg_advisory_xact_lock"));
    const publicationReadIndex = client.queries.findIndex((query) =>
      query.includes("published_at IS NOT NULL"),
    );
    const insertIndex = client.queries.findIndex((query) =>
      query.includes("INSERT INTO session_summaries"),
    );
    const taskCreateIndex = client.queries.findIndex((query) =>
      query.includes("summary-task-created"),
    );
    expect(lockIndex).toBeGreaterThanOrEqual(0);
    expect(publicationReadIndex).toBeGreaterThan(lockIndex);
    expect(insertIndex).toBeGreaterThan(publicationReadIndex);
    expect(taskCreateIndex).toBeGreaterThan(insertIndex);
    expect(taskCreateIndex).toBeLessThan(client.queries.length - 1);
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("returns in_progress while a pending reservation's task can still publish", async () => {
    const client = new ReclaimableReservationClient([
      {
        cancelledAt: null,
        completedAt: null,
        failedAt: null,
        summaryId: "summary_live",
        taskFailure: null,
        taskId: "task_summary_live",
        taskInput: pendingGenerationJob("2026-07-17T01:00:00.000Z"),
      },
    ]);
    const store = createSessionSummaryStore({ connect: async () => client });

    const result = await store.selectAndReserveGeneration(reservationInput());

    expect(result).toEqual({ status: "in_progress" });
    expect(client.queries.some((query) => query.includes("SET failure ="))).toBe(false);
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("quarantines a pending reservation whose task terminally failed and reserves anew", async () => {
    const client = new ReclaimableReservationClient([
      {
        cancelledAt: null,
        completedAt: null,
        failedAt: new Date("2026-07-17T00:06:00.000Z"),
        summaryId: "summary_dead",
        taskFailure: {
          attempt: 2,
          code: "poison_range",
          message: "Session Summary range produced invalid or oversized work",
          retryable: false,
        },
        taskId: "task_summary_dead",
        taskInput: pendingGenerationJob("2026-07-17T01:00:00.000Z"),
      },
    ]);
    const store = createSessionSummaryStore(
      { connect: async () => client },
      {
        createGenerationTask: async (transactionClient) => {
          await transactionClient.query("SELECT 'summary-task-created'");
        },
      },
    );

    const result = await store.selectAndReserveGeneration(reservationInput());

    expect(result.status).toBe("reserved");
    const quarantine = client.calls.find((call) => call.sql.includes("SET failure ="));
    expect(quarantine?.values[0]).toBe("summary_dead");
    expect(String(quarantine?.values[1])).toContain("poison_range");
    const lockIndex = client.queries.findIndex((query) => query.includes("pg_advisory_xact_lock"));
    const quarantineIndex = client.queries.findIndex((query) => query.includes("SET failure ="));
    const insertIndex = client.queries.findIndex((query) =>
      query.includes("INSERT INTO session_summaries"),
    );
    expect(quarantineIndex).toBeGreaterThan(lockIndex);
    expect(insertIndex).toBeGreaterThan(quarantineIndex);
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("quarantines a pending reservation whose job deadline elapsed and reserves anew", async () => {
    const client = new ReclaimableReservationClient([
      {
        cancelledAt: null,
        completedAt: null,
        failedAt: null,
        summaryId: "summary_elapsed",
        taskFailure: null,
        taskId: "task_summary_elapsed",
        taskInput: pendingGenerationJob("2026-07-17T00:05:00.000Z"),
      },
    ]);
    const store = createSessionSummaryStore(
      { connect: async () => client },
      {
        createGenerationTask: async (transactionClient) => {
          await transactionClient.query("SELECT 'summary-task-created'");
        },
      },
    );

    const result = await store.selectAndReserveGeneration(reservationInput());

    expect(result.status).toBe("reserved");
    const quarantine = client.calls.find((call) => call.sql.includes("SET failure ="));
    expect(quarantine?.values[0]).toBe("summary_elapsed");
    expect(String(quarantine?.values[1])).toContain("deadline_exceeded");
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("reads only the active published summary for one session and budget class", async () => {
    const client = new PublishedSummaryClient();
    const store = createSessionSummaryStore({ connect: async () => client });

    const summary = await store.readLatestPublished("sess_1", "8k");

    expect(summary).toMatchObject({
      budgetClass: "8k",
      content: { headline: "Durable context" },
      coversSeqFrom: 1,
      coversSeqTo: 42,
      integrity: { algorithm: "sha256", hash: "a".repeat(64) },
      sessionId: "sess_1",
      summaryId: "summary_active",
    });
    expect(client.values).toEqual(["sess_1", "8k"]);
    expect(client.queryText).toContain("published_at IS NOT NULL");
    expect(client.queryText).toContain("superseded_at IS NULL");
  });

  it("preserves bounded correlation on typed publication failures", async () => {
    const summaryId = "summary_sensitive_identifier";
    const store = createSessionSummaryStore({ connect: async () => new MissingSummaryClient() });

    await expect(store.publishCandidate(summaryId)).rejects.toMatchObject({
      code: "summary_not_found",
      correlationId: deriveSessionSummaryCorrelationId(summaryId),
    });
  });
});

class MissingSummaryClient implements SessionSummaryStoreClient {
  async query<TRow>(): Promise<{ readonly rows: TRow[] }> {
    return { rows: [] };
  }

  release(): void {}
}

class PublishedSummaryClient implements SessionSummaryStoreClient {
  queryText = "";
  values: readonly unknown[] = [];

  async query<TRow>(sql: string, values?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queryText = sql.replaceAll(/\s+/gu, " ").trim();
    this.values = values ?? [];
    return {
      rows: [
        {
          budgetClass: "8k",
          content: {
            facts: [],
            headline: "Durable context",
            narrative: "Earlier exact events.",
            openQuestions: [],
          },
          coversSeqFrom: "1",
          coversSeqTo: "42",
          createdAt: new Date("2026-07-17T00:00:00.000Z"),
          failure: null,
          generationTaskId: "task_summary",
          integrityAlgorithm: "sha256",
          integrityHash: "a".repeat(64),
          ollamaContextSize: 32_768,
          ollamaModel: "local-model",
          ollamaQuantization: "q4_k_m",
          ollamaRevision: "revision-1",
          ollamaThinkingMode: "low",
          outputSchemaVersion: "summary.v1",
          producerId: "summary-worker",
          producerVersion: "1.0.0",
          promptVersion: "prompt.v1",
          publishedAt: new Date("2026-07-17T00:01:00.000Z"),
          quarantinedAt: null,
          sessionId: "sess_1",
          sourceEventCount: "42",
          sourceFirstEventId: "evt_1",
          sourceLastEventId: "evt_42",
          sourceRangeHash: "b".repeat(64),
          summaryId: "summary_active",
          supersededAt: null,
          validatedAt: new Date("2026-07-17T00:00:59.000Z"),
        },
      ] as TRow[],
    };
  }

  release(): void {}
}

interface PendingReservationFakeRow {
  readonly cancelledAt: Date | null;
  readonly completedAt: Date | null;
  readonly failedAt: Date | null;
  readonly summaryId: string;
  readonly taskFailure: unknown;
  readonly taskId: string | null;
  readonly taskInput: unknown;
}

class ReclaimableReservationClient implements SessionSummaryStoreClient {
  readonly calls: { readonly sql: string; readonly values: readonly unknown[] }[] = [];
  readonly queries: string[] = [];

  constructor(private readonly pendingRows: readonly PendingReservationFakeRow[]) {}

  async query<TRow>(sql: string, values?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    const normalized = sql.replaceAll(/\s+/gu, " ").trim();
    this.queries.push(normalized);
    this.calls.push({ sql: normalized, values: values ?? [] });
    if (normalized.includes("LEFT JOIN tasks")) {
      return { rows: [...this.pendingRows] as TRow[] };
    }
    if (normalized.includes("SET failure =")) {
      return { rows: [{ summaryId: values?.[0] }] as TRow[] };
    }
    if (normalized.includes('AS "now"')) {
      return { rows: [{ now: new Date("2026-07-17T00:10:00.000Z") }] as TRow[] };
    }
    if (normalized.includes("published_at IS NOT NULL")) {
      return { rows: [] };
    }
    if (normalized.includes("min(seq)")) {
      return { rows: [{ streamEndSeq: "3", streamStartSeq: "1" }] as TRow[] };
    }
    if (normalized.includes("FROM session_events") && normalized.includes("ORDER BY seq")) {
      return {
        rows: [
          { eventId: "evt_1", seq: "1" },
          { eventId: "evt_2", seq: "2" },
        ] as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

function reservationInput(): Parameters<
  ReturnType<typeof createSessionSummaryStore>["selectAndReserveGeneration"]
>[0] {
  return {
    budgetClass: "standard",
    deadlineAt: new Date("2026-07-17T00:35:00.000Z"),
    inputLimitBytes: 262_144,
    maxEventCount: 2,
    ollama: {
      contextSize: 32_768,
      model: "qwen3:8b",
      quantization: "Q4_K_M",
      revision: "sha256:model-revision",
      thinkingMode: "enabled",
    },
    outputLimitBytes: 32_768,
    outputSchemaVersion: "session-summary.v1",
    producer: { id: "session-summary-worker", version: "1.0.0" },
    promptVersion: "session-summary-prompt.v1",
    sessionId: "sess_1",
    summaryId: "summary_next",
    taskId: "task_summary_next",
  };
}

function pendingGenerationJob(deadlineAt: string): Record<string, unknown> {
  return {
    budgetClass: "standard",
    deadlineAt,
    expectedPrevious: { coversSeqTo: null, summaryId: null },
    inputLimitBytes: 262_144,
    kind: "session_summary.generate.v1",
    ollama: {
      contextSize: 32_768,
      model: "qwen3:8b",
      quantization: "Q4_K_M",
      revision: "sha256:model-revision",
      thinkingMode: "enabled",
    },
    outputLimitBytes: 32_768,
    outputSchemaVersion: "session-summary.v1",
    previousSummary: null,
    producer: { id: "session-summary-worker", version: "1.0.0" },
    promptVersion: "session-summary-prompt.v1",
    range: { from: 1, to: 2 },
    sessionId: "sess_1",
    source: {
      eventCount: 2,
      firstEventId: "evt_1",
      lastEventId: "evt_2",
      rangeHash: "b".repeat(64),
    },
    summaryId: "summary_live",
    taskId: "task_summary_live",
  };
}

class RangeSelectionClient implements SessionSummaryStoreClient {
  readonly queries: string[] = [];

  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    const normalized = sql.replaceAll(/\s+/gu, " ").trim();
    this.queries.push(normalized);
    if (normalized.includes("FROM session_summaries") && normalized.includes("published_at")) {
      return { rows: [] };
    }
    if (normalized.includes("FROM session_summaries") && normalized.includes("content IS NULL")) {
      return { rows: [] };
    }
    if (normalized.includes("min(seq)")) {
      return { rows: [{ streamEndSeq: "3", streamStartSeq: "1" }] as TRow[] };
    }
    if (normalized.includes("FROM session_events") && normalized.includes("ORDER BY seq")) {
      return {
        rows: [
          { eventId: "evt_1", seq: "1" },
          { eventId: "evt_2", seq: "2" },
        ] as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}
