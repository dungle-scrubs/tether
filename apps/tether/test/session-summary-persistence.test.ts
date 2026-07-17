import { readdir, readFile } from "node:fs/promises";

import { getTableConfig } from "drizzle-orm/pg-core";
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
});

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
