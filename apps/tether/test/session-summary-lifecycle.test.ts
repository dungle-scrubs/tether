import type {
  SessionSummaryCandidateSubmission,
  SessionSummaryContent,
  SessionSummaryGenerationJob,
} from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import {
  computeSessionSummaryIntegrityHash,
  createSessionSummaryStore,
  type SessionSummaryStoreClient,
} from "../src/session-summary-store.js";

describe("Session Summary lifecycle persistence", () => {
  it("inserts a claimed and Control Epoch-fenced candidate under the publication lock", async () => {
    const job = generationJob();
    const submission = candidateSubmission(job);
    const client = new CandidateInsertionClient(job);
    const store = createSessionSummaryStore({ connect: async () => client });

    const result = await store.insertCandidate(submission);

    expect(result).toEqual({ status: "inserted", summaryId: job.summaryId });
    expect(client.queries.some((query) => query.includes("participant_control_leases"))).toBe(true);
    expect(client.queries.some((query) => query.includes("claimed_by"))).toBe(true);
    expect(client.queries.some((query) => query.includes("UPDATE session_summaries"))).toBe(true);
    expect(client.queries.at(-1)).toBe("COMMIT");
    expect(
      client.queries.findIndex((query) => query.includes("participant_control_leases")),
    ).toBeLessThan(client.queries.findIndex((query) => query.includes("FROM tasks")));
    expect(client.queries.findIndex((query) => query.includes("FROM tasks"))).toBeLessThan(
      client.queries.findIndex((query) => query.includes("pg_advisory_xact_lock")),
    );
  });

  it("atomically inserts, validates, and publishes a candidate", async () => {
    const job = generationJob();
    const client = new CandidateInsertionClient(job);
    const store = createSessionSummaryStore({ connect: async () => client });

    const result = await store.submitCandidate(candidateSubmission(job));

    expect(result).toEqual({
      status: "published",
      summaryId: job.summaryId,
      supersededSummaryId: null,
    });
    expect(client.queries.at(-1)).toBe("COMMIT");
    const lifecycleUpdate = client.queries.find((query) =>
      query.includes("validated_at = clock_timestamp()"),
    );
    expect(lifecycleUpdate).toContain("content = $2::jsonb");
    expect(client.queries.some((query) => query.includes("published_at = clock_timestamp()"))).toBe(
      true,
    );
  });

  it("accepts the current WebSocket control fence used by an external participant runtime", async () => {
    const job = generationJob();
    const client = new CandidateInsertionClient(job, null, "ws");
    const store = createSessionSummaryStore({ connect: async () => client });

    await expect(store.submitCandidate(candidateSubmission(job))).resolves.toMatchObject({
      status: "published",
      summaryId: job.summaryId,
    });
  });

  it("rejects a candidate submitted by a participant that does not own the task claim", async () => {
    const job = generationJob();
    const store = createSessionSummaryStore({
      connect: async () => new CandidateInsertionClient(job),
    });

    await expect(
      store.insertCandidate({ ...candidateSubmission(job), claimantId: "part_wrong_worker" }),
    ).rejects.toMatchObject({ code: "wrong_claimant" });
  });

  it("rejects a candidate whose integrity hash does not match its structured content", async () => {
    const job = generationJob();
    const store = createSessionSummaryStore({
      connect: async () => new CandidateInsertionClient(job),
    });

    await expect(
      store.insertCandidate({
        ...candidateSubmission(job),
        integrity: { algorithm: "sha256", hash: "c".repeat(64) },
      }),
    ).rejects.toMatchObject({ code: "integrity_mismatch" });
  });

  it("rejects a duplicate candidate submission before changing lifecycle state", async () => {
    const job = generationJob();
    const submission = candidateSubmission(job);
    const store = createSessionSummaryStore({
      connect: async () => new CandidateInsertionClient(job, submission.content),
    });

    await expect(store.insertCandidate(submission)).rejects.toMatchObject({
      code: "duplicate_submission",
    });
  });

  it("validates an inserted candidate without publishing it", async () => {
    const client = new LifecycleTransitionClient();
    const store = createSessionSummaryStore({ connect: async () => client });

    const result = await store.validateCandidate("summary_1");

    expect(result).toEqual({ status: "validated", summaryId: "summary_1" });
    const update = client.queries.find((query) => query.includes("UPDATE session_summaries"));
    expect(update).toContain("validated_at = clock_timestamp()");
    expect(update).toContain("content IS NOT NULL");
    expect(update).not.toContain("published_at =");
  });

  it("quarantines a candidate with bounded failure metadata without publishing it", async () => {
    const client = new LifecycleTransitionClient();
    const store = createSessionSummaryStore({ connect: async () => client });
    const failure = {
      attempt: 3,
      code: "invalid_output" as const,
      message: "Structured output failed validation.",
      retryable: false,
    };

    const result = await store.quarantineCandidate({ failure, summaryId: "summary_1" });

    expect(result).toEqual({ status: "quarantined", summaryId: "summary_1" });
    const update = client.queries.find((query) => query.includes("UPDATE session_summaries"));
    expect(update).toContain("failure =");
    expect(update).toContain("quarantined_at = clock_timestamp()");
    expect(update).not.toContain("published_at =");
  });

  it("serializes publication and supersedes the prior active summary atomically", async () => {
    const client = new PublicationClient();
    const store = createSessionSummaryStore({ connect: async () => client });

    const result = await store.publishCandidate("summary_2");

    expect(result).toEqual({
      status: "published",
      summaryId: "summary_2",
      supersededSummaryId: "summary_1",
    });
    const lockIndex = client.queries.findIndex((query) => query.includes("pg_advisory_xact_lock"));
    const supersedeIndex = client.queries.findIndex((query) =>
      query.includes("SET superseded_at = clock_timestamp()"),
    );
    const publishIndex = client.queries.findIndex((query) =>
      query.includes("SET published_at = clock_timestamp()"),
    );
    expect(lockIndex).toBeGreaterThanOrEqual(0);
    expect(supersedeIndex).toBeGreaterThan(lockIndex);
    expect(publishIndex).toBeGreaterThan(supersedeIndex);
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("inspects complete lifecycle metadata without returning summary content", async () => {
    const store = createSessionSummaryStore({ connect: async () => new InspectionClient() });

    const inspection = await store.inspectCandidate("summary_1");

    expect(inspection).toMatchObject({
      active: true,
      budgetClass: "standard",
      status: "published",
      summaryId: "summary_1",
    });
    expect(inspection).not.toHaveProperty("content");
    expect(inspection?.ollama).toEqual({
      contextSize: 32_768,
      model: "qwen3:8b",
      quantization: "Q4_K_M",
      revision: "sha256:model-revision",
      thinkingMode: "enabled",
    });
  });
});

class InspectionClient implements SessionSummaryStoreClient {
  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_summaries")) {
      return {
        rows: [
          {
            budgetClass: "standard",
            coversSeqFrom: "1",
            coversSeqTo: "20",
            failure: null,
            generationTaskId: "task_summary_1",
            ollamaContextSize: 32_768,
            ollamaModel: "qwen3:8b",
            ollamaQuantization: "Q4_K_M",
            ollamaRevision: "sha256:model-revision",
            ollamaThinkingMode: "enabled",
            outputSchemaVersion: "session-summary.v1",
            producerId: "session-summary-worker",
            producerVersion: "1.0.0",
            promptVersion: "session-summary-prompt.v1",
            publishedAt: new Date("2026-07-17T00:02:00.000Z"),
            quarantinedAt: null,
            sessionId: "sess_1",
            sourceEventCount: "20",
            sourceFirstEventId: "evt_1",
            sourceLastEventId: "evt_20",
            sourceRangeHash: "b".repeat(64),
            summaryId: "summary_1",
            supersededAt: null,
            validatedAt: new Date("2026-07-17T00:01:00.000Z"),
          },
        ] as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

class PublicationClient implements SessionSummaryStoreClient {
  readonly queries: string[] = [];

  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    const normalized = sql.replaceAll(/\s+/gu, " ").trim();
    this.queries.push(normalized);
    if (normalized.includes("FROM session_summaries") && normalized.includes("WHERE summary_id")) {
      return {
        rows: [
          {
            budgetClass: "standard",
            content: {
              facts: [],
              headline: "Next range",
              narrative: "The next contiguous range.",
              openQuestions: [],
            },
            coversSeqFrom: "1",
            coversSeqTo: "120",
            generationTaskId: "task_summary_2",
            publishedAt: null,
            quarantinedAt: null,
            sessionId: "sess_1",
            summaryId: "summary_2",
            supersededAt: null,
            validatedAt: new Date("2026-07-17T00:01:00.000Z"),
          },
        ] as TRow[],
      };
    }
    if (normalized.includes("published_at IS NOT NULL")) {
      return {
        rows: [
          {
            content: {
              facts: [],
              headline: "Prior range",
              narrative: "The prior cumulative range.",
              openQuestions: [],
            },
            coversSeqFrom: "1",
            coversSeqTo: "100",
            summaryId: "summary_1",
          },
        ] as TRow[],
      };
    }
    if (normalized.includes("min(seq)")) {
      return { rows: [{ streamStartSeq: "1" }] as TRow[] };
    }
    if (normalized.includes("UPDATE session_summaries")) {
      return { rows: [{ summaryId: "summary_2" }] as TRow[] };
    }
    return { rows: [] };
  }

  release(): void {}
}

class LifecycleTransitionClient implements SessionSummaryStoreClient {
  readonly queries: string[] = [];

  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    const normalized = sql.replaceAll(/\s+/gu, " ").trim();
    this.queries.push(normalized);
    if (normalized.includes("UPDATE session_summaries")) {
      return { rows: [{ summaryId: "summary_1" }] as TRow[] };
    }
    return { rows: [] };
  }

  release(): void {}
}

class CandidateInsertionClient implements SessionSummaryStoreClient {
  readonly queries: string[] = [];

  constructor(
    private readonly job: SessionSummaryGenerationJob,
    private readonly existingContent: SessionSummaryContent | null = null,
    private readonly controlChannel: "rest" | "ws" = "rest",
  ) {}

  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    const normalized = sql.replaceAll(/\s+/gu, " ").trim();
    this.queries.push(normalized);
    if (normalized.includes("FROM session_summaries") && normalized.includes("WHERE summary_id")) {
      return {
        rows: [
          {
            budgetClass: this.job.budgetClass,
            content: this.existingContent,
            coversSeqFrom: String(this.job.range.from),
            coversSeqTo: String(this.job.range.to),
            generationTaskId: this.job.taskId,
            publishedAt: null,
            quarantinedAt: null,
            sessionId: this.job.sessionId,
            summaryId: this.job.summaryId,
            supersededAt: null,
            validatedAt: null,
          },
        ] as TRow[],
      };
    }
    if (normalized.includes("published_at IS NOT NULL")) {
      return { rows: [] };
    }
    if (normalized.includes("FROM tasks") && normalized.includes("FOR UPDATE")) {
      return {
        rows: [
          {
            cancelledAt: null,
            claimActive: true,
            claimedBy: "part_worker_1",
            completedAt: null,
            failedAt: null,
            input: this.job,
            kind: "session_summary_generation",
          },
        ] as TRow[],
      };
    }
    if (normalized.includes("FROM participant_control_leases")) {
      return {
        rows: [
          {
            controlChannel: this.controlChannel,
            epoch: "4",
            instanceId: "inst_worker_1",
            leaseActive: true,
          },
        ] as TRow[],
      };
    }
    if (normalized.includes("min(seq)")) {
      return { rows: [{ streamStartSeq: "1" }] as TRow[] };
    }
    if (normalized.includes("clock_timestamp()")) {
      return { rows: [{ now: new Date("2026-07-17T00:01:00.000Z") }] as TRow[] };
    }
    if (normalized.includes("UPDATE session_summaries")) {
      return { rows: [{ summaryId: this.job.summaryId }] as TRow[] };
    }
    return { rows: [] };
  }

  release(): void {}
}

function generationJob(): SessionSummaryGenerationJob {
  return {
    budgetClass: "standard",
    deadlineAt: "2026-07-17T00:05:00.000Z",
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
    range: { from: 1, to: 20 },
    sessionId: "sess_1",
    source: {
      eventCount: 20,
      firstEventId: "evt_1",
      lastEventId: "evt_20",
      rangeHash: "b".repeat(64),
    },
    summaryId: "summary_1",
    taskId: "task_summary_1",
  };
}

function candidateSubmission(job: SessionSummaryGenerationJob): SessionSummaryCandidateSubmission {
  const content = {
    facts: [],
    headline: "Bounded history",
    narrative: "A compact source-grounded session history.",
    openQuestions: [],
  };
  return {
    claimantId: "part_worker_1",
    content,
    controlEpoch: 4,
    instanceId: "inst_worker_1",
    integrity: {
      algorithm: "sha256",
      hash: computeSessionSummaryIntegrityHash(content),
    },
    kind: "session_summary.candidate.v1",
    ollama: job.ollama,
    range: job.range,
    sessionId: job.sessionId,
    source: job.source,
    summaryId: job.summaryId,
    taskId: job.taskId,
  };
}
