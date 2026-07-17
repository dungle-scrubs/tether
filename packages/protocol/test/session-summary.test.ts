import { describe, expect, it } from "vitest";

import {
  sessionSummaryCandidateSubmissionSchema,
  sessionSummaryGenerationJobSchema,
  sessionSummaryInspectionSchema,
  sessionSummaryRecordSchema,
} from "../src/index.js";

describe("Session Summary protocol", () => {
  it("validates a complete durable summary record and rejects an unsafe range", () => {
    const record = {
      budgetClass: "standard",
      content: {
        facts: [
          {
            category: "decision",
            sourceEventIds: ["evt_1"],
            statement: "Use the durable projection as the validation authority.",
            subjectIds: ["decision_projection_authority"],
          },
        ],
        headline: "Projection-backed summary",
        narrative: "The session selected a projection-backed summary lifecycle.",
        openQuestions: ["Which evaluated Ollama candidate will be enabled?"],
      },
      coversSeqFrom: 1,
      coversSeqTo: 20,
      createdAt: "2026-07-17T00:00:00.000Z",
      failure: null,
      generationTaskId: "task_summary_1",
      integrity: {
        algorithm: "sha256",
        hash: "a".repeat(64),
      },
      ollama: {
        contextSize: 32_768,
        model: "qwen3:8b",
        quantization: "Q4_K_M",
        revision: "sha256:model-revision",
        thinkingMode: "enabled",
      },
      outputSchemaVersion: "session-summary.v1",
      producer: {
        id: "session-summary-worker",
        version: "1.0.0",
      },
      promptVersion: "session-summary-prompt.v1",
      publishedAt: "2026-07-17T00:00:03.000Z",
      quarantinedAt: null,
      sessionId: "sess_1",
      source: {
        eventCount: 20,
        firstEventId: "evt_1",
        lastEventId: "evt_20",
        rangeHash: "b".repeat(64),
      },
      summaryId: "summary_1",
      supersededAt: null,
      validatedAt: "2026-07-17T00:00:02.000Z",
    };

    expect(sessionSummaryRecordSchema.parse(record)).toEqual(record);
    expect(
      sessionSummaryRecordSchema.safeParse({
        ...record,
        coversSeqFrom: Number.MAX_SAFE_INTEGER + 1,
      }).success,
    ).toBe(false);
  });

  it("validates generation jobs, fenced candidate submissions, inspections, and failures", () => {
    const content = {
      facts: [],
      headline: "Bounded history",
      narrative: "A compact source-grounded session history.",
      openQuestions: [],
    };
    const ollama = {
      contextSize: 32_768,
      model: "qwen3:8b",
      quantization: "Q4_K_M",
      revision: "sha256:model-revision",
      thinkingMode: "enabled" as const,
    };
    const producer = { id: "session-summary-worker", version: "1.0.0" };
    const source = {
      eventCount: 20,
      firstEventId: "evt_1",
      lastEventId: "evt_20",
      rangeHash: "b".repeat(64),
    };
    const job = {
      budgetClass: "standard",
      deadlineAt: "2026-07-17T00:05:00.000Z",
      expectedPrevious: { coversSeqTo: null, summaryId: null },
      inputLimitBytes: 262_144,
      kind: "session_summary.generate.v1" as const,
      ollama,
      outputLimitBytes: 32_768,
      outputSchemaVersion: "session-summary.v1",
      previousSummary: null,
      producer,
      promptVersion: "session-summary-prompt.v1",
      range: { from: 1, to: 20 },
      sessionId: "sess_1",
      source,
      summaryId: "summary_1",
      taskId: "task_summary_1",
    };
    const submission = {
      claimantId: "part_worker_1",
      content,
      controlEpoch: 4,
      instanceId: "inst_worker_1",
      integrity: { algorithm: "sha256" as const, hash: "a".repeat(64) },
      kind: "session_summary.candidate.v1" as const,
      ollama,
      range: job.range,
      sessionId: job.sessionId,
      source,
      summaryId: job.summaryId,
      taskId: job.taskId,
    };
    const inspection = {
      active: false,
      budgetClass: "standard",
      coversSeqFrom: 1,
      coversSeqTo: 20,
      failure: {
        attempt: 3,
        code: "invalid_output" as const,
        message: "Structured output failed validation.",
        retryable: false,
      },
      generationTaskId: job.taskId,
      ollama,
      outputSchemaVersion: job.outputSchemaVersion,
      producer,
      promptVersion: job.promptVersion,
      publishedAt: null,
      quarantinedAt: "2026-07-17T00:00:03.000Z",
      sessionId: job.sessionId,
      source,
      status: "quarantined" as const,
      summaryId: job.summaryId,
      supersededAt: null,
      validatedAt: null,
    };

    expect(sessionSummaryGenerationJobSchema.parse(job)).toEqual(job);
    expect(sessionSummaryCandidateSubmissionSchema.parse(submission)).toEqual(submission);
    expect(sessionSummaryInspectionSchema.parse(inspection)).toEqual(inspection);
    expect(
      sessionSummaryInspectionSchema.safeParse({
        ...inspection,
        failure: { ...inspection.failure, message: "x".repeat(513) },
      }).success,
    ).toBe(false);
  });
});
