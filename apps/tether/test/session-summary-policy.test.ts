import type {
  SessionSummaryCandidateSubmission,
  SessionSummaryGenerationJob,
} from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import {
  decideSessionSummaryPublication,
  selectSessionSummaryRange,
  validateSessionSummaryCandidate,
} from "../src/session-summary-policy.js";

describe("Session Summary publication policy", () => {
  it("selects from stream start, then contiguously after the published head", () => {
    expect(
      selectSessionSummaryRange({
        maxEventCount: 100,
        publishedHead: null,
        streamEndSeq: 250,
        streamStartSeq: 1,
      }),
    ).toEqual({ range: { from: 1, to: 100 }, status: "selected" });
    expect(
      selectSessionSummaryRange({
        maxEventCount: 100,
        publishedHead: { coversSeqFrom: 1, coversSeqTo: 100, summaryId: "summary_1" },
        streamEndSeq: 250,
        streamStartSeq: 1,
      }),
    ).toEqual({ range: { from: 101, to: 200 }, status: "selected" });
  });

  it("rejects a candidate from the wrong complete Ollama identity", () => {
    const job = generationJob();
    const submission = candidateSubmission(job);

    expect(() =>
      validateSessionSummaryCandidate({
        currentPublishedHead: null,
        job,
        submission: {
          ...submission,
          ollama: { ...submission.ollama, revision: "sha256:wrong-revision" },
        },
      }),
    ).toThrowError(expect.objectContaining({ code: "candidate_identity_mismatch" }));
  });

  it("rejects publication gaps and overlaps around the current head", () => {
    const publishedHead = { coversSeqFrom: 1, coversSeqTo: 100, summaryId: "summary_1" };

    expect(() =>
      decideSessionSummaryPublication({
        candidateRange: { from: 2, to: 120 },
        publishedHead,
        streamStartSeq: 1,
      }),
    ).toThrowError(expect.objectContaining({ code: "gap" }));
    expect(() =>
      decideSessionSummaryPublication({
        candidateRange: { from: 1, to: 100 },
        publishedHead,
        streamStartSeq: 1,
      }),
    ).toThrowError(expect.objectContaining({ code: "overlap" }));
    expect(
      decideSessionSummaryPublication({
        candidateRange: { from: 1, to: 120 },
        publishedHead,
        streamStartSeq: 1,
      }),
    ).toEqual({ status: "publish", supersedeSummaryId: "summary_1" });
  });

  it("rejects a stale job after the publication head changes", () => {
    const job = generationJob();

    expect(() =>
      validateSessionSummaryCandidate({
        currentPublishedHead: {
          coversSeqFrom: 1,
          coversSeqTo: 20,
          summaryId: "summary_other",
        },
        job,
        submission: candidateSubmission(job),
      }),
    ).toThrowError(expect.objectContaining({ code: "stale_job" }));
  });
});

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
  return {
    claimantId: "part_worker_1",
    content: {
      facts: [],
      headline: "Bounded history",
      narrative: "A compact source-grounded session history.",
      openQuestions: [],
    },
    controlEpoch: 4,
    instanceId: "inst_worker_1",
    integrity: { algorithm: "sha256", hash: "a".repeat(64) },
    kind: "session_summary.candidate.v1",
    ollama: job.ollama,
    range: job.range,
    sessionId: job.sessionId,
    source: job.source,
    summaryId: job.summaryId,
    taskId: job.taskId,
  };
}
