import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import {
  selectSessionSummaryPublication,
  sessionSummaryPublicationBaseline,
} from "../src/session-summary-publication-config.js";

describe("Session Summary publication selection", () => {
  it("binds the production-safe outcome to the committed A-005 baseline", () => {
    const report: unknown = JSON.parse(
      readFileSync(
        new URL("../evals/session-summary/reports/a-005-baseline.json", import.meta.url),
        "utf8",
      ),
    );

    expect(selectSessionSummaryPublication(report)).toEqual(sessionSummaryPublicationBaseline);
    expect(report).toMatchObject({ selection: sessionSummaryPublicationBaseline });
  });

  it("does not retain canaries, prompts, raw events, or generated output", () => {
    const serialized = readFileSync(
      new URL("../evals/session-summary/reports/a-005-baseline.json", import.meta.url),
      "utf8",
    );

    expect(serialized).not.toContain("EVAL_SECRET_CANARY_7F3A");
    expect(serialized).not.toContain('"coveredEvents"');
    expect(serialized).not.toContain('"messages"');
    expect(serialized).not.toContain('"payload"');
    expect(serialized).not.toContain('"response"');
  });

  it("fails closed when the report metadata is incomplete", () => {
    const selection = selectSessionSummaryPublication({ candidates: [] });

    expect(selection).toEqual({ reason: "invalid_report", status: "disabled" });
  });

  it("fails closed when semantic safety remains unproven", () => {
    const selection = selectSessionSummaryPublication({
      candidates: [
        {
          hardGatesPassed: true,
          identity: {
            contextSize: 32_768,
            model: "qwen3:0.6b",
            quantization: "Q4_K_M",
            revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
            thinkingMode: "disabled",
          },
          metrics: {
            peakMemoryBytes: 751_632_384,
            p95LatencyMs: 13_500,
            repeats: 3,
            stability: 1,
          },
          outputSchemaVersion: "session-summary.v1",
          promptVersion: "session-summary.v1",
          semanticSafety: "unproven",
        },
      ],
      corpusVersion: "session-summary-corpus.v1",
      generatedAt: "2026-07-17T00:00:00.000Z",
      ollamaVersion: "0.31.1",
      reportVersion: "session-summary-a005.v1",
      thresholds: {
        maximumP95LatencyMs: 30_000,
        maximumPeakMemoryBytes: 2_147_483_648,
        minimumRepeats: 3,
        minimumStability: 1,
      },
    });

    expect(selection).toEqual({
      reason: "semantic_safety_unproven",
      status: "disabled",
    });
  });

  it("fails closed when a required resource metric is zero", () => {
    const selection = selectSessionSummaryPublication({
      candidates: [
        {
          hardGatesPassed: true,
          identity: {
            contextSize: 32_768,
            model: "qwen3:0.6b",
            quantization: "Q4_K_M",
            revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
            thinkingMode: "disabled",
          },
          metrics: {
            peakMemoryBytes: 0,
            p95LatencyMs: 13_500,
            repeats: 3,
            stability: 1,
          },
          outputSchemaVersion: "session-summary.v1",
          promptVersion: "session-summary.v1",
          semanticSafety: "proven",
        },
      ],
      corpusVersion: "session-summary-corpus.v1",
      generatedAt: "2026-07-17T00:00:00.000Z",
      ollamaVersion: "0.31.1",
      reportVersion: "session-summary-a005.v1",
      thresholds: {
        maximumP95LatencyMs: 30_000,
        maximumPeakMemoryBytes: 2_147_483_648,
        minimumRepeats: 3,
        minimumStability: 1,
      },
    });

    expect(selection).toEqual({
      reason: "invalid_resource_metrics",
      status: "disabled",
    });
  });

  it("enables the fully pinned candidate only when every gate passes", () => {
    const selection = selectSessionSummaryPublication({
      candidates: [
        {
          hardGatesPassed: true,
          identity: {
            contextSize: 32_768,
            model: "qwen3:0.6b",
            quantization: "Q4_K_M",
            revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
            thinkingMode: "disabled",
          },
          metrics: {
            peakMemoryBytes: 751_632_384,
            p95LatencyMs: 13_500,
            repeats: 3,
            stability: 1,
          },
          outputSchemaVersion: "session-summary.v1",
          promptVersion: "session-summary.v1",
          semanticSafety: "proven",
        },
      ],
      corpusVersion: "session-summary-corpus.v1",
      generatedAt: "2026-07-17T00:00:00.000Z",
      ollamaVersion: "0.31.1",
      reportVersion: "session-summary-a005.v1",
      thresholds: {
        maximumP95LatencyMs: 30_000,
        maximumPeakMemoryBytes: 2_147_483_648,
        minimumRepeats: 3,
        minimumStability: 1,
      },
    });

    expect(selection).toEqual({
      candidate: {
        identity: {
          contextSize: 32_768,
          model: "qwen3:0.6b",
          quantization: "Q4_K_M",
          revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
          thinkingMode: "disabled",
        },
        outputSchemaVersion: "session-summary.v1",
        promptVersion: "session-summary.v1",
      },
      status: "enabled",
    });
  });

  it("fails closed when the model revision is not a pinned sha256 digest", () => {
    const selection = selectSessionSummaryPublication({
      candidates: [
        {
          hardGatesPassed: true,
          identity: {
            contextSize: 32_768,
            model: "qwen3:0.6b",
            quantization: "Q4_K_M",
            revision: "latest",
            thinkingMode: "disabled",
          },
          metrics: {
            peakMemoryBytes: 751_632_384,
            p95LatencyMs: 13_500,
            repeats: 3,
            stability: 1,
          },
          outputSchemaVersion: "session-summary.v1",
          promptVersion: "session-summary.v1",
          semanticSafety: "proven",
        },
      ],
      corpusVersion: "session-summary-corpus.v1",
      generatedAt: "2026-07-17T00:00:00.000Z",
      ollamaVersion: "0.31.1",
      reportVersion: "session-summary-a005.v1",
      thresholds: {
        maximumP95LatencyMs: 30_000,
        maximumPeakMemoryBytes: 2_147_483_648,
        minimumRepeats: 3,
        minimumStability: 1,
      },
    });

    expect(selection).toEqual({
      reason: "candidate_identity_invalid",
      status: "disabled",
    });
  });
});
