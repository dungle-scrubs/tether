import { describe, expect, it } from "vitest";

import { buildSessionSummaryEvalCandidate } from "../evals/session-summary/report.js";

describe("Session Summary evaluation report aggregation", () => {
  it("computes reproducible latency, memory, repeat, and stability metrics", () => {
    const candidate = buildSessionSummaryEvalCandidate({
      configuration: {
        identity: {
          contextSize: 32_768,
          model: "qwen3:0.6b",
          quantization: "Q4_K_M",
          revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
          thinkingMode: "disabled",
        },
        outputSchemaVersion: "session-summary.v1",
        promptVersion: "session-summary.v1",
        requestOptions: {
          numContext: 32_768,
          numPredict: 2_048,
          seed: 33,
          temperature: 0,
        },
      },
      runs: [
        run("case-a", 1, 100, 700, true),
        run("case-a", 2, 200, 800, true),
        run("case-a", 3, 300, 900, true),
        run("case-b", 1, 50, 1_000, true),
        run("case-b", 2, 500, 1_100, false),
        run("case-b", 3, 600, 1_200, true),
      ],
    });

    expect(candidate.hardGatesPassed).toBe(false);
    expect(candidate.deterministicGatesPassed).toBe(false);
    expect(candidate.metrics).toEqual({
      p95LatencyMs: 600,
      peakMemoryBytes: 1_200,
      repeats: 3,
      stability: 5 / 6,
    });
    expect(candidate.latency).toEqual({ coldP95Ms: 100, warmP95Ms: 600 });
    expect(candidate.semanticSafety).toBe("unproven");
  });
});

function run(
  caseId: string,
  repeat: number,
  wallDurationMs: number,
  peakMemoryBytes: number,
  passed: boolean,
) {
  return {
    caseId,
    cold: repeat === 1,
    failures: passed ? [] : ["criticalFacts: missing critical fact tuple"],
    ollamaDurationMs: wallDurationMs - 10,
    ollamaLoadDurationMs: repeat === 1 ? 20 : 0,
    passed,
    peakMemoryBytes,
    peakVramBytes: 0,
    repeat,
    wallDurationMs,
  } as const;
}
