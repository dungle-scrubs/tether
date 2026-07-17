import type { SessionSummaryOllamaIdentity } from "@dungle-scrubs/tether-protocol";

import type {
  SessionSummaryEvalCandidate,
  SessionSummaryEvalCandidateMetrics,
} from "../../src/session-summary-publication-config.js";

/** Fully reproducible request configuration measured by one eval candidate. */
export interface SessionSummaryEvalConfiguration {
  readonly identity: SessionSummaryOllamaIdentity;
  readonly outputSchemaVersion: string;
  readonly promptVersion: string;
  readonly requestOptions: {
    readonly numContext: number;
    readonly numPredict: number;
    readonly seed: number;
    readonly temperature: number;
  };
}

/** Bounded measurements and scorer outcome for one case repetition. */
export interface SessionSummaryEvalRun {
  readonly caseId: string;
  readonly cold: boolean;
  readonly failures: readonly string[];
  readonly ollamaDurationMs: number;
  readonly ollamaLoadDurationMs: number;
  readonly passed: boolean;
  readonly peakMemoryBytes: number;
  readonly peakVramBytes: number;
  readonly repeat: number;
  readonly wallDurationMs: number;
}

/** Complete candidate report retained by the A-005 baseline. */
export interface SessionSummaryEvalCandidateReport extends SessionSummaryEvalCandidate {
  readonly configuration: SessionSummaryEvalConfiguration;
  readonly deterministicGatesPassed: boolean;
  readonly latency: {
    readonly coldP95Ms: number;
    readonly warmP95Ms: number;
  };
  readonly runs: readonly SessionSummaryEvalRun[];
}

/** Input to deterministic candidate report aggregation. */
export interface BuildSessionSummaryEvalCandidateInput {
  readonly configuration: SessionSummaryEvalConfiguration;
  readonly runs: readonly SessionSummaryEvalRun[];
}

/** Aggregates one complete candidate configuration and its measured runs. */
export function buildSessionSummaryEvalCandidate(
  input: BuildSessionSummaryEvalCandidateInput,
): SessionSummaryEvalCandidateReport {
  const metrics = aggregateMetrics(input.runs);
  const deterministicGatesPassed = input.runs.length > 0 && input.runs.every((run) => run.passed);
  return {
    configuration: input.configuration,
    deterministicGatesPassed,
    hardGatesPassed: false,
    identity: input.configuration.identity,
    latency: {
      coldP95Ms: percentile95(
        input.runs.filter((run) => run.cold).map((run) => run.wallDurationMs),
      ),
      warmP95Ms: percentile95(
        input.runs.filter((run) => !run.cold).map((run) => run.wallDurationMs),
      ),
    },
    metrics,
    outputSchemaVersion: input.configuration.outputSchemaVersion,
    promptVersion: input.configuration.promptVersion,
    runs: input.runs,
    semanticSafety: "unproven",
  };
}

function aggregateMetrics(
  runs: readonly SessionSummaryEvalRun[],
): SessionSummaryEvalCandidateMetrics {
  const latencyValues = runs.map((run) => run.wallDurationMs).sort((left, right) => left - right);
  const caseRepeatCounts = [...new Set(runs.map((run) => run.caseId))].map(
    (caseId) => runs.filter((run) => run.caseId === caseId).length,
  );
  return {
    p95LatencyMs: percentile95(latencyValues),
    peakMemoryBytes: Math.max(0, ...runs.map((run) => run.peakMemoryBytes)),
    repeats: caseRepeatCounts.length === 0 ? 0 : Math.min(...caseRepeatCounts),
    stability: runs.length === 0 ? 0 : runs.filter((run) => run.passed).length / runs.length,
  };
}

function percentile95(values: readonly number[]): number {
  if (values.length === 0) {
    return 0;
  }
  const sortedValues = [...values].sort((left, right) => left - right);
  const index = Math.ceil(sortedValues.length * 0.95) - 1;
  return sortedValues[index] ?? 0;
}
