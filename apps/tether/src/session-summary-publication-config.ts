import { sessionSummaryOllamaIdentitySchema } from "@dungle-scrubs/tether-protocol";
import type { SessionSummaryOllamaIdentity } from "@dungle-scrubs/tether-protocol";

/**
 * Pure Session Summary report-to-publication policy.
 *
 * This Module owns fail-closed candidate qualification and deterministic
 * publication selection. It does not run evals or perform filesystem, model,
 * or network operations.
 */

/** Resource and repeatability measurements for one report candidate. */
export interface SessionSummaryEvalCandidateMetrics {
  readonly p95LatencyMs: number;
  readonly peakMemoryBytes: number;
  readonly repeats: number;
  readonly stability: number;
}

/** Bounded semantic-safety outcome recorded by the report. */
export type SessionSummaryEvalSemanticSafety = "proven" | "unproven";

/** Complete measured candidate entry in a Session Summary report. */
export interface SessionSummaryEvalCandidate {
  readonly hardGatesPassed: boolean;
  readonly identity: SessionSummaryOllamaIdentity;
  readonly metrics: SessionSummaryEvalCandidateMetrics;
  readonly outputSchemaVersion: string;
  readonly promptVersion: string;
  readonly semanticSafety: SessionSummaryEvalSemanticSafety;
}

/** Qualification thresholds captured with a Session Summary report. */
export interface SessionSummaryEvalThresholds {
  readonly maximumP95LatencyMs: number;
  readonly maximumPeakMemoryBytes: number;
  readonly minimumRepeats: number;
  readonly minimumStability: number;
}

/** Complete report input accepted by publication selection. */
export interface SessionSummaryEvalReport {
  readonly candidates: readonly SessionSummaryEvalCandidate[];
  readonly corpusVersion: string;
  readonly generatedAt: string;
  readonly ollamaVersion: string;
  readonly reportVersion: string;
  readonly thresholds: SessionSummaryEvalThresholds;
}

/** Fully pinned candidate configuration safe to publish. */
export interface SessionSummaryPublicationCandidate {
  readonly identity: SessionSummaryOllamaIdentity;
  readonly outputSchemaVersion: string;
  readonly promptVersion: string;
}

/** Bounded reason that no report candidate may be published. */
export type SessionSummaryPublicationDisabledReason =
  | "candidate_identity_invalid"
  | "hard_gates_not_passed"
  | "insufficient_repeats"
  | "invalid_report"
  | "invalid_resource_metrics"
  | "invalid_thresholds"
  | "latency_threshold_exceeded"
  | "memory_threshold_exceeded"
  | "no_candidates"
  | "semantic_safety_unproven"
  | "stability_below_threshold";

/** Discriminated publication selection derived from one report. */
export type SessionSummaryPublicationSelection =
  | {
      readonly candidate: SessionSummaryPublicationCandidate;
      readonly status: "enabled";
    }
  | {
      readonly reason: SessionSummaryPublicationDisabledReason;
      readonly status: "disabled";
    };

/** Production-safe A-005 outcome. Summary generation remains disabled. */
export const sessionSummaryPublicationBaseline: SessionSummaryPublicationSelection = {
  reason: "hard_gates_not_passed",
  status: "disabled",
};

type CandidateEvaluation =
  | {
      readonly candidate: SessionSummaryEvalCandidate;
      readonly status: "qualified";
    }
  | {
      readonly reason: SessionSummaryPublicationDisabledReason;
      readonly status: "rejected";
    };

const rejectionProgress: Readonly<Record<SessionSummaryPublicationDisabledReason, number>> = {
  candidate_identity_invalid: 2,
  hard_gates_not_passed: 0,
  insufficient_repeats: 4,
  invalid_report: -1,
  invalid_resource_metrics: 3,
  invalid_thresholds: -1,
  latency_threshold_exceeded: 5,
  memory_threshold_exceeded: 6,
  no_candidates: -1,
  semantic_safety_unproven: 1,
  stability_below_threshold: 7,
};

/** Selects the fastest qualifying fully pinned candidate, or fails closed. */
export function selectSessionSummaryPublication(
  report: unknown,
): SessionSummaryPublicationSelection {
  if (!isCompleteReportEnvelope(report)) {
    return { reason: "invalid_report", status: "disabled" };
  }
  if (report.candidates.length === 0) {
    return { reason: "no_candidates", status: "disabled" };
  }
  if (!isValidThresholds(report.thresholds)) {
    return { reason: "invalid_thresholds", status: "disabled" };
  }

  const evaluations = (report.candidates as readonly unknown[]).map((candidate) =>
    evaluateCandidate(candidate, report.thresholds),
  );
  const qualified = evaluations
    .filter(
      (evaluation): evaluation is Extract<CandidateEvaluation, { readonly status: "qualified" }> =>
        evaluation.status === "qualified",
    )
    .map((evaluation) => evaluation.candidate)
    .sort(compareCandidates);

  const selected = qualified[0];
  if (selected === undefined) {
    const rejected = evaluations.filter(
      (evaluation): evaluation is Extract<CandidateEvaluation, { readonly status: "rejected" }> =>
        evaluation.status === "rejected",
    );
    const furthest = rejected.reduce((current, evaluation) =>
      rejectionProgress[evaluation.reason] > rejectionProgress[current.reason]
        ? evaluation
        : current,
    );
    return { reason: furthest.reason, status: "disabled" };
  }

  return {
    candidate: {
      identity: {
        contextSize: selected.identity.contextSize,
        model: selected.identity.model,
        quantization: selected.identity.quantization,
        revision: selected.identity.revision,
        thinkingMode: selected.identity.thinkingMode,
      },
      outputSchemaVersion: selected.outputSchemaVersion,
      promptVersion: selected.promptVersion,
    },
    status: "enabled",
  };
}

/** Recognizes the report envelope required before candidate evaluation. */
function isCompleteReportEnvelope(value: unknown): value is SessionSummaryEvalReport {
  return (
    isRecord(value) &&
    Array.isArray(value.candidates) &&
    isBoundedIdentifier(value.corpusVersion) &&
    isIsoTimestamp(value.generatedAt) &&
    isBoundedIdentifier(value.ollamaVersion) &&
    isBoundedIdentifier(value.reportVersion) &&
    "thresholds" in value
  );
}

/** Applies every fail-closed gate to one untrusted candidate value. */
function evaluateCandidate(
  value: unknown,
  thresholds: SessionSummaryEvalThresholds,
): CandidateEvaluation {
  if (!isRecord(value) || value.hardGatesPassed !== true) {
    return { reason: "hard_gates_not_passed", status: "rejected" };
  }
  if (value.semanticSafety !== "proven") {
    return { reason: "semantic_safety_unproven", status: "rejected" };
  }
  if (
    !isPinnedIdentity(value.identity) ||
    !isBoundedIdentifier(value.outputSchemaVersion) ||
    !isBoundedIdentifier(value.promptVersion)
  ) {
    return { reason: "candidate_identity_invalid", status: "rejected" };
  }
  if (!isValidMetrics(value.metrics)) {
    return { reason: "invalid_resource_metrics", status: "rejected" };
  }

  const candidate: SessionSummaryEvalCandidate = {
    hardGatesPassed: true,
    identity: value.identity,
    metrics: value.metrics,
    outputSchemaVersion: value.outputSchemaVersion,
    promptVersion: value.promptVersion,
    semanticSafety: "proven",
  };
  if (candidate.metrics.repeats < thresholds.minimumRepeats) {
    return { reason: "insufficient_repeats", status: "rejected" };
  }
  if (candidate.metrics.p95LatencyMs > thresholds.maximumP95LatencyMs) {
    return { reason: "latency_threshold_exceeded", status: "rejected" };
  }
  if (candidate.metrics.peakMemoryBytes > thresholds.maximumPeakMemoryBytes) {
    return { reason: "memory_threshold_exceeded", status: "rejected" };
  }
  if (candidate.metrics.stability < thresholds.minimumStability) {
    return { reason: "stability_below_threshold", status: "rejected" };
  }
  return { candidate, status: "qualified" };
}

/** Orders qualified candidates by resources and stable pinned identity. */
function compareCandidates(
  left: SessionSummaryEvalCandidate,
  right: SessionSummaryEvalCandidate,
): number {
  const latencyOrder = left.metrics.p95LatencyMs - right.metrics.p95LatencyMs;
  if (latencyOrder !== 0) {
    return latencyOrder;
  }
  const memoryOrder = left.metrics.peakMemoryBytes - right.metrics.peakMemoryBytes;
  if (memoryOrder !== 0) {
    return memoryOrder;
  }
  return compareStrings(stableCandidateIdentity(left), stableCandidateIdentity(right));
}

/** Serializes every pinned publication field in a fixed order. */
function stableCandidateIdentity(candidate: SessionSummaryEvalCandidate): string {
  return (
    JSON.stringify([
      candidate.identity.contextSize,
      candidate.identity.model,
      candidate.identity.quantization,
      candidate.identity.revision,
      candidate.identity.thinkingMode,
      candidate.outputSchemaVersion,
      candidate.promptVersion,
    ]) ?? "[]"
  );
}

/** Compares strings without process-locale dependence. */
function compareStrings(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

/** Recognizes one bounded non-empty report identifier. */
function isBoundedIdentifier(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256;
}

/** Recognizes one canonical ISO timestamp retained by a report. */
function isIsoTimestamp(value: unknown): value is string {
  if (typeof value !== "string") {
    return false;
  }
  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) && new Date(milliseconds).toISOString() === value;
}

/** Recognizes one complete pinned candidate identity. */
function isPinnedIdentity(value: unknown): value is SessionSummaryOllamaIdentity {
  const parsed = sessionSummaryOllamaIdentitySchema.safeParse(value);
  return parsed.success && /^sha256:[0-9a-f]{64}$/u.test(parsed.data.revision);
}

/** Recognizes one valid resource and repeatability measurement set. */
function isValidMetrics(value: unknown): value is SessionSummaryEvalCandidateMetrics {
  return (
    isRecord(value) &&
    isPositiveFiniteNumber(value.p95LatencyMs) &&
    Number.isSafeInteger(value.peakMemoryBytes) &&
    typeof value.peakMemoryBytes === "number" &&
    value.peakMemoryBytes > 0 &&
    Number.isSafeInteger(value.repeats) &&
    typeof value.repeats === "number" &&
    value.repeats > 0 &&
    isUnitInterval(value.stability)
  );
}

/** Recognizes a safe, internally coherent threshold set. */
function isValidThresholds(value: unknown): value is SessionSummaryEvalThresholds {
  return (
    isRecord(value) &&
    isPositiveFiniteNumber(value.maximumP95LatencyMs) &&
    Number.isSafeInteger(value.maximumPeakMemoryBytes) &&
    typeof value.maximumPeakMemoryBytes === "number" &&
    value.maximumPeakMemoryBytes > 0 &&
    Number.isSafeInteger(value.minimumRepeats) &&
    typeof value.minimumRepeats === "number" &&
    value.minimumRepeats > 0 &&
    isUnitInterval(value.minimumStability)
  );
}

/** Recognizes one finite number above zero. */
function isPositiveFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

/** Narrows an unknown object without admitting mutable value types. */
function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null;
}

/** Recognizes one finite ratio in the inclusive unit interval. */
function isUnitInterval(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1;
}
