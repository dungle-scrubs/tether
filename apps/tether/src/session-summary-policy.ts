import type {
  SessionSummaryCandidateSubmission,
  SessionSummaryGenerationJob,
  SessionSummaryRange,
} from "@dungle-scrubs/tether-protocol";

/**
 * Pure Session Summary range and publication policy.
 *
 * This Module owns sequence, identity, and publication decisions. It does not
 * issue SQL, acquire locks, authenticate callers, or persist lifecycle state.
 */

/** Published summary head relevant to one session and budget class. */
export interface SessionSummaryPublishedHead {
  readonly coversSeqFrom: number;
  readonly coversSeqTo: number;
  readonly summaryId: string;
}

/** Input for selecting one bounded contiguous generation range. */
export interface SelectSessionSummaryRangeInput {
  readonly maxEventCount: number;
  readonly publishedHead: SessionSummaryPublishedHead | null;
  readonly streamEndSeq: number;
  readonly streamStartSeq: number;
}

/** Bounded range-selection outcome. */
export type SessionSummaryRangeSelection =
  | { readonly status: "caught_up" }
  | { readonly range: SessionSummaryRange; readonly status: "selected" };

/** Typed policy violation returned before persistence is allowed. */
export class SessionSummaryPolicyError extends Error {
  constructor(
    readonly code:
      | "candidate_identity_mismatch"
      | "gap"
      | "overlap"
      | "stale_job"
      | "unsafe_sequence",
    message: string,
  ) {
    super(message);
    this.name = "SessionSummaryPolicyError";
  }
}

/** Selects the next bounded range from stream start or the active head. */
export function selectSessionSummaryRange(
  input: SelectSessionSummaryRangeInput,
): SessionSummaryRangeSelection {
  assertSafePositiveSequence(input.maxEventCount, "maxEventCount");
  assertSafePositiveSequence(input.streamStartSeq, "streamStartSeq");
  assertSafePositiveSequence(input.streamEndSeq, "streamEndSeq");
  if (input.streamStartSeq > input.streamEndSeq) {
    throw new SessionSummaryPolicyError(
      "unsafe_sequence",
      "streamStartSeq must not exceed streamEndSeq",
    );
  }
  if (input.publishedHead !== null) {
    assertSafeRange(input.publishedHead);
  }
  const from = input.publishedHead
    ? addSafeSequence(input.publishedHead.coversSeqTo, 1)
    : input.streamStartSeq;
  if (from > input.streamEndSeq) {
    return { status: "caught_up" };
  }
  const boundedTo = addSafeSequence(from, input.maxEventCount - 1);
  return {
    range: { from, to: Math.min(input.streamEndSeq, boundedTo) },
    status: "selected",
  };
}

/** Input for validating immutable worker output against its selected job. */
export interface ValidateSessionSummaryCandidateInput {
  readonly currentPublishedHead: SessionSummaryPublishedHead | null;
  readonly job: SessionSummaryGenerationJob;
  readonly submission: SessionSummaryCandidateSubmission;
}

/**
 * Verifies that a worker submitted exactly the selected job identity and that
 * the publication head has not changed since range selection.
 */
export function validateSessionSummaryCandidate(input: ValidateSessionSummaryCandidateInput): void {
  const { job, submission } = input;
  if (
    submission.sessionId !== job.sessionId ||
    submission.summaryId !== job.summaryId ||
    submission.taskId !== job.taskId ||
    submission.range.from !== job.range.from ||
    submission.range.to !== job.range.to ||
    !sameOllamaIdentity(submission.ollama, job.ollama) ||
    !sameSourceMetadata(submission.source, job.source)
  ) {
    throw new SessionSummaryPolicyError(
      "candidate_identity_mismatch",
      "candidate identity does not match the selected generation job",
    );
  }
  const expected = job.expectedPrevious;
  const current = input.currentPublishedHead;
  if (
    (expected.summaryId === null && current !== null) ||
    (expected.summaryId !== null &&
      (current === null ||
        current.summaryId !== expected.summaryId ||
        current.coversSeqTo !== expected.coversSeqTo))
  ) {
    throw new SessionSummaryPolicyError(
      "stale_job",
      "published summary head changed after range selection",
    );
  }
}

/** Input for deciding whether one validated candidate can become active. */
export interface DecideSessionSummaryPublicationInput {
  readonly candidateRange: SessionSummaryRange;
  readonly publishedHead: SessionSummaryPublishedHead | null;
  readonly streamStartSeq: number;
}

/** Successful publication decision and optional summary it replaces. */
export interface SessionSummaryPublicationDecision {
  readonly status: "publish";
  readonly supersedeSummaryId: string | null;
}

/**
 * Requires a first candidate to begin at stream start and each replacement to
 * preserve that start while extending cumulative coverage beyond the head.
 */
export function decideSessionSummaryPublication(
  input: DecideSessionSummaryPublicationInput,
): SessionSummaryPublicationDecision {
  assertSafePositiveSequence(input.candidateRange.from, "candidateRange.from");
  assertSafePositiveSequence(input.candidateRange.to, "candidateRange.to");
  assertSafePositiveSequence(input.streamStartSeq, "streamStartSeq");
  if (input.candidateRange.from > input.candidateRange.to) {
    throw new SessionSummaryPolicyError(
      "unsafe_sequence",
      "candidate range start must not exceed its end",
    );
  }
  if (input.publishedHead !== null) {
    assertSafeRange(input.publishedHead);
  }
  const expectedFrom = input.publishedHead?.coversSeqFrom ?? input.streamStartSeq;
  if (input.candidateRange.from > expectedFrom) {
    throw new SessionSummaryPolicyError("gap", "candidate leaves a publication gap");
  }
  if (input.candidateRange.from < expectedFrom) {
    throw new SessionSummaryPolicyError("overlap", "candidate overlaps published coverage");
  }
  if (input.publishedHead !== null && input.candidateRange.to <= input.publishedHead.coversSeqTo) {
    throw new SessionSummaryPolicyError("overlap", "candidate does not extend published coverage");
  }
  return {
    status: "publish",
    supersedeSummaryId: input.publishedHead?.summaryId ?? null,
  };
}

/** Compares every persisted Ollama candidate-identity field. */
function sameOllamaIdentity(
  left: SessionSummaryCandidateSubmission["ollama"],
  right: SessionSummaryGenerationJob["ollama"],
): boolean {
  return (
    left.contextSize === right.contextSize &&
    left.model === right.model &&
    left.quantization === right.quantization &&
    left.revision === right.revision &&
    left.thinkingMode === right.thinkingMode
  );
}

/** Compares every immutable exact-source identity field. */
function sameSourceMetadata(
  left: SessionSummaryCandidateSubmission["source"],
  right: SessionSummaryGenerationJob["source"],
): boolean {
  return (
    left.eventCount === right.eventCount &&
    left.firstEventId === right.firstEventId &&
    left.lastEventId === right.lastEventId &&
    left.rangeHash === right.rangeHash
  );
}

/** Validates a safe inclusive range. */
function assertSafeRange(range: {
  readonly coversSeqFrom: number;
  readonly coversSeqTo: number;
}): void {
  assertSafePositiveSequence(range.coversSeqFrom, "coversSeqFrom");
  assertSafePositiveSequence(range.coversSeqTo, "coversSeqTo");
  if (range.coversSeqFrom > range.coversSeqTo) {
    throw new SessionSummaryPolicyError(
      "unsafe_sequence",
      "coversSeqFrom must not exceed coversSeqTo",
    );
  }
}

/** Requires one positive JSON-safe sequence number. */
function assertSafePositiveSequence(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new SessionSummaryPolicyError(
      "unsafe_sequence",
      `${field} must be a positive safe integer`,
    );
  }
}

/** Adds a non-negative delta without leaving the JSON safe-integer range. */
function addSafeSequence(value: number, delta: number): number {
  const result = value + delta;
  if (!Number.isSafeInteger(result) || result < 1) {
    throw new SessionSummaryPolicyError("unsafe_sequence", "sequence arithmetic overflowed");
  }
  return result;
}
