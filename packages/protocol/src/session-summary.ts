import { z } from "zod";

/** Maximum sequence number that remains exact across public JSON boundaries. */
export const sessionSummaryMaxSequence = Number.MAX_SAFE_INTEGER;

/** Structured fact categories retained by a Session Summary. */
export const sessionSummaryFactCategories = [
  "approval",
  "decision",
  "other",
  "participant",
  "task",
  "user_preference",
] as const;

/** Supported Ollama thinking-mode configurations retained with a candidate. */
export const sessionSummaryThinkingModes = [
  "disabled",
  "enabled",
  "high",
  "low",
  "medium",
] as const;

/** Bounded failure codes shared by generation workers and Tether. */
export const sessionSummaryFailureCodes = [
  "cancelled",
  "deadline_exceeded",
  "generation_unavailable",
  "identity_mismatch",
  "input_too_large",
  "invalid_output",
  "poison_range",
  "publication_conflict",
  "stale_job",
  "unsafe_sequence",
] as const;

/** One source-grounded fact in validated structured summary content. */
export interface SessionSummaryFact {
  readonly category: (typeof sessionSummaryFactCategories)[number];
  readonly sourceEventIds: readonly string[];
  readonly statement: string;
  readonly subjectIds: readonly string[];
}

/** Structured summary output shared by workers, persistence, and context consumers. */
export interface SessionSummaryContent {
  readonly facts: readonly SessionSummaryFact[];
  readonly headline: string;
  readonly narrative: string;
  readonly openQuestions: readonly string[];
}

/** Complete Ollama runtime and model identity used for one generated candidate. */
export interface SessionSummaryOllamaIdentity {
  readonly contextSize: number;
  readonly model: string;
  readonly quantization: string;
  readonly revision: string;
  readonly thinkingMode: (typeof sessionSummaryThinkingModes)[number];
}

/** Service-independent producer identity retained with generated output. */
export interface SessionSummaryProducerIdentity {
  readonly id: string;
  readonly version: string;
}

/** Integrity metadata for structured summary output. */
export interface SessionSummaryIntegrity {
  readonly algorithm: "sha256";
  readonly hash: string;
}

/** Exact source-range identity retained independently from summary text. */
export interface SessionSummarySourceMetadata {
  readonly eventCount: number;
  readonly firstEventId: string;
  readonly lastEventId: string;
  readonly rangeHash: string;
}

/** Bounded terminal or quarantine failure metadata. */
export interface SessionSummaryFailure {
  readonly attempt: number;
  readonly code: (typeof sessionSummaryFailureCodes)[number];
  readonly message: string;
  readonly retryable: boolean;
}

/** Durable Session Summary record shared across Tether protocol boundaries. */
export interface SessionSummaryRecord {
  readonly budgetClass: string;
  readonly content: SessionSummaryContent | null;
  readonly coversSeqFrom: number;
  readonly coversSeqTo: number;
  readonly createdAt: string;
  readonly failure: SessionSummaryFailure | null;
  readonly generationTaskId: string;
  readonly integrity: SessionSummaryIntegrity | null;
  readonly ollama: SessionSummaryOllamaIdentity;
  readonly outputSchemaVersion: string;
  readonly producer: SessionSummaryProducerIdentity;
  readonly promptVersion: string;
  readonly publishedAt: string | null;
  readonly quarantinedAt: string | null;
  readonly sessionId: string;
  readonly source: SessionSummarySourceMetadata;
  readonly summaryId: string;
  readonly supersededAt: string | null;
  readonly validatedAt: string | null;
}

/** Exact inclusive Session Event range selected by Tether. */
export interface SessionSummaryRange {
  readonly from: number;
  readonly to: number;
}

/** Publication head observed while Tether selected a generation range. */
export interface SessionSummaryExpectedPrevious {
  readonly coversSeqTo: number | null;
  readonly summaryId: string | null;
}

/** Validated cumulative summary supplied as context for the next raw suffix. */
export interface SessionSummaryPrevious {
  readonly content: SessionSummaryContent;
  readonly coversSeqFrom: number;
  readonly coversSeqTo: number;
  readonly summaryId: string;
}

/** Service-owned task input consumed by an external Session Summary worker. */
export interface SessionSummaryGenerationJob {
  readonly budgetClass: string;
  readonly deadlineAt: string;
  readonly expectedPrevious: SessionSummaryExpectedPrevious;
  readonly inputLimitBytes: number;
  readonly kind: "session_summary.generate.v1";
  readonly ollama: SessionSummaryOllamaIdentity;
  readonly outputLimitBytes: number;
  readonly outputSchemaVersion: string;
  readonly previousSummary: SessionSummaryPrevious | null;
  readonly producer: SessionSummaryProducerIdentity;
  readonly promptVersion: string;
  readonly range: SessionSummaryRange;
  readonly sessionId: string;
  readonly source: SessionSummarySourceMetadata;
  readonly summaryId: string;
  readonly taskId: string;
}

/** Authenticated worker submission for one claimed Session Summary task. */
export interface SessionSummaryCandidateSubmission {
  readonly claimantId: string;
  readonly content: SessionSummaryContent;
  readonly controlEpoch: number;
  readonly instanceId: string;
  readonly integrity: SessionSummaryIntegrity;
  readonly kind: "session_summary.candidate.v1";
  readonly ollama: SessionSummaryOllamaIdentity;
  readonly range: SessionSummaryRange;
  readonly sessionId: string;
  readonly source: SessionSummarySourceMetadata;
  readonly summaryId: string;
  readonly taskId: string;
}

/** Derived lifecycle state exposed by safe Session Summary inspection. */
export type SessionSummaryInspectionStatus =
  | "candidate"
  | "published"
  | "quarantined"
  | "superseded"
  | "validated";

/** Content-free Session Summary inspection record safe for operator diagnostics. */
export interface SessionSummaryInspection {
  readonly active: boolean;
  readonly budgetClass: string;
  readonly coversSeqFrom: number;
  readonly coversSeqTo: number;
  readonly failure: SessionSummaryFailure | null;
  readonly generationTaskId: string;
  readonly ollama: SessionSummaryOllamaIdentity;
  readonly outputSchemaVersion: string;
  readonly producer: SessionSummaryProducerIdentity;
  readonly promptVersion: string;
  readonly publishedAt: string | null;
  readonly quarantinedAt: string | null;
  readonly sessionId: string;
  readonly source: SessionSummarySourceMetadata;
  readonly status: SessionSummaryInspectionStatus;
  readonly summaryId: string;
  readonly supersededAt: string | null;
  readonly validatedAt: string | null;
}

const boundedIdentifierSchema = z.string().min(1).max(256);
const sha256Schema = z.string().regex(/^[a-f0-9]{64}$/u);
const sequenceSchema = z.number().int().positive().max(sessionSummaryMaxSequence);
const boundedByteLimitSchema = z
  .number()
  .int()
  .positive()
  .max(16 * 1_024 * 1_024);

/** Runtime validator for one source-grounded structured fact. */
export const sessionSummaryFactSchema = z.strictObject({
  category: z.enum(sessionSummaryFactCategories),
  sourceEventIds: z.array(boundedIdentifierSchema).max(64),
  statement: z.string().min(1).max(1_024),
  subjectIds: z.array(boundedIdentifierSchema).max(64),
});

/** Runtime validator for bounded structured Session Summary content. */
export const sessionSummaryContentSchema = z.strictObject({
  facts: z.array(sessionSummaryFactSchema).max(256),
  headline: z.string().min(1).max(256),
  narrative: z.string().min(1).max(16_384),
  openQuestions: z.array(z.string().min(1).max(1_024)).max(64),
});

/** Runtime validator for a complete Ollama candidate identity. */
export const sessionSummaryOllamaIdentitySchema = z.strictObject({
  contextSize: z.number().int().positive().max(1_048_576),
  model: boundedIdentifierSchema,
  quantization: boundedIdentifierSchema,
  revision: boundedIdentifierSchema,
  thinkingMode: z.enum(sessionSummaryThinkingModes),
});

/** Runtime validator for a Session Summary producer identity. */
export const sessionSummaryProducerIdentitySchema = z.strictObject({
  id: boundedIdentifierSchema,
  version: boundedIdentifierSchema,
});

/** Runtime validator for structured-output integrity metadata. */
export const sessionSummaryIntegritySchema = z.strictObject({
  algorithm: z.literal("sha256"),
  hash: sha256Schema,
});

/** Runtime validator for exact source-range metadata. */
export const sessionSummarySourceMetadataSchema = z.strictObject({
  eventCount: z.number().int().positive().max(sessionSummaryMaxSequence),
  firstEventId: boundedIdentifierSchema,
  lastEventId: boundedIdentifierSchema,
  rangeHash: sha256Schema,
});

/** Runtime validator for bounded summary failure metadata. */
export const sessionSummaryFailureSchema = z.strictObject({
  attempt: z.number().int().positive().max(10),
  code: z.enum(sessionSummaryFailureCodes),
  message: z.string().min(1).max(512),
  retryable: z.boolean(),
});

/** Runtime validator for durable Session Summary records. */
export const sessionSummaryRecordSchema = z
  .strictObject({
    budgetClass: z.string().regex(/^[a-z0-9][a-z0-9._-]{0,63}$/u),
    content: sessionSummaryContentSchema.nullable(),
    coversSeqFrom: sequenceSchema,
    coversSeqTo: sequenceSchema,
    createdAt: z.iso.datetime(),
    failure: sessionSummaryFailureSchema.nullable(),
    generationTaskId: boundedIdentifierSchema,
    integrity: sessionSummaryIntegritySchema.nullable(),
    ollama: sessionSummaryOllamaIdentitySchema,
    outputSchemaVersion: boundedIdentifierSchema,
    producer: sessionSummaryProducerIdentitySchema,
    promptVersion: boundedIdentifierSchema,
    publishedAt: z.iso.datetime().nullable(),
    quarantinedAt: z.iso.datetime().nullable(),
    sessionId: boundedIdentifierSchema,
    source: sessionSummarySourceMetadataSchema,
    summaryId: boundedIdentifierSchema,
    supersededAt: z.iso.datetime().nullable(),
    validatedAt: z.iso.datetime().nullable(),
  })
  .refine((record) => record.coversSeqFrom <= record.coversSeqTo, {
    message: "coversSeqFrom must not exceed coversSeqTo",
    path: ["coversSeqFrom"],
  });

/** Runtime validator for an exact inclusive summary source range. */
export const sessionSummaryRangeSchema = z
  .strictObject({
    from: sequenceSchema,
    to: sequenceSchema,
  })
  .refine((range) => range.from <= range.to, {
    message: "range.from must not exceed range.to",
    path: ["from"],
  });

/** Runtime validator for the publication head captured during range selection. */
export const sessionSummaryExpectedPreviousSchema = z
  .strictObject({
    coversSeqTo: sequenceSchema.nullable(),
    summaryId: boundedIdentifierSchema.nullable(),
  })
  .refine(
    (previous) => (previous.coversSeqTo === null) === (previous.summaryId === null),
    "expectedPrevious fields must both be null or both be present",
  );

/** Runtime validator for cumulative summary context supplied to a generation job. */
export const sessionSummaryPreviousSchema = z
  .strictObject({
    content: sessionSummaryContentSchema,
    coversSeqFrom: sequenceSchema,
    coversSeqTo: sequenceSchema,
    summaryId: boundedIdentifierSchema,
  })
  .refine((summary) => summary.coversSeqFrom <= summary.coversSeqTo, {
    message: "coversSeqFrom must not exceed coversSeqTo",
    path: ["coversSeqFrom"],
  });

/** Runtime validator for service-owned Session Summary generation task input. */
export const sessionSummaryGenerationJobSchema = z.strictObject({
  budgetClass: z.string().regex(/^[a-z0-9][a-z0-9._-]{0,63}$/u),
  deadlineAt: z.iso.datetime(),
  expectedPrevious: sessionSummaryExpectedPreviousSchema,
  inputLimitBytes: boundedByteLimitSchema,
  kind: z.literal("session_summary.generate.v1"),
  ollama: sessionSummaryOllamaIdentitySchema,
  outputLimitBytes: boundedByteLimitSchema,
  outputSchemaVersion: boundedIdentifierSchema,
  previousSummary: sessionSummaryPreviousSchema.nullable(),
  producer: sessionSummaryProducerIdentitySchema,
  promptVersion: boundedIdentifierSchema,
  range: sessionSummaryRangeSchema,
  sessionId: boundedIdentifierSchema,
  source: sessionSummarySourceMetadataSchema,
  summaryId: boundedIdentifierSchema,
  taskId: boundedIdentifierSchema,
});

/** Runtime validator for an authenticated and fenced summary candidate submission. */
export const sessionSummaryCandidateSubmissionSchema = z.strictObject({
  claimantId: boundedIdentifierSchema,
  content: sessionSummaryContentSchema,
  controlEpoch: sequenceSchema,
  instanceId: boundedIdentifierSchema,
  integrity: sessionSummaryIntegritySchema,
  kind: z.literal("session_summary.candidate.v1"),
  ollama: sessionSummaryOllamaIdentitySchema,
  range: sessionSummaryRangeSchema,
  sessionId: boundedIdentifierSchema,
  source: sessionSummarySourceMetadataSchema,
  summaryId: boundedIdentifierSchema,
  taskId: boundedIdentifierSchema,
});

/** Runtime validator for content-free Session Summary inspection records. */
export const sessionSummaryInspectionSchema = z
  .strictObject({
    active: z.boolean(),
    budgetClass: z.string().regex(/^[a-z0-9][a-z0-9._-]{0,63}$/u),
    coversSeqFrom: sequenceSchema,
    coversSeqTo: sequenceSchema,
    failure: sessionSummaryFailureSchema.nullable(),
    generationTaskId: boundedIdentifierSchema,
    ollama: sessionSummaryOllamaIdentitySchema,
    outputSchemaVersion: boundedIdentifierSchema,
    producer: sessionSummaryProducerIdentitySchema,
    promptVersion: boundedIdentifierSchema,
    publishedAt: z.iso.datetime().nullable(),
    quarantinedAt: z.iso.datetime().nullable(),
    sessionId: boundedIdentifierSchema,
    source: sessionSummarySourceMetadataSchema,
    status: z.enum(["candidate", "published", "quarantined", "superseded", "validated"]),
    summaryId: boundedIdentifierSchema,
    supersededAt: z.iso.datetime().nullable(),
    validatedAt: z.iso.datetime().nullable(),
  })
  .refine((record) => record.coversSeqFrom <= record.coversSeqTo, {
    message: "coversSeqFrom must not exceed coversSeqTo",
    path: ["coversSeqFrom"],
  });

/**
 * Serializes structured content canonically for integrity hashing by workers
 * and Tether.
 * Object keys are sorted recursively so serialization order cannot change the
 * digest input for equivalent structured output.
 */
export function canonicalizeSessionSummaryContent(content: SessionSummaryContent): string {
  return canonicalJson(content);
}

/** Serializes JSON data with recursively sorted object keys. */
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value).sort(([left], [right]) => left.localeCompare(right));
    return `{${entries
      .map(([key, entryValue]) => `${JSON.stringify(key)}:${canonicalJson(entryValue)}`)
      .join(",")}}`;
  }
  const serialized = JSON.stringify(value);
  if (serialized === undefined) {
    throw new Error("Session Summary integrity input must be valid JSON");
  }
  return serialized;
}
