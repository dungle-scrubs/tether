import { createHash } from "node:crypto";

import {
  canonicalizeSessionSummaryContent,
  deriveSessionSummaryCandidateConfigurationId,
  deriveSessionSummaryCorrelationId,
  type SessionSummaryCandidateSubmission,
  type SessionSummaryContent,
  type SessionSummaryFailure,
  type SessionSummaryGenerationJob,
  type SessionSummaryInspection,
  type SessionSummaryOllamaIdentity,
  type SessionSummaryProducerIdentity,
  type SessionSummaryRecord,
  sessionSummaryCandidateSubmissionSchema,
  sessionSummaryFailureSchema,
  sessionSummaryGenerationJobSchema,
  sessionSummaryInspectionSchema,
  sessionSummaryRecordSchema,
  sessionScalabilitySpanNames,
} from "@dungle-scrubs/tether-protocol";
import { SpanStatusCode, trace } from "@opentelemetry/api";
import type pg from "pg";

import { createTaskWithEventOnClient, type TransactionClient } from "./db.js";
import {
  decideSessionSummaryPublication,
  type SessionSummaryPublishedHead,
  selectSessionSummaryRange,
  validateSessionSummaryCandidate,
} from "./session-summary-policy.js";

/**
 * PostgreSQL persistence for the Session Summary lifecycle.
 *
 * This Module owns SQL, transaction boundaries, and session/budget advisory
 * locks. It delegates all range and publication decisions to
 * `session-summary-policy.ts` and owns no policy of its own.
 */

/** Narrow transaction client used by the store and deterministic test fakes. */
export interface SessionSummaryStoreClient {
  readonly query: <TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ) => Promise<{ readonly rows: TRow[] }>;
  readonly release: () => void;
}

/** Narrow connection pool used by Session Summary persistence. */
export interface SessionSummaryStorePool {
  readonly connect: () => Promise<SessionSummaryStoreClient>;
}

/** Injectable atomic collaborators used by deterministic store tests. */
export interface SessionSummaryStoreDependencies {
  readonly createGenerationTask?: (
    client: TransactionClient,
    job: SessionSummaryGenerationJob,
  ) => Promise<void>;
}

/** Fixed generation configuration selected by Tether. */
export interface SelectAndReserveSessionSummaryGenerationInput {
  readonly budgetClass: string;
  readonly deadlineAt: Date;
  readonly inputLimitBytes: number;
  readonly maxEventCount: number;
  readonly ollama: SessionSummaryOllamaIdentity;
  readonly outputLimitBytes: number;
  readonly outputSchemaVersion: string;
  readonly producer: SessionSummaryProducerIdentity;
  readonly promptVersion: string;
  readonly sessionId: string;
  readonly summaryId: string;
  readonly taskId: string;
}

/** Range reservation result for service-owned generation work. */
export type SessionSummaryGenerationReservation =
  | { readonly status: "caught_up" | "in_progress" | "no_events" }
  | { readonly job: SessionSummaryGenerationJob; readonly status: "reserved" };

/** Session Summary persistence interface. */
export interface SessionSummaryStore {
  /** Inserts one authenticated, claimed, fenced, identity-matched candidate. */
  readonly insertCandidate: (
    submission: SessionSummaryCandidateSubmission,
  ) => Promise<{ readonly status: "inserted"; readonly summaryId: string }>;
  /** Reads one content-free protocol inspection record. */
  readonly inspectCandidate: (summaryId: string) => Promise<SessionSummaryInspection | null>;
  /** Quarantines a non-published candidate with bounded failure metadata. */
  readonly quarantineCandidate: (input: {
    readonly failure: SessionSummaryFailure;
    readonly summaryId: string;
  }) => Promise<{ readonly status: "quarantined"; readonly summaryId: string }>;
  /** Reads the single active published summary for one context budget class. */
  readonly readLatestPublished: (
    sessionId: string,
    budgetClass: string,
  ) => Promise<SessionSummaryRecord | null>;
  /** Publishes one validated candidate and supersedes its prior head atomically. */
  readonly publishCandidate: (summaryId: string) => Promise<{
    readonly status: "published";
    readonly summaryId: string;
    readonly supersededSummaryId: string | null;
  }>;
  /** Selects and durably reserves one exact generation range under its policy lock. */
  readonly selectAndReserveGeneration: (
    input: SelectAndReserveSessionSummaryGenerationInput,
  ) => Promise<SessionSummaryGenerationReservation>;
  /** Atomically inserts, validates, and publishes one fenced candidate. */
  readonly submitCandidate: (submission: SessionSummaryCandidateSubmission) => Promise<{
    readonly status: "published";
    readonly summaryId: string;
    readonly supersededSummaryId: string | null;
  }>;
  /** Marks one structurally inserted candidate validated without publishing it. */
  readonly validateCandidate: (
    summaryId: string,
  ) => Promise<{ readonly status: "validated"; readonly summaryId: string }>;
}

interface StreamBoundsRow {
  readonly streamEndSeq: unknown;
  readonly streamStartSeq: unknown;
}

interface SourceEventIdentityRow {
  readonly eventId: string;
  readonly seq: unknown;
}

interface PublishedHeadRow {
  readonly content: SessionSummaryContent | null;
  readonly coversSeqFrom: unknown;
  readonly coversSeqTo: unknown;
  readonly summaryId: string;
}

interface PublishedSummaryHead extends SessionSummaryPublishedHead {
  readonly content: SessionSummaryContent;
}

interface CandidateRow {
  readonly budgetClass: string;
  readonly content: SessionSummaryContent | null;
  readonly coversSeqFrom: unknown;
  readonly coversSeqTo: unknown;
  readonly generationTaskId: string;
  readonly integrityAlgorithm: string | null;
  readonly integrityHash: string | null;
  readonly publishedAt: Date | null;
  readonly quarantinedAt: Date | null;
  readonly sessionId: string;
  readonly summaryId: string;
  readonly supersededAt: Date | null;
  readonly validatedAt: Date | null;
}

interface SummaryTaskFenceRow {
  readonly cancelledAt: Date | null;
  readonly claimActive: boolean;
  readonly claimedBy: string | null;
  readonly completedAt: Date | null;
  readonly failedAt: Date | null;
  readonly input: unknown;
  readonly kind: string;
}

interface ControlFenceRow {
  readonly controlChannel: string;
  readonly epoch: unknown;
  readonly instanceId: string;
  readonly leaseActive: boolean;
}

interface InspectionRow {
  readonly budgetClass: string;
  readonly coversSeqFrom: unknown;
  readonly coversSeqTo: unknown;
  readonly failure: unknown;
  readonly generationTaskId: string;
  readonly ollamaContextSize: number;
  readonly ollamaModel: string;
  readonly ollamaQuantization: string;
  readonly ollamaRevision: string;
  readonly ollamaThinkingMode: string;
  readonly outputSchemaVersion: string;
  readonly producerId: string;
  readonly producerVersion: string;
  readonly promptVersion: string;
  readonly publishedAt: Date | null;
  readonly quarantinedAt: Date | null;
  readonly sessionId: string;
  readonly sourceEventCount: unknown;
  readonly sourceFirstEventId: string;
  readonly sourceLastEventId: string;
  readonly sourceRangeHash: string;
  readonly summaryId: string;
  readonly supersededAt: Date | null;
  readonly validatedAt: Date | null;
}

interface PublishedRecordRow extends InspectionRow {
  readonly content: SessionSummaryContent | null;
  readonly createdAt: Date;
  readonly integrityAlgorithm: string | null;
  readonly integrityHash: string | null;
}

/** Expected candidate persistence failure with a stable route-safe reason. */
export class SessionSummaryStoreError extends Error {
  constructor(
    readonly code:
      | "control_epoch_stale"
      | "deadline_exceeded"
      | "duplicate_submission"
      | "integrity_mismatch"
      | "invalid_state"
      | "persistence_failure"
      | "summary_not_found"
      | "task_not_claimed"
      | "task_not_found"
      | "wrong_claimant"
      | "wrong_task_kind",
    message: string,
    readonly correlationId: string | null = null,
  ) {
    super(message);
    this.name = "SessionSummaryStoreError";
  }

  /** Attaches the bounded cross-boundary correlation id without raw identities. */
  withCorrelationId(correlationId: string): SessionSummaryStoreError {
    return this.correlationId === correlationId
      ? this
      : new SessionSummaryStoreError(this.code, this.message, correlationId);
  }
}

/** Computes the canonical SHA-256 integrity digest for structured output. */
export function computeSessionSummaryIntegrityHash(content: SessionSummaryContent): string {
  return createHash("sha256").update(canonicalizeSessionSummaryContent(content)).digest("hex");
}

/** Creates the PostgreSQL Session Summary store. */
export function createSessionSummaryStore(
  pool: SessionSummaryStorePool,
  dependencies: SessionSummaryStoreDependencies = {},
): SessionSummaryStore {
  const createGenerationTask =
    dependencies.createGenerationTask ??
    (async (client: TransactionClient, job: SessionSummaryGenerationJob): Promise<void> => {
      await createTaskWithEventOnClient(client, {
        eventSourceId: "session-summary-service",
        input: { ...job },
        kind: "session_summary_generation",
        objective: `Generate ${job.budgetClass} Session Summary for events ${job.range.from}-${job.range.to}`,
        sessionId: job.sessionId,
        taskId: job.taskId,
      });
    });
  return {
    insertCandidate: (submission) => insertCandidate(pool, submission),
    inspectCandidate: (summaryId) => inspectCandidate(pool, summaryId),
    publishCandidate: (summaryId) =>
      traceSummaryStoreOperation(
        sessionScalabilitySpanNames.summaryPublish,
        summaryId,
        {},
        () => publishCandidate(pool, summaryId),
        (result) => ({ superseded: result.supersededSummaryId !== null }),
      ),
    quarantineCandidate: (input) => quarantineCandidate(pool, input),
    readLatestPublished: (sessionId, budgetClass) =>
      readLatestPublished(pool, sessionId, budgetClass),
    selectAndReserveGeneration: (input) =>
      traceSummaryStoreOperation(
        sessionScalabilitySpanNames.summaryRangeSelect,
        input.summaryId,
        {
          "summary.budget_class": input.budgetClass,
          "summary.candidate_configuration_id": deriveSessionSummaryCandidateConfigurationId(
            input.ollama,
          ),
        },
        () => selectAndReserveGeneration(pool, input, createGenerationTask),
        (result) => ({
          "summary.range_size":
            result.status === "reserved" ? result.job.range.to - result.job.range.from + 1 : 0,
          "summary.status": result.status,
        }),
      ),
    submitCandidate: (submission) =>
      traceSummaryStoreOperation(
        sessionScalabilitySpanNames.summaryCandidateSubmit,
        submission.summaryId,
        {
          "summary.candidate_configuration_id": deriveSessionSummaryCandidateConfigurationId(
            submission.ollama,
          ),
          "summary.range_size": submission.range.to - submission.range.from + 1,
        },
        () => submitCandidate(pool, submission),
        (result) => ({
          "summary.published": true,
          "summary.superseded": result.supersededSummaryId !== null,
        }),
      ),
    validateCandidate: (summaryId) => validateCandidate(pool, summaryId),
  };
}

/** Reads the current published record without selecting superseded candidates. */
async function readLatestPublished(
  pool: SessionSummaryStorePool,
  sessionId: string,
  budgetClass: string,
): Promise<SessionSummaryRecord | null> {
  const client = await pool.connect();
  try {
    const result = await client.query<PublishedRecordRow>(
      `
        SELECT
          budget_class AS "budgetClass",
          content,
          covers_seq_from AS "coversSeqFrom",
          covers_seq_to AS "coversSeqTo",
          created_at AS "createdAt",
          failure,
          generation_task_id AS "generationTaskId",
          integrity_algorithm AS "integrityAlgorithm",
          integrity_hash AS "integrityHash",
          ollama_context_size AS "ollamaContextSize",
          ollama_model AS "ollamaModel",
          ollama_quantization AS "ollamaQuantization",
          ollama_revision AS "ollamaRevision",
          ollama_thinking_mode AS "ollamaThinkingMode",
          output_schema_version AS "outputSchemaVersion",
          producer_id AS "producerId",
          producer_version AS "producerVersion",
          prompt_version AS "promptVersion",
          published_at AS "publishedAt",
          quarantined_at AS "quarantinedAt",
          session_id AS "sessionId",
          source_event_count AS "sourceEventCount",
          source_first_event_id AS "sourceFirstEventId",
          source_last_event_id AS "sourceLastEventId",
          source_range_hash AS "sourceRangeHash",
          summary_id AS "summaryId",
          superseded_at AS "supersededAt",
          validated_at AS "validatedAt"
        FROM session_summaries
        WHERE session_id = $1
          AND budget_class = $2
          AND published_at IS NOT NULL
          AND superseded_at IS NULL
        LIMIT 1
      `,
      [sessionId, budgetClass],
    );
    const row = result.rows[0];
    if (!row) {
      return null;
    }
    return sessionSummaryRecordSchema.parse({
      budgetClass: row.budgetClass,
      content: row.content,
      coversSeqFrom: parseSafeSequence(row.coversSeqFrom, "coversSeqFrom"),
      coversSeqTo: parseSafeSequence(row.coversSeqTo, "coversSeqTo"),
      createdAt: row.createdAt.toISOString(),
      failure: row.failure,
      generationTaskId: row.generationTaskId,
      integrity:
        row.integrityAlgorithm === null || row.integrityHash === null
          ? null
          : { algorithm: row.integrityAlgorithm, hash: row.integrityHash },
      ollama: {
        contextSize: row.ollamaContextSize,
        model: row.ollamaModel,
        quantization: row.ollamaQuantization,
        revision: row.ollamaRevision,
        thinkingMode: row.ollamaThinkingMode,
      },
      outputSchemaVersion: row.outputSchemaVersion,
      producer: { id: row.producerId, version: row.producerVersion },
      promptVersion: row.promptVersion,
      publishedAt: row.publishedAt?.toISOString() ?? null,
      quarantinedAt: row.quarantinedAt?.toISOString() ?? null,
      sessionId: row.sessionId,
      source: {
        eventCount: parseSafeSequence(row.sourceEventCount, "sourceEventCount"),
        firstEventId: row.sourceFirstEventId,
        lastEventId: row.sourceLastEventId,
        rangeHash: row.sourceRangeHash,
      },
      summaryId: row.summaryId,
      supersededAt: row.supersededAt?.toISOString() ?? null,
      validatedAt: row.validatedAt?.toISOString() ?? null,
    });
  } finally {
    client.release();
  }
}

/** Atomically inserts, validates, and publishes one candidate with replay safety. */
async function submitCandidate(
  pool: SessionSummaryStorePool,
  candidateInput: SessionSummaryCandidateSubmission,
): Promise<{
  readonly status: "published";
  readonly summaryId: string;
  readonly supersededSummaryId: string | null;
}> {
  const submission = sessionSummaryCandidateSubmissionSchema.parse(candidateInput);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const observed = await readCandidate(client, submission.summaryId, false);
    assertCandidateRouteIdentity(observed, submission);
    if (observed.publishedAt !== null && observed.supersededAt === null) {
      const replayTask = await readSummaryTaskFence(
        client,
        submission.sessionId,
        submission.taskId,
      );
      assertTaskOwner(replayTask, submission.claimantId);
      assertSubmittedContent(observed, submission);
      await client.query("COMMIT");
      return {
        status: "published",
        summaryId: submission.summaryId,
        supersededSummaryId: null,
      };
    }

    await assertControlFence(client, submission);
    const task = await readSummaryTaskFence(client, submission.sessionId, submission.taskId);
    assertTaskClaim(task, submission.claimantId);
    await lockSessionBudgetClass(client, observed.sessionId, observed.budgetClass);
    const candidate = await readCandidate(client, submission.summaryId, true);
    assertCandidateRouteIdentity(candidate, submission);
    const jobResult = sessionSummaryGenerationJobSchema.safeParse(task.input);
    if (!jobResult.success) {
      throw new SessionSummaryStoreError(
        "wrong_task_kind",
        "Session Summary task input does not match the protocol generation contract",
      );
    }
    const job = jobResult.data;
    if (candidate.publishedAt !== null && candidate.supersededAt === null) {
      assertSubmittedContent(candidate, submission);
      await client.query("COMMIT");
      return {
        status: "published",
        summaryId: submission.summaryId,
        supersededSummaryId: null,
      };
    }
    if (candidate.quarantinedAt !== null || candidate.supersededAt !== null) {
      throw new SessionSummaryStoreError(
        "invalid_state",
        "Session Summary candidate cannot transition to published",
      );
    }
    const publishedHead = await readPublishedHead(
      client,
      candidate.sessionId,
      candidate.budgetClass,
    );
    validateSessionSummaryCandidate({
      currentPublishedHead: publishedHead,
      job,
      submission,
    });
    const expectedHash = computeSessionSummaryIntegrityHash(submission.content);
    if (submission.integrity.hash !== expectedHash) {
      throw new SessionSummaryStoreError(
        "integrity_mismatch",
        "Session Summary candidate integrity hash does not match structured content",
      );
    }
    if (candidate.content === null) {
      const databaseNow = await readDatabaseNow(client);
      if (Date.parse(job.deadlineAt) <= databaseNow.getTime()) {
        throw new SessionSummaryStoreError(
          "deadline_exceeded",
          "Session Summary generation job deadline elapsed",
        );
      }
      const inserted = await client.query<{ readonly summaryId: string }>(
        `
          UPDATE session_summaries
          SET
            content = $2::jsonb,
            integrity_algorithm = $3,
            integrity_hash = $4,
            validated_at = clock_timestamp()
          WHERE summary_id = $1
            AND content IS NULL
            AND validated_at IS NULL
            AND published_at IS NULL
            AND quarantined_at IS NULL
            AND superseded_at IS NULL
          RETURNING summary_id AS "summaryId"
        `,
        [
          submission.summaryId,
          JSON.stringify(submission.content),
          submission.integrity.algorithm,
          submission.integrity.hash,
        ],
      );
      if (!inserted.rows[0]) {
        throw new SessionSummaryStoreError(
          "duplicate_submission",
          "Session Summary candidate changed before insertion",
        );
      }
    } else {
      assertSubmittedContent(candidate, submission);
      if (candidate.validatedAt === null) {
        const validated = await client.query<{ readonly summaryId: string }>(
          `
            UPDATE session_summaries
            SET validated_at = clock_timestamp()
            WHERE summary_id = $1
              AND content IS NOT NULL
              AND validated_at IS NULL
              AND published_at IS NULL
              AND quarantined_at IS NULL
              AND superseded_at IS NULL
            RETURNING summary_id AS "summaryId"
          `,
          [submission.summaryId],
        );
        if (!validated.rows[0]) {
          throw new SessionSummaryStoreError(
            "invalid_state",
            "Session Summary candidate changed before validation",
          );
        }
      }
    }

    const result = await publishLockedCandidate(client, {
      ...candidate,
      content: submission.content,
      integrityAlgorithm: submission.integrity.algorithm,
      integrityHash: submission.integrity.hash,
      validatedAt: candidate.validatedAt ?? new Date(),
    });
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

/** Reads one complete, content-free inspection record. */
async function inspectCandidate(
  pool: SessionSummaryStorePool,
  summaryId: string,
): Promise<SessionSummaryInspection | null> {
  const client = await pool.connect();
  try {
    const result = await client.query<InspectionRow>(
      `
        SELECT
          budget_class AS "budgetClass",
          covers_seq_from AS "coversSeqFrom",
          covers_seq_to AS "coversSeqTo",
          failure,
          generation_task_id AS "generationTaskId",
          ollama_context_size AS "ollamaContextSize",
          ollama_model AS "ollamaModel",
          ollama_quantization AS "ollamaQuantization",
          ollama_revision AS "ollamaRevision",
          ollama_thinking_mode AS "ollamaThinkingMode",
          output_schema_version AS "outputSchemaVersion",
          producer_id AS "producerId",
          producer_version AS "producerVersion",
          prompt_version AS "promptVersion",
          published_at AS "publishedAt",
          quarantined_at AS "quarantinedAt",
          session_id AS "sessionId",
          source_event_count AS "sourceEventCount",
          source_first_event_id AS "sourceFirstEventId",
          source_last_event_id AS "sourceLastEventId",
          source_range_hash AS "sourceRangeHash",
          summary_id AS "summaryId",
          superseded_at AS "supersededAt",
          validated_at AS "validatedAt"
        FROM session_summaries
        WHERE summary_id = $1
      `,
      [summaryId],
    );
    const row = result.rows[0];
    if (!row) {
      return null;
    }
    return sessionSummaryInspectionSchema.parse({
      active: row.publishedAt !== null && row.supersededAt === null,
      budgetClass: row.budgetClass,
      coversSeqFrom: parseSafeSequence(row.coversSeqFrom, "coversSeqFrom"),
      coversSeqTo: parseSafeSequence(row.coversSeqTo, "coversSeqTo"),
      failure: row.failure,
      generationTaskId: row.generationTaskId,
      ollama: {
        contextSize: row.ollamaContextSize,
        model: row.ollamaModel,
        quantization: row.ollamaQuantization,
        revision: row.ollamaRevision,
        thinkingMode: row.ollamaThinkingMode,
      },
      outputSchemaVersion: row.outputSchemaVersion,
      producer: { id: row.producerId, version: row.producerVersion },
      promptVersion: row.promptVersion,
      publishedAt: row.publishedAt?.toISOString() ?? null,
      quarantinedAt: row.quarantinedAt?.toISOString() ?? null,
      sessionId: row.sessionId,
      source: {
        eventCount: parseSafeSequence(row.sourceEventCount, "sourceEventCount"),
        firstEventId: row.sourceFirstEventId,
        lastEventId: row.sourceLastEventId,
        rangeHash: row.sourceRangeHash,
      },
      status: inspectionStatus(row),
      summaryId: row.summaryId,
      supersededAt: row.supersededAt?.toISOString() ?? null,
      validatedAt: row.validatedAt?.toISOString() ?? null,
    });
  } finally {
    client.release();
  }
}

/** Derives one explicit lifecycle state from timestamp evidence. */
function inspectionStatus(row: InspectionRow): SessionSummaryInspection["status"] {
  if (row.quarantinedAt !== null) {
    return "quarantined";
  }
  if (row.supersededAt !== null) {
    return "superseded";
  }
  if (row.publishedAt !== null) {
    return "published";
  }
  if (row.validatedAt !== null) {
    return "validated";
  }
  return "candidate";
}

/** Serializes publication and active-head supersession under the policy lock. */
async function publishCandidate(
  pool: SessionSummaryStorePool,
  summaryId: string,
): Promise<{
  readonly status: "published";
  readonly summaryId: string;
  readonly supersededSummaryId: string | null;
}> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const observed = await readCandidate(client, summaryId, false);
    await lockSessionBudgetClass(client, observed.sessionId, observed.budgetClass);
    const candidate = await readCandidate(client, summaryId, true);
    const result = await publishLockedCandidate(client, candidate);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

/** Publishes one candidate while the canonical session/budget lock is held. */
async function publishLockedCandidate(
  client: SessionSummaryStoreClient,
  candidate: CandidateRow,
): Promise<{
  readonly status: "published";
  readonly summaryId: string;
  readonly supersededSummaryId: string | null;
}> {
  if (
    candidate.content === null ||
    candidate.validatedAt === null ||
    candidate.publishedAt !== null ||
    candidate.quarantinedAt !== null ||
    candidate.supersededAt !== null
  ) {
    throw new SessionSummaryStoreError(
      "invalid_state",
      "Session Summary candidate cannot transition to published",
    );
  }
  const publishedHead = await readPublishedHead(client, candidate.sessionId, candidate.budgetClass);
  const streamStartResult = await client.query<{ readonly streamStartSeq: unknown }>(
    `SELECT min(seq) AS "streamStartSeq" FROM session_events WHERE session_id = $1`,
    [candidate.sessionId],
  );
  const streamStartValue = streamStartResult.rows[0]?.streamStartSeq;
  if (streamStartValue === null || streamStartValue === undefined) {
    throw new SessionSummaryStoreError(
      "invalid_state",
      "Session Summary source stream no longer exists",
    );
  }
  const decision = decideSessionSummaryPublication({
    candidateRange: {
      from: parseSafeSequence(candidate.coversSeqFrom, "coversSeqFrom"),
      to: parseSafeSequence(candidate.coversSeqTo, "coversSeqTo"),
    },
    publishedHead,
    streamStartSeq: parseSafeSequence(streamStartValue, "streamStartSeq"),
  });
  if (decision.supersedeSummaryId !== null) {
    const superseded = await client.query<{ readonly summaryId: string }>(
      `
          UPDATE session_summaries
          SET superseded_at = clock_timestamp()
          WHERE summary_id = $1
            AND session_id = $2
            AND budget_class = $3
            AND published_at IS NOT NULL
            AND superseded_at IS NULL
          RETURNING summary_id AS "summaryId"
        `,
      [decision.supersedeSummaryId, candidate.sessionId, candidate.budgetClass],
    );
    if (!superseded.rows[0]) {
      throw new SessionSummaryStoreError(
        "invalid_state",
        "Active Session Summary changed during supersession",
      );
    }
  }
  const published = await client.query<{ readonly summaryId: string }>(
    `
        UPDATE session_summaries
        SET published_at = clock_timestamp()
        WHERE summary_id = $1
          AND validated_at IS NOT NULL
          AND published_at IS NULL
          AND quarantined_at IS NULL
          AND superseded_at IS NULL
        RETURNING summary_id AS "summaryId"
      `,
    [candidate.summaryId],
  );
  if (!published.rows[0]) {
    throw new SessionSummaryStoreError(
      "invalid_state",
      "Session Summary candidate changed during publication",
    );
  }
  return {
    status: "published",
    summaryId: candidate.summaryId,
    supersededSummaryId: decision.supersedeSummaryId,
  };
}

/** Persists a bounded failure while preserving the current active summary. */
async function quarantineCandidate(
  pool: SessionSummaryStorePool,
  input: { readonly failure: SessionSummaryFailure; readonly summaryId: string },
): Promise<{ readonly status: "quarantined"; readonly summaryId: string }> {
  const failure = sessionSummaryFailureSchema.parse(input.failure);
  const client = await pool.connect();
  try {
    const result = await client.query<{ readonly summaryId: string }>(
      `
        UPDATE session_summaries
        SET failure = $2::jsonb, quarantined_at = clock_timestamp()
        WHERE summary_id = $1
          AND published_at IS NULL
          AND quarantined_at IS NULL
          AND superseded_at IS NULL
        RETURNING summary_id AS "summaryId"
      `,
      [input.summaryId, JSON.stringify(failure)],
    );
    if (!result.rows[0]) {
      throw new SessionSummaryStoreError(
        "invalid_state",
        "Session Summary candidate cannot transition to quarantined",
      );
    }
    return { status: "quarantined", summaryId: input.summaryId };
  } finally {
    client.release();
  }
}

/** Timestamps structural validation without coupling it to publication. */
async function validateCandidate(
  pool: SessionSummaryStorePool,
  summaryId: string,
): Promise<{ readonly status: "validated"; readonly summaryId: string }> {
  const client = await pool.connect();
  try {
    const result = await client.query<{ readonly summaryId: string }>(
      `
        UPDATE session_summaries
        SET validated_at = clock_timestamp()
        WHERE summary_id = $1
          AND content IS NOT NULL
          AND integrity_algorithm IS NOT NULL
          AND integrity_hash IS NOT NULL
          AND validated_at IS NULL
          AND published_at IS NULL
          AND quarantined_at IS NULL
          AND superseded_at IS NULL
        RETURNING summary_id AS "summaryId"
      `,
      [summaryId],
    );
    if (!result.rows[0]) {
      throw new SessionSummaryStoreError(
        "invalid_state",
        "Session Summary candidate cannot transition to validated",
      );
    }
    return { status: "validated", summaryId };
  } finally {
    client.release();
  }
}

/** Inserts one candidate only after all durable claim and identity fences pass. */
async function insertCandidate(
  pool: SessionSummaryStorePool,
  candidateInput: SessionSummaryCandidateSubmission,
): Promise<{ readonly status: "inserted"; readonly summaryId: string }> {
  const submission = sessionSummaryCandidateSubmissionSchema.parse(candidateInput);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const observed = await readCandidate(client, submission.summaryId, false);
    assertCandidateRouteIdentity(observed, submission);
    await assertControlFence(client, submission);
    const task = await readSummaryTaskFence(client, submission.sessionId, submission.taskId);
    assertTaskClaim(task, submission.claimantId);
    await lockSessionBudgetClass(client, observed.sessionId, observed.budgetClass);
    const candidate = await readCandidate(client, submission.summaryId, true);
    assertCandidateRouteIdentity(candidate, submission);
    if (
      candidate.content !== null ||
      candidate.validatedAt !== null ||
      candidate.publishedAt !== null ||
      candidate.quarantinedAt !== null ||
      candidate.supersededAt !== null
    ) {
      throw new SessionSummaryStoreError(
        "duplicate_submission",
        "Session Summary candidate was already submitted or reached a later lifecycle state",
      );
    }
    const jobResult = sessionSummaryGenerationJobSchema.safeParse(task.input);
    if (!jobResult.success) {
      throw new SessionSummaryStoreError(
        "wrong_task_kind",
        "Session Summary task input does not match the protocol generation contract",
      );
    }
    const job = jobResult.data;
    const publishedHead = await readPublishedHead(
      client,
      candidate.sessionId,
      candidate.budgetClass,
    );
    validateSessionSummaryCandidate({
      currentPublishedHead: publishedHead,
      job,
      submission,
    });
    const databaseNow = await readDatabaseNow(client);
    if (Date.parse(job.deadlineAt) <= databaseNow.getTime()) {
      throw new SessionSummaryStoreError(
        "deadline_exceeded",
        "Session Summary generation job deadline elapsed",
      );
    }
    if (submission.integrity.hash !== computeSessionSummaryIntegrityHash(submission.content)) {
      throw new SessionSummaryStoreError(
        "integrity_mismatch",
        "Session Summary candidate integrity hash does not match structured content",
      );
    }
    const updated = await client.query<{ readonly summaryId: string }>(
      `
        UPDATE session_summaries
        SET content = $2::jsonb, integrity_algorithm = $3, integrity_hash = $4
        WHERE summary_id = $1
          AND content IS NULL
          AND validated_at IS NULL
          AND published_at IS NULL
          AND quarantined_at IS NULL
          AND superseded_at IS NULL
        RETURNING summary_id AS "summaryId"
      `,
      [
        submission.summaryId,
        JSON.stringify(submission.content),
        submission.integrity.algorithm,
        submission.integrity.hash,
      ],
    );
    if (!updated.rows[0]) {
      throw new SessionSummaryStoreError(
        "duplicate_submission",
        "Session Summary candidate changed before insertion",
      );
    }
    await client.query("COMMIT");
    return { status: "inserted", summaryId: submission.summaryId };
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

/** Reads one candidate before or after acquiring its publication-policy lock. */
async function readCandidate(
  client: SessionSummaryStoreClient,
  summaryId: string,
  forUpdate: boolean,
): Promise<CandidateRow> {
  const result = await client.query<CandidateRow>(
    `
      SELECT
        budget_class AS "budgetClass",
        content,
        covers_seq_from AS "coversSeqFrom",
        covers_seq_to AS "coversSeqTo",
        generation_task_id AS "generationTaskId",
        integrity_algorithm AS "integrityAlgorithm",
        integrity_hash AS "integrityHash",
        published_at AS "publishedAt",
        quarantined_at AS "quarantinedAt",
        session_id AS "sessionId",
        summary_id AS "summaryId",
        superseded_at AS "supersededAt",
        validated_at AS "validatedAt"
      FROM session_summaries
      WHERE summary_id = $1
      ${forUpdate ? "FOR UPDATE" : ""}
    `,
    [summaryId],
  );
  const row = result.rows[0];
  if (!row) {
    throw new SessionSummaryStoreError("summary_not_found", "Session Summary was not found");
  }
  return row;
}

/** Rejects route/body identity drift before inspecting task state. */
function assertCandidateRouteIdentity(
  candidate: CandidateRow,
  submission: SessionSummaryCandidateSubmission,
): void {
  if (
    candidate.sessionId !== submission.sessionId ||
    candidate.summaryId !== submission.summaryId ||
    candidate.generationTaskId !== submission.taskId
  ) {
    throw new SessionSummaryStoreError(
      "summary_not_found",
      "Session Summary route and durable identity do not match",
    );
  }
}

/** Requires a retry to carry byte-equivalent structured content and integrity. */
function assertSubmittedContent(
  candidate: CandidateRow,
  submission: SessionSummaryCandidateSubmission,
): void {
  if (
    candidate.content === null ||
    canonicalizeSessionSummaryContent(candidate.content) !==
      canonicalizeSessionSummaryContent(submission.content) ||
    candidate.integrityAlgorithm !== submission.integrity.algorithm ||
    candidate.integrityHash !== submission.integrity.hash
  ) {
    throw new SessionSummaryStoreError(
      "duplicate_submission",
      "Session Summary retry does not match the durable candidate",
    );
  }
}

/** Locks and reads the task claim that authorizes candidate insertion. */
async function readSummaryTaskFence(
  client: SessionSummaryStoreClient,
  sessionId: string,
  taskId: string,
): Promise<SummaryTaskFenceRow> {
  const result = await client.query<SummaryTaskFenceRow>(
    `
      SELECT
        cancelled_at AS "cancelledAt",
        claim_expires_at > clock_timestamp() AS "claimActive",
        claimed_by AS "claimedBy",
        completed_at AS "completedAt",
        failed_at AS "failedAt",
        input,
        kind
      FROM tasks
      WHERE session_id = $1 AND task_id = $2
      FOR UPDATE
    `,
    [sessionId, taskId],
  );
  const row = result.rows[0];
  if (!row) {
    throw new SessionSummaryStoreError("task_not_found", "Summary generation task was not found");
  }
  return row;
}

/** Requires one active nonterminal claim owned by the authenticated worker. */
function assertTaskClaim(task: SummaryTaskFenceRow, claimantId: string): void {
  assertTaskOwner(task, claimantId);
  if (
    !task.claimActive ||
    task.cancelledAt !== null ||
    task.completedAt !== null ||
    task.failedAt !== null
  ) {
    throw new SessionSummaryStoreError(
      "task_not_claimed",
      "Summary generation task claim is inactive or terminal",
    );
  }
}

/** Requires the summary task kind and its durable claimant identity. */
function assertTaskOwner(task: SummaryTaskFenceRow, claimantId: string): void {
  if (task.kind !== "session_summary_generation") {
    throw new SessionSummaryStoreError(
      "wrong_task_kind",
      "Claimed task is not a Session Summary generation task",
    );
  }
  if (task.claimedBy !== claimantId) {
    throw new SessionSummaryStoreError(
      "wrong_claimant",
      "Summary generation task is claimed by another participant",
    );
  }
}

/** Requires the current durable participant Control Epoch in the candidate transaction. */
async function assertControlFence(
  client: SessionSummaryStoreClient,
  submission: SessionSummaryCandidateSubmission,
): Promise<void> {
  const result = await client.query<ControlFenceRow>(
    `
      SELECT
        control_channel AS "controlChannel",
        epoch,
        instance_id AS "instanceId",
        lease_expires_at > clock_timestamp() AS "leaseActive"
      FROM participant_control_leases
      WHERE session_id = $1
        AND participant_id = $2
        AND released_at IS NULL
        AND superseded_at IS NULL
      ORDER BY lease_expires_at DESC, claimed_at DESC, instance_id
      FOR UPDATE
    `,
    [submission.sessionId, submission.claimantId],
  );
  const current = result.rows[0];
  if (
    !current ||
    (current.controlChannel !== "rest" && current.controlChannel !== "ws") ||
    current.instanceId !== submission.instanceId ||
    !current.leaseActive ||
    parseSafeSequence(current.epoch, "controlEpoch") !== submission.controlEpoch
  ) {
    throw new SessionSummaryStoreError(
      "control_epoch_stale",
      "Session Summary candidate Control Epoch is stale",
    );
  }
}

/** Reads the database clock after all lock waits complete. */
async function readDatabaseNow(client: SessionSummaryStoreClient): Promise<Date> {
  const result = await client.query<{ readonly now: Date }>(`SELECT clock_timestamp() AS "now"`);
  const now = result.rows[0]?.now;
  if (!(now instanceof Date)) {
    throw new Error("PostgreSQL did not return a wall-clock timestamp");
  }
  return now;
}

/** Selects source coverage and inserts its candidate row in one locked transaction. */
async function selectAndReserveGeneration(
  pool: SessionSummaryStorePool,
  input: SelectAndReserveSessionSummaryGenerationInput,
  createGenerationTask: (
    client: TransactionClient,
    job: SessionSummaryGenerationJob,
  ) => Promise<void>,
): Promise<SessionSummaryGenerationReservation> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await lockSessionBudgetClass(client, input.sessionId, input.budgetClass);
    const pending = await client.query<{ readonly summaryId: string }>(
      `
        SELECT summary_id AS "summaryId"
        FROM session_summaries
        WHERE session_id = $1
          AND budget_class = $2
          AND published_at IS NULL
          AND quarantined_at IS NULL
          AND superseded_at IS NULL
        ORDER BY created_at, summary_id
        LIMIT 1
      `,
      [input.sessionId, input.budgetClass],
    );
    if (pending.rows.length > 0) {
      await client.query("COMMIT");
      return { status: "in_progress" };
    }
    const publishedHead = await readPublishedHead(client, input.sessionId, input.budgetClass);
    const bounds = await client.query<StreamBoundsRow>(
      `
        SELECT min(seq) AS "streamStartSeq", max(seq) AS "streamEndSeq"
        FROM session_events
        WHERE session_id = $1
      `,
      [input.sessionId],
    );
    const boundsRow = bounds.rows[0];
    if (boundsRow?.streamStartSeq === null || boundsRow?.streamEndSeq === null || !boundsRow) {
      await client.query("COMMIT");
      return { status: "no_events" };
    }
    const selection = selectSessionSummaryRange({
      maxEventCount: input.maxEventCount,
      publishedHead,
      streamEndSeq: parseSafeSequence(boundsRow.streamEndSeq, "streamEndSeq"),
      streamStartSeq: parseSafeSequence(boundsRow.streamStartSeq, "streamStartSeq"),
    });
    if (selection.status === "caught_up") {
      await client.query("COMMIT");
      return selection;
    }
    const sourceEvents = await client.query<SourceEventIdentityRow>(
      `
        SELECT event_id AS "eventId", seq
        FROM session_events
        WHERE session_id = $1 AND seq BETWEEN $2 AND $3
        ORDER BY seq
      `,
      [input.sessionId, selection.range.from, selection.range.to],
    );
    const source = buildSourceMetadata(sourceEvents.rows, selection.range);
    const job = sessionSummaryGenerationJobSchema.parse({
      budgetClass: input.budgetClass,
      deadlineAt: input.deadlineAt.toISOString(),
      expectedPrevious: {
        coversSeqTo: publishedHead?.coversSeqTo ?? null,
        summaryId: publishedHead?.summaryId ?? null,
      },
      inputLimitBytes: input.inputLimitBytes,
      kind: "session_summary.generate.v1",
      ollama: input.ollama,
      outputLimitBytes: input.outputLimitBytes,
      outputSchemaVersion: input.outputSchemaVersion,
      previousSummary: publishedHead
        ? {
            content: publishedHead.content,
            coversSeqFrom: publishedHead.coversSeqFrom,
            coversSeqTo: publishedHead.coversSeqTo,
            summaryId: publishedHead.summaryId,
          }
        : null,
      producer: input.producer,
      promptVersion: input.promptVersion,
      range: selection.range,
      sessionId: input.sessionId,
      source,
      summaryId: input.summaryId,
      taskId: input.taskId,
    });
    await insertReservedSummary(client, job);
    await traceSummaryStoreOperation(
      sessionScalabilitySpanNames.summaryJobCreate,
      job.summaryId,
      {
        "summary.budget_class": job.budgetClass,
        "summary.candidate_configuration_id": deriveSessionSummaryCandidateConfigurationId(
          job.ollama,
        ),
        "summary.range_size": job.range.to - job.range.from + 1,
      },
      () => createGenerationTask(client, job),
      () => ({ "summary.created": true }),
    );
    await client.query("COMMIT");
    return { job, status: "reserved" };
  } catch (error) {
    await rollbackQuietly(client);
    throw error;
  } finally {
    client.release();
  }
}

async function traceSummaryStoreOperation<TValue>(
  name: string,
  summaryId: string,
  attributes: Readonly<Record<string, boolean | number | string>>,
  action: () => Promise<TValue>,
  summarize: (value: TValue) => Readonly<Record<string, boolean | number | string>>,
): Promise<TValue> {
  const correlationId = deriveSessionSummaryCorrelationId(summaryId);
  return trace.getTracer("tether-session-summary").startActiveSpan(
    name,
    {
      attributes: {
        ...attributes,
        "summary.correlation_id": correlationId,
      },
    },
    async (span) => {
      try {
        const value = await action();
        span.setAttributes(summarize(value));
        span.setStatus({ code: SpanStatusCode.OK });
        return value;
      } catch (error) {
        span.setAttribute(
          "summary.failure_code",
          error instanceof SessionSummaryStoreError ? error.code : "persistence_failure",
        );
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error instanceof SessionSummaryStoreError
          ? error.withCorrelationId(correlationId)
          : new SessionSummaryStoreError(
              "persistence_failure",
              "Session Summary persistence boundary failed",
              correlationId,
            );
      } finally {
        span.end();
      }
    },
  );
}

/** Acquires the canonical transaction lock for one session and budget class. */
async function lockSessionBudgetClass(
  client: SessionSummaryStoreClient,
  sessionId: string,
  budgetClass: string,
): Promise<void> {
  await client.query(`SELECT pg_advisory_xact_lock(hashtext($1::text), hashtext($2::text))`, [
    sessionId,
    budgetClass,
  ]);
}

/** Reads the single database-enforced active publication head. */
async function readPublishedHead(
  client: SessionSummaryStoreClient,
  sessionId: string,
  budgetClass: string,
): Promise<PublishedSummaryHead | null> {
  const result = await client.query<PublishedHeadRow>(
    `
      SELECT
        content,
        covers_seq_from AS "coversSeqFrom",
        covers_seq_to AS "coversSeqTo",
        summary_id AS "summaryId"
      FROM session_summaries
      WHERE session_id = $1
        AND budget_class = $2
        AND published_at IS NOT NULL
        AND superseded_at IS NULL
      LIMIT 1
    `,
    [sessionId, budgetClass],
  );
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  if (row.content === null) {
    throw new SessionSummaryStoreError(
      "invalid_state",
      "Published Session Summary is missing structured content",
    );
  }
  return {
    content: row.content,
    coversSeqFrom: parseSafeSequence(row.coversSeqFrom, "coversSeqFrom"),
    coversSeqTo: parseSafeSequence(row.coversSeqTo, "coversSeqTo"),
    summaryId: row.summaryId,
  };
}

/** Builds bounded source identity and rejects any sequence hole in the range. */
function buildSourceMetadata(
  rows: readonly SourceEventIdentityRow[],
  range: { readonly from: number; readonly to: number },
): SessionSummaryGenerationJob["source"] {
  const expectedCount = range.to - range.from + 1;
  if (rows.length !== expectedCount) {
    throw new Error("Selected Session Summary range is not contiguous");
  }
  const hash = createHash("sha256");
  rows.forEach((row, index) => {
    const seq = parseSafeSequence(row.seq, "sourceSeq");
    if (seq !== range.from + index) {
      throw new Error("Selected Session Summary source sequence is not contiguous");
    }
    hash.update(`${seq}:${row.eventId}\n`);
  });
  const first = rows[0];
  const last = rows.at(-1);
  if (!first || !last) {
    throw new Error("Selected Session Summary source range is empty");
  }
  return {
    eventCount: rows.length,
    firstEventId: first.eventId,
    lastEventId: last.eventId,
    rangeHash: hash.digest("hex"),
  };
}

/** Persists one protocol-validated generation reservation. */
async function insertReservedSummary(
  client: SessionSummaryStoreClient,
  job: SessionSummaryGenerationJob,
): Promise<void> {
  await client.query(
    `
      INSERT INTO session_summaries (
        budget_class, covers_seq_from, covers_seq_to, generation_task_id,
        ollama_context_size, ollama_model, ollama_quantization,
        ollama_revision, ollama_thinking_mode, output_schema_version,
        producer_id, producer_version, prompt_version, session_id,
        source_event_count, source_first_event_id, source_last_event_id,
        source_range_hash, summary_id
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19
      )
    `,
    [
      job.budgetClass,
      job.previousSummary?.coversSeqFrom ?? job.range.from,
      job.range.to,
      job.taskId,
      job.ollama.contextSize,
      job.ollama.model,
      job.ollama.quantization,
      job.ollama.revision,
      job.ollama.thinkingMode,
      job.outputSchemaVersion,
      job.producer.id,
      job.producer.version,
      job.promptVersion,
      job.sessionId,
      job.source.eventCount,
      job.source.firstEventId,
      job.source.lastEventId,
      job.source.rangeHash,
      job.summaryId,
    ],
  );
}

/** Converts PostgreSQL bigint values into exact public sequence numbers. */
function parseSafeSequence(value: unknown, field: string): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${field} is not a positive safe integer`);
  }
  return parsed;
}

/** Rolls back without replacing the operation's original failure. */
async function rollbackQuietly(client: SessionSummaryStoreClient): Promise<void> {
  try {
    await client.query("ROLLBACK");
  } catch {}
}
