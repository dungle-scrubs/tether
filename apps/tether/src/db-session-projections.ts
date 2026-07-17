/**
 * PostgreSQL persistence for live Session Projection updates and backfill.
 *
 * This module owns projection SQL and row conversion. Its store Interface
 * accepts a caller-owned transaction and never commits or rolls back, so the
 * event append seam retains atomic transaction ownership. It also owns bounded
 * backfill compare-and-set writes and projection-backed inventory reads, but
 * not HTTP or process-local Host Presence assembly.
 */

import { isDeepStrictEqual } from "node:util";

import type pg from "pg";
import { sessionScalabilitySpanNames } from "@dungle-scrubs/tether-protocol";
import { SpanStatusCode, trace } from "@opentelemetry/api";

import {
  foldSessionProjection,
  isMalformedSessionProjectionEvent,
  reduceSessionProjection,
  SESSION_PROJECTION_REDUCER_VERSION,
  type SessionProjection,
  type SessionProjectionActivity,
  type SessionProjectionForkLineage,
  type SessionProjectionTangentLineage,
} from "./session-projection.js";
import type { SessionBindingSummary, SessionEvent, SessionListItem } from "./types.js";
import { sessionScalabilityRuntimeState } from "./session-scalability-runtime-state.js";

/** Minimal transaction Interface required by projection persistence. */
export interface SessionProjectionTransaction {
  readonly query: <TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ) => Promise<{ readonly rows: TRow[] }>;
}

/**
 * Store Interface for applying one newly inserted event to its projection.
 * Callers must supply the same transaction that owns the event insertion.
 */
export interface SessionProjectionStore {
  /** Persists complete empty coverage for a newly inserted session. */
  readonly initializeForNewSession: (
    client: SessionProjectionTransaction,
    sessionId: string,
  ) => Promise<SessionProjection>;
  readonly updateForAppendedEvent: (
    client: SessionProjectionTransaction,
    event: SessionEvent,
  ) => Promise<SessionProjection>;
}

/** Payload-free outcome from one bounded projection backfill attempt. */
export interface SessionProjectionBackfillResult {
  /** Number of bounded event queries used by this attempt. */
  readonly batchesRead: number;
  /** Exact sequence covered by the candidate. */
  readonly coversSeqTo: number;
  /** Coverage observed as durable after this attempt. */
  readonly currentCoversSeqTo: number | null;
  /** Cumulative exact event count represented by the candidate. */
  readonly eventCount: number;
  /** Invalid known event payloads ignored during this attempt. */
  readonly malformedEventCount: number;
  /** Whether the compare-and-set installed the candidate. */
  readonly outcome: "stale" | "unchanged" | "written";
  /** Reducer contract version used for the candidate. */
  readonly reducerVersion: number;
  /** Older reducer version replaced by this attempt, when applicable. */
  readonly replacedReducerVersion: number | null;
  /** Durable coverage reused as the fold's trusted restart point. */
  readonly resumedFromSeq: number;
}

/** Inputs for one bounded projection backfill attempt. */
export interface SessionProjectionBackfillInput {
  /** Maximum Session Events materialized by any event query. */
  readonly batchSize: number;
  /** Ignores durable coverage so verification mismatches can be repaired. */
  readonly rebuildFromStart?: boolean;
  /** Durable session whose projection is being rebuilt or advanced. */
  readonly sessionId: string;
}

/** Raised when projection-backed reads encounter an unsafe cutover state. */
export class SessionProjectionCutoverError extends Error {
  readonly sessionId: string;

  constructor(sessionId: string, detail: string) {
    super(`Session Projection cutover is unsafe for ${sessionId}: ${detail}`);
    this.name = "SessionProjectionCutoverError";
    this.sessionId = sessionId;
  }
}

/** Payload-free deterministic comparison against a fresh event-stream fold. */
export interface SessionProjectionVerificationReport {
  readonly batchesRead: number;
  readonly differenceFields: readonly (keyof SessionProjection)[];
  readonly freshCoversSeqTo: number;
  readonly freshEventCount: number;
  readonly malformedEventCount: number;
  readonly repair: "backfill" | "none";
  readonly status: "current" | "mismatch" | "missing";
  readonly storedCoversSeqTo: number | null;
  readonly storedEventCount: number | null;
  readonly storedReducerVersion: number | null;
}

interface PgSessionProjectionRow {
  readonly activeRunId: string | null;
  readonly activity: SessionProjectionActivity;
  readonly activityChangedAt: Date | null;
  readonly archivedAt: Date | null;
  readonly coversSeqTo: unknown;
  readonly deletedAt: Date | null;
  readonly eventCount: unknown;
  readonly forkedFrom: SessionProjectionForkLineage | null;
  readonly hostMetadata: Record<string, unknown> | null;
  readonly hostMetadataSourceSeq: unknown;
  readonly lastEventAt: Date | null;
  readonly reducerVersion: number;
  readonly tangentOf: SessionProjectionTangentLineage | null;
  readonly title: string | null;
  readonly titleSourceSeq: unknown;
}

interface PgSessionProjectionEventRow {
  readonly createdAt: Date;
  readonly eventId: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly seq: unknown;
  readonly sessionId: string;
  readonly type: string;
}

interface PgSessionProjectionInventoryRow extends PgSessionProjectionRow {
  readonly activeTaskCount: unknown;
  readonly bindings: readonly SessionBindingSummary[];
  readonly createdAt: Date;
  readonly hasProjection: boolean;
  readonly participantCount: unknown;
  readonly sessionId: string;
  readonly taskCount: unknown;
}

const liveRepairBatchSize = 500;
const projectionVerificationFields = [
  "activeRunId",
  "activity",
  "activityChangedAt",
  "archivedAt",
  "coversSeqTo",
  "deletedAt",
  "eventCount",
  "forkedFrom",
  "hostMetadata",
  "hostMetadataSourceSeq",
  "lastEventAt",
  "reducerVersion",
  "tangentOf",
  "title",
  "titleSourceSeq",
] as const satisfies readonly (keyof SessionProjection)[];

/**
 * Lists projection-backed inventory plus counts from authoritative companion
 * tables. The query never reads or groups the raw Session Event table.
 */
export async function listSessionProjectionInventory(
  client: SessionProjectionTransaction,
): Promise<SessionListItem[]> {
  const rows = await client.query<PgSessionProjectionInventoryRow>(`
    WITH participant_counts AS (
      SELECT session_id, count(*)::int AS total
      FROM participants
      GROUP BY session_id
    ),
    task_counts AS (
      SELECT
        session_id,
        count(*)::int AS total,
        (count(*) FILTER (
          WHERE cancelled_at IS NULL
            AND completed_at IS NULL
            AND failed_at IS NULL
        ))::int AS active
      FROM tasks
      GROUP BY session_id
    ),
    binding_lists AS (
      SELECT
        session_id,
        jsonb_agg(
          jsonb_build_object('externalId', external_id, 'provider', provider)
          ORDER BY provider, external_id
        ) AS bindings
      FROM client_session_bindings
      WHERE archived_at IS NULL
      GROUP BY session_id
    )
    SELECT
      projection.active_run_id AS "activeRunId",
      COALESCE(task_counts.active, 0) AS "activeTaskCount",
      projection.activity,
      projection.activity_changed_at AS "activityChangedAt",
      projection.archived_at AS "archivedAt",
      COALESCE(binding_lists.bindings, '[]'::jsonb) AS bindings,
      projection.covers_seq_to AS "coversSeqTo",
      session.created_at AS "createdAt",
      projection.deleted_at AS "deletedAt",
      projection.event_count AS "eventCount",
      projection.forked_from AS "forkedFrom",
      projection.host_metadata AS "hostMetadata",
      projection.host_metadata_source_seq AS "hostMetadataSourceSeq",
      projection.session_id IS NOT NULL AS "hasProjection",
      projection.last_event_at AS "lastEventAt",
      COALESCE(participant_counts.total, 0) AS "participantCount",
      projection.reducer_version AS "reducerVersion",
      session.session_id AS "sessionId",
      projection.tangent_of AS "tangentOf",
      COALESCE(task_counts.total, 0) AS "taskCount",
      projection.title,
      projection.title_source_seq AS "titleSourceSeq"
    FROM sessions AS session
    LEFT JOIN session_projections AS projection
      ON projection.session_id = session.session_id
    LEFT JOIN participant_counts
      ON participant_counts.session_id = session.session_id
    LEFT JOIN task_counts
      ON task_counts.session_id = session.session_id
    LEFT JOIN binding_lists
      ON binding_lists.session_id = session.session_id
  `);
  return rows.rows.map(toSessionProjectionInventoryItem).sort(compareInventoryActivity);
}

/** Verifies a stored projection against a bounded, fresh deterministic fold. */
export async function verifySessionProjection(
  client: SessionProjectionTransaction,
  input: SessionProjectionBackfillInput,
): Promise<SessionProjectionVerificationReport> {
  const report = await traceProjectionOperation(
    sessionScalabilitySpanNames.projectionVerify,
    { "projection.batch_size": input.batchSize },
    () => verifySessionProjectionInternal(client, input),
    (report) => ({
      "projection.batch_count": report.batchesRead,
      "projection.difference_count": report.differenceFields.length,
      "projection.status": report.status,
    }),
  );
  sessionScalabilityRuntimeState.recordVerification(input.sessionId, {
    batchesRead: report.batchesRead,
    differenceCount: report.differenceFields.length,
    malformedEventCount: report.malformedEventCount,
    status: report.status,
  });
  return report;
}

async function verifySessionProjectionInternal(
  client: SessionProjectionTransaction,
  input: SessionProjectionBackfillInput,
): Promise<SessionProjectionVerificationReport> {
  assertPositiveBatchSize(input.batchSize);
  const stored = await readProjection(client, input.sessionId);
  const fresh = await foldSessionHistoryBatches(
    client,
    input.sessionId,
    foldSessionProjection([]),
    input.batchSize,
  );
  const differenceFields = stored
    ? projectionVerificationFields.filter(
        (field) => !isDeepStrictEqual(stored[field], fresh.projection[field]),
      )
    : [];
  const status =
    stored === null ? "missing" : differenceFields.length === 0 ? "current" : "mismatch";
  return {
    batchesRead: fresh.batchesRead,
    differenceFields,
    freshCoversSeqTo: fresh.projection.coversSeqTo,
    freshEventCount: fresh.projection.eventCount,
    malformedEventCount: fresh.malformedEventCount,
    repair: status === "current" ? "none" : "backfill",
    status,
    storedCoversSeqTo: stored?.coversSeqTo ?? null,
    storedEventCount: stored?.eventCount ?? null,
    storedReducerVersion: stored?.reducerVersion ?? null,
  };
}

/**
 * Folds one session through the visible event head using bounded reads, then
 * installs the candidate only if the projection row still matches its start.
 */
export async function backfillSessionProjection(
  client: SessionProjectionTransaction,
  input: SessionProjectionBackfillInput,
): Promise<SessionProjectionBackfillResult> {
  const result = await traceProjectionOperation(
    sessionScalabilitySpanNames.projectionBackfill,
    {
      "projection.batch_size": input.batchSize,
      "projection.rebuild_from_start": input.rebuildFromStart === true,
    },
    () => backfillSessionProjectionInternal(client, input),
    (result) => ({
      "projection.batch_count": result.batchesRead,
      "projection.malformed_event_count": result.malformedEventCount,
      "projection.status": result.outcome,
    }),
  );
  sessionScalabilityRuntimeState.recordBackfill(input.sessionId, {
    batchesRead: result.batchesRead,
    malformedEventCount: result.malformedEventCount,
    status: result.outcome,
  });
  return result;
}

async function backfillSessionProjectionInternal(
  client: SessionProjectionTransaction,
  input: SessionProjectionBackfillInput,
): Promise<SessionProjectionBackfillResult> {
  assertPositiveBatchSize(input.batchSize);
  const stored = await readProjection(client, input.sessionId);
  if (input.rebuildFromStart === true) {
    return rebuildSessionProjectionFromStart(client, input, stored);
  }
  const canResume = stored !== null && isCompleteProjection(stored);
  const replacedReducerVersion =
    stored !== null && stored.reducerVersion !== SESSION_PROJECTION_REDUCER_VERSION
      ? stored.reducerVersion
      : null;
  const resumedFromSeq = canResume ? stored.coversSeqTo : 0;
  let batchesRead = 0;
  let expected = stored;
  let malformedEventCount = 0;
  let projection = canResume ? stored : foldSessionProjection([]);
  let wroteBatch = false;
  while (true) {
    const events = await readSessionProjectionEventBatch(
      client,
      input.sessionId,
      projection.coversSeqTo,
      input.batchSize,
    );
    batchesRead += 1;
    for (const event of events) {
      if (isMalformedSessionProjectionEvent(event)) {
        malformedEventCount += 1;
      }
      projection = reduceSessionProjection(projection, event);
    }
    const requiresInitialWrite =
      expected === null || expected.reducerVersion !== SESSION_PROJECTION_REDUCER_VERSION;
    if (events.length > 0 || requiresInitialWrite) {
      const written = await compareAndSetBackfillCandidate(
        client,
        input.sessionId,
        expected,
        projection,
      );
      if (!written) {
        const current = await readProjection(client, input.sessionId);
        return backfillResult(
          { batchesRead, malformedEventCount, projection, resumedFromSeq },
          current?.coversSeqTo ?? null,
          "stale",
          replacedReducerVersion,
        );
      }
      expected = projection;
      wroteBatch = true;
    }
    if (events.length < input.batchSize) {
      return backfillResult(
        { batchesRead, malformedEventCount, projection, resumedFromSeq },
        projection.coversSeqTo,
        wroteBatch ? "written" : "unchanged",
        replacedReducerVersion,
      );
    }
  }
}

async function rebuildSessionProjectionFromStart(
  client: SessionProjectionTransaction,
  input: SessionProjectionBackfillInput,
  stored: SessionProjection | null,
): Promise<SessionProjectionBackfillResult> {
  const folded = await foldSessionHistoryBatches(
    client,
    input.sessionId,
    foldSessionProjection([]),
    input.batchSize,
  );
  const written = await compareAndSetBackfillCandidate(
    client,
    input.sessionId,
    stored,
    folded.projection,
  );
  if (written) {
    return backfillResult(
      folded,
      folded.projection.coversSeqTo,
      "written",
      stored !== null && stored.reducerVersion !== SESSION_PROJECTION_REDUCER_VERSION
        ? stored.reducerVersion
        : null,
    );
  }
  const current = await readProjection(client, input.sessionId);
  return backfillResult(
    folded,
    current?.coversSeqTo ?? null,
    "stale",
    stored !== null && stored.reducerVersion !== SESSION_PROJECTION_REDUCER_VERSION
      ? stored.reducerVersion
      : null,
  );
}

/** Builds the PostgreSQL-backed live projection store. */
export function createSessionProjectionStore(): SessionProjectionStore {
  return { initializeForNewSession, updateForAppendedEvent };
}

async function initializeForNewSession(
  client: SessionProjectionTransaction,
  sessionId: string,
): Promise<SessionProjection> {
  const projection = foldSessionProjection([]);
  await writeProjection(client, sessionId, projection);
  return projection;
}

async function updateForAppendedEvent(
  client: SessionProjectionTransaction,
  event: SessionEvent,
): Promise<SessionProjection> {
  return traceProjectionOperation(
    sessionScalabilitySpanNames.projectionApply,
    { "projection.event_seq": event.seq },
    () => updateForAppendedEventInternal(client, event),
    (projection) => ({
      "projection.covers_seq_to": projection.coversSeqTo,
      "projection.reducer_version": projection.reducerVersion,
    }),
  );
}

async function updateForAppendedEventInternal(
  client: SessionProjectionTransaction,
  event: SessionEvent,
): Promise<SessionProjection> {
  const prior = await readProjection(client, event.sessionId);
  const projection =
    prior && isCompletePredecessor(prior, event)
      ? reduceSessionProjection(prior, event)
      : await foldCompleteSessionHistory(client, event.sessionId);
  await writeProjection(client, event.sessionId, projection);
  return projection;
}

async function traceProjectionOperation<TValue>(
  name: string,
  attributes: Readonly<Record<string, boolean | number | string>>,
  action: () => Promise<TValue>,
  summarize: (value: TValue) => Readonly<Record<string, boolean | number | string>>,
): Promise<TValue> {
  return trace
    .getTracer("tether-session-projection")
    .startActiveSpan(name, { attributes }, async (span) => {
      try {
        const value = await action();
        span.setAttributes(summarize(value));
        span.setStatus({ code: SpanStatusCode.OK });
        return value;
      } catch (error) {
        span.setAttribute("projection.failure_code", "projection_operation_failed");
        span.setStatus({ code: SpanStatusCode.ERROR });
        throw error;
      } finally {
        span.end();
      }
    });
}

function isCompletePredecessor(projection: SessionProjection, event: SessionEvent): boolean {
  return isCompleteProjection(projection) && projection.coversSeqTo === event.seq - 1;
}

function isCompleteProjection(projection: SessionProjection): boolean {
  return projection.reducerVersion === SESSION_PROJECTION_REDUCER_VERSION;
}

/**
 * Rebuilds a missing projection through the caller transaction's visible head.
 * Bounded reads prevent a legacy stream from being materialized all at once.
 */
async function foldCompleteSessionHistory(
  client: SessionProjectionTransaction,
  sessionId: string,
): Promise<SessionProjection> {
  const folded = await foldSessionHistoryBatches(
    client,
    sessionId,
    foldSessionProjection([]),
    liveRepairBatchSize,
  );
  return folded.projection;
}

interface FoldedSessionHistory {
  readonly batchesRead: number;
  readonly malformedEventCount: number;
  readonly projection: SessionProjection;
  readonly resumedFromSeq: number;
}

async function foldSessionHistoryBatches(
  client: SessionProjectionTransaction,
  sessionId: string,
  initial: SessionProjection,
  batchSize: number,
): Promise<FoldedSessionHistory> {
  let batchesRead = 0;
  let malformedEventCount = 0;
  let projection = initial;
  const resumedFromSeq = initial.coversSeqTo;
  while (true) {
    const events = await readSessionProjectionEventBatch(
      client,
      sessionId,
      projection.coversSeqTo,
      batchSize,
    );
    batchesRead += 1;
    for (const event of events) {
      if (isMalformedSessionProjectionEvent(event)) {
        malformedEventCount += 1;
      }
      projection = reduceSessionProjection(projection, event);
    }
    if (events.length < batchSize) {
      return { batchesRead, malformedEventCount, projection, resumedFromSeq };
    }
  }
}

async function readSessionProjectionEventBatch(
  client: SessionProjectionTransaction,
  sessionId: string,
  afterSeq: number,
  batchSize: number,
): Promise<SessionEvent[]> {
  const rows = await client.query<PgSessionProjectionEventRow>(
    `
      SELECT
        created_at AS "createdAt",
        event_id AS "eventId",
        payload,
        producer_id AS "producerId",
        seq,
        session_id AS "sessionId",
        type
      FROM session_events
      WHERE session_id = $1 AND seq > $2
      ORDER BY seq
      LIMIT $3
    `,
    [sessionId, afterSeq, batchSize],
  );
  return rows.rows.map(toSessionEvent);
}

async function compareAndSetBackfillCandidate(
  client: SessionProjectionTransaction,
  sessionId: string,
  expected: SessionProjection | null,
  candidate: SessionProjection,
): Promise<boolean> {
  const rows = await client.query<{ readonly sessionId: string }>(
    `
      INSERT INTO session_projections (
        active_run_id,
        activity,
        activity_changed_at,
        archived_at,
        covers_seq_to,
        deleted_at,
        event_count,
        forked_from,
        host_metadata,
        host_metadata_source_seq,
        last_event_at,
        reducer_version,
        session_id,
        tangent_of,
        title,
        title_source_seq
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8::jsonb,
        $9::jsonb,
        $10,
        $11,
        $12,
        $13,
        $14::jsonb,
        $15,
        $16
      )
      ON CONFLICT (session_id) DO UPDATE
      SET
        active_run_id = EXCLUDED.active_run_id,
        activity = EXCLUDED.activity,
        activity_changed_at = EXCLUDED.activity_changed_at,
        archived_at = EXCLUDED.archived_at,
        covers_seq_to = EXCLUDED.covers_seq_to,
        deleted_at = EXCLUDED.deleted_at,
        event_count = EXCLUDED.event_count,
        forked_from = EXCLUDED.forked_from,
        host_metadata = EXCLUDED.host_metadata,
        host_metadata_source_seq = EXCLUDED.host_metadata_source_seq,
        last_event_at = EXCLUDED.last_event_at,
        reducer_version = EXCLUDED.reducer_version,
        tangent_of = EXCLUDED.tangent_of,
        title = EXCLUDED.title,
        title_source_seq = EXCLUDED.title_source_seq,
        updated_at = clock_timestamp()
      WHERE $17::boolean
        AND session_projections.reducer_version = $18
        AND session_projections.covers_seq_to = $19
        AND session_projections.event_count = $20
        AND EXCLUDED.covers_seq_to >= session_projections.covers_seq_to
      RETURNING session_id AS "sessionId"
    `,
    [
      ...projectionSqlValues(sessionId, candidate),
      expected !== null,
      expected?.reducerVersion ?? 0,
      expected?.coversSeqTo ?? 0,
      expected?.eventCount ?? 0,
    ],
  );
  return rows.rows.length === 1;
}

function assertPositiveBatchSize(value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Session Projection batch size must be a positive safe integer: ${value}`);
  }
}

function backfillResult(
  folded: FoldedSessionHistory,
  currentCoversSeqTo: number | null,
  outcome: SessionProjectionBackfillResult["outcome"],
  replacedReducerVersion: number | null,
): SessionProjectionBackfillResult {
  return {
    batchesRead: folded.batchesRead,
    coversSeqTo: folded.projection.coversSeqTo,
    currentCoversSeqTo,
    eventCount: folded.projection.eventCount,
    malformedEventCount: folded.malformedEventCount,
    outcome,
    reducerVersion: folded.projection.reducerVersion,
    replacedReducerVersion,
    resumedFromSeq: folded.resumedFromSeq,
  };
}

async function readProjection(
  client: SessionProjectionTransaction,
  sessionId: string,
): Promise<SessionProjection | null> {
  const rows = await client.query<PgSessionProjectionRow>(
    `
      SELECT
        active_run_id AS "activeRunId",
        activity,
        activity_changed_at AS "activityChangedAt",
        archived_at AS "archivedAt",
        covers_seq_to AS "coversSeqTo",
        deleted_at AS "deletedAt",
        event_count AS "eventCount",
        forked_from AS "forkedFrom",
        host_metadata AS "hostMetadata",
        host_metadata_source_seq AS "hostMetadataSourceSeq",
        last_event_at AS "lastEventAt",
        reducer_version AS "reducerVersion",
        tangent_of AS "tangentOf",
        title,
        title_source_seq AS "titleSourceSeq"
      FROM session_projections
      WHERE session_id = $1
    `,
    [sessionId],
  );
  return rows.rows[0] ? toSessionProjection(rows.rows[0]) : null;
}

async function writeProjection(
  client: SessionProjectionTransaction,
  sessionId: string,
  projection: SessionProjection,
): Promise<void> {
  await client.query(
    `
      INSERT INTO session_projections (
        active_run_id,
        activity,
        activity_changed_at,
        archived_at,
        covers_seq_to,
        deleted_at,
        event_count,
        forked_from,
        host_metadata,
        host_metadata_source_seq,
        last_event_at,
        reducer_version,
        session_id,
        tangent_of,
        title,
        title_source_seq
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8::jsonb,
        $9::jsonb,
        $10,
        $11,
        $12,
        $13,
        $14::jsonb,
        $15,
        $16
      )
      ON CONFLICT (session_id) DO UPDATE
      SET
        active_run_id = EXCLUDED.active_run_id,
        activity = EXCLUDED.activity,
        activity_changed_at = EXCLUDED.activity_changed_at,
        archived_at = EXCLUDED.archived_at,
        covers_seq_to = EXCLUDED.covers_seq_to,
        deleted_at = EXCLUDED.deleted_at,
        event_count = EXCLUDED.event_count,
        forked_from = EXCLUDED.forked_from,
        host_metadata = EXCLUDED.host_metadata,
        host_metadata_source_seq = EXCLUDED.host_metadata_source_seq,
        last_event_at = EXCLUDED.last_event_at,
        reducer_version = EXCLUDED.reducer_version,
        tangent_of = EXCLUDED.tangent_of,
        title = EXCLUDED.title,
        title_source_seq = EXCLUDED.title_source_seq,
        updated_at = clock_timestamp()
    `,
    projectionSqlValues(sessionId, projection),
  );
}

function projectionSqlValues(sessionId: string, projection: SessionProjection): readonly unknown[] {
  return [
    projection.activeRunId,
    projection.activity,
    projection.activityChangedAt,
    projection.archivedAt,
    projection.coversSeqTo,
    projection.deletedAt,
    projection.eventCount,
    serializeNullableJson(projection.forkedFrom),
    serializeNullableJson(projection.hostMetadata),
    projection.hostMetadataSourceSeq,
    projection.lastEventAt,
    projection.reducerVersion,
    sessionId,
    serializeNullableJson(projection.tangentOf),
    projection.title,
    projection.titleSourceSeq,
  ];
}

function serializeNullableJson(value: unknown | null): string | null {
  return value === null ? null : JSON.stringify(value);
}

function toSessionProjection(row: PgSessionProjectionRow): SessionProjection {
  return {
    activeRunId: row.activeRunId,
    activity: row.activity,
    activityChangedAt: toNullableIsoString(row.activityChangedAt),
    archivedAt: toNullableIsoString(row.archivedAt),
    coversSeqTo: parseProjectionSequence(row.coversSeqTo, "covers_seq_to"),
    deletedAt: toNullableIsoString(row.deletedAt),
    eventCount: parseProjectionSequence(row.eventCount, "event_count"),
    forkedFrom: row.forkedFrom,
    hostMetadata: row.hostMetadata,
    hostMetadataSourceSeq: parseNullableProjectionSequence(
      row.hostMetadataSourceSeq,
      "host_metadata_source_seq",
    ),
    lastEventAt: toNullableIsoString(row.lastEventAt),
    reducerVersion: row.reducerVersion,
    tangentOf: row.tangentOf,
    title: row.title,
    titleSourceSeq: parseNullableProjectionSequence(row.titleSourceSeq, "title_source_seq"),
  };
}

function toSessionEvent(row: PgSessionProjectionEventRow): SessionEvent {
  return {
    createdAt: row.createdAt.toISOString(),
    eventId: row.eventId,
    payload: row.payload,
    producerId: row.producerId,
    seq: parseProjectionSequence(row.seq, "session_events.seq"),
    sessionId: row.sessionId,
    type: row.type,
  };
}

function toSessionProjectionInventoryItem(row: PgSessionProjectionInventoryRow): SessionListItem {
  if (row.hasProjection === false) {
    throw new SessionProjectionCutoverError(row.sessionId, "projection is missing");
  }
  const projection = toSessionProjection(row);
  if (!isCompleteProjection(projection)) {
    throw new SessionProjectionCutoverError(
      row.sessionId,
      `reducer version ${projection.reducerVersion} does not match ${SESSION_PROJECTION_REDUCER_VERSION}`,
    );
  }
  const createdAt = row.createdAt.toISOString();
  const hostMetadata = projection.hostMetadata;
  const cwd = recordStringField(hostMetadata, "cwd");
  const workspace = recordStringField(hostMetadata, "workspace");
  return {
    activeTaskCount: parseProjectionSequence(row.activeTaskCount, "active_task_count"),
    activity: projection.activity,
    archived: projection.archivedAt !== null,
    bindings: row.bindings,
    branch: recordStringField(hostMetadata, "branch"),
    createdAt,
    cwd,
    deleted: projection.deletedAt !== null,
    eventCount: projection.eventCount,
    forkedFrom: projection.forkedFrom,
    git: recordRecordField(hostMetadata, "git"),
    host: hostMetadata === null ? "none" : "stale",
    lastEventAt: projection.lastEventAt,
    participantCount: parseProjectionSequence(row.participantCount, "participant_count"),
    project: projectName(workspace, cwd),
    sessionId: row.sessionId,
    tangentOf: projection.tangentOf,
    taskCount: parseProjectionSequence(row.taskCount, "task_count"),
    title: projection.title ?? row.sessionId,
    updatedAt: projection.lastEventAt ?? createdAt,
    workspace,
  };
}

function compareInventoryActivity(left: SessionListItem, right: SessionListItem): number {
  const leftActivity = left.updatedAt ?? left.createdAt;
  const rightActivity = right.updatedAt ?? right.createdAt;
  if (leftActivity === rightActivity) {
    return left.sessionId.localeCompare(right.sessionId);
  }
  return leftActivity < rightActivity ? 1 : -1;
}

function projectName(workspace: string | null, cwd: string | null): string | null {
  const path = workspace ?? cwd;
  if (path === null) {
    return null;
  }
  const trimmed = path.replace(/\/+$/u, "");
  const base = trimmed.split("/").at(-1);
  return base && base.length > 0 ? base : trimmed;
}

function recordStringField(
  value: Readonly<Record<string, unknown>> | null,
  key: string,
): string | null {
  const field = value?.[key];
  return typeof field === "string" ? field : null;
}

function recordRecordField(
  value: Readonly<Record<string, unknown>> | null,
  key: string,
): Record<string, unknown> | null {
  const field = value?.[key];
  return typeof field === "object" && field !== null && !Array.isArray(field)
    ? { ...(field as Record<string, unknown>) }
    : null;
}

function toNullableIsoString(value: Date | null): string | null {
  return value?.toISOString() ?? null;
}

function parseNullableProjectionSequence(value: unknown, column: string): number | null {
  return value === null ? null : parseProjectionSequence(value, column);
}

function parseProjectionSequence(value: unknown, column: string): number {
  const parsed =
    typeof value === "number" || typeof value === "string" || typeof value === "bigint"
      ? Number(value)
      : Number.NaN;
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`Invalid Session Projection ${column}: ${String(value)}`);
  }
  return parsed;
}
