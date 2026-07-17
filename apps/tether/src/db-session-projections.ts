/**
 * PostgreSQL persistence for live Session Projection updates.
 *
 * This module owns projection SQL and row conversion. Its store Interface
 * accepts a caller-owned transaction and never commits or rolls back, so the
 * event append seam retains atomic transaction ownership. It does not own
 * backfill, verification, or projection-backed reads.
 */

import type pg from "pg";

import {
  foldSessionProjection,
  reduceSessionProjection,
  type SessionProjection,
  type SessionProjectionActivity,
  type SessionProjectionForkLineage,
  type SessionProjectionTangentLineage,
} from "./session-projection.js";
import type { SessionEvent } from "./types.js";

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
  readonly updateForAppendedEvent: (
    client: SessionProjectionTransaction,
    event: SessionEvent,
  ) => Promise<SessionProjection>;
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

/** Builds the PostgreSQL-backed live projection store. */
export function createSessionProjectionStore(): SessionProjectionStore {
  return { updateForAppendedEvent };
}

async function updateForAppendedEvent(
  client: SessionProjectionTransaction,
  event: SessionEvent,
): Promise<SessionProjection> {
  const prior = await readProjection(client, event.sessionId);
  const projection = reduceSessionProjection(prior ?? foldSessionProjection([]), event);
  await writeProjection(client, event.sessionId, projection);
  return projection;
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
    [
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
    ],
  );
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
