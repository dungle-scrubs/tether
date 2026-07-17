/**
 * Pure Session Projection reduction from durable Session Events.
 *
 * This module owns deterministic event-to-projection semantics. It does not
 * access PostgreSQL, HTTP, process-local Host Presence state, or Ollama.
 */

import type { SessionEvent } from "./types.js";

/** Active durable Session Projection reducer contract version. */
export const SESSION_PROJECTION_REDUCER_VERSION = 1;

/** Event contracts whose semantic fields are owned by the current reducer. */
export const SESSION_PROJECTION_EVENT_CONTRACTS = [
  "assistant.completed",
  "assistant.started",
  "host.online",
  "session.archived",
  "session.deleted",
  "session.forkedFrom",
  "session.tangentOf",
  "session.title",
  "user.command",
  "user.message",
] as const;

/** Stable codes for caller violations of the reducer contract. */
export type SessionProjectionInvariantCode =
  | "coverage_regression"
  | "reducer_version_mismatch"
  | "unsafe_sequence";

/** Typed failure raised when a caller violates the reducer contract. */
export class SessionProjectionInvariantError extends Error {
  /** Stable invariant code for tests and future persistence adapters. */
  readonly code: SessionProjectionInvariantCode;

  constructor(code: SessionProjectionInvariantCode, message: string) {
    super(message);
    this.code = code;
    this.name = "SessionProjectionInvariantError";
  }
}

/** Durable assistant activity states represented by the projection. */
export type SessionProjectionActivity = "idle" | "queued" | "running" | "settled";

/** Validated durable fork lineage. */
export interface SessionProjectionForkLineage {
  /** Sequence in the parent stream where the fork originated. */
  readonly forkSeq: number;
  /** Durable parent session identifier. */
  readonly parentSessionId: string;
}

/** Validated durable tangent lineage. */
export interface SessionProjectionTangentLineage {
  /** Timestamp of the tangent event. */
  readonly createdAt: string;
  /** Optional user-facing tangent label. */
  readonly label: string | null;
  /** Durable parent session identifier. */
  readonly parentSessionId: string;
  /** Quoted source text retained by the tangent contract. */
  readonly quote: string;
  /** Durable source message identifier in the parent session. */
  readonly sourceMessageId: string;
}

/** Durable semantic state derived from one Session Event stream. */
export interface SessionProjection {
  /** Run currently represented as active, or null when no run is active. */
  readonly activeRunId: string | null;
  /** Durable assistant activity derived from lifecycle events. */
  readonly activity: SessionProjectionActivity;
  /** Timestamp of the most recent valid activity transition. */
  readonly activityChangedAt: string | null;
  /** Archive transition timestamp, or null while the session is unarchived. */
  readonly archivedAt: string | null;
  /** Highest exact Session Event sequence incorporated into this projection. */
  readonly coversSeqTo: number;
  /** Deletion transition timestamp, or null while the session is restored. */
  readonly deletedAt: string | null;
  /** Cumulative number of exact Session Events incorporated into the projection. */
  readonly eventCount: number;
  /** Validated fork lineage, or null when the session has no fork origin. */
  readonly forkedFrom: SessionProjectionForkLineage | null;
  /** Latest durable host metadata payload, independent of process-local presence. */
  readonly hostMetadata: Readonly<Record<string, unknown>> | null;
  /** Event sequence that supplied {@link hostMetadata}. */
  readonly hostMetadataSourceSeq: number | null;
  /** Timestamp of the latest exact Session Event incorporated into the projection. */
  readonly lastEventAt: string | null;
  /** Reducer contract version used to produce this record. */
  readonly reducerVersion: number;
  /** Validated tangent lineage, or null when the session has no tangent origin. */
  readonly tangentOf: SessionProjectionTangentLineage | null;
  /** Resolved session title, or null until a title source is observed. */
  readonly title: string | null;
  /** Event sequence that supplied the resolved title. */
  readonly titleSourceSeq: number | null;
}

/** Folds an exact Session Event stream into one deterministic projection. */
export function foldSessionProjection(events: readonly SessionEvent[]): SessionProjection {
  return events.reduce<SessionProjection>(reduceSessionProjection, initialSessionProjection());
}

function initialSessionProjection(): SessionProjection {
  return {
    activeRunId: null,
    activity: "idle",
    activityChangedAt: null,
    archivedAt: null,
    coversSeqTo: 0,
    deletedAt: null,
    eventCount: 0,
    forkedFrom: null,
    hostMetadata: null,
    hostMetadataSourceSeq: null,
    lastEventAt: null,
    reducerVersion: SESSION_PROJECTION_REDUCER_VERSION,
    tangentOf: null,
    title: null,
    titleSourceSeq: null,
  };
}

/**
 * Applies one exact Session Event to a prior projection without side effects.
 * Malformed projection-affecting payloads are semantic no-ops, while count,
 * time, and coverage still advance because the exact event was appended.
 */
export function reduceSessionProjection(
  projection: SessionProjection,
  event: SessionEvent,
): SessionProjection {
  assertEventSequenceAdvances(projection, event);
  const nextProjection = advanceCumulativeMetadata(projection, event);
  switch (event.type) {
    case "assistant.completed": {
      const runId = nonEmptyStringField(event.payload, "runId");
      return runId !== null && runId === nextProjection.activeRunId
        ? {
            ...nextProjection,
            activeRunId: null,
            activity: "settled",
            activityChangedAt: event.createdAt,
          }
        : nextProjection;
    }
    case "assistant.started": {
      const runId = nonEmptyStringField(event.payload, "runId");
      return runId
        ? {
            ...nextProjection,
            activeRunId: runId,
            activity: "running",
            activityChangedAt: event.createdAt,
          }
        : nextProjection;
    }
    case "host.online":
      return {
        ...nextProjection,
        hostMetadata: { ...event.payload },
        hostMetadataSourceSeq: event.seq,
      };
    case "session.archived": {
      const archived = booleanField(event.payload, "archived");
      return archived === null
        ? nextProjection
        : { ...nextProjection, archivedAt: archived ? event.createdAt : null };
    }
    case "session.deleted": {
      const deleted = booleanField(event.payload, "deleted");
      return deleted === null
        ? nextProjection
        : { ...nextProjection, deletedAt: deleted ? event.createdAt : null };
    }
    case "session.forkedFrom": {
      const forkSeq = nonNegativeSafeIntegerField(event.payload, "forkSeq");
      const parentSessionId = nonEmptyStringField(event.payload, "parentSessionId");
      return forkSeq === null || parentSessionId === null
        ? nextProjection
        : { ...nextProjection, forkedFrom: { forkSeq, parentSessionId } };
    }
    case "session.tangentOf": {
      const parentSessionId = nonEmptyStringField(event.payload, "parentSessionId");
      const quote = nonEmptyStringField(event.payload, "quote");
      const sourceMessageId = nonEmptyStringField(event.payload, "sourceMessageId");
      return parentSessionId === null || quote === null || sourceMessageId === null
        ? nextProjection
        : {
            ...nextProjection,
            tangentOf: {
              createdAt: event.createdAt,
              label: stringField(event.payload, "label"),
              parentSessionId,
              quote,
              sourceMessageId,
            },
          };
    }
    case "session.title": {
      const title = normalizedTitle(stringField(event.payload, "title"));
      return title ? { ...nextProjection, title, titleSourceSeq: event.seq } : nextProjection;
    }
    case "user.command":
      return stringField(event.payload, "command") === "/clear"
        ? {
            ...nextProjection,
            activeRunId: null,
            activity: "idle",
            activityChangedAt: event.createdAt,
          }
        : nextProjection;
    case "user.message": {
      if (nextProjection.title !== null) {
        return nextProjection;
      }
      const message = event.payload.message;
      const title = normalizedTitle(
        stringField(event.payload, "text") ??
          stringField(event.payload, "message") ??
          (isRecord(message) ? stringField(message, "text") : null),
      );
      return title ? { ...nextProjection, title, titleSourceSeq: event.seq } : nextProjection;
    }
    default:
      return nextProjection;
  }
}

/**
 * Reports whether a known projection-affecting event has an invalid payload.
 * The result contains no payload data and follows the same field validators as
 * {@link reduceSessionProjection}.
 */
export function isMalformedSessionProjectionEvent(event: SessionEvent): boolean {
  switch (event.type) {
    case "assistant.completed":
    case "assistant.started":
      return nonEmptyStringField(event.payload, "runId") === null;
    case "session.archived":
      return booleanField(event.payload, "archived") === null;
    case "session.deleted":
      return booleanField(event.payload, "deleted") === null;
    case "session.forkedFrom":
      return (
        nonNegativeSafeIntegerField(event.payload, "forkSeq") === null ||
        nonEmptyStringField(event.payload, "parentSessionId") === null
      );
    case "session.tangentOf":
      return (
        nonEmptyStringField(event.payload, "parentSessionId") === null ||
        nonEmptyStringField(event.payload, "quote") === null ||
        nonEmptyStringField(event.payload, "sourceMessageId") === null
      );
    case "session.title":
      return normalizedTitle(stringField(event.payload, "title")) === null;
    case "user.command":
      return event.payload.command !== undefined && stringField(event.payload, "command") === null;
    case "user.message": {
      const message = event.payload.message;
      return (
        normalizedTitle(
          stringField(event.payload, "text") ??
            stringField(event.payload, "message") ??
            (isRecord(message) ? stringField(message, "text") : null),
        ) === null
      );
    }
    default:
      return false;
  }
}

function assertEventSequenceAdvances(projection: SessionProjection, event: SessionEvent): void {
  if (projection.reducerVersion !== SESSION_PROJECTION_REDUCER_VERSION) {
    throw new SessionProjectionInvariantError(
      "reducer_version_mismatch",
      `Session Projection reducer version mismatch: expected ${SESSION_PROJECTION_REDUCER_VERSION}, received ${projection.reducerVersion}`,
    );
  }
  if (!Number.isSafeInteger(event.seq) || event.seq <= 0) {
    throw new SessionProjectionInvariantError(
      "unsafe_sequence",
      `Session Projection received unsafe sequence ${event.seq}`,
    );
  }
  if (event.seq <= projection.coversSeqTo) {
    throw new SessionProjectionInvariantError(
      "coverage_regression",
      `Session Projection coverage regression: event ${event.seq} does not advance ${projection.coversSeqTo}`,
    );
  }
}

function advanceCumulativeMetadata(
  projection: SessionProjection,
  event: SessionEvent,
): SessionProjection {
  return {
    ...projection,
    coversSeqTo: event.seq,
    eventCount: projection.eventCount + 1,
    lastEventAt: event.createdAt,
    reducerVersion: SESSION_PROJECTION_REDUCER_VERSION,
  };
}

function booleanField(value: Readonly<Record<string, unknown>>, key: string): boolean | null {
  const field = value[key];
  return typeof field === "boolean" ? field : null;
}

function nonEmptyStringField(value: Readonly<Record<string, unknown>>, key: string): string | null {
  const field = stringField(value, key);
  return field && field.trim().length > 0 ? field : null;
}

function normalizedTitle(value: string | null): string | null {
  const normalized = value?.trim().replace(/\s+/gu, " ");
  return normalized ? truncateTitle(normalized) : null;
}

function nonNegativeSafeIntegerField(
  value: Readonly<Record<string, unknown>>,
  key: string,
): number | null {
  const field = value[key];
  return typeof field === "number" && Number.isSafeInteger(field) && field >= 0 ? field : null;
}

function stringField(value: Readonly<Record<string, unknown>>, key: string): string | null {
  const field = value[key];
  return typeof field === "string" ? field : null;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function truncateTitle(value: string): string {
  return value.length > 60 ? value.slice(0, 60) : value;
}
