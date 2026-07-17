import type {
  ParticipantRuntimeKind,
  SessionEvent,
  SessionSummaryContent,
  SessionSummaryIntegrity,
  SessionSummaryOllamaIdentity,
  SessionSummaryProducerIdentity,
  SessionSummarySourceMetadata,
  TaskContractJsonSchema,
  TaskContractSummary,
  TaskRecord,
} from "@dungle-scrubs/tether-protocol";

export type {
  CandidateScheduleIdentity,
  ClientSessionBindingRecord,
  ControlChannel,
  ControlLeaseSnapshot,
  ControlLeaseStatus,
  MailboxScope,
  ParticipantRecord,
  ParticipantRuntimeKind,
  ParticipantRuntimeSnapshot,
  ParticipantRuntimeSnapshotStatus,
  ScheduledMaintenanceIdentity,
  ScheduledSupersessionRefusalReason,
  ScheduleWindow,
  SessionDebugControlLeaseSummary,
  SessionDebugParticipantSummary,
  SessionDebugSummary,
  SessionDebugTaskSummary,
  SessionEvent,
  SessionEventType,
  SessionRecord,
  TaskApprovalRecord,
  TaskListStatus,
  TaskRecord,
  TaskSnapshot,
  TaskSnapshotStatus,
} from "@dungle-scrubs/tether-protocol";

/** Optional bounded read settings for durable session event-list queries. */
export interface SessionEventListOptions {
  /** Maximum number of events to materialize after the sequence cursor. */
  readonly limit?: number | undefined;
}

/**
 * Lightweight external-conversation binding shown alongside a session in the
 * operator session list.
 */
export interface SessionBindingSummary {
  /** Provider-specific conversation id, such as an external chat id. */
  readonly externalId: string;
  /** Client integration provider, such as `external-chat` or `slack`. */
  readonly provider: string;
}

/**
 * Read-only session row with aggregate activity counts for the operator session
 * list view. Counts are computed with grouped queries so the list stays O(1) in
 * round trips regardless of how many sessions exist.
 */
export interface SessionListItem {
  /** Number of durable tasks that have not reached a terminal state. */
  readonly activeTaskCount: number;
  /** Host-presence durable activity state derived from the event log. */
  readonly activity?: "idle" | "queued" | "running" | "settled";
  /** Whether Host-presence consumers should treat the session as archived. */
  readonly archived?: boolean;
  /** Active external conversation bindings that resolve to this session. */
  readonly bindings: readonly SessionBindingSummary[];
  /** Host-presence branch name derived from host metadata, when present. */
  readonly branch?: string | null;
  /** Time this session was first created. */
  readonly createdAt: string;
  /** Host-presence current working directory derived from host metadata. */
  readonly cwd?: string | null;
  /** Whether Host-presence consumers should treat the session as soft-deleted. */
  readonly deleted?: boolean;
  /** Total number of appended session events. */
  readonly eventCount: number;
  /** Host-presence fork lineage metadata, when present. */
  readonly forkedFrom?: SessionLineage | null;
  /** Host-presence git status metadata derived from host metadata. */
  readonly git?: Record<string, unknown> | null;
  /** Host-presence host presence state derived from durable and live state. */
  readonly host?: "live" | "none" | "stale";
  /** Most recent session event time, or null when no events exist yet. */
  readonly lastEventAt: string | null;
  /** Number of participants currently attached to the session. */
  readonly participantCount: number;
  /** Host-presence project name derived from workspace or cwd. */
  readonly project?: string | null;
  /** Durable session identifier. */
  readonly sessionId: string;
  /** Host-presence tangent lineage metadata, when present. */
  readonly tangentOf?: TangentAnchor | null;
  /** Total number of durable tasks across every lifecycle state. */
  readonly taskCount: number;
  /** Host-presence display title derived from events or session id. */
  readonly title?: string;
  /** Host-presence updated timestamp, equivalent to last activity. */
  readonly updatedAt?: string;
  /** Host-presence workspace path derived from host metadata. */
  readonly workspace?: string | null;
}

/** Host-presence fork lineage exposed in the session inventory superset. */
export interface SessionLineage {
  readonly forkSeq: number;
  readonly parentSessionId: string;
}

/** Host-presence tangent anchor exposed in the session inventory superset. */
export interface TangentAnchor {
  readonly createdAt: string;
  readonly label: string | null;
  readonly parentSessionId: string;
  readonly quote: string;
  readonly sourceMessageId: string;
}

/**
 * Normalized task contract discovered from one participant's advertised
 * capabilities.
 */
export interface ParticipantTaskContractRecord extends TaskContractSummary {
  /** Human-readable participant display name. */
  readonly displayName: string;
  /** Inline input schema advertised by the participant, when available. */
  readonly inputJsonSchema?: TaskContractJsonSchema;
  /** Participant identity that advertised this task contract. */
  readonly participantId: string;
  /** Inline result schema advertised by the participant, when available. */
  readonly resultJsonSchema?: TaskContractJsonSchema;
  /** Runtime kind from the participant presence record. */
  readonly runtimeKind: ParticipantRuntimeKind;
  /** Durable Tether session that owns the advertisement. */
  readonly sessionId: string;
}

/** Request parameters for a deterministic bounded session context view. */
export interface SessionContextViewRequest {
  /** Approximate context budget available to the requesting participant. */
  readonly budgetTokens: number;
  /** Optional participant identity the context is being built for. */
  readonly forParticipant: string | null;
  /** Durable Tether session being projected. */
  readonly sessionId: string;
}

/** Budget accounting for a deterministic session context view. */
export interface SessionContextViewBudget {
  /** Approximate tokens consumed by the returned context packet. */
  readonly estimatedTokens: number;
  /** Number of oldest events omitted to fit the budget. */
  readonly omittedEventCount: number;
  /** Budget requested by the caller after server-side clamping. */
  readonly requestedTokens: number;
}

/** Cursor metadata for the raw event range included in a context view. */
export interface SessionContextViewEventRange {
  /** Highest event sequence included in the view, or null when no events fit. */
  readonly endSeq: number | null;
  /** Lowest event sequence included in the view, or null when no events fit. */
  readonly startSeq: number | null;
}

/** Placeholder for a durable compacted summary once compaction exists. */
export interface SessionContextViewSummary {
  /** Token budget class used by the summary producer. */
  readonly budgetClass: string;
  /** Validated structured summary content. */
  readonly content: SessionSummaryContent;
  /** Highest durable event sequence covered by the summary. */
  readonly coversSeqTo: number;
  /** Lowest durable event sequence covered by the summary. */
  readonly coversSeqFrom: number;
  /** Integrity digest of the canonical structured content. */
  readonly integrity: SessionSummaryIntegrity;
  /** Complete local model configuration that produced the summary. */
  readonly ollama: SessionSummaryOllamaIdentity;
  /** Structured-output schema version used by the producer. */
  readonly outputSchemaVersion: string;
  /** Producer identity and version. */
  readonly producer: SessionSummaryProducerIdentity;
  /** Prompt contract version used by the producer. */
  readonly promptVersion: string;
  /** Exact source range identity covered by the summary. */
  readonly source: SessionSummarySourceMetadata;
  /** Stable durable summary identity. */
  readonly summaryId: string;
}

/** Deterministic, budgeted context packet for coordinators and participants. */
export interface SessionContextView {
  /** Active, claimed, or otherwise non-terminal tasks. */
  readonly activeTasks: readonly TaskRecord[];
  /** Budget accounting for this context packet. */
  readonly budget: SessionContextViewBudget;
  /** Participant identity this view was built for, when supplied. */
  readonly forParticipant: string | null;
  /** Current implementation marker for clients and tests. */
  readonly kind: "session_context";
  /** Durable summary covering older events, once compaction exists. */
  readonly latestSummary: SessionContextViewSummary | null;
  /** Whether this view uses a published summary or deterministic raw fallback. */
  readonly mode: "raw_only" | "summary_with_raw_tail";
  /** Advertised task contracts visible in this session. */
  readonly taskContracts: readonly ParticipantTaskContractRecord[];
  /** Recent terminal tasks, newest first. */
  readonly recentTerminalTasks: readonly TaskRecord[];
  /** Recent raw events after the latest summary cursor, in sequence order. */
  readonly recentEvents: readonly SessionEvent[];
  /** Included raw event range metadata. */
  readonly recentEventRange: SessionContextViewEventRange;
  /** Durable Tether session being projected. */
  readonly sessionId: string;
}

export type Result<TValue, TError extends Error = Error> =
  | { readonly ok: true; readonly value: TValue }
  | { readonly ok: false; readonly error: TError };
