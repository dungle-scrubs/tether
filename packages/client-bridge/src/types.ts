import type {
  ApprovalTarget,
  ScheduleWindow,
  SessionEvent as ProtocolSessionEvent,
  TaskListStatus as ProtocolTaskListStatus,
  TaskApprovalRecord,
  TaskRecord as ProtocolTaskRecord,
} from "@dungle-scrubs/tether-protocol";
import type {
  RestParticipantControlClient,
  RestParticipantControlClientDebugInfo,
} from "@dungle-scrubs/tether-client";
import type { z } from "zod";

import type { taskContractRecordSchema } from "./schemas.js";
import type { ClientBridgeFetch } from "./transport.js";

/** Durable Tether session selected by a client bridge. */
export interface ClientBridgeSession {
  /** Creation timestamp returned by Tether. */
  readonly createdAt: string;
  /** Durable Tether session id. */
  readonly sessionId: string;
}

/** Durable binding between an external client conversation and a Tether session. */
export interface ClientBridgeSessionBinding {
  /** Nullable archive timestamp returned by Tether. */
  readonly archivedAt: string | null;
  /** Creation timestamp returned by Tether. */
  readonly createdAt: string;
  /** Provider-specific conversation id, such as an external chat id. */
  readonly externalId: string;
  /** Most recent time Tether observed this binding. */
  readonly lastSeenAt: string;
  /** External client provider, such as `external-chat` or `slack`. */
  readonly provider: string;
  /** Durable Tether session associated with the external conversation. */
  readonly sessionId: string;
}

/** Result returned after resolving or creating a session binding. */
export interface ClientBridgeResolveSessionResult {
  /** Durable client/session binding. */
  readonly binding: ClientBridgeSessionBinding;
  /** Whether Tether created the binding during this resolution. */
  readonly created: boolean;
  /** Durable session selected by the binding. */
  readonly session: ClientBridgeSession;
}

/** Durable task record returned by Tether's client-facing task endpoints. */
export type ClientBridgeTaskRecord = ProtocolTaskRecord;

/** Durable session event returned by Tether's client-facing event endpoints. */
export type ClientBridgeSessionEvent = ProtocolSessionEvent;

/** Participant task contract record returned by Tether inspection endpoints. */
export type ClientBridgeTaskContractRecord = z.infer<typeof taskContractRecordSchema>;

/** Task row plus the currently advertised matching contract, when available. */
export interface ClientBridgeTaskInspection {
  /** Current advertised contract for the task kind, or null when no participant advertises it. */
  readonly contract: ClientBridgeTaskContractRecord | null;
  /** Durable task record returned by Tether. */
  readonly task: ClientBridgeTaskRecord;
}

/** Task list filter accepted by Tether's client-facing task endpoint. */
export type ClientBridgeTaskListStatus = ProtocolTaskListStatus;

/** Runtime settings for generic client bridge task operations. */
export interface ClientBridgeTaskClientConfig {
  /** Bearer token sent to Tether; falls back to SERVICE_AUTH_TOKEN/TETHER_AUTH_TOKEN when omitted. */
  readonly authToken?: string | null;
  /** Participant lifecycle identity used by cancellation and approval. */
  readonly control?: {
    readonly capabilities?: Readonly<Record<string, unknown>>;
    readonly displayName?: string;
    readonly instanceId: string;
    readonly participantId: string;
    readonly runtimeKind: string;
  };
  /** Tether service URL. */
  readonly serviceUrl: string;
}

/** Optional dependencies for generic client bridge task operations. */
export interface ClientBridgeTaskClientOptions {
  /** Prebuilt lifecycle client for focused composition tests. */
  readonly controlClient?: RestParticipantControlClient;
  /** Fetch implementation, injected by tests and host runtimes. */
  readonly fetch?: ClientBridgeFetch;
}

/** Runtime diagnostics for a generic client bridge task client. */
export interface ClientBridgeTaskClientDebugInfo {
  /** Bounded lifecycle state when protected operations are configured. */
  readonly control?: RestParticipantControlClientDebugInfo;
  /** Number of HTTP requests issued to Tether by this task client. */
  readonly requestCount: number;
}

/** Runtime settings for generic client bridge session event reads. */
export interface ClientBridgeSessionEventClientConfig {
  /** Bearer token sent to Tether; falls back to SERVICE_AUTH_TOKEN/TETHER_AUTH_TOKEN when omitted. */
  readonly authToken?: string | null;
  /** Tether service URL. */
  readonly serviceUrl: string;
}

/** Optional dependencies for generic client bridge session event reads. */
export interface ClientBridgeSessionEventClientOptions {
  /** Fetch implementation, injected by tests and host runtimes. */
  readonly fetch?: ClientBridgeFetch;
}

/** Options for listing durable session events. */
export interface ClientBridgeListSessionEventsOptions {
  /** Event sequence cursor to start after. Defaults to zero. */
  readonly afterSeq?: number;
}

/** Runtime diagnostics for a generic client bridge session event client. */
export interface ClientBridgeSessionEventClientDebugInfo {
  /** Number of HTTP requests issued to Tether by this event client. */
  readonly requestCount: number;
}

/** Input for creating one Tether task from an external client bridge. */
export interface ClientBridgeCreateTaskInput {
  /** Structured task input supplied by a coordinator or client, when available. */
  readonly input?: Record<string, unknown> | null;
  /** Task kind used by participant runtimes to decide claimability. */
  readonly kind: string;
  /** User-visible task objective. */
  readonly objective: string;
  /** Require an active participant contract matching this task kind. */
  readonly requireContract?: boolean;
  /** Optional deterministic task id for test-support callers. */
  readonly taskId?: string;
}

/**
 * Input for ensuring one deterministic scheduled maintenance run. The bridge
 * supplies the current Schedule Window and opaque scope key; the client derives the
 * stable task id and drives the existing task-creation idempotency seam so a
 * repeated tick returns the existing run rather than racing a read-then-create.
 */
export interface ClientBridgeCreateScheduledTaskInput {
  /** Structured task input supplied by the scheduler, when available. */
  readonly input?: Record<string, unknown> | null;
  /** Task kind used by participant runtimes to decide claimability. */
  readonly kind: string;
  /** User-visible task objective. */
  readonly objective: string;
  /** Current deterministic Schedule Window the run belongs to. */
  readonly scheduleWindow: ScheduleWindow;
  /** Opaque participant-owned recurring-work scope key. */
  readonly scopeKey: string;
}

/** Input for cancelling one Tether task from an external client bridge. */
export interface ClientBridgeCancelTaskInput {
  /** Optional bridge-specific cancellation context. */
  readonly reason?: Record<string, unknown>;
  /** Durable task id to cancel. */
  readonly taskId: string;
}

/** Input for recording approval intent for one durable task. */
export interface ClientBridgeRecordTaskApprovalInput {
  /** Approval decision to record. */
  readonly decision: "approved" | "rejected";
  /** Optional bridge-specific approval context. */
  readonly reason?: Record<string, unknown>;
  /** Opaque target that must exactly match the completed task manifest. */
  readonly target?: ApprovalTarget;
  /** Durable task id to approve or reject. */
  readonly taskId: string;
}

/** Result returned after recording new approval intent. */
export interface ClientBridgeTaskApprovalRecorded {
  /** Canonical first-committer-wins approval row. */
  readonly approval: TaskApprovalRecord;
  /** Approval decision recorded by Tether. */
  readonly decision: "approved" | "rejected";
  /** Durable approval event returned by Tether. */
  readonly event: ClientBridgeSessionEvent;
  /** Durable approval event id. */
  readonly eventId: string;
  /** Approval result status. */
  readonly status: "recorded";
  /** Durable task the approval decision references. */
  readonly task: ClientBridgeTaskRecord;
}

/** Result returned when approval intent already exists. */
export interface ClientBridgeTaskApprovalIgnored {
  /** Canonical first-committer-wins approval row. */
  readonly approval: TaskApprovalRecord;
  /** Approval decision requested by the bridge. */
  readonly decision: "approved" | "rejected";
  /** Existing durable decision for this task. */
  readonly existingDecision: "approved" | "rejected";
  /** Stable reason no new approval event was appended. */
  readonly ignoredReason: "already_approved" | "already_rejected";
  /** Approval result status. */
  readonly status: "ignored";
  /** Durable task the existing decision references. */
  readonly task: ClientBridgeTaskRecord;
}

/** Result returned after attempting to record approval intent. */
export type ClientBridgeTaskApprovalRecord =
  | ClientBridgeTaskApprovalIgnored
  | ClientBridgeTaskApprovalRecorded;

/** Runtime settings for provider conversation session resolution. */
export interface ClientBridgeSessionResolverConfig {
  /** Bearer token sent to Tether; falls back to SERVICE_AUTH_TOKEN/TETHER_AUTH_TOKEN when omitted. */
  readonly authToken?: string | null;
  /**
   * Maximum number of external-id → session-id entries retained by the bounded
   * LRU cache. Must be a positive finite integer; the resolver never runs
   * unbounded. Defaults to 1,024.
   */
  readonly cacheCapacity?: number;
  /** Optional session id that new bindings should target. */
  readonly defaultSessionId?: string | null;
  /**
   * Idle time-to-live, in milliseconds, after which an untouched cache entry is
   * treated as a miss and evicted. Must be positive and finite. Defaults to
   * 1,800,000 (30 minutes).
   */
  readonly idleTtlMs?: number;
  /**
   * Maximum number of distinct in-flight resolutions allowed at once. Must be a
   * positive finite integer; a new distinct key beyond this cap is rejected with
   * a {@link ClientBridgeSessionResolverResourceLimitError}. Defaults to 64.
   */
  readonly maxInFlight?: number;
  /** External client provider, such as `external-chat` or `slack`. */
  readonly provider: string;
  /** Tether service URL. */
  readonly serviceUrl: string;
}

/** Monotonic-enough clock used to drive deterministic cache expiry. */
export interface ClientBridgeSessionResolverClock {
  /** Returns the current time in epoch milliseconds. */
  readonly now: () => number;
}

/** Optional dependencies for bridge session resolution. */
export interface ClientBridgeSessionResolverOptions {
  /** Clock used for idle-expiry decisions; defaults to `Date.now`. */
  readonly clock?: ClientBridgeSessionResolverClock;
  /** Fetch implementation, injected by tests and host runtimes. */
  readonly fetch?: ClientBridgeFetch;
}

/** Most recent bounded-cache outcome observed by a bridge session resolver. */
export type ClientBridgeSessionResolverOutcome =
  | "cache-hit"
  | "evicted"
  | "expired"
  | "in-flight-joined"
  | "in-flight-overflow"
  | "invalidated"
  | "resolved"
  | "seeded";

/** Runtime diagnostics for a bridge session resolver. */
export interface ClientBridgeSessionResolverDebugInfo {
  /** Maximum number of cache entries retained before LRU eviction. */
  readonly cacheCapacity: number;
  /** Number of external ids currently cached by this resolver. */
  readonly cacheSize: number;
  /** Most recent session id resolved by this process. */
  readonly currentSessionId: string | null;
  /** Number of entries dropped by LRU eviction over this resolver's lifetime. */
  readonly evictionCount: number;
  /** Number of entries dropped by idle expiry over this resolver's lifetime. */
  readonly expiryCount: number;
  /** Number of distinct resolutions currently in flight. */
  readonly inFlightCount: number;
  /** Most recent bounded-cache outcome, or null before any activity. */
  readonly lastOutcome: ClientBridgeSessionResolverOutcome | null;
  /** Maximum number of distinct in-flight resolutions allowed at once. */
  readonly maxInFlight: number;
  /** Number of HTTP requests issued to Tether by this resolver. */
  readonly requestCount: number;
}
