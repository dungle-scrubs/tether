import type {
  ControlEpochGuard,
  ControlLeaseClaim,
  ControlLeaseRenewal,
  CreateSessionResult,
  EnsureScheduledRunInput,
  EnsureScheduledRunResult,
  HeartbeatParticipantWithEventResult,
  ParticipantRegistration,
  PersistedEventAppendResult,
  PersistedParticipantRegistrationResult,
  PersistedTaskApprovalResult,
  PersistedTaskClaimResult,
  PersistedTaskCreateResult,
  PersistedTaskEventResult,
  RestControlAcquisition,
  RestControlLeaseRelease,
  ScheduledTaskIdentityInput,
  SupersededScheduledRunsResult,
  SupersedeScheduledRunsInput,
} from "./db.js";
import type { AppendSessionEventInput, ApprovalDecision } from "./protocol.js";
import type {
  ClientSessionBindingRecord,
  ControlLeaseSnapshot,
  ParticipantRecord,
  ParticipantRuntimeKind,
  ParticipantRuntimeSnapshot,
  SessionDebugSummary,
  SessionEvent,
  SessionEventListOptions,
  SessionListItem,
  SessionRecord,
  TaskListStatus,
  TaskRecord,
  TaskSnapshot,
} from "./types.js";

export type { PersistedTaskApprovalResult };

/** Focused persistence stores used by the session service boundary. */
export interface SessionPersistenceStores {
  /** External conversation to Tether session binding persistence. */
  readonly clientBindings: ClientBindingStore;
  /** Participant control-lease persistence and diagnostics. */
  readonly controlLeases: ControlLeaseStore;
  /** Durable session event-log persistence. */
  readonly events: SessionEventStore;
  /** Participant presence and runtime snapshot persistence. */
  readonly participants: ParticipantStore;
  /** Session record and aggregate debug-summary persistence. */
  readonly sessions: SessionStore;
  /** Durable task lifecycle persistence. */
  readonly tasks: TaskStore;
}

/** Persistence for external client/session bindings. */
export interface ClientBindingStore {
  readonly archive: (input: {
    readonly externalId: string;
    readonly provider: string;
  }) => Promise<ClientSessionBindingRecord | null>;
  readonly find: (input: {
    readonly externalId: string;
    readonly provider: string;
  }) => Promise<ClientSessionBindingRecord | null>;
  readonly list: (input?: {
    readonly provider?: string | undefined;
  }) => Promise<ClientSessionBindingRecord[]>;
  readonly upsert: (input: {
    readonly externalId: string;
    readonly provider: string;
    readonly sessionId?: string | undefined;
  }) => Promise<ClientSessionBindingUpsertResult>;
}

/** Internal binding lifecycle decided by the atomic persistence mutation. */
export type ClientBindingLifecycleStatus = "inserted" | "rebound" | "refreshed";

/**
 * Result from resolving an external binding. Public `created` remains true only
 * for a newly inserted active binding, while `status` keeps internal lifecycle
 * semantics available to the service boundary.
 */
export type ClientSessionBindingUpsertResult =
  | {
      readonly binding: ClientSessionBindingRecord;
      readonly created: true;
      readonly sessionCreated: boolean;
      readonly status: "inserted";
    }
  | {
      readonly binding: ClientSessionBindingRecord;
      readonly created: false;
      readonly sessionCreated: boolean;
      readonly status: Exclude<ClientBindingLifecycleStatus, "inserted">;
    };

/** Persistence for participant control leases. */
export interface ControlLeaseStore {
  /** Atomically acquires REST control with participant registration effects. */
  readonly acquireRest?: (input: {
    readonly acquisitionId: string;
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly eventSourceId: string;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly runtimeKind: string;
    readonly sessionId: string;
  }) => Promise<RestControlAcquisition>;
  /** Acquires or supersedes control, advancing the Control Epoch on reconnect. */
  readonly claim: (input: {
    readonly controlChannel: "rest" | "ws";
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Promise<ControlLeaseClaim>;
  readonly listSnapshots: (sessionId: string) => Promise<ControlLeaseSnapshot[]>;
  /** Renews an existing lease, comparing the Control Epoch without advancing it. */
  readonly renew: (input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Promise<ControlLeaseRenewal>;
  readonly release: (input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Promise<boolean | undefined>;
  /** Atomically classifies and releases an exact current REST generation. */
  readonly releaseRest: (input: {
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Promise<RestControlLeaseRelease>;
}

/** Persistence for durable session events. */
export interface SessionEventStore {
  readonly append: (
    input: AppendSessionEventInput,
    options: {
      readonly controlGuard?: ControlEpochGuard | undefined;
      readonly sourceId: string;
    },
  ) => Promise<SessionEvent>;
  readonly appendIdempotent: (
    input: AppendSessionEventInput,
    options: {
      readonly controlGuard?: ControlEpochGuard | undefined;
      readonly sourceId: string;
    },
  ) => Promise<PersistedEventAppendResult>;
  readonly list: (
    sessionId: string,
    afterSeq: number,
    options?: SessionEventListOptions,
  ) => Promise<SessionEvent[]>;
  /** Reads a bounded newest tail plus aggregate eligible-suffix accounting. */
  readonly listContextSuffix: (
    sessionId: string,
    afterSeq: number,
    limit: number,
  ) => Promise<{
    readonly eligibleEventCount: number;
    readonly estimatedTokens: number;
    readonly events: readonly SessionEvent[];
    readonly truncated: boolean;
  }>;
}

/** Persistence for participant presence and runtime diagnostics. */
export interface ParticipantStore {
  readonly heartbeat: (input: {
    readonly capabilities?: Record<string, unknown>;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Promise<ParticipantRecord | null>;
  /**
   * Refreshes presence and appends the durable heartbeat event in one atomic
   * transaction, fenced by the Control Epoch guard when one is supplied so a
   * stale-epoch heartbeat cannot refresh presence or capabilities.
   */
  readonly heartbeatWithEvent: (input: {
    readonly capabilities?: Record<string, unknown> | undefined;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Promise<HeartbeatParticipantWithEventResult>;
  readonly list: (sessionId: string) => Promise<ParticipantRecord[]>;
  readonly listRuntimeSnapshots: (sessionId: string) => Promise<ParticipantRuntimeSnapshot[]>;
  readonly upsert: (input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly participantId: string;
    readonly runtimeKind: ParticipantRuntimeKind;
    readonly sessionId: string;
  }) => Promise<ParticipantRegistration>;
  readonly upsertWithEvent: (input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly runtimeKind: ParticipantRuntimeKind;
    readonly sessionId: string;
  }) => Promise<PersistedParticipantRegistrationResult>;
}

/** Persistence for session records and aggregate diagnostics. */
export interface SessionStore {
  readonly create: (sessionId: string) => Promise<CreateSessionResult>;
  readonly delete: (sessionId: string) => Promise<boolean>;
  readonly list: () => Promise<SessionListItem[]>;
  readonly read: (sessionId: string) => Promise<SessionRecord>;
  readonly readDebugSummary: (sessionId: string) => Promise<SessionDebugSummary>;
}

/** Persistence for durable task lifecycle state. */
export interface TaskStore {
  readonly recordApproval: (input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly decision: ApprovalDecision;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly reason: Record<string, unknown>;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<PersistedTaskApprovalResult | null>;
  readonly claimWithEvent: (input: {
    readonly claimLeaseTtlMs: number;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<PersistedTaskClaimResult>;
  readonly completeWithEvent: (input: {
    readonly claimId: string;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly result: Record<string, unknown>;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<PersistedTaskEventResult>;
  readonly createWithEvent: (input: {
    readonly eventSourceId: string;
    readonly input?: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly schedule?: ScheduledTaskIdentityInput | undefined;
    readonly sessionId: string;
    readonly taskId: string;
    readonly taskIdSource: "caller" | "generated";
  }) => Promise<PersistedTaskCreateResult>;
  readonly expireClaims: (input: {
    readonly batchSize: number;
    readonly sourceId: string;
  }) => Promise<SessionEvent[]>;
  readonly failWithEvent: (input: {
    readonly claimId: string;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly failure: Record<string, unknown>;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<PersistedTaskEventResult>;
  readonly get: (input: {
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<TaskRecord | null>;
  readonly list: (sessionId: string, status?: TaskListStatus) => Promise<TaskRecord[]>;
  readonly listSnapshots: (sessionId: string) => Promise<TaskSnapshot[]>;
  readonly refreshClaim: (input: {
    readonly claimId: string;
    readonly claimLeaseTtlMs: number;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<TaskRecord | null>;
  readonly releaseWithEvent: (input: {
    readonly claimId: string;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<PersistedTaskEventResult>;
  readonly cancelWithEvent: (input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly reason?: Record<string, unknown> | undefined;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Promise<PersistedTaskEventResult>;
  /**
   * Atomically supersedes older scheduled runs matching one schedule identity.
   * Kept distinct from `cancelWithEvent` so scheduled supersession authorization
   * never folds into generic task cancellation.
   */
  readonly supersedeScheduled: (
    input: SupersedeScheduledRunsInput,
  ) => Promise<SupersededScheduledRunsResult>;
  /**
   * Atomically supersedes older scheduled runs and then inserts-or-replays the
   * current deterministic run in one transaction. Rejects a supplied task id that
   * does not match the derived scheduled identity.
   */
  readonly ensureScheduledRun: (
    input: EnsureScheduledRunInput,
  ) => Promise<EnsureScheduledRunResult>;
}
