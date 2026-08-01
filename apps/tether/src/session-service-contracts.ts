import type { Effect } from "effect";
import type {
  SessionScalabilityDebugRecord,
  SessionScalabilityHealthWarning,
} from "@dungle-scrubs/tether-protocol";

import type {
  ApprovalTargetManifestErrorReason,
  ControlEpochGuard,
  ControlLeaseClaim,
  ParticipantRegistration,
  PermanentSessionDeleteResult,
  PersistedEventAppendResult,
  PersistedTaskCreateResult,
  OperatorGrantAuthorityErrorReason,
  OperatorCommandTaskAuthority,
  ScheduledTaskIdentityInput,
} from "./db.js";

export type { ControlEpochGuard };

import type { ClientBindingLifecycleStatus } from "./db-store-contracts.js";
import type {
  BoundaryDebugInfo,
  ModuleObservability,
  ModuleObservabilityOptions,
} from "./observability.js";
import type { ApprovalDecision } from "./protocol.js";
import type { RestControlPolicyDebugInfo } from "./rest-control-policy.js";
import type {
  ApprovalTarget,
  ClientSessionBindingRecord,
  ControlChannel,
  ControlLeaseSnapshot,
  ParticipantRecord,
  ParticipantRuntimeKind,
  ParticipantRuntimeSnapshot,
  ParticipantTaskContractRecord,
  ScheduledMaintenanceIdentity,
  ScheduledSupersessionRefusalReason,
  ScheduleWindow,
  SessionContextView,
  SessionContextViewRequest,
  SessionDebugSummary,
  SessionEvent,
  SessionEventListOptions,
  SessionListItem,
  SessionRecord,
  TaskListStatus,
  TaskApprovalRecord,
  TaskRecord,
  TaskSnapshot,
} from "./types.js";

export type { ScheduledMaintenanceIdentity, ScheduleWindow };

export const restControlLeaseTtlMs = 60_000;
export const taskClaimLeaseTtlMs = 30_000;
export const wsControlLeaseTtlMs = 3_600_000;

export type ControlLeaseConflict = Extract<ControlLeaseClaim, { readonly status: "conflict" }>;

/** Tagged failure used when a session service persistence operation rejects. */
export class SessionServicePersistenceError extends Error {
  readonly _tag = "SessionServicePersistenceError";

  constructor(
    readonly operation: string,
    readonly cause: unknown,
  ) {
    super(`Session service persistence failed during ${operation}`);
  }
}

/** Reason an approval decision was not recorded. */
export type TaskApprovalRejectionReason =
  | ApprovalTargetManifestErrorReason
  | OperatorGrantAuthorityErrorReason
  | "invalid_approval_plan"
  | "task_not_completed"
  | "task_not_found"
  | "unsupported_task_kind";

/** Reason an approval decision was ignored instead of appended. */
export type TaskApprovalIgnoredReason = "already_approved" | "already_rejected";

export const defaultRecentTerminalTaskLimit = 10;

/**
 * Runtime instance identity that currently owns a participant control channel.
 */
export interface ParticipantControlContext {
  /** Server-bound Control Epoch issued when this socket acquired control. */
  readonly controlEpoch: number;
  readonly instanceId: string;
  readonly participantId: string;
}

/**
 * Rejection returned when a control-protected request carries a missing,
 * invalid, or fenced Control Epoch. Surfaced as the CONTROL_EPOCH_STALE code.
 */
export interface ControlEpochStaleResult {
  /** Current durable generation, or null when no lease is current. */
  readonly currentEpoch: number | null;
  readonly status: "control_epoch_stale";
}

/** Missing Control Epoch rejected before a protected mutation in enforced mode. */
export interface ControlEpochRequiredResult {
  readonly status: "control_epoch_required";
}

/** Missing Acquisition ID rejected at REST registration in enforced mode. */
export interface ControlAcquisitionIdRequiredResult {
  readonly status: "control_acquisition_id_required";
}

/** Inactive Acquisition ID rejected without changing immutable history. */
export interface ControlAcquisitionStaleResult {
  readonly status: "control_acquisition_stale";
}

/**
 * Outcome of validating REST participant control before a protected mutation.
 */
export type RestControlOutcome =
  | { readonly leaseExpiresAt?: string | null; readonly status: "ok" }
  | {
      readonly leaseClaim: ControlLeaseConflict;
      readonly status: "control_conflict";
    }
  | ControlEpochRequiredResult
  | ControlEpochStaleResult;

/**
 * Runtime diagnostics for the durable session service boundary.
 */
export interface SessionServiceDebugInfo extends BoundaryDebugInfo {
  /** Process-local identifier used to tag emitted event fanout notifications. */
  readonly eventSourceId: string;
  readonly restControlLeaseTtlMs: number;
  /** Content-free scalability diagnostic boundary counters and last failure. */
  readonly scalability?: BoundaryDebugInfo & {
    readonly rawOnlyCount: number;
    readonly summaryBackedCount: number;
  };
  /** Bounded REST participant-control policy state. */
  readonly restControl?: RestControlPolicyDebugInfo;
  readonly taskClaimLeaseTtlMs: number;
  readonly wsControlLeaseTtlMs: number;
}

/**
 * Domain-owned approval validator for one task kind.
 */
export interface TaskApprovalValidator {
  /** Task kind this validator handles. */
  readonly taskKind: string;
  /** Returns whether the completed task result can receive an approval decision. */
  readonly validate: (task: TaskRecord) => boolean;
}

/**
 * Optional observability configuration for SessionService.
 */
export interface SessionServiceOptions {
  /** Domain validators that decide whether a completed task result is approvable. */
  readonly approvalValidators?: readonly TaskApprovalValidator[];
  /** Whether a control-protected REST request missing its Control Epoch is rejected. */
  readonly controlEpochEnforcement?: boolean;
  /** Optional source id override for deterministic fanout tests. */
  readonly eventSourceId?: string;
  readonly observability?: ModuleObservabilityOptions;
  readonly taskClaimLeaseTtlMs?: number;
  readonly wsControlLeaseTtlMs?: number;
}

/**
 * Effect-native durable session service boundary. The Promise-based
 * SessionService class delegates common operations here while existing callers
 * continue to use the current API.
 */
export interface SessionServiceEffect {
  /** Returns inspectable runtime state for the service boundary. */
  readonly debugInfo: () => SessionServiceDebugInfo;
  /** Creates a session and appends the canonical creation event. */
  readonly createSession: (input: {
    readonly sessionId: string | undefined;
  }) => Effect.Effect<SessionCreatedResult, SessionServiceFailure>;
  /** Ensures a public REST session exists without appending lifecycle events. */
  readonly ensurePublicSession: (input: {
    readonly sessionId: string | undefined;
  }) => Effect.Effect<PublicSessionEnsureResult, SessionServiceFailure>;
  /**
   * Permanently deletes one eligible session. Route-level compatibility checks
   * remain advisory; eligibility is re-verified inside the delete transaction
   * under row locks, with the optional `hasLiveHost` probe re-checking
   * process-local Host Presence between the locks and the delete.
   */
  readonly deleteSession: (input: {
    readonly hasLiveHost?: (() => boolean) | undefined;
    readonly sessionId: string;
  }) => Effect.Effect<PermanentSessionDeleteResult, SessionServiceFailure>;
  /** Lists every session with aggregate activity counts for the operator UI. */
  readonly listSessions: () => Effect.Effect<SessionListItem[], SessionServiceFailure>;
  /** Resolves an external client conversation to a durable Tether session. */
  readonly resolveClientSession: (
    input: ResolveClientSessionInput,
  ) => Effect.Effect<ClientSessionBindingResult, SessionServiceFailure>;
  /** Lists active external client bindings for startup recovery and diagnostics. */
  readonly listClientSessionBindings: (input?: {
    readonly provider?: string | undefined;
  }) => Effect.Effect<ClientSessionBindingRecord[], SessionServiceFailure>;
  /** Archives one external client binding. */
  readonly archiveClientSessionBinding: (input: {
    readonly externalId: string;
    readonly provider: string;
  }) => Effect.Effect<ClientSessionBindingRecord | null, SessionServiceFailure>;
  /** Lists session events after the supplied sequence cursor. */
  readonly listEvents: (
    sessionId: string,
    afterSeq: number,
    options?: SessionEventListOptions,
  ) => Effect.Effect<SessionEvent[], SessionServiceFailure>;
  /** Lists visible participants for a session. */
  readonly listParticipants: (
    sessionId: string,
    options?: {
      readonly before?: Pick<ParticipantRecord, "lastSeenAt" | "participantId"> | undefined;
      readonly limit?: number | undefined;
    },
  ) => Effect.Effect<ParticipantRecord[], SessionServiceFailure>;
  /** Lists durable tasks for a session and lifecycle filter. */
  readonly listTasks: (
    sessionId: string,
    status?: TaskListStatus,
    options?: {
      readonly before?: Pick<TaskRecord, "createdAt" | "taskId"> | undefined;
      readonly limit?: number | undefined;
    },
  ) => Effect.Effect<TaskRecord[], SessionServiceFailure>;
  /** Lists normalized participant task contracts advertised in one session. */
  readonly listParticipantTaskContracts: (
    sessionId: string,
  ) => Effect.Effect<ParticipantTaskContractRecord[], SessionServiceFailure>;
  /** Finds the first active participant contract for one task kind. */
  readonly findParticipantTaskContract: (input: {
    readonly sessionId: string;
    readonly taskKind: string;
  }) => Effect.Effect<ParticipantTaskContractRecord | null, SessionServiceFailure>;
  /** Lists all active participant contracts for one task kind. */
  readonly listParticipantTaskContractsByKind: (input: {
    readonly sessionId: string;
    readonly taskKind: string;
  }) => Effect.Effect<ParticipantTaskContractRecord[], SessionServiceFailure>;
  /** Lists read-only participant runtime diagnostics for one session. */
  readonly listParticipantRuntimeSnapshots: (
    sessionId: string,
  ) => Effect.Effect<ParticipantRuntimeSnapshot[], SessionServiceFailure>;
  /** Builds a deterministic bounded context packet. */
  readonly buildSessionContextView: (
    input: SessionContextViewRequest,
  ) => Effect.Effect<SessionContextView, SessionServiceFailure>;
  /** Reads one durable task in a session. */
  readonly getTask: (
    sessionId: string,
    taskId: string,
  ) => Effect.Effect<TaskRecord | null, SessionServiceFailure>;
  /** Lists read-only task diagnostics for one session. */
  readonly listTaskSnapshots: (
    sessionId: string,
  ) => Effect.Effect<TaskSnapshot[], SessionServiceFailure>;
  /** Reads aggregate read-only debug counts for one session. */
  readonly readSessionDebugSummary: (
    sessionId: string,
  ) => Effect.Effect<SessionDebugSummary, SessionServiceFailure>;
  /** Reads content-free scalability diagnostics for one session. */
  readonly readSessionScalabilityDebug: (
    sessionId: string,
  ) => Effect.Effect<SessionScalabilityDebugRecord, SessionServiceFailure>;
  /** Reads aggregate scalability warnings for the health projection. */
  readonly readScalabilityHealthWarnings: () => Effect.Effect<
    readonly SessionScalabilityHealthWarning[],
    SessionServiceFailure
  >;
  /** Lists read-only control-lease diagnostics for one session. */
  readonly listControlLeaseSnapshots: (
    sessionId: string,
  ) => Effect.Effect<ControlLeaseSnapshot[], SessionServiceFailure>;
  /** Creates a task and appends the canonical task-created event. */
  readonly createTask: (
    input: CreateTaskInput,
  ) => Effect.Effect<TaskCreatedResult, SessionServiceFailure>;
  /** Expires elapsed task claim leases and returns committed expiry events. */
  readonly expireTaskClaims: (input: {
    readonly batchSize: number;
  }) => Effect.Effect<TaskClaimsExpiredResult, SessionServiceFailure>;
  /**
   * Atomically supersedes older scheduled runs matching one schedule identity
   * and returns typed refusals for inspected candidates that were not eligible.
   */
  readonly supersedeScheduledRuns: (
    input: SupersedeScheduledRunsRequest,
  ) => Effect.Effect<ScheduledSupersessionResult, SessionServiceFailure>;
  /**
   * Atomically supersedes eligible older scheduled runs and then inserts-or-
   * replays the current deterministic run in one transaction, so the current run
   * is never claimable before older runs are cancelled.
   */
  readonly ensureScheduledRun: (
    input: EnsureScheduledRunRequest,
  ) => Effect.Effect<ScheduledRunEnsureResult, SessionServiceFailure>;
  /** Claims a task and appends the canonical claim event. */
  readonly claimTask: (
    input: TaskParticipantInput,
  ) => Effect.Effect<TaskMutationResult, SessionServiceFailure>;
  /** Claims a task over REST after refreshing participant control. */
  readonly claimTaskOverRest: (
    input: RestControlledInput & TaskParticipantInput,
  ) => Effect.Effect<RestTaskMutationResult, SessionServiceFailure>;
  /** Refreshes an active task claim without appending a visible event. */
  readonly refreshTaskClaim: (
    input: ClaimOwnedTaskInput,
  ) => Effect.Effect<TaskClaimRefreshResult, SessionServiceFailure>;
  /** Refreshes a task claim over REST after refreshing participant control. */
  readonly refreshTaskClaimOverRest: (
    input: RestControlledInput & ClaimOwnedTaskInput,
  ) => Effect.Effect<RestTaskClaimRefreshResult, SessionServiceFailure>;
  /** Cancels a task and appends the canonical cancellation event. */
  readonly cancelTask: (
    input: CancelTaskInput,
  ) => Effect.Effect<TaskMutationResult, SessionServiceFailure>;
  /** Cancels a task over REST after refreshing participant control. */
  readonly cancelTaskOverRest: (
    input: CancelTaskInput & RestControlledInput,
  ) => Effect.Effect<RestTaskMutationResult, SessionServiceFailure>;
  /** Completes a task and appends the canonical completion event. */
  readonly completeTask: (
    input: CompleteTaskInput,
  ) => Effect.Effect<TaskMutationResult, SessionServiceFailure>;
  /** Completes a task over REST after refreshing participant control. */
  readonly completeTaskOverRest: (
    input: CompleteTaskInput & RestControlledInput,
  ) => Effect.Effect<RestTaskMutationResult, SessionServiceFailure>;
  /** Fails a task and appends the canonical failure event. */
  readonly failTask: (
    input: FailTaskInput,
  ) => Effect.Effect<TaskMutationResult, SessionServiceFailure>;
  /** Fails a task over REST after refreshing participant control. */
  readonly failTaskOverRest: (
    input: FailTaskInput & RestControlledInput,
  ) => Effect.Effect<RestTaskMutationResult, SessionServiceFailure>;
  /** Releases a task claim and appends the canonical release event. */
  readonly releaseTask: (
    input: ClaimOwnedTaskInput,
  ) => Effect.Effect<TaskMutationResult, SessionServiceFailure>;
  /** Releases a task over REST after refreshing participant control. */
  readonly releaseTaskOverRest: (
    input: RestControlledInput & ClaimOwnedTaskInput,
  ) => Effect.Effect<RestTaskMutationResult, SessionServiceFailure>;
  /** Records an approval decision as a durable session event. */
  readonly recordTaskApproval: (
    input: RecordTaskApprovalInput,
  ) => Effect.Effect<TaskApprovalResult, SessionServiceFailure>;
  /** Records task approval over REST after refreshing participant control. */
  readonly recordTaskApprovalOverRest: (
    input: RecordTaskApprovalInput & RestControlledInput,
  ) => Effect.Effect<RestTaskApprovalResult, SessionServiceFailure>;
  /** Publishes a generic session event. */
  readonly publishEvent: (
    input: PublishEventInput,
  ) => Effect.Effect<PublishedEventResult, SessionServiceFailure>;
  /** Publishes a generic REST event after optional control-lease refresh. */
  readonly publishRestEvent: (
    input: PublishRestEventInput,
  ) => Effect.Effect<ControlProtectedResult<PublishedEventResult>, SessionServiceFailure>;
  /** Registers or refreshes a REST-controlled participant. */
  readonly registerRestParticipant: (
    input: RegisterParticipantInput,
  ) => Effect.Effect<RestParticipantRegistrationResult, SessionServiceFailure>;
  /** Registers or refreshes a WebSocket-controlled participant. */
  readonly registerWebSocketParticipant: (
    input: RegisterWebSocketParticipantInput,
  ) => Effect.Effect<
    ControlProtectedResult<
      RegisteredParticipantResult & {
        readonly context: ParticipantControlContext;
      }
    >,
    SessionServiceFailure
  >;
  /** Refreshes a WebSocket control lease. */
  readonly refreshWebSocketControlLease: (input: {
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Effect.Effect<ControlProtectedResult<{ readonly refreshed: true }>, SessionServiceFailure>;
  /** Refreshes a REST participant heartbeat and emits a visible event when found. */
  readonly heartbeatRestParticipant: (
    input: HeartbeatParticipantInput,
  ) => Effect.Effect<ControlProtectedResult<RestHeartbeatParticipantResult>, SessionServiceFailure>;
  /** Releases a participant control lease. */
  readonly releaseControlLease: (input: {
    readonly controlChannel: ControlChannel;
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Effect.Effect<boolean, SessionServiceFailure>;
  /** Releases or classifies an exact REST participant control generation. */
  readonly releaseRestControlLease: (input: {
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) => Effect.Effect<RestControlReleaseResult, SessionServiceFailure>;
}

/**
 * Event append result with the singleton event also shaped as a broadcast list.
 */
export type PublishedEventResult = PersistedEventAppendResult;

/**
 * Participant registration result after durable presence state is upserted.
 */
export interface RegisteredParticipantResult {
  /** Server-issued Control Epoch the caller must echo on protected requests. */
  readonly controlEpoch: number;
  readonly events: readonly SessionEvent[];
  readonly participant: ParticipantRecord;
  readonly registrationStatus: ParticipantRegistration["status"];
  readonly status: "ok";
}

/** REST registration result including its retained lifecycle context. */
export interface RegisteredRestParticipantResult extends RegisteredParticipantResult {
  readonly acquisitionId: string;
  readonly acquisitionStatus: "claimed" | "replayed" | "superseded";
  readonly leaseExpiresAt: string;
  readonly renewAfterMs: number;
}

/**
 * Session creation result including the canonical creation event.
 */
export interface SessionCreatedResult {
  readonly events: readonly SessionEvent[];
  readonly session: SessionRecord;
}

/** Result of the public REST session ensure operation. */
export interface PublicSessionEnsureResult {
  readonly created: boolean;
  readonly session: SessionRecord;
}

/**
 * Result for resolving a client conversation to a durable session.
 */
export interface ClientSessionBindingResult {
  readonly binding: ClientSessionBindingRecord;
  /** Internal binding lifecycle classification used for service diagnostics. */
  readonly bindingStatus: ClientBindingLifecycleStatus;
  readonly created: boolean;
  readonly events: readonly SessionEvent[];
  readonly session: SessionRecord;
}

/**
 * Task creation result including the canonical task-created event.
 */
export type TaskCreatedResult = PersistedTaskCreateResult;

/**
 * Result for expiring elapsed task claim leases.
 */
export interface TaskClaimsExpiredResult {
  readonly events: readonly SessionEvent[];
  readonly expiredCount: number;
}

/**
 * Result for service methods that must respect participant control leases.
 */
export type ControlProtectedResult<TValue extends object> =
  | ({
      readonly events: readonly SessionEvent[];
      readonly status: "conflict" | "created" | "ok" | "replayed";
    } & TValue)
  | {
      readonly leaseClaim: ControlLeaseConflict;
      readonly status: "control_conflict";
    }
  | ControlEpochRequiredResult
  | ControlEpochStaleResult;

/** Typed result of the participant-scoped REST control release route. */
export type RestControlReleaseResult = ControlProtectedResult<{
  readonly released: boolean;
}>;

/** REST acquisition result including Acquisition-ID-specific rejection modes. */
export type RestParticipantRegistrationResult =
  | ControlProtectedResult<RegisteredRestParticipantResult>
  | ControlAcquisitionIdRequiredResult
  | ControlAcquisitionStaleResult;

/**
 * Result for task lifecycle mutations after the control channel has been
 * accepted or is not required.
 */
export type TaskMutationResult =
  | {
      // Single-event mutations (complete, fail, release, cancel) commit exactly
      // one event; an atomic claim that reclaims an elapsed claim commits the
      // ordered pair `task.claim_expired` then `task.claimed`, so this is a list.
      readonly events: readonly SessionEvent[];
      readonly status: "applied";
      readonly task: TaskRecord;
    }
  | {
      readonly events: readonly [];
      readonly status: "rejected";
      readonly task: null;
    };

/**
 * Result for refreshing an active task claim without producing a visible event.
 */
export type TaskClaimRefreshResult =
  | {
      readonly events: readonly [];
      readonly status: "applied";
      readonly task: TaskRecord;
    }
  | {
      readonly events: readonly [];
      readonly status: "rejected";
      readonly task: null;
    };

/**
 * REST task mutation result, including possible control-channel conflicts.
 */
export type RestTaskMutationResult =
  | TaskMutationResult
  | {
      readonly leaseClaim: ControlLeaseConflict;
      readonly status: "control_conflict";
    }
  | ControlEpochRequiredResult
  | ControlEpochStaleResult;

/**
 * REST task claim-refresh result, including possible control-channel conflicts.
 */
export type RestTaskClaimRefreshResult =
  | TaskClaimRefreshResult
  | {
      readonly leaseClaim: ControlLeaseConflict;
      readonly status: "control_conflict";
    }
  | ControlEpochRequiredResult
  | ControlEpochStaleResult;

/** Result for recording approval intent without mutating the task itself. */
export type TaskApprovalResult =
  | {
      readonly approval: TaskApprovalRecord;
      readonly decision: ApprovalDecision;
      readonly event: SessionEvent;
      readonly events: readonly [SessionEvent];
      readonly status: "recorded";
      readonly task: TaskRecord;
    }
  | {
      readonly approval: TaskApprovalRecord;
      readonly decision: ApprovalDecision;
      readonly events: readonly [];
      readonly existingDecision: ApprovalDecision;
      readonly ignoredReason: TaskApprovalIgnoredReason;
      readonly status: "ignored";
      readonly task: TaskRecord;
    }
  | {
      readonly decision: ApprovalDecision;
      readonly events: readonly [];
      readonly rejectionReason: TaskApprovalRejectionReason;
      readonly status: "rejected";
      readonly task: TaskRecord | null;
    };

export type AppliedTaskMutationResult = Extract<TaskMutationResult, { readonly status: "applied" }>;
export type AppliedTaskClaimRefreshResult = Extract<
  TaskClaimRefreshResult,
  { readonly status: "applied" }
>;
export type RecordedTaskApprovalResult = Extract<
  TaskApprovalResult,
  { readonly status: "recorded" }
>;

/** Tagged failure used inside the Effect boundary for rejected task mutations. */
export class TaskMutationRejectedError extends Error {
  readonly _tag = "TaskMutationRejected";

  constructor(readonly operation: string) {
    super(`${operation} was rejected`);
  }
}

/** Tagged failure used inside the Effect boundary for rejected claim refreshes. */
export class TaskClaimRefreshRejectedError extends Error {
  readonly _tag = "TaskClaimRefreshRejected";

  constructor() {
    super("Task claim refresh was rejected");
  }
}

/** Tagged failure used inside the Effect boundary for non-recordable approvals. */
export class TaskApprovalRejectedError extends Error {
  readonly _tag = "TaskApprovalRejected";

  constructor(
    readonly decision: ApprovalDecision,
    readonly rejectionReason: TaskApprovalRejectionReason,
    readonly task: TaskRecord | null,
  ) {
    super(`Task approval was rejected: ${rejectionReason}`);
  }
}

/** Tagged failure used inside the Effect boundary for duplicate approvals. */
export class TaskApprovalIgnoredError extends Error {
  readonly _tag = "TaskApprovalIgnored";

  constructor(
    readonly approval: TaskApprovalRecord,
    readonly decision: ApprovalDecision,
    readonly existingDecision: ApprovalDecision,
    readonly ignoredReason: TaskApprovalIgnoredReason,
    readonly task: TaskRecord,
  ) {
    super(`Task approval was ignored: ${ignoredReason}`);
  }
}

/** Typed failure channel for the Effect-native session service boundary. */
export type SessionServiceFailure =
  | SessionServicePersistenceError
  | TaskMutationRejectedError
  | TaskClaimRefreshRejectedError
  | TaskApprovalRejectedError
  | TaskApprovalIgnoredError;

/** REST approval result, including possible control-channel conflicts. */
export type RestTaskApprovalResult =
  | TaskApprovalResult
  | {
      readonly leaseClaim: ControlLeaseConflict;
      readonly status: "control_conflict";
    }
  | ControlEpochRequiredResult
  | ControlEpochStaleResult;

export interface RegisterParticipantInput {
  readonly acquisitionId: string | undefined;
  readonly capabilities: Record<string, unknown>;
  readonly displayName: string | undefined;
  readonly instanceId: string | undefined;
  readonly participantId: string | undefined;
  readonly runtimeKind: ParticipantRuntimeKind;
  readonly sessionId: string;
}

export interface RegisterWebSocketParticipantInput {
  readonly capabilities: Record<string, unknown>;
  readonly displayName: string;
  readonly instanceId: string;
  readonly participantId: string;
  readonly runtimeKind: ParticipantRuntimeKind;
  readonly sessionId: string;
}

export interface RestControlledInput {
  /** Server-issued Control Epoch echoed by the caller; undefined when legacy. */
  readonly controlEpoch?: number;
  readonly instanceId: string;
  readonly participantId: string;
  readonly sessionId: string;
}

export interface TaskParticipantInput {
  /**
   * Optional atomic Control Epoch fence validated in the same transaction as the
   * mutation. Present when the caller (WebSocket socket context or an
   * epoch-carrying REST request) supplies a server-issued epoch.
   */
  readonly controlGuard?: ControlEpochGuard | undefined;
  readonly participantId: string;
  readonly sessionId: string;
  readonly taskId: string;
}

/**
 * Participant input for a claim-owned mutation, fenced by the Claim ID minted on
 * the current claim generation. The value must equal the task's current
 * `claim_id` or the mutation is rejected.
 */
export interface ClaimOwnedTaskInput extends TaskParticipantInput {
  /** Server-issued Claim ID of the current claim generation. */
  readonly claimId: string;
}

export interface CompleteTaskInput extends ClaimOwnedTaskInput {
  readonly result: Record<string, unknown>;
}

export interface CancelTaskInput extends TaskParticipantInput {
  readonly reason: Record<string, unknown>;
}

export interface RecordTaskApprovalInput extends TaskParticipantInput {
  readonly decision: ApprovalDecision;
  readonly operatorGrantJti?: string | undefined;
  readonly reason: Record<string, unknown>;
  readonly target?: ApprovalTarget | undefined;
}

export interface StandardCreateTaskInput {
  readonly input: Record<string, unknown> | null;
  readonly kind: string;
  readonly objective: string;
  readonly operatorAuthority?: undefined;
  /** Deterministic provider-neutral identity for scheduled runs. */
  readonly schedule?: ScheduledTaskIdentityInput | undefined;
  readonly sessionId: string;
  readonly taskId: string | undefined;
}

export type CreateTaskInput =
  | StandardCreateTaskInput
  | {
      readonly input?: never;
      readonly kind?: never;
      readonly objective?: never;
      /** Transaction-owned browser command authority with server-derived durable task fields. */
      readonly operatorAuthority: OperatorCommandTaskAuthority;
      readonly schedule?: never;
      readonly taskId: string | undefined;
    };

/**
 * Request for one atomic scheduled-run supersession. The identity fixes the
 * current window; when `candidateTaskIds` is supplied, apply is additionally
 * bounded to exactly those reviewed ids and each candidate the atomic predicate
 * did not supersede is classified into a typed refusal.
 */
export interface SupersedeScheduledRunsRequest {
  /**
   * Optional task ids the operator reviewed. When present, apply is bounded to
   * these ids so a stale run that appeared after dry-run cannot be cancelled,
   * and any reviewed candidate not superseded is classified into a typed
   * refusal.
   */
  readonly candidateTaskIds?: readonly string[] | undefined;
  /** Current scheduled identity whose older matching runs are superseded. */
  readonly identity: ScheduledMaintenanceIdentity;
  /** Actor recorded on supersession cancellation events. */
  readonly participantId?: string | undefined;
  readonly reason?: Record<string, unknown> | undefined;
}

/** One refused candidate in a scheduled-supersession result. */
export interface ScheduledSupersessionRefusal {
  readonly reason: ScheduledSupersessionRefusalReason;
  readonly taskId: string;
}

/** Typed result of one atomic scheduled-run supersession. */
export interface ScheduledSupersessionResult {
  readonly events: readonly SessionEvent[];
  /** Candidate tasks the atomic predicate refused, with typed reasons. */
  readonly refusals: readonly ScheduledSupersessionRefusal[];
  readonly status: "applied";
  /** Older matching unclaimed nonterminal runs the operation cancelled. */
  readonly supersededTasks: readonly TaskRecord[];
}

/**
 * Request for one atomic ensure-scheduled-run operation. The identity fixes the
 * deterministic current run; `expectedTaskId`, when supplied, is verified against
 * the derived identity so a caller cannot smuggle in a mismatched scheduled id.
 */
export interface EnsureScheduledRunRequest {
  /** Optional caller-supplied task id verified against the derived identity. */
  readonly expectedTaskId?: string | undefined;
  readonly identity: ScheduledMaintenanceIdentity;
  readonly input?: Record<string, unknown> | null;
  readonly objective: string;
  /** Actor recorded on supersession cancellation events. */
  readonly participantId?: string | undefined;
  readonly reason?: Record<string, unknown> | undefined;
}

/** Typed result of one atomic ensure-scheduled-run operation. */
export interface ScheduledRunEnsureResult {
  /** True when the current-window run was inserted, false when it replayed. */
  readonly created: boolean;
  /** The current deterministic run after supersession of older runs. */
  readonly current: TaskRecord;
  /** All events to broadcast: supersession cancellations plus any created event. */
  readonly events: readonly SessionEvent[];
  readonly status: "ensured";
  /** Older matching unclaimed nonterminal runs the operation cancelled. */
  readonly supersededTasks: readonly TaskRecord[];
  /** Derived deterministic task id of the current run. */
  readonly taskId: string;
}

export interface ResolveClientSessionInput {
  readonly externalId: string;
  readonly provider: string;
  readonly sessionId: string | undefined;
}

export interface FailTaskInput extends ClaimOwnedTaskInput {
  readonly failure: Record<string, unknown>;
}

export interface PublishEventInput {
  /**
   * Optional atomic Control Epoch fence for a participant-owned producer,
   * validated in the same transaction as the event append.
   */
  readonly controlGuard?: ControlEpochGuard | undefined;
  readonly eventId: string | undefined;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly sessionId: string;
  readonly type: string;
}

export interface PublishRestEventInput extends PublishEventInput {
  readonly controlEpoch?: number;
  readonly instanceId: string | undefined;
}

export interface HeartbeatParticipantInput extends RestControlledInput {
  readonly capabilities: Record<string, unknown> | undefined;
}

/** Successful REST heartbeat result with server-derived renewal guidance. */
export interface RestHeartbeatParticipantResult {
  readonly controlEpoch: number | null;
  readonly leaseExpiresAt: string | null;
  readonly participant: ParticipantRecord | null;
  readonly renewAfterMs: number;
}

export interface SessionServiceEffectRuntimeOptions {
  readonly approvalValidators?: ReadonlyMap<string, TaskApprovalValidator>;
  readonly controlEpochEnforcement?: boolean;
  readonly eventSourceId?: string;
  readonly observability?: ModuleObservability;
  readonly taskClaimLeaseTtlMs?: number;
  readonly wsControlLeaseTtlMs?: number;
}
