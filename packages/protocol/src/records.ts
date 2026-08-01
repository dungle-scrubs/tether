import { z } from "zod";

import { approvalDecisionSchema, taskResultSchema } from "./approval-targets.js";
import type { CandidateScheduleIdentity } from "./task-contracts.js";
import { candidateScheduleIdentitySchema } from "./task-contracts.js";

/**
 * Durable task record returned by Tether task APIs and embedded in task
 * lifecycle events.
 */
export interface TaskRecord {
  /** Cancellation timestamp, when the task has been cancelled. */
  readonly cancelledAt: string | null;
  /** Claim-expiration timestamp persisted after the scheduler clears an elapsed claim. */
  readonly claimExpiredAt: string | null;
  /** Participant id whose claim elapsed, when the scheduler cleared it. */
  readonly claimExpiredBy: string | null;
  /** Claim lease expiry timestamp, when a claimed task should become claimable again. */
  readonly claimExpiresAt: string | null;
  /**
   * Server-issued opaque identity of the current claim generation, or null when
   * the task is unclaimed or was claimed before Claim IDs existed. Every
   * successful claim mints a new value; claim-owned mutations must carry the
   * exact current value to be accepted.
   */
  readonly claimId: string | null;
  /** Claim timestamp, when a participant runtime currently owns the task. */
  readonly claimedAt: string | null;
  /** Participant id currently claiming the task. */
  readonly claimedBy: string | null;
  /** Completion timestamp, when the task has completed successfully. */
  readonly completedAt: string | null;
  /** Creation timestamp returned by Tether. */
  readonly createdAt: string;
  /** Failure timestamp, when the task has reached a terminal failure. */
  readonly failedAt: string | null;
  /** Structured failure payload, when the task failed with details. */
  readonly failure: Record<string, unknown> | null;
  /** Structured task input supplied at creation time, when available. */
  readonly input: Record<string, unknown> | null;
  /** Task kind used by participant runtimes to decide claimability. */
  readonly kind: string;
  /** User-visible task objective. */
  readonly objective: string;
  /** Last claim release timestamp, when a participant explicitly released the task. */
  readonly releasedAt: string | null;
  /** Participant id that explicitly released the task claim. */
  readonly releasedBy: string | null;
  /** Structured result payload, when the task completed with details. */
  readonly result: Record<string, unknown> | null;
  /**
   * Deterministic schedule and opaque scope identity for recurring work
   * runs; null for manual tasks. Present only when the durable row carries a
   * complete Schedule Window and scope key.
   */
  readonly schedule?: CandidateScheduleIdentity | null | undefined;
  /** Durable Tether session that owns the task. */
  readonly sessionId: string;
  /** Durable task id. */
  readonly taskId: string;
}

/** Public session event type names used by REST and WebSocket boundaries. */
export type SessionEventType =
  | "agent.output"
  | "approval.recorded"
  | "control.cancel"
  | "participant.heartbeat"
  | "participant.joined"
  | "participant.updated"
  | "session.created"
  | "task.claim_expired"
  | "task.claimed"
  | "task.completed"
  | "task.created"
  | "task.failed"
  | "task.progress"
  | "task.released"
  | "user.message"
  | (string & {});

/** Runtime kind advertised by a participant. */
export type ParticipantRuntimeKind =
  | "claude_code"
  | "codex"
  | "generic_agent"
  | "openai_agent"
  | "pi_coding_agent"
  | (string & {});

/** Control channel used by a participant runtime. */
export type ControlChannel = "rest" | "ws";

/** Durable event visible in one Tether session stream. */
export interface SessionEvent {
  readonly createdAt: string;
  readonly eventId: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly seq: number;
  readonly sessionId: string;
  readonly type: SessionEventType;
}

/** Visible participant presence record. */
export interface ParticipantRecord {
  readonly capabilities: Record<string, unknown>;
  readonly displayName: string;
  readonly joinedAt: string;
  readonly lastSeenAt: string;
  readonly participantId: string;
  readonly runtimeKind: ParticipantRuntimeKind;
  readonly sessionId: string;
}

/**
 * Task lifecycle filter for user-facing task lists.
 */
export type TaskListStatus = "active" | "all" | "terminal";

/** Durable Tether session row shared by service and adapter diagnostics. */
export interface SessionRecord {
  /** Time this session was first created. */
  readonly createdAt: string;
  /** Durable session identifier. */
  readonly sessionId: string;
}

/** Scope marker for process-local Host Presence responses. */
export const replicaPresenceScope = "replica" as const;

/** Live host metadata carried by Replica Scope presence responses. */
export interface LiveHostPresence {
  readonly displayName: string;
  readonly instanceId: string;
  readonly participantId: string;
}

/** Runtime validator for one live host in a presence response. */
export const liveHostPresenceSchema: z.ZodType<LiveHostPresence> = z.object({
  displayName: z.string(),
  instanceId: z.string().min(1),
  participantId: z.string().min(1),
});

/**
 * Host Presence session inventory whose contents are complete only for the
 * replica identified by {@link replicaId}.
 */
export interface HostPresenceInventory<TSession = unknown> {
  /** Opaque identity stable for the lifetime of the serving app process. */
  readonly replicaId: string;
  /** Declares that the inventory is process-local rather than cluster-complete. */
  readonly scope: typeof replicaPresenceScope;
  /** Existing session inventory payload. */
  readonly sessions: readonly TSession[];
}

/** Runtime validator for Replica Scope Host Presence inventory responses. */
export const hostPresenceInventorySchema: z.ZodType<HostPresenceInventory> = z.object({
  replicaId: z.string().min(1),
  scope: z.literal(replicaPresenceScope),
  sessions: z.array(z.unknown()),
});

/**
 * Durable association between an external client conversation and one Tether
 * session.
 */
export interface ClientSessionBindingRecord {
  /** Time this binding was archived, or null while active. */
  readonly archivedAt: string | null;
  /** Time this binding was first created. */
  readonly createdAt: string;
  /** Provider-specific conversation id, such as an external chat id. */
  readonly externalId: string;
  /** Most recent time a bridge resolved this binding. */
  readonly lastSeenAt: string;
  /** Client integration provider, such as `external-chat` or `slack`. */
  readonly provider: string;
  /** Durable Tether session associated with the external conversation. */
  readonly sessionId: string;
}

/** Operator-facing state derived from the durable participant control lease. */
export type ControlLeaseStatus = "active" | "expired" | "released" | "superseded";

/** Read-only diagnostic view of one participant control lease. */
export interface ControlLeaseSnapshot {
  /** When this runtime instance first claimed the participant control channel. */
  readonly claimedAt: string;
  /** Whether this runtime instance controls the participant through REST or WebSocket. */
  readonly controlChannel: ControlChannel;
  /** Immutable, strictly-monotonic server-issued Control Epoch for this lease. */
  readonly epoch: number;
  /** Concrete runtime process currently or formerly associated with the lease. */
  readonly instanceId: string;
  /** Most recent durable refresh observed for this lease. */
  readonly lastSeenAt: string;
  /** Time after which an unreleased lease no longer blocks another runtime. */
  readonly leaseExpiresAt: string;
  /** Participant identity controlled by this runtime instance. */
  readonly participantId: string;
  /** Time this lease was explicitly released, or null while unreleased. */
  readonly releasedAt: string | null;
  /** Session that owns this participant control lease. */
  readonly sessionId: string;
  /** Time this lease was superseded by another runtime, or null while current. */
  readonly supersededAt: string | null;
  /** Current derived lease state for operator diagnostics. */
  readonly status: ControlLeaseStatus;
}

/** Operator-facing state derived from a durable task row. */
export type TaskSnapshotStatus =
  | "cancelled"
  | "claim_active"
  | "claim_cleared"
  | "claim_expired"
  | "completed"
  | "failed"
  | "unclaimed";

/** Durable approval decision attached to one task target. */
export interface TaskApprovalRecord {
  /** Event id of the corresponding `approval.recorded` event. */
  readonly approvalEventId: string;
  /** Time the winning approval decision was committed. */
  readonly decidedAt: string;
  /** Participant that issued the winning decision. */
  readonly decidedByParticipantId: string;
  /** Winning approval decision for the target. */
  readonly decision: "approved" | "rejected";
  /** Original approval reason used to derive the target key. */
  readonly reason: Record<string, unknown>;
  /** Session that owns the task. */
  readonly sessionId: string;
  /** Canonical durable approval target key. */
  readonly targetKey: string;
  /** Approved task id. */
  readonly taskId: string;
}

/** Runtime validator for one canonical durable task approval record. */
export const taskApprovalRecordSchema = z.object({
  approvalEventId: z.string().min(1),
  decidedAt: z.string().datetime({ offset: true }),
  decidedByParticipantId: z.string().min(1),
  decision: approvalDecisionSchema,
  reason: z.record(z.string(), z.unknown()),
  sessionId: z.string().min(1),
  targetKey: z.string().min(1),
  taskId: z.string().min(1),
});

/** Read-only diagnostic view of one task with derived lifecycle state. */
export interface TaskSnapshot extends TaskRecord {
  /** Schema-backed approval decisions recorded for this task. */
  readonly approvals: readonly TaskApprovalRecord[];
  /** Current derived task state for operator diagnostics. */
  readonly status: TaskSnapshotStatus;
}

/**
 * Operator-facing state derived from visible participant presence and control
 * lease state.
 */
export type ParticipantRuntimeSnapshotStatus =
  | "lease_without_presence"
  | "registered_control_active"
  | "registered_control_inactive"
  | "registered_without_control";

/**
 * Read-only diagnostic view of a participant identity and the runtime instance
 * that currently or most recently controlled it.
 */
export interface ParticipantRuntimeSnapshot {
  /** Number of durable control-lease rows associated with this participant. */
  readonly controlLeaseCount: number;
  /** Active control lease for this participant, or null when no runtime currently controls it. */
  readonly currentControlLease: ControlLeaseSnapshot | null;
  /** Most recently observed control lease for this participant, including inactive leases. */
  readonly latestControlLease: ControlLeaseSnapshot | null;
  /** Visible participant presence record, or null when only lease state exists. */
  readonly participant: ParticipantRecord | null;
  /** Participant identity being diagnosed. */
  readonly participantId: string;
  /** Whether this participant has visible presence in the session. */
  readonly registered: boolean;
  /** Session that owns the participant runtime state. */
  readonly sessionId: string;
  /** Current derived participant runtime state for operator diagnostics. */
  readonly status: ParticipantRuntimeSnapshotStatus;
}

/** Participant counts in a session debug summary. */
export interface SessionDebugParticipantSummary {
  /** Number of visible participant identities with an active control lease. */
  readonly activeControl: number;
  /** Number of participant identities represented only by lease state. */
  readonly leaseOnly: number;
  /** Number of visible participant identities. */
  readonly registered: number;
  /** Number of participant runtime snapshots in the summary. */
  readonly total: number;
  /** Number of visible participant identities without active control. */
  readonly withoutActiveControl: number;
}

/** Control lease counts in a session debug summary. */
export interface SessionDebugControlLeaseSummary {
  /** Number of unreleased control leases whose lease time has not elapsed. */
  readonly active: number;
  /** Number of unreleased control leases whose lease time has elapsed. */
  readonly expired: number;
  /** Number of explicitly released control leases. */
  readonly released: number;
  /** Number of control leases superseded by a later owner. */
  readonly superseded: number;
  /** Number of control lease snapshots in the summary. */
  readonly total: number;
}

/** Task counts in a session debug summary. */
export interface SessionDebugTaskSummary {
  /** Number of currently active task claims. */
  readonly activeClaims: number;
  /** Number of cancelled tasks. */
  readonly cancelled: number;
  /** Number of tasks that can be claimed without waiting for the scheduler. */
  readonly claimable: number;
  /** Number of active claim snapshots. */
  readonly claimActive: number;
  /** Number of cleared claim snapshots. */
  readonly claimCleared: number;
  /** Number of expired claim snapshots waiting for scheduler cleanup. */
  readonly claimExpired: number;
  /** Number of completed tasks. */
  readonly completed: number;
  /** Number of expired task claims waiting for scheduler cleanup. */
  readonly expiredClaims: number;
  /** Number of failed tasks. */
  readonly failed: number;
  /** Number of terminal tasks. */
  readonly terminal: number;
  /** Number of task snapshots in the summary. */
  readonly total: number;
  /** Number of unclaimed tasks. */
  readonly unclaimed: number;
}

/** Read-only aggregate diagnostic view for one session. */
export interface SessionDebugSummary {
  /** Participant runtime snapshot counts. */
  readonly participants: SessionDebugParticipantSummary;
  /** Control lease snapshot counts. */
  readonly controlLeases: SessionDebugControlLeaseSummary;
  /** Session being summarized. */
  readonly sessionId: string;
  /** Task snapshot counts. */
  readonly tasks: SessionDebugTaskSummary;
}

/** Stable warning vocabulary for scalability health projection. */
export type SessionScalabilityHealthWarning =
  | "ollama_disabled"
  | "projection_stale"
  | "retention_disabled"
  | "summary_invalid"
  | "summary_publication_disabled"
  | "summary_worker_disabled";

/** Safe latest backfill state observed by the serving process. */
export type SessionScalabilityBackfillOutcome =
  | { readonly status: "not_observed" }
  | {
      readonly batchesRead: number;
      readonly malformedEventCount: number;
      readonly status: "stale" | "unchanged" | "written";
    };

/** Safe latest verification state observed by the serving process. */
export type SessionScalabilityVerificationOutcome =
  | { readonly status: "not_observed" }
  | {
      readonly batchesRead: number;
      readonly differenceCount: number;
      readonly malformedEventCount: number;
      readonly status: "current" | "mismatch" | "missing";
    };

/** Content-free active summary head exposed to operators. */
export interface SessionScalabilitySummaryHead {
  readonly budgetClass: string;
  readonly coversSeqFrom: number;
  readonly coversSeqTo: number;
  readonly producerId: string;
  readonly producerVersion: string;
  readonly summaryId: string;
}

/** Future safety gates that must pass before raw event retention may exist. */
export type SessionEventRetentionGate =
  | "atomic_boundary_advance"
  | "backup_restore_validation"
  | "consumer_cursor_coverage"
  | "recovery_contract"
  | "replica_convergence";

/** Explicitly disabled retention state exposed to operators in this cutoff. */
export interface SessionEventRetentionStatus {
  readonly boundaryAdvancementEnabled: false;
  readonly deletionEnabled: false;
  readonly reason: "future_safety_gates_unmet";
  readonly status: "disabled";
  readonly unmetGates: readonly SessionEventRetentionGate[];
}

/** Protocol-owned safe diagnostics for projections, summaries, and context. */
export interface SessionScalabilityDebugRecord {
  readonly context: {
    readonly rawOnlyCount: number;
    readonly summaryBackedCount: number;
  };
  readonly healthWarnings: readonly SessionScalabilityHealthWarning[];
  readonly projection: {
    readonly activeReducerVersion: number;
    readonly coverage: { readonly coversSeqTo: number; readonly eventCount: number } | null;
    readonly current: boolean;
    readonly currentCount: number;
    readonly enabled: boolean;
    readonly latestBackfill: SessionScalabilityBackfillOutcome;
    readonly latestVerification: SessionScalabilityVerificationOutcome;
    readonly staleCount: number;
  };
  readonly retention: SessionEventRetentionStatus;
  readonly sessionId: string;
  readonly summary: {
    readonly active: readonly SessionScalabilitySummaryHead[];
    readonly activeCandidate: SessionScalabilitySummaryHead | null;
    readonly disabledReason: string | null;
    readonly publicationEnabled: boolean;
    readonly rejectionCode: string | null;
    readonly retentionEnabled: boolean;
  };
  readonly worker: {
    readonly ollamaStatus: "disabled" | "ready" | "unavailable";
    readonly reason: string | null;
    readonly status: "disabled" | "ready" | "unavailable";
  };
}

/** Producer id used by Tether-owned system events. */
export const systemProducerId = "tether";

/** Canonical session event names. */
export const sessionEventType = {
  agentOutput: "agent.output",
  approvalRecorded: "approval.recorded",
  controlCancel: "control.cancel",
  participantHeartbeat: "participant.heartbeat",
  participantJoined: "participant.joined",
  participantUpdated: "participant.updated",
  sessionCreated: "session.created",
  taskClaimExpired: "task.claim_expired",
  taskClaimed: "task.claimed",
  taskCompleted: "task.completed",
  taskCreated: "task.created",
  taskFailed: "task.failed",
  taskProgress: "task.progress",
  taskReleased: "task.released",
  userMessage: "user.message",
} as const satisfies Record<string, SessionEventType>;

/** Canonical WebSocket operation names. */
export const webSocketOperation = {
  commandResult: "command.result",
  error: "error",
  event: "event",
  presence: "presence",
  publish: "publish",
  replayComplete: "replay.complete",
  taskCancel: "task.cancel",
  taskClaim: "task.claim",
  taskComplete: "task.complete",
  taskFail: "task.fail",
  taskRefresh: "task.refresh",
  taskRelease: "task.release",
} as const;

/**
 * Runtime validator for durable task records crossing REST and WebSocket
 * protocol boundaries.
 */
export const taskRecordSchema = z.object({
  cancelledAt: z.string().nullable(),
  claimExpiredAt: z.string().nullable(),
  claimExpiredBy: z.string().nullable(),
  claimExpiresAt: z.string().datetime({ offset: true }).nullable(),
  claimId: z.string().nullable(),
  claimedAt: z.string().nullable(),
  claimedBy: z.string().nullable(),
  completedAt: z.string().nullable(),
  createdAt: z.string(),
  failedAt: z.string().nullable(),
  failure: z.record(z.string(), z.unknown()).nullable(),
  input: z.record(z.string(), z.unknown()).nullable(),
  kind: z.string().min(1),
  objective: z.string().min(1),
  releasedAt: z.string().nullable(),
  releasedBy: z.string().nullable(),
  result: taskResultSchema.nullable(),
  schedule: candidateScheduleIdentitySchema.nullable().optional(),
  sessionId: z.string().min(1),
  taskId: z.string().min(1),
});

/** Runtime validator for participant control channels. */
export const controlChannelSchema = z.union([z.literal("rest"), z.literal("ws")]);

/** Runtime validator for participant runtime kinds. */
export const participantRuntimeKindSchema = z
  .union([
    z.literal("claude_code"),
    z.literal("codex"),
    z.literal("generic_agent"),
    z.literal("openai_agent"),
    z.literal("pi_coding_agent"),
    z.string().min(1),
  ])
  .default("generic_agent");
