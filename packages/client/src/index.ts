/**
 * Public adapter API for external participant runtimes.
 *
 * Adapter implementations should import from this module instead of reaching
 * into Tether service implementation paths.
 */

export type {
  ObserveTaskApprovalsOptions,
  TaskApprovalObserver,
  TaskApprovalObserverClient,
  TaskApprovalObserverDebugInfo,
  TaskApprovalObserverFailureInfo,
} from "./approval-observer.js";
export { observeTaskApprovals, TaskApprovalProcessingError } from "./approval-observer.js";
export { readServiceAuthToken, resolveServiceAuthToken } from "./auth-token.js";
export type {
  SessionEventStreamClientConfig,
  SessionEventStreamClientDebugInfo,
  SessionEventStreamWebSocketFactory,
} from "./session-event-stream-client.js";
export {
  buildSessionEventStreamUrl,
  sessionEventObserverRuntimeKind,
  SessionEventStreamClient,
} from "./session-event-stream-client.js";
export type {
  ParticipantRuntimeClientConfig,
  ParticipantRuntimeClientDebugInfo,
  ParticipantRuntimeCursorPersistPolicy,
  ParticipantRuntimeCursorStore,
  ParticipantRuntimeTaskLoopOptions,
  ParticipantRuntimeCleanup,
  ParticipantTaskExecutor,
  ParticipantTaskExecutorContext,
  ParticipantTaskExecutorResult,
  ParticipantTaskSelector,
  RunParticipantRuntimeHooks,
  RunParticipantRuntimeInput,
} from "./participant-runtime-client.js";
export {
  buildParticipantRuntimeStreamUrl,
  ParticipantRuntimeClient,
  ParticipantRuntimeClientConfigurationError,
  ParticipantRuntimeCommandError,
  ParticipantRuntimeCommandTimeoutError,
  resolveCommandTimeoutMs,
  resolveResumeSeq,
  runParticipantRuntime,
  shouldClaimParticipantTask,
} from "./participant-runtime-client.js";

export {
  buildPublishedEventInput,
  taskFromClaimableEvent,
  taskFromCreatedEvent,
  taskIdFromCancelledEvent,
} from "./protocol.js";
export { TaskCancellationRegistry } from "./task-cancellation-registry.js";

export type {
  ClientSessionBindingRecord,
  ControlLeaseSnapshot,
  ControlLeaseStatus,
  ParticipantRuntimeKind,
  ParticipantRuntimeSnapshot,
  ParticipantRuntimeSnapshotStatus,
  SessionDebugSummary,
  SessionEvent,
  SessionRecord,
  TaskRecord,
  TaskSnapshot,
  TaskSnapshotStatus,
} from "./types.js";
