/**
 * Compatibility re-export for app-local imports. New external adapters should
 * import directly from `@dungle-scrubs/tether-client`.
 */
export {
  buildParticipantRuntimeStreamUrl,
  ParticipantRuntimeClient,
  runParticipantRuntime,
  shouldClaimParticipantTask,
  TaskCancellationRegistry,
} from "@dungle-scrubs/tether-client";

export type {
  ParticipantRuntimeClientConfig,
  ParticipantRuntimeClientDebugInfo,
  ParticipantRuntimeKind,
  ParticipantRuntimeTaskLoopOptions,
  ParticipantTaskExecutorContext,
  ParticipantTaskExecutorResult,
  ParticipantTaskExecutor,
  ParticipantTaskSelector,
  RunParticipantRuntimeInput,
  SessionEvent,
  TaskRecord,
} from "@dungle-scrubs/tether-client";
