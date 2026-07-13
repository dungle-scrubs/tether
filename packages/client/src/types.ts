export type {
  ClientSessionBindingRecord,
  ControlChannel,
  ControlLeaseSnapshot,
  ControlLeaseStatus,
  ParticipantRecord,
  ParticipantRuntimeKind,
  ParticipantRuntimeSnapshot,
  ParticipantRuntimeSnapshotStatus,
  SessionDebugControlLeaseSummary,
  SessionDebugParticipantSummary,
  SessionDebugSummary,
  SessionDebugTaskSummary,
  SessionEvent,
  SessionEventType,
  SessionRecord,
  TaskListStatus,
  TaskRecord,
  TaskSnapshot,
  TaskSnapshotStatus,
} from "@dungle-scrubs/tether-protocol";

export type Result<TValue, TError extends Error = Error> =
  | { readonly ok: true; readonly value: TValue }
  | { readonly ok: false; readonly error: TError };
