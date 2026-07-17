import type {
  ControlEpochGuard,
  EnsureScheduledRunInput,
  ScheduledTaskIdentityInput,
  SupersedeScheduledRunsInput,
} from "./db.js";
import {
  acquireRestParticipantControl,
  appendEvent,
  appendEventIdempotent,
  archiveClientSessionBinding,
  cancelTaskWithEvent,
  claimControlLease,
  claimTaskWithEvent,
  completeTaskWithEvent,
  createSession,
  createTaskWithEventIdempotent,
  type DatabasePool,
  deleteSession,
  ensureScheduledRunWithEvents,
  expireTaskClaims,
  failTaskWithEvent,
  findClientSessionBinding,
  getTask,
  heartbeatParticipant,
  heartbeatParticipantWithEvent,
  listClientSessionBindings,
  listControlLeaseSnapshots,
  listEvents,
  listContextEventSuffix,
  listParticipantRuntimeSnapshots,
  listParticipants,
  listSessions,
  listTaskSnapshots,
  listTasks,
  readSession,
  readSessionDebugSummary,
  recordTaskApproval,
  refreshTaskClaim,
  releaseControlLease,
  releaseRestControlLease,
  releaseTaskWithEvent,
  renewControlLease,
  supersedeScheduledRunsWithEvent,
  upsertClientSessionBinding,
  upsertParticipant,
  upsertParticipantWithEvent,
} from "./db.js";
import type {
  ClientBindingStore,
  ControlLeaseStore,
  ParticipantStore,
  SessionEventStore,
  SessionPersistenceStores,
  SessionStore,
  TaskStore,
} from "./db-store-contracts.js";
import type { AppendSessionEventInput } from "./protocol.js";
import type { ParticipantRuntimeKind, TaskListStatus } from "./types.js";

/** Builds focused persistence stores over the current database function implementations. */
export function createSessionPersistenceStores(database: DatabasePool): SessionPersistenceStores {
  return {
    clientBindings: new DbClientBindingStore(database),
    controlLeases: new DbControlLeaseStore(database),
    events: new DbSessionEventStore(database),
    participants: new DbParticipantStore(database),
    sessions: new DbSessionStore(database),
    tasks: new DbTaskStore(database),
  };
}

class DbClientBindingStore implements ClientBindingStore {
  constructor(private readonly database: DatabasePool) {}

  archive(input: { readonly externalId: string; readonly provider: string }) {
    return archiveClientSessionBinding(this.database, input);
  }

  find(input: { readonly externalId: string; readonly provider: string }) {
    return findClientSessionBinding(this.database, input);
  }

  list(input: { readonly provider?: string | undefined } = {}) {
    return listClientSessionBindings(this.database, input);
  }

  upsert(input: {
    readonly externalId: string;
    readonly provider: string;
    readonly sessionId?: string | undefined;
  }) {
    return upsertClientSessionBinding(this.database, input);
  }
}

class DbControlLeaseStore implements ControlLeaseStore {
  constructor(private readonly database: DatabasePool) {}

  acquireRest(input: {
    readonly acquisitionId: string;
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly eventSourceId: string;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly runtimeKind: string;
    readonly sessionId: string;
  }) {
    return acquireRestParticipantControl(this.database, input);
  }

  claim(input: {
    readonly controlChannel: "rest" | "ws";
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  }) {
    return claimControlLease(this.database, input);
  }

  listSnapshots(sessionId: string) {
    return listControlLeaseSnapshots(this.database, sessionId);
  }

  renew(input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  }) {
    return renewControlLease(this.database, input);
  }

  release(input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) {
    return releaseControlLease(this.database, input);
  }

  releaseRest(input: {
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) {
    return releaseRestControlLease(this.database, input);
  }
}

class DbSessionEventStore implements SessionEventStore {
  constructor(private readonly database: DatabasePool) {}

  append(
    input: AppendSessionEventInput,
    options: {
      readonly controlGuard?: ControlEpochGuard | undefined;
      readonly sourceId: string;
    },
  ) {
    return appendEvent(this.database, input, options);
  }

  appendIdempotent(
    input: AppendSessionEventInput,
    options: {
      readonly controlGuard?: ControlEpochGuard | undefined;
      readonly sourceId: string;
    },
  ) {
    return appendEventIdempotent(this.database, input, options);
  }

  list(sessionId: string, afterSeq: number, options?: { readonly limit?: number | undefined }) {
    return listEvents(this.database, sessionId, afterSeq, options);
  }

  listContextSuffix(sessionId: string, afterSeq: number, limit: number) {
    return listContextEventSuffix(this.database, sessionId, afterSeq, limit);
  }
}

class DbParticipantStore implements ParticipantStore {
  constructor(private readonly database: DatabasePool) {}

  heartbeat(input: {
    readonly capabilities?: Record<string, unknown>;
    readonly participantId: string;
    readonly sessionId: string;
  }) {
    return heartbeatParticipant(this.database, input);
  }

  heartbeatWithEvent(input: {
    readonly capabilities?: Record<string, unknown> | undefined;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }) {
    return heartbeatParticipantWithEvent(this.database, input);
  }

  list(sessionId: string) {
    return listParticipants(this.database, sessionId);
  }

  listRuntimeSnapshots(sessionId: string) {
    return listParticipantRuntimeSnapshots(this.database, sessionId);
  }

  upsert(input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly participantId: string;
    readonly runtimeKind: ParticipantRuntimeKind;
    readonly sessionId: string;
  }) {
    return upsertParticipant(this.database, input);
  }

  upsertWithEvent(input: {
    readonly capabilities: Record<string, unknown>;
    readonly displayName: string;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly runtimeKind: ParticipantRuntimeKind;
    readonly sessionId: string;
  }) {
    return upsertParticipantWithEvent(this.database, input);
  }
}

class DbSessionStore implements SessionStore {
  constructor(private readonly database: DatabasePool) {}

  create(sessionId: string) {
    return createSession(this.database, sessionId);
  }

  delete(
    sessionId: string,
    options?: {
      readonly hasLiveHost?: (() => boolean) | undefined;
    },
  ) {
    return deleteSession(this.database, sessionId, options);
  }

  list() {
    return listSessions(this.database);
  }

  read(sessionId: string) {
    return readSession(this.database, sessionId);
  }

  readDebugSummary(sessionId: string) {
    return readSessionDebugSummary(this.database, sessionId);
  }
}

class DbTaskStore implements TaskStore {
  constructor(private readonly database: DatabasePool) {}

  cancelWithEvent(input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly reason?: Record<string, unknown> | undefined;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return cancelTaskWithEvent(this.database, input);
  }

  recordApproval(input: {
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly decision: "approved" | "rejected";
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly reason: Record<string, unknown>;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return recordTaskApproval(this.database, input);
  }

  claimWithEvent(input: {
    readonly claimLeaseTtlMs: number;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return claimTaskWithEvent(this.database, input);
  }

  completeWithEvent(input: {
    readonly claimId: string;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly result: Record<string, unknown>;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return completeTaskWithEvent(this.database, input);
  }

  createWithEvent(input: {
    readonly eventSourceId: string;
    readonly input?: Record<string, unknown> | null;
    readonly kind: string;
    readonly objective: string;
    readonly schedule?: ScheduledTaskIdentityInput | undefined;
    readonly sessionId: string;
    readonly taskId: string;
    readonly taskIdSource: "caller" | "generated";
  }) {
    return createTaskWithEventIdempotent(this.database, input);
  }

  supersedeScheduled(input: SupersedeScheduledRunsInput) {
    return supersedeScheduledRunsWithEvent(this.database, input);
  }

  ensureScheduledRun(input: EnsureScheduledRunInput) {
    return ensureScheduledRunWithEvents(this.database, input);
  }

  expireClaims(input: { readonly batchSize: number; readonly sourceId: string }) {
    return expireTaskClaims(this.database, input);
  }

  failWithEvent(input: {
    readonly claimId: string;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly failure: Record<string, unknown>;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return failTaskWithEvent(this.database, input);
  }

  get(input: { readonly sessionId: string; readonly taskId: string }) {
    return getTask(this.database, input);
  }

  list(sessionId: string, status: TaskListStatus = "active") {
    return listTasks(this.database, sessionId, status);
  }

  listSnapshots(sessionId: string) {
    return listTaskSnapshots(this.database, sessionId);
  }

  refreshClaim(input: {
    readonly claimId: string;
    readonly claimLeaseTtlMs: number;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return refreshTaskClaim(this.database, input);
  }

  releaseWithEvent(input: {
    readonly claimId: string;
    readonly controlGuard?: ControlEpochGuard | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    return releaseTaskWithEvent(this.database, input);
  }
}
