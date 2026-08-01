import {
  sessionScalabilitySpanNames,
  type SessionScalabilityDebugRecord,
  type SessionScalabilityHealthWarning,
} from "@dungle-scrubs/tether-protocol";
import { SpanStatusCode, trace } from "@opentelemetry/api";
import { Effect } from "effect";

import type { SessionPersistenceStores } from "./db-store-contracts.js";
import {
  buildBoundedSessionContextView,
  buildParticipantTaskContracts,
  canUseSessionSummaryForContext,
  classifySessionContextBudget,
} from "./session-service-context.js";
import {
  decideSessionContextMaintenance,
  enqueueSessionContextMaintenance,
  type SessionContextBudgetClass,
  truncatedSessionContextSuffixTokenFloor,
} from "./session-context-maintenance-policy.js";
import {
  defaultRecentTerminalTaskLimit,
  type SessionServiceFailure,
} from "./session-service-contracts.js";
import { trySessionPromise } from "./session-service-runtime.js";
import type { SessionSummaryStore } from "./session-summary-store.js";
import type { SessionScalabilityDiagnostics } from "./session-scalability-diagnostics.js";
import type {
  ControlLeaseSnapshot,
  ParticipantRecord,
  ParticipantRuntimeSnapshot,
  ParticipantTaskContractRecord,
  SessionContextView,
  SessionContextViewRequest,
  SessionDebugSummary,
  SessionEvent,
  SessionEventListOptions,
  SessionListItem,
  TaskListStatus,
  TaskRecord,
  TaskSnapshot,
} from "./types.js";

/** Maximum newest raw events one context read may materialize before packing. */
export const maxSessionContextCandidateEvents = 10_000;

/** Dependencies for read-only session Effect builders. */
export interface SessionReadEffectsInput {
  /** Optional scheduler bundle enabled only after summary publication gates pass. */
  readonly summaryMaintenance?: {
    readonly reportFailure: (failure: {
      readonly budgetClass: SessionContextBudgetClass;
      readonly code: "schedule_failed";
    }) => void;
    readonly schedule: (input: {
      readonly budgetClass: SessionContextBudgetClass;
      readonly sessionId: string;
      readonly unsummarizedTokens: number;
    }) => Promise<void>;
  };
  readonly sessionSummaryStore: Pick<SessionSummaryStore, "readLatestPublished">;
  readonly scalabilityDiagnostics: SessionScalabilityDiagnostics;
  readonly stores: SessionPersistenceStores;
}

/** Read-only session, participant, task, and diagnostic Effect programs. */
export interface SessionReadEffects {
  readonly buildSessionContextViewEffect: (
    input: SessionContextViewRequest,
  ) => Effect.Effect<SessionContextView, SessionServiceFailure>;
  readonly findParticipantTaskContractEffect: (input: {
    readonly sessionId: string;
    readonly taskKind: string;
  }) => Effect.Effect<ParticipantTaskContractRecord | null, SessionServiceFailure>;
  readonly getTaskEffect: (
    sessionId: string,
    taskId: string,
  ) => Effect.Effect<TaskRecord | null, SessionServiceFailure>;
  readonly listControlLeaseSnapshotsEffect: (
    sessionId: string,
  ) => Effect.Effect<ControlLeaseSnapshot[], SessionServiceFailure>;
  readonly listEventsEffect: (
    sessionId: string,
    afterSeq: number,
    options?: SessionEventListOptions,
  ) => Effect.Effect<SessionEvent[], SessionServiceFailure>;
  readonly listParticipantRuntimeSnapshotsEffect: (
    sessionId: string,
  ) => Effect.Effect<ParticipantRuntimeSnapshot[], SessionServiceFailure>;
  readonly listParticipantsEffect: (
    sessionId: string,
    options?: {
      readonly before?: Pick<ParticipantRecord, "lastSeenAt" | "participantId"> | undefined;
      readonly limit?: number | undefined;
    },
  ) => Effect.Effect<ParticipantRecord[], SessionServiceFailure>;
  readonly listSessionsEffect: () => Effect.Effect<SessionListItem[], SessionServiceFailure>;
  readonly listParticipantTaskContractsByKindEffect: (input: {
    readonly sessionId: string;
    readonly taskKind: string;
  }) => Effect.Effect<ParticipantTaskContractRecord[], SessionServiceFailure>;
  readonly listParticipantTaskContractsEffect: (
    sessionId: string,
  ) => Effect.Effect<ParticipantTaskContractRecord[], SessionServiceFailure>;
  readonly listTasksEffect: (
    sessionId: string,
    status?: TaskListStatus,
    options?: {
      readonly before?: Pick<TaskRecord, "createdAt" | "taskId"> | undefined;
      readonly limit?: number | undefined;
    },
  ) => Effect.Effect<TaskRecord[], SessionServiceFailure>;
  readonly listTaskSnapshotsEffect: (
    sessionId: string,
  ) => Effect.Effect<TaskSnapshot[], SessionServiceFailure>;
  readonly readSessionDebugSummaryEffect: (
    sessionId: string,
  ) => Effect.Effect<SessionDebugSummary, SessionServiceFailure>;
  readonly readSessionScalabilityDebugEffect: (
    sessionId: string,
  ) => Effect.Effect<SessionScalabilityDebugRecord, SessionServiceFailure>;
  readonly readScalabilityHealthWarningsEffect: () => Effect.Effect<
    readonly SessionScalabilityHealthWarning[],
    SessionServiceFailure
  >;
}

/** Builds read-only effects for one service instance. */
export function createSessionReadEffects(input: SessionReadEffectsInput): SessionReadEffects {
  const listParticipantsEffect = (
    sessionId: string,
    options?: {
      readonly before?: Pick<ParticipantRecord, "lastSeenAt" | "participantId"> | undefined;
      readonly limit?: number | undefined;
    },
  ): Effect.Effect<ParticipantRecord[], SessionServiceFailure> =>
    trySessionPromise(() => input.stores.participants.list(sessionId, options));
  const listParticipantTaskContractsEffect = (
    sessionId: string,
  ): Effect.Effect<ParticipantTaskContractRecord[], SessionServiceFailure> =>
    Effect.map(listParticipantsEffect(sessionId), buildParticipantTaskContracts);
  const listTasksEffect = (
    sessionId: string,
    status: TaskListStatus = "active",
    options?: {
      readonly before?: Pick<TaskRecord, "createdAt" | "taskId"> | undefined;
      readonly limit?: number | undefined;
    },
  ): Effect.Effect<TaskRecord[], SessionServiceFailure> =>
    trySessionPromise(() => input.stores.tasks.list(sessionId, status, options));

  return {
    buildSessionContextViewEffect: (contextInput) =>
      Effect.gen(function* () {
        const budgetClass = classifySessionContextBudget(contextInput.budgetTokens);
        const publishedSummary = yield* trySessionPromise(() =>
          input.sessionSummaryStore.readLatestPublished(contextInput.sessionId, budgetClass),
        );
        const latestSummary = canUseSessionSummaryForContext({
          budgetTokens: contextInput.budgetTokens,
          forParticipant: contextInput.forParticipant,
          sessionId: contextInput.sessionId,
          summary: publishedSummary,
        })
          ? publishedSummary
          : null;
        const [suffix, activeTasks, terminalTasks, participants] = yield* trySessionPromise(() =>
          Promise.all([
            input.stores.events.listContextSuffix(
              contextInput.sessionId,
              latestSummary?.coversSeqTo ?? 0,
              maxSessionContextCandidateEvents,
            ),
            input.stores.tasks.list(contextInput.sessionId, "active"),
            input.stores.tasks.list(contextInput.sessionId, "terminal"),
            input.stores.participants.list(contextInput.sessionId),
          ]),
        );
        const events = suffix.events;
        const context = buildTracedSessionContextView({
          activeTasks,
          budgetTokens: contextInput.budgetTokens,
          events,
          eligibleEventCount: suffix.eligibleEventCount,
          forParticipant: contextInput.forParticipant,
          latestSummary,
          recentTerminalTasks: terminalTasks.slice(0, defaultRecentTerminalTaskLimit),
          sessionId: contextInput.sessionId,
          taskContracts: buildParticipantTaskContracts(participants),
        });
        input.scalabilityDiagnostics.recordContext(context.mode);
        const maintenanceDecision = decideSessionContextMaintenance({
          budgetClass,
          unsummarizedTokens: suffix.truncated
            ? Math.max(suffix.estimatedTokens, truncatedSessionContextSuffixTokenFloor)
            : suffix.estimatedTokens,
        });
        if (input.summaryMaintenance !== undefined) {
          enqueueSessionContextMaintenance(
            maintenanceDecision,
            (maintenance) =>
              input.summaryMaintenance?.schedule({
                ...maintenance,
                sessionId: contextInput.sessionId,
              }) ?? Promise.resolve(),
            input.summaryMaintenance.reportFailure,
          );
        }
        return context;
      }),
    findParticipantTaskContractEffect: (contractInput) =>
      Effect.map(
        listParticipantTaskContractsEffect(contractInput.sessionId),
        (contracts) =>
          contracts.find((contract) => contract.taskKind === contractInput.taskKind) ?? null,
      ),
    getTaskEffect: (sessionId, taskId) =>
      trySessionPromise(() => input.stores.tasks.get({ sessionId, taskId })),
    listControlLeaseSnapshotsEffect: (sessionId) =>
      trySessionPromise(() => input.stores.controlLeases.listSnapshots(sessionId)),
    listEventsEffect: (sessionId, afterSeq, options) =>
      trySessionPromise(() => input.stores.events.list(sessionId, afterSeq, options)),
    listParticipantRuntimeSnapshotsEffect: (sessionId) =>
      trySessionPromise(() => input.stores.participants.listRuntimeSnapshots(sessionId)),
    listParticipantsEffect,
    listSessionsEffect: () => trySessionPromise(() => input.stores.sessions.list()),
    listParticipantTaskContractsByKindEffect: (contractInput) =>
      Effect.map(listParticipantTaskContractsEffect(contractInput.sessionId), (contracts) =>
        contracts.filter((contract) => contract.taskKind === contractInput.taskKind),
      ),
    listParticipantTaskContractsEffect,
    listTasksEffect,
    listTaskSnapshotsEffect: (sessionId) =>
      trySessionPromise(() => input.stores.tasks.listSnapshots(sessionId)),
    readSessionDebugSummaryEffect: (sessionId) =>
      trySessionPromise(() => input.stores.sessions.readDebugSummary(sessionId)),
    readSessionScalabilityDebugEffect: (sessionId) =>
      trySessionPromise(() => input.scalabilityDiagnostics.read(sessionId)),
    readScalabilityHealthWarningsEffect: () =>
      trySessionPromise(() => input.scalabilityDiagnostics.readHealthWarnings()),
  };
}

function buildTracedSessionContextView(
  input: Parameters<typeof buildBoundedSessionContextView>[0],
): SessionContextView {
  const span = trace
    .getTracer("tether-session-context")
    .startSpan(sessionScalabilitySpanNames.contextBuild, {
      attributes: {
        "context.active_task_count": input.activeTasks.length,
        "context.budget_tokens": input.budgetTokens,
        "context.candidate_event_count": input.events.length,
        "context.summary_available": input.latestSummary !== null,
      },
    });
  try {
    const context = buildBoundedSessionContextView(input);
    span.setAttributes({
      "context.estimated_tokens": context.budget.estimatedTokens,
      "context.mode": context.mode,
      "context.omitted_event_count": context.budget.omittedEventCount,
    });
    span.setStatus({ code: SpanStatusCode.OK });
    return context;
  } catch (error) {
    span.setAttribute("context.failure_code", "context_build_failed");
    span.setStatus({ code: SpanStatusCode.ERROR });
    throw error;
  } finally {
    span.end();
  }
}
