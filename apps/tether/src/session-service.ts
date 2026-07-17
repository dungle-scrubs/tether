import { Context, Effect, Layer } from "effect";

import { approvalTargetKey } from "./approval-target-key.js";
import { ServerConfigService } from "./config.js";
import { type DatabasePool, DatabaseService } from "./db.js";
import { createSessionPersistenceStores } from "./db-stores.js";
import { ModuleObservability, readModuleObservabilityOptions } from "./observability.js";
import type { AppendSessionEventInput } from "./protocol.js";
import { RestControlPolicy, type RestControlOutcomeName } from "./rest-control-policy.js";
import {
  type ControlEpochGuard,
  type PublicSessionEnsureResult,
  type RestControlledInput,
  type RestControlReleaseResult,
  type RestTaskClaimRefreshResult,
  type RestTaskMutationResult,
  restControlLeaseTtlMs,
  type SessionServiceEffect,
  type SessionServiceEffectRuntimeOptions,
  type SessionServiceFailure,
  type SessionServiceOptions,
  type TaskApprovalValidator,
  type TaskClaimRefreshResult,
  type TaskMutationResult,
  type TaskParticipantInput,
  taskClaimLeaseTtlMs,
  wsControlLeaseTtlMs,
} from "./session-service-contracts.js";
import { createSessionControlEffects } from "./session-service-control-effects.js";
import { createSessionCoreEffects } from "./session-service-core-effects.js";
import { createSessionReadEffects } from "./session-service-read-effects.js";
import { SessionScalabilityDiagnostics } from "./session-scalability-diagnostics.js";
import { createSessionSummaryStore } from "./session-summary-store.js";
import {
  assertBroadcastEventsWithObservability,
  catchAtomicEpochStale,
  createSessionTraceEffect,
  newEventSourceId,
  trySessionPromise,
} from "./session-service-runtime.js";
import {
  createSessionTaskEffects,
  mapTaskApprovalRejection,
  mapTaskClaimRefreshRejection,
  mapTaskMutationRejection,
  summarizeRestTaskClaimRefreshResult,
  summarizeRestTaskMutationResult,
  summarizeTaskClaimRefreshResult,
  summarizeTaskMutationResult,
  taskParticipantTraceInput,
} from "./session-service-task-effects.js";
import type { SessionEvent } from "./types.js";

const restTaskRouteNames = {
  cancelTaskOverRest: "task.cancel",
  claimTaskOverRest: "task.claim",
  completeTaskOverRest: "task.complete",
  failTaskOverRest: "task.fail",
  releaseTaskOverRest: "task.release",
} as const;

type RestTaskOperation = keyof typeof restTaskRouteNames;

export type {
  CancelTaskInput,
  ClientSessionBindingResult,
  CompleteTaskInput,
  ControlLeaseConflict,
  ControlProtectedResult,
  CreateTaskInput,
  FailTaskInput,
  HeartbeatParticipantInput,
  ParticipantControlContext,
  PublicSessionEnsureResult,
  PublishEventInput,
  PublishedEventResult,
  PublishRestEventInput,
  RecordTaskApprovalInput,
  RegisteredParticipantResult,
  RegisteredRestParticipantResult,
  RegisterParticipantInput,
  RegisterWebSocketParticipantInput,
  ResolveClientSessionInput,
  RestControlledInput,
  RestControlReleaseResult,
  RestParticipantRegistrationResult,
  RestTaskApprovalResult,
  RestTaskClaimRefreshResult,
  RestTaskMutationResult,
  SessionCreatedResult,
  SessionServiceDebugInfo,
  SessionServiceEffect,
  SessionServiceEffectRuntimeOptions,
  SessionServiceOptions,
  TaskApprovalIgnoredReason,
  TaskApprovalRejectionReason,
  TaskApprovalResult,
  TaskApprovalValidator,
  TaskClaimRefreshResult,
  TaskClaimsExpiredResult,
  TaskCreatedResult,
  TaskMutationResult,
  TaskParticipantInput,
} from "./session-service-contracts.js";
export {
  restControlLeaseTtlMs,
  taskClaimLeaseTtlMs,
  wsControlLeaseTtlMs,
} from "./session-service-contracts.js";

/**
 * Effect service tag for the durable session service boundary.
 */
export class SessionServiceEffectService extends Context.Tag("tether/SessionServiceEffect")<
  SessionServiceEffectService,
  SessionServiceEffect
>() {}

/**
 * Live Effect-native session service layer backed by the shared database pool.
 */
export const SessionServiceEffectLive = Layer.effect(
  SessionServiceEffectService,
  Effect.gen(function* () {
    const database = yield* DatabaseService;
    const config = yield* ServerConfigService;
    return createSessionServiceEffect(database, {
      controlEpochEnforcement: config.controlEpochEnforcement,
    });
  }),
);

/**
 * Attaches the atomic REST Control Epoch fence to a control-protected input. The
 * guard is present only when the caller supplied an epoch; a legacy epoch-less
 * request carries no guard so the mutation preserves today's behavior.
 */
function withRestControlGuard<TInput extends RestControlledInput & TaskParticipantInput>(
  input: TInput,
): TInput {
  if (input.controlEpoch === undefined) {
    return input;
  }
  const controlGuard: ControlEpochGuard = {
    controlChannel: "rest",
    controlEpoch: input.controlEpoch,
    instanceId: input.instanceId,
    participantId: input.participantId,
    sessionId: input.sessionId,
  };
  return { ...input, controlGuard };
}

/**
 * Builds a configured Effect-native session service for local wiring and tests.
 */
export function createSessionServiceEffect(
  database: DatabasePool,
  options: SessionServiceOptions = {},
): SessionServiceEffect {
  const observability = new ModuleObservability(
    options.observability ?? readModuleObservabilityOptions("SessionService"),
  );
  const eventSourceId = options.eventSourceId ?? newEventSourceId();
  const approvalValidators = new Map(
    (options.approvalValidators ?? []).map((validator) => [validator.taskKind, validator]),
  );
  return makeSessionServiceEffect(database, {
    approvalValidators,
    controlEpochEnforcement: options.controlEpochEnforcement ?? true,
    eventSourceId,
    observability,
    taskClaimLeaseTtlMs: options.taskClaimLeaseTtlMs ?? taskClaimLeaseTtlMs,
    wsControlLeaseTtlMs: options.wsControlLeaseTtlMs ?? wsControlLeaseTtlMs,
  });
}

/**
 * Builds the Effect-native implementation of common durable session
 * operations. The Promise facade owns compatibility; this object owns the
 * incremental Effect migration surface.
 */
function makeSessionServiceEffect(
  database: DatabasePool,
  options: SessionServiceEffectRuntimeOptions = {},
): SessionServiceEffect {
  const observability =
    options.observability ??
    new ModuleObservability(readModuleObservabilityOptions("SessionService"));
  const approvalValidators = options.approvalValidators ?? new Map<string, TaskApprovalValidator>();
  const eventSourceId = options.eventSourceId ?? newEventSourceId();
  const claimLeaseTtlMs = options.taskClaimLeaseTtlMs ?? taskClaimLeaseTtlMs;
  const wsLeaseTtlMs = options.wsControlLeaseTtlMs ?? wsControlLeaseTtlMs;
  const stores = createSessionPersistenceStores(database);
  const sessionSummaryStore = createSessionSummaryStore(database.pool);
  const scalabilityDiagnostics = new SessionScalabilityDiagnostics(database.pool);
  const restControlPolicy = new RestControlPolicy(options.controlEpochEnforcement ?? true);
  const appendEventEffect = (
    input: AppendSessionEventInput,
  ): Effect.Effect<SessionEvent, SessionServiceFailure> =>
    trySessionPromise(() => stores.events.append(input, { sourceId: eventSourceId }));
  const {
    claimRestControlEffect,
    heartbeatRestParticipantEffect,
    refreshWebSocketControlLeaseEffect,
    registerRestParticipantEffect,
    registerWebSocketParticipantEffect,
    releaseControlLeaseEffect,
    releaseRestControlLeaseEffect,
  } = createSessionControlEffects({
    assertBroadcastEvents: assertBroadcastEventsWithObservability,
    controlEpochEnforcement: options.controlEpochEnforcement ?? true,
    eventSourceId,
    observability,
    restControlPolicy,
    stores,
    wsControlLeaseTtlMs: wsLeaseTtlMs,
  });
  const {
    cancelTaskEffect,
    claimTaskEffect,
    completeTaskEffect,
    createTaskEffect,
    ensureScheduledRunEffect,
    expireTaskClaimsEffect,
    failTaskEffect,
    recordTaskApprovalEffect,
    refreshTaskClaimEffect,
    releaseTaskEffect,
    supersedeScheduledRunsEffect,
  } = createSessionTaskEffects({
    approvalValidators,
    assertBroadcastEvents: assertBroadcastEventsWithObservability,
    eventSourceId,
    observability,
    stores,
    taskClaimLeaseTtlMs: claimLeaseTtlMs,
  });
  const traceEffect = createSessionTraceEffect(observability);
  const withRestControlOutcome = <TValue, TError>(
    routeName: string,
    action: Effect.Effect<TValue, TError>,
    classify: (value: TValue) => RestControlOutcomeName,
  ): Effect.Effect<TValue, TError> =>
    action.pipe(
      Effect.tap((value) =>
        Effect.sync(() => {
          restControlPolicy.record(routeName, classify(value));
        }),
      ),
      Effect.tapError(() =>
        Effect.sync(() => {
          restControlPolicy.record(routeName, "persistence_failure");
        }),
      ),
    );
  const restControlTraceInput = (routeName: string): Record<string, unknown> => ({
    "rest.control.mode": restControlPolicy.mode(),
    "rest.control.route": routeName,
  });
  const {
    archiveClientSessionBindingEffect,
    createSessionEffect,
    deleteSessionEffect,
    ensurePublicSessionEffect,
    listClientSessionBindingsEffect,
    publishEventEffect,
    publishRestEventEffect,
    resolveClientSessionEffect,
  } = createSessionCoreEffects({
    appendEventEffect,
    assertBroadcastEvents: (operation, sessionId, events, expectedCount) =>
      assertBroadcastEventsWithObservability(
        observability,
        operation,
        sessionId,
        events,
        expectedCount,
      ),
    claimRestControlEffect,
    controlEpochEnforcement: options.controlEpochEnforcement ?? true,
    eventSourceId,
    stores,
  });
  const {
    buildSessionContextViewEffect,
    findParticipantTaskContractEffect,
    getTaskEffect,
    listControlLeaseSnapshotsEffect,
    listEventsEffect,
    listParticipantsEffect,
    listParticipantRuntimeSnapshotsEffect,
    listSessionsEffect,
    listParticipantTaskContractsByKindEffect,
    listParticipantTaskContractsEffect,
    listTasksEffect,
    listTaskSnapshotsEffect,
    readSessionDebugSummaryEffect,
    readSessionScalabilityDebugEffect,
    readScalabilityHealthWarningsEffect,
  } = createSessionReadEffects({ scalabilityDiagnostics, sessionSummaryStore, stores });
  const withRestTaskMutationControl = <TInput extends RestControlledInput & TaskParticipantInput>(
    operation: RestTaskOperation,
    input: TInput,
    buildAction: (guarded: TInput) => Effect.Effect<TaskMutationResult, SessionServiceFailure>,
  ): Effect.Effect<RestTaskMutationResult, SessionServiceFailure> =>
    traceEffect(
      operation,
      restControlTraceInput(restTaskRouteNames[operation]),
      withRestControlOutcome(
        restTaskRouteNames[operation],
        Effect.gen(function* () {
          const control = yield* claimRestControlEffect(input, restTaskRouteNames[operation]);
          if (control.status !== "ok") {
            return control;
          }
          // The pre-check classifies conflicts and renews the deadline; the guard
          // built here re-validates the epoch atomically inside the mutation
          // transaction so a same-instance reconnect cannot slip through the gap.
          return yield* catchAtomicEpochStale(buildAction(withRestControlGuard(input)));
        }),
        (result) => classifyProtectedRestOutcome(result.status, input.controlEpoch),
      ),
      (result) => ({
        ...summarizeRestTaskMutationResult(result),
        "rest.control.outcome": classifyProtectedRestOutcome(result.status, input.controlEpoch),
      }),
    );
  const withRestTaskClaimRefreshControl = <
    TInput extends RestControlledInput & TaskParticipantInput,
  >(
    input: TInput,
    buildAction: (guarded: TInput) => Effect.Effect<TaskClaimRefreshResult, SessionServiceFailure>,
  ): Effect.Effect<RestTaskClaimRefreshResult, SessionServiceFailure> =>
    traceEffect(
      "refreshTaskClaimOverRest",
      restControlTraceInput("task.claim.refresh"),
      withRestControlOutcome(
        "task.claim.refresh",
        Effect.gen(function* () {
          const control = yield* claimRestControlEffect(input, "task.claim.refresh");
          if (control.status !== "ok") {
            return control;
          }
          return yield* catchAtomicEpochStale(buildAction(withRestControlGuard(input)));
        }),
        (result) => classifyProtectedRestOutcome(result.status, input.controlEpoch),
      ),
      (result) => ({
        ...summarizeRestTaskClaimRefreshResult(result),
        "rest.control.outcome": classifyProtectedRestOutcome(result.status, input.controlEpoch),
      }),
    );

  return {
    debugInfo: () => ({
      ...observability.debugInfo(),
      eventSourceId,
      restControlLeaseTtlMs,
      restControl: restControlPolicy.debugInfo(),
      scalability: scalabilityDiagnostics.debugInfo(),
      taskClaimLeaseTtlMs: claimLeaseTtlMs,
      wsControlLeaseTtlMs: wsLeaseTtlMs,
    }),
    createSession: (input) =>
      traceEffect(
        "createSession",
        { requestedSessionId: input.sessionId ?? null },
        createSessionEffect(input),
        (result) => ({
          eventCount: result.events.length,
          sessionId: result.session.sessionId,
        }),
      ),
    deleteSession: (input) =>
      traceEffect(
        "deleteSession",
        { sessionId: input.sessionId },
        deleteSessionEffect(input),
        (deleted) => ({ deleted }),
      ),
    ensurePublicSession: (input) =>
      traceEffect(
        "ensurePublicSession",
        { requestedSessionId: input.sessionId ?? null },
        ensurePublicSessionEffect(input),
        (result: PublicSessionEnsureResult) => ({
          created: result.created,
          sessionId: result.session.sessionId,
        }),
      ),
    listEvents: (sessionId, afterSeq, options) =>
      traceEffect(
        "listEvents",
        { afterSeq, limit: options?.limit ?? null, sessionId },
        listEventsEffect(sessionId, afterSeq, options),
        (events) => ({ eventCount: events.length }),
      ),
    listParticipants: (sessionId) =>
      traceEffect(
        "listParticipants",
        { sessionId },
        listParticipantsEffect(sessionId),
        (participants) => ({ participantCount: participants.length }),
      ),
    listSessions: () =>
      traceEffect("listSessions", {}, listSessionsEffect(), (sessions) => ({
        sessionCount: sessions.length,
      })),
    listTasks: (sessionId, status = "active") =>
      traceEffect(
        "listTasks",
        { sessionId, status },
        listTasksEffect(sessionId, status),
        (tasks) => ({ status, taskCount: tasks.length }),
      ),
    listParticipantTaskContracts: (sessionId) =>
      traceEffect(
        "listParticipantTaskContracts",
        { sessionId },
        listParticipantTaskContractsEffect(sessionId),
        (contracts) => ({ contractCount: contracts.length }),
      ),
    findParticipantTaskContract: (input) =>
      traceEffect(
        "findParticipantTaskContract",
        { sessionId: input.sessionId, taskKind: input.taskKind },
        findParticipantTaskContractEffect(input),
        (contract) => ({ found: contract !== null }),
      ),
    listParticipantTaskContractsByKind: (input) =>
      traceEffect(
        "listParticipantTaskContractsByKind",
        { sessionId: input.sessionId, taskKind: input.taskKind },
        listParticipantTaskContractsByKindEffect(input),
        (contracts) => ({ contractCount: contracts.length }),
      ),
    listParticipantRuntimeSnapshots: (sessionId) =>
      traceEffect(
        "listParticipantRuntimeSnapshots",
        { sessionId },
        listParticipantRuntimeSnapshotsEffect(sessionId),
        (participantRuntimes) => ({
          participantRuntimeCount: participantRuntimes.length,
        }),
      ),
    buildSessionContextView: (input) =>
      traceEffect(
        "buildSessionContextView",
        {
          budgetTokens: input.budgetTokens,
          forParticipant: input.forParticipant,
          sessionId: input.sessionId,
        },
        buildSessionContextViewEffect(input),
        (context) => ({
          estimatedTokens: context.budget.estimatedTokens,
          omittedEventCount: context.budget.omittedEventCount,
          recentEventCount: context.recentEvents.length,
        }),
      ),
    getTask: (sessionId, taskId) =>
      traceEffect("getTask", { sessionId, taskId }, getTaskEffect(sessionId, taskId), (task) => ({
        found: task !== null,
      })),
    listTaskSnapshots: (sessionId) =>
      traceEffect(
        "listTaskSnapshots",
        { sessionId },
        listTaskSnapshotsEffect(sessionId),
        (tasks) => ({ taskCount: tasks.length }),
      ),
    readSessionDebugSummary: (sessionId) =>
      traceEffect(
        "readSessionDebugSummary",
        { sessionId },
        readSessionDebugSummaryEffect(sessionId),
        (summary) => ({
          activeControlLeaseCount: summary.controlLeases.active,
          taskCount: summary.tasks.total,
        }),
      ),
    readSessionScalabilityDebug: (sessionId) =>
      traceEffect(
        "readSessionScalabilityDebug",
        { sessionIdPresent: sessionId.length > 0 },
        readSessionScalabilityDebugEffect(sessionId),
        (record) => ({
          activeSummaryCount: record.summary.active.length,
          healthWarningCount: record.healthWarnings.length,
          projectionCurrent: record.projection.current,
        }),
      ),
    readScalabilityHealthWarnings: () =>
      traceEffect(
        "readScalabilityHealthWarnings",
        {},
        readScalabilityHealthWarningsEffect(),
        (warnings) => ({ healthWarningCount: warnings.length }),
      ),
    listControlLeaseSnapshots: (sessionId) =>
      traceEffect(
        "listControlLeaseSnapshots",
        { sessionId },
        listControlLeaseSnapshotsEffect(sessionId),
        (controlLeases) => ({ controlLeaseCount: controlLeases.length }),
      ),
    createTask: (input) =>
      traceEffect(
        "createTask",
        {
          hasTaskId: input.taskId !== undefined,
          kind: input.kind,
          sessionId: input.sessionId,
        },
        createTaskEffect(input),
        (result) => ({
          conflictReason: result.status === "conflict" ? "task_id_conflict" : null,
          eventCount: result.events.length,
          replayReason: result.status === "replayed" ? "task_id_replay" : null,
          status: result.status,
          taskId: result.status === "conflict" ? result.taskId : result.task.taskId,
        }),
      ),
    expireTaskClaims: (input) =>
      traceEffect(
        "expireTaskClaims",
        { batchSize: input.batchSize },
        expireTaskClaimsEffect(input),
        (result) => ({ expiredCount: result.expiredCount }),
      ),
    supersedeScheduledRuns: (input) =>
      traceEffect(
        "supersedeScheduledRuns",
        {
          candidateCount: input.candidateTaskIds?.length ?? 0,
          kind: input.identity.kind,
          scheduleWindowStart: input.identity.scheduleWindow.startMs,
          sessionId: input.identity.sessionId,
        },
        supersedeScheduledRunsEffect(input),
        (result) => ({
          refusalCount: result.refusals.length,
          supersededCount: result.supersededTasks.length,
        }),
      ),
    ensureScheduledRun: (input) =>
      traceEffect(
        "ensureScheduledRun",
        {
          hasExpectedTaskId: input.expectedTaskId !== undefined,
          kind: input.identity.kind,
          scheduleWindowStart: input.identity.scheduleWindow.startMs,
          sessionId: input.identity.sessionId,
        },
        ensureScheduledRunEffect(input),
        (result) => ({
          created: result.created,
          supersededCount: result.supersededTasks.length,
          taskId: result.taskId,
        }),
      ),
    resolveClientSession: (input) =>
      traceEffect(
        "resolveClientSession",
        {
          externalId: input.externalId,
          hasRequestedSessionId: input.sessionId !== undefined,
          provider: input.provider,
        },
        resolveClientSessionEffect(input),
        (result) => ({
          bindingStatus: result.bindingStatus,
          created: result.created,
          sessionId: result.session.sessionId,
        }),
      ),
    listClientSessionBindings: (input = {}) =>
      traceEffect(
        "listClientSessionBindings",
        { provider: input.provider ?? null },
        listClientSessionBindingsEffect(input),
        (bindings) => ({ bindingCount: bindings.length }),
      ),
    archiveClientSessionBinding: (input) =>
      traceEffect(
        "archiveClientSessionBinding",
        { externalId: input.externalId, provider: input.provider },
        archiveClientSessionBindingEffect(input),
        (binding) => ({ archived: binding !== null }),
      ),
    claimTask: (input) =>
      traceEffect(
        "claimTask",
        taskParticipantTraceInput(input),
        mapTaskMutationRejection(claimTaskEffect(input)),
        summarizeTaskMutationResult,
      ),
    claimTaskOverRest: (input) =>
      withRestTaskMutationControl("claimTaskOverRest", input, (guarded) =>
        mapTaskMutationRejection(claimTaskEffect(guarded)),
      ),
    refreshTaskClaim: (input) =>
      traceEffect(
        "refreshTaskClaim",
        taskParticipantTraceInput(input),
        mapTaskClaimRefreshRejection(refreshTaskClaimEffect(input)),
        summarizeTaskClaimRefreshResult,
      ),
    refreshTaskClaimOverRest: (input) =>
      withRestTaskClaimRefreshControl(input, (guarded) =>
        mapTaskClaimRefreshRejection(refreshTaskClaimEffect(guarded)),
      ),
    cancelTask: (input) =>
      traceEffect(
        "cancelTask",
        taskParticipantTraceInput(input),
        mapTaskMutationRejection(cancelTaskEffect(input)),
        summarizeTaskMutationResult,
      ),
    cancelTaskOverRest: (input) =>
      withRestTaskMutationControl("cancelTaskOverRest", input, (guarded) =>
        mapTaskMutationRejection(cancelTaskEffect(guarded)),
      ),
    completeTask: (input) =>
      traceEffect(
        "completeTask",
        taskParticipantTraceInput(input),
        mapTaskMutationRejection(completeTaskEffect(input)),
        summarizeTaskMutationResult,
      ),
    completeTaskOverRest: (input) =>
      withRestTaskMutationControl("completeTaskOverRest", input, (guarded) =>
        mapTaskMutationRejection(completeTaskEffect(guarded)),
      ),
    failTask: (input) =>
      traceEffect(
        "failTask",
        taskParticipantTraceInput(input),
        mapTaskMutationRejection(failTaskEffect(input)),
        summarizeTaskMutationResult,
      ),
    failTaskOverRest: (input) =>
      withRestTaskMutationControl("failTaskOverRest", input, (guarded) =>
        mapTaskMutationRejection(failTaskEffect(guarded)),
      ),
    releaseTask: (input) =>
      traceEffect(
        "releaseTask",
        taskParticipantTraceInput(input),
        mapTaskMutationRejection(releaseTaskEffect(input)),
        summarizeTaskMutationResult,
      ),
    releaseTaskOverRest: (input) =>
      withRestTaskMutationControl("releaseTaskOverRest", input, (guarded) =>
        mapTaskMutationRejection(releaseTaskEffect(guarded)),
      ),
    recordTaskApproval: (input) =>
      traceEffect(
        "recordTaskApproval",
        restControlTraceInput("task.approval"),
        mapTaskApprovalRejection(recordTaskApprovalEffect(input)),
        (result) => ({
          decision: result.decision,
          eventCount: result.events.length,
          existingDecision: result.status === "ignored" ? result.existingDecision : null,
          eventId: result.status === "recorded" ? result.event.eventId : null,
          status: result.status,
          taskId: result.task?.taskId ?? null,
        }),
      ),
    recordTaskApprovalOverRest: (input) =>
      traceEffect(
        "recordTaskApprovalOverRest",
        {
          ...taskParticipantTraceInput(input),
          decision: input.decision,
          targetKey: approvalTargetKey(input.reason),
        },
        withRestControlOutcome(
          "task.approval",
          Effect.gen(function* () {
            const control = yield* claimRestControlEffect(input, "task.approval");
            if (control.status !== "ok") {
              return control;
            }
            return yield* catchAtomicEpochStale(
              mapTaskApprovalRejection(
                recordTaskApprovalEffect(withRestControlGuard(input), "recordTaskApprovalOverRest"),
              ),
            );
          }),
          (result) => classifyProtectedRestOutcome(result.status, input.controlEpoch),
        ),
        (result) => ({
          decision: "decision" in result ? result.decision : null,
          eventCount: "events" in result ? result.events.length : 0,
          existingDecision: "existingDecision" in result ? result.existingDecision : null,
          eventId: "event" in result ? result.event.eventId : null,
          status: result.status,
          "rest.control.outcome": classifyProtectedRestOutcome(result.status, input.controlEpoch),
        }),
      ),
    publishEvent: (input) =>
      traceEffect(
        "publishEvent",
        {
          eventId: input.eventId ?? null,
          producerId: input.producerId,
          sessionId: input.sessionId,
          type: input.type,
        },
        publishEventEffect(input),
        (result) => ({
          conflictReason: result.status === "conflict" ? "event_id_conflict" : null,
          eventCount: result.events.length,
          eventId: result.status === "conflict" ? result.eventId : result.event.eventId,
          eventType: result.status === "conflict" ? null : result.event.type,
          replayReason: result.status === "replayed" ? "event_id_replay" : null,
          status: result.status,
        }),
      ),
    publishRestEvent: (input) =>
      traceEffect(
        "publishRestEvent",
        restControlTraceInput("session.events.append"),
        withRestControlOutcome("session.events.append", publishRestEventEffect(input), (result) =>
          classifyProtectedRestOutcome(result.status, input.controlEpoch),
        ),
        (result) => ({
          conflictReason: result.status === "conflict" ? "event_id_conflict" : null,
          eventCount: "events" in result ? result.events.length : 0,
          replayReason: result.status === "replayed" ? "event_id_replay" : null,
          "rest.control.outcome": classifyProtectedRestOutcome(result.status, input.controlEpoch),
          status: result.status,
        }),
      ),
    registerRestParticipant: (input) =>
      traceEffect(
        "registerRestParticipant",
        restControlTraceInput("session.participant.register"),
        withRestControlOutcome(
          "session.participant.register",
          registerRestParticipantEffect(input),
          classifyAcquisitionOutcome,
        ),
        (result) => ({
          eventCount: result.status === "ok" ? result.events.length : 0,
          registrationStatus: result.status === "ok" ? result.registrationStatus : null,
          "rest.control.outcome": classifyAcquisitionOutcome(result),
          status: result.status,
        }),
      ),
    registerWebSocketParticipant: (input) =>
      traceEffect(
        "registerWebSocketParticipant",
        {
          participantId: input.participantId,
          runtimeKind: input.runtimeKind,
          sessionId: input.sessionId,
        },
        registerWebSocketParticipantEffect(input),
        (result) => ({
          eventCount: result.status === "ok" ? result.events.length : 0,
          registrationStatus: result.status === "ok" ? result.registrationStatus : null,
          status: result.status,
        }),
      ),
    refreshWebSocketControlLease: (input) =>
      traceEffect(
        "refreshWebSocketControlLease",
        {
          participantId: input.participantId,
          sessionId: input.sessionId,
        },
        refreshWebSocketControlLeaseEffect(input),
        (result) => ({ status: result.status }),
      ),
    heartbeatRestParticipant: (input) =>
      traceEffect(
        "heartbeatRestParticipant",
        restControlTraceInput("session.participant.heartbeat"),
        withRestControlOutcome(
          "session.participant.heartbeat",
          heartbeatRestParticipantEffect(input),
          (result) => classifyProtectedRestOutcome(result.status, input.controlEpoch),
        ),
        (result) => ({
          eventCount: result.status === "ok" ? result.events.length : 0,
          participantFound: result.status === "ok" && result.participant !== null,
          "rest.control.outcome": classifyProtectedRestOutcome(result.status, input.controlEpoch),
          status: result.status,
        }),
      ),
    releaseControlLease: (input) =>
      traceEffect(
        "releaseControlLease",
        {
          controlChannel: input.controlChannel,
          participantId: input.participantId,
          sessionId: input.sessionId,
        },
        releaseControlLeaseEffect(input),
      ),
    releaseRestControlLease: (
      input,
    ): Effect.Effect<RestControlReleaseResult, SessionServiceFailure> =>
      traceEffect(
        "releaseRestControlLease",
        restControlTraceInput("session.participant.control.release"),
        withRestControlOutcome(
          "session.participant.control.release",
          releaseRestControlLeaseEffect(input),
          (result) => classifyProtectedRestOutcome(result.status, input.controlEpoch),
        ),
        (result) => ({
          released: result.status === "ok" ? result.released : false,
          "rest.control.outcome": classifyProtectedRestOutcome(result.status, input.controlEpoch),
          status: result.status,
        }),
      ),
  };
}

/** Maps one final protected REST result onto the bounded control telemetry taxonomy. */
function classifyProtectedRestOutcome(
  status: string,
  controlEpoch: number | undefined,
): RestControlOutcomeName {
  if (status === "control_epoch_required") {
    return "epoch_required";
  }
  if (status === "control_epoch_stale") {
    return "epoch_stale";
  }
  if (status === "control_conflict") {
    return "control_conflict";
  }
  return controlEpoch === undefined ? "unfenced_accepted" : "fenced_accepted";
}

/** Maps one final acquisition result onto the bounded control telemetry taxonomy. */
function classifyAcquisitionOutcome(result: {
  readonly acquisitionStatus?: string | undefined;
  readonly status: string;
}): RestControlOutcomeName {
  if (result.status === "control_acquisition_id_required") {
    return "acquisition_id_required";
  }
  if (result.status === "control_acquisition_stale") {
    return "acquisition_stale";
  }
  if (result.status === "control_conflict") {
    return "control_conflict";
  }
  return result.acquisitionStatus === "replayed" ? "acquisition_replayed" : "fenced_accepted";
}
