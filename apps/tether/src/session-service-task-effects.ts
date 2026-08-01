import { Effect } from "effect";

import { approvalTargetKey } from "./approval-target-key.js";
import type {
  PersistedTaskApprovalResult,
  SessionPersistenceStores,
} from "./db-store-contracts.js";
import {
  ApprovalTargetManifestError,
  type EnsureScheduledRunResult,
  ScheduledRunIdentityConflictError,
  ScheduledTaskIdentityMismatchError,
  TaskClaimExpirationDeadlockError,
} from "./db.js";
import type { ModuleObservability } from "./observability.js";
import { classifyScheduledSupersession, deriveScheduledTaskId, newTaskId } from "./protocol.js";
import {
  type AppliedTaskClaimRefreshResult,
  type AppliedTaskMutationResult,
  type CancelTaskInput,
  type ClaimOwnedTaskInput,
  type CompleteTaskInput,
  type CreateTaskInput,
  type EnsureScheduledRunRequest,
  type FailTaskInput,
  type RecordedTaskApprovalResult,
  type RecordTaskApprovalInput,
  type RestTaskClaimRefreshResult,
  type RestTaskMutationResult,
  type ScheduledRunEnsureResult,
  type ScheduledSupersessionRefusal,
  type ScheduledSupersessionResult,
  type SessionServiceFailure,
  type SupersedeScheduledRunsRequest,
  TaskApprovalIgnoredError,
  TaskApprovalRejectedError,
  type TaskApprovalRejectionReason,
  type TaskApprovalResult,
  type TaskApprovalValidator,
  TaskClaimRefreshRejectedError,
  type TaskClaimRefreshResult,
  type TaskClaimsExpiredResult,
  type TaskCreatedResult,
  TaskMutationRejectedError,
  type TaskMutationResult,
  type TaskParticipantInput,
  SessionServicePersistenceError,
} from "./session-service-contracts.js";
import { trySessionPromise } from "./session-service-runtime.js";
import type {
  CandidateScheduleIdentity,
  ScheduledMaintenanceIdentity,
  SessionEvent,
  TaskRecord,
} from "./types.js";

/** System actor recorded on supersession events when no operator is supplied. */
const scheduledSupersessionActorId = "system";

interface GenericApprovableTaskResult {
  readonly dryRun: {
    readonly approvalSummary: readonly string[];
    readonly authorization: "needs_approval";
    readonly request: unknown;
    readonly target: string;
  };
  readonly kind: string;
}

/** Broadcast invariant assertion supplied by the session service boundary. */
export type AssertBroadcastEvents = (
  observability: ModuleObservability,
  operation: string,
  sessionId: string,
  events: readonly SessionEvent[],
  expectedCount?: number,
) => void;

/** Dependencies for task-specific Effect builders. */
export interface SessionTaskEffectsInput {
  readonly approvalValidators: ReadonlyMap<string, TaskApprovalValidator>;
  readonly assertBroadcastEvents: AssertBroadcastEvents;
  readonly eventSourceId: string;
  readonly observability: ModuleObservability;
  readonly stores: SessionPersistenceStores;
  readonly taskClaimLeaseTtlMs: number;
}

/** Task-specific Effect programs used by the durable session service. */
export interface SessionTaskEffects {
  readonly cancelTaskEffect: (
    input: CancelTaskInput,
  ) => Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure>;
  readonly claimTaskEffect: (
    input: TaskParticipantInput,
  ) => Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure>;
  readonly completeTaskEffect: (
    input: CompleteTaskInput,
  ) => Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure>;
  readonly createTaskEffect: (
    input: CreateTaskInput,
  ) => Effect.Effect<TaskCreatedResult, SessionServiceFailure>;
  readonly expireTaskClaimsEffect: (input: {
    readonly batchSize: number;
  }) => Effect.Effect<TaskClaimsExpiredResult, SessionServiceFailure>;
  readonly failTaskEffect: (
    input: FailTaskInput,
  ) => Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure>;
  readonly recordTaskApprovalEffect: (
    input: RecordTaskApprovalInput,
    operation?: string,
  ) => Effect.Effect<RecordedTaskApprovalResult, SessionServiceFailure>;
  readonly refreshTaskClaimEffect: (
    input: ClaimOwnedTaskInput,
  ) => Effect.Effect<AppliedTaskClaimRefreshResult, SessionServiceFailure>;
  readonly releaseTaskEffect: (
    input: ClaimOwnedTaskInput,
  ) => Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure>;
  readonly supersedeScheduledRunsEffect: (
    input: SupersedeScheduledRunsRequest,
  ) => Effect.Effect<ScheduledSupersessionResult, SessionServiceFailure>;
  readonly ensureScheduledRunEffect: (
    input: EnsureScheduledRunRequest,
  ) => Effect.Effect<ScheduledRunEnsureResult, SessionServiceFailure>;
}

/** Builds task lifecycle and approval Effect programs for one service instance. */
export function createSessionTaskEffects(input: SessionTaskEffectsInput): SessionTaskEffects {
  const taskMutationWithEventEffect = (
    operation: string,
    taskInput: TaskParticipantInput,
    persist: () => Promise<{
      readonly event: SessionEvent;
      readonly task: AppliedTaskMutationResult["task"];
    } | null>,
  ): Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure> =>
    Effect.gen(function* () {
      const persisted = yield* trySessionPromise(persist);
      if (!persisted) {
        return yield* Effect.fail(new TaskMutationRejectedError(operation));
      }
      const result = {
        events: [persisted.event] as const,
        status: "applied" as const,
        task: persisted.task,
      };
      yield* Effect.sync(() =>
        assertTaskMutationResultWithObservability(
          input.observability,
          input.assertBroadcastEvents,
          operation,
          taskInput,
          result,
        ),
      );
      return result;
    });

  return {
    cancelTaskEffect: (taskInput) =>
      taskMutationWithEventEffect("cancelTask", taskInput, () =>
        input.stores.tasks.cancelWithEvent({
          controlGuard: taskInput.controlGuard,
          eventSourceId: input.eventSourceId,
          participantId: taskInput.participantId,
          reason: taskInput.reason,
          sessionId: taskInput.sessionId,
          taskId: taskInput.taskId,
        }),
      ),
    claimTaskEffect: (taskInput) =>
      // A claim has its own path because it can commit MORE than one event: a
      // normal claim appends a single `task.claimed`, while an atomic reclaim of
      // an elapsed claim appends the ordered pair `task.claim_expired` then
      // `task.claimed`. Every committed event is broadcast, unlike the
      // single-event mutations routed through `taskMutationWithEventEffect`.
      Effect.gen(function* () {
        const persisted = yield* trySessionPromise(() =>
          input.stores.tasks.claimWithEvent({
            ...taskInput,
            claimLeaseTtlMs: input.taskClaimLeaseTtlMs,
            eventSourceId: input.eventSourceId,
          }),
        );
        if (!persisted) {
          return yield* Effect.fail(new TaskMutationRejectedError("claimTask"));
        }
        const result = {
          events: persisted.events,
          status: "applied" as const,
          task: persisted.task,
        };
        yield* Effect.sync(() => {
          // A normal claim commits one event; an atomic reclaim commits exactly
          // two (`task.claim_expired` then `task.claimed`). Any other count is an
          // invariant violation.
          input.observability.assertInvariant(
            result.events.length === 1 || result.events.length === 2,
            "claimTask",
            "Claim must commit one or two events",
            {
              eventCount: result.events.length,
              participantId: taskInput.participantId,
              sessionId: taskInput.sessionId,
              taskId: taskInput.taskId,
            },
          );
          assertTaskMutationResultWithObservability(
            input.observability,
            input.assertBroadcastEvents,
            "claimTask",
            taskInput,
            result,
            result.events.length,
          );
        });
        return result;
      }),
    completeTaskEffect: (taskInput) =>
      taskMutationWithEventEffect("completeTask", taskInput, () =>
        input.stores.tasks.completeWithEvent({ ...taskInput, eventSourceId: input.eventSourceId }),
      ),
    createTaskEffect: (taskInput) =>
      Effect.gen(function* () {
        // A create that carries a schedule identity is ALWAYS routed through the
        // deterministic ensure-scheduled-run path with a server-derived id, whether
        // or not the caller supplied a taskId. This is the service chokepoint: a
        // generic create can never produce a scheduled run under a random or omitted
        // id that would evade schedule-identity uniqueness and atomic supersession
        // of older windows. A supplied id must equal the derived identity or the
        // create is rejected.
        if (taskInput.schedule !== undefined) {
          return yield* createScheduledTaskEffect(input, taskInput, taskInput.schedule);
        }
        const persisted = yield* trySessionPromise(() =>
          input.stores.tasks.createWithEvent({
            eventSourceId: input.eventSourceId,
            input: taskInput.input ?? null,
            kind: taskInput.kind,
            objective: taskInput.objective,
            sessionId: taskInput.sessionId,
            taskId: taskInput.taskId ?? newTaskId(),
            taskIdSource: taskInput.taskId === undefined ? "generated" : "caller",
          }),
        );
        yield* Effect.sync(() =>
          input.assertBroadcastEvents(
            input.observability,
            "createTask",
            taskInput.sessionId,
            persisted.events,
            persisted.status === "created" ? 1 : 0,
          ),
        );
        return persisted;
      }),
    expireTaskClaimsEffect: (taskInput) =>
      Effect.gen(function* () {
        const events = yield* trySessionPromise(
          () =>
            input.stores.tasks.expireClaims({
              ...taskInput,
              sourceId: input.eventSourceId,
            }),
          "expireTaskClaims",
        ).pipe(
          Effect.tapError((failure) =>
            Effect.sync(() => logTaskClaimExpirationRetryDiagnostics(input.observability, failure)),
          ),
        );
        const result = { events, expiredCount: events.length };
        yield* Effect.sync(() =>
          assertExpiredClaimEventsWithObservability(
            input.observability,
            "expireTaskClaims",
            result.events,
          ),
        );
        return result;
      }),
    failTaskEffect: (taskInput) =>
      taskMutationWithEventEffect("failTask", taskInput, () =>
        input.stores.tasks.failWithEvent({ ...taskInput, eventSourceId: input.eventSourceId }),
      ),
    recordTaskApprovalEffect: (taskInput, operation = "recordTaskApproval") =>
      Effect.gen(function* () {
        const task = yield* trySessionPromise(() =>
          input.stores.tasks.get({
            sessionId: taskInput.sessionId,
            taskId: taskInput.taskId,
          }),
        );
        if (!task) {
          return yield* Effect.fail(
            new TaskApprovalRejectedError(taskInput.decision, "task_not_found", task),
          );
        }
        const rejectionReason = approvalRejectionReasonFromValidators(
          input.approvalValidators,
          task,
        );
        if (rejectionReason) {
          return yield* Effect.fail(
            new TaskApprovalRejectedError(taskInput.decision, rejectionReason, task),
          );
        }
        const targetKey = approvalTargetKey(taskInput.reason, taskInput.target);
        const persisted: PersistedTaskApprovalResult | null = yield* trySessionPromise(() =>
          input.stores.tasks.recordApproval({
            controlGuard: taskInput.controlGuard,
            decision: taskInput.decision,
            eventSourceId: input.eventSourceId,
            participantId: taskInput.participantId,
            reason: taskInput.reason,
            sessionId: taskInput.sessionId,
            ...(taskInput.target === undefined ? {} : { target: taskInput.target }),
            taskId: taskInput.taskId,
          }),
        ).pipe(
          Effect.catchAll((error): Effect.Effect<never, SessionServiceFailure> => {
            if (error.cause instanceof ApprovalTargetManifestError) {
              input.observability.debug(operation, "approval.target_manifest.rejected", {
                reason: error.cause.reason,
                sessionId: taskInput.sessionId,
                taskId: taskInput.taskId,
              });
              return Effect.fail(
                new TaskApprovalRejectedError(taskInput.decision, error.cause.reason, task),
              );
            }
            return Effect.fail(error);
          }),
        );
        if (!persisted) {
          return yield* Effect.fail(
            new TaskApprovalRejectedError(taskInput.decision, "task_not_found", task),
          );
        }
        yield* Effect.sync(() =>
          assertTaskApprovalResultWithObservability(
            input.observability,
            input.assertBroadcastEvents,
            operation,
            taskInput,
            targetKey,
            persisted,
          ),
        );
        if (persisted.status === "ignored") {
          return yield* Effect.fail(
            new TaskApprovalIgnoredError(
              persisted.approval,
              taskInput.decision,
              persisted.existingDecision,
              persisted.existingDecision === "approved" ? "already_approved" : "already_rejected",
              task,
            ),
          );
        }
        const result = {
          approval: persisted.approval,
          decision: taskInput.decision,
          event: persisted.event,
          events: persisted.events,
          status: "recorded" as const,
          task,
        };
        return result;
      }),
    refreshTaskClaimEffect: (taskInput) =>
      Effect.gen(function* () {
        const task = yield* trySessionPromise(() =>
          input.stores.tasks.refreshClaim({
            ...taskInput,
            claimLeaseTtlMs: input.taskClaimLeaseTtlMs,
          }),
        );
        if (!task) {
          return yield* Effect.fail(new TaskClaimRefreshRejectedError());
        }
        return { events: [], status: "applied" as const, task };
      }),
    releaseTaskEffect: (taskInput) =>
      taskMutationWithEventEffect("releaseTask", taskInput, () =>
        input.stores.tasks.releaseWithEvent({ ...taskInput, eventSourceId: input.eventSourceId }),
      ),
    supersedeScheduledRunsEffect: (request) =>
      Effect.gen(function* () {
        const identity = request.identity;
        const superseded = yield* trySessionPromise(
          () =>
            input.stores.tasks.supersedeScheduled({
              ...(request.candidateTaskIds !== undefined
                ? { candidateTaskIds: request.candidateTaskIds }
                : {}),
              eventSourceId: input.eventSourceId,
              kind: identity.kind,
              participantId: request.participantId ?? scheduledSupersessionActorId,
              ...(request.reason === undefined ? {} : { reason: request.reason }),
              scheduleAlgorithmVersion: identity.scheduleWindow.algorithmVersion,
              scheduleIdentityVersion: identity.identityVersion,
              scheduleIntervalMs: identity.scheduleWindow.intervalMs,
              scheduleScopeKey: identity.scopeKey,
              scheduleWindowStart: identity.scheduleWindow.startMs,
              sessionId: identity.sessionId,
            }),
          "supersedeScheduledRuns",
        );
        const supersededIds = new Set(superseded.tasks.map((task) => task.taskId));
        const refusals = yield* classifySupersessionRefusals(input, request, supersededIds);
        const result: ScheduledSupersessionResult = {
          events: superseded.events,
          refusals,
          status: "applied",
          supersededTasks: superseded.tasks,
        };
        return result;
      }),
    ensureScheduledRunEffect: (request) =>
      Effect.gen(function* () {
        const ensured = yield* ensureScheduledRunStoreEffect(input, request);
        const createdEvents = ensured.current.status === "created" ? [ensured.current.event] : [];
        const result: ScheduledRunEnsureResult = {
          created: ensured.current.status === "created",
          current: ensured.current.task,
          events: [...ensured.supersededEvents, ...createdEvents],
          status: "ensured",
          supersededTasks: ensured.supersededTasks,
          taskId: ensured.taskId,
        };
        return result;
      }),
  };
}

/**
 * Runs the atomic ensure-scheduled-run store operation for one schedule identity.
 * Shared by the dedicated ensure effect and the generic create chokepoint so both
 * derive the deterministic id server-side and participate in one atomic
 * supersede-then-insert transaction.
 */
function ensureScheduledRunStoreEffect(
  input: SessionTaskEffectsInput,
  request: EnsureScheduledRunRequest,
): Effect.Effect<EnsureScheduledRunResult, SessionServiceFailure> {
  const identity = request.identity;
  return trySessionPromise(
    () =>
      input.stores.tasks.ensureScheduledRun({
        eventSourceId: input.eventSourceId,
        ...(request.expectedTaskId !== undefined ? { expectedTaskId: request.expectedTaskId } : {}),
        input: request.input ?? null,
        kind: identity.kind,
        objective: request.objective,
        participantId: request.participantId ?? scheduledSupersessionActorId,
        ...(request.reason === undefined ? {} : { reason: request.reason }),
        scheduleAlgorithmVersion: identity.scheduleWindow.algorithmVersion,
        scheduleIdentityVersion: identity.identityVersion,
        scheduleIntervalMs: identity.scheduleWindow.intervalMs,
        scheduleScopeKey: identity.scopeKey,
        scheduleWindowStart: identity.scheduleWindow.startMs,
        sessionId: identity.sessionId,
      }),
    "ensureScheduledRun",
  );
}

/**
 * Handles a create that carries a schedule identity. The deterministic task id is
 * derived server-side; a caller-supplied id that does not equal it is rejected so
 * a generic create cannot smuggle in a mismatched scheduled id. The create then
 * runs through the atomic ensure path, which supersedes older windows and
 * inserts-or-replays the current run in one transaction, and the ensure result is
 * mapped back into the generic create result shape (older-window supersession
 * cancellation events are carried on the broadcast list).
 */
function createScheduledTaskEffect(
  input: SessionTaskEffectsInput,
  taskInput: CreateTaskInput,
  schedule: NonNullable<CreateTaskInput["schedule"]>,
): Effect.Effect<TaskCreatedResult, SessionServiceFailure> {
  return Effect.gen(function* () {
    const identity: ScheduledMaintenanceIdentity = {
      identityVersion: schedule.scheduleIdentityVersion,
      kind: taskInput.kind,
      scheduleWindow: {
        algorithmVersion: schedule.scheduleAlgorithmVersion,
        endMs: schedule.scheduleWindowStart + schedule.scheduleIntervalMs,
        intervalMs: schedule.scheduleIntervalMs,
        startMs: schedule.scheduleWindowStart,
      },
      scopeKey: schedule.scheduleScopeKey,
      sessionId: taskInput.sessionId,
    };
    const derivedTaskId = deriveScheduledTaskId(identity);
    if (taskInput.taskId !== undefined && taskInput.taskId !== derivedTaskId) {
      return yield* Effect.fail(
        new SessionServicePersistenceError(
          "createTask",
          new ScheduledTaskIdentityMismatchError({
            derivedTaskId,
            suppliedTaskId: taskInput.taskId,
          }),
        ),
      );
    }
    const ensuredOrConflict = yield* ensureScheduledRunStoreEffect(input, {
      ...(taskInput.taskId !== undefined ? { expectedTaskId: taskInput.taskId } : {}),
      identity,
      input: taskInput.input ?? null,
      objective: taskInput.objective,
    }).pipe(
      Effect.map((ensured) => ({ ensured, outcome: "ensured" as const })),
      Effect.catchAll((error) => {
        // An unrelated task occupying the derived deterministic id is the same
        // caller-visible situation as a duplicate caller-supplied task id, so
        // it maps to the generic create's typed conflict result instead of an
        // opaque persistence failure.
        const conflict = scheduledRunIdentityConflictFromFailure(error);
        return conflict !== null
          ? Effect.succeed({ conflict, outcome: "conflict" as const })
          : Effect.fail(error);
      }),
    );
    if (ensuredOrConflict.outcome === "conflict") {
      return {
        conflictingFields: ensuredOrConflict.conflict.conflictingFields,
        events: [],
        status: "conflict",
        task: null,
        taskId: ensuredOrConflict.conflict.taskId,
      };
    }
    const ensured = ensuredOrConflict.ensured;
    const createdEvents = ensured.current.status === "created" ? [ensured.current.event] : [];
    const events = [...ensured.supersededEvents, ...createdEvents];
    yield* Effect.sync(() =>
      input.assertBroadcastEvents(input.observability, "createTask", taskInput.sessionId, events),
    );
    if (ensured.current.status === "created") {
      return {
        event: ensured.current.event,
        events,
        status: "created",
        task: ensured.current.task,
      };
    }
    return { events, status: "replayed", task: ensured.current.task };
  });
}

/** Extracts a scheduled-run occupant conflict from a service-wrapped failure. */
function scheduledRunIdentityConflictFromFailure(
  error: SessionServiceFailure,
): ScheduledRunIdentityConflictError | null {
  if (
    error instanceof SessionServicePersistenceError &&
    error.cause instanceof ScheduledRunIdentityConflictError
  ) {
    return error.cause;
  }
  return null;
}

/**
 * Reads each inspected candidate that the atomic predicate did not supersede and
 * classifies it into a typed refusal. A candidate that raced a claim now carries
 * a claim, so it classifies as `claimed` and can never be cancelled afterward.
 */
function classifySupersessionRefusals(
  input: SessionTaskEffectsInput,
  request: SupersedeScheduledRunsRequest,
  supersededIds: ReadonlySet<string>,
): Effect.Effect<readonly ScheduledSupersessionRefusal[], SessionServiceFailure> {
  return Effect.gen(function* () {
    const candidateIds = request.candidateTaskIds ?? [];
    const refusals: ScheduledSupersessionRefusal[] = [];
    for (const taskId of candidateIds) {
      if (supersededIds.has(taskId)) {
        continue;
      }
      const task = yield* trySessionPromise(() =>
        input.stores.tasks.get({ sessionId: request.identity.sessionId, taskId }),
      );
      if (!task) {
        continue;
      }
      const outcome = classifyScheduledSupersession(
        {
          cancelledAt: task.cancelledAt,
          claimedBy: task.claimedBy,
          completedAt: task.completedAt,
          failedAt: task.failedAt,
          kind: task.kind,
          schedule: toCandidateScheduleIdentity(task),
          sessionId: task.sessionId,
        },
        request.identity,
      );
      if (outcome.decision === "refuse") {
        refusals.push({ reason: outcome.reason, taskId: task.taskId });
      }
    }
    return refusals;
  });
}

/** Extracts the schedule identity attached to a durable task record. */
function toCandidateScheduleIdentity(task: TaskRecord): CandidateScheduleIdentity | null {
  return task.schedule ?? null;
}

/**
 * Maps internal Effect task mutation rejection into the public compatibility
 * result shape.
 */
export function mapTaskMutationRejection(
  effect: Effect.Effect<AppliedTaskMutationResult, SessionServiceFailure>,
): Effect.Effect<TaskMutationResult, SessionServiceFailure> {
  return effect.pipe(
    Effect.catchAll((error) => {
      if (error instanceof TaskMutationRejectedError) {
        const result: TaskMutationResult = { events: [], status: "rejected", task: null };
        return Effect.succeed(result);
      }
      return Effect.fail(error);
    }),
  );
}

/**
 * Maps internal Effect task claim refresh rejection into the public result
 * shape.
 */
export function mapTaskClaimRefreshRejection(
  effect: Effect.Effect<AppliedTaskClaimRefreshResult, SessionServiceFailure>,
): Effect.Effect<TaskClaimRefreshResult, SessionServiceFailure> {
  return effect.pipe(
    Effect.catchAll((error) => {
      if (error instanceof TaskClaimRefreshRejectedError) {
        const result: TaskClaimRefreshResult = { events: [], status: "rejected", task: null };
        return Effect.succeed(result);
      }
      return Effect.fail(error);
    }),
  );
}

/**
 * Maps internal Effect approval failures into the existing public approval
 * result union.
 */
export function mapTaskApprovalRejection(
  effect: Effect.Effect<RecordedTaskApprovalResult, SessionServiceFailure>,
): Effect.Effect<TaskApprovalResult, SessionServiceFailure> {
  const recover = (
    error: SessionServiceFailure,
  ): Effect.Effect<TaskApprovalResult, SessionServiceFailure> => {
    if (error instanceof TaskApprovalRejectedError) {
      const result: TaskApprovalResult = {
        decision: error.decision,
        events: [],
        rejectionReason: error.rejectionReason,
        status: "rejected",
        task: error.task,
      };
      return Effect.succeed(result);
    }
    if (error instanceof TaskApprovalIgnoredError) {
      const result: TaskApprovalResult = {
        approval: error.approval,
        decision: error.decision,
        events: [],
        existingDecision: error.existingDecision,
        ignoredReason: error.ignoredReason,
        status: "ignored",
        task: error.task,
      };
      return Effect.succeed(result);
    }
    return Effect.fail(error);
  };
  return effect.pipe(
    Effect.map((result): TaskApprovalResult => result),
    Effect.catchAll(recover),
  );
}

/**
 * Builds the common trace input for task operations scoped to a participant.
 */
export function taskParticipantTraceInput(input: TaskParticipantInput): Record<string, unknown> {
  return {
    participantId: input.participantId,
    sessionId: input.sessionId,
    taskId: input.taskId,
  };
}

/**
 * Summarizes task mutation results for boundary logs and spans.
 */
export function summarizeTaskMutationResult(result: TaskMutationResult): Record<string, unknown> {
  return {
    eventCount: result.events.length,
    status: result.status,
    taskId: result.task?.taskId ?? null,
  };
}

/**
 * Summarizes task claim-refresh results for boundary logs and spans.
 */
export function summarizeTaskClaimRefreshResult(
  result: TaskClaimRefreshResult,
): Record<string, unknown> {
  return {
    status: result.status,
    taskId: result.task?.taskId ?? null,
  };
}

/**
 * Summarizes REST task mutation results, including control-conflict outcomes.
 */
export function summarizeRestTaskMutationResult(
  result: RestTaskMutationResult,
): Record<string, unknown> {
  return {
    eventCount: "events" in result ? result.events.length : 0,
    status: result.status,
    taskId: "task" in result ? (result.task?.taskId ?? null) : null,
  };
}

/**
 * Summarizes REST task claim-refresh results, including control-conflict
 * outcomes.
 */
export function summarizeRestTaskClaimRefreshResult(
  result: RestTaskClaimRefreshResult,
): Record<string, unknown> {
  return {
    status: result.status,
    taskId: "task" in result ? (result.task?.taskId ?? null) : null,
  };
}

/**
 * Checks the invariants shared by task mutation result shapes.
 */
function assertTaskMutationResultWithObservability(
  observability: ModuleObservability,
  assertBroadcastEvents: AssertBroadcastEvents,
  operation: string,
  input: TaskParticipantInput,
  result: TaskMutationResult,
  expectedEventCount = 1,
): void {
  if (result.status === "rejected") {
    observability.assertInvariant(
      result.events.length === 0 && result.task === null,
      operation,
      "Rejected task mutation must not produce task or events",
      taskParticipantTraceInput(input),
    );
    return;
  }
  observability.assertInvariant(
    result.task.taskId === input.taskId,
    operation,
    "Task mutation returned a different task",
    {
      actualTaskId: result.task.taskId,
      expectedTaskId: input.taskId,
      participantId: input.participantId,
      sessionId: input.sessionId,
    },
  );
  assertBroadcastEvents(
    observability,
    operation,
    input.sessionId,
    result.events,
    expectedEventCount,
  );
}

/** Ensures claim-expiration sweeps only produce claim-expired events. */
function assertExpiredClaimEventsWithObservability(
  observability: ModuleObservability,
  operation: string,
  events: readonly SessionEvent[],
): void {
  for (const event of events) {
    observability.assertInvariant(
      event.type === "task.claim_expired",
      operation,
      "Claim expiry must only emit task.claim_expired events",
      {
        eventId: event.eventId,
        sessionId: event.sessionId,
        type: event.type,
      },
    );
  }
}

/** Emits retry metadata before the service failure wrapper is normalized. */
function logTaskClaimExpirationRetryDiagnostics(
  observability: ModuleObservability,
  failure: SessionServiceFailure,
): void {
  if (!(failure instanceof SessionServicePersistenceError)) {
    return;
  }
  const cause = failure.cause;
  if (!(cause instanceof TaskClaimExpirationDeadlockError)) {
    return;
  }
  observability.debug("expireTaskClaims", "task_claim_expiration.retry_exhausted", {
    ...cause.diagnostics,
  });
}

function assertTaskApprovalResultWithObservability(
  observability: ModuleObservability,
  assertBroadcastEvents: AssertBroadcastEvents,
  operation: string,
  input: RecordTaskApprovalInput,
  targetKey: string,
  result: PersistedTaskApprovalResult,
): void {
  observability.debug(operation, "approval.recording.result", {
    approvalEventId: result.status === "recorded" ? result.event.eventId : null,
    decision: input.decision,
    existingDecision: result.status === "ignored" ? result.existingDecision : null,
    participantId: input.participantId,
    resultStatus: result.status,
    sessionId: input.sessionId,
    targetKey,
    taskId: input.taskId,
  });
  if (result.status === "recorded") {
    observability.assertInvariant(
      result.events.length === 1 && result.event.type === "approval.recorded",
      operation,
      "Recorded approval must emit exactly one approval.recorded event",
      {
        eventCount: result.events.length,
        eventType: result.event.type,
        sessionId: input.sessionId,
        targetKey,
        taskId: input.taskId,
      },
    );
    assertBroadcastEvents(observability, operation, input.sessionId, result.events, 1);
    return;
  }
  observability.assertInvariant(
    result.events.length === 0,
    operation,
    "Ignored approval must not emit events",
    {
      existingDecision: result.existingDecision,
      sessionId: input.sessionId,
      targetKey,
      taskId: input.taskId,
    },
  );
}

/**
 * Returns the first reason a task cannot accept an approval/rejection decision.
 */
function approvalRejectionReasonFromValidators(
  approvalValidators: ReadonlyMap<string, TaskApprovalValidator>,
  task: TaskRecord,
): TaskApprovalRejectionReason | null {
  const validator = approvalValidators.get(task.kind);
  if (task.completedAt === null) {
    return "task_not_completed";
  }
  if (!validator) {
    return isGenericApprovableTaskResult(task.kind, task.result) ? null : "unsupported_task_kind";
  }
  return validator.validate(task) ? null : "invalid_approval_plan";
}

/** Returns whether a completed participant result exposes a generic approval dry-run envelope. */
function isGenericApprovableTaskResult(
  taskKind: string,
  result: unknown,
): result is GenericApprovableTaskResult {
  if (!isRecord(result) || result.kind !== taskKind || !isRecord(result.dryRun)) {
    return false;
  }
  return (
    result.dryRun.authorization === "needs_approval" &&
    Array.isArray(result.dryRun.approvalSummary) &&
    result.dryRun.approvalSummary.every((line) => typeof line === "string") &&
    typeof result.dryRun.target === "string" &&
    result.dryRun.target.length > 0 &&
    "request" in result.dryRun
  );
}

/** Checks for a plain object record. */
function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
