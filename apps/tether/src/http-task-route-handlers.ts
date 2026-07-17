import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { validateJsonSchemaSubset } from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";
import { ZodError, z } from "zod";

import {
  authorize,
  authorizeParticipantIdentity,
  effectiveParticipantId,
} from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import { ScheduledRunIdentityConflictError, ScheduledTaskIdentityMismatchError } from "./db.js";
import {
  broadcastEvents,
  parseJsonBody,
  sendAuthError,
  sendControlEpochRequired,
  sendControlEpochStale,
  sendControlLeaseConflict,
  sendJson,
} from "./http-route-runtime.js";
import { defineHttpRoute, type HttpRouteSpec, matchHttpRoute } from "./http-route-spec.js";
import type { SubscriptionHub } from "./hub.js";
import {
  cancelTaskSchema,
  claimTaskSchema,
  completeTaskSchema,
  createTaskSchema,
  failTaskSchema,
  recordTaskApprovalSchema,
  refreshTaskClaimSchema,
  releaseTaskSchema,
  scheduledTaskIdentitySchema,
} from "./protocol.js";
import type { ResourceLimits } from "./resource-limits.js";
import type {
  RestTaskApprovalResult,
  RestTaskClaimRefreshResult,
  RestTaskMutationResult,
  SessionServiceEffect,
  TaskApprovalRejectionReason,
} from "./session-service.js";
import {
  type ScheduledSupersessionResult,
  SessionServicePersistenceError,
} from "./session-service-contracts.js";
import type { TaskListStatus } from "./types.js";

/** Deterministic schedule and Mailbox Scope identity carried on a scheduled create. */
interface ScheduledTaskCreateSchedule {
  readonly mailboxAccountId: string;
  readonly mailboxProvider: string;
  readonly scheduleAlgorithmVersion: number;
  readonly scheduleIntervalMs: number;
  readonly scheduleWindowStart: number;
}

interface ScheduledTaskCreateInput {
  readonly body: {
    readonly input?: Record<string, unknown> | null | undefined;
    readonly kind: string;
    readonly objective: string;
    readonly schedule: ScheduledTaskCreateSchedule;
    readonly taskId?: string | undefined;
  };
  readonly hub: SubscriptionHub;
  readonly response: ServerResponse;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
}

interface TaskHttpRouteHandlerInput {
  readonly authContext: AuthContext | null;
  readonly hub: SubscriptionHub;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly service: SessionServiceEffect;
  readonly url: URL;
}

interface RestTaskMutationRouteInput<
  TBody extends {
    readonly instanceId?: string | undefined;
    readonly participantId: string;
  },
> {
  readonly authContext: AuthContext | null;
  readonly hub: SubscriptionHub;
  readonly mutate: (input: {
    readonly body: TBody;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) => Effect.Effect<RestTaskMutationResult, unknown>;
  readonly rejectedMessage: string;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly route: HttpRouteSpec;
  readonly schema: z.ZodType<TBody>;
  readonly url: URL;
}

interface TaskInputValidationResult {
  readonly issues: readonly string[];
  readonly valid: boolean;
}

export const taskHttpRoutes = {
  approval: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.approval",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/approval$/u,
  }),
  cancel: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.cancel",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/cancel$/u,
  }),
  claim: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.claim",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/claim$/u,
  }),
  claimRefresh: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.claim.refresh",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/claim\/refresh$/u,
  }),
  complete: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.complete",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/complete$/u,
  }),
  create: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "task.create",
    pattern: /^\/sessions\/([^/]+)\/tasks$/u,
  }),
  fail: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.fail",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/fail$/u,
  }),
  list: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "task.list",
    pattern: /^\/sessions\/([^/]+)\/tasks$/u,
  }),
  read: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "task.read",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)$/u,
  }),
  release: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "task.release",
    pattern: /^\/sessions\/([^/]+)\/tasks\/([^/]+)\/release$/u,
  }),
  supersedeScheduled: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "task.scheduled.supersede",
    pattern: /^\/sessions\/([^/]+)\/scheduled-runs\/supersede$/u,
  }),
} as const;

/**
 * Operator backlog-reconciliation body for one atomic scheduled-run supersession.
 * The actor recorded on supersession events is taken from the authenticated
 * operator identity, never from the request body, so a relabeled body cannot
 * forge the operator on record.
 */
const supersedeScheduledRunsSchema = z.object({
  candidateTaskIds: z.array(z.string().min(1)).optional(),
  kind: z.string().min(1),
  reason: z.record(z.string(), z.unknown()).optional(),
  schedule: scheduledTaskIdentitySchema,
});

/** Routes task REST requests and owns task-specific HTTP response mapping. */
export function handleTaskHttpRoute(
  input: TaskHttpRouteHandlerInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const { hub, request, response, service, url } = input;
    const tasksListMatch = matchHttpRoute(taskHttpRoutes.list, request.method, url.pathname);
    if (tasksListMatch?.[1]) {
      const sessionId = routeMatchParam(tasksListMatch, 1);
      if (!authorizeRoute(input, "read", sessionId)) {
        return true;
      }
      const taskStatus = parseTaskListStatus(url.searchParams.get("status"));
      if (!taskStatus) {
        sendJson(response, 400, { error: "Invalid task status filter" });
        return true;
      }
      const tasks = yield* service.listTasks(sessionId, taskStatus);
      sendJson(response, 200, { tasks });
      return true;
    }

    const tasksCreateMatch = matchHttpRoute(taskHttpRoutes.create, request.method, url.pathname);
    if (tasksCreateMatch?.[1]) {
      const sessionId = routeMatchParam(tasksCreateMatch, 1);
      if (!authorizeRoute(input, "task-mutate", sessionId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, createTaskSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: taskHttpRoutes.create.name,
      });
      if (body.requireContract === true) {
        const contract = yield* service.findParticipantTaskContract({
          sessionId,
          taskKind: body.kind,
        });
        if (!contract) {
          sendJson(response, 409, {
            error: "No active participant advertises this task contract",
            taskKind: body.kind,
          });
          return true;
        }
        const validation = validateTaskInputAgainstContract(body.input ?? null, contract);
        if (!validation.valid) {
          sendJson(response, 400, {
            error: "Task input does not match advertised task contract",
            issues: validation.issues,
            taskKind: body.kind,
          });
          return true;
        }
      }
      if (body.schedule !== undefined) {
        return yield* handleScheduledTaskCreate({
          body: { ...body, schedule: body.schedule },
          hub,
          response,
          service,
          sessionId,
        });
      }
      const result = yield* service.createTask({
        input: body.input ?? null,
        kind: body.kind,
        objective: body.objective,
        sessionId,
        taskId: body.taskId,
      });
      if (result.status === "conflict") {
        sendJson(response, 409, {
          conflictingFields: result.conflictingFields,
          error: "Task id conflict",
          reason: "task_id_conflict",
          taskId: result.taskId,
        });
        return true;
      }
      broadcastEvents(hub, result.events);
      sendJson(response, result.status === "created" ? 201 : 200, {
        status: result.status,
        task: result.task,
      });
      return true;
    }

    const supersedeScheduledMatch = matchHttpRoute(
      taskHttpRoutes.supersedeScheduled,
      request.method,
      url.pathname,
    );
    if (supersedeScheduledMatch?.[1]) {
      const sessionId = routeMatchParam(supersedeScheduledMatch, 1);
      // Scheduled-run supersession is operator backlog reconciliation, gated at
      // the operator (admin) level. A scheduler/participant task-mutate token is
      // rejected here with the typed role auth error before any store work runs.
      if (!authorizeRoute(input, "scheduled-supersede", sessionId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, supersedeScheduledRunsSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: taskHttpRoutes.supersedeScheduled.name,
      });
      const result = yield* service.supersedeScheduledRuns({
        ...(body.candidateTaskIds !== undefined ? { candidateTaskIds: body.candidateTaskIds } : {}),
        identity: {
          kind: body.kind,
          mailboxScope: {
            accountId: body.schedule.mailboxAccountId,
            provider: body.schedule.mailboxProvider,
          },
          scheduleWindow: {
            algorithmVersion: body.schedule.scheduleAlgorithmVersion,
            endMs: body.schedule.scheduleWindowStart + body.schedule.scheduleIntervalMs,
            intervalMs: body.schedule.scheduleIntervalMs,
            startMs: body.schedule.scheduleWindowStart,
          },
          sessionId,
        },
        // The recorded actor is the authenticated operator identity, never a
        // body-supplied label, so supersession audit records cannot be forged.
        ...(input.authContext ? { participantId: input.authContext.participantId } : {}),
        ...(body.reason !== undefined ? { reason: body.reason } : {}),
      });
      sendScheduledSupersessionResult(response, hub, result);
      return true;
    }

    const approvalMatch = matchHttpRoute(taskHttpRoutes.approval, request.method, url.pathname);
    if (approvalMatch?.[1] && approvalMatch[2]) {
      const sessionId = routeMatchParam(approvalMatch, 1);
      if (!authorizeRoute(input, "task-mutate", sessionId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, recordTaskApprovalSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: taskHttpRoutes.approval.name,
      });
      if (!authorizeParticipant(input, body.participantId)) {
        return true;
      }
      const participantId = effectiveParticipantId(input.authContext, body.participantId);
      const result = yield* service.recordTaskApprovalOverRest({
        ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
        decision: body.decision,
        instanceId: body.instanceId ?? participantId,
        participantId,
        reason: body.reason,
        sessionId,
        taskId: routeMatchParam(approvalMatch, 2),
      });
      sendRestTaskApprovalResult(response, hub, result);
      return true;
    }

    const taskReadMatch = matchHttpRoute(taskHttpRoutes.read, request.method, url.pathname);
    if (taskReadMatch?.[1] && taskReadMatch[2]) {
      const sessionId = routeMatchParam(taskReadMatch, 1);
      if (!authorizeRoute(input, "read", sessionId)) {
        return true;
      }
      const task = yield* service.getTask(sessionId, routeMatchParam(taskReadMatch, 2));
      if (!task) {
        sendJson(response, 404, { error: "Task not found" });
        return true;
      }
      if (parseIncludeContract(url.searchParams.get("include"))) {
        const contract = yield* service.findParticipantTaskContract({
          sessionId,
          taskKind: task.kind,
        });
        sendJson(response, 200, { contract, task });
        return true;
      }
      sendJson(response, 200, { task });
      return true;
    }

    const mutationHandlers = [
      () =>
        handleRestTaskMutationRoute({
          hub,
          authContext: input.authContext,
          mutate: ({ body, instanceId, participantId, sessionId, taskId }) =>
            service.completeTaskOverRest({
              ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
              claimId: body.claimId,
              instanceId,
              participantId,
              result: body.result,
              sessionId,
              taskId,
            }),
          rejectedMessage: "Task is not claimed by this participant or is already terminal",
          request,
          resourceLimits: input.resourceLimits,
          response,
          route: taskHttpRoutes.complete,
          schema: completeTaskSchema,
          url,
        }),
      () =>
        handleRestTaskMutationRoute({
          hub,
          authContext: input.authContext,
          mutate: ({ body, instanceId, participantId, sessionId, taskId }) =>
            service.failTaskOverRest({
              ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
              claimId: body.claimId,
              failure: body.failure,
              instanceId,
              participantId,
              sessionId,
              taskId,
            }),
          rejectedMessage: "Task is not claimed by this participant or is already terminal",
          request,
          resourceLimits: input.resourceLimits,
          response,
          route: taskHttpRoutes.fail,
          schema: failTaskSchema,
          url,
        }),
      () =>
        handleRestTaskMutationRoute({
          hub,
          authContext: input.authContext,
          mutate: ({ body, instanceId, participantId, sessionId, taskId }) =>
            service.releaseTaskOverRest({
              ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
              claimId: body.claimId,
              instanceId,
              participantId,
              sessionId,
              taskId,
            }),
          rejectedMessage: "Task is not claimed by this participant or is already terminal",
          request,
          resourceLimits: input.resourceLimits,
          response,
          route: taskHttpRoutes.release,
          schema: releaseTaskSchema,
          url,
        }),
      () =>
        handleRestTaskMutationRoute({
          hub,
          authContext: input.authContext,
          mutate: ({ body, instanceId, participantId, sessionId, taskId }) =>
            service.claimTaskOverRest({
              ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
              instanceId,
              participantId,
              sessionId,
              taskId,
            }),
          rejectedMessage: "Task is already claimed or terminal",
          request,
          resourceLimits: input.resourceLimits,
          response,
          route: taskHttpRoutes.claim,
          schema: claimTaskSchema,
          url,
        }),
      () =>
        handleRestTaskMutationRoute({
          hub,
          authContext: input.authContext,
          mutate: ({ body, instanceId, participantId, sessionId, taskId }) =>
            service.cancelTaskOverRest({
              ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
              instanceId,
              participantId,
              reason: body.reason,
              sessionId,
              taskId,
            }),
          rejectedMessage: "Task is already terminal",
          request,
          resourceLimits: input.resourceLimits,
          response,
          route: taskHttpRoutes.cancel,
          schema: cancelTaskSchema,
          url,
        }),
    ] as const;
    for (const handleMutation of mutationHandlers) {
      if (yield* handleMutation()) {
        return true;
      }
    }

    const claimRefreshMatch = matchHttpRoute(
      taskHttpRoutes.claimRefresh,
      request.method,
      url.pathname,
    );
    if (claimRefreshMatch?.[1] && claimRefreshMatch[2]) {
      const sessionId = routeMatchParam(claimRefreshMatch, 1);
      if (!authorizeRoute(input, "task-mutate", sessionId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, refreshTaskClaimSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: taskHttpRoutes.claimRefresh.name,
      });
      if (!authorizeParticipant(input, body.participantId)) {
        return true;
      }
      const participantId = effectiveParticipantId(input.authContext, body.participantId);
      const result = yield* service.refreshTaskClaimOverRest({
        ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
        claimId: body.claimId,
        instanceId: body.instanceId ?? participantId,
        participantId,
        sessionId,
        taskId: routeMatchParam(claimRefreshMatch, 2),
      });
      sendRestTaskClaimRefreshResult(response, result);
      return true;
    }

    return false;
  }).pipe(
    Effect.catchIf(
      (error) => error instanceof ZodError,
      (error) =>
        Effect.sync(() => {
          sendJson(input.response, 400, {
            error: "Invalid request",
            issues: error.issues,
          });
          return true;
        }),
    ),
  );
}

/**
 * Routes a create request that carries a schedule identity through the
 * service-owned atomic ensure-scheduled-run path. The deterministic task id is
 * derived server-side from the schedule identity, so a scheduled create never
 * lands on a random or omitted id and always participates in atomic supersession
 * of older windows. A client-supplied task id that does not equal the derived
 * identity is rejected with a typed 409.
 */
function handleScheduledTaskCreate(
  input: ScheduledTaskCreateInput,
): Effect.Effect<boolean, unknown> {
  const { body, hub, response, service, sessionId } = input;
  return service
    .ensureScheduledRun({
      ...(body.taskId !== undefined ? { expectedTaskId: body.taskId } : {}),
      identity: {
        kind: body.kind,
        mailboxScope: {
          accountId: body.schedule.mailboxAccountId,
          provider: body.schedule.mailboxProvider,
        },
        scheduleWindow: {
          algorithmVersion: body.schedule.scheduleAlgorithmVersion,
          endMs: body.schedule.scheduleWindowStart + body.schedule.scheduleIntervalMs,
          intervalMs: body.schedule.scheduleIntervalMs,
          startMs: body.schedule.scheduleWindowStart,
        },
        sessionId,
      },
      input: body.input ?? null,
      objective: body.objective,
    })
    .pipe(
      Effect.map((result) => {
        broadcastEvents(hub, result.events);
        sendJson(response, result.created ? 201 : 200, {
          status: result.created ? "created" : "replayed",
          task: result.current,
        });
        return true;
      }),
      Effect.catchAll((error) => {
        const occupant = scheduledRunIdentityConflictFromUnknown(error);
        if (occupant) {
          return Effect.sync(() => {
            sendJson(response, 409, {
              conflictingFields: occupant.conflictingFields,
              error: "Derived scheduled-run id is occupied by a task with a different identity",
              reason: "scheduled_run_identity_conflict",
              taskId: occupant.taskId,
            });
            return true;
          });
        }
        const mismatch = scheduledTaskIdentityMismatchFromUnknown(error);
        if (!mismatch) {
          return Effect.fail(error);
        }
        return Effect.sync(() => {
          sendJson(response, 409, {
            derivedTaskId: mismatch.derivedTaskId,
            error: "Scheduled task id does not match its derived schedule identity",
            reason: "scheduled_task_identity_mismatch",
            suppliedTaskId: mismatch.suppliedTaskId,
          });
          return true;
        });
      }),
    );
}

/** Extracts a scheduled-run occupant conflict from a direct or service-wrapped error. */
function scheduledRunIdentityConflictFromUnknown(
  error: unknown,
): ScheduledRunIdentityConflictError | null {
  if (error instanceof ScheduledRunIdentityConflictError) {
    return error;
  }
  if (
    error instanceof SessionServicePersistenceError &&
    error.cause instanceof ScheduledRunIdentityConflictError
  ) {
    return error.cause;
  }
  return null;
}

/** Extracts a scheduled-identity mismatch from a direct or service-wrapped error. */
function scheduledTaskIdentityMismatchFromUnknown(
  error: unknown,
): ScheduledTaskIdentityMismatchError | null {
  if (error instanceof ScheduledTaskIdentityMismatchError) {
    return error;
  }
  if (
    error instanceof SessionServicePersistenceError &&
    error.cause instanceof ScheduledTaskIdentityMismatchError
  ) {
    return error.cause;
  }
  return null;
}

function handleRestTaskMutationRoute<
  TBody extends {
    readonly instanceId?: string | undefined;
    readonly participantId: string;
  },
>(input: RestTaskMutationRouteInput<TBody>): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const match = matchHttpRoute(input.route, input.request.method, input.url.pathname);
    if (!match?.[1] || !match[2]) {
      return false;
    }
    const sessionId = routeMatchParam(match, 1);
    if (!authorizeTaskMutation(input, sessionId)) {
      return true;
    }
    const body = yield* parseJsonBody(input.request, input.schema, {
      maxBytes: input.resourceLimits.httpMaxBodyBytes,
      routeName: input.route.name,
    });
    const denied = authorizeParticipantIdentity(input.authContext, body.participantId);
    if (denied) {
      sendAuthError(input.response, denied);
      return true;
    }
    const participantId = effectiveParticipantId(input.authContext, body.participantId);
    const result = yield* input.mutate({
      body,
      instanceId: body.instanceId ?? participantId,
      participantId,
      sessionId,
      taskId: routeMatchParam(match, 2),
    });
    sendRestTaskMutationResult(input.response, input.hub, result, input.rejectedMessage);
    return true;
  });
}

/** Applies route-level authorization for task REST resources. */
function authorizeRoute(
  input: TaskHttpRouteHandlerInput,
  action: Parameters<typeof authorize>[0]["action"],
  sessionId: string,
): boolean {
  const denied = authorize({ action, context: input.authContext, sessionId });
  if (!denied) {
    return true;
  }
  sendAuthError(input.response, denied);
  return false;
}

/** Applies task-mutation authorization from a generic mutation route helper. */
function authorizeTaskMutation<
  TBody extends {
    readonly instanceId?: string | undefined;
    readonly participantId: string;
  },
>(input: RestTaskMutationRouteInput<TBody>, sessionId: string): boolean {
  const denied = authorize({ action: "task-mutate", context: input.authContext, sessionId });
  if (!denied) {
    return true;
  }
  sendAuthError(input.response, denied);
  return false;
}

/** Applies authenticated participant identity binding for task mutations. */
function authorizeParticipant(input: TaskHttpRouteHandlerInput, participantId: string): boolean {
  const denied = authorizeParticipantIdentity(input.authContext, participantId);
  if (!denied) {
    return true;
  }
  sendAuthError(input.response, denied);
  return false;
}

function validateTaskInputAgainstContract(
  input: Record<string, unknown> | null,
  contract: {
    readonly inputJsonSchema?: Record<string, unknown>;
    readonly taskKind: string;
  },
): TaskInputValidationResult {
  if (!contract.inputJsonSchema) {
    return { issues: [], valid: true };
  }
  const issues = validateJsonSchemaSubset(input ?? {}, contract.inputJsonSchema, "input");
  return { issues, valid: issues.length === 0 };
}

function sendRestTaskMutationResult(
  response: ServerResponse,
  hub: SubscriptionHub,
  result: RestTaskMutationResult,
  rejectedMessage: string,
): void {
  if (result.status === "control_conflict") {
    sendControlLeaseConflict(response, result.leaseClaim, "rest");
    return;
  }
  if (result.status === "control_epoch_required") {
    sendControlEpochRequired(response);
    return;
  }
  if (result.status === "control_epoch_stale") {
    sendControlEpochStale(response, result.currentEpoch);
    return;
  }
  if (result.status === "rejected") {
    sendJson(response, 409, { error: rejectedMessage });
    return;
  }
  broadcastEvents(hub, result.events);
  sendJson(response, 200, { task: result.task });
}

function sendScheduledSupersessionResult(
  response: ServerResponse,
  hub: SubscriptionHub,
  result: ScheduledSupersessionResult,
): void {
  broadcastEvents(hub, result.events);
  sendJson(response, 200, {
    refusals: result.refusals,
    status: result.status,
    supersededTasks: result.supersededTasks,
  });
}

function sendRestTaskApprovalResult(
  response: ServerResponse,
  hub: SubscriptionHub,
  result: RestTaskApprovalResult,
): void {
  if (result.status === "control_conflict") {
    sendControlLeaseConflict(response, result.leaseClaim, "rest");
    return;
  }
  if (result.status === "control_epoch_required") {
    sendControlEpochRequired(response);
    return;
  }
  if (result.status === "control_epoch_stale") {
    sendControlEpochStale(response, result.currentEpoch);
    return;
  }
  if (result.status === "rejected") {
    sendJson(response, taskApprovalRejectionStatus(result.rejectionReason), {
      error: renderTaskApprovalRejection(result.rejectionReason),
      rejectionReason: result.rejectionReason,
      task: result.task,
    });
    return;
  }
  if (result.status === "ignored") {
    sendJson(response, 200, {
      decision: result.decision,
      existingDecision: result.existingDecision,
      ignoredReason: result.ignoredReason,
      status: result.status,
      task: result.task,
    });
    return;
  }
  broadcastEvents(hub, result.events);
  sendJson(response, 200, {
    decision: result.decision,
    event: result.event,
    status: result.status,
    task: result.task,
  });
}

function sendRestTaskClaimRefreshResult(
  response: ServerResponse,
  result: RestTaskClaimRefreshResult,
): void {
  if (result.status === "control_conflict") {
    sendControlLeaseConflict(response, result.leaseClaim, "rest");
    return;
  }
  if (result.status === "control_epoch_required") {
    sendControlEpochRequired(response);
    return;
  }
  if (result.status === "control_epoch_stale") {
    sendControlEpochStale(response, result.currentEpoch);
    return;
  }
  if (result.status === "rejected") {
    sendJson(response, 409, {
      error: "Task claim is missing, expired, or terminal",
    });
    return;
  }
  sendJson(response, 200, { task: result.task });
}

function taskApprovalRejectionStatus(reason: TaskApprovalRejectionReason): number {
  return reason === "task_not_found" ? 404 : 409;
}

function renderTaskApprovalRejection(reason: TaskApprovalRejectionReason): string {
  switch (reason) {
    case "invalid_approval_plan":
      return "Task does not contain an actionable approval dry-run plan";
    case "task_not_completed":
      return "Task must be completed before it can be approved or rejected";
    case "task_not_found":
      return "Task not found";
    case "unsupported_task_kind":
      return "This task kind cannot be approved or rejected";
    default:
      return "Task approval was rejected";
  }
}

function routeMatchParam(match: RegExpMatchArray, index: number): string {
  const value = match[index];
  if (value === undefined || value.length === 0) {
    throw new Error(`Missing route parameter at index ${index}`);
  }
  return value;
}

function parseTaskListStatus(status: string | null): TaskListStatus | null {
  if (status === null || status === "" || status === "active") {
    return "active";
  }
  if (status === "all" || status === "terminal") {
    return status;
  }
  return null;
}

function parseIncludeContract(value: string | null): boolean {
  if (value === null) {
    return false;
  }
  return value
    .split(",")
    .map((item) => item.trim())
    .includes("contract");
}
