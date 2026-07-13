import { deriveScheduledTaskId } from "@dungle-scrubs/tether-protocol";

import { clientBridgeRoutes } from "./routes.js";
import {
  taskApprovalResponseSchema,
  taskInspectionResponseSchema,
  taskResponseSchema,
  tasksResponseSchema,
} from "./schemas.js";
import { type ClientBridgeTransport, createClientBridgeTransport } from "./transport.js";
import type {
  ClientBridgeCancelTaskInput,
  ClientBridgeCreateScheduledTaskInput,
  ClientBridgeCreateTaskInput,
  ClientBridgeRecordTaskApprovalInput,
  ClientBridgeTaskApprovalRecord,
  ClientBridgeTaskClientConfig,
  ClientBridgeTaskClientDebugInfo,
  ClientBridgeTaskClientOptions,
  ClientBridgeTaskInspection,
  ClientBridgeTaskListStatus,
  ClientBridgeTaskRecord,
} from "./types.js";

/**
 * Performs client-facing task operations through Tether's REST API. External
 * clients use this instead of carrying local copies of task endpoints and
 * response validation.
 */
export class ClientBridgeTaskClient {
  private readonly transport: ClientBridgeTransport;

  /** Creates a task client for one Tether service URL. */
  constructor(config: ClientBridgeTaskClientConfig, options: ClientBridgeTaskClientOptions = {}) {
    this.transport = createClientBridgeTransport({
      ...(config.authToken === undefined ? {} : { authToken: config.authToken }),
      ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
      serviceUrl: config.serviceUrl,
    });
  }

  /** Returns inspectable runtime state for task REST operations. */
  debugInfo(): ClientBridgeTaskClientDebugInfo {
    return this.transport.debugInfo();
  }

  /** Creates one task in a durable Tether session. */
  async createTask(
    sessionId: string,
    input: ClientBridgeCreateTaskInput,
  ): Promise<ClientBridgeTaskRecord> {
    const body = await this.requestTaskResponse({
      body: {
        ...(input.input !== undefined ? { input: input.input } : {}),
        kind: input.kind,
        objective: input.objective,
        ...(input.requireContract !== undefined ? { requireContract: input.requireContract } : {}),
        ...(input.taskId !== undefined ? { taskId: input.taskId } : {}),
      },
      method: "POST",
      path: clientBridgeRoutes.sessionTasks(sessionId),
    });
    return body.task;
  }

  /**
   * Ensures the one deterministic scheduled maintenance run for the current
   * Schedule Window. The task id is derived from session, kind, Mailbox Scope,
   * interval, algorithm version, and window start, so a repeated tick reuses the
   * existing task through Tether's create idempotency seam instead of a
   * read-then-create race. Missed windows are not backfilled; only the supplied
   * current window is ensured.
   */
  async createScheduledTask(
    sessionId: string,
    input: ClientBridgeCreateScheduledTaskInput,
  ): Promise<ClientBridgeTaskRecord> {
    const taskId = deriveScheduledTaskId({
      kind: input.kind,
      mailboxScope: input.mailboxScope,
      scheduleWindow: input.scheduleWindow,
      sessionId,
    });
    const body = await this.requestTaskResponse({
      body: {
        ...(input.input !== undefined ? { input: input.input } : {}),
        kind: input.kind,
        objective: input.objective,
        schedule: {
          mailboxAccountId: input.mailboxScope.accountId,
          mailboxProvider: input.mailboxScope.provider,
          scheduleAlgorithmVersion: input.scheduleWindow.algorithmVersion,
          scheduleIntervalMs: input.scheduleWindow.intervalMs,
          scheduleWindowStart: input.scheduleWindow.startMs,
        },
        taskId,
      },
      method: "POST",
      path: clientBridgeRoutes.sessionTasks(sessionId),
    });
    return body.task;
  }

  /** Lists tasks in a durable Tether session. */
  async listTasks(
    sessionId: string,
    status: ClientBridgeTaskListStatus = "active",
  ): Promise<readonly ClientBridgeTaskRecord[]> {
    const body = await this.requestTasksResponse(sessionId, status);
    return body.tasks;
  }

  /** Reads one task in a durable Tether session. */
  async getTask(sessionId: string, taskId: string): Promise<ClientBridgeTaskRecord> {
    const body = await this.requestTaskResponse({
      body: null,
      method: "GET",
      path: clientBridgeRoutes.task(sessionId, taskId),
    });
    return body.task;
  }

  /** Reads one task with the currently advertised matching participant contract. */
  async getTaskInspection(sessionId: string, taskId: string): Promise<ClientBridgeTaskInspection> {
    const body = await this.requestTaskInspectionResponse(sessionId, taskId);
    return { contract: body.contract ?? null, task: body.task };
  }

  /** Cancels one task in a durable Tether session. */
  async cancelTask(
    sessionId: string,
    input: ClientBridgeCancelTaskInput,
  ): Promise<ClientBridgeTaskRecord> {
    const body = await this.requestTaskResponse({
      body: {
        instanceId: input.instanceId,
        participantId: input.participantId,
        ...(input.reason === undefined ? {} : { reason: input.reason }),
      },
      method: "POST",
      path: clientBridgeRoutes.taskCancel(sessionId, input.taskId),
    });
    return body.task;
  }

  /** Records approval intent for one task without mutating task or mailbox state. */
  async recordTaskApproval(
    sessionId: string,
    input: ClientBridgeRecordTaskApprovalInput,
  ): Promise<ClientBridgeTaskApprovalRecord> {
    const body = await this.requestTaskApprovalResponse({
      body: {
        decision: input.decision,
        instanceId: input.instanceId,
        participantId: input.participantId,
        ...(input.reason === undefined ? {} : { reason: input.reason }),
      },
      path: clientBridgeRoutes.taskApproval(sessionId, input.taskId),
    });
    return body.status === "recorded"
      ? {
          decision: body.decision,
          event: body.event,
          eventId: body.event.eventId,
          status: body.status,
          task: body.task,
        }
      : {
          decision: body.decision,
          existingDecision: body.existingDecision,
          ignoredReason: body.ignoredReason,
          status: body.status,
          task: body.task,
        };
  }

  /** Sends a task endpoint request that returns the standard `{ task }` envelope. */
  private requestTaskResponse(input: {
    readonly body: Record<string, unknown> | null;
    readonly method: "GET" | "POST";
    readonly path: string;
  }) {
    return this.transport.requestJson({
      ...input,
      schema: taskResponseSchema,
    });
  }

  /** Sends a task approval endpoint request with the standard approval envelope. */
  private requestTaskApprovalResponse(input: {
    readonly body: Record<string, unknown>;
    readonly path: string;
  }) {
    return this.transport.requestJson({
      ...input,
      method: "POST",
      schema: taskApprovalResponseSchema,
    });
  }

  /** Sends a task inspection endpoint request with the standard inspection envelope. */
  private requestTaskInspectionResponse(sessionId: string, taskId: string) {
    return this.transport.requestJson({
      body: null,
      method: "GET",
      path: clientBridgeRoutes.taskInspection(sessionId, taskId),
      schema: taskInspectionResponseSchema,
    });
  }

  /** Sends a task-list endpoint request with the standard task-list envelope. */
  private requestTasksResponse(sessionId: string, status: ClientBridgeTaskListStatus) {
    return this.transport.requestJson({
      body: null,
      method: "GET",
      path: clientBridgeRoutes.sessionTasksWithStatus(sessionId, status),
      schema: tasksResponseSchema,
    });
  }
}
