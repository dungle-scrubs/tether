import {
  RestParticipantControlClient,
  type RestParticipantControlContext,
} from "@dungle-scrubs/tether-client";
import {
  currentScheduledTaskIdentityVersion,
  deriveScheduledTaskId,
} from "@dungle-scrubs/tether-protocol";

import { clientBridgeRoutes } from "./routes.js";
import {
  taskApprovalResponseSchema,
  taskInspectionResponseSchema,
  taskResponseSchema,
  tasksResponseSchema,
} from "./schemas.js";
import {
  ClientBridgeRequestError,
  type ClientBridgeTransport,
  createClientBridgeTransport,
} from "./transport.js";
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
  private readonly control: RestParticipantControlClient | null;
  private readonly transport: ClientBridgeTransport;

  /** Creates a task client for one Tether service URL. */
  constructor(config: ClientBridgeTaskClientConfig, options: ClientBridgeTaskClientOptions = {}) {
    this.transport = createClientBridgeTransport({
      ...(config.authToken === undefined ? {} : { authToken: config.authToken }),
      ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
      serviceUrl: config.serviceUrl,
    });
    this.control =
      options.controlClient ??
      (config.control
        ? new RestParticipantControlClient(
            {
              ...(config.authToken === undefined ? {} : { authToken: config.authToken }),
              ...config.control,
              serviceUrl: config.serviceUrl,
            },
            {
              ...(options.fetch === undefined ? {} : { fetch: options.fetch }),
            },
          )
        : null);
  }

  /** Returns inspectable runtime state for task REST operations. */
  debugInfo(): ClientBridgeTaskClientDebugInfo {
    return {
      ...this.transport.debugInfo(),
      ...(this.control ? { control: this.control.debugInfo() } : {}),
    };
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
   * Schedule Window. The task id is derived from session, kind, scope key,
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
      identityVersion: currentScheduledTaskIdentityVersion,
      kind: input.kind,
      scheduleWindow: input.scheduleWindow,
      scopeKey: input.scopeKey,
      sessionId,
    });
    const body = await this.requestTaskResponse({
      body: {
        ...(input.input !== undefined ? { input: input.input } : {}),
        kind: input.kind,
        objective: input.objective,
        schedule: {
          scheduleAlgorithmVersion: input.scheduleWindow.algorithmVersion,
          scheduleIntervalMs: input.scheduleWindow.intervalMs,
          scheduleWindowStart: input.scheduleWindow.startMs,
          scopeKey: input.scopeKey,
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
    const context = await this.controlContext(sessionId);
    const body = await this.controlledTaskRequest(context, {
      body: {
        ...(input.reason === undefined ? {} : { reason: input.reason }),
      },
      path: clientBridgeRoutes.taskCancel(sessionId, input.taskId),
    });
    return body.task;
  }

  /** Records approval intent for one task without mutating task-owned state. */
  async recordTaskApproval(
    sessionId: string,
    input: ClientBridgeRecordTaskApprovalInput,
  ): Promise<ClientBridgeTaskApprovalRecord> {
    const context = await this.controlContext(sessionId);
    let body: Awaited<ReturnType<ClientBridgeTaskClient["requestTaskApprovalResponse"]>>;
    try {
      body = await this.requestTaskApprovalResponse({
        body: {
          controlEpoch: context.controlEpoch,
          decision: input.decision,
          instanceId: context.instanceId,
          participantId: context.participantId,
          ...(input.reason === undefined ? {} : { reason: input.reason }),
          ...(input.target === undefined ? {} : { target: input.target }),
        },
        path: clientBridgeRoutes.taskApproval(sessionId, input.taskId),
      });
    } catch (error) {
      this.invalidateFailedContext(context, error);
      throw error;
    }
    return body.status === "recorded"
      ? {
          approval: body.approval,
          decision: body.decision,
          event: body.event,
          eventId: body.event.eventId,
          status: body.status,
          task: body.task,
        }
      : {
          approval: body.approval,
          decision: body.decision,
          existingDecision: body.existingDecision,
          ignoredReason: body.ignoredReason,
          status: body.status,
          task: body.task,
        };
  }

  /** Releases every active REST participant context exactly once. */
  async shutdown(): Promise<void> {
    await this.control?.stop();
  }

  /** Returns the configured participant lifecycle context for one session. */
  private controlContext(sessionId: string): Promise<RestParticipantControlContext> {
    if (!this.control) {
      throw new Error("ClientBridgeTaskClient control configuration is required");
    }
    return this.control.context(sessionId);
  }

  /** Sends one fenced task mutation without retrying it. */
  private async controlledTaskRequest(
    context: RestParticipantControlContext,
    input: {
      readonly body: Record<string, unknown>;
      readonly path: string;
    },
  ) {
    try {
      return await this.requestTaskResponse({
        body: {
          ...input.body,
          controlEpoch: context.controlEpoch,
          instanceId: context.instanceId,
          participantId: context.participantId,
        },
        method: "POST",
        path: input.path,
      });
    } catch (error) {
      this.invalidateFailedContext(context, error);
      throw error;
    }
  }

  /** Invalidates only the matching generation after stale or uncertain protected outcomes. */
  private invalidateFailedContext(context: RestParticipantControlContext, error: unknown): void {
    if (!this.control || !(error instanceof ClientBridgeRequestError)) {
      return;
    }
    const serverCode = error.details.serverCode;
    const status = error.details.status;
    const uncertain =
      error.code === "NETWORK_ERROR" ||
      error.code === "INVALID_RESPONSE" ||
      (error.code === "HTTP_ERROR" && typeof status === "number" && status >= 500);
    if (serverCode === "CONTROL_CONFLICT" || serverCode === "CONTROL_EPOCH_STALE" || uncertain) {
      this.control.invalidate(context);
    }
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
