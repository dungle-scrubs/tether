/**
 * REST task-route client for external participants.
 *
 * Complements `RestParticipantControlClient`, which owns the lease
 * lifecycle: this module covers claimable discovery, fenced task
 * mutations, task reads, event append and list, and approval recording
 * over plain HTTP. It intentionally holds no connection and no lease
 * state, and it never retries a protected mutation: a transport failure
 * surfaces as a typed error so the caller decides what it means.
 *
 * Import from `@dungle-scrubs/tether-client/rest` to stay free of the
 * WebSocket runtime and its dependencies.
 */

import {
  eventListResponseSchema,
  taskRecordSchema,
  type EventListResponse,
  type TaskRecord,
} from "@dungle-scrubs/tether-protocol";
import { z } from "zod";

/** Claim responses wrap the record: { status: "claimed", task: {...} } or null. */
const claimResponseSchema = z.object({
  status: z.string(),
  task: taskRecordSchema.nullable(),
});

import { resolveServiceAuthToken } from "./auth-token.js";
import {
  ModuleObservability,
  type ModuleObservabilityOptions,
  readModuleObservabilityOptions,
} from "./observability.js";

/** Fetch-compatible function used by the REST task client. */
export type RestParticipantTaskFetch = (input: URL, init: RequestInit) => Promise<Response>;

/** Static participant identity and service configuration. */
export interface RestParticipantTaskClientConfig {
  readonly authToken?: string | null;
  /** Installed instance id; attached to fenced mutations when present. */
  readonly instanceId?: string;
  readonly participantId: string;
  readonly serviceUrl: string;
}

/** Injectable runtime dependencies. */
export interface RestParticipantTaskClientOptions {
  readonly fetch?: RestParticipantTaskFetch;
  readonly observability?: ModuleObservabilityOptions;
}

/** Stable failure classifications surfaced to callers. */
export type RestParticipantTaskErrorCode =
  | "AUTHENTICATION"
  | "CLAIM_CONFLICT"
  | "INVALID_RESPONSE"
  | "NOT_FOUND"
  | "PERSISTENCE"
  | "TRANSPORT";

/** Typed, operation-scoped REST task-route failure. */
export class RestParticipantTaskError extends Error {
  readonly code: RestParticipantTaskErrorCode;
  readonly details: {
    readonly operation: string;
    readonly status: number | null;
  };

  constructor(input: {
    readonly code: RestParticipantTaskErrorCode;
    readonly operation: string;
    readonly status?: number | null;
  }) {
    super(`REST participant task ${input.operation} failed: ${input.code}`);
    this.code = input.code;
    this.details = {
      operation: input.operation,
      status: input.status ?? null,
    };
    this.name = "RestParticipantTaskError";
  }
}

/** Options shared by every fenced mutation. */
export interface FencedTaskCall {
  readonly claimId: string;
  readonly controlEpoch?: number;
}

/** Options for one approval decision on a task. */
export interface RecordTaskApprovalCall {
  readonly decision: "approved" | "rejected";
  readonly reason?: Record<string, unknown>;
  readonly target?: Record<string, unknown>;
}

/** Options for one participant-originated session event. */
export interface AppendTaskEventCall {
  readonly controlEpoch?: number;
  readonly eventId?: string;
  readonly instanceId?: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly type: string;
}

/** Bounded event-list query. */
export interface ListEventsQuery {
  readonly after?: number;
  readonly limit?: number;
}

type TaskOperation =
  | "append-event"
  | "claim"
  | "complete"
  | "fail"
  | "list-active"
  | "list-events"
  | "read"
  | "record-approval"
  | "refresh-claim"
  | "release";

/** Reusable REST task-route client for one participant identity. */
export class RestParticipantTaskClient {
  readonly #authToken: string | null;
  readonly #fetch: RestParticipantTaskFetch;
  readonly #instanceId: string | undefined;
  readonly #observability: ModuleObservability;
  readonly #participantId: string;
  readonly #serviceUrl: string;

  constructor(
    config: RestParticipantTaskClientConfig,
    options: RestParticipantTaskClientOptions = {},
  ) {
    this.#authToken = resolveServiceAuthToken(config.authToken);
    this.#fetch = options.fetch ?? fetch;
    this.#instanceId = config.instanceId;
    this.#observability = new ModuleObservability(
      options.observability ?? readModuleObservabilityOptions("RestParticipantTaskClient"),
    );
    this.#participantId = config.participantId;
    this.#serviceUrl = config.serviceUrl.replace(/\/$/u, "");
  }

  /**
   * Lists active (claimable) tasks for a session. Rows that fail the
   * protocol schema are dropped, not fatal: one malformed row must not
   * starve the healthy rows behind it (harnesses#32). The caller sees
   * how many rows were dropped.
   */
  async listActiveTasks(sessionId: string): Promise<{
    readonly invalidRowCount: number;
    readonly tasks: readonly TaskRecord[];
  }> {
    const { body } = await this.#request(
      "list-active",
      "GET",
      `/sessions/${encodeURIComponent(sessionId)}/tasks?status=active`,
    );
    const rows = z.object({ tasks: z.array(z.unknown()) }).safeParse(body);
    if (!rows.success) {
      throw this.#error("INVALID_RESPONSE", "list-active");
    }
    const tasks: TaskRecord[] = [];
    let invalidRowCount = 0;
    for (const row of rows.data.tasks) {
      const parsed = taskRecordSchema.safeParse(row);
      if (parsed.success) {
        tasks.push(parsed.data);
      } else {
        invalidRowCount += 1;
      }
    }
    return { invalidRowCount, tasks };
  }

  /** Reads one task; null when the server reports it absent. */
  async readTask(sessionId: string, taskId: string): Promise<TaskRecord | null> {
    try {
      const { body } = await this.#request(
        "read",
        "GET",
        `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}`,
      );
      return parseTask(body, this.#error.bind(this), "read");
    } catch (error) {
      if (error instanceof RestParticipantTaskError && error.code === "NOT_FOUND") {
        return null;
      }
      throw error;
    }
  }

  /**
   * Claims a task. Returns the claimed record, or null when the server
   * reports no claim available. A 409 surfaces as CLAIM_CONFLICT.
   */
  async claimTask(
    sessionId: string,
    taskId: string,
    call: { readonly controlEpoch?: number } = {},
  ): Promise<TaskRecord | null> {
    const { body } = await this.#request(
      "claim",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/claim`,
      this.#fencedBody(call),
    );
    const wrapped = claimResponseSchema.safeParse(body);
    if (wrapped.success) {
      return wrapped.data.task;
    }
    return parseTask(body, this.#error.bind(this), "claim");
  }

  /** Refreshes one claim; returns the refreshed record. */
  async refreshClaim(sessionId: string, taskId: string, call: FencedTaskCall): Promise<TaskRecord> {
    const { body } = await this.#request(
      "refresh-claim",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/claim/refresh`,
      this.#fencedBody(call),
    );
    const parsed = parseTask(body, this.#error.bind(this), "refresh-claim");
    if (parsed === null) {
      throw this.#error("INVALID_RESPONSE", "refresh-claim");
    }
    return parsed;
  }

  /** Completes a claimed task, fenced by its claim id. */
  async completeTask(
    sessionId: string,
    taskId: string,
    call: FencedTaskCall & { readonly result: Record<string, unknown> },
  ): Promise<void> {
    await this.#request(
      "complete",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/complete`,
      {
        claimId: call.claimId,
        result: call.result,
        ...this.#identity(call),
      },
    );
  }

  /** Fails a claimed task, fenced by its claim id. */
  async failTask(
    sessionId: string,
    taskId: string,
    call: FencedTaskCall & { readonly failure: Record<string, unknown> },
  ): Promise<void> {
    await this.#request(
      "fail",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/fail`,
      {
        claimId: call.claimId,
        failure: call.failure,
        ...this.#identity(call),
      },
    );
  }

  /** Releases a claim without completing or failing the task. */
  async releaseTask(sessionId: string, taskId: string, call: FencedTaskCall): Promise<void> {
    await this.#request(
      "release",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/release`,
      this.#fencedBody(call),
    );
  }

  /** Records one approval decision on a task. */
  async recordApproval(
    sessionId: string,
    taskId: string,
    call: RecordTaskApprovalCall & { readonly controlEpoch?: number },
  ): Promise<void> {
    await this.#request(
      "record-approval",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/approval`,
      {
        decision: call.decision,
        ...(call.reason === undefined ? {} : { reason: call.reason }),
        ...(call.target === undefined ? {} : { target: call.target }),
        ...this.#identity(call),
      },
    );
  }

  /** Appends one participant-originated session event. */
  /**
   * Appends one participant-originated session event. The wire body is
   * exactly the caller's fields: events are not fenced, so no identity
   * is injected beyond what the caller passes.
   */
  async appendEvent(sessionId: string, event: AppendTaskEventCall): Promise<void> {
    await this.#request(
      "append-event",
      "POST",
      `/sessions/${encodeURIComponent(sessionId)}/events`,
      {
        ...(event.eventId === undefined ? {} : { eventId: event.eventId }),
        ...(event.instanceId === undefined ? {} : { instanceId: event.instanceId }),
        payload: event.payload,
        producerId: event.producerId,
        type: event.type,
        ...(event.controlEpoch === undefined ? {} : { controlEpoch: event.controlEpoch }),
      },
    );
  }

  /** Lists session events with bounded pagination. */
  async listEvents(sessionId: string, query: ListEventsQuery = {}): Promise<EventListResponse> {
    const params = new URLSearchParams();
    if (query.after !== undefined) {
      params.set("after", String(query.after));
    }
    if (query.limit !== undefined) {
      params.set("limit", String(query.limit));
    }
    const suffix = params.size > 0 ? `?${params.toString()}` : "";
    const { body } = await this.#request(
      "list-events",
      "GET",
      `/sessions/${encodeURIComponent(sessionId)}/events${suffix}`,
    );
    const parsed = eventListResponseSchema.safeParse(body);
    if (!parsed.success) {
      throw this.#error("INVALID_RESPONSE", "list-events");
    }
    return parsed.data;
  }

  #error(code: RestParticipantTaskErrorCode, operation: TaskOperation, status?: number | null) {
    return new RestParticipantTaskError({
      code,
      operation,
      status: status ?? null,
    });
  }

  #fencedBody(call: {
    readonly claimId?: string;
    readonly controlEpoch?: number;
  }): Record<string, unknown> {
    return {
      ...(call.claimId === undefined ? {} : { claimId: call.claimId }),
      ...this.#identity(call),
    };
  }

  #identity(call: { readonly controlEpoch?: number }): Record<string, unknown> {
    return {
      ...(call.controlEpoch === undefined ? {} : { controlEpoch: call.controlEpoch }),
      ...(this.#instanceId === undefined ? {} : { instanceId: this.#instanceId }),
      participantId: this.#participantId,
    };
  }

  async #request(
    operation: TaskOperation,
    method: "GET" | "POST",
    path: string,
    body?: Record<string, unknown>,
  ): Promise<{ readonly body: unknown; readonly status: number }> {
    return this.#observability.traceBoundary(
      operation,
      { operation },
      async () => {
        let response: Response;
        try {
          response = await this.#fetch(new URL(path, `${this.#serviceUrl}/`), {
            ...(body === undefined
              ? {}
              : {
                  body: JSON.stringify(body),
                  headers: { "content-type": "application/json" },
                }),
            headers: this.#authToken ? { authorization: `Bearer ${this.#authToken}` } : {},
            method,
          });
        } catch {
          throw this.#error("TRANSPORT", operation);
        }
        const responseBody = await readJsonBody(response);
        if (!response.ok) {
          throw this.#error(mapTaskServerError(response.status), operation, response.status);
        }
        if (response.status === 204) {
          return { body: null, status: response.status };
        }
        return { body: responseBody, status: response.status };
      },
      () => ({ outcome: "done" }),
    );
  }
}

/** Parses one task-or-null response body with the shared protocol schema. */
function parseTask(
  body: unknown,
  error: (code: RestParticipantTaskErrorCode, operation: TaskOperation) => Error,
  operation: TaskOperation,
): TaskRecord | null {
  if (body === null) {
    return null;
  }
  const parsed = taskRecordSchema.safeParse(body);
  if (!parsed.success) {
    throw error("INVALID_RESPONSE", operation);
  }
  return parsed.data;
}

async function readJsonBody(response: Response): Promise<unknown> {
  let text: string;
  try {
    text = await response.text();
  } catch {
    throw new Error("Response body transport failed");
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

/** Maps public HTTP status to a bounded client failure. */
function mapTaskServerError(status: number): RestParticipantTaskErrorCode {
  if (status === 401 || status === 403) {
    return "AUTHENTICATION";
  }
  if (status === 404) {
    return "NOT_FOUND";
  }
  if (status === 409) {
    return "CLAIM_CONFLICT";
  }
  return status >= 500 ? "PERSISTENCE" : "INVALID_RESPONSE";
}
