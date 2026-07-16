import { describe, expect, it, vi } from "vitest";

import type { ClientBridgeFetch, ClientBridgeTaskRecord } from "../src/index.js";
import {
  ClientBridgeRequestError,
  ClientBridgeTaskClient,
  computeScheduleWindow,
  deriveScheduledTaskId,
} from "../src/index.js";

describe("ClientBridgeTaskClient", () => {
  it("creates tasks through the client-facing task endpoint", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          task: createTaskFixture({
            objective: "handle request",
            sessionId: "sess_1",
            taskId: "task_1",
          }),
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(
      client.createTask("sess_1", {
        input: { source: "external-chat" },
        kind: "generic_request",
        objective: "handle request",
        requireContract: true,
      }),
    ).resolves.toMatchObject({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });

    expect(requests).toEqual([
      {
        body: {
          input: { source: "external-chat" },
          kind: "generic_request",
          objective: "handle request",
          requireContract: true,
        },
        method: "POST",
        path: "/sessions/sess_1/tasks",
      },
    ]);
    expect(client.debugInfo()).toEqual({ requestCount: 1 });
  });

  it("ensures a deterministic scheduled run through the create idempotency seam", async () => {
    const requests: CapturedRequest[] = [];
    const scheduleWindow = computeScheduleWindow(1_700_000_123_456, 3_600_000);
    const mailboxScope = { accountId: "acct_opaque_1", provider: "fastmail" };
    const expectedTaskId = deriveScheduledTaskId({
      kind: "email_organization",
      mailboxScope,
      scheduleWindow,
      sessionId: "sess_1",
    });
    const fetch = createJsonFetch(requests, [
      {
        body: {
          task: createTaskFixture({
            objective: "organize mailbox",
            schedule: { mailboxScope, scheduleWindow },
            sessionId: "sess_1",
            taskId: expectedTaskId,
          }),
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(
      client.createScheduledTask("sess_1", {
        kind: "email_organization",
        mailboxScope,
        objective: "organize mailbox",
        scheduleWindow,
      }),
    ).resolves.toMatchObject({
      schedule: { mailboxScope, scheduleWindow },
      taskId: expectedTaskId,
    });

    expect(requests).toEqual([
      {
        body: {
          kind: "email_organization",
          objective: "organize mailbox",
          schedule: {
            mailboxAccountId: "acct_opaque_1",
            mailboxProvider: "fastmail",
            scheduleAlgorithmVersion: scheduleWindow.algorithmVersion,
            scheduleIntervalMs: scheduleWindow.intervalMs,
            scheduleWindowStart: scheduleWindow.startMs,
          },
          taskId: expectedTaskId,
        },
        method: "POST",
        path: "/sessions/sess_1/tasks",
      },
    ]);
  });

  it("sends configured bearer tokens with task requests", async () => {
    const fetch: ClientBridgeFetch = async (_url, init) => {
      expect(new Headers(init.headers).get("authorization")).toBe("Bearer bridge-token");
      return new Response(
        JSON.stringify({
          task: createTaskFixture({
            objective: "handle request",
            sessionId: "sess_1",
            taskId: "task_1",
          }),
        }),
        {
          headers: { "content-type": "application/json" },
          status: 200,
        },
      );
    };
    const client = new ClientBridgeTaskClient(
      { authToken: "bridge-token", serviceUrl: "http://tether.test" },
      { fetch },
    );

    await expect(
      client.createTask("sess_1", {
        kind: "generic_request",
        objective: "handle request",
      }),
    ).resolves.toMatchObject({ taskId: "task_1" });
  });

  it("lists active tasks through the client-facing task endpoint", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          tasks: [
            createTaskFixture({
              objective: "handle request",
              sessionId: "sess_1",
              taskId: "task_1",
            }),
          ],
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(client.listTasks("sess_1")).resolves.toHaveLength(1);

    expect(requests).toEqual([
      {
        body: null,
        method: "GET",
        path: "/sessions/sess_1/tasks?status=active",
      },
    ]);
  });

  it("passes explicit task list filters through the client-facing task endpoint", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          tasks: [],
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(client.listTasks("sess_1", "terminal")).resolves.toEqual([]);

    expect(requests).toEqual([
      {
        body: null,
        method: "GET",
        path: "/sessions/sess_1/tasks?status=terminal",
      },
    ]);
  });

  it("reads one task through the client-facing task endpoint", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          task: createTaskFixture({
            objective: "handle request",
            sessionId: "sess_1",
            taskId: "task_1",
          }),
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(client.getTask("sess_1", "task_1")).resolves.toMatchObject({
      taskId: "task_1",
    });

    expect(requests).toEqual([
      {
        body: null,
        method: "GET",
        path: "/sessions/sess_1/tasks/task_1",
      },
    ]);
  });

  it("reads one task with advertised contract inspection", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: {
          contract: {
            approval: "required_for_mutation",
            description: "Process a request.",
            displayName: "Generic Agent",
            inputJsonSchema: {
              properties: { query: { type: "string" } },
              required: ["query"],
              type: "object",
            },
            inputSchemaRef: "task-contract:generic_request:v1:input",
            participantId: "part_generic",
            participantRuntimeKind: "generic_agent",
            readOnlyByDefault: true,
            resultJsonSchema: { type: "object" },
            resultSchemaRef: "task-contract:generic_request:v1:result",
            runtimeKind: "generic_agent",
            sessionId: "sess_1",
            taskKind: "generic_request",
            title: "Process request",
            version: "1",
          },
          task: createTaskFixture({
            objective: "handle request",
            sessionId: "sess_1",
            taskId: "task_1",
          }),
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(client.getTaskInspection("sess_1", "task_1")).resolves.toMatchObject({
      contract: {
        inputSchemaRef: "task-contract:generic_request:v1:input",
        taskKind: "generic_request",
      },
      task: {
        taskId: "task_1",
      },
    });

    expect(requests).toEqual([
      {
        body: null,
        method: "GET",
        path: "/sessions/sess_1/tasks/task_1?include=contract",
      },
    ]);
  });

  it("cancels tasks with a bridge control identity and reason", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: createControlAcquisitionFixture(),
        status: 201,
      },
      {
        body: {
          task: createTaskFixture({
            cancelledAt: "2026-01-01T00:00:01.000Z",
            objective: "handle request",
            sessionId: "sess_1",
            taskId: "task_1",
          }),
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), { fetch });

    await expect(
      client.cancelTask("sess_1", {
        reason: { chatId: "123", source: "external-chat" },
        taskId: "task_1",
      }),
    ).resolves.toMatchObject({
      cancelledAt: "2026-01-01T00:00:01.000Z",
      taskId: "task_1",
    });

    expect(requests).toEqual([
      {
        body: {
          acquisitionId: expect.any(String),
          capabilities: {},
          controlChannel: "rest",
          displayName: "part_bridge",
          instanceId: "inst_bridge",
          participantId: "part_bridge",
          runtimeKind: "generic_agent",
        },
        method: "POST",
        path: "/sessions/sess_1/participants",
      },
      {
        body: {
          controlEpoch: 1,
          instanceId: "inst_bridge",
          participantId: "part_bridge",
          reason: { chatId: "123", source: "external-chat" },
        },
        method: "POST",
        path: "/sessions/sess_1/tasks/task_1/cancel",
      },
    ]);
  });

  it("records task approval intent without mutating the task", async () => {
    const requests: CapturedRequest[] = [];
    const task = createTaskFixture({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });
    const fetch = createJsonFetch(requests, [
      {
        body: createControlAcquisitionFixture(),
        status: 201,
      },
      {
        body: {
          decision: "approved",
          event: createSessionEventFixture({
            eventId: "evt_approval",
            sessionId: "sess_1",
            type: "approval.recorded",
          }),
          status: "recorded",
          task,
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), { fetch });

    await expect(
      client.recordTaskApproval("sess_1", {
        decision: "approved",
        reason: { chatId: "123", source: "external-chat" },
        taskId: "task_1",
      }),
    ).resolves.toEqual({
      decision: "approved",
      event: createSessionEventFixture({
        eventId: "evt_approval",
        sessionId: "sess_1",
        type: "approval.recorded",
      }),
      eventId: "evt_approval",
      status: "recorded",
      task,
    });

    expect(requests).toEqual([
      {
        body: {
          acquisitionId: expect.any(String),
          capabilities: {},
          controlChannel: "rest",
          displayName: "part_bridge",
          instanceId: "inst_bridge",
          participantId: "part_bridge",
          runtimeKind: "generic_agent",
        },
        method: "POST",
        path: "/sessions/sess_1/participants",
      },
      {
        body: {
          controlEpoch: 1,
          decision: "approved",
          instanceId: "inst_bridge",
          participantId: "part_bridge",
          reason: { chatId: "123", source: "external-chat" },
        },
        method: "POST",
        path: "/sessions/sess_1/tasks/task_1/approval",
      },
    ]);
  });

  it("returns ignored task approval results without requiring an event", async () => {
    const requests: CapturedRequest[] = [];
    const task = createTaskFixture({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });
    const fetch = createJsonFetch(requests, [
      {
        body: createControlAcquisitionFixture(),
        status: 201,
      },
      {
        body: {
          decision: "rejected",
          existingDecision: "approved",
          ignoredReason: "already_approved",
          status: "ignored",
          task,
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), { fetch });

    await expect(
      client.recordTaskApproval("sess_1", {
        decision: "rejected",
        taskId: "task_1",
      }),
    ).resolves.toEqual({
      decision: "rejected",
      existingDecision: "approved",
      ignoredReason: "already_approved",
      status: "ignored",
      task,
    });
  });

  it("reuses one session acquisition for cancellation and approval, then releases on shutdown", async () => {
    const requests: CapturedRequest[] = [];
    const task = createTaskFixture({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });
    const fetch = createJsonFetch(requests, [
      { body: createControlAcquisitionFixture(), status: 201 },
      { body: { task }, status: 200 },
      {
        body: {
          decision: "approved",
          event: createSessionEventFixture({
            eventId: "evt_approval_reuse",
            sessionId: "sess_1",
            type: "approval.recorded",
          }),
          status: "recorded",
          task,
        },
        status: 200,
      },
      { body: { released: true }, status: 200 },
    ]);
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), { fetch });

    await client.cancelTask("sess_1", { taskId: "task_1" });
    await client.recordTaskApproval("sess_1", {
      decision: "approved",
      taskId: "task_1",
    });
    await client.shutdown();

    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/cancel",
      "/sessions/sess_1/tasks/task_1/approval",
      "/sessions/sess_1/participants/part_bridge/control/release",
    ]);
    expect(requests[1]?.body).toMatchObject({ controlEpoch: 1 });
    expect(requests[2]?.body).toMatchObject({ controlEpoch: 1 });
  });

  it("invalidates the matching context after an uncertain protected mutation", async () => {
    const requests: CapturedRequest[] = [];
    let requestIndex = 0;
    const task = createTaskFixture({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), {
      fetch: async (url, init) => {
        const request = {
          body: typeof init.body === "string" ? JSON.parse(init.body) : null,
          method: init.method ?? "GET",
          path: `${url.pathname}${url.search}`,
        };
        requests.push(request);
        requestIndex += 1;
        if (requestIndex === 1 || requestIndex === 3) {
          return new Response(
            JSON.stringify(
              createControlAcquisitionFixture(
                typeof request.body === "object" &&
                  request.body !== null &&
                  "acquisitionId" in request.body &&
                  typeof request.body.acquisitionId === "string"
                  ? request.body.acquisitionId
                  : "acq_missing",
              ),
            ),
            { status: 201 },
          );
        }
        if (requestIndex === 2) {
          throw new Error("mutation response lost");
        }
        return new Response(
          JSON.stringify({
            decision: "approved",
            event: createSessionEventFixture({
              eventId: "evt_after_uncertain",
              sessionId: "sess_1",
              type: "approval.recorded",
            }),
            status: "recorded",
            task,
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        );
      },
    });

    await expect(client.cancelTask("sess_1", { taskId: "task_1" })).rejects.toMatchObject({
      code: "NETWORK_ERROR",
    });
    await client.recordTaskApproval("sess_1", {
      decision: "approved",
      taskId: "task_1",
    });

    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/cancel",
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/approval",
    ]);
  });

  it("invalidates the matching context when a protected response body is lost", async () => {
    const requests: CapturedRequest[] = [];
    let requestIndex = 0;
    const task = createTaskFixture({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), {
      fetch: async (url, init) => {
        const request: CapturedRequest = {
          body: typeof init.body === "string" ? JSON.parse(init.body) : null,
          method: init.method ?? "GET",
          path: `${url.pathname}${url.search}`,
        };
        requests.push(request);
        requestIndex += 1;
        if (requestIndex === 1 || requestIndex === 3) {
          const acquisitionId =
            typeof request.body === "object" &&
            request.body !== null &&
            "acquisitionId" in request.body &&
            typeof request.body.acquisitionId === "string"
              ? request.body.acquisitionId
              : "acq_missing";
          return new Response(JSON.stringify(createControlAcquisitionFixture(acquisitionId)), {
            headers: { "content-type": "application/json" },
            status: 201,
          });
        }
        if (requestIndex === 2) {
          return new Response(
            new ReadableStream({
              start(controller) {
                controller.error(new Error("response body lost"));
              },
            }),
            {
              headers: { "content-type": "application/json" },
              status: 200,
            },
          );
        }
        return new Response(
          JSON.stringify({
            decision: "approved",
            event: createSessionEventFixture({
              eventId: "evt_after_body_loss",
              sessionId: "sess_1",
              type: "approval.recorded",
            }),
            status: "recorded",
            task,
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        );
      },
    });

    await expect(client.cancelTask("sess_1", { taskId: "task_1" })).rejects.toMatchObject({
      code: "NETWORK_ERROR",
    });
    await client.recordTaskApproval("sess_1", {
      decision: "approved",
      taskId: "task_1",
    });

    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/cancel",
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/approval",
    ]);
  });

  it("invalidates the matching context after a REST control conflict", async () => {
    const requests: CapturedRequest[] = [];
    const task = createTaskFixture({
      objective: "handle request",
      sessionId: "sess_1",
      taskId: "task_1",
    });
    const fetch = createJsonFetch(requests, [
      { body: createControlAcquisitionFixture(), status: 201 },
      { body: { code: "CONTROL_CONFLICT" }, status: 409 },
      { body: createControlAcquisitionFixture(), status: 201 },
      {
        body: {
          decision: "approved",
          event: createSessionEventFixture({
            eventId: "evt_after_conflict",
            sessionId: "sess_1",
            type: "approval.recorded",
          }),
          status: "recorded",
          task,
        },
        status: 200,
      },
    ]);
    const client = new ClientBridgeTaskClient(createControlledTaskClientConfig(), { fetch });

    await expect(client.cancelTask("sess_1", { taskId: "task_1" })).rejects.toMatchObject({
      code: "HTTP_ERROR",
      details: { serverCode: "CONTROL_CONFLICT", status: 409 },
    });
    await client.recordTaskApproval("sess_1", {
      decision: "approved",
      taskId: "task_1",
    });

    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/cancel",
      "/sessions/sess_1/participants",
      "/sessions/sess_1/tasks/task_1/approval",
    ]);
  });

  it("returns typed errors for invalid task responses", async () => {
    const client = new ClientBridgeTaskClient(
      { serviceUrl: "http://tether.test" },
      {
        fetch: async () =>
          new Response(JSON.stringify({ nope: true }), {
            headers: { "content-type": "application/json" },
            status: 200,
          }),
      },
    );

    await expect(client.listTasks("sess_1")).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      name: "ClientBridgeRequestError",
    } satisfies Partial<ClientBridgeRequestError>);
  });

  it("maps malformed scheduled responses without exposing the raw payload", async () => {
    const scheduleWindow = computeScheduleWindow(1_700_000_123_456, 3_600_000);
    const mailboxScope = {
      accountId: "raw_payload_marker",
      provider: "fastmail",
    };
    const task = createTaskFixture({
      objective: "organize mailbox",
      schedule: { mailboxScope, scheduleWindow },
      sessionId: "sess_1",
      taskId: "task_sched_invalid",
    });
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const client = new ClientBridgeTaskClient(
      { serviceUrl: "http://tether.test" },
      {
        fetch: async () =>
          new Response(
            JSON.stringify({
              task: {
                ...task,
                schedule: {
                  mailboxScope,
                  scheduleWindow: {
                    ...scheduleWindow,
                    endMs: scheduleWindow.endMs + 1,
                  },
                },
              },
            }),
            { headers: { "content-type": "application/json" }, status: 200 },
          ),
      },
    );

    let caught: unknown;
    try {
      await client.createScheduledTask("sess_1", {
        kind: "email_organization",
        mailboxScope,
        objective: "organize mailbox",
        scheduleWindow,
      });
    } catch (error) {
      caught = error;
    } finally {
      consoleError.mockRestore();
    }

    expect(caught).toBeInstanceOf(ClientBridgeRequestError);
    if (!(caught instanceof ClientBridgeRequestError)) {
      throw new Error("Expected malformed response validation to throw ClientBridgeRequestError");
    }
    expect(caught.code).toBe("INVALID_RESPONSE");
    expect(caught.details).toEqual({
      method: "POST",
      path: "/sessions/sess_1/tasks",
    });
    expect(caught.message).not.toContain("raw_payload_marker");
    expect(consoleError).not.toHaveBeenCalled();
  });
});

interface CapturedRequest {
  readonly body: unknown;
  readonly method: string;
  readonly path: string;
}

function createControlledTaskClientConfig() {
  return {
    control: {
      instanceId: "inst_bridge",
      participantId: "part_bridge",
      runtimeKind: "generic_agent",
    },
    serviceUrl: "http://tether.test",
  } as const;
}

function createControlAcquisitionFixture(acquisitionId = "acq_bridge") {
  return {
    acquisitionId,
    acquisitionStatus: "claimed",
    controlEpoch: 1,
    leaseExpiresAt: "2026-07-16T12:01:00.000Z",
    participant: { participantId: "part_bridge" },
    registrationStatus: "joined",
    renewAfterMs: 30_000,
  } as const;
}

interface JsonResponse {
  readonly body: unknown;
  readonly status: number;
}

/**
 * Creates a fetch implementation that records requests and returns queued JSON
 * responses.
 */
function createJsonFetch(
  requests: CapturedRequest[],
  responses: readonly JsonResponse[],
): ClientBridgeFetch {
  let index = 0;
  return async (url, init) => {
    const request: CapturedRequest = {
      body: typeof init.body === "string" ? JSON.parse(init.body) : null,
      method: init.method ?? "GET",
      path: `${url.pathname}${url.search}`,
    };
    requests.push(request);
    const response = responses[index];
    index += 1;
    if (!response) {
      throw new Error("No queued response");
    }
    const responseBody =
      typeof response.body === "object" &&
      response.body !== null &&
      "acquisitionStatus" in response.body &&
      typeof request.body === "object" &&
      request.body !== null &&
      "acquisitionId" in request.body
        ? {
            ...response.body,
            acquisitionId: (request.body as Record<string, unknown>).acquisitionId,
          }
        : response.body;
    return new Response(JSON.stringify(responseBody), {
      headers: { "content-type": "application/json" },
      status: response.status,
    });
  };
}

/**
 * Builds a task fixture returned by Tether.
 */
function createTaskFixture(input: {
  readonly cancelledAt?: string | null;
  readonly objective: string;
  readonly schedule?: ClientBridgeTaskRecord["schedule"];
  readonly sessionId: string;
  readonly taskId: string;
}): ClientBridgeTaskRecord {
  return {
    cancelledAt: input.cancelledAt ?? null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: null,
    kind: "generic_request",
    objective: input.objective,
    releasedAt: null,
    releasedBy: null,
    result: null,
    ...(input.schedule !== undefined ? { schedule: input.schedule } : {}),
    sessionId: input.sessionId,
    taskId: input.taskId,
  };
}

/**
 * Builds a durable session event fixture returned by Tether.
 */
function createSessionEventFixture(input: {
  readonly eventId: string;
  readonly sessionId: string;
  readonly type: string;
}): {
  readonly createdAt: string;
  readonly eventId: string;
  readonly payload: Record<string, unknown>;
  readonly producerId: string;
  readonly seq: number;
  readonly sessionId: string;
  readonly type: string;
} {
  return {
    createdAt: "2026-01-01T00:00:00.000Z",
    eventId: input.eventId,
    payload: {},
    producerId: "system",
    seq: 1,
    sessionId: input.sessionId,
    type: input.type,
  };
}
