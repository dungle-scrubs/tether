import { describe, expect, it } from "vitest";

import {
  ClientBridgeTaskClient,
  computeScheduleWindow,
  deriveScheduledTaskId,
  type ClientBridgeFetch,
  type ClientBridgeRequestError,
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
    ).resolves.toMatchObject({ taskId: expectedTaskId });

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
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(
      client.cancelTask("sess_1", {
        instanceId: "inst_bridge",
        participantId: "part_bridge",
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
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(
      client.recordTaskApproval("sess_1", {
        decision: "approved",
        instanceId: "inst_bridge",
        participantId: "part_bridge",
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
    const client = new ClientBridgeTaskClient({ serviceUrl: "http://tether.test" }, { fetch });

    await expect(
      client.recordTaskApproval("sess_1", {
        decision: "rejected",
        instanceId: "inst_bridge",
        participantId: "part_bridge",
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
});

interface CapturedRequest {
  readonly body: unknown;
  readonly method: string;
  readonly path: string;
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
    requests.push({
      body: typeof init.body === "string" ? JSON.parse(init.body) : null,
      method: init.method ?? "GET",
      path: `${url.pathname}${url.search}`,
    });
    const response = responses[index];
    index += 1;
    if (!response) {
      throw new Error("No queued response");
    }
    return new Response(JSON.stringify(response.body), {
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
  readonly sessionId: string;
  readonly taskId: string;
}): {
  readonly cancelledAt: string | null;
  readonly claimExpiredAt: string | null;
  readonly claimExpiredBy: string | null;
  readonly claimExpiresAt: string | null;
  readonly claimedAt: string | null;
  readonly claimedBy: string | null;
  readonly completedAt: string | null;
  readonly createdAt: string;
  readonly failedAt: string | null;
  readonly failure: Record<string, unknown> | null;
  readonly input: Record<string, unknown> | null;
  readonly kind: string;
  readonly objective: string;
  readonly releasedAt: string | null;
  readonly releasedBy: string | null;
  readonly result: Record<string, unknown> | null;
  readonly sessionId: string;
  readonly taskId: string;
} {
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
