import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createReplayRestTaskClient,
  RestParticipantTaskClient,
  RestParticipantTaskError,
  type RestParticipantTaskFetch,
} from "../src/rest-participant-task-client.js";

interface CapturedRequest {
  readonly body: unknown;
  readonly headers: Record<string, string>;
  readonly method: string;
  readonly path: string;
}

const taskRecord = {
  cancelledAt: null,
  claimExpiredAt: null,
  claimExpiredBy: null,
  claimExpiresAt: null,
  claimId: null,
  claimedAt: null,
  claimedBy: null,
  completedAt: null,
  createdAt: "2026-10-01T00:00:00.000Z",
  failedAt: null,
  failure: null,
  input: { contextClass: "local" },
  kind: "media_status",
  objective: "Report status",
  releasedAt: null,
  releasedBy: null,
  result: null,
  sessionId: "sess_1",
  taskId: "task_1",
};

function capture(url: URL, init: RequestInit): CapturedRequest {
  const headers = (init.headers ?? {}) as Record<string, string>;
  return {
    body: init.body === undefined ? null : (JSON.parse(String(init.body)) as unknown),
    headers,
    method: init.method ?? "GET",
    path: `${url.pathname}${url.search}`,
  };
}

function fetchJson(
  status: number,
  body: unknown,
): {
  client: RestParticipantTaskClient;
  requests: CapturedRequest[];
} {
  const requests: CapturedRequest[] = [];
  const fetch: RestParticipantTaskFetch = async (url, init) => {
    requests.push(capture(url, init));
    return new Response(status === 204 ? null : JSON.stringify(body), {
      status,
    });
  };
  const client = new RestParticipantTaskClient(
    {
      authToken: "tok",
      instanceId: "inst_1",
      participantId: "part_1",
      serviceUrl: "http://127.0.0.1:3025",
    },
    { fetch },
  );
  return { client, requests };
}

function posted(requests: CapturedRequest[], path: string): Record<string, unknown> {
  const hit = requests.find((r) => r.path === path);
  if (hit === undefined || hit.body === null) {
    throw new Error(`no body captured for ${path}`);
  }
  return hit.body as Record<string, unknown>;
}

/** Uses only an injected synthetic response with the runtime's internal read policy. */
function boundedTaskClient(
  response: Response,
  input: Partial<Parameters<typeof createReplayRestTaskClient>[1]> = {},
): RestParticipantTaskClient {
  vi.stubGlobal("fetch", async () => response);
  return createReplayRestTaskClient(
    { authToken: null, participantId: "part_1", serviceUrl: "https://fixture.invalid" },
    {
      maxBytes: input.maxBytes ?? 10_000,
      maxActiveTasks: input.maxActiveTasks ?? 10,
      signal: input.signal ?? new AbortController().signal,
      timeoutMs: input.timeoutMs ?? 1_000,
    },
  );
}

describe("RestParticipantTaskClient", () => {
  afterEach(() => vi.unstubAllGlobals());
  it("cancels an error response without reading or parsing its body", async () => {
    let pulls = 0;
    let cancellations = 0;
    const stream = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => {
          pulls += 1;
          controller.enqueue(new Uint8Array(1_000));
        },
        cancel: () => {
          cancellations += 1;
        },
      },
      { highWaterMark: 0 },
    );
    const client = new RestParticipantTaskClient(
      { authToken: null, participantId: "part_1", serviceUrl: "https://fixture.invalid" },
      { fetch: async () => new Response(stream, { status: 503 }) },
    );
    await expect(client.listActiveTasks("sess_1")).rejects.toMatchObject({
      code: "PERSISTENCE",
      details: { status: 503 },
    });
    expect(pulls).toBe(0);
    expect(cancellations).toBe(1);
  });

  it("stores resource failure metadata under its discriminated failure value", () => {
    const error = new RestParticipantTaskError({
      code: "RESOURCE_LIMIT",
      operation: "list-active",
      resourceLimit: { resource: "active_tasks", limit: 2, observed: 3 },
    });
    expect(error.failure).toEqual({
      code: "RESOURCE_LIMIT",
      details: {
        operation: "list-active",
        status: null,
        resourceLimit: { resource: "active_tasks", limit: 2, observed: 3 },
      },
    });
    expect(error.code).toBe("RESOURCE_LIMIT");
    expect(error.details).toBe(error.failure.details);
    if (error.failure.code === "RESOURCE_LIMIT")
      expect(error.failure.details.resourceLimit.observed).toBe(3);
  });

  it("bundles a resource limit into one bounded details object", async () => {
    const failure = boundedTaskClient(new Response(new Uint8Array(80)), {
      maxBytes: 64,
    }).listActiveTasks("sess_1");
    await expect(failure).rejects.toMatchObject({ code: "RESOURCE_LIMIT" });
    await expect(failure).rejects.toHaveProperty("details", {
      operation: "list-active",
      status: null,
      resourceLimit: { resource: "response_bytes", limit: 64, observed: 80 },
    });
  });

  it.each([
    { status: 401, code: "AUTHENTICATION" },
    { status: 503, code: "PERSISTENCE" },
    { status: 501, code: "PERSISTENCE" },
  ])("maps HTTP $status before discarding an oversized error body", async ({ status, code }) => {
    const bytes = new TextEncoder().encode("x".repeat(8_000));
    let offset = 0;
    let pulls = 0;
    let cancellations = 0;
    const stream = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => {
          pulls += 1;
          const chunk = bytes.subarray(offset, offset + 1_000);
          offset += chunk.length;
          if (chunk.length === 0) controller.close();
          else controller.enqueue(chunk);
        },
        cancel: () => {
          cancellations += 1;
        },
      },
      { highWaterMark: 0 },
    );
    const client = boundedTaskClient(new Response(stream, { status }), { maxBytes: 64 });
    await expect(client.listActiveTasks("sess_1")).rejects.toMatchObject({
      code,
      details: { operation: "list-active", status },
    });
    expect(pulls).toBe(0);
    expect(cancellations).toBe(1);
  });

  it("cancels an over-budget body while reading chunks before buffering or parsing it", async () => {
    const bytes = new TextEncoder().encode(JSON.stringify({ tasks: [taskRecord] }));
    let offset = 0;
    let pulls = 0;
    let cancellations = 0;
    const stream = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => {
          pulls += 1;
          const chunk = bytes.subarray(offset, offset + 40);
          offset += chunk.length;
          if (chunk.length === 0) controller.close();
          else controller.enqueue(chunk);
        },
        cancel: () => {
          cancellations += 1;
        },
      },
      { highWaterMark: 0 },
    );
    const client = boundedTaskClient(new Response(stream), { maxBytes: 64 });
    await expect(client.listActiveTasks("sess_1")).rejects.toMatchObject({
      code: "RESOURCE_LIMIT",
      details: {
        operation: "list-active",
        resourceLimit: { resource: "response_bytes", limit: 64, observed: 80 },
      },
    });
    expect(cancellations).toBe(1);
    expect(pulls).toBe(2);
  });

  it("aborts and cancels a stalled response body without waiting for it to finish", async () => {
    const cancellation = new AbortController();
    let pulled = false;
    let cancelled = false;
    let finish: (() => void) | undefined;
    const stream = new ReadableStream<Uint8Array>(
      {
        start: (controller) => {
          finish = () => controller.close();
        },
        pull: () => {
          pulled = true;
        },
        cancel: () => {
          cancelled = true;
        },
      },
      { highWaterMark: 0 },
    );
    const request = boundedTaskClient(new Response(stream), {
      signal: cancellation.signal,
    }).listActiveTasks("sess_1");
    request.catch(() => undefined);
    try {
      for (let i = 0; i < 50 && !pulled; i += 1) await Promise.resolve();
      expect(pulled).toBe(true);
      cancellation.abort();
      for (let i = 0; i < 50 && !cancelled; i += 1) await Promise.resolve();
      expect(cancelled).toBe(true);
      await expect(request).rejects.toMatchObject({
        code: "TRANSPORT",
        details: { operation: "list-active" },
      });
    } finally {
      if (!cancelled) finish?.();
    }
  });

  it("preserves typed transport errors when a response body stream fails", async () => {
    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        controller.error(new Error("synthetic body failure"));
      },
    });
    await expect(
      boundedTaskClient(new Response(stream)).listActiveTasks("sess_1"),
    ).rejects.toMatchObject({
      name: "RestParticipantTaskError",
      code: "TRANSPORT",
      details: { operation: "list-active" },
    });
  });

  it("decodes split UTF-8 chunks without counting characters instead of response bytes", async () => {
    const bytes = new TextEncoder().encode(
      JSON.stringify({ tasks: [{ ...taskRecord, objective: "é🗓" }] }),
    );
    let offset = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull: (controller) => {
        if (offset === bytes.length) controller.close();
        else controller.enqueue(bytes.subarray(offset, ++offset));
      },
    });
    const result = await boundedTaskClient(new Response(stream), {
      maxBytes: bytes.length,
    }).listActiveTasks("sess_1");
    expect(result.tasks[0]?.objective).toBe("é🗓");
    expect(result.invalidRowCount).toBe(0);
  });
  it("checks raw task count before parsing rows while leaving tolerant callers unchanged", async () => {
    const body = { tasks: [taskRecord, { malformed: "synthetic" }] };
    const bounded = boundedTaskClient(Response.json(body), { maxActiveTasks: 1 });
    await expect(bounded.listActiveTasks("sess_1")).rejects.toMatchObject({
      code: "RESOURCE_LIMIT",
      details: { resourceLimit: { resource: "active_tasks", limit: 1, observed: 2 } },
    });
    const legacy = fetchJson(200, body);
    await expect(legacy.client.listActiveTasks("sess_1")).resolves.toMatchObject({
      invalidRowCount: 1,
      tasks: [taskRecord],
    });
  });

  it("claims a task with the fenced identity and parses the wrapped record", async () => {
    const { client, requests } = fetchJson(200, {
      status: "claimed",
      task: taskRecord,
    });

    const claimed = await client.claimTask("sess_1", "task_1", {
      controlEpoch: 3,
    });

    expect(claimed?.taskId).toBe("task_1");
    expect(claimed?.kind).toBe("media_status");
    expect(requests[0]).toMatchObject({
      body: { controlEpoch: 3, instanceId: "inst_1", participantId: "part_1" },
      headers: { authorization: "Bearer tok" },
      method: "POST",
      path: "/sessions/sess_1/tasks/task_1/claim",
    });
  });

  it("returns null when the server reports no claim available", async () => {
    const { client } = fetchJson(200, null);

    await expect(client.claimTask("sess_1", "task_1")).resolves.toBe(null);
  });

  it("maps 409 on claim to CLAIM_CONFLICT", async () => {
    const { client } = fetchJson(409, { code: "TASK_CLAIM_CONFLICT" });

    await expect(client.claimTask("sess_1", "task_1")).rejects.toMatchObject({
      code: "CLAIM_CONFLICT",
    });
  });

  it("creates a task and parses the wrapped record, sending input only when defined", async () => {
    const withInput = fetchJson(201, { status: "created", task: taskRecord });

    const created = await withInput.client.createTask("sess_1", {
      input: { identifier: "kevin@fastmail.com" },
      kind: "rolodex.who_is",
      objective: "resolve sender kevin@fastmail.com",
    });

    expect(created.taskId).toBe("task_1");
    expect(withInput.requests[0]).toMatchObject({
      body: {
        input: { identifier: "kevin@fastmail.com" },
        kind: "rolodex.who_is",
        objective: "resolve sender kevin@fastmail.com",
      },
      headers: { authorization: "Bearer tok" },
      method: "POST",
      path: "/sessions/sess_1/tasks",
    });

    const withoutInput = fetchJson(201, { status: "created", task: taskRecord });
    await withoutInput.client.createTask("sess_1", {
      kind: "media_status",
      objective: "Report status",
    });
    expect(withoutInput.requests[0]?.body).toEqual({
      kind: "media_status",
      objective: "Report status",
    });
  });

  it("creates a task from a bare record when the response is not wrapped", async () => {
    const { client, requests } = fetchJson(200, taskRecord);

    const created = await client.createTask("sess_1", {
      kind: "rolodex.who_is",
      objective: "resolve sender kevin@fastmail.com",
    });

    expect(created.taskId).toBe("task_1");
    expect(requests[0]).toMatchObject({
      method: "POST",
      path: "/sessions/sess_1/tasks",
    });
  });

  it("maps a malformed create response to INVALID_RESPONSE", async () => {
    const { client } = fetchJson(201, { status: "created", task: { kind: "media_status" } });

    await expect(
      client.createTask("sess_1", { kind: "media_status", objective: "Report status" }),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      details: { operation: "create" },
    });
  });

  it("maps an envelope with a null task, and a null body, to INVALID_RESPONSE", async () => {
    const wrapped = fetchJson(201, { status: "created", task: null });
    await expect(
      wrapped.client.createTask("sess_1", { kind: "media_status", objective: "Report status" }),
    ).rejects.toMatchObject({ code: "INVALID_RESPONSE", details: { operation: "create" } });

    const nullBody = fetchJson(201, null);
    await expect(
      nullBody.client.createTask("sess_1", { kind: "media_status", objective: "Report status" }),
    ).rejects.toMatchObject({ code: "INVALID_RESPONSE", details: { operation: "create" } });
  });

  it("completes with the claim id and result, and sends no body on GET", async () => {
    const requests: CapturedRequest[] = [];
    const fetch: RestParticipantTaskFetch = async (url, init) => {
      requests.push(capture(url, init));
      const path = `${url.pathname}${url.search}`;
      if (path.endsWith("/complete")) {
        return new Response(null, { status: 204 });
      }
      return new Response(JSON.stringify({ tasks: [taskRecord] }), {
        status: 200,
      });
    };
    const client = new RestParticipantTaskClient(
      {
        authToken: "tok",
        instanceId: "inst_1",
        participantId: "part_1",
        serviceUrl: "http://127.0.0.1:3025",
      },
      { fetch },
    );

    await client.completeTask("sess_1", "task_1", {
      claimId: "claim_1",
      controlEpoch: 3,
      result: { status: "resolved" },
    });

    expect(requests[0]).toMatchObject({
      body: {
        claimId: "claim_1",
        controlEpoch: 3,
        instanceId: "inst_1",
        participantId: "part_1",
        result: { status: "resolved" },
      },
      method: "POST",
      path: "/sessions/sess_1/tasks/task_1/complete",
    });

    await client.listActiveTasks("sess_1");
    expect(requests[1]).toMatchObject({
      body: null,
      method: "GET",
      path: "/sessions/sess_1/tasks?status=active",
    });
  });

  it("claims a task wrapped in the bare {task} envelope (newer server)", async () => {
    const enveloped = fetchJson(200, { task: taskRecord });
    await expect(enveloped.client.claimTask("sess_1", "task_1")).resolves.toMatchObject({
      taskId: "task_1",
    });
  });

  it("refreshes a claim across wrapped and bare-envelope shapes", async () => {
    const wrapped = fetchJson(200, { status: "claimed", task: taskRecord });
    await expect(
      wrapped.client.refreshClaim("sess_1", "task_1", { claimId: "claim_1" }),
    ).resolves.toMatchObject({ taskId: "task_1" });

    const enveloped = fetchJson(200, { task: taskRecord });
    await expect(
      enveloped.client.refreshClaim("sess_1", "task_1", { claimId: "claim_1" }),
    ).resolves.toMatchObject({ taskId: "task_1" });
  });

  it("reads a task and returns null on 404", async () => {
    const found = fetchJson(200, taskRecord);
    await expect(found.client.readTask("sess_1", "task_1")).resolves.toMatchObject({
      taskId: "task_1",
    });
    expect(found.requests[0]).toMatchObject({
      method: "GET",
      path: "/sessions/sess_1/tasks/task_1",
    });

    const missing = fetchJson(404, { code: "TASK_NOT_FOUND" });
    await expect(missing.client.readTask("sess_1", "task_x")).resolves.toBe(null);
  });

  it("reads a task wrapped in the server's {task} envelope", async () => {
    const wrapped = fetchJson(200, { task: taskRecord });
    await expect(wrapped.client.readTask("sess_1", "task_1")).resolves.toMatchObject({
      taskId: "task_1",
      kind: "media_status",
      sessionId: "sess_1",
    });

    const nullTask = fetchJson(200, { task: null });
    await expect(nullTask.client.readTask("sess_1", "task_null")).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
    });
  });

  it("lists active tasks as records", async () => {
    const { client } = fetchJson(200, { tasks: [taskRecord] });

    const page = await client.listActiveTasks("sess_1");
    expect(page.tasks).toHaveLength(1);
    expect(page.invalidRowCount).toBe(0);
  });

  it("drops malformed rows and reports the count instead of failing the page (#32)", async () => {
    const badRow = {
      claimId: null,
      input: {},
      kind: "media_status",
      status: "active",
    };
    const { client } = fetchJson(200, { tasks: [badRow, taskRecord] });

    const page = await client.listActiveTasks("sess_1");
    expect(page.tasks).toHaveLength(1);
    expect(page.invalidRowCount).toBe(1);
  });

  it("lists events with pagination and parses the protocol response", async () => {
    const { client, requests } = fetchJson(200, {
      events: [
        {
          createdAt: "2026-10-01T00:00:00.000Z",
          eventId: "evt_1",
          payload: { text: "hi" },
          producerId: "media",
          seq: 7,
          sessionId: "sess_1",
          type: "agent.output",
        },
      ],
      pagination: {
        afterSeq: 0,
        hasMore: false,
        limit: 50,
        nextAfterSeq: 7,
        returned: 1,
      },
    });

    const page = await client.listEvents("sess_1", { after: 0, limit: 50 });

    expect(page.pagination.returned).toBe(1);
    expect(requests[0]?.path).toBe("/sessions/sess_1/events?after=0&limit=50");
  });

  it("records an approval decision", async () => {
    const { client, requests } = fetchJson(204, null);

    await client.recordApproval("sess_1", "task_1", {
      decision: "approved",
      reason: { ref: "rm-0001" },
    });

    expect(requests[0]).toMatchObject({
      body: {
        decision: "approved",
        instanceId: "inst_1",
        participantId: "part_1",
        reason: { ref: "rm-0001" },
      },
      method: "POST",
      path: "/sessions/sess_1/tasks/task_1/approval",
    });
  });

  it("appends a participant event", async () => {
    const { client, requests } = fetchJson(204, null);

    await client.appendEvent("sess_1", {
      eventId: "evt_2",
      payload: { v: "proof-shell/chat-v1", text: "done" },
      producerId: "media",
      type: "agent.output",
    });

    expect(posted(requests, "/sessions/sess_1/events")).toEqual({
      eventId: "evt_2",
      payload: { text: "done", v: "proof-shell/chat-v1" },
      producerId: "media",
      type: "agent.output",
    });
  });

  it("maps failure classes: transport, auth, and server persistence", async () => {
    const transport: RestParticipantTaskFetch = async () => {
      throw new Error("connection reset");
    };
    const broken = new RestParticipantTaskClient(
      { participantId: "part_1", serviceUrl: "http://127.0.0.1:3025" },
      { fetch: transport },
    );
    await expect(broken.listActiveTasks("sess_1")).rejects.toMatchObject({
      code: "TRANSPORT",
    });

    const auth = fetchJson(401, { code: "AUTH_REQUIRED" });
    await expect(auth.client.listActiveTasks("sess_1")).rejects.toMatchObject({
      code: "AUTHENTICATION",
    });

    const persist = fetchJson(503, { code: "STORE_BUSY" });
    await expect(persist.client.listActiveTasks("sess_1")).rejects.toMatchObject({
      code: "PERSISTENCE",
    });
  });
});
