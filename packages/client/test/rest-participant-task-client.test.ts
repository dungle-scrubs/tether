import { describe, expect, it } from "vitest";

import {
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
    return new Response(status === 204 ? null : JSON.stringify(body), { status });
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

describe("RestParticipantTaskClient", () => {
  it("claims a task with the fenced identity and parses the record", async () => {
    const { client, requests } = fetchJson(200, taskRecord);

    const claimed = await client.claimTask("sess_1", "task_1", { controlEpoch: 3 });

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

  it("completes with the claim id and result, and sends no body on GET", async () => {
    const requests: CapturedRequest[] = [];
    const fetch: RestParticipantTaskFetch = async (url, init) => {
      requests.push(capture(url, init));
      const path = `${url.pathname}${url.search}`;
      if (path.endsWith("/complete")) {
        return new Response(null, { status: 204 });
      }
      return new Response(JSON.stringify({ tasks: [taskRecord] }), { status: 200 });
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

  it("reads a task and returns null on 404", async () => {
    const found = fetchJson(200, taskRecord);
    await expect(found.client.readTask("sess_1", "task_1")).resolves.toMatchObject({
      taskId: "task_1",
    });

    const missing = fetchJson(404, { code: "TASK_NOT_FOUND" });
    await expect(missing.client.readTask("sess_1", "task_x")).resolves.toBe(null);
  });

  it("lists active tasks as records", async () => {
    const { client } = fetchJson(200, { tasks: [taskRecord] });

    await expect(client.listActiveTasks("sess_1")).resolves.toHaveLength(1);
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
      pagination: { afterSeq: 0, hasMore: false, limit: 50, nextAfterSeq: 7, returned: 1 },
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

    expect(requests[0]).toMatchObject({
      body: {
        eventId: "evt_2",
        payload: { text: "done", v: "proof-shell/chat-v1" },
        producerId: "media",
        type: "agent.output",
      },
      method: "POST",
      path: "/sessions/sess_1/events",
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
