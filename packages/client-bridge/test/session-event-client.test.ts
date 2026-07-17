import { describe, expect, it } from "vitest";

import {
  ClientBridgeSessionEventClient,
  clientBridgeRoutes,
  SessionEventStreamClient,
  SessionEventStreamError,
  type ClientBridgeFetch,
  type ClientBridgeRequestError,
} from "../src/index.js";

describe("ClientBridgeSessionEventClient", () => {
  it("re-exports the typed observer transport used by streaming bridges", () => {
    expect(SessionEventStreamClient).toBeTypeOf("function");
    expect(new SessionEventStreamError({ message: "Replay failed" })).toMatchObject({
      reason: null,
      safeDetails: {},
    });
  });

  it("builds encoded session event routes", () => {
    expect(clientBridgeRoutes.sessionEvents("sess/with space", 42)).toBe(
      "/sessions/sess%2Fwith%20space/events?after=42",
    );
  });

  it("lists paginated events and counts every request", async () => {
    const requests: CapturedRequest[] = [];
    const fetch = createJsonFetch(requests, [
      {
        body: createEventListResponse({
          events: [createEventFixture({ eventId: "evt_1", seq: 1 })],
          hasMore: true,
          nextAfterSeq: 1,
        }),
        status: 200,
      },
      {
        body: createEventListResponse({
          events: [createEventFixture({ eventId: "evt_2", seq: 2 })],
          hasMore: false,
          nextAfterSeq: 2,
        }),
        status: 200,
      },
    ]);
    const client = new ClientBridgeSessionEventClient(
      { serviceUrl: "http://tether.test" },
      { fetch },
    );

    await expect(client.listEvents("sess_1")).resolves.toMatchObject([
      { eventId: "evt_1" },
      { eventId: "evt_2" },
    ]);

    expect(requests.map((request) => request.path)).toEqual([
      "/sessions/sess_1/events?after=0",
      "/sessions/sess_1/events?after=1",
    ]);
    expect(client.debugInfo()).toEqual({ requestCount: 2 });
  });

  it("sends configured bearer tokens", async () => {
    const fetch: ClientBridgeFetch = async (_url, init) => {
      expect(new Headers(init.headers).get("authorization")).toBe("Bearer bridge-token");
      return jsonResponse(
        createEventListResponse({
          events: [],
          hasMore: false,
          nextAfterSeq: 0,
        }),
        200,
      );
    };
    const client = new ClientBridgeSessionEventClient(
      { authToken: "bridge-token", serviceUrl: "http://tether.test" },
      { fetch },
    );

    await expect(client.listEvents("sess_1")).resolves.toEqual([]);
  });

  it("returns zero for empty latest sequence and max sequence for populated histories", async () => {
    const client = new ClientBridgeSessionEventClient(
      { serviceUrl: "http://tether.test" },
      {
        fetch: createJsonFetch(
          [],
          [
            {
              body: createEventListResponse({
                events: [
                  createEventFixture({ eventId: "evt_2", seq: 2 }),
                  createEventFixture({ eventId: "evt_5", seq: 5 }),
                ],
                hasMore: false,
                nextAfterSeq: 5,
              }),
              status: 200,
            },
            {
              body: createEventListResponse({
                events: [],
                hasMore: false,
                nextAfterSeq: 0,
              }),
              status: 200,
            },
          ],
        ),
      },
    );

    await expect(client.readLatestEventSeq("sess_1")).resolves.toBe(5);
    await expect(client.readLatestEventSeq("sess_1")).resolves.toBe(0);
  });

  it("throws typed errors for non-advancing pagination", async () => {
    const client = new ClientBridgeSessionEventClient(
      { serviceUrl: "http://tether.test" },
      {
        fetch: createJsonFetch(
          [],
          [
            {
              body: createEventListResponse({
                events: [createEventFixture({ eventId: "evt_1", seq: 1 })],
                hasMore: true,
                nextAfterSeq: 0,
              }),
              status: 200,
            },
          ],
        ),
      },
    );

    await expect(client.listEvents("sess_1")).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
      name: "ClientBridgeRequestError",
    } satisfies Partial<ClientBridgeRequestError>);
  });

  it("preserves shared transport error codes", async () => {
    await expect(
      new ClientBridgeSessionEventClient(
        { serviceUrl: "http://tether.test" },
        {
          fetch: async () => {
            throw new Error("network down");
          },
        },
      ).listEvents("sess_1"),
    ).rejects.toMatchObject({ code: "NETWORK_ERROR" } satisfies Partial<ClientBridgeRequestError>);

    await expect(
      new ClientBridgeSessionEventClient(
        { serviceUrl: "http://tether.test" },
        {
          fetch: async () => jsonResponse({ error: "nope" }, 500),
        },
      ).listEvents("sess_1"),
    ).rejects.toMatchObject({ code: "HTTP_ERROR" } satisfies Partial<ClientBridgeRequestError>);

    await expect(
      new ClientBridgeSessionEventClient(
        { serviceUrl: "http://tether.test" },
        {
          fetch: async () => jsonResponse({ nope: true }, 200),
        },
      ).listEvents("sess_1"),
    ).rejects.toMatchObject({
      code: "INVALID_RESPONSE",
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

/** Creates a fetch implementation that records requests and returns queued JSON responses. */
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
    return jsonResponse(response.body, response.status);
  };
}

/** Creates one JSON response for a fake fetch call. */
function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    headers: { "content-type": "application/json" },
    status,
  });
}

/** Builds an event-list response fixture. */
function createEventListResponse(input: {
  readonly events: readonly ReturnType<typeof createEventFixture>[];
  readonly hasMore: boolean;
  readonly nextAfterSeq: number;
}) {
  return {
    events: input.events,
    pagination: {
      afterSeq: 0,
      hasMore: input.hasMore,
      limit: 100,
      nextAfterSeq: input.nextAfterSeq,
      returned: input.events.length,
    },
  };
}

/** Builds a durable session event fixture. */
function createEventFixture(input: { readonly eventId: string; readonly seq: number }) {
  return {
    createdAt: "2026-07-01T00:00:00.000Z",
    eventId: input.eventId,
    payload: {},
    producerId: "bridge-test",
    seq: input.seq,
    sessionId: "sess_1",
    type: "agent.output",
  };
}
