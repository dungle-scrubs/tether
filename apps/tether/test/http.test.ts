import { Effect } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket from "ws";

import type { DatabasePool } from "../src/db.js";
import { HostPresenceRuntime, projectSessionInventory } from "../src/host-presence.js";
import { createAppServerWithSessionService, type AppServer } from "../src/http.js";
import { hostPresenceInventorySchema } from "../src/protocol.js";
import { defaultResourceLimits, sessionEventByteLength } from "../src/resource-limits.js";
import type { SessionEvent } from "../src/types.js";
import type {
  SessionServiceDebugInfo,
  SessionServiceEffect,
} from "../src/session-service-contracts.js";

describe("HTTP app server error boundary", () => {
  const openApps: AppServer[] = [];

  afterEach(async () => {
    const apps = openApps.splice(0);
    await Promise.all(apps.map((app) => app.close()));
  });

  it("exposes runtime topology and the service event source as replica identity", () => {
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createReplayInterleavingSessionService(createDeferred(), createDeferred()),
      { runtimeTopology: "multi" },
    );

    expect(app.debugInfo()).toMatchObject({
      replicaId: "test-replay-interleaving",
      runtimeTopology: "multi",
    });
  });

  it("projects HTTP session inventory with mandatory Replica Scope metadata", () => {
    const inventory = projectSessionInventory({
      replicaId: "replica_http_1",
      runtime: new HostPresenceRuntime(),
      sessions: [],
    });

    expect(hostPresenceInventorySchema.parse(inventory)).toEqual({
      replicaId: "replica_http_1",
      scope: "replica",
      sessions: [],
    });
  });

  it("rejects multi-topology permanent delete before consulting local presence state", async () => {
    const listSessions = vi.fn(() => Effect.die(new Error("local presence consulted")));
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createPermanentDeleteSessionService(listSessions),
      {
        auth: { activeKid: "disabled", mode: "disabled", secrets: {} },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        runtimeTopology: "multi",
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);

    const response = await fetch(`http://127.0.0.1:${port}/sessions/sess_remote_host/delete`, {
      method: "POST",
    });

    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({
      detail: "permanent delete requires cluster-complete Host Presence",
      ok: false,
      reason: "presence_scope_insufficient",
    });
    expect(listSessions).not.toHaveBeenCalled();
  });

  it.each([
    "multi",
    "single",
  ] as const)("keeps %s-topology inventory readable and replica-scoped", async (runtimeTopology) => {
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createPermanentDeleteSessionService(() => Effect.succeed([])),
      {
        auth: { activeKid: "disabled", mode: "disabled", secrets: {} },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        runtimeTopology,
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);

    const response = await fetch(`http://127.0.0.1:${port}/sessions`);

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      replicaId: "test-permanent-delete",
      scope: "replica",
      sessions: [],
    });
  });

  it("serves readiness without authentication while keeping health as independent liveness", async () => {
    const app = createAppServerWithSessionService(
      {
        pool: { query: () => Promise.resolve({ rows: [] }) },
      } as unknown as DatabasePool,
      createPermanentDeleteSessionService(() => Effect.succeed([])),
      {
        auth: { activeKid: "test", mode: "required", secrets: { test: "secret" } },
        eventFanout: { catchUpPollIntervalMs: 10, listenEnabled: false },
        runtimeTopology: "multi",
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);

    const readiness = await fetch(`http://127.0.0.1:${port}/ready`);
    const health = await fetch(`http://127.0.0.1:${port}/health`);
    const protectedSessions = await fetch(`http://127.0.0.1:${port}/sessions`);

    expect(readiness.status).toBe(200);
    expect(await readiness.json()).toEqual({
      ready: true,
      replicaId: "test-permanent-delete",
      runtimeTopology: "multi",
    });
    expect(health.status).toBe(200);
    expect(protectedSessions.status).toBe(401);
  });

  it("redacts service failures that travel through the app-server catch boundary", async () => {
    const logs: RouteErrorLog[] = [];
    const secret = "secret db detail: unique constraint sessions_session_id_key";
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createThrowingSessionService(new Error(secret)),
      {
        auth: { activeKid: "disabled", mode: "disabled", secrets: {} },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        httpRouteErrors: {
          logger: {
            error: (event, details) => {
              logs.push({ details, event });
            },
          },
          requestIdFactory: () => "req_app_boundary",
        },
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);

    const response = await fetch(`http://127.0.0.1:${port}/sessions`, {
      body: "{}",
      headers: { "content-type": "application/json" },
      method: "POST",
    });
    const body = (await response.json()) as unknown;

    expect(response.status).toBe(500);
    expect(body).toEqual({
      error: "Internal server error",
      requestId: "req_app_boundary",
    });
    expect(JSON.stringify(body)).not.toContain(secret);
    expect(logs).toEqual([
      {
        details: {
          error: {
            message: secret,
            name: "Error",
          },
          requestId: "req_app_boundary",
        },
        event: "http.route_error",
      },
    ]);
  });

  it("repairs gaps before completing replay when live events arrive during replay", async () => {
    const replayEvents = createDeferred<SessionEvent[]>();
    const listEventsStarted = createDeferred<void>();
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createReplayInterleavingSessionService(replayEvents, listEventsStarted),
      {
        auth: { activeKid: "disabled", mode: "disabled", secrets: {} },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);
    const socket = new WebSocket(`ws://127.0.0.1:${port}/sessions/sess_gateway/stream?after=0`);
    const messages: unknown[] = [];
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await listEventsStarted.promise;

    const liveResponse = await fetch(`http://127.0.0.1:${port}/sessions/sess_gateway/events`, {
      body: JSON.stringify({
        payload: { text: "live during replay" },
        producerId: "gateway-test",
        type: "user.message",
      }),
      headers: { "content-type": "application/json" },
      method: "POST",
    });
    expect(liveResponse.status).toBe(201);
    expect(readEventSeqs(messages)).toEqual([]);

    replayEvents.resolve([
      createSessionEvent(1, "sess_gateway"),
      createSessionEvent(2, "sess_gateway"),
    ]);
    await waitFor(() => hasReplayComplete(messages));

    expect(readEventSeqs(messages)).toEqual([1, 2, 3, 4]);
    socket.close();
    await waitForSocketClose(socket);
  });
});

describe("REST events-list byte budget", () => {
  const openApps: AppServer[] = [];

  afterEach(async () => {
    const apps = openApps.splice(0);
    await Promise.all(apps.map((app) => app.close()));
  });

  it("truncates a page whose events exceed the byte budget and signals more", async () => {
    const sessionId = "sess_events_budget";
    const events = Array.from({ length: 12 }, (_, index) =>
      createSessionEvent(index + 1, sessionId, { blob: "x".repeat(400) }),
    );
    const perEventBytes = sessionEventByteLength(
      createSessionEvent(1, sessionId, { blob: "x".repeat(400) }),
    );
    const maxBytes = perEventBytes * 3 + 1;
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createEventListSessionService(events),
      {
        auth: { activeKid: "disabled", mode: "disabled", secrets: {} },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        resourceLimits: { ...defaultResourceLimits, restEventListMaxBytes: maxBytes },
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);

    const response = await fetch(`http://127.0.0.1:${port}/sessions/${sessionId}/events`);
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      readonly events: SessionEvent[];
      readonly pagination: { readonly hasMore: boolean; readonly nextAfterSeq: number };
    };

    expect(body.events.length).toBeGreaterThan(0);
    expect(body.events.length).toBeLessThan(events.length);
    expect(body.pagination.hasMore).toBe(true);
    expect(body.pagination.nextAfterSeq).toBe(body.events.at(-1)?.seq);
    // The returned page fits the budget, and the first dropped event would have
    // pushed it past the boundary.
    const returnedBytes = body.events.reduce(
      (total, event) => total + sessionEventByteLength(event),
      0,
    );
    expect(returnedBytes).toBeLessThanOrEqual(maxBytes);
    const nextEvent = events[body.events.length];
    expect(nextEvent).toBeDefined();
    expect(returnedBytes + (nextEvent ? sessionEventByteLength(nextEvent) : 0)).toBeGreaterThan(
      maxBytes,
    );
  });

  it("returns a small page whole when it fits within the byte budget", async () => {
    const sessionId = "sess_events_small";
    const events = [
      createSessionEvent(1, sessionId, { blob: "a" }),
      createSessionEvent(2, sessionId, { blob: "b" }),
      createSessionEvent(3, sessionId, { blob: "c" }),
    ];
    const app = createAppServerWithSessionService(
      createUnusedDatabasePool(),
      createEventListSessionService(events),
      {
        auth: { activeKid: "disabled", mode: "disabled", secrets: {} },
        eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
        taskClaimSweeper: { intervalMs: 0 },
      },
    );
    openApps.push(app);
    const port = await app.listen(0);

    const response = await fetch(`http://127.0.0.1:${port}/sessions/${sessionId}/events`);
    expect(response.status).toBe(200);
    const body = (await response.json()) as {
      readonly events: SessionEvent[];
      readonly pagination: { readonly hasMore: boolean; readonly returned: number };
    };

    expect(body.events.map((event) => event.seq)).toEqual([1, 2, 3]);
    expect(body.pagination.hasMore).toBe(false);
    expect(body.pagination.returned).toBe(3);
  });
});

/** Serves an in-memory event log paged by afterSeq for events-list tests. */
function createEventListSessionService(events: readonly SessionEvent[]): SessionServiceEffect {
  return {
    debugInfo: createSessionServiceDebugInfo,
    expireTaskClaims: () => Effect.succeed({ events: [], expiredCount: 0 }),
    listEvents: (_sessionId: string, afterSeq: number, options?: { readonly limit?: number }) =>
      Effect.succeed(
        events.filter((event) => event.seq > afterSeq).slice(0, options?.limit ?? events.length),
      ),
  } as unknown as SessionServiceEffect;
}

interface RouteErrorLog {
  readonly details: {
    readonly error: {
      readonly message: string;
      readonly name: string;
    };
    readonly requestId: string;
  };
  readonly event: "http.route_error";
}

function createThrowingSessionService(error: Error): SessionServiceEffect {
  const debugInfo = (): SessionServiceDebugInfo => ({
    activeOperations: 0,
    boundaryCalls: 0,
    boundaryFailures: 0,
    boundaryLogsEnabled: false,
    debugEnabled: false,
    eventSourceId: "test-http-error-boundary",
    lastError: null,
    lastOperation: null,
    moduleName: "TestSessionService",
    restControlLeaseTtlMs: 60_000,
    taskClaimLeaseTtlMs: 30_000,
    wsControlLeaseTtlMs: 3_600_000,
  });
  return {
    debugInfo,
    ensurePublicSession: () => Effect.fail(error),
    expireTaskClaims: () => Effect.succeed({ events: [], expiredCount: 0 }),
    listEvents: () => Effect.succeed([]),
  } as unknown as SessionServiceEffect;
}

function createReplayInterleavingSessionService(
  replayEvents: Deferred<SessionEvent[]>,
  listEventsStarted: Deferred<void>,
): SessionServiceEffect {
  let listEventsCallCount = 0;
  const debugInfo = createSessionServiceDebugInfo;
  return {
    debugInfo,
    expireTaskClaims: () => Effect.succeed({ events: [], expiredCount: 0 }),
    listEvents: () =>
      Effect.promise(() => {
        listEventsCallCount += 1;
        if (listEventsCallCount === 1) {
          listEventsStarted.resolve();
          return replayEvents.promise;
        }
        return Promise.resolve([createSessionEvent(3, "sess_gateway")]);
      }),
    publishRestEvent: (input: {
      readonly payload: Record<string, unknown>;
      readonly producerId: string;
      readonly sessionId: string;
      readonly type: string;
    }) =>
      Effect.succeed({
        event: createSessionEvent(4, input.sessionId, input.payload, input.producerId, input.type),
        events: [
          createSessionEvent(4, input.sessionId, input.payload, input.producerId, input.type),
        ],
        status: "created",
      }),
  } as unknown as SessionServiceEffect;
}

/** Builds the minimal service surface used by topology-gated delete tests. */
function createPermanentDeleteSessionService(
  listSessions: SessionServiceEffect["listSessions"],
): SessionServiceEffect {
  return {
    debugInfo: () => ({
      ...createSessionServiceDebugInfo(),
      eventSourceId: "test-permanent-delete",
    }),
    expireTaskClaims: () => Effect.succeed({ events: [], expiredCount: 0 }),
    listEvents: () => Effect.succeed([]),
    listSessions,
  } as unknown as SessionServiceEffect;
}

function createSessionServiceDebugInfo(): SessionServiceDebugInfo {
  return {
    activeOperations: 0,
    boundaryCalls: 0,
    boundaryFailures: 0,
    boundaryLogsEnabled: false,
    debugEnabled: false,
    eventSourceId: "test-replay-interleaving",
    lastError: null,
    lastOperation: null,
    moduleName: "TestSessionService",
    restControlLeaseTtlMs: 60_000,
    taskClaimLeaseTtlMs: 30_000,
    wsControlLeaseTtlMs: 3_600_000,
  };
}

function createUnusedDatabasePool(): DatabasePool {
  return {} as unknown as DatabasePool;
}

interface Deferred<TValue> {
  readonly promise: Promise<TValue>;
  readonly resolve: (value: TValue | PromiseLike<TValue>) => void;
}

function createDeferred<TValue>(): Deferred<TValue> {
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

function createSessionEvent(
  seq: number,
  sessionId: string,
  payload: Record<string, unknown> = {},
  producerId = "test",
  type = "user.message",
): SessionEvent {
  return {
    createdAt: "2026-05-21T00:00:00.000Z",
    eventId: `evt_gateway_${seq}`,
    payload,
    producerId,
    seq,
    sessionId,
    type,
  };
}

function readEventSeqs(messages: readonly unknown[]): number[] {
  return messages
    .filter(
      (message): message is { readonly event: { readonly seq: number }; readonly op: "event" } =>
        typeof message === "object" &&
        message !== null &&
        "op" in message &&
        message.op === "event" &&
        "event" in message &&
        typeof message.event === "object" &&
        message.event !== null &&
        "seq" in message.event &&
        typeof message.event.seq === "number",
    )
    .map((message) => message.event.seq);
}

function hasReplayComplete(messages: readonly unknown[]): boolean {
  return messages.some(
    (message) =>
      typeof message === "object" &&
      message !== null &&
      "op" in message &&
      message.op === "replay.complete",
  );
}

async function waitForSocketOpen(socket: WebSocket): Promise<void> {
  if (socket.readyState === WebSocket.OPEN) {
    return;
  }
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
}

async function waitForSocketClose(socket: WebSocket): Promise<void> {
  if (socket.readyState === WebSocket.CLOSED) {
    return;
  }
  await new Promise<void>((resolve) => {
    socket.once("close", () => resolve());
  });
}

async function waitFor(assertion: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (!assertion()) {
    if (Date.now() - startedAt > 1_000) {
      throw new Error("Timed out waiting for condition");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
