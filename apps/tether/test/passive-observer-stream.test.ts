import { createServer } from "node:net";

import { Effect } from "effect";
import { afterEach, describe, expect, it } from "vitest";
import WebSocket from "ws";

import {
  mintTestAuthToken,
  testAuthSigningKid,
  testAuthSigningSecret,
} from "../src/auth/test-tokens.js";
import type { DatabasePool } from "../src/db.js";
import { HostPresenceRuntime, projectWebSocketPresenceEnvelope } from "../src/host-presence.js";
import { createAppServerWithSessionService, type AppServer } from "../src/http.js";
import { webSocketPresenceEnvelopeSchema } from "../src/protocol.js";
import type { SessionEvent } from "../src/types.js";
import type {
  SessionServiceDebugInfo,
  SessionServiceEffect,
} from "../src/session-service-contracts.js";

const observerSessionId = "sess_observer";

describe("passive full-event observer stream", () => {
  const openApps: AppServer[] = [];

  afterEach(async () => {
    const apps = openApps.splice(0);
    await Promise.all(apps.map((app) => app.close()));
  });

  it("projects protocol-owned Replica Scope WebSocket presence envelopes", () => {
    const runtime = new HostPresenceRuntime();
    runtime.upsertHost(observerSessionId, {
      displayName: "Host One",
      instanceId: "inst_1",
      participantId: "part_1",
    });

    expect(
      webSocketPresenceEnvelopeSchema.parse(
        projectWebSocketPresenceEnvelope({
          replicaId: "replica_ws_1",
          runtime,
          sessionId: observerSessionId,
        }),
      ),
    ).toEqual({
      hosts: [
        {
          displayName: "Host One",
          instanceId: "inst_1",
          participantId: "part_1",
        },
      ],
      op: "presence",
      replicaId: "replica_ws_1",
      scope: "replica",
    });
  });

  it("replays and follows durable events without acquiring a control lease", async () => {
    const service = createObserverSessionService({
      replayEvents: [createSessionEvent(1), createSessionEvent(2)],
    });
    const { app, port } = await startObserverApp(service.service);

    const messages: unknown[] = [];
    const socket = new WebSocket(observerStreamUrl(port, { role: "observer" }));
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => hasReplayComplete(messages));

    // A live event published after replay is delivered through the same hub the
    // participant path uses, so the passive observer follows the live stream.
    const liveResponse = await publishEvent(port);
    expect(liveResponse.status).toBe(201);
    await waitFor(() => readEventSeqs(messages).includes(3));

    expect(readEventSeqs(messages)).toEqual([1, 2, 3]);
    // The observer never registers a durable participant and never acquires or
    // refreshes a control lease: it is routed to the passive read-only path.
    expect(service.registerCount()).toBe(0);
    expect(service.refreshCount()).toBe(0);
    expect(service.releaseCount()).toBe(0);
    expect(app.debugInfo().hostPresence.nativeParticipantControlSocketCount).toBe(0);
    expect(app.debugInfo().hostPresence.passiveSocketCount).toBeGreaterThanOrEqual(1);

    socket.close();
    await waitForSocketClose(socket);
  });

  it("lets multiple concurrent observers share one identity without conflict", async () => {
    const service = createObserverSessionService({ replayEvents: [createSessionEvent(1)] });
    const { app, port } = await startObserverApp(service.service);

    const first = new WebSocket(observerStreamUrl(port, { role: "observer" }));
    const second = new WebSocket(observerStreamUrl(port, { role: "observer" }));
    const firstMessages: unknown[] = [];
    const secondMessages: unknown[] = [];
    first.on("message", (data) => firstMessages.push(JSON.parse(String(data)) as unknown));
    second.on("message", (data) => secondMessages.push(JSON.parse(String(data)) as unknown));
    await Promise.all([waitForSocketOpen(first), waitForSocketOpen(second)]);
    await waitFor(() => hasReplayComplete(firstMessages) && hasReplayComplete(secondMessages));

    // A control participant would collide on a second connection for the same
    // identity; two passive observers hold no lease, so neither is fenced.
    expect(firstMessages.some(isErrorEnvelope)).toBe(false);
    expect(secondMessages.some(isErrorEnvelope)).toBe(false);
    expect(service.registerCount()).toBe(0);
    expect(app.debugInfo().hostPresence.passiveSocketCount).toBe(2);
    expect(app.debugInfo().hostPresence.nativeParticipantControlSocketCount).toBe(0);

    first.close();
    second.close();
    await Promise.all([waitForSocketClose(first), waitForSocketClose(second)]);
  });

  it("rejects control commands sent over a passive observer stream", async () => {
    const service = createObserverSessionService({ replayEvents: [] });
    const { port } = await startObserverApp(service.service);

    const messages: unknown[] = [];
    const socket = new WebSocket(observerStreamUrl(port, { role: "observer" }));
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => hasReplayComplete(messages));

    socket.send(JSON.stringify({ op: "task.claim", requestId: "req_1", taskId: "task_1" }));
    await waitFor(() =>
      messages.some((message) => isErrorEnvelope(message) && /read-only/u.test(message.error)),
    );

    const rejection = messages.find(isErrorEnvelope);
    expect(rejection?.error).toBe("Passive observer stream is read-only");
    expect(service.registerCount()).toBe(0);
    expect(service.refreshCount()).toBe(0);

    socket.close();
    await waitForSocketClose(socket);
  });

  it("keeps observers free of presence frames while viewer streams still get them", async () => {
    const service = createObserverSessionService({ replayEvents: [] });
    const { port } = await startObserverApp(service.service);

    const observerMessages: unknown[] = [];
    const viewerMessages: unknown[] = [];
    const observer = new WebSocket(observerStreamUrl(port, { role: "observer" }));
    const viewer = new WebSocket(
      observerStreamUrl(port, { role: "observer", runtimeKind: "viewer" }),
    );
    observer.on("message", (data) => observerMessages.push(JSON.parse(String(data)) as unknown));
    viewer.on("message", (data) => viewerMessages.push(JSON.parse(String(data)) as unknown));
    await Promise.all([waitForSocketOpen(observer), waitForSocketOpen(viewer)]);
    await waitFor(() => hasReplayComplete(observerMessages) && hasReplayComplete(viewerMessages));
    await waitFor(() => viewerMessages.some(isPresenceEnvelope));

    // Host/viewer presence behavior is unchanged: viewers still receive
    // process-local presence frames. Observers receive only durable stream
    // envelopes a bare SessionEventStreamClient can parse.
    expect(viewerMessages.some(isPresenceEnvelope)).toBe(true);
    expect(observerMessages.some(isPresenceEnvelope)).toBe(false);

    observer.close();
    viewer.close();
    await Promise.all([waitForSocketClose(observer), waitForSocketClose(viewer)]);
  });

  it("still routes control participants through durable lease acquisition", async () => {
    const service = createObserverSessionService({ replayEvents: [] });
    const { app, port } = await startObserverApp(service.service);

    const url = new URL(`ws://127.0.0.1:${port}/sessions/${observerSessionId}/stream?after=0`);
    url.searchParams.set("participantId", "part_observer_identity");
    url.searchParams.set("instanceId", "inst_observer_identity");
    url.searchParams.set(
      "access_token",
      mintTestAuthToken({
        participantId: "part_observer_identity",
        role: "participant",
        sessionId: observerSessionId,
      }),
    );
    const messages: unknown[] = [];
    const socket = new WebSocket(url);
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => hasReplayComplete(messages));

    // The participant path is untouched: a control participant still acquires a
    // durable lease and registers a native control socket.
    expect(service.registerCount()).toBe(1);
    expect(app.debugInfo().hostPresence.nativeParticipantControlSocketCount).toBe(1);

    socket.close();
    await waitForSocketClose(socket);
  });
});

interface ObserverSessionService {
  readonly service: SessionServiceEffect;
  readonly registerCount: () => number;
  readonly refreshCount: () => number;
  readonly releaseCount: () => number;
}

function createObserverSessionService(options: {
  readonly replayEvents: readonly SessionEvent[];
}): ObserverSessionService {
  let registerCount = 0;
  let refreshCount = 0;
  let releaseCount = 0;
  let liveSeq = maxSeq(options.replayEvents);
  const service = {
    debugInfo: createObserverDebugInfo,
    expireTaskClaims: () => Effect.succeed({ events: [], expiredCount: 0 }),
    listEvents: () => Effect.succeed([...options.replayEvents]),
    publishRestEvent: (input: {
      readonly payload: Record<string, unknown>;
      readonly producerId: string;
      readonly sessionId: string;
      readonly type: string;
    }) =>
      Effect.sync(() => {
        liveSeq += 1;
        const event = createSessionEvent(liveSeq, input.payload, input.producerId, input.type);
        return { event, events: [event], status: "created" as const };
      }),
    refreshWebSocketControlLease: () =>
      Effect.sync(() => {
        refreshCount += 1;
        return { status: "ok" as const };
      }),
    registerWebSocketParticipant: (input: {
      readonly instanceId: string;
      readonly participantId: string;
    }) =>
      Effect.sync(() => {
        registerCount += 1;
        return {
          context: {
            controlEpoch: 1,
            instanceId: input.instanceId,
            participantId: input.participantId,
          },
          events: [],
          status: "ok" as const,
        };
      }),
    releaseControlLease: () =>
      Effect.sync(() => {
        releaseCount += 1;
        return { status: "released" as const };
      }),
  } as unknown as SessionServiceEffect;
  return {
    registerCount: () => registerCount,
    refreshCount: () => refreshCount,
    releaseCount: () => releaseCount,
    service,
  };
}

async function startObserverApp(
  service: SessionServiceEffect,
): Promise<{ readonly app: AppServer; readonly port: number }> {
  const app = createAppServerWithSessionService(createUnusedDatabasePool(), service, {
    auth: {
      activeKid: testAuthSigningKid,
      // These fixtures authenticate with legacy stateless test tokens, which
      // required mode rejects unless the migration escape hatch is enabled.
      allowLegacyTokens: true,
      mode: "required",
      secrets: { [testAuthSigningKid]: testAuthSigningSecret },
    },
    eventFanout: { catchUpPollIntervalMs: 0, listenEnabled: false },
    taskClaimSweeper: { intervalMs: 0 },
  });
  const port = await findOpenPort();
  await app.listen(port);
  return { app, port };
}

function observerStreamUrl(
  port: number,
  options: { readonly role: "admin" | "observer" | "participant"; readonly runtimeKind?: string },
): string {
  const url = new URL(`ws://127.0.0.1:${port}/sessions/${observerSessionId}/stream?after=0`);
  url.searchParams.set("runtimeKind", options.runtimeKind ?? "observer");
  url.searchParams.set(
    "access_token",
    mintTestAuthToken({
      participantId: "part_observer_identity",
      role: options.role,
      sessionId: observerSessionId,
    }),
  );
  return url.toString();
}

async function publishEvent(port: number): Promise<Response> {
  const producerId = "observer-test-producer";
  return fetch(`http://127.0.0.1:${port}/sessions/${observerSessionId}/events`, {
    body: JSON.stringify({
      payload: { text: "live after replay" },
      producerId,
      type: "user.message",
    }),
    headers: {
      // A publish is authorized only when the token identity matches the
      // producer, so mint a participant token bound to the producer id.
      authorization: `Bearer ${mintTestAuthToken({
        participantId: producerId,
        role: "participant",
        sessionId: observerSessionId,
      })}`,
      "content-type": "application/json",
    },
    method: "POST",
  });
}

function createObserverDebugInfo(): SessionServiceDebugInfo {
  return {
    activeOperations: 0,
    boundaryCalls: 0,
    boundaryFailures: 0,
    boundaryLogsEnabled: false,
    debugEnabled: false,
    eventSourceId: "test-passive-observer",
    lastError: null,
    lastOperation: null,
    moduleName: "TestObserverSessionService",
    restControlLeaseTtlMs: 60_000,
    taskClaimLeaseTtlMs: 30_000,
    wsControlLeaseTtlMs: 3_600_000,
  };
}

function createUnusedDatabasePool(): DatabasePool {
  return {} as unknown as DatabasePool;
}

function createSessionEvent(
  seq: number,
  payload: Record<string, unknown> = {},
  producerId = "test",
  type = "user.message",
): SessionEvent {
  return {
    createdAt: "2026-07-12T00:00:00.000Z",
    eventId: `evt_observer_${seq}`,
    payload,
    producerId,
    seq,
    sessionId: observerSessionId,
    type,
  };
}

function maxSeq(events: readonly SessionEvent[]): number {
  return events.reduce((highest, event) => Math.max(highest, event.seq), 0);
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

function isErrorEnvelope(
  message: unknown,
): message is { readonly error: string; readonly op: "error" } {
  return (
    typeof message === "object" &&
    message !== null &&
    "op" in message &&
    message.op === "error" &&
    "error" in message &&
    typeof message.error === "string"
  );
}

function isPresenceEnvelope(message: unknown): boolean {
  return (
    typeof message === "object" && message !== null && "op" in message && message.op === "presence"
  );
}

async function findOpenPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, resolve);
  });
  const address = server.address();
  if (typeof address !== "object" || address === null) {
    throw new Error("Expected TCP server address");
  }
  const port = address.port;
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
  return port;
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
