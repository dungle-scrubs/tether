import { createServer } from "node:net";

import { Effect } from "effect";
import { afterEach, describe, expect, it, vi } from "vitest";
import WebSocket from "ws";

import {
  mintTestAuthToken,
  testAuthSigningKid,
  testAuthSigningSecret,
} from "../src/auth/test-tokens.js";
import type { DatabasePool } from "../src/db.js";
import { createAppServerWithSessionService, type AppServer } from "../src/http.js";
import { defaultResourceLimits, type ResourceLimits } from "../src/resource-limits.js";
import type {
  SessionServiceDebugInfo,
  SessionServiceEffect,
} from "../src/session-service-contracts.js";
import type { SessionEvent } from "../src/types.js";

const gatewaySessionId = "sess_gateway";
const heartbeatIntervalMs = 30_000;

describe("participant WebSocket gateway", () => {
  const openApps: AppServer[] = [];

  afterEach(async () => {
    const apps = openApps.splice(0);
    await Promise.all(apps.map((app) => app.close()));
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("releases the control lease when the peer disconnects during registration", async () => {
    const registration = createDeferred<void>();
    let registerStartedCount = 0;
    const service = createGatewaySessionService({
      registerGate: () => {
        registerStartedCount += 1;
        return registration.promise;
      },
      replayEvents: [],
    });
    const { app, port } = await startGatewayApp(service.service, openApps);

    const socket = new WebSocket(
      participantStreamUrl(port, {
        instanceId: "inst_mid_close",
        participantId: "part_mid_close",
      }),
    );
    await waitForSocketOpen(socket);
    await waitFor(() => registerStartedCount === 1);

    // The peer drops while the durable registration transaction is still in
    // flight; the transaction then commits against a dead socket.
    socket.close();
    await waitForSocketClose(socket);
    registration.resolve();

    // The full cleanup path still runs: the durable lease is released instead
    // of surviving until TTL, and the native-socket counter never drifts.
    await waitFor(() => service.releaseCount() === 1);
    expect(app.debugInfo().hostPresence.nativeParticipantControlSocketCount).toBe(0);
  });

  it("closes the replay with a typed error when the byte budget is exceeded", async () => {
    const events = Array.from({ length: 20 }, (_, index) =>
      createSessionEvent(index + 1, { text: "x".repeat(1_000) }),
    );
    const service = createGatewaySessionService({ replayEvents: events });
    const { port } = await startGatewayApp(service.service, openApps, {
      ...defaultResourceLimits,
      wsMaxPayloadBytes: 1_000,
      wsReplayMaxBytes: 5_000,
    });

    const messages: unknown[] = [];
    const socket = new WebSocket(observerStreamUrl(port));
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    const closeCode = await waitForCloseCode(socket);

    const rejection = messages.find(isErrorEnvelope);
    expect(closeCode).toBe(1013);
    expect(rejection).toMatchObject({
      byteLimit: 5_000,
      reason: "replay_window_exceeded",
    });
    // The bounded page fetch stops as soon as the budget is exceeded instead
    // of materializing the whole stream: with a page size of five events, the
    // first page already exceeds five thousand bytes.
    expect(service.maxListedEventCount()).toBeLessThanOrEqual(5);
  });

  it("replays a window under the byte budget to completion in bounded pages", async () => {
    const events = Array.from({ length: 7 }, (_, index) =>
      createSessionEvent(index + 1, { text: "small" }),
    );
    const service = createGatewaySessionService({ replayEvents: events });
    const { port } = await startGatewayApp(service.service, openApps, {
      ...defaultResourceLimits,
      wsMaxPayloadBytes: 100_000,
      wsReplayMaxBytes: 300_000,
    });

    const messages: unknown[] = [];
    const socket = new WebSocket(observerStreamUrl(port));
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => hasReplayComplete(messages));

    expect(readEventSeqs(messages)).toEqual([1, 2, 3, 4, 5, 6, 7]);

    socket.close();
    await waitForSocketClose(socket);
  });

  it("rejects a read-only principal presenting as a host runtime", async () => {
    const service = createGatewaySessionService({ replayEvents: [] });
    const { port } = await startGatewayApp(service.service, openApps);

    const messages: unknown[] = [];
    const socket = new WebSocket(hostStreamUrl(port, { role: "observer" }));
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    const closeCode = await waitForCloseCode(socket);

    const rejection = messages.find(isErrorEnvelope);
    expect(closeCode).toBe(1008);
    expect(rejection?.error).toBe("WebSocket host presence is not authorized");
    expect(rejection).toMatchObject({ reason: "role" });
  });

  it("still accepts host presence from a participant principal", async () => {
    const service = createGatewaySessionService({ replayEvents: [] });
    const { port } = await startGatewayApp(service.service, openApps);

    const messages: unknown[] = [];
    const socket = new WebSocket(hostStreamUrl(port, { role: "participant" }));
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitForSocketOpen(socket);
    await waitFor(() => messages.some(isPresenceEnvelope));

    const presence = messages.find(isPresenceEnvelope);
    expect(presence).toMatchObject({
      hosts: [{ participantId: "part_gateway_host" }],
      op: "presence",
    });

    socket.close();
    await waitForSocketClose(socket);
  });

  it("refreshes a participant's last-seen time on pongs, at most once per heartbeat interval", async () => {
    useFakeHeartbeatClock();
    const service = createGatewaySessionService({ replayEvents: [] });
    const { port } = await startGatewayApp(service.service, openApps);
    const connectedAt = Date.now();

    const messages: unknown[] = [];
    const socket = new WebSocket(
      participantStreamUrl(port, { instanceId: "inst_presence", participantId: "part_presence" }),
    );
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    const hostSocket = new WebSocket(hostStreamUrl(port, { role: "participant" }));
    await waitForSocketOpen(hostSocket);
    await waitFor(() => hasReplayComplete(messages));

    // Registration just wrote last_seen_at; a pong inside the same interval
    // does nothing.
    socket.pong();
    await settleSocketTraffic();
    expect(service.presenceRefreshes()).toEqual([]);

    await advanceHeartbeat();
    await waitFor(() => service.presenceRefreshes().length === 1);
    socket.pong();
    await settleSocketTraffic();
    await advanceHeartbeat();
    await waitFor(() => service.presenceRefreshes().length === 2);

    // Only the registered participant is refreshed, never the host stream,
    // and the refreshes are one heartbeat interval apart.
    expect(service.presenceRefreshes()).toEqual([
      {
        at: connectedAt + heartbeatIntervalMs,
        participantId: "part_presence",
        sessionId: gatewaySessionId,
      },
      {
        at: connectedAt + 2 * heartbeatIntervalMs,
        participantId: "part_presence",
        sessionId: gatewaySessionId,
      },
    ]);
    // A refresh appends no session event.
    expect(readEventSeqs(messages)).toEqual([]);
    expect(socket.readyState).toBe(WebSocket.OPEN);

    socket.close();
    hostSocket.close();
    await Promise.all([waitForSocketClose(socket), waitForSocketClose(hostSocket)]);
  });

  it("stops refreshing the last-seen time once a silent socket is terminated", async () => {
    useFakeHeartbeatClock();
    const service = createGatewaySessionService({ replayEvents: [] });
    const { port } = await startGatewayApp(service.service, openApps);

    const messages: unknown[] = [];
    let answeringPings = true;
    const socket = new WebSocket(
      participantStreamUrl(port, { instanceId: "inst_silent", participantId: "part_silent" }),
      { autoPong: false },
    );
    socket.on("ping", () => {
      if (answeringPings) {
        socket.pong();
      }
    });
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitFor(() => hasReplayComplete(messages));

    await advanceHeartbeat();
    await waitFor(() => service.presenceRefreshes().length === 1);

    answeringPings = false;
    const closed = waitForSocketClose(socket);
    await advanceHeartbeat();
    await settleSocketTraffic();
    await advanceHeartbeat();
    await closed;

    await advanceHeartbeat();
    await advanceHeartbeat();
    await settleSocketTraffic();
    expect(service.presenceRefreshes()).toHaveLength(1);
  });

  it("logs a failed last-seen refresh and keeps the socket and later refreshes", async () => {
    useFakeHeartbeatClock();
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    const refreshFailure = new Error("participants table unavailable");
    const service = createGatewaySessionService({
      presenceRefreshFailures: [refreshFailure],
      replayEvents: [],
    });
    const { port } = await startGatewayApp(service.service, openApps);
    const connectedAt = Date.now();

    const messages: unknown[] = [];
    const socket = new WebSocket(
      participantStreamUrl(port, { instanceId: "inst_flaky", participantId: "part_flaky" }),
    );
    socket.on("message", (data) => {
      messages.push(JSON.parse(String(data)) as unknown);
    });
    await waitFor(() => hasReplayComplete(messages));

    await advanceHeartbeat();
    await waitFor(() => service.presenceRefreshAttempts() === 1);
    // The failure is logged with its cause rather than surfacing as an
    // unhandled rejection or a socket close.
    await waitFor(() =>
      consoleError.mock.calls.some((call) =>
        call.some((argument) => String(argument).includes(refreshFailure.message)),
      ),
    );
    expect(socket.readyState).toBe(WebSocket.OPEN);

    await advanceHeartbeat();
    await waitFor(() => service.presenceRefreshes().length === 1);
    expect(service.presenceRefreshes()[0]?.at).toBe(connectedAt + 2 * heartbeatIntervalMs);
    expect(socket.readyState).toBe(WebSocket.OPEN);
    expect(messages.some(isErrorEnvelope)).toBe(false);

    socket.close();
    await waitForSocketClose(socket);
  });
});

/** One successful last-seen refresh the fake service recorded. */
interface PresenceRefresh {
  /** Fake-clock time the refresh ran at. */
  readonly at: number;
  /** Participant whose last-seen time was refreshed. */
  readonly participantId: string;
  /** Session the participant belongs to. */
  readonly sessionId: string;
}

interface GatewaySessionService {
  readonly maxListedEventCount: () => number;
  readonly presenceRefreshAttempts: () => number;
  readonly presenceRefreshes: () => readonly PresenceRefresh[];
  readonly releaseCount: () => number;
  readonly service: SessionServiceEffect;
}

function createGatewaySessionService(options: {
  readonly presenceRefreshFailures?: readonly Error[];
  readonly registerGate?: () => Promise<void>;
  readonly replayEvents: readonly SessionEvent[];
}): GatewaySessionService {
  let maxListedEventCount = 0;
  let presenceRefreshAttempts = 0;
  const presenceRefreshes: PresenceRefresh[] = [];
  const pendingPresenceRefreshFailures = [...(options.presenceRefreshFailures ?? [])];
  let releaseCount = 0;
  const service = {
    debugInfo: createGatewayDebugInfo,
    expireTaskClaims: () => Effect.succeed({ events: [], expiredCount: 0 }),
    listEvents: (_sessionId: string, afterSeq: number, listOptions?: { readonly limit?: number }) =>
      Effect.sync(() => {
        const page = options.replayEvents
          .filter((event) => event.seq > afterSeq)
          .slice(0, listOptions?.limit ?? options.replayEvents.length);
        maxListedEventCount = Math.max(maxListedEventCount, page.length);
        return page;
      }),
    refreshWebSocketControlLease: () => Effect.succeed({ status: "ok" as const }),
    refreshWebSocketParticipantPresence: (input: {
      readonly participantId: string;
      readonly sessionId: string;
    }) =>
      Effect.suspend(() => {
        presenceRefreshAttempts += 1;
        const failure = pendingPresenceRefreshFailures.shift();
        if (failure) {
          return Effect.fail(failure);
        }
        presenceRefreshes.push({
          at: Date.now(),
          participantId: input.participantId,
          sessionId: input.sessionId,
        });
        return Effect.succeed(true);
      }),
    registerWebSocketParticipant: (input: {
      readonly instanceId: string;
      readonly participantId: string;
    }) =>
      Effect.promise(async () => {
        await (options.registerGate?.() ?? Promise.resolve());
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
    maxListedEventCount: () => maxListedEventCount,
    presenceRefreshAttempts: () => presenceRefreshAttempts,
    presenceRefreshes: () => presenceRefreshes,
    releaseCount: () => releaseCount,
    service,
  };
}

async function startGatewayApp(
  service: SessionServiceEffect,
  openApps: AppServer[],
  resourceLimits?: ResourceLimits,
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
    ...(resourceLimits ? { resourceLimits } : {}),
    readiness: {
      configurationCompatible: true,
      databaseMigrationReadiness: async () => "current",
      signingAuthorityReady: true,
    },
    taskClaimSweeper: { intervalMs: 0 },
  });
  openApps.push(app);
  const port = await findOpenPort();
  await app.listen(port);
  return { app, port };
}

function participantStreamUrl(
  port: number,
  identity: { readonly instanceId: string; readonly participantId: string },
): string {
  const url = new URL(`ws://127.0.0.1:${port}/sessions/${gatewaySessionId}/stream?after=0`);
  url.searchParams.set("participantId", identity.participantId);
  url.searchParams.set("instanceId", identity.instanceId);
  url.searchParams.set(
    "access_token",
    mintTestAuthToken({
      participantId: identity.participantId,
      role: "participant",
      sessionId: gatewaySessionId,
    }),
  );
  return url.toString();
}

function observerStreamUrl(port: number): string {
  const url = new URL(`ws://127.0.0.1:${port}/sessions/${gatewaySessionId}/stream?after=0`);
  url.searchParams.set(
    "access_token",
    mintTestAuthToken({
      participantId: "part_gateway_observer",
      role: "observer",
      sessionId: gatewaySessionId,
    }),
  );
  return url.toString();
}

function hostStreamUrl(
  port: number,
  options: { readonly role: "observer" | "participant" },
): string {
  const url = new URL(`ws://127.0.0.1:${port}/sessions/${gatewaySessionId}/stream?after=0`);
  url.searchParams.set("runtimeKind", "host");
  url.searchParams.set("participantId", "part_gateway_host");
  url.searchParams.set("instanceId", "inst_gateway_host");
  url.searchParams.set(
    "access_token",
    mintTestAuthToken({
      participantId: "part_gateway_host",
      role: options.role,
      sessionId: gatewaySessionId,
    }),
  );
  return url.toString();
}

function createGatewayDebugInfo(): SessionServiceDebugInfo {
  return {
    activeOperations: 0,
    boundaryCalls: 0,
    boundaryFailures: 0,
    boundaryLogsEnabled: false,
    debugEnabled: false,
    eventSourceId: "test-gateway",
    lastError: null,
    lastOperation: null,
    moduleName: "TestGatewaySessionService",
    restControlLeaseTtlMs: 60_000,
    taskClaimLeaseTtlMs: 30_000,
    wsControlLeaseTtlMs: 3_600_000,
  };
}

function createUnusedDatabasePool(): DatabasePool {
  return {} as unknown as DatabasePool;
}

function createSessionEvent(seq: number, payload: Record<string, unknown>): SessionEvent {
  return {
    createdAt: "2026-07-17T00:00:00.000Z",
    eventId: `evt_gateway_${seq}`,
    payload,
    producerId: "test",
    seq,
    sessionId: gatewaySessionId,
    type: "user.message",
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

function isErrorEnvelope(message: unknown): message is {
  readonly details?: Record<string, unknown>;
  readonly error: string;
  readonly op: "error";
} {
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

async function waitForCloseCode(socket: WebSocket): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    socket.once("close", (code) => resolve(code));
    socket.once("error", reject);
  });
}

/**
 * Fakes only the heartbeat's interval timer and the wall clock, so a test can
 * step the server-wide ping loop while sockets, replay, and polling keep real
 * timers and real I/O. Call before the app starts so its heartbeat interval is
 * registered on the fake clock.
 */
function useFakeHeartbeatClock(): void {
  vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "Date"] });
}

/** Fires the next server-wide heartbeat tick and lets its pings reach the peers. */
async function advanceHeartbeat(): Promise<void> {
  vi.advanceTimersByTime(heartbeatIntervalMs);
  await settleSocketTraffic();
}

/** Waits real time for in-flight loopback frames and their handlers to run. */
async function settleSocketTraffic(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

async function waitFor(assertion: () => boolean): Promise<void> {
  // performance.now keeps advancing while a test fakes Date.
  const startedAt = performance.now();
  while (!assertion()) {
    if (performance.now() - startedAt > 2_000) {
      throw new Error("Timed out waiting for condition");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Creates a manually controlled promise for mid-registration close timing. */
function createDeferred<TValue>(): {
  readonly promise: Promise<TValue>;
  readonly resolve: (value: TValue | PromiseLike<TValue>) => void;
} {
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}
