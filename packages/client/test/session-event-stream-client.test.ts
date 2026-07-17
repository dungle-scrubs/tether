import { EventEmitter } from "node:events";

import { describe, expect, it } from "vitest";
import WebSocket from "ws";

import {
  buildSessionEventStreamUrl,
  SessionEventStreamClient,
  SessionEventStreamError,
  type SessionEvent,
  type SessionEventStreamWebSocketFactory,
} from "../src/index.js";

describe("SessionEventStreamClient", () => {
  it("builds observer stream URLs without participant task authority", () => {
    const url = new URL(
      buildSessionEventStreamUrl({
        afterSeq: 12,
        authToken: "observer-token",
        serviceUrl: "https://tether.test/base",
        sessionId: "sess/with space",
      }),
    );

    expect(url.protocol).toBe("wss:");
    expect(url.pathname).toBe("/sessions/sess%2Fwith%20space/stream");
    expect(url.searchParams.get("after")).toBe("12");
    expect(url.searchParams.get("access_token")).toBe("observer-token");
    // The passive observer opts into the server's read-only full-event mode so
    // it never acquires a control lease, but still carries no participant or
    // task authority (no participantId, instanceId, or capabilities).
    expect(url.searchParams.get("runtimeKind")).toBe("observer");
    expect(url.searchParams.has("participantId")).toBe(false);
    expect(url.searchParams.has("instanceId")).toBe(false);
    expect(url.searchParams.has("capabilities")).toBe(false);
  });

  it("delivers replayed and backlogged events, then resolves replay completion", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }

    socket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    socket.emitServerEvent(createEvent({ eventId: "evt_2", seq: 2 }));

    const observed: string[] = [];
    client.onEvent((event) => {
      observed.push(event.eventId);
    });
    socket.emitServerEvent(createEvent({ eventId: "evt_3", seq: 3 }));
    socket.emitReplayComplete();
    await client.waitForReplayComplete();

    expect(observed).toEqual(["evt_1", "evt_2", "evt_3"]);
    expect(client.debugInfo()).toMatchObject({
      connectCount: 1,
      eventCount: 3,
      lastObservedSeq: 3,
      replayCompleteCount: 1,
    });
  });

  it("routes server errors and handler rejections to onError", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    const errors: string[] = [];
    client.onError((error) => {
      errors.push(error.message);
    });
    client.onEvent(async () => {
      throw new Error("renderer failed");
    });

    socket.emit("message", JSON.stringify({ error: "server rejected stream", op: "error" }));
    socket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    await waitFor(() => errors.includes("renderer failed"));

    expect(errors).toContain("server rejected stream");
    expect(errors.some((message) => message.length > 0)).toBe(true);
    client.close();
  });

  it("preserves typed replay-window details without leaking unknown fields", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 12,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    const replay = client.waitForReplayComplete();
    const errors: Error[] = [];
    client.onError((error) => errors.push(error));

    socket.emit(
      "message",
      JSON.stringify({
        error: "Replay window exceeded",
        limit: 2_000,
        op: "error",
        reason: "replay_window_exceeded",
        secret: "must-not-cross-client-boundary",
      }),
    );

    await expect(replay).rejects.toBeInstanceOf(SessionEventStreamError);
    expect(errors).toEqual([
      expect.objectContaining({
        reason: "replay_window_exceeded",
        safeDetails: { limit: 2_000 },
      }),
    ]);
    expect(JSON.stringify(errors[0])).not.toContain("must-not-cross-client-boundary");
    client.close();
  });

  it("closes an invalid observer stream and replays from the handled cursor", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing first fake socket");
    }
    client.onEvent(() => undefined);
    firstSocket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    await waitFor(() => client.debugInfo().lastObservedSeq === 1);

    firstSocket.emit("message", JSON.stringify({ op: "unsupported" }));
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement fake socket");
    }

    expect(firstSocket.readyState).toBe(WebSocket.CLOSED);
    expect(new URL(replacementSocket.url).searchParams.get("after")).toBe("1");
    client.close();
  });

  it("coalesces caller reconnect with automatic observer recovery", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing first fake socket");
    }
    client.onError(() => {
      void client.reconnect();
    });
    client.onEvent(() => {
      throw new Error("observer handler failed");
    });

    firstSocket.emitServerEvent(createEvent({ eventId: "evt_failed", seq: 1 }));
    await waitFor(() => sockets.length >= 2);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(sockets).toHaveLength(2);
    expect(sockets.filter((socket) => socket.readyState === WebSocket.OPEN)).toHaveLength(1);
    client.close();
  });

  it("stops advancing when a handler rejects and does not deliver later events past it", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 10_000, maxDelayMs: 10_000 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    const errors: string[] = [];
    client.onError((error) => {
      errors.push(error.message);
    });
    const delivered: number[] = [];
    client.onEvent(async (event) => {
      delivered.push(event.seq);
      if (event.seq === 2) {
        throw new Error("poison event 2");
      }
    });

    socket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    socket.emitServerEvent(createEvent({ eventId: "evt_2", seq: 2 }));
    socket.emitServerEvent(createEvent({ eventId: "evt_3", seq: 3 }));

    await waitFor(() => errors.includes("poison event 2"));
    // Give any erroneous later delivery a chance to surface before asserting.
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(delivered).toEqual([1, 2]);
    expect(client.debugInfo().lastObservedSeq).toBe(1);
    client.close();
  });

  it("runs handlers strictly sequentially and in seq order for a socket burst", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    const entered: number[] = [];
    const exited: number[] = [];
    let inFlight = 0;
    let sawOverlap = false;
    client.onEvent(async (event) => {
      inFlight += 1;
      if (inFlight > 1) {
        sawOverlap = true;
      }
      entered.push(event.seq);
      await new Promise((resolve) => setTimeout(resolve, 5));
      exited.push(event.seq);
      inFlight -= 1;
    });

    for (const seq of [1, 2, 3, 4]) {
      socket.emitServerEvent(createEvent({ eventId: `evt_${seq}`, seq }));
    }

    await waitFor(() => exited.length === 4);

    expect(sawOverlap).toBe(false);
    expect(entered).toEqual([1, 2, 3, 4]);
    expect(exited).toEqual([1, 2, 3, 4]);
    expect(client.debugInfo().lastObservedSeq).toBe(4);
    client.close();
  });

  it("replays a backlog to a late handler in seq order under a burst", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }

    for (const seq of [1, 2, 3]) {
      socket.emitServerEvent(createEvent({ eventId: `evt_${seq}`, seq }));
    }

    const observed: number[] = [];
    client.onEvent(async (event) => {
      await new Promise((resolve) => setTimeout(resolve, 2));
      observed.push(event.seq);
    });
    socket.emitServerEvent(createEvent({ eventId: "evt_4", seq: 4 }));
    socket.emitReplayComplete();
    await client.waitForReplayComplete();

    expect(observed).toEqual([1, 2, 3, 4]);
    expect(client.debugInfo().lastObservedSeq).toBe(4);
    client.close();
  });

  it("ignores event frames from a stale connection generation", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing first fake socket");
    }
    const observed: string[] = [];
    client.onEvent((event) => {
      observed.push(event.eventId);
    });

    await client.reconnect();
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement fake socket");
    }
    firstSocket.emitServerEvent(createEvent({ eventId: "evt_stale", seq: 1 }));
    replacementSocket.emitServerEvent(createEvent({ eventId: "evt_current", seq: 1 }));
    replacementSocket.emitReplayComplete();
    await client.waitForReplayComplete();

    expect(observed).toEqual(["evt_current"]);
    expect(client.debugInfo().connectCount).toBe(2);
    client.close();
  });

  it("does not let a stale close or error reject replacement replay", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const firstSocket = sockets[0];
    if (!firstSocket) {
      throw new Error("Missing first fake socket");
    }
    firstSocket.deferClose = true;
    const errors: string[] = [];
    client.onError((error) => {
      errors.push(error.message);
    });
    client.onEvent((event) => {
      if (event.eventId === "evt_failed") {
        throw new Error("observer handler failed");
      }
    });
    let supersededReplaySettled = false;
    const supersededReplay = client.waitForReplayComplete().catch(() => {
      supersededReplaySettled = true;
    });

    firstSocket.emitServerEvent(createEvent({ eventId: "evt_failed", seq: 1 }));
    await waitFor(() => sockets.length === 2);
    await waitFor(() => supersededReplaySettled);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement fake socket");
    }
    const replacementReplay = client.waitForReplayComplete();
    firstSocket.emit("error", new Error("stale socket error"));
    firstSocket.emitClose();
    replacementSocket.emitServerEvent(createEvent({ eventId: "evt_current", seq: 1 }));
    replacementSocket.emitReplayComplete();

    await replacementReplay;
    await supersededReplay;
    expect(errors).toEqual(["observer handler failed"]);
    expect(client.debugInfo()).toMatchObject({ connectCount: 2, lastObservedSeq: 1 });
    client.close();
  });

  it("does not expose task command methods on observer stream instances", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const keys = Object.getOwnPropertyNames(SessionEventStreamClient.prototype);

    expect(keys).not.toContain("claimTask");
    expect(keys).not.toContain("appendEvent");
    expect(keys).not.toContain("completeTask");
    expect(keys).not.toContain("refreshTaskClaim");
    expect("claimTask" in client).toBe(false);
  });
});

/** Minimal in-memory WebSocket implementation for observer client tests. */
class FakeWebSocket extends EventEmitter {
  deferClose = false;
  readyState = WebSocket.CONNECTING;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    queueMicrotask(() => {
      this.readyState = WebSocket.OPEN;
      this.emit("open");
    });
  }

  close(): void {
    if (this.deferClose) {
      this.readyState = WebSocket.CLOSING;
      return;
    }
    this.emitClose();
  }

  emitClose(): void {
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  send(data: string): void {
    this.sent.push(data);
  }

  terminate(): void {
    this.close();
  }

  emitServerEvent(event: SessionEvent): void {
    this.emit("message", JSON.stringify({ event, op: "event" }));
  }

  emitReplayComplete(): void {
    this.emit("message", JSON.stringify({ op: "replay.complete" }));
  }
}

/** Creates a WebSocket factory that records each constructed fake socket. */
function createFakeWebSocketFactory(sockets: FakeWebSocket[]): SessionEventStreamWebSocketFactory {
  return (url) => {
    const socket = new FakeWebSocket(url);
    sockets.push(socket);
    return socket as unknown as WebSocket;
  };
}

/** Builds a durable session event fixture. */
function createEvent(input: { readonly eventId: string; readonly seq: number }): SessionEvent {
  return {
    createdAt: "2026-07-01T00:00:00.000Z",
    eventId: input.eventId,
    payload: {},
    producerId: "observer-test",
    seq: input.seq,
    sessionId: "sess_stream",
    type: "agent.output",
  };
}

/** Waits until a predicate becomes true or fails with a timeout. */
async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > 1_000) {
      throw new Error("Timed out waiting for condition");
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
