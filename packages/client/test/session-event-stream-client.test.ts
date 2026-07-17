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

  it("closes the stream on a malformed known-op frame and replays from the handled cursor", async () => {
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

    firstSocket.emit("message", JSON.stringify({ op: "event" }));
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement fake socket");
    }

    expect(firstSocket.readyState).toBe(WebSocket.CLOSED);
    expect(new URL(replacementSocket.url).searchParams.get("after")).toBe("1");
    client.close();
  });

  it("skips unknown-op server frames without recovery or event loss", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    const errors: Error[] = [];
    client.onError((error) => {
      errors.push(error);
    });
    const observed: number[] = [];
    client.onEvent((event) => {
      observed.push(event.seq);
    });

    socket.emit("message", JSON.stringify({ op: "presence.v2", payload: { future: true } }));
    socket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    socket.emitReplayComplete();
    await client.waitForReplayComplete();
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(observed).toEqual([1]);
    expect(errors).toEqual([]);
    expect(sockets).toHaveLength(1);
    expect(client.debugInfo().pausedReason).toBeNull();
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

  it("automatically reconnects after an unexpected close and resumes from the handled cursor", async () => {
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
    const observed: number[] = [];
    client.onEvent((event) => {
      observed.push(event.seq);
    });
    firstSocket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    await waitFor(() => client.debugInfo().lastObservedSeq === 1);

    firstSocket.emitClose();
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement fake socket");
    }
    expect(new URL(replacementSocket.url).searchParams.get("after")).toBe("1");
    replacementSocket.emitServerEvent(createEvent({ eventId: "evt_2", seq: 2 }));
    replacementSocket.emitReplayComplete();
    await client.waitForReplayComplete();

    expect(observed).toEqual([1, 2]);
    expect(client.debugInfo()).toMatchObject({
      connectCount: 2,
      lastObservedSeq: 2,
      reconnectSuccessCount: 1,
    });
    client.close();
  });

  it("waits for an in-flight handler to settle before reconnecting after a close", async () => {
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
    let releaseHandler: (() => void) | undefined;
    const handlerBlocked = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    const entered: number[] = [];
    client.onEvent(async (event) => {
      entered.push(event.seq);
      await handlerBlocked;
    });
    firstSocket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    await waitFor(() => entered.length === 1);

    firstSocket.emitClose();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sockets).toHaveLength(1);

    releaseHandler?.();
    await waitFor(() => sockets.length === 2);
    const replacementSocket = sockets[1];
    if (!replacementSocket) {
      throw new Error("Missing replacement fake socket");
    }
    expect(new URL(replacementSocket.url).searchParams.get("after")).toBe("1");
    client.close();
  });

  it("leaves no background reconnect activity after a failed initial connect", async () => {
    const sockets: FakeWebSocket[] = [];
    const factory: SessionEventStreamWebSocketFactory = (url) => {
      const socket = new FakeWebSocket(url);
      socket.failOpen = true;
      sockets.push(socket);
      return socket as unknown as WebSocket;
    };

    await expect(
      SessionEventStreamClient.connect({
        afterSeq: 0,
        reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
        serviceUrl: "http://tether.test",
        sessionId: "sess_stream",
        webSocketFactory: factory,
      }),
    ).rejects.toBeTruthy();

    expect(sockets).toHaveLength(1);
    // A failed connect() rejects to a caller holding no client reference. The
    // close that follows the failed open must not spawn a zombie reconnect
    // loop, so no further open attempts may occur.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(sockets).toHaveLength(1);
  });

  it("does not stack a second reconnect when reconnect() runs during pending settlement", async () => {
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
    let releaseHandler: (() => void) | undefined;
    const handlerBlocked = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    const entered: number[] = [];
    client.onEvent(async (event) => {
      entered.push(event.seq);
      await handlerBlocked;
    });

    firstSocket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    await waitFor(() => entered.length === 1);

    // Unexpected close of the established gen-1 socket while its handler is
    // still in flight: this schedules a reconnect gated on delivery settlement.
    firstSocket.emitClose();
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(sockets).toHaveLength(1);

    // A concurrent manual reconnect() opens gen 2 before settlement resolves.
    await client.reconnect();
    expect(sockets).toHaveLength(2);
    const secondSocket = sockets[1];
    if (!secondSocket) {
      throw new Error("Missing replacement fake socket");
    }

    // Releasing the blocked gen-1 handler resolves the stale settlement waiter.
    // Its continuation must observe the newer generation and do nothing rather
    // than opening gen 3 and orphaning the live gen-2 socket.
    releaseHandler?.();
    await new Promise((resolve) => setTimeout(resolve, 30));

    expect(sockets).toHaveLength(2);
    expect(secondSocket.readyState).toBe(WebSocket.OPEN);
    expect(client.debugInfo().connectCount).toBe(2);
    client.close();
  });

  it("does not reconnect after an intentional close", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });

    client.close();
    await client.waitForClose();
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(sockets).toHaveLength(1);
    expect(client.debugInfo().stopped).toBe(true);
  });

  it("rejects reconnect on an intentionally closed observer with a typed error", async () => {
    const sockets: FakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });

    client.close();

    await expect(client.reconnect()).rejects.toBeInstanceOf(SessionEventStreamError);
    expect(sockets).toHaveLength(1);
  });

  it("explicit reconnect clears observer Paused State after handler remediation", async () => {
    const sockets: FakeWebSocket[] = [];
    const errors: Error[] = [];
    let failDelivery = true;
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    client.onError((error) => {
      errors.push(error);
    });
    const observed: number[] = [];
    client.onEvent((event) => {
      observed.push(event.seq);
      if (failDelivery) {
        throw new Error("observer handler requires remediation");
      }
    });

    for (let attempt = 1; attempt <= 5; attempt += 1) {
      const socket = sockets[attempt - 1];
      if (!socket) {
        throw new Error(`Missing observer socket for attempt ${attempt}`);
      }
      socket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
      if (attempt < 5) {
        await waitFor(() => sockets.length === attempt + 1);
      } else {
        await waitFor(() => client.debugInfo().pausedReason !== null);
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(sockets).toHaveLength(5);
    expect(client.debugInfo()).toMatchObject({
      pausedReason: "delivery_recovery_exhausted",
      stopped: false,
    });

    failDelivery = false;
    await client.reconnect();
    const remediatedSocket = sockets[5];
    if (!remediatedSocket) {
      throw new Error("Missing remediated fake socket");
    }
    const replay = client.waitForReplayComplete();
    remediatedSocket.emitServerEvent(createEvent({ eventId: "evt_1", seq: 1 }));
    remediatedSocket.emitReplayComplete();
    await replay;

    expect(client.debugInfo()).toMatchObject({
      lastObservedSeq: 1,
      pausedReason: null,
    });
    client.close();
  });

  it("pauses on a typed terminal replay reason instead of reconnect-looping", async () => {
    const sockets: FakeWebSocket[] = [];
    const errors: Error[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 12,
      reconnect: { baseDelayMs: 0, maxDelayMs: 0 },
      serviceUrl: "http://tether.test",
      sessionId: "sess_stream",
      webSocketFactory: createFakeWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    client.onError((error) => {
      errors.push(error);
    });
    const replay = client.waitForReplayComplete();

    socket.emit(
      "message",
      JSON.stringify({
        error: "Replay window exceeded",
        limit: 2_000,
        op: "error",
        reason: "replay_window_exceeded",
      }),
    );
    await expect(replay).rejects.toBeInstanceOf(SessionEventStreamError);
    socket.emitClose();
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(sockets).toHaveLength(1);
    expect(client.debugInfo().pausedReason).toBe("replay_window_exceeded");

    await client.reconnect();

    expect(sockets).toHaveLength(2);
    expect(client.debugInfo().pausedReason).toBeNull();
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
  failOpen = false;
  readyState = WebSocket.CONNECTING;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    queueMicrotask(() => {
      if (this.failOpen) {
        // Model a server that is down at connect time: the socket surfaces an
        // error and closes without ever establishing.
        this.readyState = WebSocket.CLOSED;
        this.emit("error", new Error("connection refused"));
        this.emit("close");
        return;
      }
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
