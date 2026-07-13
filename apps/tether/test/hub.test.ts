import { describe, expect, it } from "vitest";
import type { WebSocket } from "ws";

import { SubscriptionHub } from "../src/hub.js";
import type { SessionEvent } from "../src/types.js";

describe("SubscriptionHub cursor integrity", () => {
  it("keeps replay and live delivery contiguous when live events arrive during replay", () => {
    const hub = new SubscriptionHub({
      limits: { wsBackpressureBufferedBytes: 10_000, wsReplayMaxEvents: 10 },
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });
    const sessionId = "sess_replay_live";

    hub.add(sessionId, socket.asWebSocket(), 0);
    hub.broadcast(createSessionEvent(3, sessionId));

    expect(hub.sessionCursors()).toEqual([{ lastDeliveredSeq: 0, sessionId }]);

    hub.sendReplayEvent(sessionId, socket.asWebSocket(), createSessionEvent(1, sessionId));
    hub.sendReplayEvent(sessionId, socket.asWebSocket(), createSessionEvent(2, sessionId));

    expect(readEventSeqs(socket)).toEqual([1, 2, 3]);
    expect(hub.sessionCursors()).toEqual([{ lastDeliveredSeq: 3, sessionId }]);
  });

  it("buffers future live events and ignores duplicates without moving the contiguous cursor", () => {
    const hub = new SubscriptionHub({
      limits: { wsBackpressureBufferedBytes: 10_000, wsReplayMaxEvents: 10 },
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });
    const sessionId = "sess_future_duplicates";

    hub.add(sessionId, socket.asWebSocket(), 0);
    hub.broadcast(createSessionEvent(2, sessionId));
    hub.broadcast(createSessionEvent(2, sessionId));

    expect(readEventSeqs(socket)).toEqual([]);
    expect(hub.sessionCursors()).toEqual([{ lastDeliveredSeq: 0, sessionId }]);

    hub.broadcast(createSessionEvent(1, sessionId));
    hub.broadcast(createSessionEvent(2, sessionId));

    expect(readEventSeqs(socket)).toEqual([1, 2]);
    expect(hub.debugInfo()).toMatchObject({
      duplicateEventSkipCount: 2,
      outOfOrderLiveEventCount: 1,
    });
  });

  it("closes with a typed error when the future event buffer exceeds the repair limit", () => {
    const hub = new SubscriptionHub({
      limits: { wsBackpressureBufferedBytes: 10_000, wsReplayMaxEvents: 1 },
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });
    const sessionId = "sess_unrepaired_gap";

    hub.add(sessionId, socket.asWebSocket(), 0);
    hub.broadcast(createSessionEvent(3, sessionId));
    hub.broadcast(createSessionEvent(4, sessionId));

    expect(socket.closeCode).toBe(1013);
    expect(JSON.parse(socket.sentMessages[0] ?? "{}")).toMatchObject({
      op: "error",
      reason: "replay_gap_unrepaired",
    });
    expect(hub.debugInfo()).toMatchObject({
      pendingEventCount: 0,
      socketCount: 0,
    });
  });
});

describe("SubscriptionHub backpressure", () => {
  it("closes only the slow socket and keeps healthy sockets deliverable", () => {
    const hub = new SubscriptionHub({
      limits: { wsBackpressureBufferedBytes: 10, wsReplayMaxEvents: 10 },
    });
    const slowSocket = new FakeSocket({ bufferedAmount: 11 });
    const healthySocket = new FakeSocket({ bufferedAmount: 0 });
    const event = createSessionEvent(1);

    hub.add(event.sessionId, slowSocket.asWebSocket(), 0);
    hub.add(event.sessionId, healthySocket.asWebSocket(), 0);
    hub.broadcast(event);

    expect(slowSocket.closeCode).toBe(1013);
    expect(slowSocket.sentMessages).toHaveLength(1);
    expect(JSON.parse(slowSocket.sentMessages[0] ?? "{}")).toMatchObject({
      op: "error",
      reason: "backpressure",
    });
    expect(healthySocket.sentMessages).toHaveLength(1);
    expect(JSON.parse(healthySocket.sentMessages[0] ?? "{}")).toMatchObject({
      event: { seq: 1 },
      op: "event",
    });
    expect(hub.debugInfo()).toMatchObject({
      backpressureCloseCount: 1,
      socketCount: 1,
    });
  });

  it("uses the same backpressure check for replay sends", () => {
    const hub = new SubscriptionHub({
      limits: { wsBackpressureBufferedBytes: 10, wsReplayMaxEvents: 10 },
    });
    const socket = new FakeSocket({ bufferedAmount: 11 });
    const event = createSessionEvent(1);

    hub.add(event.sessionId, socket.asWebSocket(), 0);
    hub.sendReplayEvent(event.sessionId, socket.asWebSocket(), event);

    expect(socket.closeCode).toBe(1013);
    expect(JSON.parse(socket.sentMessages[0] ?? "{}")).toMatchObject({
      op: "error",
      reason: "backpressure",
    });
    expect(hub.debugInfo()).toMatchObject({
      backpressureCloseCount: 1,
      socketCount: 0,
    });
  });
});

class FakeSocket {
  readonly OPEN = 1;
  readonly sentMessages: string[] = [];
  bufferedAmount: number;
  closeCode: number | null = null;
  readyState = this.OPEN;
  private readonly closeListeners: Array<() => void> = [];

  constructor(input: { readonly bufferedAmount: number }) {
    this.bufferedAmount = input.bufferedAmount;
  }

  asWebSocket(): WebSocket {
    return this as unknown as WebSocket;
  }

  close(code?: number): void {
    this.closeCode = code ?? null;
    this.readyState = 3;
    for (const listener of this.closeListeners) {
      listener();
    }
  }

  on(eventName: string, listener: () => void): this {
    if (eventName === "close") {
      this.closeListeners.push(listener);
    }
    return this;
  }

  send(message: string): void {
    this.sentMessages.push(message);
  }
}

function createSessionEvent(seq: number, sessionId = "sess_backpressure"): SessionEvent {
  return {
    createdAt: "2026-05-21T00:00:00.000Z",
    eventId: `evt_${seq}`,
    payload: { text: "hello" },
    producerId: "test",
    seq,
    sessionId,
    type: "user.message",
  };
}

function readEventSeqs(socket: FakeSocket): number[] {
  return socket.sentMessages
    .map((message) => JSON.parse(message) as unknown)
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
