import { describe, expect, it } from "vitest";
import type { WebSocket } from "ws";

import { SubscriptionHub } from "../src/hub.js";
import { sessionEventByteLength } from "../src/resource-limits.js";
import type { SessionEvent } from "../src/types.js";

const roomyLimits = {
  wsGapRepairGraceMs: 10_000,
  wsReplayMaxBytes: 10_000_000,
} as const;

describe("SubscriptionHub cursor integrity", () => {
  it("keeps replay and live delivery contiguous when live events arrive during replay", () => {
    const hub = new SubscriptionHub({
      limits: { ...roomyLimits, wsBackpressureBufferedBytes: 10_000, wsReplayMaxEvents: 10 },
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
      limits: { ...roomyLimits, wsBackpressureBufferedBytes: 10_000, wsReplayMaxEvents: 10 },
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

  it("closes a replaying socket with a typed error when the future event buffer exceeds the repair limit", () => {
    const hub = new SubscriptionHub({
      limits: { ...roomyLimits, wsBackpressureBufferedBytes: 10_000, wsReplayMaxEvents: 1 },
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });
    const sessionId = "sess_unrepaired_gap";

    hub.add(sessionId, socket.asWebSocket(), { afterSeq: 0, replaying: true });
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

  it("closes a replaying socket when the future event buffer exceeds the byte budget", () => {
    const bufferedEvent = createSessionEvent(3, "sess_byte_gap");
    const hub = new SubscriptionHub({
      limits: {
        wsBackpressureBufferedBytes: 10_000,
        wsGapRepairGraceMs: 10_000,
        wsReplayMaxBytes: sessionEventByteLength(bufferedEvent),
        wsReplayMaxEvents: 10,
      },
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });

    hub.add("sess_byte_gap", socket.asWebSocket(), { afterSeq: 0, replaying: true });
    hub.broadcast(bufferedEvent);
    hub.broadcast(createSessionEvent(4, "sess_byte_gap"));

    expect(socket.closeCode).toBe(1013);
    expect(JSON.parse(socket.sentMessages[0] ?? "{}")).toMatchObject({
      op: "error",
      reason: "replay_gap_unrepaired",
    });
  });
});

describe("SubscriptionHub live gap repair grace", () => {
  it("drops a saturated live gap buffer instead of closing while repair is pending", () => {
    let now = 1_000;
    const hub = new SubscriptionHub({
      limits: {
        wsBackpressureBufferedBytes: 10_000,
        wsGapRepairGraceMs: 500,
        wsReplayMaxBytes: 10_000_000,
        wsReplayMaxEvents: 2,
      },
      now: () => now,
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });
    const sessionId = "sess_gap_grace";

    hub.add(sessionId, socket.asWebSocket(), 0);
    hub.broadcast(createSessionEvent(3, sessionId));
    hub.broadcast(createSessionEvent(4, sessionId));
    hub.broadcast(createSessionEvent(5, sessionId));

    // The saturated buffer is dropped, not closed: the durable catch-up poll
    // re-reads everything after the contiguous cursor from durable storage.
    expect(socket.closeCode).toBeNull();
    expect(hub.debugInfo()).toMatchObject({
      gapBufferDropCount: 1,
      pendingEventCount: 1,
      socketCount: 1,
    });

    // The catch-up poll repairs the gap from durable storage; delivery resumes
    // contiguously and drains the surviving buffered event.
    now = 1_400;
    for (const seq of [1, 2, 3, 4]) {
      hub.broadcast(createSessionEvent(seq, sessionId));
    }
    expect(readEventSeqs(socket)).toEqual([1, 2, 3, 4, 5]);
    expect(socket.closeCode).toBeNull();
  });

  it("closes a live socket as replay_gap_unrepaired only after the repair grace elapses", () => {
    let now = 1_000;
    const hub = new SubscriptionHub({
      limits: {
        wsBackpressureBufferedBytes: 10_000,
        wsGapRepairGraceMs: 500,
        wsReplayMaxBytes: 10_000_000,
        wsReplayMaxEvents: 2,
      },
      now: () => now,
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });
    const sessionId = "sess_gap_grace_expired";

    hub.add(sessionId, socket.asWebSocket(), 0);
    hub.broadcast(createSessionEvent(3, sessionId));
    hub.broadcast(createSessionEvent(4, sessionId));
    hub.broadcast(createSessionEvent(5, sessionId));
    expect(socket.closeCode).toBeNull();

    // No contiguous progress happens before the grace window elapses, so the
    // next saturation is a genuine repair failure and closes with the typed
    // replay_gap_unrepaired error.
    now = 1_600;
    hub.broadcast(createSessionEvent(6, sessionId));
    hub.broadcast(createSessionEvent(7, sessionId));

    expect(socket.closeCode).toBe(1013);
    expect(JSON.parse(socket.sentMessages[0] ?? "{}")).toMatchObject({
      op: "error",
      reason: "replay_gap_unrepaired",
    });
    expect(hub.debugInfo()).toMatchObject({
      gapBufferDropCount: 1,
      socketCount: 0,
    });
  });
});

describe("SubscriptionHub backpressure", () => {
  it("closes only the slow socket and keeps healthy sockets deliverable", () => {
    const hub = new SubscriptionHub({
      limits: { ...roomyLimits, wsBackpressureBufferedBytes: 10, wsReplayMaxEvents: 10 },
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
      limits: { ...roomyLimits, wsBackpressureBufferedBytes: 10, wsReplayMaxEvents: 10 },
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

  it("counts buffered future-event bytes toward the slow-consumer budget", () => {
    const sessionId = "sess_pending_backpressure";
    const buffered = [createSessionEvent(2, sessionId), createSessionEvent(3, sessionId)];
    const pendingBytes = buffered.reduce(
      (total, event) => total + sessionEventByteLength(event),
      0,
    );
    const hub = new SubscriptionHub({
      limits: {
        ...roomyLimits,
        wsBackpressureBufferedBytes: pendingBytes - 1,
        wsReplayMaxEvents: 10,
      },
    });
    const socket = new FakeSocket({ bufferedAmount: 0 });

    hub.add(sessionId, socket.asWebSocket(), 0);
    for (const event of buffered) {
      hub.broadcast(event);
    }
    // The ws send buffer is empty, but the pending future-event buffer alone
    // exceeds the byte budget, so the next contiguous send closes the socket.
    hub.broadcast(createSessionEvent(1, sessionId));

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
