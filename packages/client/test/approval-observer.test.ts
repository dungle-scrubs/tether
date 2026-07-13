import { describe, expect, it } from "vitest";
import { EventEmitter } from "node:events";
import WebSocket from "ws";

import {
  observeTaskApprovals,
  SessionEventStreamClient,
  TaskApprovalProcessingError,
  type TaskApprovalObserverClient,
  type SessionEventStreamWebSocketFactory,
} from "../src/index.js";
import type { SessionEvent } from "../src/types.js";

describe("observeTaskApprovals", () => {
  it("buffers approvals until replay completes and skips previously processed approvals", async () => {
    const fixture = createApprovalObserverFixture();
    const processed: string[] = [];

    observeTaskApprovals({
      approvalId: (approval) => approval.id,
      client: fixture.client,
      parseApproval: (event) => (event.type === "approval.recorded" ? { id: event.eventId } : null),
      processApproval: async (approval) => {
        processed.push(approval.id);
      },
      processedApprovalIdFromEvent: (event) =>
        event.type === "agent.output" ? readStringField(event.payload, "approvalId") : null,
    });

    fixture.emit(createEvent("approval.recorded", "evt_seen"));
    fixture.emit(createEvent("agent.output", "evt_output", { approvalId: "evt_seen" }));
    fixture.emit(createEvent("approval.recorded", "evt_new"));

    expect(processed).toEqual([]);
    fixture.replayComplete.resolve();
    await waitFor(() => processed.length === 1);

    expect(processed).toEqual(["evt_new"]);
  });

  it("accepts SessionEventStreamClient and preserves replay suppression", async () => {
    const sockets: ApprovalFakeWebSocket[] = [];
    const client = await SessionEventStreamClient.connect({
      afterSeq: 0,
      serviceUrl: "http://tether.test",
      sessionId: "sess_approval",
      webSocketFactory: createApprovalWebSocketFactory(sockets),
    });
    const socket = sockets[0];
    if (!socket) {
      throw new Error("Missing fake socket");
    }
    const processed: string[] = [];

    observeTaskApprovals({
      approvalId: (approval) => approval.id,
      client,
      parseApproval: parseTestApproval,
      processApproval: async (approval) => {
        processed.push(approval.id);
      },
      processedApprovalIdFromEvent,
    });

    socket.emitServerEvent(createEvent("approval.recorded", "evt_seen"));
    socket.emitServerEvent(createEvent("agent.output", "evt_output", { approvalId: "evt_seen" }));
    socket.emitServerEvent(createEvent("approval.recorded", "evt_new"));
    expect(processed).toEqual([]);

    socket.emitReplayComplete();
    await waitFor(() => processed.length === 1);
    socket.emitServerEvent(createEvent("approval.recorded", "evt_live"));
    await waitFor(() => processed.length === 2);

    expect(processed).toEqual(["evt_new", "evt_live"]);
  });

  it("keeps a rejected replayed approval retryable for a later duplicate delivery", async () => {
    const fixture = createApprovalObserverFixture();
    const failure = new Error("append failed");
    const errors: unknown[] = [];
    const attempts: string[] = [];

    const observer = observeTaskApprovals({
      approvalId: (approval) => approval.id,
      client: fixture.client,
      onError: (error) => errors.push(error),
      parseApproval: parseTestApproval,
      processApproval: async (approval) => {
        attempts.push(approval.id);
        if (attempts.length === 1) {
          throw failure;
        }
      },
      processedApprovalIdFromEvent,
    });

    fixture.emit(createEvent("approval.recorded", "evt_retry"));
    fixture.replayComplete.resolve();
    await waitFor(() => errors.length === 1);

    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(TaskApprovalProcessingError);
    expect(errors[0]).toMatchObject({
      approvalId: "evt_retry",
      cause: failure,
      operation: "processApproval",
    });
    expect(observer.debugInfo()).toMatchObject({
      activeProcessingCount: 0,
      inFlightApprovalIds: [],
      lastError: { approvalId: "evt_retry", message: "append failed" },
      processedApprovalIds: [],
    });

    fixture.emit(createEvent("approval.recorded", "evt_retry"));
    await waitFor(() => attempts.length === 2);
    await waitFor(() => observer.debugInfo().processedApprovalIds.length === 1);

    expect(attempts).toEqual(["evt_retry", "evt_retry"]);
    expect(observer.debugInfo().processedApprovalIds).toEqual(["evt_retry"]);
  });

  it("keeps a rejected live approval retryable for a later duplicate delivery", async () => {
    const fixture = createApprovalObserverFixture();
    const attempts: string[] = [];

    observeTaskApprovals({
      approvalId: (approval) => approval.id,
      client: fixture.client,
      onError: () => undefined,
      parseApproval: parseTestApproval,
      processApproval: async (approval) => {
        attempts.push(approval.id);
        if (attempts.length === 1) {
          throw new Error("live append failed");
        }
      },
      processedApprovalIdFromEvent,
    });

    fixture.replayComplete.resolve();
    await Promise.resolve();
    fixture.emit(createEvent("approval.recorded", "evt_live_retry"));
    await waitFor(() => attempts.length === 1);

    fixture.emit(createEvent("approval.recorded", "evt_live_retry"));
    await waitFor(() => attempts.length === 2);

    expect(attempts).toEqual(["evt_live_retry", "evt_live_retry"]);
  });

  it("suppresses duplicate deliveries while processing is in flight", async () => {
    const fixture = createApprovalObserverFixture();
    const activeAttempt = createDeferred<void>();
    const attempts: string[] = [];
    const observer = observeTaskApprovals({
      approvalId: (approval) => approval.id,
      client: fixture.client,
      parseApproval: parseTestApproval,
      processApproval: async (approval) => {
        attempts.push(approval.id);
        await activeAttempt.promise;
      },
      processedApprovalIdFromEvent,
    });

    fixture.replayComplete.resolve();
    await Promise.resolve();
    fixture.emit(createEvent("approval.recorded", "evt_in_flight"));
    fixture.emit(createEvent("approval.recorded", "evt_in_flight"));
    await waitFor(() => attempts.length === 1);

    expect(observer.debugInfo()).toMatchObject({
      activeProcessingCount: 1,
      inFlightApprovalIds: ["evt_in_flight"],
      processedApprovalIds: [],
    });

    activeAttempt.resolve();
    await waitFor(() => observer.debugInfo().activeProcessingCount === 0);
    fixture.emit(createEvent("approval.recorded", "evt_in_flight"));
    await Promise.resolve();

    expect(attempts).toEqual(["evt_in_flight"]);
    expect(observer.debugInfo().processedApprovalIds).toEqual(["evt_in_flight"]);
  });

  it("clears in-flight state after a rejected pending attempt", async () => {
    const fixture = createApprovalObserverFixture();
    const firstAttempt = createDeferred<void>();
    const errors: unknown[] = [];
    const attempts: string[] = [];

    observeTaskApprovals({
      approvalId: (approval) => approval.id,
      client: fixture.client,
      onError: (error) => errors.push(error),
      parseApproval: parseTestApproval,
      processApproval: async (approval) => {
        attempts.push(approval.id);
        if (attempts.length === 1) {
          await firstAttempt.promise;
          throw new Error("pending failed");
        }
      },
      processedApprovalIdFromEvent,
    });

    fixture.replayComplete.resolve();
    await Promise.resolve();
    fixture.emit(createEvent("approval.recorded", "evt_pending_retry"));
    fixture.emit(createEvent("approval.recorded", "evt_pending_retry"));
    await waitFor(() => attempts.length === 1);

    firstAttempt.resolve();
    await waitFor(() => errors.length === 1);
    fixture.emit(createEvent("approval.recorded", "evt_pending_retry"));
    await waitFor(() => attempts.length === 2);

    expect(attempts).toEqual(["evt_pending_retry", "evt_pending_retry"]);
  });
});

/** Creates a fake approval observer client plus event delivery helpers. */
function createApprovalObserverFixture(): {
  readonly client: TaskApprovalObserverClient;
  readonly emit: (event: SessionEvent) => void;
  readonly replayComplete: ReturnType<typeof createDeferred<void>>;
} {
  const replayComplete = createDeferred<void>();
  const handlers = new Set<(event: SessionEvent) => void>();
  const client: TaskApprovalObserverClient = {
    onEvent: (handler) => {
      handlers.add(handler);
      return () => handlers.delete(handler);
    },
    waitForReplayComplete: async () => replayComplete.promise,
  };
  return {
    client,
    emit: (event) => emit(handlers, event),
    replayComplete,
  };
}

/** Minimal WebSocket used to drive a real SessionEventStreamClient in observer tests. */
class ApprovalFakeWebSocket extends EventEmitter {
  readyState = WebSocket.CONNECTING;

  constructor() {
    super();
    queueMicrotask(() => {
      this.readyState = WebSocket.OPEN;
      this.emit("open");
    });
  }

  close(): void {
    this.readyState = WebSocket.CLOSED;
    this.emit("close");
  }

  send(): void {}

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

/** Creates fake sockets for SessionEventStreamClient approval tests. */
function createApprovalWebSocketFactory(
  sockets: ApprovalFakeWebSocket[],
): SessionEventStreamWebSocketFactory {
  return () => {
    const socket = new ApprovalFakeWebSocket();
    sockets.push(socket);
    return socket as unknown as WebSocket;
  };
}

/** Parses a simple approval fixture event. */
function parseTestApproval(event: SessionEvent): { readonly id: string } | null {
  return event.type === "approval.recorded" ? { id: event.eventId } : null;
}

/** Extracts simple processed approval markers from test output events. */
function processedApprovalIdFromEvent(event: SessionEvent): string | null {
  return event.type === "agent.output" ? readStringField(event.payload, "approvalId") : null;
}

/** Emits one event to all registered handlers. */
function emit(handlers: ReadonlySet<(event: SessionEvent) => void>, event: SessionEvent): void {
  for (const handler of handlers) {
    handler(event);
  }
}

/** Builds a small session event fixture. */
function createEvent(
  type: string,
  eventId: string,
  payload: Record<string, unknown> = {},
): SessionEvent {
  return {
    createdAt: "2026-05-29T00:00:00.000Z",
    eventId,
    payload,
    producerId: "part_test",
    seq: 1,
    sessionId: "sess_test",
    type,
  };
}

/** Reads one string field from an unknown payload object. */
function readStringField(payload: Record<string, unknown>, field: string): string | null {
  const value = payload[field];
  return typeof value === "string" && value.length > 0 ? value : null;
}

/** Creates a manually controlled promise. */
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

/** Waits for one synchronous condition. */
async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 1_000) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("Timed out waiting for condition");
}
