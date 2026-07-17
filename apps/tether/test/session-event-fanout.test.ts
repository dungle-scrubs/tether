import { EventEmitter } from "node:events";

import { afterEach, describe, expect, it, vi } from "vitest";
import { Effect } from "effect";

import { SessionEventFanout } from "../src/session-event-fanout.js";
import { sessionEventNotificationChannel, type DatabasePool } from "../src/db.js";
import type { SubscriptionHub } from "../src/hub.js";
import type { StructuredLogEntry } from "../src/observability.js";
import type { SessionEvent } from "../src/types.js";

describe("SessionEventFanout", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("waits for an in-flight catch-up poll before stop resolves", async () => {
    vi.useFakeTimers();
    let now = 100;
    const catchUp = createDeferred<SessionEvent[]>();
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 10,
      database: createDatabasePoolFixture(),
      eventBatchLimit: 1,
      hub: createHubFixture(),
      listenEnabled: false,
      now: () => now,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: () => Effect.promise(() => catchUp.promise),
      },
    });

    await fanout.start();
    await vi.advanceTimersByTimeAsync(10);
    now = 160;
    expect(fanout.debugInfo().sessionLag).toMatchObject([
      { lagAgeMs: 60, outcome: "events_pending" },
    ]);
    const stopped = fanout.stop();
    let resolved = false;
    stopped.then(() => {
      resolved = true;
    });
    await flushPromises();

    expect(resolved).toBe(false);
    catchUp.resolve([]);
    await stopped;

    expect(resolved).toBe(true);
    expect(fanout.debugInfo().scheduled).toBe(false);
  });

  it("catches up durable events in configured batches", async () => {
    vi.useFakeTimers();
    const calls: Array<{
      readonly afterSeq: number;
      readonly limit: number | undefined;
    }> = [];
    const broadcasted: number[] = [];
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 10,
      database: createDatabasePoolFixture(),
      eventBatchLimit: 2,
      hub: createHubFixture((event) => {
        broadcasted.push(event.seq);
      }),
      listenEnabled: false,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: (_sessionId, afterSeq, options) =>
          Effect.succeed(eventsAfter(afterSeq, options?.limit ?? 0)),
      },
    });

    await fanout.start();
    await vi.advanceTimersByTimeAsync(20);
    await flushPromises();
    await fanout.stop();

    expect(calls).toEqual([
      { afterSeq: 0, limit: 2 },
      { afterSeq: 2, limit: 2 },
    ]);
    expect(broadcasted).toEqual([1, 2, 3]);
    expect(fanout.debugInfo()).toMatchObject({
      catchUpBatchCount: 2,
      catchUpEventCount: 3,
    });

    function eventsAfter(afterSeq: number, limit: number): SessionEvent[] {
      calls.push({ afterSeq, limit });
      return [createSessionEvent(1), createSessionEvent(2), createSessionEvent(3)]
        .filter((event) => event.seq > afterSeq)
        .slice(0, limit);
    }
  });

  it("gives every subscribed session one bounded batch and rotates the next round", async () => {
    vi.useFakeTimers();
    const calls: string[] = [];
    const broadcasted: string[] = [];
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 10,
      database: createDatabasePoolFixture(),
      eventBatchLimit: 1,
      hub: createHubFixture(
        (event) => {
          broadcasted.push(`${event.sessionId}:${event.seq}`);
        },
        [
          { lastDeliveredSeq: 0, sessionId: "sess_hot" },
          { lastDeliveredSeq: 0, sessionId: "sess_later" },
        ],
      ),
      listenEnabled: false,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: (sessionId, afterSeq) => {
          calls.push(sessionId);
          return Effect.succeed([createSessionEvent(afterSeq + 1, sessionId)]);
        },
      },
    });

    await fanout.start();
    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();

    expect(calls).toEqual(["sess_hot", "sess_later"]);
    expect(broadcasted).toEqual(["sess_hot:1", "sess_later:1"]);

    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();
    await fanout.stop();

    expect(calls).toEqual(["sess_hot", "sess_later", "sess_later", "sess_hot"]);
    expect(fanout.debugInfo().catchUpBatchCount).toBe(4);
  });

  it("reports bounded hashed lag, failure recovery, and payload-free batch traces", async () => {
    vi.useFakeTimers();
    let now = 100;
    let attempt = 0;
    const logs: StructuredLogEntry[] = [];
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 10,
      database: createDatabasePoolFixture(),
      eventBatchLimit: 1,
      hub: createHubFixture(),
      listenEnabled: false,
      now: () => now,
      observability: {
        boundaryLogsEnabled: true,
        logger: { log: (entry) => logs.push(entry) },
      },
      service: {
        debugInfo: () => ({ eventSourceId: "src_trace" }),
        listEvents: () => {
          attempt += 1;
          if (attempt === 1) {
            return Effect.fail(new Error("temporary database failure"));
          }
          return Effect.succeed(attempt === 2 ? [createSessionEvent(1)] : []);
        },
      },
    });
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    await fanout.start();
    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();
    now = 160;

    expect(fanout.debugInfo()).toMatchObject({
      catchUpFailureCount: 1,
      lastCatchUpOutcome: "failed",
      sessionLag: [
        {
          lagAgeMs: 60,
          outcome: "failed",
        },
      ],
    });

    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();

    expect(fanout.debugInfo()).toMatchObject({
      catchUpRecoveryCount: 1,
      lastCatchUpOutcome: "events_pending",
      sessionLag: [{ outcome: "events_pending" }],
    });

    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();
    await fanout.stop();

    expect(fanout.debugInfo()).toMatchObject({
      catchUpRecoveryCount: 1,
      lastCatchUpOutcome: "caught_up",
      sessionLag: [{ lagAgeMs: 0, outcome: "caught_up" }],
    });
    const exitLogs = logs.filter(
      (entry) => entry.message === "boundary.exit" && entry.operation === "catchUpBatch",
    );
    const exitLog = exitLogs[exitLogs.length - 1];
    expect(exitLog?.data).toMatchObject({
      batchSize: 0,
      durationMs: expect.any(Number),
      outcome: "caught_up",
      sequenceEnd: 1,
      sequenceStart: 1,
    });
    expect(JSON.stringify(logs)).not.toContain("payload");
    expect(JSON.stringify(logs)).not.toContain("sess_fanout");
  });

  it("does not discard a lower catch-up event after a higher event was broadcast", async () => {
    const broadcasted: number[] = [];
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 10,
      database: createDatabasePoolFixture(),
      eventBatchLimit: 1,
      hub: createHubFixture((event) => {
        broadcasted.push(event.seq);
      }),
      listenEnabled: false,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: () => Effect.succeed([]),
      },
    });
    const fanoutInternals = fanout as unknown as {
      readonly broadcastEvent: (event: SessionEvent) => void;
    };

    fanoutInternals.broadcastEvent(createSessionEvent(7));
    fanoutInternals.broadcastEvent(createSessionEvent(6));

    expect(broadcasted).toEqual([7, 6]);
  });

  it("marks the listener down, records diagnostics, and reconnects after errors", async () => {
    vi.useFakeTimers();
    const firstClient = new FakeListenClient();
    const secondClient = new FakeListenClient();
    const database = createListenDatabaseFixture([firstClient, secondClient]);
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database,
      hub: createHubFixture(),
      reconnectBaseDelayMs: 5,
      reconnectMaxDelayMs: 5,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: () => Effect.succeed([]),
      },
    });
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    await fanout.start();
    firstClient.emitError(new Error("listener failed"));

    expect(fanout.debugInfo()).toMatchObject({
      connected: false,
      listenerErrorCount: 1,
      listenerState: "disconnected",
      lastListenerError: { message: "listener failed", name: "Error" },
    });
    expect(firstClient.releaseCount).toBe(1);
    expect(firstClient.detachedEvents()).toEqual(["end", "error", "notification"]);

    await vi.advanceTimersByTimeAsync(5);

    expect(secondClient.queries).toEqual([`LISTEN ${sessionEventNotificationChannel}`]);
    expect(fanout.debugInfo()).toMatchObject({
      connected: true,
      listenerState: "connected",
      reconnectAttemptCount: 1,
      reconnectSuccessCount: 1,
    });

    await fanout.stop();
  });

  it("treats unexpected listener end as a reconnectable disconnect", async () => {
    vi.useFakeTimers();
    const firstClient = new FakeListenClient();
    const secondClient = new FakeListenClient();
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([firstClient, secondClient]),
      hub: createHubFixture(),
      reconnectBaseDelayMs: 5,
      reconnectMaxDelayMs: 5,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: () => Effect.succeed([]),
      },
    });

    await fanout.start();
    firstClient.emitEnd();

    expect(fanout.debugInfo()).toMatchObject({
      connected: false,
      listenerState: "disconnected",
    });
    expect(firstClient.releaseCount).toBe(1);

    await vi.advanceTimersByTimeAsync(5);

    expect(secondClient.queries).toEqual([`LISTEN ${sessionEventNotificationChannel}`]);
    expect(fanout.debugInfo().connected).toBe(true);

    await fanout.stop();
  });

  it("detaches and releases once when a listener emits error and end", async () => {
    vi.useFakeTimers();
    const client = new FakeListenClient();
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([client]),
      hub: createHubFixture(),
      reconnectBaseDelayMs: 100,
      reconnectMaxDelayMs: 100,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: () => Effect.succeed([]),
      },
    });
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    await fanout.start();
    client.emitError(new Error("listener failed"));
    client.emitEnd();

    expect(client.releaseCount).toBe(1);
    expect(client.detachedEvents()).toEqual(["end", "error", "notification"]);
    expect(fanout.debugInfo().connected).toBe(false);

    await fanout.stop();
  });

  it("cancels pending reconnect work during stop", async () => {
    vi.useFakeTimers();
    const client = new FakeListenClient();
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([client]),
      hub: createHubFixture(),
      reconnectBaseDelayMs: 100,
      reconnectMaxDelayMs: 100,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: () => Effect.succeed([]),
      },
    });
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    await fanout.start();
    client.emitError(new Error("listener failed"));
    await fanout.stop();
    await vi.advanceTimersByTimeAsync(100);

    expect(client.releaseCount).toBe(1);
    expect(fanout.debugInfo()).toMatchObject({
      connected: false,
      listenerState: "stopped",
      reconnectAttemptCount: 0,
    });
  });

  it("keeps catch-up polling usable while listener reconnect is pending", async () => {
    vi.useFakeTimers();
    const client = new FakeListenClient();
    const broadcasted: number[] = [];
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 10,
      database: createListenDatabaseFixture([client]),
      eventBatchLimit: 2,
      hub: createHubFixture((event) => {
        broadcasted.push(event.seq);
      }),
      reconnectBaseDelayMs: 100,
      reconnectMaxDelayMs: 100,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents: (_sessionId, afterSeq, options) =>
          Effect.succeed(
            [createSessionEvent(1)]
              .filter((event) => event.seq > afterSeq)
              .slice(0, options?.limit),
          ),
      },
    });
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    await fanout.start();
    client.emitError(new Error("listener failed"));
    await vi.advanceTimersByTimeAsync(10);
    await flushPromises();

    expect(broadcasted).toEqual([1]);
    expect(fanout.debugInfo()).toMatchObject({
      connected: false,
      scheduled: true,
    });

    await fanout.stop();
  });

  it("does not fetch notified events without a local socket", async () => {
    const client = new FakeListenClient();
    const listEvents = vi.fn(() => Effect.succeed([]));
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([client]),
      hub: createHubFixture(undefined, []),
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents,
      },
    });

    await fanout.start();
    client.emitNotification({
      channel: sessionEventNotificationChannel,
      payload: JSON.stringify({
        eventId: "evt_remote",
        seq: 1,
        sessionId: "sess_missing",
        sourceId: "src_remote",
      }),
    });
    await flushPromises();

    expect(listEvents).not.toHaveBeenCalled();

    await fanout.stop();
  });

  it("fetches one notified event and broadcasts it when a local socket exists", async () => {
    const client = new FakeListenClient();
    const broadcasted: number[] = [];
    const listEvents = vi.fn(
      (sessionId: string, afterSeq: number, options?: { readonly limit?: number | undefined }) =>
        Effect.succeed([createSessionEvent(afterSeq + 1, sessionId)].slice(0, options?.limit)),
    );
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([client]),
      eventBatchLimit: 5,
      hub: createHubFixture((event) => {
        broadcasted.push(event.seq);
      }),
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents,
      },
    });

    await fanout.start();
    client.emitNotification({
      channel: sessionEventNotificationChannel,
      payload: JSON.stringify({
        eventId: "evt_remote",
        seq: 1,
        sessionId: "sess_fanout",
        sourceId: "src_remote",
      }),
    });
    await flushPromises();
    await flushPromises();

    expect(listEvents).toHaveBeenCalledWith("sess_fanout", 0, { limit: 5 });
    expect(broadcasted).toEqual([1]);
    expect(fanout.debugInfo()).toMatchObject({
      fanoutCursorSessionCount: 0,
      sessionCursorCount: 1,
    });

    await fanout.stop();
  });

  it("coalesces bursts of notifications into one pending marker per session", async () => {
    const client = new FakeListenClient();
    const broadcasted: number[] = [];
    const firstFetch = createDeferred<SessionEvent[]>();
    let call = 0;
    const listEvents = vi.fn((_sessionId: string, afterSeq: number) => {
      call += 1;
      if (call === 1) {
        return Effect.promise(() => firstFetch.promise);
      }
      return Effect.succeed([
        createSessionEvent(afterSeq + 1),
        createSessionEvent(afterSeq + 2),
        createSessionEvent(afterSeq + 3),
      ]);
    });
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([client]),
      eventBatchLimit: 10,
      hub: createHubFixture((event) => {
        broadcasted.push(event.seq);
      }),
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents,
      },
    });

    await fanout.start();
    const notify = (seq: number): void => {
      client.emitNotification({
        channel: sessionEventNotificationChannel,
        payload: JSON.stringify({
          eventId: `evt_remote_${seq}`,
          seq,
          sessionId: "sess_fanout",
          sourceId: "src_remote",
        }),
      });
    };
    // The first notification opens a pending marker whose fetch stays in
    // flight while a burst of further notifications arrives.
    notify(1);
    await flushPromises();
    notify(2);
    notify(3);
    notify(4);
    await flushPromises();

    // Notification 2 opens the one follow-up marker; 3 and 4 coalesce onto it
    // instead of appending chain links or DB roundtrips.
    expect(fanout.debugInfo()).toMatchObject({
      coalescedNotificationCount: 2,
      droppedNotificationCount: 0,
    });
    firstFetch.resolve([createSessionEvent(1)]);
    await flushPromises();
    await flushPromises();
    await flushPromises();
    await fanout.stop();

    // Two DB roundtrips serve four notifications, and the coalesced batch
    // fetch delivers the burst's events contiguously.
    expect(listEvents).toHaveBeenCalledTimes(2);
    expect(broadcasted).toEqual([1, 2, 3, 4]);
  });

  it("drops notifications with a diagnostic when the pending queue is saturated", async () => {
    const client = new FakeListenClient();
    const fetches: Array<ReturnType<typeof createDeferred<SessionEvent[]>>> = [];
    const listEvents = vi.fn(() => {
      const deferred = createDeferred<SessionEvent[]>();
      fetches.push(deferred);
      return Effect.promise(() => deferred.promise);
    });
    const fanout = new SessionEventFanout({
      catchUpPollIntervalMs: 0,
      database: createListenDatabaseFixture([client]),
      hub: createHubFixture(undefined, [
        { lastDeliveredSeq: 0, sessionId: "sess_a" },
        { lastDeliveredSeq: 0, sessionId: "sess_b" },
        { lastDeliveredSeq: 0, sessionId: "sess_c" },
      ]),
      notificationQueueLimit: 2,
      service: {
        debugInfo: () => ({ eventSourceId: "src_test" }),
        listEvents,
      },
    });

    await fanout.start();
    for (const sessionId of ["sess_a", "sess_b", "sess_c"]) {
      client.emitNotification({
        channel: sessionEventNotificationChannel,
        payload: JSON.stringify({
          eventId: `evt_${sessionId}`,
          seq: 1,
          sessionId,
          sourceId: "src_remote",
        }),
      });
    }

    // The notifications arrive in one tick: the third session's marker is
    // dropped at the depth cap, and the durable catch-up poll remains its
    // delivery path.
    expect(fanout.debugInfo()).toMatchObject({
      droppedNotificationCount: 1,
      pendingNotificationSessionCount: 2,
    });

    for (let round = 0; round < 6; round += 1) {
      for (const fetch of fetches) {
        fetch.resolve([]);
      }
      await flushPromises();
    }
    await fanout.stop();

    expect(fanout.debugInfo().pendingNotificationSessionCount).toBe(0);
    expect(listEvents).toHaveBeenCalledTimes(2);
  });
});

/** Builds the database surface unused when Postgres LISTEN is disabled. */
function createDatabasePoolFixture(): DatabasePool {
  return {
    db: {} as DatabasePool["db"],
    end: async () => undefined,
    pool: {} as DatabasePool["pool"],
  };
}

/** Builds the subscription hub surface required by catch-up polling. */
function createHubFixture(
  onBroadcast: (event: SessionEvent) => void = () => undefined,
  cursors: ReturnType<SubscriptionHub["sessionCursors"]> = [
    { lastDeliveredSeq: 0, sessionId: "sess_fanout" },
  ],
): SubscriptionHub {
  const mutableCursors = cursors.map((cursor) => ({ ...cursor }));
  return {
    broadcast: (event: SessionEvent) => {
      const index = mutableCursors.findIndex((cursor) => cursor.sessionId === event.sessionId);
      const cursor = mutableCursors[index];
      if (cursor && event.seq > cursor.lastDeliveredSeq) {
        mutableCursors[index] = {
          lastDeliveredSeq: event.seq,
          sessionId: cursor.sessionId,
        };
      }
      onBroadcast(event);
    },
    sessionCursors: () => mutableCursors,
  } as unknown as SubscriptionHub;
}

/** Builds a minimal durable session event for fanout tests. */
function createSessionEvent(seq: number, sessionId = "sess_fanout"): SessionEvent {
  return {
    createdAt: "2026-05-21T00:00:00.000Z",
    eventId: `evt_fanout_${seq}`,
    payload: {},
    producerId: "remote",
    seq,
    sessionId,
    type: "user.message",
  };
}

/** Builds a LISTEN-capable database fixture with queued fake clients. */
function createListenDatabaseFixture(clients: readonly FakeListenClient[]): DatabasePool {
  const remaining = [...clients];
  return {
    db: {} as DatabasePool["db"],
    end: async () => undefined,
    pool: {
      connect: async () => {
        const client = remaining.shift();
        if (!client) {
          throw new Error("No fake LISTEN client queued");
        }
        return client.asPoolClient();
      },
    } as unknown as DatabasePool["pool"],
  };
}

/** Controllable pg client test double for LISTEN lifecycle coverage. */
class FakeListenClient extends EventEmitter {
  readonly queries: string[] = [];
  private readonly detached = new Set<string>();
  releaseCount = 0;

  async query(sql: string): Promise<{ readonly rows: readonly unknown[] }> {
    this.queries.push(sql);
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }

  override off(eventName: string | symbol, listener: (...args: readonly unknown[]) => void): this {
    if (typeof eventName === "string") {
      this.detached.add(eventName);
    }
    return super.off(eventName, listener);
  }

  emitNotification(notification: { readonly channel: string; readonly payload?: string }): void {
    this.emit("notification", notification);
  }

  emitError(error: Error): void {
    this.emit("error", error);
  }

  emitEnd(): void {
    this.emit("end");
  }

  detachedEvents(): string[] {
    return [...this.detached].sort();
  }

  asPoolClient(): DatabasePool["pool"] extends {
    connect: () => Promise<infer TClient>;
  }
    ? TClient
    : never {
    return this as DatabasePool["pool"] extends {
      connect: () => Promise<infer TClient>;
    }
      ? TClient
      : never;
  }
}

/** Creates a manually controlled promise for lifecycle tests. */
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

/** Allows queued promise continuations to run. */
async function flushPromises(): Promise<void> {
  await Promise.resolve();
}
