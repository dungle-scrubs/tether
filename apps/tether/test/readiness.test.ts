import { describe, expect, it, vi } from "vitest";

import type { DatabasePool } from "../src/db.js";
import { projectReadiness } from "../src/readiness.js";
import type { ReadinessInput } from "../src/readiness.js";
import type { SessionEventFanoutDebugInfo } from "../src/session-event-fanout.js";

describe("readiness", () => {
  it("returns a bounded database failure and recovers after connectivity returns", async () => {
    let available = false;
    const database = createDatabase(() =>
      available ? Promise.resolve({ rows: [] }) : Promise.reject(new Error("secret database host")),
    );
    const input = createReadinessInput(database, createFanoutDebug());

    await expect(projectReadiness(input)).resolves.toEqual({
      body: {
        ready: false,
        reason: "database_unavailable",
        replicaId: "replica_ready",
        runtimeTopology: "multi",
      },
      status: 503,
    });

    available = true;
    await expect(projectReadiness(input)).resolves.toEqual({
      body: {
        ready: true,
        replicaId: "replica_ready",
        runtimeTopology: "multi",
      },
      status: 200,
    });
  });

  it("accepts healthy polling-only repair and rejects disabling both repair paths", async () => {
    const database = createDatabase(() => Promise.resolve({ rows: [] }));

    await expect(
      projectReadiness(
        createReadinessInput(
          database,
          createFanoutDebug({
            catchUpPollIntervalMs: 10,
            connected: false,
            listenerState: "disabled",
            sessionCursorCount: 1,
          }),
        ),
      ),
    ).resolves.toMatchObject({ status: 200 });
    await expect(
      projectReadiness(
        createReadinessInput(
          database,
          createFanoutDebug({
            catchUpPollIntervalMs: 0,
            connected: false,
            listenerState: "disabled",
            sessionCursorCount: 1,
          }),
        ),
      ),
    ).resolves.toMatchObject({
      body: { ready: false, reason: "fanout_catchup_stale" },
      status: 503,
    });
  });

  it("fails on excess lag and recovers when catch-up returns within the bound", async () => {
    const database = createDatabase(() => Promise.resolve({ rows: [] }));
    let lagAgeMs = 101;
    const fanout = {
      debugInfo: () =>
        createFanoutDebug({
          sessionCursorCount: 1,
          sessionLag: [{ lagAgeMs, outcome: "events_pending", sessionHash: "hash_ready" }],
        }),
    };
    const input = {
      ...createReadinessInput(database, createFanoutDebug()),
      fanout,
      fanoutStaleAfterMs: 100,
    };

    await expect(projectReadiness(input)).resolves.toMatchObject({
      body: { ready: false, reason: "fanout_catchup_stale" },
      status: 503,
    });
    lagAgeMs = 0;
    await expect(projectReadiness(input)).resolves.toMatchObject({
      body: { ready: true },
      status: 200,
    });
  });
});

/** Builds the database query seam used by readiness probes. */
function createDatabase(query: () => Promise<{ readonly rows: readonly unknown[] }>): DatabasePool {
  return {
    pool: { query: vi.fn(query) },
  } as unknown as DatabasePool;
}

/** Builds common readiness dependencies around a fanout snapshot. */
function createReadinessInput(
  database: DatabasePool,
  debugInfo: SessionEventFanoutDebugInfo,
): ReadinessInput {
  return {
    database,
    fanout: { debugInfo: () => debugInfo },
    fanoutStaleAfterMs: 100,
    replicaId: "replica_ready",
    runtimeTopology: "multi" as const,
  };
}

/** Builds a complete healthy fanout snapshot with targeted overrides. */
function createFanoutDebug(
  overrides: Partial<SessionEventFanoutDebugInfo> = {},
): SessionEventFanoutDebugInfo {
  return {
    broadcastCount: 0,
    catchUpBatchCount: 0,
    catchUpEventCount: 0,
    catchUpFailureCount: 0,
    catchUpPollCount: 0,
    catchUpPollIntervalMs: 1_000,
    catchUpRecoveryCount: 0,
    coalescedNotificationCount: 0,
    connected: true,
    droppedNotificationCount: 0,
    fanoutCursorSessionCount: 0,
    ignoredSelfNotificationCount: 0,
    invalidNotificationCount: 0,
    lastCatchUpOutcome: "caught_up",
    lastConnectedAt: null,
    lastDisconnectedAt: null,
    lastListenerError: null,
    lastReconnectDelayMs: null,
    listenerErrorCount: 0,
    listenerState: "connected",
    notificationCount: 0,
    pendingNotificationSessionCount: 0,
    reconnectAttemptCount: 0,
    reconnectSuccessCount: 0,
    scheduled: true,
    sessionCursorCount: 0,
    sessionLag: [],
    ...overrides,
  };
}
