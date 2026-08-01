import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import {
  claimTaskWithEvent,
  type ControlEpochGuard,
  type DatabasePool,
  ensureScheduledRunWithEvents,
  ScheduledTaskIdentityMismatchError,
} from "../src/db.js";
import { ControlEpochStaleError, isControlEpochStaleError } from "../src/control-epoch.js";
import { computeScheduleWindow, deriveScheduledTaskId } from "../src/protocol.js";
import {
  type ScheduledMaintenanceIdentity,
  SessionServicePersistenceError,
} from "../src/session-service-contracts.js";
import { catchAtomicEpochStale } from "../src/session-service-runtime.js";

interface CapturedQuery {
  readonly params: readonly unknown[] | undefined;
  readonly sql: string;
}

/**
 * pg client double that records queries and answers the control-lease guard
 * SELECT and the tasks SELECT with configured rows so the atomic epoch fence and
 * the ensure-scheduled-run ordering can be exercised without a database.
 */
class ScriptedClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;

  constructor(private readonly rowsFor: (sql: string) => readonly unknown[]) {}

  async query<TRow>(sql: string, params?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queries.push({ params, sql });
    if (sql.includes("clock_timestamp()")) {
      return { rows: [{ now: new Date("2026-07-16T12:00:00.000Z") }] as TRow[] };
    }
    return { rows: this.rowsFor(sql) as TRow[] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

function scriptedDatabase(client: ScriptedClient): DatabasePool {
  return { pool: { connect: async () => client } } as unknown as DatabasePool;
}

const guard: ControlEpochGuard = {
  controlChannel: "ws",
  controlEpoch: 5,
  instanceId: "inst_a",
  participantId: "part_1",
  sessionId: "sess_1",
};

function leaseRow(epoch: number | string, overrides: Record<string, unknown> = {}) {
  return {
    controlChannel: "ws",
    epoch,
    instanceId: "inst_a",
    leaseExpiresAt: new Date("2026-07-16T12:01:00.000Z"),
    ...overrides,
  };
}

function isControlLeaseGuardSelect(sql: string): boolean {
  return sql.includes("FROM participant_control_leases") && sql.includes("FOR UPDATE");
}

/** Matches the claimant's task-row lock, which must run before any event write. */
function isTaskLockSelect(sql: string): boolean {
  return sql.includes("FROM tasks") && sql.includes("FOR UPDATE");
}

/** Builds an unclaimed task row for the FOR UPDATE lock read. */
function unclaimedTaskLockRow(overrides: Record<string, unknown> = {}) {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: new Date("2026-07-16T11:00:00.000Z"),
    // The claimant samples now() in the lock read; the elapsed decision uses it.
    databaseNow: new Date("2026-07-16T12:00:00.000Z"),
    failedAt: null,
    failure: null,
    input: null,
    kind: "projection-test",
    objective: "Preserve lock ordering",
    releasedAt: null,
    releasedBy: null,
    result: null,
    scheduleAlgorithmVersion: null,
    scheduleIdentityVersion: null,
    scheduleIntervalMs: null,
    scheduleScopeKey: null,
    scheduleWindowStart: null,
    sessionId: "sess_1",
    taskId: "task_1",
    ...overrides,
  };
}

/** Builds the claimed task row returned by the claim UPDATE. */
function claimedTaskRow() {
  return unclaimedTaskLockRow({
    claimExpiresAt: new Date("2026-07-16T12:01:00.000Z"),
    claimId: "claim_new",
    claimedAt: new Date("2026-07-16T12:00:00.000Z"),
    claimedBy: "part_1",
  });
}

const claimedEventRow = {
  createdAt: new Date("2026-07-16T12:00:00.000Z"),
  eventId: "evt_claim_projection",
  payload: {},
  producerId: "part_1",
  seq: "1",
  sessionId: "sess_1",
  type: "task.claimed",
};

describe("atomic control epoch fence in task mutations", () => {
  it("locks the current lease and rejects a fenced epoch before the mutation runs", async () => {
    // The current durable generation is 6; the caller still carries epoch 5.
    const client = new ScriptedClient((sql) =>
      isControlLeaseGuardSelect(sql) ? [leaseRow(6)] : [],
    );

    await expect(
      claimTaskWithEvent(scriptedDatabase(client), {
        claimLeaseTtlMs: 1_000,
        controlGuard: guard,
        eventSourceId: "src_atomic_test",
        participantId: "part_1",
        sessionId: "sess_1",
        taskId: "task_1",
      }),
    ).rejects.toSatisfy(isControlEpochStaleError);

    const sqls = client.queries.map((query) => query.sql);
    expect(sqls[0]).toBe("BEGIN");
    // The epoch guard SELECT ... FOR UPDATE runs inside the transaction, before
    // the task row is even locked, and the transaction rolls back without
    // touching tasks.
    expect(sqls.some(isControlLeaseGuardSelect)).toBe(true);
    expect(sqls.some(isTaskLockSelect)).toBe(false);
    expect(sqls.some((sql) => sql.includes("UPDATE tasks"))).toBe(false);
    expect(sqls.at(-1)).toBe("ROLLBACK");
    expect(client.releaseCount).toBe(1);
  });

  it("locks the task after a current epoch passes the fence, then claims it", async () => {
    // Current generation equals the supplied epoch (returned as a bigint string).
    const client = new ScriptedClient((sql) => {
      if (isControlLeaseGuardSelect(sql)) {
        return [leaseRow("5")];
      }
      if (isTaskLockSelect(sql)) {
        return [unclaimedTaskLockRow()];
      }
      if (sql.includes("SELECT EXISTS")) {
        return [{ exists: true }];
      }
      if (sql.includes("UPDATE tasks")) {
        return [claimedTaskRow()];
      }
      if (sql.includes("UPDATE session_event_sequences")) {
        return [{ seq: "1" }];
      }
      if (sql.includes("INSERT INTO session_events")) {
        return [claimedEventRow];
      }
      return [];
    });

    const result = await claimTaskWithEvent(scriptedDatabase(client), {
      claimLeaseTtlMs: 1_000,
      controlGuard: guard,
      eventSourceId: "src_atomic_test",
      participantId: "part_1",
      sessionId: "sess_1",
      taskId: "task_1",
    });

    // The task row was unclaimed, so the claim commits one task.claimed event.
    expect(result?.events).toHaveLength(1);
    expect(result?.events[0]?.type).toBe("task.claimed");
    const sqls = client.queries.map((query) => query.sql);
    const guardIndex = sqls.findIndex(isControlLeaseGuardSelect);
    const lockIndex = sqls.findIndex(isTaskLockSelect);
    const updateIndex = sqls.findIndex((sql) => sql.includes("UPDATE tasks"));
    // The guard fences first, THEN the task row is locked, THEN the claim writes.
    expect(guardIndex).toBeGreaterThanOrEqual(0);
    expect(lockIndex).toBeGreaterThan(guardIndex);
    expect(updateIndex).toBeGreaterThan(lockIndex);
    expect(sqls.at(-1)).toBe("COMMIT");
  });

  it("preserves the fenced-mutation order through projection persistence", async () => {
    const client = new ScriptedClient((sql) => {
      if (isControlLeaseGuardSelect(sql)) {
        return [leaseRow("5")];
      }
      if (isTaskLockSelect(sql)) {
        return [unclaimedTaskLockRow()];
      }
      if (sql.includes("SELECT EXISTS")) {
        return [{ exists: true }];
      }
      if (sql.includes("UPDATE tasks")) {
        return [claimedTaskRow()];
      }
      if (sql.includes("UPDATE session_event_sequences")) {
        return [{ seq: "1" }];
      }
      if (sql.includes("INSERT INTO session_events")) {
        return [claimedEventRow];
      }
      return [];
    });

    const result = await claimTaskWithEvent(scriptedDatabase(client), {
      claimLeaseTtlMs: 1_000,
      controlGuard: guard,
      eventSourceId: "src_atomic_test",
      participantId: "part_1",
      sessionId: "sess_1",
      taskId: "task_1",
    });

    expect(result?.events[0]?.type).toBe("task.claimed");
    const sqls = client.queries.map((query) => query.sql);
    const guardIndex = sqls.findIndex(isControlLeaseGuardSelect);
    const lockIndex = sqls.findIndex(isTaskLockSelect);
    const mutationIndex = sqls.findIndex((sql) => sql.includes("UPDATE tasks"));
    const eventIndex = sqls.findIndex((sql) => sql.includes("INSERT INTO session_events"));
    const projectionIndex = sqls.findIndex((sql) =>
      sql.includes("INSERT INTO session_projections"),
    );
    const notifyIndex = sqls.findIndex((sql) => sql.includes("pg_notify"));
    const commitIndex = sqls.indexOf("COMMIT");
    expect(guardIndex).toBeGreaterThanOrEqual(0);
    // The task row is locked before the event sequence is allocated: guard, then
    // task lock, then mutation, then event, then projection, then notify.
    expect(lockIndex).toBeGreaterThan(guardIndex);
    expect(mutationIndex).toBeGreaterThan(lockIndex);
    expect(eventIndex).toBeGreaterThan(mutationIndex);
    expect(projectionIndex).toBeGreaterThan(eventIndex);
    expect(notifyIndex).toBeGreaterThan(projectionIndex);
    expect(commitIndex).toBeGreaterThan(notifyIndex);
  });

  it("skips the epoch fence entirely when no guard is supplied (legacy path)", async () => {
    const client = new ScriptedClient(() => []);

    await claimTaskWithEvent(scriptedDatabase(client), {
      claimLeaseTtlMs: 1_000,
      eventSourceId: "src_atomic_test",
      participantId: "part_1",
      sessionId: "sess_1",
      taskId: "task_1",
    });

    expect(client.queries.some((query) => isControlLeaseGuardSelect(query.sql))).toBe(false);
  });
});

const scopeKey = "scope_1";
const currentWindow = computeScheduleWindow(1_700_003_600_000, 3_600_000);
const identity: ScheduledMaintenanceIdentity = {
  identityVersion: 2,
  kind: "email_organization",
  scheduleWindow: currentWindow,
  scopeKey,
  sessionId: "sess_mailbox_1",
};

function ensureInput(overrides: Partial<Parameters<typeof ensureScheduledRunWithEvents>[1]> = {}) {
  return {
    eventSourceId: "src_ensure_test",
    input: null,
    kind: identity.kind,
    objective: "Organize the mailbox",
    participantId: "system",
    scheduleAlgorithmVersion: currentWindow.algorithmVersion,
    scheduleIdentityVersion: identity.identityVersion,
    scheduleIntervalMs: currentWindow.intervalMs,
    scheduleScopeKey: identity.scopeKey,
    scheduleWindowStart: currentWindow.startMs,
    sessionId: identity.sessionId,
    ...overrides,
  };
}

function staleError(currentEpoch: number | null): ControlEpochStaleError {
  return new ControlEpochStaleError({
    controlChannel: "rest",
    currentEpoch,
    participantId: "part_1",
    providedEpoch: 5,
    sessionId: "sess_1",
  });
}

describe("catchAtomicEpochStale mapping for REST mutations", () => {
  it("maps a wrapped atomic fence failure to the typed stale control result", async () => {
    const wrapped = new SessionServicePersistenceError("claimTask", staleError(8));
    const result = await Effect.runPromise(catchAtomicEpochStale(Effect.fail(wrapped)));
    expect(result).toEqual({ currentEpoch: 8, status: "control_epoch_stale" });
  });

  it("maps a direct atomic fence failure to the typed stale control result", async () => {
    const failing = Effect.fail(staleError(null)) as unknown as Effect.Effect<
      never,
      SessionServicePersistenceError
    >;
    const result = await Effect.runPromise(catchAtomicEpochStale(failing));
    expect(result).toEqual({ currentEpoch: null, status: "control_epoch_stale" });
  });

  it("passes a genuine persistence failure through unchanged", async () => {
    const failure = new SessionServicePersistenceError("claimTask", new Error("db down"));
    const propagated = await Effect.runPromise(
      catchAtomicEpochStale(Effect.fail(failure)).pipe(Effect.flip),
    );
    expect(propagated).toBe(failure);
  });
});

describe("ensureScheduledRunWithEvents identity and ordering", () => {
  it("rejects a supplied task id that does not match the derived identity", async () => {
    const client = new ScriptedClient(() => []);
    await expect(
      ensureScheduledRunWithEvents(
        scriptedDatabase(client),
        ensureInput({ expectedTaskId: "task_wrong" }),
      ),
    ).rejects.toBeInstanceOf(ScheduledTaskIdentityMismatchError);
    // The mismatch is rejected before opening a transaction.
    expect(client.queries).toEqual([]);
  });

  it("supersedes older runs before establishing the current run in one transaction", async () => {
    const taskId = deriveScheduledTaskId(identity);
    const existingTaskRow = {
      createdAt: new Date("2026-07-12T00:00:00.000Z"),
      kind: identity.kind,
      objective: "Organize the mailbox",
      scheduleAlgorithmVersion: currentWindow.algorithmVersion,
      scheduleIdentityVersion: identity.identityVersion,
      scheduleIntervalMs: currentWindow.intervalMs,
      scheduleScopeKey: identity.scopeKey,
      scheduleWindowStart: currentWindow.startMs,
      sessionId: identity.sessionId,
      taskId,
    };
    // Supersede UPDATE returns no older runs; the current-window read replays the
    // already-present deterministic run so no insert/append is needed.
    const client = new ScriptedClient((sql) =>
      sql.includes("FROM tasks") && sql.includes("LIMIT 1") ? [existingTaskRow] : [],
    );

    const result = await ensureScheduledRunWithEvents(
      scriptedDatabase(client),
      ensureInput({ expectedTaskId: taskId }),
    );

    expect(result.taskId).toBe(taskId);
    expect(result.current.status).toBe("replayed");
    expect(result.current.task.taskId).toBe(taskId);

    const sqls = client.queries.map((query) => query.sql);
    expect(sqls[0]).toBe("BEGIN");
    expect(sqls.at(-1)).toBe("COMMIT");
    const supersedeIndex = sqls.findIndex(
      (sql) => sql.includes("UPDATE tasks") && sql.includes("schedule_window_start < $7"),
    );
    const currentReadIndex = sqls.findIndex(
      (sql) => sql.includes("FROM tasks") && sql.includes("LIMIT 1"),
    );
    expect(supersedeIndex).toBeGreaterThanOrEqual(0);
    // The supersession of older runs happens before the current run is
    // established, so the current run is never claimable before older runs are
    // cancelled.
    expect(currentReadIndex).toBeGreaterThan(supersedeIndex);
  });

  it("serializes different windows on a window-agnostic advisory lock", async () => {
    const scheduledRow = (windowStart: number) => ({
      createdAt: new Date("2026-07-12T00:00:00.000Z"),
      kind: identity.kind,
      objective: "Organize the mailbox",
      scheduleAlgorithmVersion: currentWindow.algorithmVersion,
      scheduleIdentityVersion: identity.identityVersion,
      scheduleIntervalMs: currentWindow.intervalMs,
      scheduleScopeKey: identity.scopeKey,
      scheduleWindowStart: windowStart,
      sessionId: identity.sessionId,
      taskId: deriveScheduledTaskId({
        ...identity,
        scheduleWindow: computeScheduleWindow(windowStart, currentWindow.intervalMs),
      }),
    });
    const windowB = computeScheduleWindow(
      currentWindow.startMs + currentWindow.intervalMs,
      currentWindow.intervalMs,
    );

    // Each ensure replays an existing current-window run, so it just commits after
    // taking the lock.
    const clientA = new ScriptedClient((sql) =>
      sql.includes("FROM tasks") && sql.includes("task_id = $2")
        ? [scheduledRow(currentWindow.startMs)]
        : [],
    );
    await ensureScheduledRunWithEvents(
      scriptedDatabase(clientA),
      ensureInput({ scheduleWindowStart: currentWindow.startMs }),
    );
    const clientB = new ScriptedClient((sql) =>
      sql.includes("FROM tasks") && sql.includes("task_id = $2")
        ? [scheduledRow(windowB.startMs)]
        : [],
    );
    await ensureScheduledRunWithEvents(
      scriptedDatabase(clientB),
      ensureInput({ scheduleWindowStart: windowB.startMs }),
    );

    const lockA = clientA.queries.find((query) => query.sql.includes("pg_advisory_xact_lock"));
    const lockB = clientB.queries.find((query) => query.sql.includes("pg_advisory_xact_lock"));
    expect(lockA?.params).toBeDefined();
    // The advisory lock key excludes the window start, so two different windows
    // take the SAME lock and cannot both insert concurrently.
    expect(lockB?.params).toEqual(lockA?.params);
  });

  it("refuses to create an older-window run when a newer window run already exists", async () => {
    const newerWindow = computeScheduleWindow(
      currentWindow.startMs + currentWindow.intervalMs,
      currentWindow.intervalMs,
    );
    const newerTaskId = deriveScheduledTaskId({ ...identity, scheduleWindow: newerWindow });
    const newerRow = {
      createdAt: new Date("2026-07-12T01:00:00.000Z"),
      kind: identity.kind,
      objective: "Organize the mailbox",
      scheduleAlgorithmVersion: newerWindow.algorithmVersion,
      scheduleIdentityVersion: identity.identityVersion,
      scheduleIntervalMs: newerWindow.intervalMs,
      scheduleScopeKey: identity.scopeKey,
      scheduleWindowStart: newerWindow.startMs,
      sessionId: identity.sessionId,
      taskId: newerTaskId,
    };
    // No older runs to supersede, no current-window run yet, but a strictly-newer
    // window run already exists (the concurrent newer ensure committed first).
    const client = new ScriptedClient((sql) =>
      sql.includes("schedule_window_start > $7") ? [newerRow] : [],
    );

    const result = await ensureScheduledRunWithEvents(
      scriptedDatabase(client),
      ensureInput({ scheduleWindowStart: currentWindow.startMs }),
    );

    expect(result.current.status).toBe("superseded_by_newer");
    expect(result.current.task.taskId).toBe(newerTaskId);
    expect(result.taskId).toBe(newerTaskId);
    // The older window must never insert a second active run.
    const sqls = client.queries.map((query) => query.sql);
    expect(sqls.some((sql) => sql.includes("INSERT INTO tasks"))).toBe(false);
    expect(sqls.at(-1)).toBe("COMMIT");
  });
});
