import { describe, expect, it } from "vitest";

import {
  createTaskWithEventIdempotent,
  type DatabasePool,
  ensureScheduledRunWithEvents,
  ScheduledRunIdentityConflictError,
} from "../src/db.js";
import { deriveScheduledTaskId } from "../src/protocol.js";

interface CapturedQuery {
  readonly params: readonly unknown[] | undefined;
  readonly sql: string;
}

const schedule = {
  scheduleAlgorithmVersion: 1,
  scheduleIdentityVersion: 2,
  scheduleIntervalMs: 3_600_000,
  scheduleScopeKey: "scope_1",
  scheduleWindowStart: 1_700_000_000_000,
} as const;

const derivedTaskId = deriveScheduledTaskId({
  identityVersion: schedule.scheduleIdentityVersion,
  kind: "email_organization",
  scheduleWindow: {
    algorithmVersion: schedule.scheduleAlgorithmVersion,
    endMs: schedule.scheduleWindowStart + schedule.scheduleIntervalMs,
    intervalMs: schedule.scheduleIntervalMs,
    startMs: schedule.scheduleWindowStart,
  },
  scopeKey: schedule.scheduleScopeKey,
  sessionId: "sess_sched",
});

const ensureInput = {
  eventSourceId: "src_test",
  input: null,
  kind: "email_organization",
  objective: "Organize the mailbox",
  participantId: "system",
  scheduleAlgorithmVersion: schedule.scheduleAlgorithmVersion,
  scheduleIdentityVersion: schedule.scheduleIdentityVersion,
  scheduleIntervalMs: schedule.scheduleIntervalMs,
  scheduleScopeKey: schedule.scheduleScopeKey,
  scheduleWindowStart: schedule.scheduleWindowStart,
  sessionId: "sess_sched",
} as const;

/** A durable tasks row as returned by the shared task RETURNING columns. */
function taskDbRow(overrides: Record<string, unknown> = {}) {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: new Date("2026-07-12T00:00:00.000Z"),
    failedAt: null,
    failure: null,
    input: null,
    kind: "email_organization",
    objective: "Organize the mailbox",
    releasedAt: null,
    releasedBy: null,
    result: null,
    scheduleAlgorithmVersion: schedule.scheduleAlgorithmVersion,
    scheduleIdentityVersion: schedule.scheduleIdentityVersion,
    scheduleIntervalMs: schedule.scheduleIntervalMs,
    scheduleScopeKey: schedule.scheduleScopeKey,
    scheduleWindowStart: schedule.scheduleWindowStart,
    sessionId: "sess_sched",
    taskId: derivedTaskId,
    ...overrides,
  };
}

function isAdvisoryLock(sql: string): boolean {
  return sql.includes("pg_advisory_xact_lock");
}

function isSupersedeUpdate(sql: string): boolean {
  return sql.includes("UPDATE tasks") && sql.includes("cancelled_at = now()");
}

function isTaskReadSelect(sql: string): boolean {
  return sql.includes("FROM tasks") && sql.includes("task_id = $2");
}

/**
 * pg client double that answers the ensure-scheduled-run transaction with a
 * configurable occupant row at the derived deterministic task id.
 */
class ScriptedEnsureClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;

  constructor(private readonly occupant: Record<string, unknown> | null) {}

  async query<TRow>(sql: string, params?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queries.push({ params, sql });
    if (isSupersedeUpdate(sql)) {
      return { rows: [] };
    }
    if (isTaskReadSelect(sql)) {
      return { rows: this.occupant ? ([this.occupant] as TRow[]) : [] };
    }
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

function scriptedDatabase(client: { query: unknown; release: unknown }): DatabasePool {
  return {
    pool: {
      connect: async () => client,
    },
  } as unknown as DatabasePool;
}

describe("ensureScheduledRunWithEvents occupant identity verification", () => {
  it("rejects a schedule-less squatter at the derived id with a typed conflict", async () => {
    // A generic caller-id create legally inserted a manual task (NULL schedule
    // columns) at the deterministic scheduled-run id. Replaying it would skip
    // the window's maintenance work behind a success response.
    const client = new ScriptedEnsureClient(
      taskDbRow({
        scheduleAlgorithmVersion: null,
        scheduleIdentityVersion: null,
        scheduleIntervalMs: null,
        scheduleScopeKey: null,
        scheduleWindowStart: null,
      }),
    );

    const attempt = ensureScheduledRunWithEvents(scriptedDatabase(client), ensureInput);

    await expect(attempt).rejects.toBeInstanceOf(ScheduledRunIdentityConflictError);
    await expect(attempt).rejects.toMatchObject({
      conflictingFields: ["schedule"],
      taskId: derivedTaskId,
    });
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
    expect(client.releaseCount).toBe(1);
  });

  it("rejects an occupant whose kind differs from the schedule identity", async () => {
    const client = new ScriptedEnsureClient(taskDbRow({ kind: "manual_review" }));

    await expect(
      ensureScheduledRunWithEvents(scriptedDatabase(client), ensureInput),
    ).rejects.toMatchObject({
      conflictingFields: ["kind"],
      name: "ScheduledRunIdentityConflictError",
    });
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
  });

  it("replays an occupant that carries the exact schedule identity", async () => {
    const client = new ScriptedEnsureClient(taskDbRow());

    const result = await ensureScheduledRunWithEvents(scriptedDatabase(client), ensureInput);

    expect(result.current.status).toBe("replayed");
    expect(result.current.task.taskId).toBe(derivedTaskId);
    expect(client.queries.at(-1)?.sql).toBe("COMMIT");
  });

  it("serializes on the identity lock and then the derived task-id lock", async () => {
    const client = new ScriptedEnsureClient(taskDbRow());

    await ensureScheduledRunWithEvents(scriptedDatabase(client), ensureInput);

    const locks = client.queries.filter((query) => isAdvisoryLock(query.sql));
    expect(locks).toHaveLength(2);
    // First the window-agnostic schedule identity lock, then the derived task
    // id lock shared with createTaskWithEventIdempotent, so a schedule-less
    // create carrying the derived id can never race the ensure insert.
    expect(locks[0]?.params?.[0]).toBe("sess_sched");
    expect(String(locks[0]?.params?.[1])).toContain("scheduled_run");
    expect(locks[1]?.params).toEqual(["sess_sched", derivedTaskId]);
    // Both locks precede the existence read whose outcome they fence.
    const readIndex = client.queries.findIndex((query) => isTaskReadSelect(query.sql));
    const lastLockIndex = client.queries
      .map((query) => isAdvisoryLock(query.sql))
      .lastIndexOf(true);
    expect(lastLockIndex).toBeLessThan(readIndex);
  });
});

/**
 * pg client double for the idempotent caller-id create whose task insert loses
 * a residual duplicate-id race as a primary-key unique violation. The winner's
 * committed row becomes visible to a task read only after this transaction
 * rolls back, mirroring the real re-read on the same autocommit-usable client.
 */
class ScriptedCreateRaceClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;
  private rolledBack = false;

  constructor(private readonly winnerRow: Record<string, unknown>) {}

  async query<TRow>(sql: string, params?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queries.push({ params, sql });
    if (sql === "ROLLBACK") {
      this.rolledBack = true;
      return { rows: [] };
    }
    if (isTaskReadSelect(sql)) {
      // The pre-insert existence check sees no row: the racing winner has not
      // committed yet. The post-ROLLBACK re-read on this same client sees the
      // winner's now-committed row.
      return { rows: this.rolledBack ? ([this.winnerRow] as TRow[]) : [] };
    }
    if (sql.includes("INSERT INTO tasks")) {
      const violation = new Error(
        'duplicate key value violates unique constraint "tasks_session_id_task_id_pk"',
      ) as Error & { code?: string; constraint?: string };
      violation.code = "23505";
      violation.constraint = "tasks_session_id_task_id_pk";
      throw violation;
    }
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

function racingDatabase(client: ScriptedCreateRaceClient): DatabasePool {
  let connectCount = 0;
  return {
    pool: {
      connect: async () => {
        connectCount += 1;
        // The residual-race path must re-read the winner on the already
        // checked-out client. A second checkout (or a pool.query, which itself
        // checks out) would self-deadlock the pool at max=1, so the double
        // refuses it outright to pin the single-connection contract.
        if (connectCount > 1) {
          throw new Error(
            "pool exhausted: residual-race re-read must reuse the checked-out client",
          );
        }
        return client;
      },
    },
  } as unknown as DatabasePool;
}

describe("createTaskWithEventIdempotent residual duplicate-id race", () => {
  const createInput = {
    eventSourceId: "src_test",
    input: null,
    kind: "email_organization",
    objective: "Organize the mailbox",
    sessionId: "sess_sched",
    taskId: derivedTaskId,
    taskIdSource: "caller",
  } as const;

  it("maps a unique violation to an idempotent replay when the winner matches", async () => {
    const client = new ScriptedCreateRaceClient(
      taskDbRow({
        mailboxAccountId: null,
        mailboxProvider: null,
        scheduleAlgorithmVersion: null,
        scheduleIntervalMs: null,
        scheduleWindowStart: null,
      }),
    );
    const database = racingDatabase(client);

    const result = await createTaskWithEventIdempotent(database, createInput);

    expect(result.status).toBe("replayed");
    if (result.status === "replayed") {
      expect(result.task.taskId).toBe(derivedTaskId);
      expect(result.events).toEqual([]);
    }
    // The winner row is re-read on the same client after the transaction rolls
    // back, so the final client query is the re-read SELECT and ROLLBACK
    // precedes it. No second pool connection is ever acquired.
    expect(isTaskReadSelect(client.queries.at(-1)?.sql ?? "")).toBe(true);
    const rollbackIndex = client.queries.map((query) => query.sql).lastIndexOf("ROLLBACK");
    expect(rollbackIndex).toBeGreaterThanOrEqual(0);
    expect(rollbackIndex).toBeLessThan(client.queries.length - 1);
    expect(client.releaseCount).toBe(1);
  });

  it("maps a unique violation to a typed conflict when the winner differs", async () => {
    const client = new ScriptedCreateRaceClient(
      taskDbRow({
        kind: "manual_review",
        mailboxAccountId: null,
        mailboxProvider: null,
        objective: "Different objective",
        scheduleAlgorithmVersion: null,
        scheduleIntervalMs: null,
        scheduleWindowStart: null,
      }),
    );
    const database = racingDatabase(client);

    const result = await createTaskWithEventIdempotent(database, createInput);

    expect(result.status).toBe("conflict");
    if (result.status === "conflict") {
      expect(result.taskId).toBe(derivedTaskId);
      expect(result.conflictingFields).toContain("kind");
      expect(result.conflictingFields).toContain("objective");
    }
    expect(client.releaseCount).toBe(1);
  });
});
