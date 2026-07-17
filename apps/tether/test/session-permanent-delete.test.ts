import { describe, expect, it } from "vitest";

import { createSession, type DatabasePool, deleteSession, SessionDeletedError } from "../src/db.js";

interface CapturedQuery {
  readonly params: readonly unknown[] | undefined;
  readonly sql: string;
}

/** Drizzle issues query-config objects; raw transaction statements are strings. */
function queryText(sql: string | { readonly text: string }): string {
  return typeof sql === "string" ? sql : sql.text;
}

function isSessionLockSelect(sql: string): boolean {
  return sql.includes("FROM sessions") && sql.includes("FOR UPDATE");
}

function isLeaseLockSelect(sql: string): boolean {
  return sql.includes("FROM participant_control_leases") && sql.includes("FOR UPDATE");
}

function isTaskLockSelect(sql: string): boolean {
  return sql.includes("FROM tasks") && sql.includes("FOR UPDATE");
}

function isSequenceLockSelect(sql: string): boolean {
  return sql.includes("FROM session_event_sequences") && sql.includes("FOR UPDATE");
}

function isProjectionLockSelect(sql: string): boolean {
  return sql.includes("FROM session_projections") && sql.includes("FOR UPDATE");
}

function isTombstoneInsert(sql: string): boolean {
  return sql.includes("INSERT INTO session_tombstones");
}

function isSessionDelete(sql: string): boolean {
  return sql.includes('delete from "sessions"');
}

interface ScriptedDeleteState {
  /** Projection row returned by the fenced eligibility re-check, or null. */
  readonly projection: { readonly activity: string; readonly archivedAt: Date | null } | null;
  /** Whether the sessions FOR UPDATE lock finds a row. */
  readonly sessionExists: boolean;
  /** next_seq returned by the locked sequence read; undefined for no row. */
  readonly sequenceNextSeq?: number | string | undefined;
}

/**
 * pg client double that answers the fenced permanent-delete transaction with a
 * configurable durable state so refusal and success paths can be exercised
 * without a database.
 */
class ScriptedDeleteClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;

  constructor(private readonly state: ScriptedDeleteState) {}

  async query<TRow>(
    rawSql: string | { readonly text: string },
    params?: readonly unknown[],
  ): Promise<{ readonly rows: TRow[] }> {
    const sql = queryText(rawSql);
    this.queries.push({ params, sql });
    if (isSessionLockSelect(sql)) {
      return {
        rows: this.state.sessionExists ? ([{ sessionId: "sess_del" }] as TRow[]) : [],
      };
    }
    if (isSequenceLockSelect(sql)) {
      return {
        rows:
          this.state.sequenceNextSeq === undefined
            ? []
            : ([{ seq: this.state.sequenceNextSeq }] as TRow[]),
      };
    }
    if (isProjectionLockSelect(sql)) {
      return { rows: this.state.projection ? ([this.state.projection] as TRow[]) : [] };
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

const archivedSettled = { activity: "settled", archivedAt: new Date("2026-07-12T00:00:00.000Z") };

describe("deleteSession fenced permanent delete", () => {
  it("locks session-owned rows in D-009 order, writes the tombstone, then deletes", async () => {
    const client = new ScriptedDeleteClient({
      projection: archivedSettled,
      sequenceNextSeq: "6",
      sessionExists: true,
    });

    const result = await deleteSession(scriptedDatabase(client), "sess_del");

    expect(result).toEqual({ status: "deleted" });
    const order = [
      client.queries.findIndex((query) => isSessionLockSelect(query.sql)),
      client.queries.findIndex((query) => isLeaseLockSelect(query.sql)),
      client.queries.findIndex((query) => isTaskLockSelect(query.sql)),
      client.queries.findIndex((query) => isSequenceLockSelect(query.sql)),
      client.queries.findIndex((query) => isProjectionLockSelect(query.sql)),
      client.queries.findIndex((query) => isTombstoneInsert(query.sql)),
      client.queries.findIndex((query) => isSessionDelete(query.sql)),
    ];
    // Every step ran, and in the mandatory lock order: lease rows, then task
    // rows, then the sequence row, then the projection row, before the
    // tombstone insert and the cascade delete.
    for (const index of order) {
      expect(index).toBeGreaterThanOrEqual(0);
    }
    expect([...order]).toEqual([...order].sort((left, right) => left - right));
    // The tombstone records the last allocated sequence (next_seq - 1) so the
    // dropped log head remains diagnosable.
    const tombstone = client.queries.find((query) => isTombstoneInsert(query.sql));
    expect(tombstone?.params).toEqual([5, "sess_del"]);
    expect(client.queries.at(-1)?.sql).toBe("COMMIT");
    expect(client.releaseCount).toBe(1);
  });

  it("returns not_found without touching tombstones when the session row is gone", async () => {
    const client = new ScriptedDeleteClient({
      projection: archivedSettled,
      sessionExists: false,
    });

    const result = await deleteSession(scriptedDatabase(client), "sess_del");

    expect(result).toEqual({ status: "not_found" });
    expect(client.queries.some((query) => isTombstoneInsert(query.sql))).toBe(false);
    expect(client.queries.some((query) => isSessionDelete(query.sql))).toBe(false);
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
  });

  it("refuses inside the transaction when the projection is no longer archived", async () => {
    const client = new ScriptedDeleteClient({
      projection: { activity: "settled", archivedAt: null },
      sequenceNextSeq: 1,
      sessionExists: true,
    });

    const result = await deleteSession(scriptedDatabase(client), "sess_del");

    expect(result).toEqual({
      detail: "only archived sessions can be permanently deleted",
      reason: "not-archived",
      status: "refused",
    });
    expect(client.queries.some((query) => isSessionDelete(query.sql))).toBe(false);
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
  });

  it("refuses inside the transaction when a turn became active after the snapshot", async () => {
    const client = new ScriptedDeleteClient({
      projection: { activity: "running", archivedAt: new Date("2026-07-12T00:00:00.000Z") },
      sequenceNextSeq: 9,
      sessionExists: true,
    });

    const result = await deleteSession(scriptedDatabase(client), "sess_del");

    expect(result).toEqual({
      detail: "a turn is active on this session",
      reason: "protected",
      status: "refused",
    });
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
  });

  it("refuses when the projection row is missing rather than deleting blindly", async () => {
    const client = new ScriptedDeleteClient({
      projection: null,
      sequenceNextSeq: 1,
      sessionExists: true,
    });

    const result = await deleteSession(scriptedDatabase(client), "sess_del");

    expect(result).toEqual({
      detail: "session projection is missing",
      reason: "not-archived",
      status: "refused",
    });
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
  });

  it("re-runs the live-host probe after the row locks and refuses on a late host", async () => {
    const client = new ScriptedDeleteClient({
      projection: archivedSettled,
      sequenceNextSeq: 4,
      sessionExists: true,
    });
    let probedAfterProjectionLock = false;

    const result = await deleteSession(scriptedDatabase(client), "sess_del", {
      hasLiveHost: () => {
        probedAfterProjectionLock = client.queries.some((query) =>
          isProjectionLockSelect(query.sql),
        );
        return true;
      },
    });

    expect(result).toEqual({
      detail: "a host is live on this session",
      reason: "protected",
      status: "refused",
    });
    // The probe runs inside the transaction, after every durable lock is held.
    expect(probedAfterProjectionLock).toBe(true);
    expect(client.queries.some((query) => isSessionDelete(query.sql))).toBe(false);
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
  });
});

/**
 * pg client double for session creation against a tombstoned id: the insert
 * appears to succeed, then the tombstone read reports the permanent delete.
 */
class ScriptedCreateClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;

  constructor(private readonly tombstoned: boolean) {}

  async query<TRow>(sql: string, params?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queries.push({ params, sql });
    if (sql.includes("INSERT INTO sessions")) {
      return {
        rows: [
          { createdAt: new Date("2026-07-12T00:00:00.000Z"), sessionId: "sess_del" },
        ] as TRow[],
      };
    }
    if (sql.includes("FROM session_tombstones")) {
      return {
        rows: this.tombstoned
          ? ([{ deletedAt: new Date("2026-07-11T00:00:00.000Z") }] as TRow[])
          : [],
      };
    }
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

describe("createSession tombstone fence", () => {
  it("refuses to recreate a permanently deleted session id with a typed error", async () => {
    const client = new ScriptedCreateClient(true);

    await expect(createSession(scriptedDatabase(client), "sess_del")).rejects.toBeInstanceOf(
      SessionDeletedError,
    );

    // The tombstone read runs after the insert attempt so a tombstone committed
    // while the insert waited on the deleted row still rolls the recreation back.
    const insertIndex = client.queries.findIndex((query) =>
      query.sql.includes("INSERT INTO sessions"),
    );
    const tombstoneIndex = client.queries.findIndex((query) =>
      query.sql.includes("FROM session_tombstones"),
    );
    expect(insertIndex).toBeGreaterThanOrEqual(0);
    expect(tombstoneIndex).toBeGreaterThan(insertIndex);
    expect(client.queries.at(-1)?.sql).toBe("ROLLBACK");
    expect(client.releaseCount).toBe(1);
  });

  it("creates a fresh session id normally when no tombstone exists", async () => {
    const client = new ScriptedCreateClient(false);

    const result = await createSession(scriptedDatabase(client), "sess_del");

    expect(result.created).toBe(true);
    expect(result.session.sessionId).toBe("sess_del");
    expect(client.queries.at(-1)?.sql).toBe("COMMIT");
  });
});
