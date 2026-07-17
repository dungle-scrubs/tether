import type pg from "pg";
import { describe, expect, it } from "vitest";

import {
  backfillSessionProjection,
  type SessionProjectionTransaction,
  verifySessionProjection,
} from "../src/db-session-projections.js";
import type { SessionEvent } from "../src/types.js";

describe("Session Projection backfill", () => {
  it("folds a missing projection in bounded batches including a partial tail", async () => {
    const client = new BackfillProjectionClient([
      createEvent(1, "user.message", { text: "Bounded history" }),
      createEvent(2, "task.created", {}),
      createEvent(3, "task.completed", {}),
      createEvent(4, "session.archived", { archived: true }),
      createEvent(5, "session.title", { title: "Bounded result" }),
    ]);

    const result = await backfillSessionProjection(client, {
      batchSize: 2,
      sessionId: "sess_backfill",
    });

    expect(result).toEqual({
      batchesRead: 3,
      coversSeqTo: 5,
      currentCoversSeqTo: 5,
      eventCount: 5,
      malformedEventCount: 0,
      outcome: "written",
      reducerVersion: 1,
      replacedReducerVersion: null,
      resumedFromSeq: 0,
    });
    expect(client.batchLimits).toEqual([2, 2, 2]);
  });

  it("reports that a stale candidate lost to newer live coverage", async () => {
    const client = new StaleBackfillProjectionClient([
      createEvent(1, "user.message", { text: "Prefix" }),
      createEvent(2, "task.created", {}),
      createEvent(3, "session.archived", { archived: true }),
    ]);

    const result = await backfillSessionProjection(client, {
      batchSize: 10,
      sessionId: "sess_backfill",
    });

    expect(result).toEqual({
      batchesRead: 1,
      coversSeqTo: 3,
      currentCoversSeqTo: 4,
      eventCount: 3,
      malformedEventCount: 0,
      outcome: "stale",
      reducerVersion: 1,
      replacedReducerVersion: null,
      resumedFromSeq: 0,
    });
  });

  it("restarts from complete durable coverage instead of refolding the prefix", async () => {
    const client = new RestartedBackfillProjectionClient([
      createEvent(1, "user.message", { text: "Already folded" }),
      createEvent(2, "task.created", {}),
      createEvent(3, "session.title", { title: "Durable prefix" }),
      createEvent(4, "task.completed", {}),
      createEvent(5, "session.archived", { archived: true }),
    ]);

    const result = await backfillSessionProjection(client, {
      batchSize: 10,
      sessionId: "sess_backfill",
    });

    expect(result).toMatchObject({
      coversSeqTo: 5,
      eventCount: 5,
      outcome: "written",
      resumedFromSeq: 3,
    });
    expect(client.batchAfterSeqs).toEqual([3]);
  });

  it("commits each bounded batch so an interrupted run resumes from its last batch", async () => {
    const client = new InterruptedBackfillProjectionClient([
      createEvent(1, "user.message", { text: "First batch" }),
      createEvent(2, "task.created", {}),
      createEvent(3, "session.title", { title: "Resumed batch" }),
      createEvent(4, "session.archived", { archived: true }),
    ]);

    await expect(
      backfillSessionProjection(client, { batchSize: 2, sessionId: "sess_backfill" }),
    ).rejects.toThrow("simulated interruption");

    const resumed = await backfillSessionProjection(client, {
      batchSize: 2,
      sessionId: "sess_backfill",
    });

    expect(resumed).toMatchObject({
      coversSeqTo: 4,
      eventCount: 4,
      outcome: "written",
      resumedFromSeq: 2,
    });
    expect(client.batchAfterSeqs).toEqual([0, 2, 4]);
  });

  it("rebuilds an older reducer version from the stream without mixing old state", async () => {
    const client = new ReducerUpgradeBackfillProjectionClient([
      createEvent(1, "user.message", { text: "Fresh history" }),
      createEvent(2, "task.created", {}),
      createEvent(3, "session.title", { title: "Current reducer" }),
    ]);

    const result = await backfillSessionProjection(client, {
      batchSize: 2,
      sessionId: "sess_backfill",
    });

    expect(result).toMatchObject({
      coversSeqTo: 3,
      eventCount: 3,
      outcome: "written",
      replacedReducerVersion: 0,
      resumedFromSeq: 0,
    });
    expect(client.writtenTitle).toBe("Current reducer");
  });

  it("includes a sequence head that moves while bounded batches are being read", async () => {
    const client = new MovingHeadBackfillProjectionClient([
      createEvent(1, "user.message", { text: "Initial head" }),
      createEvent(2, "task.created", {}),
    ]);

    const result = await backfillSessionProjection(client, {
      batchSize: 2,
      sessionId: "sess_backfill",
    });

    expect(result).toMatchObject({
      batchesRead: 2,
      coversSeqTo: 3,
      eventCount: 3,
      outcome: "written",
    });
  });

  it("reports malformed historic projection payloads without exposing their content", async () => {
    const secret = "historic-secret-payload";
    const client = new BackfillProjectionClient([
      createEvent(1, "user.message", { text: "Safe title" }),
      createEvent(2, "assistant.started", { detail: secret, runId: "   " }),
    ]);

    const result = await backfillSessionProjection(client, {
      batchSize: 10,
      sessionId: "sess_backfill",
    });

    expect(result).toMatchObject({
      coversSeqTo: 2,
      eventCount: 2,
      malformedEventCount: 1,
      outcome: "written",
    });
    expect(JSON.stringify(result)).not.toContain(secret);
  });

  it("verifies against a fresh fold and returns only a payload-free repair report", async () => {
    const secret = "verification-secret-title";
    const client = new VerificationProjectionClient(
      [
        createEvent(1, "user.message", { text: "Initial title" }),
        createEvent(2, "session.title", { secret, title: secret }),
      ],
      { ...projectionRow(2, 2), title: "Incorrect stored title", titleSourceSeq: "2" },
    );

    const report = await verifySessionProjection(client, {
      batchSize: 1,
      sessionId: "sess_backfill",
    });

    expect(report).toEqual({
      batchesRead: 3,
      differenceFields: ["title"],
      freshCoversSeqTo: 2,
      freshEventCount: 2,
      malformedEventCount: 0,
      repair: "backfill",
      status: "mismatch",
      storedCoversSeqTo: 2,
      storedEventCount: 2,
      storedReducerVersion: 1,
    });
    expect(JSON.stringify(report)).not.toContain(secret);
  });

  it("repairs a current-version verification mismatch by rebuilding from the start", async () => {
    const client = new RepairableProjectionClient(
      [
        createEvent(1, "user.message", { text: "Initial title" }),
        createEvent(2, "session.title", { title: "Correct title" }),
      ],
      { ...projectionRow(2, 2), title: "Incorrect stored title", titleSourceSeq: "2" },
    );

    const result = await backfillSessionProjection(client, {
      batchSize: 10,
      rebuildFromStart: true,
      sessionId: "sess_backfill",
    });

    expect(result).toMatchObject({
      coversSeqTo: 2,
      eventCount: 2,
      outcome: "written",
      resumedFromSeq: 0,
    });
    expect(client.writtenTitle).toBe("Correct title");
  });
});

class BackfillProjectionClient implements SessionProjectionTransaction {
  readonly batchAfterSeqs: number[] = [];
  readonly batchLimits: number[] = [];

  constructor(private readonly history: readonly SessionEvent[]) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      return { rows: [] };
    }
    if (sql.includes("FROM session_events")) {
      const afterSeq = Number(values[1]);
      const limit = Number(values[2]);
      this.batchAfterSeqs.push(afterSeq);
      this.batchLimits.push(limit);
      return {
        rows: this.history
          .filter((event) => event.seq > afterSeq)
          .slice(0, limit)
          .map((event) => ({
            ...event,
            createdAt: new Date(event.createdAt),
            seq: String(event.seq),
          })) as unknown as TRow[],
      };
    }
    if (sql.includes("INSERT INTO session_projections")) {
      return { rows: [{ sessionId: "sess_backfill" }] as unknown as TRow[] };
    }
    return { rows: [] };
  }
}

class RestartedBackfillProjectionClient extends BackfillProjectionClient {
  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      return { rows: [projectionRow(3, 3)] as unknown as TRow[] };
    }
    return super.query(sql, values);
  }
}

class ReducerUpgradeBackfillProjectionClient extends BackfillProjectionClient {
  writtenTitle: unknown = null;

  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      return {
        rows: [
          { ...projectionRow(2, 2), reducerVersion: 0, title: "Old reducer state" },
        ] as unknown as TRow[],
      };
    }
    if (sql.includes("INSERT INTO session_projections")) {
      this.writtenTitle = values[14];
    }
    return super.query(sql, values);
  }
}

class InterruptedBackfillProjectionClient extends BackfillProjectionClient {
  private interrupt = true;
  private stored: Record<string, unknown> | null = null;
  private writeCount = 0;

  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      return {
        rows: (this.stored === null ? [] : [this.stored]) as unknown as TRow[],
      };
    }
    if (sql.includes("FROM session_events") && this.writeCount === 1 && this.interrupt) {
      this.interrupt = false;
      throw new Error("simulated interruption");
    }
    if (sql.includes("INSERT INTO session_projections")) {
      this.writeCount += 1;
      this.stored = {
        ...projectionRow(Number(values[4]), Number(values[6])),
        title: values[14],
        titleSourceSeq: values[15],
      };
      return { rows: [{ sessionId: "sess_backfill" }] as unknown as TRow[] };
    }
    return super.query(sql, values);
  }
}

class MovingHeadBackfillProjectionClient extends BackfillProjectionClient {
  private eventBatchCount = 0;

  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_events")) {
      this.eventBatchCount += 1;
      if (this.eventBatchCount === 2) {
        return {
          rows: [
            {
              ...createEvent(3, "session.title", { title: "Moving head" }),
              createdAt: new Date(3),
              seq: "3",
            },
          ] as unknown as TRow[],
        };
      }
    }
    return super.query(sql, values);
  }
}

class VerificationProjectionClient extends BackfillProjectionClient {
  constructor(
    history: readonly SessionEvent[],
    private readonly stored: Record<string, unknown>,
  ) {
    super(history);
  }

  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      return { rows: [this.stored] as unknown as TRow[] };
    }
    return super.query(sql, values);
  }
}

class RepairableProjectionClient extends VerificationProjectionClient {
  writtenTitle: unknown = null;

  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("INSERT INTO session_projections")) {
      this.writtenTitle = values[14];
    }
    return super.query(sql, values);
  }
}

class StaleBackfillProjectionClient extends BackfillProjectionClient {
  private projectionReadCount = 0;

  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values: readonly unknown[] = [],
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      this.projectionReadCount += 1;
      return this.projectionReadCount === 1
        ? { rows: [] }
        : { rows: [projectionRow(4, 4)] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO session_projections")) {
      return { rows: [] };
    }
    return super.query(sql, values);
  }
}

function projectionRow(coversSeqTo: number, eventCount: number): Record<string, unknown> {
  return {
    activeRunId: null,
    activity: "idle",
    activityChangedAt: null,
    archivedAt: null,
    coversSeqTo: String(coversSeqTo),
    deletedAt: null,
    eventCount: String(eventCount),
    forkedFrom: null,
    hostMetadata: null,
    hostMetadataSourceSeq: null,
    lastEventAt: new Date(coversSeqTo),
    reducerVersion: 1,
    tangentOf: null,
    title: "Live head",
    titleSourceSeq: String(coversSeqTo),
  };
}

function createEvent(seq: number, type: string, payload: Record<string, unknown>): SessionEvent {
  return {
    createdAt: new Date(seq).toISOString(),
    eventId: `evt_backfill_${seq}`,
    payload,
    producerId: "projection-backfill-test",
    seq,
    sessionId: "sess_backfill",
    type,
  };
}
