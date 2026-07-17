import { readdir, readFile } from "node:fs/promises";

import { getTableConfig } from "drizzle-orm/pg-core";
import type pg from "pg";
import { describe, expect, it } from "vitest";

import {
  createSessionProjectionStore,
  type SessionProjectionTransaction,
} from "../src/db-session-projections.js";
import {
  appendEvent,
  appendEventIdempotent,
  createSession,
  createTaskWithEvent,
  type DatabasePool,
  heartbeatParticipantWithEvent,
  recordTaskApproval,
  supersedeScheduledRunsWithEvent,
  upsertParticipantWithEvent,
} from "../src/db.js";
import { sessionProjections } from "../src/schema.js";
import type { SessionEvent } from "../src/types.js";

describe("Session Projection persistence", () => {
  it("defines the complete session_projections schema contract", () => {
    const table = getTableConfig(sessionProjections);

    expect(table.name).toBe("session_projections");
    expect(table.columns.map((column) => column.name)).toEqual([
      "active_run_id",
      "activity",
      "activity_changed_at",
      "archived_at",
      "covers_seq_to",
      "created_at",
      "deleted_at",
      "event_count",
      "forked_from",
      "host_metadata",
      "host_metadata_source_seq",
      "last_event_at",
      "reducer_version",
      "session_id",
      "tangent_of",
      "title",
      "title_source_seq",
      "updated_at",
    ]);
    expect(table.primaryKeys).toHaveLength(0);
    expect(table.columns.find((column) => column.name === "session_id")?.primary).toBe(true);
  });

  it("generates the complete session_projections migration contract", async () => {
    const migrationDirectory = new URL("../drizzle/", import.meta.url);
    const migrationNames = (await readdir(migrationDirectory))
      .filter((name) => /^\d{4}_.+\.sql$/u.test(name))
      .sort();
    const projectionMigrationName = migrationNames.find((name) => name.startsWith("0014_"));

    expect(projectionMigrationName).toBeDefined();
    const migrationSql = await readFile(
      new URL(projectionMigrationName ?? "missing.sql", migrationDirectory),
      "utf8",
    );
    expect(migrationSql).toContain('CREATE TABLE "session_projections"');
    expect(migrationSql).toContain('"archived_at" timestamp with time zone');
    expect(migrationSql).toContain('"deleted_at" timestamp with time zone');
    expect(migrationSql).toContain('"event_count" bigint NOT NULL');
    expect(migrationSql).toContain('"covers_seq_to" bigint NOT NULL');
    expect(migrationSql).toContain('"reducer_version" integer NOT NULL');
  });

  it("persists an appended event through the narrow projection store interface", async () => {
    const event = createProjectionEvent(1, "session.title", { title: "Stored projection" });
    const client = new RecordingProjectionClient([event]);

    const projection = await createSessionProjectionStore().updateForAppendedEvent(client, event);

    expect(projection).toMatchObject({
      coversSeqTo: 1,
      eventCount: 1,
      title: "Stored projection",
    });
    expect(client.queries.some((query) => query.includes("FROM session_projections"))).toBe(true);
    expect(client.queries.some((query) => query.includes("INSERT INTO session_projections"))).toBe(
      true,
    );
  });

  it("never creates apparently complete coverage from only a live tail event", async () => {
    const history = [
      createProjectionEvent(1, "user.message", { text: "Complete history" }),
      createProjectionEvent(2, "task.created", {}),
      createProjectionEvent(3, "session.archived", { archived: true }),
    ];
    const client = new MissingProjectionHistoryClient(history);

    const projection = await createSessionProjectionStore().updateForAppendedEvent(
      client,
      history[2] as SessionEvent,
    );

    expect(projection).toMatchObject({
      archivedAt: history[2]?.createdAt,
      coversSeqTo: 3,
      eventCount: 3,
      title: "Complete history",
    });
  });

  it("rebuilds a stale reducer projection before applying a live append", async () => {
    const history = [
      createProjectionEvent(1, "user.message", { text: "Recovered prefix" }),
      createProjectionEvent(2, "task.created", {}),
      createProjectionEvent(3, "session.archived", { archived: true }),
      createProjectionEvent(4, "session.title", { title: "Live head" }),
    ];
    const client = new IncompleteProjectionHistoryClient(history);

    const projection = await createSessionProjectionStore().updateForAppendedEvent(
      client,
      history[3] as SessionEvent,
    );

    expect(projection).toMatchObject({
      archivedAt: history[2]?.createdAt,
      coversSeqTo: 4,
      eventCount: 4,
      title: "Live head",
    });
  });

  it("updates the projection inside the generic event append transaction", async () => {
    const client = new AppendedEventTransactionClient();

    await appendEvent(client.database, createProjectionEvent(1, "session.title", { title: "A" }), {
      sourceId: "src_projection_persistence_test",
    });

    const projectionIndex = client.queries.findIndex((query) =>
      query.includes("INSERT INTO session_projections"),
    );
    const notifyIndex = client.queries.findIndex((query) => query.includes("pg_notify"));
    const commitIndex = client.queries.indexOf("COMMIT");
    expect(projectionIndex).toBeGreaterThanOrEqual(0);
    expect(notifyIndex).toBeGreaterThan(projectionIndex);
    expect(commitIndex).toBeGreaterThan(notifyIndex);
  });

  it("initializes a complete empty projection for a newly created session", async () => {
    const client = new NewSessionTransactionClient();

    const result = await createSession(client.database, "sess_projection_empty");

    expect(result.created).toBe(true);
    expect(client.queries.some((query) => query.includes("INSERT INTO session_projections"))).toBe(
      true,
    );
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("commits participant registration and heartbeat projections atomically", async () => {
    const registrationClient = new ParticipantEventTransactionClient();
    const registration = await upsertParticipantWithEvent(registrationClient.database, {
      capabilities: { role: "worker" },
      displayName: "Projection worker",
      eventSourceId: "src_projection_persistence_test",
      participantId: "part_projection",
      runtimeKind: "worker",
      sessionId: "sess_projection_persistence",
    });
    const heartbeatClient = new ParticipantEventTransactionClient();
    const heartbeat = await heartbeatParticipantWithEvent(heartbeatClient.database, {
      eventSourceId: "src_projection_persistence_test",
      participantId: "part_projection",
      sessionId: "sess_projection_persistence",
    });

    expect(registration.events).toHaveLength(1);
    expect(heartbeat.participant).not.toBeNull();
    expect(commitsEventAndProjection(registrationClient.queries)).toBe(true);
    expect(commitsEventAndProjection(heartbeatClient.queries)).toBe(true);
  });

  it("commits task lifecycle and approval projections atomically", async () => {
    const taskClient = new TaskEventTransactionClient();
    const taskResult = await createTaskWithEvent(taskClient.database, {
      eventSourceId: "src_projection_persistence_test",
      kind: "projection-test",
      objective: "Persist the projection",
      sessionId: "sess_projection_persistence",
      taskId: "task_projection_persistence",
    });
    const approvalClient = new TaskEventTransactionClient();
    const approvalResult = await recordTaskApproval(approvalClient.database, {
      decision: "approved",
      eventSourceId: "src_projection_persistence_test",
      participantId: "part_projection",
      reason: {},
      sessionId: "sess_projection_persistence",
      taskId: "task_projection_persistence",
    });

    expect(taskResult?.event.type).toBe("task.created");
    expect(approvalResult?.status).toBe("recorded");
    expect(commitsEventAndProjection(taskClient.queries)).toBe(true);
    expect(commitsEventAndProjection(approvalClient.queries)).toBe(true);
  });

  it("commits scheduled-run supersession events and projections atomically", async () => {
    const client = new TaskEventTransactionClient([
      taskDatabaseRow({
        cancelledAt: new Date(2),
        mailboxAccountId: "mailbox_projection",
        mailboxProvider: "gmail",
        scheduleAlgorithmVersion: 1,
        scheduleIntervalMs: "60000",
        scheduleWindowStart: new Date(1),
      }),
    ]);

    const result = await supersedeScheduledRunsWithEvent(client.database, {
      eventSourceId: "src_projection_persistence_test",
      kind: "projection-test",
      mailboxAccountId: "mailbox_projection",
      mailboxProvider: "gmail",
      participantId: "part_projection",
      scheduleAlgorithmVersion: 1,
      scheduleIntervalMs: 60_000,
      scheduleWindowStart: 2,
      sessionId: "sess_projection_persistence",
    });

    expect(result.events).toHaveLength(1);
    expect(result.tasks).toHaveLength(1);
    expect(commitsEventAndProjection(client.queries)).toBe(true);
  });

  it("does not write a projection when an idempotent append replays an existing event", async () => {
    const event = createProjectionEvent(1, "session.title", { title: "Already stored" });
    const client = new IdempotentReplayTransactionClient(event);

    const result = await appendEventIdempotent(client.database, event, {
      sourceId: "src_projection_persistence_test",
    });

    expect(result.status).toBe("replayed");
    expect(result.events).toEqual([]);
    expect(client.queries.some((query) => query.includes("session_projections"))).toBe(false);
    expect(client.queries.at(-1)).toBe("COMMIT");
  });

  it("rolls back the event and domain mutation when projection persistence fails", async () => {
    const client = new FailureInjectionTransactionClient("projection");

    await expect(
      createTaskWithEvent(client.database, {
        eventSourceId: "src_projection_persistence_test",
        kind: "projection-test",
        objective: "Roll back on projection failure",
        sessionId: "sess_projection_persistence",
        taskId: "task_projection_failure",
      }),
    ).rejects.toThrow("createTask");

    expect(client.queries.some((query) => query.includes("INSERT INTO tasks"))).toBe(true);
    expect(client.queries.some((query) => query.includes("INSERT INTO session_events"))).toBe(true);
    expect(client.queries.some((query) => query.includes("INSERT INTO session_projections"))).toBe(
      true,
    );
    expect(client.committedWrites).toEqual({ domain: false, event: false, projection: false });
    expect(client.queries.at(-1)).toBe("ROLLBACK");
  });

  it("rolls back the domain mutation without writing a projection when event persistence fails", async () => {
    const client = new FailureInjectionTransactionClient("event");

    await expect(
      createTaskWithEvent(client.database, {
        eventSourceId: "src_projection_persistence_test",
        kind: "projection-test",
        objective: "Roll back on event failure",
        sessionId: "sess_projection_persistence",
        taskId: "task_event_failure",
      }),
    ).rejects.toThrow("createTask");

    expect(client.queries.some((query) => query.includes("INSERT INTO tasks"))).toBe(true);
    expect(client.queries.some((query) => query.includes("INSERT INTO session_events"))).toBe(true);
    expect(client.queries.some((query) => query.includes("session_projections"))).toBe(false);
    expect(client.committedWrites).toEqual({ domain: false, event: false, projection: false });
    expect(client.queries.at(-1)).toBe("ROLLBACK");
  });
});

class RecordingProjectionClient implements SessionProjectionTransaction {
  readonly queries: string[] = [];

  constructor(private readonly history: readonly SessionEvent[] = []) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql);
    if (sql.includes("FROM session_events")) {
      return {
        rows: this.history.map((event) => ({
          ...event,
          createdAt: new Date(event.createdAt),
          seq: String(event.seq),
        })) as unknown as TRow[],
      };
    }
    return { rows: [] };
  }
}

class MissingProjectionHistoryClient implements SessionProjectionTransaction {
  readonly queries: string[] = [];

  constructor(private readonly history: readonly SessionEvent[]) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql.trim());
    if (sql.includes("FROM session_events")) {
      return {
        rows: this.history.map((event) => ({
          ...event,
          createdAt: new Date(event.createdAt),
          seq: String(event.seq),
        })) as unknown as TRow[],
      };
    }
    return { rows: [] };
  }
}

class IncompleteProjectionHistoryClient extends MissingProjectionHistoryClient {
  override async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    if (sql.includes("FROM session_projections")) {
      return {
        rows: [
          {
            activeRunId: null,
            activity: "idle",
            activityChangedAt: null,
            archivedAt: new Date(3),
            coversSeqTo: "3",
            deletedAt: null,
            eventCount: "1",
            forkedFrom: null,
            hostMetadata: null,
            hostMetadataSourceSeq: null,
            lastEventAt: new Date(3),
            reducerVersion: 0,
            tangentOf: null,
            title: null,
            titleSourceSeq: null,
          },
        ] as unknown as TRow[],
      };
    }
    return super.query(sql);
  }
}

class AppendedEventTransactionClient implements SessionProjectionTransaction {
  readonly database = {
    pool: { connect: async () => this },
  } as unknown as DatabasePool;
  readonly queries: string[] = [];

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql.trim());
    if (sql.includes("SELECT EXISTS")) {
      return { rows: [{ exists: true }] as unknown as TRow[] };
    }
    if (sql.includes("UPDATE session_event_sequences")) {
      return { rows: [{ seq: "1" }] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO session_events")) {
      return {
        rows: [
          {
            createdAt: new Date(1),
            eventId: "evt_projection_1",
            payload: { title: "A" },
            producerId: "projection-persistence-test",
            seq: "1",
            sessionId: "sess_projection_persistence",
            type: "session.title",
          },
        ] as unknown as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

class NewSessionTransactionClient implements SessionProjectionTransaction {
  readonly database = {
    pool: { connect: async () => this },
  } as unknown as DatabasePool;
  readonly queries: string[] = [];

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql.trim());
    if (sql.includes("INSERT INTO sessions")) {
      return {
        rows: [{ createdAt: new Date(0), sessionId: "sess_projection_empty" }] as unknown as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

class ParticipantEventTransactionClient implements SessionProjectionTransaction {
  readonly database = {
    pool: { connect: async () => this },
  } as unknown as DatabasePool;
  readonly queries: string[] = [];

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql.trim());
    if (sql.includes("SELECT EXISTS")) {
      return { rows: [{ exists: true }] as unknown as TRow[] };
    }
    if (sql.includes("FROM participants") && sql.includes("FOR UPDATE")) {
      return { rows: [] };
    }
    if (sql.includes("INSERT INTO participants") || sql.includes("UPDATE participants")) {
      return { rows: [participantDatabaseRow()] as unknown as TRow[] };
    }
    if (sql.includes("UPDATE session_event_sequences")) {
      return { rows: [{ seq: "1" }] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO session_events")) {
      return {
        rows: [
          {
            createdAt: new Date(1),
            eventId: values?.[0],
            payload: JSON.parse(String(values?.[1])) as Record<string, unknown>,
            producerId: values?.[2],
            seq: "1",
            sessionId: values?.[4],
            type: values?.[5],
          },
        ] as unknown as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

class IdempotentReplayTransactionClient implements SessionProjectionTransaction {
  readonly database = {
    pool: { connect: async () => this },
  } as unknown as DatabasePool;
  readonly queries: string[] = [];

  constructor(private readonly event: SessionEvent) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql.trim());
    if (sql.includes("FROM session_events") && sql.includes("WHERE event_id")) {
      return {
        rows: [
          {
            createdAt: new Date(this.event.createdAt),
            eventId: this.event.eventId,
            payload: this.event.payload,
            producerId: this.event.producerId,
            seq: String(this.event.seq),
            sessionId: this.event.sessionId,
            type: this.event.type,
          },
        ] as unknown as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

type FailurePoint = "event" | "projection";

interface TransactionWrites {
  readonly domain: boolean;
  readonly event: boolean;
  readonly projection: boolean;
}

class FailureInjectionTransactionClient implements SessionProjectionTransaction {
  readonly database = {
    pool: { connect: async () => this },
  } as unknown as DatabasePool;
  readonly queries: string[] = [];
  committedWrites: TransactionWrites = { domain: false, event: false, projection: false };
  private pendingWrites: TransactionWrites = { domain: false, event: false, projection: false };

  constructor(private readonly failurePoint: FailurePoint) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ): Promise<{ readonly rows: TRow[] }> {
    const normalizedSql = sql.trim();
    this.queries.push(normalizedSql);
    if (normalizedSql === "ROLLBACK") {
      this.pendingWrites = { domain: false, event: false, projection: false };
      return { rows: [] };
    }
    if (normalizedSql === "COMMIT") {
      this.committedWrites = this.pendingWrites;
      return { rows: [] };
    }
    if (sql.includes("SELECT EXISTS")) {
      return { rows: [{ exists: true }] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO tasks")) {
      this.pendingWrites = { ...this.pendingWrites, domain: true };
      return { rows: [taskDatabaseRow()] as unknown as TRow[] };
    }
    if (sql.includes("UPDATE session_event_sequences")) {
      return { rows: [{ seq: "1" }] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO session_events")) {
      if (this.failurePoint === "event") {
        throw new Error("injected event failure");
      }
      this.pendingWrites = { ...this.pendingWrites, event: true };
      return { rows: [eventDatabaseRow(values)] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO session_projections")) {
      if (this.failurePoint === "projection") {
        throw new Error("injected projection failure");
      }
      this.pendingWrites = { ...this.pendingWrites, projection: true };
    }
    return { rows: [] };
  }

  release(): void {}
}

class TaskEventTransactionClient implements SessionProjectionTransaction {
  readonly database = {
    pool: { connect: async () => this },
  } as unknown as DatabasePool;
  readonly queries: string[] = [];

  constructor(private readonly supersededTasks: readonly Record<string, unknown>[] = []) {}

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
    values?: readonly unknown[],
  ): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql.trim());
    if (sql.includes("SELECT EXISTS")) {
      return { rows: [{ exists: true }] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO tasks")) {
      return { rows: [taskDatabaseRow()] as unknown as TRow[] };
    }
    if (sql.includes("UPDATE tasks") && sql.includes("schedule_window_start <")) {
      return { rows: [...this.supersededTasks] as unknown as TRow[] };
    }
    if (sql.includes("FROM tasks") && sql.includes("LIMIT 1")) {
      return {
        rows: [taskDatabaseRow({ completedAt: new Date(1) })] as unknown as TRow[],
      };
    }
    if (sql.includes("INSERT INTO task_approvals")) {
      return {
        rows: [
          {
            approvalEventId: values?.[0],
            decidedAt: new Date(1),
            decidedByParticipantId: values?.[1],
            decision: values?.[2],
            reason: {},
            sessionId: values?.[4],
            targetKey: values?.[5],
            taskId: values?.[6],
          },
        ] as unknown as TRow[],
      };
    }
    if (sql.includes("UPDATE session_event_sequences")) {
      return { rows: [{ seq: "1" }] as unknown as TRow[] };
    }
    if (sql.includes("INSERT INTO session_events")) {
      return {
        rows: [eventDatabaseRow(values)] as unknown as TRow[],
      };
    }
    return { rows: [] };
  }

  release(): void {}
}

function participantDatabaseRow(): Record<string, unknown> {
  return {
    capabilities: { role: "worker" },
    displayName: "Projection worker",
    joinedAt: new Date(1),
    lastSeenAt: new Date(1),
    participantId: "part_projection",
    runtimeKind: "worker",
    sessionId: "sess_projection_persistence",
  };
}

function taskDatabaseRow(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: new Date(1),
    failedAt: null,
    failure: null,
    input: null,
    kind: "projection-test",
    mailboxAccountId: null,
    mailboxProvider: null,
    objective: "Persist the projection",
    releasedAt: null,
    releasedBy: null,
    result: null,
    scheduleAlgorithmVersion: null,
    scheduleIntervalMs: null,
    scheduleWindowStart: null,
    sessionId: "sess_projection_persistence",
    taskId: "task_projection_persistence",
    ...overrides,
  };
}

function eventDatabaseRow(values: readonly unknown[] | undefined): Record<string, unknown> {
  return {
    createdAt: new Date(1),
    eventId: values?.[0],
    payload: JSON.parse(String(values?.[1])) as Record<string, unknown>,
    producerId: values?.[2],
    seq: "1",
    sessionId: values?.[4],
    type: values?.[5],
  };
}

function commitsEventAndProjection(queries: readonly string[]): boolean {
  const eventIndex = queries.findIndex((query) => query.includes("INSERT INTO session_events"));
  const projectionIndex = queries.findIndex((query) =>
    query.includes("INSERT INTO session_projections"),
  );
  return eventIndex >= 0 && projectionIndex > eventIndex && queries.at(-1) === "COMMIT";
}

function createProjectionEvent(
  seq: number,
  type: string,
  payload: Record<string, unknown>,
): SessionEvent {
  return {
    createdAt: new Date(seq).toISOString(),
    eventId: `evt_projection_${seq}`,
    payload,
    producerId: "projection-persistence-test",
    seq,
    sessionId: "sess_projection_persistence",
    type,
  };
}
