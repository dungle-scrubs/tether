import type pg from "pg";
import { describe, expect, it } from "vitest";
import { type DatabasePool, listSessions } from "../src/db.js";
import {
  listSessionProjectionInventory,
  SessionProjectionCutoverError,
  type SessionProjectionTransaction,
} from "../src/db-session-projections.js";
import { HostPresenceRuntime, projectSessionInventory } from "../src/host-presence.js";
import type { SessionListItem } from "../src/types.js";

describe("projection-backed session inventory", () => {
  it("combines durable host metadata with replica-local live presence", () => {
    const durableSession = createProjectedSession();
    const replicaA = new HostPresenceRuntime();
    const replicaB = new HostPresenceRuntime();
    replicaA.upsertHost(durableSession.sessionId, {
      displayName: "Host A",
      instanceId: "host-a",
      participantId: "part-host-a",
    });

    const projectedA = projectSessionInventory({
      replicaId: "replica-a",
      runtime: replicaA,
      sessions: [durableSession],
    });
    const projectedB = projectSessionInventory({
      replicaId: "replica-b",
      runtime: replicaB,
      sessions: [durableSession],
    });

    expect(projectedA.sessions[0]).toMatchObject({
      branch: "main",
      host: "live",
      project: "tether",
      title: "Durable projection",
    });
    expect(projectedB.sessions[0]).toMatchObject({
      branch: "main",
      host: "stale",
      project: "tether",
      title: "Durable projection",
    });
  });

  it("assembles projection state with table-derived counts without reading raw events", async () => {
    const client = new ProjectionInventoryClient();

    const inventory = await listSessionProjectionInventory(client);

    expect(inventory).toEqual([
      {
        activeTaskCount: 2,
        activity: "settled",
        archived: true,
        bindings: [{ externalId: "thread-1", provider: "external-chat" }],
        branch: "main",
        createdAt: "2026-07-17T00:00:00.000Z",
        cwd: "/workspace/tether/apps/tether",
        deleted: false,
        eventCount: 10_001,
        forkedFrom: null,
        git: { clean: true },
        host: "stale",
        lastEventAt: "2026-07-17T01:00:00.000Z",
        participantCount: 3,
        project: "tether",
        sessionId: "sess_projection_inventory",
        tangentOf: null,
        taskCount: 5,
        title: "Durable projection",
        updatedAt: "2026-07-17T01:00:00.000Z",
        workspace: "/workspace/tether",
      },
    ]);
    expect(client.sql).not.toMatch(/session_events/iu);
  });

  it("cuts the session store inventory read over to the projection query", async () => {
    const client = new ProjectionInventoryClient();
    const database = {
      pool: { query: client.query.bind(client) },
    } as unknown as DatabasePool;

    const inventory = await listSessions(database);

    expect(inventory[0]).toMatchObject({
      eventCount: 10_001,
      participantCount: 3,
      taskCount: 5,
      title: "Durable projection",
    });
    expect(client.sql).not.toMatch(/session_events/iu);
  });

  it("fails cutover explicitly instead of hiding a session with no projection", async () => {
    const client = new MissingProjectionInventoryClient();

    await expect(listSessionProjectionInventory(client)).rejects.toEqual(
      expect.objectContaining({
        message: expect.stringContaining("projection is missing"),
        name: "SessionProjectionCutoverError",
        sessionId: "sess_missing_projection",
      }),
    );
    await expect(listSessionProjectionInventory(client)).rejects.toBeInstanceOf(
      SessionProjectionCutoverError,
    );
  });
});

class ProjectionInventoryClient implements SessionProjectionTransaction {
  sql = "";

  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string,
  ): Promise<{ readonly rows: TRow[] }> {
    this.sql = sql;
    return {
      rows: [
        {
          activeRunId: null,
          activeTaskCount: 2,
          activity: "settled",
          activityChangedAt: new Date("2026-07-17T00:30:00.000Z"),
          archivedAt: new Date("2026-07-17T00:45:00.000Z"),
          bindings: [{ externalId: "thread-1", provider: "external-chat" }],
          coversSeqTo: "10001",
          createdAt: new Date("2026-07-17T00:00:00.000Z"),
          deletedAt: null,
          eventCount: "10001",
          forkedFrom: null,
          hostMetadata: {
            branch: "main",
            cwd: "/workspace/tether/apps/tether",
            git: { clean: true },
            workspace: "/workspace/tether",
          },
          hostMetadataSourceSeq: "10000",
          hasProjection: true,
          lastEventAt: new Date("2026-07-17T01:00:00.000Z"),
          participantCount: 3,
          reducerVersion: 1,
          sessionId: "sess_projection_inventory",
          tangentOf: null,
          taskCount: 5,
          title: "Durable projection",
          titleSourceSeq: "10001",
        },
      ] as unknown as TRow[],
    };
  }
}

class MissingProjectionInventoryClient implements SessionProjectionTransaction {
  async query<TRow extends pg.QueryResultRow = pg.QueryResultRow>(): Promise<{
    readonly rows: TRow[];
  }> {
    return {
      rows: [
        {
          hasProjection: false,
          sessionId: "sess_missing_projection",
        },
      ] as unknown as TRow[],
    };
  }
}

function createProjectedSession(): SessionListItem {
  return {
    activeTaskCount: 0,
    activity: "idle",
    archived: false,
    bindings: [],
    branch: "main",
    createdAt: "2026-07-17T00:00:00.000Z",
    cwd: "/workspace/tether/apps/tether",
    deleted: false,
    eventCount: 10_001,
    forkedFrom: null,
    git: { clean: true },
    host: "stale",
    lastEventAt: "2026-07-17T01:00:00.000Z",
    participantCount: 0,
    project: "tether",
    sessionId: "sess_projection_inventory",
    tangentOf: null,
    taskCount: 0,
    title: "Durable projection",
    updatedAt: "2026-07-17T01:00:00.000Z",
    workspace: "/workspace/tether",
  };
}
