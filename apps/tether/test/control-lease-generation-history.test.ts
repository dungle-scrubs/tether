import { describe, expect, it } from "vitest";

import { claimControlLease, type DatabasePool } from "../src/db.js";

interface CapturedQuery {
  readonly params: readonly unknown[] | undefined;
  readonly sql: string;
}

/** A durable participant_control_leases row as returned by the RETURNING columns. */
function leaseDbRow(epoch: number, overrides: Record<string, unknown> = {}) {
  const now = new Date("2026-07-12T00:00:00.000Z");
  return {
    claimedAt: now,
    controlChannel: "ws",
    epoch,
    instanceId: "inst_a",
    lastSeenAt: now,
    leaseExpiresAt: new Date("2026-07-12T00:01:00.000Z"),
    participantId: "part_1",
    releasedAt: null,
    sessionId: "sess_1",
    supersededAt: null,
    ...overrides,
  };
}

function isActiveLeaseSelect(sql: string): boolean {
  return sql.includes("FROM participant_control_leases") && sql.includes("FOR UPDATE");
}

function isDatabaseClockSelect(sql: string): boolean {
  return sql.includes("SELECT clock_timestamp()");
}

function isMaxEpochSelect(sql: string): boolean {
  return sql.includes("max(epoch)");
}

function isSupersedeUpdate(sql: string): boolean {
  return sql.includes("UPDATE participant_control_leases") && sql.includes("SET superseded_at");
}

function isLeaseInsert(sql: string): boolean {
  return sql.includes("INSERT INTO participant_control_leases");
}

/**
 * pg client double that records queries and answers the claim-control-lease
 * transaction with a current epoch-N row and a fresh epoch-(N+1) insert so the
 * generation-history behavior can be exercised without a database.
 */
class ScriptedLeaseClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;

  constructor(private readonly currentEpoch: number) {}

  async query<TRow>(sql: string, params?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queries.push({ params, sql });
    if (isDatabaseClockSelect(sql)) {
      return {
        rows: [{ now: new Date("2026-07-12T00:00:00.000Z") } as unknown as TRow],
      };
    }
    if (isActiveLeaseSelect(sql)) {
      return { rows: [leaseDbRow(this.currentEpoch)] as TRow[] };
    }
    if (isMaxEpochSelect(sql)) {
      return { rows: [{ maxEpoch: this.currentEpoch } as unknown as TRow] };
    }
    if (isLeaseInsert(sql)) {
      return { rows: [leaseDbRow(this.currentEpoch + 1)] as TRow[] };
    }
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

function scriptedDatabase(client: ScriptedLeaseClient): DatabasePool {
  return {
    pool: {
      connect: async () => client,
      // requireSession runs on the pool before the transaction opens.
      query: async () => ({ rows: [{ exists: true }] }),
    },
  } as unknown as DatabasePool;
}

describe("claimControlLease generation history", () => {
  it("supersedes epoch N in place and inserts epoch N+1 as a distinct retained row", async () => {
    // The current durable generation for this same instance is epoch 7.
    const client = new ScriptedLeaseClient(7);

    const claim = await claimControlLease(scriptedDatabase(client), {
      controlChannel: "ws",
      instanceId: "inst_a",
      leaseTtlMs: 60_000,
      participantId: "part_1",
      sessionId: "sess_1",
    });

    if (claim.status === "conflict") {
      throw new Error("unexpected conflict");
    }
    // A same-instance reconnect advances the generation, fencing the prior epoch.
    expect(claim.status).toBe("superseded");
    expect(claim.lease.epoch).toBe(8);

    const supersede = client.queries.find((query) => isSupersedeUpdate(query.sql));
    expect(supersede).toBeDefined();
    const supersedeSql = supersede?.sql ?? "";
    // The prior generation is retained as an immutable row: the supersession only
    // sets superseded_at (no DELETE), so epoch 7's row still exists as history.
    expect(supersedeSql).toContain("SET superseded_at = clock_timestamp()");
    expect(supersedeSql).not.toContain("DELETE");
    // The fence now covers this instance's own current row too (no instance_id <>
    // exclusion), so the same-instance prior generation is superseded rather than
    // overwritten.
    expect(supersedeSql).not.toContain("instance_id <>");

    const insert = client.queries.find((query) => isLeaseInsert(query.sql));
    expect(insert).toBeDefined();
    // A plain insert of a new generation row: no ON CONFLICT overwrite could ever
    // replace epoch 7's retained row in place.
    expect(insert?.sql).not.toContain("ON CONFLICT");

    // The supersede-in-place happens before the new generation is inserted.
    const supersedeIndex = client.queries.findIndex((query) => isSupersedeUpdate(query.sql));
    const insertIndex = client.queries.findIndex((query) => isLeaseInsert(query.sql));
    expect(supersedeIndex).toBeGreaterThanOrEqual(0);
    expect(insertIndex).toBeGreaterThan(supersedeIndex);
    expect(client.queries.at(-1)?.sql).toBe("COMMIT");
    expect(client.releaseCount).toBe(1);
  });

  it("issues the first generation as a claim without superseding history", async () => {
    // No current row: the active-lease SELECT returns empty, MAX(epoch) is null.
    const client = new (class extends ScriptedLeaseClient {
      async query<TRow>(
        sql: string,
        params?: readonly unknown[],
      ): Promise<{ readonly rows: TRow[] }> {
        this.queries.push({ params, sql });
        if (isDatabaseClockSelect(sql)) {
          return {
            rows: [{ now: new Date("2026-07-12T00:00:00.000Z") } as unknown as TRow],
          };
        }
        if (isActiveLeaseSelect(sql)) {
          return { rows: [] };
        }
        if (isMaxEpochSelect(sql)) {
          return { rows: [{ maxEpoch: null } as unknown as TRow] };
        }
        if (isLeaseInsert(sql)) {
          return { rows: [leaseDbRow(1)] as TRow[] };
        }
        return { rows: [] };
      }
    })(0);

    const claim = await claimControlLease(scriptedDatabase(client), {
      controlChannel: "ws",
      instanceId: "inst_a",
      leaseTtlMs: 60_000,
      participantId: "part_1",
      sessionId: "sess_1",
    });

    if (claim.status === "conflict") {
      throw new Error("unexpected conflict");
    }
    expect(claim.status).toBe("claimed");
    expect(claim.lease.epoch).toBe(1);
  });
});
