import { describe, expect, it } from "vitest";

import { type DatabasePool, touchParticipantLastSeen } from "../src/db.js";

const touchSessionId = "sess_last_seen_touch";
const touchParticipantId = "part_last_seen_touch";

describe("participant last-seen touch", () => {
  it("updates last_seen_at and nothing else for a registered participant", async () => {
    const database = new RecordingParticipantPool(1);

    const touched = await touchParticipantLastSeen(database.pool, {
      participantId: touchParticipantId,
      sessionId: touchSessionId,
    });

    expect(touched).toBe(true);
    // One statement against the participant row only: no transaction, no
    // session event, no control lease or epoch write.
    expect(database.queries).toHaveLength(1);
    const [query] = database.queries;
    expect(query?.values).toEqual([touchSessionId, touchParticipantId]);
    expect(normalizeSql(query?.sql ?? "")).toBe(
      "UPDATE participants SET last_seen_at = clock_timestamp() WHERE session_id = $1 AND participant_id = $2",
    );
  });

  it("does nothing for a participant that is not registered", async () => {
    const database = new RecordingParticipantPool(0);

    const touched = await touchParticipantLastSeen(database.pool, {
      participantId: "part_never_registered",
      sessionId: touchSessionId,
    });

    expect(touched).toBe(false);
    expect(database.queries).toHaveLength(1);
    expect(normalizeSql(database.queries[0]?.sql ?? "")).toMatch(/^UPDATE participants SET/u);
    expect(database.queries.some((query) => /INSERT|session_events/u.test(query.sql))).toBe(false);
  });
});

/** One SQL statement the fake pool received. */
interface RecordedQuery {
  /** Statement text as sent. */
  readonly sql: string;
  /** Bound parameter values. */
  readonly values: readonly unknown[] | undefined;
}

/** Fake pool that records statements and reports a fixed affected-row count. */
class RecordingParticipantPool {
  readonly queries: RecordedQuery[] = [];
  readonly pool: DatabasePool;

  /** Creates a pool whose every statement reports `rowCount` affected rows. */
  constructor(private readonly rowCount: number) {
    this.pool = {
      pool: {
        connect: async () => {
          throw new Error("touchParticipantLastSeen must not open a transaction");
        },
        query: async (sql: string, values?: readonly unknown[]) => {
          this.queries.push({ sql, values });
          return { rowCount: this.rowCount, rows: [] };
        },
      },
    } as unknown as DatabasePool;
  }
}

/** Collapses whitespace so a statement compares independent of its layout. */
function normalizeSql(sql: string): string {
  return sql.replace(/\s+/gu, " ").trim();
}
