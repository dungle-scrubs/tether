import { describe, expect, it } from "vitest";

import { type DatabasePool, recordTaskApproval } from "../src/db.js";

const target = {
  action: "action_opaque_1",
  digest: "digest_opaque_1",
  scopeKey: "scope_opaque_1",
  targetId: "target_opaque_1",
  targetKind: "kind_opaque_1",
  targetRevision: "revision_opaque_1",
} as const;

class ManifestApprovalClient {
  readonly queries: string[] = [];

  constructor(
    private readonly taskOverrides: Record<string, unknown> = {
      result: { targetManifest: [] },
    },
    private readonly insertedApproval: Record<string, unknown> | null = null,
    private readonly canonicalApproval: Record<string, unknown> | null = null,
  ) {}

  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql);
    if (sql.includes("FROM tasks")) {
      return {
        rows: [
          completedTaskRow(
            (this.taskOverrides.result as Record<string, unknown> | undefined) ?? {},
            this.taskOverrides,
          ) as TRow,
        ],
      };
    }
    if (sql.includes("INSERT INTO task_approvals")) {
      return { rows: this.insertedApproval ? ([this.insertedApproval] as TRow[]) : [] };
    }
    if (sql.includes("FROM task_approvals")) {
      return { rows: this.canonicalApproval ? ([this.canonicalApproval] as TRow[]) : [] };
    }
    return { rows: [] };
  }

  release(): void {}
}

function completedTaskRow(
  result: Record<string, unknown>,
  overrides: Record<string, unknown> = {},
) {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: new Date("2026-08-01T00:01:00.000Z"),
    createdAt: new Date("2026-08-01T00:00:00.000Z"),
    failedAt: null,
    failure: null,
    input: null,
    kind: "opaque_review",
    objective: "Review an opaque target",
    releasedAt: null,
    releasedBy: null,
    result,
    scheduleAlgorithmVersion: null,
    scheduleIdentityVersion: null,
    scheduleIntervalMs: null,
    scheduleScopeKey: null,
    scheduleWindowStart: null,
    sessionId: "sess_manifest",
    taskId: "task_manifest",
    ...overrides,
  };
}

function database(client: ManifestApprovalClient): DatabasePool {
  return {
    pool: { connect: async () => client },
  } as unknown as DatabasePool;
}

describe("target-manifest approval commit", () => {
  it("rejects a targetless approval when the completed result declares targets", async () => {
    const client = new ManifestApprovalClient({ result: { targetManifest: [target] } });

    await expect(
      recordTaskApproval(database(client), {
        decision: "approved",
        eventSourceId: "src_manifest_test",
        participantId: "operator_1",
        reason: {},
        sessionId: "sess_manifest",
        taskId: "task_manifest",
      }),
    ).rejects.toMatchObject({
      name: "ApprovalTargetManifestError",
      reason: "target_required",
    });
    expect(client.queries.some((sql) => sql.includes("INSERT INTO task_approvals"))).toBe(false);
  });

  it("rejects a target absent from the completed result manifest before insert", async () => {
    const client = new ManifestApprovalClient();

    await expect(
      recordTaskApproval(database(client), {
        decision: "approved",
        eventSourceId: "src_manifest_test",
        participantId: "operator_1",
        reason: {},
        sessionId: "sess_manifest",
        target,
        taskId: "task_manifest",
      }),
    ).rejects.toMatchObject({
      name: "ApprovalTargetManifestError",
      reason: "target_absent",
    });
    expect(client.queries.some((sql) => sql.includes("INSERT INTO task_approvals"))).toBe(false);
    expect(client.queries.at(-1)).toBe("ROLLBACK");
  });

  it.each([
    ["target_kind_mismatch", { targetKind: "kind_other" }],
    ["scope_mismatch", { scopeKey: "scope_other" }],
    ["target_revision_mismatch", { targetRevision: "revision_other" }],
    ["action_mismatch", { action: "action_other" }],
    ["digest_mismatch", { digest: "digest_other" }],
  ] as const)("rejects %s before insert", async (reason, override) => {
    const client = new ManifestApprovalClient({ result: { targetManifest: [target] } });

    await expect(
      recordTaskApproval(database(client), {
        decision: "approved",
        eventSourceId: "src_manifest_test",
        participantId: "operator_1",
        reason: {},
        sessionId: "sess_manifest",
        target: { ...target, ...override },
        taskId: "task_manifest",
      }),
    ).rejects.toMatchObject({ name: "ApprovalTargetManifestError", reason });
    expect(client.queries.some((sql) => sql.includes("INSERT INTO task_approvals"))).toBe(false);
  });

  it("rejects a manifest target when the task is not durably completed", async () => {
    const client = new ManifestApprovalClient({
      completedAt: null,
      result: { targetManifest: [target] },
    });

    await expect(
      recordTaskApproval(database(client), {
        decision: "approved",
        eventSourceId: "src_manifest_test",
        participantId: "operator_1",
        reason: {},
        sessionId: "sess_manifest",
        target,
        taskId: "task_manifest",
      }),
    ).rejects.toMatchObject({
      name: "ApprovalTargetManifestError",
      reason: "task_not_completed",
    });
    expect(client.queries.some((sql) => sql.includes("INSERT INTO task_approvals"))).toBe(false);
  });

  it("returns one canonical approval record for identical and contradictory decisions", async () => {
    const canonicalRow = {
      approvalEventId: "evt_canonical",
      decidedAt: new Date("2026-08-01T00:02:00.000Z"),
      decidedByParticipantId: "operator_first",
      decision: "approved",
      reason: { source: "first" },
      sessionId: "sess_manifest",
      targetKey: "target_key_canonical",
      taskId: "task_manifest",
    };
    const taskOverrides = { result: { targetManifest: [target] } };
    const identical = await recordTaskApproval(
      database(new ManifestApprovalClient(taskOverrides, null, canonicalRow)),
      {
        decision: "approved",
        eventSourceId: "src_manifest_test",
        participantId: "operator_second",
        reason: { source: "second" },
        sessionId: "sess_manifest",
        target,
        taskId: "task_manifest",
      },
    );
    const contradictory = await recordTaskApproval(
      database(new ManifestApprovalClient(taskOverrides, null, canonicalRow)),
      {
        decision: "rejected",
        eventSourceId: "src_manifest_test",
        participantId: "operator_third",
        reason: { source: "third" },
        sessionId: "sess_manifest",
        target,
        taskId: "task_manifest",
      },
    );

    expect(contradictory?.approval).toEqual(identical?.approval);
    expect(identical?.approval).toMatchObject({
      decidedAt: "2026-08-01T00:02:00.000Z",
      decidedByParticipantId: "operator_first",
      decision: "approved",
    });
    expect(contradictory?.task).toEqual(identical?.task);
  });
});
