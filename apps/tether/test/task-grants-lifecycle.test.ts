import { describe, expect, it, vi } from "vitest";

import { createTaskGrantLifecycle, type TaskGrantActor } from "../src/auth/grant-lifecycle.js";
import type {
  AuthPersistenceStores,
  TaskGrantAuditRecord,
  TaskGrantRecord,
} from "../src/auth/grant-stores.js";

const admin: TaskGrantActor = { serviceAdmin: true, subject: "admin_one" };
const nonAdmin: TaskGrantActor = { serviceAdmin: false, subject: "worker_one" };

/** Builds an in-memory task-grant persistence seam with a shared audit log. */
function fakeTaskGrantStores(): AuthPersistenceStores & {
  readonly auditLog: TaskGrantAuditRecord[];
  readonly records: Map<string, TaskGrantRecord>;
} {
  const records = new Map<string, TaskGrantRecord>();
  const auditLog: TaskGrantAuditRecord[] = [];
  const stores: AuthPersistenceStores = {
    audits: { listForGrant: async () => [] },
    createGrantWithAudit: vi.fn(async () => undefined),
    createTaskGrantWithAudit: vi.fn(async ({ audit, grant }) => {
      records.set(grant.jti, grant);
      auditLog.push({ ...audit, taskGrantJti: grant.jti });
    }),
    grants: {
      findManyByJti: async () => [],
      findByJti: async () => null,
      list: async () => [],
    },
    revokeGrantWithAudit: vi.fn(async () => ({ grant: null, status: "not_found" }) as const),
    revokeTaskGrantWithAudit: vi.fn(async ({ audit, jti, revokedAt }) => {
      const record = records.get(jti);
      if (!record) return { grant: null, status: "not_found" } as const;
      if (record.revokedAt) return { grant: record, status: "already_revoked" } as const;
      const revoked = { ...record, revokedAt };
      records.set(jti, revoked);
      auditLog.push({ ...audit, taskGrantJti: jti });
      return { grant: revoked, status: "revoked" } as const;
    }),
    taskGrantAudits: {
      listForTaskGrant: async (taskGrantJti, limit) =>
        auditLog.filter((entry) => entry.taskGrantJti === taskGrantJti).slice(0, limit),
    },
    taskGrants: {
      findByJti: async (jti) => records.get(jti) ?? null,
      list: async (limit) => [...records.values()].slice(0, limit),
      listLiveForSubject: async (subject, action, now) =>
        [...records.values()].filter(
          (record) =>
            record.subject === subject &&
            record.action === action &&
            record.revokedAt === null &&
            record.expiresAt > now,
        ),
    },
    tickets: {
      consume: async () => null,
      create: async () => undefined,
      findByHash: async () => null,
    },
  };
  return { ...stores, auditLog, records };
}

function lifecycleFor(
  stores: AuthPersistenceStores,
  overrides: { issuanceEnabled?: boolean; issuer?: string | null; now?: () => Date } = {},
) {
  return createTaskGrantLifecycle({
    issuanceEnabled: true,
    issuer: "https://auth.tether.test",
    now: () => new Date("2026-09-15T00:00:00.000Z"),
    stores,
    ...overrides,
  });
}

describe("task grant lifecycle", () => {
  it("mints a 24 hour grant and joins creation to the grant audit", async () => {
    const stores = fakeTaskGrantStores();
    const lifecycle = lifecycleFor(stores);
    const created = await lifecycle.create({
      action: "task.create",
      actor: admin,
      kindAllowlist: ["build.widget"],
      reasonCode: "operator-request",
      sessionScope: "sess_one",
      subject: "worker_one",
    });

    expect(created.jti).toMatch(/^tgrant_/u);
    expect(created.action).toBe("task.create");
    expect(created.issuedAt).toBe("2026-09-15T00:00:00.000Z");
    expect(created.expiresAt).toBe("2026-09-16T00:00:00.000Z");
    expect(created.revokedAt).toBeNull();
    expect(created).not.toHaveProperty("metadata");

    const stored = stores.records.get(created.jti);
    expect(stored?.createdAuditId).toMatch(/^audit_/u);
    const trail = await stores.taskGrantAudits.listForTaskGrant(created.jti, 10);
    expect(trail.map((entry) => entry.action)).toEqual(["task_grant.created"]);
    expect(trail[0]?.auditId).toBe(stored?.createdAuditId);
    expect(trail[0]?.actorSubject).toBe("admin_one");
  });

  it("rejects non-admin actors on every operation", async () => {
    const stores = fakeTaskGrantStores();
    const lifecycle = lifecycleFor(stores);
    await expect(
      lifecycle.create({
        action: "task.create",
        actor: nonAdmin,
        reasonCode: "operator-request",
        sessionScope: "*",
        subject: "worker_one",
      }),
    ).rejects.toThrow("task_grant_admin_required");
    await expect(lifecycle.inspect(nonAdmin, "tgrant_x")).rejects.toThrow(
      "task_grant_admin_required",
    );
    await expect(lifecycle.list(nonAdmin, 10)).rejects.toThrow("task_grant_admin_required");
    await expect(lifecycle.revoke(nonAdmin, "tgrant_x", "operator-request")).rejects.toThrow(
      "task_grant_admin_required",
    );
    expect(stores.records.size).toBe(0);
  });

  it("gates issuance and rejects reserved operator kinds", async () => {
    const stores = fakeTaskGrantStores();
    await expect(
      lifecycleFor(stores, { issuanceEnabled: false }).create({
        action: "task.create",
        actor: admin,
        reasonCode: "operator-request",
        sessionScope: "*",
        subject: "worker_one",
      }),
    ).rejects.toThrow("task_grant_issuance_unavailable");
    await expect(
      lifecycleFor(stores, { issuer: null }).create({
        action: "task.create",
        actor: admin,
        reasonCode: "operator-request",
        sessionScope: "*",
        subject: "worker_one",
      }),
    ).rejects.toThrow("task_grant_issuance_unavailable");
    await expect(
      lifecycleFor(stores).create({
        action: "task.claim",
        actor: admin,
        kindAllowlist: ["operator.exec"],
        reasonCode: "operator-request",
        sessionScope: "*",
        subject: "worker_one",
      }),
    ).rejects.toThrow("task_grant_kind_reserved");
  });

  it("inspects, lists, and revokes idempotently with audit on first transition", async () => {
    const stores = fakeTaskGrantStores();
    const lifecycle = lifecycleFor(stores);
    const created = await lifecycle.create({
      action: "task.claim",
      actor: admin,
      reasonCode: "operator-request",
      sessionScope: "*",
      subject: "worker_one",
    });

    expect(await lifecycle.inspect(admin, created.jti)).toEqual(created);
    expect(await lifecycle.inspect(admin, "tgrant_missing")).toBeNull();
    expect(await lifecycle.list(admin, 10)).toEqual([created]);

    const first = await lifecycle.revoke(admin, created.jti, "operator-request");
    expect(first.status).toBe("revoked");
    expect(first.grant?.revokedAt).toBe("2026-09-15T00:00:00.000Z");
    const second = await lifecycle.revoke(admin, created.jti, "operator-request");
    expect(second.status).toBe("already_revoked");
    expect(await lifecycle.revoke(admin, "tgrant_missing", "operator-request")).toEqual({
      grant: null,
      status: "not_found",
    });

    const trail = await stores.taskGrantAudits.listForTaskGrant(created.jti, 10);
    expect(trail.map((entry) => entry.action)).toEqual([
      "task_grant.created",
      "task_grant.revoked",
    ]);
  });
});
