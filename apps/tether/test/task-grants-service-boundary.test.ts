import { readFile } from "node:fs/promises";

import { Effect } from "effect";
import { describe, expect, it, vi } from "vitest";

import type { TaskGrantActor } from "../src/auth/grant-lifecycle.js";
import { createTaskGrantLifecycle } from "../src/auth/grant-lifecycle.js";
import type { AuthPersistenceStores, TaskGrantRecord } from "../src/auth/grant-stores.js";
import { authorizeParticipantIdentity, effectiveParticipantId } from "../src/auth/authorize.js";
import { checkTaskGrantWithClient, TaskGrantDeniedError } from "../src/auth/task-grants-policy.js";
import { AuthError, type AuthContext } from "../src/auth/token.js";
import {
  cancelTaskWithEvent,
  claimTaskWithEvent,
  completeTaskWithEvent,
  createTaskWithEventIdempotent,
  type DatabasePool,
  ensureScheduledRunWithEvents,
  expireTaskClaims,
  refreshTaskClaim,
} from "../src/db.js";
import { ModuleObservability } from "../src/observability.js";
import type { SessionPersistenceStores } from "../src/db-store-contracts.js";
import {
  createSessionTaskEffects,
  mapTaskMutationRejection,
} from "../src/session-service-task-effects.js";
import { shouldClaimParticipantTask, type TaskRecord } from "../src/client.js";

const issuedAt = new Date("2026-01-01T00:00:00.000Z");
const liveExpiresAt = new Date("2030-01-01T00:00:00.000Z");
const pastExpiresAt = new Date("2000-01-01T00:00:00.000Z");

function grant(overrides: Partial<TaskGrantRecord> = {}): TaskGrantRecord {
  return {
    action: "task.claim",
    createdAuditId: "audit_seed",
    expiresAt: liveExpiresAt,
    issuedAt,
    issuer: "https://auth.tether.test",
    jti: "tgrant_seed",
    kindAllowlist: [],
    revokedAt: null,
    scopeLabelAllowlist: [],
    sessionScope: "*",
    subject: "worker_one",
    ...overrides,
  };
}

type PgRow = Record<string, unknown>;

function pgTaskRow(overrides: PgRow = {}): PgRow {
  return {
    assigneeParticipantId: null,
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: new Date("2026-09-15T00:00:00.000Z"),
    failedAt: null,
    failure: null,
    input: null,
    kind: "build.widget",
    objective: "Build the widget",
    parentTaskId: null,
    releasedAt: null,
    releasedBy: null,
    result: null,
    scheduleAlgorithmVersion: null,
    scheduleIdentityVersion: null,
    scheduleIntervalMs: null,
    scheduleScopeKey: null,
    scheduleWindowStart: null,
    scopeLabel: null,
    sessionId: "sess_one",
    taskId: "task_one",
    ...overrides,
  };
}

interface GrantWorld {
  grants: TaskGrantRecord[];
  missingGrantTable: boolean;
  seq: number;
  statements: string[];
  tasks: Map<string, PgRow>;
}

function worldKey(sessionId: string, taskId: string): string {
  return `${sessionId}~${taskId}`;
}

function liveLease(row: PgRow | undefined): boolean {
  const expires = row?.claimExpiresAt;
  return expires instanceof Date && expires.getTime() > Date.now();
}

function nonterminal(row: PgRow): boolean {
  return row.completedAt === null && row.failedAt === null && row.cancelledAt === null;
}

function throwMissingTable(): never {
  throw Object.assign(new Error("relation task_grants does not exist"), { code: "42P01" });
}

/** Stateful scripted pool: durable task rows plus the grant table, no PostgreSQL. */
function route(world: GrantWorld, text: string, values: readonly unknown[]): { rows: PgRow[] } {
  world.statements.push(text);
  const normalized = text.replaceAll(/\s+/gu, " ");
  if (/\bBEGIN\b|\bCOMMIT\b|\bROLLBACK\b/u.test(normalized)) {
    return { rows: [] };
  }
  if (normalized.includes("pg_advisory_xact_lock")) {
    return { rows: [] };
  }
  if (normalized.includes("clock_timestamp()")) {
    return { rows: [{ now: new Date() }] };
  }
  if (normalized.includes("task_grants")) {
    if (world.missingGrantTable) {
      throwMissingTable();
    }
    if (normalized.includes("LIMIT 1")) {
      return { rows: world.grants.length > 0 ? [{ present: 1 }] : [] };
    }
    const [subject, action] = values;
    return {
      rows: world.grants
        .filter((entry) => entry.subject === subject && entry.action === action)
        .map((entry) => ({ ...entry })),
    };
  }
  if (normalized.includes("session_projections")) {
    return { rows: [] };
  }
  if (normalized.includes("INSERT INTO session_events")) {
    const [eventId, payload, producerId, seq, sessionId, type] = values as [
      string,
      string,
      string,
      number,
      string,
      string,
    ];
    return {
      rows: [
        {
          createdAt: new Date(),
          eventId,
          payload: JSON.parse(payload) as unknown,
          producerId,
          seq,
          sessionId,
          type,
        },
      ],
    };
  }
  if (normalized.includes("FROM session_events")) {
    return { rows: [] };
  }
  if (normalized.includes("FROM sessions")) {
    return { rows: [{ exists: true }] };
  }
  if (normalized.includes("UPDATE session_event_sequences")) {
    world.seq += 1;
    return { rows: [{ seq: world.seq }] };
  }
  if (normalized.includes("SELECT pg_notify")) {
    return { rows: [] };
  }
  if (normalized.includes("FOR UPDATE SKIP LOCKED")) {
    return { rows: [] };
  }
  if (normalized.includes("INSERT INTO tasks")) {
    const row = pgTaskRow({
      assigneeParticipantId: (values[0] as string | null) ?? null,
      input:
        values[1] === null || values[1] === undefined
          ? null
          : (JSON.parse(values[1] as string) as unknown),
      kind: values[2] as string,
      objective: values[3] as string,
      parentTaskId: (values[6] as string | null) ?? null,
      scheduleAlgorithmVersion: (values[7] as number | null) ?? null,
      scheduleIdentityVersion: (values[8] as number | null) ?? null,
      scheduleIntervalMs: (values[9] as number | null) ?? null,
      scheduleScopeKey: (values[10] as string | null) ?? null,
      scheduleWindowStart: (values[11] as number | null) ?? null,
      scopeLabel: (values[12] as string | null) ?? null,
      sessionId: values[13] as string,
      taskId: values[14] as string,
    });
    world.tasks.set(worldKey(row.sessionId as string, row.taskId as string), row);
    return { rows: [{ ...row }] };
  }
  if (normalized.includes("FROM tasks") && normalized.includes("FOR UPDATE")) {
    const row = world.tasks.get(worldKey(values[0] as string, values[1] as string));
    return { rows: row ? [{ ...row, databaseNow: new Date() }] : [] };
  }
  if (normalized.includes("schedule_window_start >")) {
    return { rows: [] };
  }
  if (normalized.includes("FROM tasks") && normalized.includes("LIMIT 1")) {
    const row = world.tasks.get(worldKey(values[0] as string, values[1] as string));
    return { rows: row ? [{ ...row }] : [] };
  }
  if (normalized.includes("UPDATE tasks")) {
    if (normalized.includes("FROM expired")) {
      return { rows: [] };
    }
    if (normalized.includes("schedule_window_start <")) {
      return { rows: [] };
    }
    const taskRow = (sessionIndex: number, taskIndex: number): PgRow | undefined =>
      world.tasks.get(worldKey(values[sessionIndex] as string, values[taskIndex] as string));
    if (normalized.includes("claimed_by = NULL")) {
      const row = taskRow(0, 1);
      if (!row) {
        return { rows: [] };
      }
      row.claimExpiredAt = new Date();
      row.claimExpiredBy = values[2] as string;
      row.claimExpiresAt = null;
      row.claimId = null;
      row.claimedAt = null;
      row.claimedBy = null;
      row.releasedAt = null;
      row.releasedBy = null;
      return { rows: [{ ...row }] };
    }
    if (normalized.includes("claim_id = $4")) {
      const row = taskRow(0, 1);
      if (!row) {
        return { rows: [] };
      }
      const ttlMs = values[2] as number;
      row.claimExpiredAt = null;
      row.claimExpiredBy = null;
      row.claimExpiresAt = new Date(Date.now() + ttlMs);
      row.claimId = values[3] as string;
      row.claimedAt = new Date();
      row.claimedBy = values[4] as string;
      row.releasedAt = null;
      row.releasedBy = null;
      return { rows: [{ ...row }] };
    }
    if (normalized.includes("SET claim_expires_at = now()")) {
      const row = taskRow(1, 2);
      const participantId = values[3] as string;
      const claimId = values[4] as string;
      if (
        !row ||
        !nonterminal(row) ||
        row.claimedBy !== participantId ||
        row.claimId !== claimId ||
        !liveLease(row)
      ) {
        return { rows: [] };
      }
      row.claimExpiresAt = new Date(Date.now() + (values[0] as number));
      return { rows: [{ ...row }] };
    }
    if (normalized.includes("completed_at = now()")) {
      const row = taskRow(1, 2);
      const participantId = values[3] as string;
      const claimId = values[4] as string;
      if (
        !row ||
        !nonterminal(row) ||
        row.claimedBy !== participantId ||
        row.claimId !== claimId ||
        !liveLease(row)
      ) {
        return { rows: [] };
      }
      row.claimExpiresAt = null;
      row.completedAt = new Date();
      row.result = JSON.parse(values[0] as string) as unknown;
      return { rows: [{ ...row }] };
    }
    if (normalized.includes("failed_at = now()")) {
      const row = taskRow(1, 2);
      const participantId = values[3] as string;
      const claimId = values[4] as string;
      if (
        !row ||
        !nonterminal(row) ||
        row.claimedBy !== participantId ||
        row.claimId !== claimId ||
        !liveLease(row)
      ) {
        return { rows: [] };
      }
      row.claimExpiresAt = null;
      row.failedAt = new Date();
      row.failure = JSON.parse(values[0] as string) as unknown;
      return { rows: [{ ...row }] };
    }
    if (normalized.includes("released_at = now()")) {
      const row = taskRow(0, 1);
      const participantId = values[2] as string;
      const claimId = values[3] as string;
      if (
        !row ||
        !nonterminal(row) ||
        row.claimedBy !== participantId ||
        row.claimId !== claimId ||
        !liveLease(row)
      ) {
        return { rows: [] };
      }
      row.claimExpiredAt = null;
      row.claimExpiredBy = null;
      row.claimExpiresAt = null;
      row.claimId = null;
      row.claimedAt = null;
      row.claimedBy = null;
      row.releasedAt = new Date();
      row.releasedBy = participantId;
      return { rows: [{ ...row }] };
    }
    if (normalized.includes("cancelled_at = now()")) {
      const row = taskRow(0, 1);
      if (!row || !nonterminal(row)) {
        return { rows: [] };
      }
      row.cancelledAt = new Date();
      row.claimExpiresAt = null;
      return { rows: [{ ...row }] };
    }
  }
  throw new Error(`unexpected statement: ${normalized}`);
}

function makeWorld(overrides: Partial<GrantWorld> = {}): GrantWorld {
  return {
    grants: [],
    missingGrantTable: false,
    seq: 0,
    statements: [],
    tasks: new Map(),
    ...overrides,
  };
}

function poolFor(world: GrantWorld): DatabasePool {
  const query = async (text: string, values?: readonly unknown[]): Promise<{ rows: PgRow[] }> =>
    route(world, text, values ?? []);
  const client = { query, release: () => undefined };
  const drizzleStub = {
    update: () => ({
      set: () => ({
        where: () => ({
          returning: async () => [],
        }),
      }),
    }),
  };
  return {
    db: drizzleStub as unknown as DatabasePool["db"],
    end: async () => undefined,
    pool: {
      connect: async () => client,
      query,
    } as unknown as DatabasePool["pool"],
  };
}

function grantStatements(world: GrantWorld): string[] {
  return world.statements.filter((statement) => statement.includes("task_grants"));
}

function grantPoolReads(world: GrantWorld): string[] {
  return grantStatements(world).filter((statement) => !statement.includes("LIMIT 1"));
}

const claimInput = {
  claimLeaseTtlMs: 30_000,
  eventSourceId: "src_boundary_test",
  participantId: "worker_one",
  sessionId: "sess_one",
  taskId: "task_one",
} as const;

function seedTask(world: GrantWorld, overrides: PgRow = {}): void {
  const row = pgTaskRow(overrides);
  world.tasks.set(worldKey(row.sessionId as string, row.taskId as string), row);
}

async function denyReason(promise: Promise<unknown>): Promise<string | null> {
  try {
    await promise;
    return null;
  } catch (error) {
    return error instanceof TaskGrantDeniedError ? error.reason : `unexpected:${String(error)}`;
  }
}

describe("service boundary: auth-enabled positives", () => {
  it("creates without a claim when the creator holds a task.create grant", async () => {
    const world = makeWorld({
      grants: [grant({ action: "task.create", jti: "tgrant_create" })],
    });
    const result = await createTaskWithEventIdempotent(poolFor(world), {
      actorParticipantId: "worker_one",
      eventSourceId: "src_boundary_test",
      kind: "build.widget",
      objective: "Build the widget",
      sessionId: "sess_one",
      taskGrantAuthMode: "required",
      taskId: "task_one",
      taskIdSource: "caller",
    });
    expect(result.status).toBe("created");
    expect(result.task?.claimedBy).toBeNull();
    expect(result.task?.claimId).toBeNull();
    expect(result.events).toHaveLength(1);
    expect(world.statements.some((statement) => statement.includes("INSERT INTO tasks"))).toBe(
      true,
    );
  });

  it("claims a task carrying a parent link when the grant covers the claim", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world, { parentTaskId: "task_parent" });
    const result = await claimTaskWithEvent(poolFor(world), {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    expect(result?.task.claimedBy).toBe("worker_one");
    expect(result?.task.parentTaskId).toBe("task_parent");
    expect(result?.events).toHaveLength(1);
  });

  it("claims an explicitly assigned task when the assignee holds a kind-matched grant", async () => {
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_assignee", kindAllowlist: ["build.widget"] })],
    });
    seedTask(world, { assigneeParticipantId: "worker_one" });
    const result = await claimTaskWithEvent(poolFor(world), {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    expect(result?.task.claimedBy).toBe("worker_one");
  });

  it("denies a kind-matched non-assignee on an explicitly assigned task", async () => {
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_assignee", kindAllowlist: ["build.widget"] })],
    });
    seedTask(world, { assigneeParticipantId: "worker_two" });
    await expect(
      claimTaskWithEvent(poolFor(world), { ...claimInput, taskGrantAuthMode: "required" }),
    ).rejects.toBeInstanceOf(TaskGrantDeniedError);
    await expect(
      claimTaskWithEvent(poolFor(world), { ...claimInput, taskGrantAuthMode: "required" }),
    ).rejects.toMatchObject({ reason: "unauthorized_assignee" });
    expect(world.statements.some((statement) => statement.includes("UPDATE tasks"))).toBe(false);
  });

  it("refreshes a live lease while the holder keeps a live grant", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world);
    const claimed = await claimTaskWithEvent(poolFor(world), {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    const claimId = claimed?.task.claimId;
    expect(typeof claimId).toBe("string");
    const refreshed = await refreshTaskClaim(poolFor(world), {
      claimId: claimId as string,
      claimLeaseTtlMs: 30_000,
      participantId: "worker_one",
      sessionId: "sess_one",
      taskGrantAuthMode: "required",
      taskId: "task_one",
    });
    expect(refreshed?.claimId).toBe(claimId);
  });

  it("denies operator-kind creates at the service boundary even with a live grant", async () => {
    const world = makeWorld({
      grants: [grant({ action: "task.create", jti: "tgrant_create" })],
    });
    await expect(
      createTaskWithEventIdempotent(poolFor(world), {
        actorParticipantId: "worker_one",
        eventSourceId: "src_boundary_test",
        kind: "operator.exec",
        objective: "Operate",
        sessionId: "sess_one",
        taskGrantAuthMode: "required",
        taskId: "task_one",
        taskIdSource: "caller",
      }),
    ).rejects.toMatchObject({ reason: "unauthorized_kind" });
    expect(world.statements.some((statement) => statement.includes("INSERT INTO tasks"))).toBe(
      false,
    );
  });
});

describe("service boundary: concurrent claims apply exactly once", () => {
  it("lets one claimant win and keeps the loser a typeless rejection", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world);
    const pool = poolFor(world);
    const winner = await claimTaskWithEvent(pool, { ...claimInput, taskGrantAuthMode: "required" });
    expect(winner?.task.claimedBy).toBe("worker_one");
    const loser = await claimTaskWithEvent(pool, {
      ...claimInput,
      eventSourceId: "src_boundary_loser",
      participantId: "worker_two",
      taskGrantAuthMode: "required",
    });
    expect(loser).toBeNull();
    const stored = world.tasks.get(worldKey("sess_one", "task_one"));
    expect(stored?.claimedBy).toBe("worker_one");
    // Only the winner consulted the grant pool; the live-claim loss never did.
    expect(grantPoolReads(world)).toHaveLength(1);
  });
});

describe("service boundary: live-claim completion fence", () => {
  it("completes with the current claim id without consulting grants", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world);
    const pool = poolFor(world);
    const claimed = await claimTaskWithEvent(pool, {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    const claimId = claimed?.task.claimId as string;
    const grantsBefore = world.statements.length;
    const completed = await completeTaskWithEvent(pool, {
      claimId,
      eventSourceId: "src_boundary_test",
      participantId: "worker_one",
      result: { ok: true },
      sessionId: "sess_one",
      taskId: "task_one",
    });
    if (!completed) {
      throw new Error("expected the live-claim completion to apply");
    }
    expect(completed.task.completedAt).not.toBeNull();
    expect(world.statements.slice(grantsBefore).some((s) => s.includes("task_grants"))).toBe(false);
  });

  it("rejects a stale claim id as a typeless null without consulting grants", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world);
    const pool = poolFor(world);
    await claimTaskWithEvent(pool, { ...claimInput, taskGrantAuthMode: "required" });
    const before = world.statements.length;
    const stale = await completeTaskWithEvent(pool, {
      claimId: "claim_stale",
      eventSourceId: "src_boundary_test",
      participantId: "worker_one",
      result: { ok: true },
      sessionId: "sess_one",
      taskId: "task_one",
    });
    expect(stale).toBeNull();
    expect(world.statements.slice(before).some((s) => s.includes("task_grants"))).toBe(false);
  });

  it("rejects completion after the lease elapsed as a typeless null", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world);
    const pool = poolFor(world);
    const claimed = await claimTaskWithEvent(pool, {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    const claimId = claimed?.task.claimId as string;
    const stored = world.tasks.get(worldKey("sess_one", "task_one"));
    if (!stored) {
      throw new Error("expected a stored task row");
    }
    stored.claimExpiresAt = new Date("2000-01-01T00:00:00.000Z");
    const before = world.statements.length;
    await expect(
      completeTaskWithEvent(pool, {
        claimId,
        eventSourceId: "src_boundary_test",
        participantId: "worker_one",
        result: { ok: true },
        sessionId: "sess_one",
        taskId: "task_one",
      }),
    ).resolves.toBeNull();
    expect(world.statements.slice(before).some((s) => s.includes("task_grants"))).toBe(false);
  });
});

describe("service boundary: escalation negatives", () => {
  it("denies a direct claim whose rows bypass the subject filter with a typed reason", async () => {
    const unfiltered = {
      query: async () => ({
        rows: [{ ...grant({ kindAllowlist: ["other.kind"], subject: "worker_two" }) }],
      }),
    };
    const attempt = {
      action: "task.claim",
      kind: "build.widget",
      participantId: "worker_one",
      scopeLabel: null,
      sessionId: "sess_one",
      taskAssigneeParticipantId: null,
    } as const;
    const eligibility = await checkTaskGrantWithClient(unfiltered, attempt, new Date());
    expect(eligibility.eligible).toBe(false);
    if (!eligibility.eligible) {
      expect(eligibility.reason).toBe("task_grant_kind_denied");
    }
  });

  it("denies a subject-mismatched row even when the kind matches", async () => {
    const unfiltered = {
      query: async () => ({
        rows: [{ ...grant({ kindAllowlist: ["build.widget"], subject: "worker_two" }) }],
      }),
    };
    const eligibility = await checkTaskGrantWithClient(
      unfiltered,
      {
        action: "task.claim",
        kind: "build.widget",
        participantId: "worker_one",
        scopeLabel: null,
        sessionId: "sess_one",
        taskAssigneeParticipantId: null,
      },
      new Date(),
    );
    expect(eligibility).toEqual({ eligible: false, reason: "task_grant_assignee_denied" });
  });

  it("denies escalation via crafted task input without a grant", async () => {
    const world = makeWorld({
      grants: [grant({ action: "task.create", jti: "tgrant_other", subject: "worker_two" })],
    });
    const reason = await denyReason(
      createTaskWithEventIdempotent(poolFor(world), {
        actorParticipantId: "worker_one",
        eventSourceId: "src_boundary_test",
        input: { owner: "worker_one", role: "admin" },
        kind: "build.widget",
        objective: "Build the widget",
        sessionId: "sess_one",
        taskGrantAuthMode: "required",
        taskId: "task_one",
        taskIdSource: "caller",
      }),
    );
    expect(reason).toBe("unauthorized_assignee");
    expect(world.statements.some((statement) => statement.includes("INSERT INTO tasks"))).toBe(
      false,
    );
  });

  it("denies escalation via a child link to a task owned by someone else", async () => {
    const world = makeWorld({
      grants: [grant({ action: "task.create", jti: "tgrant_other", subject: "worker_two" })],
    });
    seedTask(world, { claimedBy: "worker_two", taskId: "task_parent" });
    const reason = await denyReason(
      createTaskWithEventIdempotent(poolFor(world), {
        actorParticipantId: "worker_one",
        eventSourceId: "src_boundary_test",
        kind: "build.widget",
        objective: "Child work",
        parentTaskId: "task_parent",
        sessionId: "sess_one",
        taskGrantAuthMode: "required",
        taskId: "task_child",
        taskIdSource: "caller",
      }),
    );
    expect(reason).toBe("unauthorized_assignee");
  });

  it("denies a claim on a task whose payload claims ownership without a grant", async () => {
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_other", subject: "worker_two" })],
    });
    seedTask(world, { input: { owner: "worker_one" } });
    const reason = await denyReason(
      claimTaskWithEvent(poolFor(world), { ...claimInput, taskGrantAuthMode: "required" }),
    );
    expect(reason).toBe("unauthorized_assignee");
  });
});

describe("service boundary: authenticated identity binding", () => {
  const context: AuthContext = {
    expiresAt: "2030-01-01T00:00:00.000Z",
    grantJti: null,
    grantSource: null,
    issuer: null,
    kid: "test_kid",
    participantId: "worker_one",
    role: "participant",
    sessionScope: "*",
  };

  it("rejects a mismatched body participant id before any grant check", () => {
    expect(authorizeParticipantIdentity(context, "worker_two")).toBe(AuthError.ScopeDenied);
    expect(authorizeParticipantIdentity(context, "worker_one")).toBeNull();
    expect(authorizeParticipantIdentity(null, "worker_two")).toBeNull();
  });

  it("binds the effective participant to the authenticated identity, never the body", () => {
    expect(effectiveParticipantId(context, "worker_two")).toBe("worker_one");
    expect(effectiveParticipantId(null, "worker_two")).toBe("worker_two");
  });

  it("serves WebSocket task.claim only as the socket identity", async () => {
    const source = await readFile(
      new URL("../src/websocket-participant-gateway.ts", import.meta.url),
      "utf8",
    );
    const claimBlock = source.slice(
      source.indexOf("webSocketOperation.taskClaim"),
      source.indexOf("webSocketOperation.taskCancel"),
    );
    expect(claimBlock).toContain("participantId: context.participantId");
    expect(claimBlock).not.toContain("body.participantId");
  });

  it("funnels REST claimTaskOverRest through the same claimTaskEffect as WebSocket claims", async () => {
    const source = await readFile(new URL("../src/session-service.ts", import.meta.url), "utf8");
    const restBlock = source.slice(
      source.indexOf("claimTaskOverRest"),
      source.indexOf("refreshTaskClaim:"),
    );
    expect(restBlock).toContain("claimTaskEffect");
  });

  it("maps a denied claim to the typed denial shared by every claim path", async () => {
    const stores = {
      clientBindings: {
        archive: async () => null,
        find: async () => null,
        list: async () => [],
        upsert: async () => {
          throw new Error("unexpected client binding upsert");
        },
      },
      controlLeases: {
        claim: async () => {
          throw new Error("unexpected control lease claim");
        },
        listSnapshots: async () => [],
        renew: async () => {
          throw new Error("unexpected control lease renew");
        },
        release: async () => undefined,
        releaseRest: async () => {
          throw new Error("unexpected REST control lease release");
        },
      },
      events: {
        append: async () => {
          throw new Error("unexpected event append");
        },
        appendIdempotent: async () => {
          throw new Error("unexpected idempotent append");
        },
        list: async () => [],
        listContextSuffix: async () => ({
          eligibleEventCount: 0,
          estimatedTokens: 0,
          events: [],
          truncated: false,
        }),
      },
      participants: {
        heartbeat: async () => null,
        heartbeatWithEvent: async () => ({ participant: null }),
        list: async () => [],
        listRuntimeSnapshots: async () => [],
        upsert: async () => {
          throw new Error("unexpected participant upsert");
        },
        upsertWithEvent: async () => {
          throw new Error("unexpected participant upsert with event");
        },
      },
      sessions: {
        create: async () => {
          throw new Error("unexpected session create");
        },
        delete: async () => ({ status: "deleted" }) as const,
        list: async () => [],
        read: async (sessionId: string) => ({
          createdAt: "2026-09-15T00:00:00.000Z",
          sessionId,
        }),
        readDebugSummary: async () => {
          throw new Error("unexpected debug summary read");
        },
      },
      tasks: {
        cancelWithEvent: async () => null,
        claimWithEvent: async () => {
          throw new TaskGrantDeniedError("unauthorized_assignee");
        },
        completeWithEvent: async () => null,
        createOperatorWithEvent: async () => {
          throw new Error("unexpected operator task create");
        },
        createWithEvent: async () => null,
        ensureScheduledRun: async () => {
          throw new Error("unexpected ensure scheduled run");
        },
        expireClaims: async () => [],
        failWithEvent: async () => null,
        get: async () => null,
        list: async () => [],
        listSnapshots: async () => [],
        recordApproval: async () => null,
        refreshClaim: async () => null,
        releaseWithEvent: async () => null,
        supersedeScheduled: async () => ({ events: [], tasks: [] }),
      },
    } as unknown as SessionPersistenceStores;
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: () => undefined,
      eventSourceId: "src_boundary_test",
      observability: new ModuleObservability({ moduleName: "TaskGrantsBoundaryTest" }),
      stores,
      taskClaimLeaseTtlMs: 30_000,
    });
    const result = await Effect.runPromise(
      mapTaskMutationRejection(
        effects.claimTaskEffect({
          participantId: "worker_one",
          sessionId: "sess_one",
          taskId: "task_one",
        }),
      ),
    );
    expect(result).toEqual({
      events: [],
      reason: "unauthorized_assignee",
      status: "denied",
      task: null,
    });
  });
});

describe("service boundary: only service-scoped admins mint task grants", () => {
  function fakeStores() {
    const records = new Map<string, TaskGrantRecord>();
    const stores: AuthPersistenceStores = {
      audits: { listForGrant: async () => [] },
      createGrantWithAudit: vi.fn(async () => undefined),
      createTaskGrantWithAudit: vi.fn(async ({ grant }) => {
        records.set(grant.jti, grant);
      }),
      grants: {
        findByJti: async () => null,
        findManyByJti: async () => [],
        list: async () => [],
      },
      revokeGrantWithAudit: vi.fn(async () => ({ grant: null, status: "not_found" }) as const),
      revokeTaskGrantWithAudit: vi.fn(async () => ({ grant: null, status: "not_found" }) as const),
      taskGrantAudits: { listForTaskGrant: async () => [] },
      taskGrants: {
        findByJti: async (jti: string) => records.get(jti) ?? null,
        list: async (limit: number) => [...records.values()].slice(0, limit),
        listLiveForSubject: async () => [],
      },
      tickets: {
        consume: async () => null,
        create: async () => undefined,
        findByHash: async () => null,
      },
    };
    return stores;
  }

  function lifecycle() {
    return createTaskGrantLifecycle({
      issuanceEnabled: true,
      issuer: "https://auth.tether.test",
      now: () => new Date("2026-09-15T00:00:00.000Z"),
      stores: fakeStores(),
    });
  }

  const nonAdmin: TaskGrantActor = { serviceAdmin: false, subject: "worker_one" };
  const sessionScopedAdmin: TaskGrantActor = { serviceAdmin: false, subject: "admin_sess" };

  it.each([
    ["non-admin", nonAdmin],
    ["session-scoped admin", sessionScopedAdmin],
  ] as const)("denies %s on mint", async (_label, actor) => {
    await expect(
      lifecycle().create({
        action: "task.claim",
        actor,
        reasonCode: "operator-request",
        sessionScope: "*",
        subject: "worker_one",
      }),
    ).rejects.toThrow("task_grant_admin_required");
  });

  it.each([
    ["non-admin", nonAdmin],
    ["session-scoped admin", sessionScopedAdmin],
  ] as const)("denies %s on inspect, list, and revoke", async (_label, actor) => {
    const api = lifecycle();
    await expect(api.inspect(actor, "tgrant_x")).rejects.toThrow("task_grant_admin_required");
    await expect(api.list(actor, 10)).rejects.toThrow("task_grant_admin_required");
    await expect(api.revoke(actor, "tgrant_x", "operator-request")).rejects.toThrow(
      "task_grant_admin_required",
    );
  });
});

describe("service boundary: revocation mid-lease", () => {
  it("denies refresh, keeps one terminal write, then fences post-expiry completion", async () => {
    const held = grant({ jti: "tgrant_held" });
    const world = makeWorld({ grants: [held] });
    seedTask(world);
    const pool = poolFor(world);
    const claimed = await claimTaskWithEvent(pool, {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    const claimId = claimed?.task.claimId as string;

    world.grants = [{ ...held, revokedAt: new Date("2026-09-15T12:00:00.000Z") }];

    const refreshMark = world.statements.length;
    const refreshFailure = await denyReason(
      refreshTaskClaim(pool, {
        claimId,
        claimLeaseTtlMs: 30_000,
        participantId: "worker_one",
        sessionId: "sess_one",
        taskGrantAuthMode: "required",
        taskId: "task_one",
      }),
    );
    expect(refreshFailure).toBe("grant_revoked");
    expect(
      world.statements
        .slice(refreshMark)
        .some((statement) => statement.includes("SET claim_expires_at")),
    ).toBe(false);

    const completeMark = world.statements.length;
    const completed = await completeTaskWithEvent(pool, {
      claimId,
      eventSourceId: "src_boundary_test",
      participantId: "worker_one",
      result: { ok: true },
      sessionId: "sess_one",
      taskId: "task_one",
    });
    if (!completed) {
      throw new Error("expected the live-claim completion to apply");
    }
    expect(completed.task.completedAt).not.toBeNull();
    expect(world.statements.slice(completeMark).some((s) => s.includes("task_grants"))).toBe(false);
  });

  it("lets the coordinator cancel a live-claimed task without consulting grants", async () => {
    const held = grant({ jti: "tgrant_held" });
    const world = makeWorld({ grants: [held] });
    seedTask(world);
    const pool = poolFor(world);
    await claimTaskWithEvent(pool, { ...claimInput, taskGrantAuthMode: "required" });
    world.grants = [{ ...held, revokedAt: new Date("2026-09-15T12:00:00.000Z") }];
    const before = world.statements.length;
    const cancelled = await cancelTaskWithEvent(pool, {
      eventSourceId: "src_boundary_test",
      participantId: "coordinator",
      sessionId: "sess_one",
      taskId: "task_one",
    });
    if (!cancelled) {
      throw new Error("expected the coordinator cancel to apply");
    }
    expect(cancelled.task.cancelledAt).not.toBeNull();
    expect(world.statements.slice(before).some((s) => s.includes("task_grants"))).toBe(false);
  });
});

describe("service boundary: auth-disabled control", () => {
  it("creates, claims, and refreshes without touching the grant table", async () => {
    const world = makeWorld({
      grants: [grant({ action: "task.create", jti: "tgrant_deny", subject: "worker_two" })],
    });
    seedTask(world, { taskId: "task_claim" });
    const pool = poolFor(world);
    const created = await createTaskWithEventIdempotent(pool, {
      actorParticipantId: "worker_one",
      eventSourceId: "src_boundary_test",
      kind: "build.widget",
      objective: "Build the widget",
      sessionId: "sess_one",
      taskGrantAuthMode: "disabled",
      taskId: "task_one",
      taskIdSource: "caller",
    });
    expect(created.status).toBe("created");
    const claimed = await claimTaskWithEvent(pool, {
      ...claimInput,
      taskGrantAuthMode: "disabled",
      taskId: "task_claim",
    });
    expect(claimed?.task.claimedBy).toBe("worker_one");
    // Disabled refresh takes the legacy lease path: no grant gate, and the
    // stubbed lease store reports no matching row, so a typeless null.
    await expect(
      refreshTaskClaim(pool, {
        claimId: "claim_missing",
        claimLeaseTtlMs: 30_000,
        participantId: "worker_one",
        sessionId: "sess_one",
        taskGrantAuthMode: "disabled",
        taskId: "task_claim",
      }),
    ).resolves.toBeNull();
    expect(grantStatements(world)).toEqual([]);
  });
});

describe("service boundary: SDK selection stays advisory-only", () => {
  function adapterTask(overrides: Partial<TaskRecord> = {}): TaskRecord {
    return {
      cancelledAt: null,
      claimExpiredAt: null,
      claimExpiredBy: null,
      claimExpiresAt: null,
      claimId: null,
      claimedAt: null,
      claimedBy: null,
      completedAt: null,
      createdAt: "2026-09-15T00:00:00.000Z",
      failedAt: null,
      failure: null,
      input: null,
      kind: "build.widget",
      objective: "Build the widget",
      releasedAt: null,
      releasedBy: null,
      result: null,
      sessionId: "sess_one",
      taskId: "task_one",
      ...overrides,
    };
  }

  it("advises claiming a matching unclaimed task with no grant concept", () => {
    expect(shouldClaimParticipantTask(adapterTask(), ["build.widget"], "worker_one")).toBe(true);
    expect(shouldClaimParticipantTask(adapterTask(), ["other.kind"], "worker_one")).toBe(false);
    expect(
      shouldClaimParticipantTask(
        adapterTask({ claimedBy: "worker_two" }),
        ["build.widget"],
        "worker_one",
      ),
    ).toBe(false);
    expect(
      shouldClaimParticipantTask(
        adapterTask({ completedAt: "2026-09-15T12:00:00.000Z" }),
        ["build.widget"],
        "worker_one",
      ),
    ).toBe(false);
  });

  it("never authorizes the server claim: an advisory yes still denies without a grant", async () => {
    expect(shouldClaimParticipantTask(adapterTask(), ["build.widget"], "worker_one")).toBe(true);
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_other", subject: "worker_two" })],
    });
    seedTask(world);
    const reason = await denyReason(
      claimTaskWithEvent(poolFor(world), { ...claimInput, taskGrantAuthMode: "required" }),
    );
    expect(reason).toBe("unauthorized_assignee");
  });
});

describe("service boundary: replay and reconnect re-pass policy", () => {
  it("replays an identical create without consulting the grant pool", async () => {
    const world = makeWorld({
      grants: [grant({ action: "task.create", jti: "tgrant_other", subject: "worker_two" })],
    });
    seedTask(world);
    const pool = poolFor(world);
    const input = {
      actorParticipantId: "worker_one",
      eventSourceId: "src_boundary_test",
      kind: "build.widget",
      objective: "Build the widget",
      sessionId: "sess_one",
      taskGrantAuthMode: "required",
      taskId: "task_one",
      taskIdSource: "caller",
    } as const;
    const result = await createTaskWithEventIdempotent(pool, input);
    expect(result.status).toBe("replayed");
    expect(grantPoolReads(world)).toEqual([]);
  });

  it("re-checks policy when reconnecting to reclaim an elapsed claim", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world, {
      assigneeParticipantId: "worker_two",
      claimExpiresAt: new Date("2000-01-01T00:00:00.000Z"),
      claimId: "claim_old",
      claimedAt: new Date("2000-01-01T00:00:00.000Z"),
      claimedBy: "worker_two",
    });
    const reason = await denyReason(
      claimTaskWithEvent(poolFor(world), { ...claimInput, taskGrantAuthMode: "required" }),
    );
    // worker_two still owns the task binding, so worker_one is the assignee denial.
    expect(reason).toBe("unauthorized_assignee");
  });

  it("reclaims an elapsed claim for the bound holder and emits the ordered pair", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world, {
      claimExpiresAt: new Date("2000-01-01T00:00:00.000Z"),
      claimId: "claim_old",
      claimedAt: new Date("2000-01-01T00:00:00.000Z"),
      claimedBy: "worker_one",
    });
    const result = await claimTaskWithEvent(poolFor(world), {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    expect(result?.events).toHaveLength(2);
    expect(result?.task.claimedBy).toBe("worker_one");
    expect(result?.task.claimId).not.toBe("claim_old");
  });

  it("denies the reconnect reclaim after the grant elapsed mid-lease", async () => {
    const world = makeWorld({
      grants: [grant({ expiresAt: pastExpiresAt, jti: "tgrant_stale" })],
    });
    seedTask(world, {
      claimExpiresAt: new Date("2000-01-01T00:00:00.000Z"),
      claimId: "claim_old",
      claimedAt: new Date("2000-01-01T00:00:00.000Z"),
      claimedBy: "worker_one",
    });
    const reason = await denyReason(
      claimTaskWithEvent(poolFor(world), { ...claimInput, taskGrantAuthMode: "required" }),
    );
    expect(reason).toBe("grant_expired");
    expect(world.statements.some((statement) => statement.includes("UPDATE tasks"))).toBe(false);
  });
});

describe("service boundary: no-policy-rows compatibility", () => {
  it("keeps create and claim behavior with an empty grant table", async () => {
    const world = makeWorld();
    seedTask(world, { taskId: "task_claim" });
    const pool = poolFor(world);
    const created = await createTaskWithEventIdempotent(pool, {
      actorParticipantId: "worker_one",
      eventSourceId: "src_boundary_test",
      kind: "build.widget",
      objective: "Build the widget",
      sessionId: "sess_one",
      taskGrantAuthMode: "required",
      taskId: "task_one",
      taskIdSource: "caller",
    });
    expect(created.status).toBe("created");
    const claimed = await claimTaskWithEvent(pool, {
      ...claimInput,
      taskGrantAuthMode: "required",
      taskId: "task_claim",
    });
    expect(claimed?.task.claimedBy).toBe("worker_one");
    // Only the LIMIT-1 presence probes touched the table; no pool evaluation ran.
    expect(grantStatements(world).length).toBeGreaterThan(0);
    expect(grantPoolReads(world)).toEqual([]);
  });

  it("keeps claim behavior on a pre-migration database without the grant table", async () => {
    const world = makeWorld({ missingGrantTable: true });
    seedTask(world);
    const claimed = await claimTaskWithEvent(poolFor(world), {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    expect(claimed?.task.claimedBy).toBe("worker_one");
  });
});

describe("service boundary: system-owned paths never consult grants", () => {
  const scheduleInput = {
    eventSourceId: "src_boundary_test",
    kind: "email_organization",
    objective: "Organize the mailbox",
    participantId: "scheduler",
    scheduleAlgorithmVersion: 1,
    scheduleIdentityVersion: 1,
    scheduleIntervalMs: 3_600_000,
    scheduleScopeKey: "mailbox",
    scheduleWindowStart: 1_786_771_200_000,
    sessionId: "sess_one",
  } as const;

  it("ensures a scheduled run with policy rows present and no grant read", async () => {
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_other", subject: "worker_two" })],
    });
    const result = await ensureScheduledRunWithEvents(poolFor(world), scheduleInput);
    expect(result.current.status).toBe("created");
    expect(grantStatements(world)).toEqual([]);
  });

  it("expires claims via the sweeper with policy rows present and no grant read", async () => {
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_other", subject: "worker_two" })],
    });
    const events = await expireTaskClaims(poolFor(world), {
      batchSize: 10,
      sourceId: "src_boundary_test",
    });
    expect(events).toEqual([]);
    expect(grantStatements(world)).toEqual([]);
  });

  it("cancels a terminal-adjacent task via the coordinator with no grant read", async () => {
    const world = makeWorld({
      grants: [grant({ jti: "tgrant_other", subject: "worker_two" })],
    });
    seedTask(world);
    const cancelled = await cancelTaskWithEvent(poolFor(world), {
      eventSourceId: "src_boundary_test",
      participantId: "coordinator",
      sessionId: "sess_one",
      taskId: "task_one",
    });
    expect(cancelled?.task.cancelledAt).not.toBeNull();
    expect(grantStatements(world)).toEqual([]);
  });

  it("holds the enforced refresh inside one row-locked transaction", async () => {
    const world = makeWorld({ grants: [grant({ jti: "tgrant_claim" })] });
    seedTask(world);
    const pool = poolFor(world);
    const claimed = await claimTaskWithEvent(pool, {
      ...claimInput,
      taskGrantAuthMode: "required",
    });
    const claimId = claimed?.task.claimId as string;
    world.statements.length = 0;
    await refreshTaskClaim(pool, {
      claimId,
      claimLeaseTtlMs: 30_000,
      participantId: "worker_one",
      sessionId: "sess_one",
      taskGrantAuthMode: "required",
      taskId: "task_one",
    });
    const begin = world.statements.findIndex((statement) => statement.includes("BEGIN"));
    const lock = world.statements.findIndex((statement) => statement.includes("FOR UPDATE"));
    const poolRead = world.statements.findIndex(
      (statement) => statement.includes("FROM task_grants") && !statement.includes("LIMIT 1"),
    );
    const update = world.statements.findIndex((statement) => statement.includes("UPDATE tasks"));
    const commit = world.statements.findIndex((statement) => statement.includes("COMMIT"));
    for (const index of [begin, lock, poolRead, update, commit]) {
      expect(index).toBeGreaterThanOrEqual(0);
    }
    expect([begin, lock, poolRead, update, commit]).toEqual(
      [...[begin, lock, poolRead, update, commit]].sort((a, b) => a - b),
    );
  });
});
