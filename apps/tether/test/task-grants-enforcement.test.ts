import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { TaskGrantRecord } from "../src/auth/grant-stores.js";
import {
  describeTaskGrantEnforcementDenial,
  enforceTaskGrantPolicyWithClient,
  hasTaskGrantPolicyRows,
  TaskGrantDeniedError,
  taskGrantEnforcementDenials,
  toTaskGrantEnforcementDenial,
  type TaskGrantAttempt,
  type TaskGrantDenialReason,
  type TaskGrantQueryClient,
} from "../src/auth/task-grants-policy.js";
import {
  claimTaskWithEvent,
  createTaskWithEventIdempotent,
  type DatabasePool,
  refreshTaskClaim,
} from "../src/db.js";
import type { SessionPersistenceStores } from "../src/db-store-contracts.js";
import { ModuleObservability } from "../src/observability.js";
import { sessionEventType } from "../src/protocol.js";
import { SessionServicePersistenceError } from "../src/session-service-contracts.js";
import { extractTaskGrantDenied } from "../src/session-service-runtime.js";
import {
  createSessionTaskEffects,
  mapTaskClaimRefreshRejection,
  mapTaskMutationRejection,
} from "../src/session-service-task-effects.js";
import type { SessionEvent, TaskRecord } from "../src/types.js";

const now = new Date("2026-09-15T00:00:00.000Z");
const issuedAt = new Date("2026-09-14T00:00:00.000Z");
const liveExpiresAt = new Date("2026-09-16T00:00:00.000Z");

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

function claimAttempt(
  overrides: Partial<Extract<TaskGrantAttempt, { action: "task.claim" }>> = {},
) {
  return {
    action: "task.claim",
    kind: "build.widget",
    participantId: "worker_one",
    scopeLabel: null,
    sessionId: "sess_one",
    taskAssigneeParticipantId: null,
    ...overrides,
  } as const;
}

function createAttempt(
  overrides: Partial<Extract<TaskGrantAttempt, { action: "task.create" }>> = {},
) {
  return {
    action: "task.create",
    actorParticipantId: "worker_one",
    assigneeParticipantId: null,
    kind: "build.widget",
    scopeLabel: null,
    sessionId: "sess_one",
    ...overrides,
  } as const;
}

/**
 * Scripted grant-table client emulating the enforcement SQL filters: the
 * presence probe reports whether any policy rows exist, and the pool read
 * filters by subject and action while still returning revoked, expired, and
 * out-of-scope rows for typed classification.
 */
function policyClient(grants: readonly TaskGrantRecord[]): {
  readonly client: TaskGrantQueryClient;
  readonly seen: string[];
} {
  const seen: string[] = [];
  const client: TaskGrantQueryClient = {
    query: async (text, values) => {
      seen.push(text);
      if (text.includes("LIMIT 1")) {
        return { rows: grants.length > 0 ? [{ present: 1 }] : [] } as unknown as {
          readonly rows: readonly never[];
        };
      }
      const [subject, action] = values as readonly unknown[];
      const rows = grants
        .filter((entry) => entry.subject === subject && entry.action === action)
        .map((entry) => ({ ...entry }));
      return { rows } as unknown as { readonly rows: readonly never[] };
    },
  };
  return { client, seen };
}

describe("task-grant enforcement denial taxonomy", () => {
  it("maps every internal reason onto the five public denials", () => {
    const mapping: Record<TaskGrantDenialReason, (typeof taskGrantEnforcementDenials)[number]> = {
      task_grant_action_denied: "unauthorized_assignee",
      task_grant_assignee_denied: "unauthorized_assignee",
      task_grant_expired: "grant_expired",
      task_grant_kind_denied: "unauthorized_kind",
      task_grant_kind_reserved: "unauthorized_kind",
      task_grant_no_grant: "unauthorized_assignee",
      task_grant_refresh_denied: "unauthorized_assignee",
      task_grant_revoked: "grant_revoked",
      task_grant_scope_label_denied: "unauthorized_scope",
      task_grant_session_denied: "unauthorized_scope",
    };
    for (const [internal, expected] of Object.entries(mapping)) {
      expect(toTaskGrantEnforcementDenial(internal as TaskGrantDenialReason)).toBe(expected);
    }
    expect([...taskGrantEnforcementDenials].sort()).toEqual(
      [
        "grant_expired",
        "grant_revoked",
        "unauthorized_assignee",
        "unauthorized_kind",
        "unauthorized_scope",
      ].sort(),
    );
  });

  it("describes every public denial with a stable message", () => {
    for (const reason of taskGrantEnforcementDenials) {
      expect(describeTaskGrantEnforcementDenial(reason).length).toBeGreaterThan(0);
    }
  });

  it("reports grant-table presence and tolerates a missing table", async () => {
    const present: TaskGrantQueryClient = {
      query: async () =>
        ({ rows: [{ exists: true }] }) as unknown as {
          readonly rows: readonly never[];
        },
    };
    await expect(hasTaskGrantPolicyRows(present)).resolves.toBe(true);
    const missing: TaskGrantQueryClient = {
      query: async () => {
        throw Object.assign(new Error("relation does not exist"), { code: "42P01" });
      },
    };
    await expect(hasTaskGrantPolicyRows(missing)).resolves.toBe(false);
  });
});

describe("task-grant enforcement funnel", () => {
  it("allows everything when auth is disabled without touching the table", async () => {
    const { client, seen } = policyClient([]);
    const outcome = await enforceTaskGrantPolicyWithClient(client, claimAttempt(), now, {
      authMode: "disabled",
    });
    expect(outcome).toEqual({ grant: null, status: "allowed" });
    expect(seen).toEqual([]);
  });

  it("allows everything when no policy rows exist", async () => {
    const { client, seen } = policyClient([]);
    const outcome = await enforceTaskGrantPolicyWithClient(client, claimAttempt(), now);
    expect(outcome).toEqual({ grant: null, status: "allowed" });
    expect(
      seen.filter(
        (statement) => statement.includes("FROM task_grants") && !statement.includes("LIMIT 1"),
      ),
    ).toEqual([]);
  });

  it("allows a live matching grant and returns it", async () => {
    const { client } = policyClient([grant()]);
    const outcome = await enforceTaskGrantPolicyWithClient(client, claimAttempt(), now);
    expect(outcome.status).toBe("allowed");
    if (outcome.status === "allowed") {
      expect(outcome.grant?.jti).toBe("tgrant_seed");
    }
  });

  it("denies revoked, expired, kind, scope, and assignee mismatches distinctly", async () => {
    const cases: {
      readonly expected: (typeof taskGrantEnforcementDenials)[number];
      readonly grants: readonly TaskGrantRecord[];
      readonly name: string;
    }[] = [
      {
        expected: "grant_revoked",
        grants: [grant({ jti: "tgrant_revoked", revokedAt: new Date("2026-09-15T01:00:00Z") })],
        name: "revoked",
      },
      {
        expected: "grant_expired",
        grants: [grant({ expiresAt: new Date("2026-09-14T01:00:00Z"), jti: "tgrant_expired" })],
        name: "expired",
      },
      {
        expected: "unauthorized_kind",
        grants: [grant({ jti: "tgrant_kind", kindAllowlist: ["other.kind"] })],
        name: "kind",
      },
      {
        expected: "unauthorized_scope",
        grants: [grant({ jti: "tgrant_scope", scopeLabelAllowlist: ["red"] })],
        name: "scope label",
      },
      {
        expected: "unauthorized_assignee",
        grants: [grant({ jti: "tgrant_other", subject: "worker_two" })],
        name: "assignee",
      },
    ];
    for (const { expected, grants: rows } of cases) {
      const { client } = policyClient(rows);
      const outcome = await enforceTaskGrantPolicyWithClient(
        client,
        claimAttempt({ scopeLabel: "blue" }),
        now,
      );
      expect(outcome).toEqual({ reason: expected, status: "denied" });
    }
  });

  it("denies operator kinds and out-of-scope sessions", async () => {
    const { client } = policyClient([grant()]);
    await expect(
      enforceTaskGrantPolicyWithClient(client, claimAttempt({ kind: "operator.x" }), now),
    ).resolves.toEqual({ reason: "unauthorized_kind", status: "denied" });
    const sessionScoped = policyClient([grant({ sessionScope: "sess_other" })]);
    await expect(
      enforceTaskGrantPolicyWithClient(sessionScoped.client, claimAttempt(), now),
    ).resolves.toEqual({ reason: "unauthorized_scope", status: "denied" });
  });

  it("binds creates to the assignee when one is set, else to the actor", async () => {
    const forAttempt = (attempt: TaskGrantAttempt, rows: readonly TaskGrantRecord[]) =>
      enforceTaskGrantPolicyWithClient(policyClient(rows).client, attempt, now);
    await expect(
      forAttempt(createAttempt({ assigneeParticipantId: "worker_two" }), [
        grant({ action: "task.create", subject: "worker_two" }),
      ]),
    ).resolves.toMatchObject({ status: "allowed" });
    await expect(
      forAttempt(createAttempt({ assigneeParticipantId: "worker_two" }), [
        grant({ action: "task.create", subject: "worker_one" }),
      ]),
    ).resolves.toEqual({ reason: "unauthorized_assignee", status: "denied" });
    await expect(
      forAttempt(createAttempt(), [
        grant({
          action: "task.create",
          jti: "tgrant_create_revoked",
          revokedAt: new Date("2026-09-15T01:00:00.000Z"),
        }),
      ]),
    ).resolves.toEqual({ reason: "grant_revoked", status: "denied" });
  });
});

type ScriptedTaskRow = Record<string, unknown>;

interface ScriptedDatabase {
  /** Grant rows visible to the enforcement reads. */
  readonly grants: readonly TaskGrantRecord[];
  /** Row returned by the FOR UPDATE lock read, when present. */
  lockRow?: ScriptedTaskRow | null;
  /** Row returned by the plain task read, when present. */
  readRow?: ScriptedTaskRow | null;
  /** Updated row returned by the refresh UPDATE, when allowed. */
  refreshRow?: ScriptedTaskRow | null;
  /** Every statement the code under test issued, in order. */
  readonly statements: string[];
}

function taskRow(overrides: ScriptedTaskRow = {}): ScriptedTaskRow {
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
    scopeLabel: null,
    scheduleAlgorithmVersion: null,
    scheduleIdentityVersion: null,
    scheduleIntervalMs: null,
    scheduleScopeKey: null,
    scheduleWindowStart: null,
    sessionId: "sess_one",
    taskId: "task_one",
    ...overrides,
  };
}

function grantRow(overrides: Partial<TaskGrantRecord> = {}): TaskGrantRecord {
  return grant({ action: "task.claim", ...overrides });
}

/** Scripted pool routing every persistence statement without PostgreSQL. */
function scriptedDatabase(state: ScriptedDatabase): DatabasePool {
  const query = async (
    text: string,
    values?: readonly unknown[],
  ): Promise<{ readonly rows: ScriptedTaskRow[] }> => {
    state.statements.push(text);
    const normalized = text.replaceAll(/\s+/gu, " ");
    if (/\bBEGIN\b|\bCOMMIT\b|\bROLLBACK\b/u.test(normalized)) {
      return { rows: [] };
    }
    if (normalized.includes("pg_advisory_xact_lock")) {
      return { rows: [] };
    }
    if (normalized.includes("task_grants")) {
      if (normalized.includes("LIMIT 1")) {
        return { rows: state.grants.length > 0 ? [{ present: 1 }] : [] };
      }
      const [subject, action] = (values ?? []) as readonly unknown[];
      return {
        rows: state.grants
          .filter((entry) => entry.subject === subject && entry.action === action)
          .map((entry) => ({ ...entry })),
      };
    }
    if (normalized.includes("UPDATE tasks")) {
      if (state.refreshRow === null || state.refreshRow === undefined) {
        throw new Error(`unexpected task update: ${normalized}`);
      }
      return { rows: [state.refreshRow] };
    }
    if (
      normalized.includes("INSERT INTO tasks") ||
      normalized.includes("INSERT INTO session_events")
    ) {
      throw new Error(`unexpected task insert: ${normalized}`);
    }
    if (normalized.includes("FROM tasks")) {
      if (normalized.includes("FOR UPDATE")) {
        return {
          rows: state.lockRow === null || state.lockRow === undefined ? [] : [state.lockRow],
        };
      }
      return { rows: state.readRow === null || state.readRow === undefined ? [] : [state.readRow] };
    }
    throw new Error(`unexpected statement: ${normalized}`);
  };
  const client = { query, release: () => undefined };
  return {
    db: undefined as unknown as DatabasePool["db"],
    end: async () => undefined,
    pool: {
      connect: async () => client,
      query,
    } as unknown as DatabasePool["pool"],
  };
}

function grantStatements(statements: readonly string[]): string[] {
  return statements.filter((statement) => statement.includes("task_grants"));
}

describe("task-grant enforcement in task transactions", () => {
  it("denies a claim without a grant before any update", async () => {
    const state: ScriptedDatabase = {
      grants: [grantRow({ subject: "worker_two" })],
      lockRow: { ...taskRow(), databaseNow: new Date("2026-09-15T12:00:00.000Z") },
      statements: [],
    };
    const failure = await claimTaskWithEvent(scriptedDatabase(state), {
      claimLeaseTtlMs: 30_000,
      eventSourceId: "src_test",
      participantId: "worker_one",
      sessionId: "sess_one",
      taskId: "task_one",
    }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TaskGrantDeniedError);
    expect((failure as TaskGrantDeniedError).reason).toBe("unauthorized_assignee");
    expect(state.statements.some((statement) => statement.includes("UPDATE tasks"))).toBe(false);
    expect(state.statements.some((statement) => statement.includes("INSERT INTO"))).toBe(false);
  });

  it("denies a claim whose grant was revoked with the revocation reason", async () => {
    const state: ScriptedDatabase = {
      grants: [grantRow({ revokedAt: new Date("2026-09-15T11:00:00.000Z") })],
      lockRow: { ...taskRow(), databaseNow: new Date("2026-09-15T12:00:00.000Z") },
      statements: [],
    };
    const failure = await claimTaskWithEvent(scriptedDatabase(state), {
      claimLeaseTtlMs: 30_000,
      eventSourceId: "src_test",
      participantId: "worker_one",
      sessionId: "sess_one",
      taskId: "task_one",
    }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TaskGrantDeniedError);
    expect((failure as TaskGrantDeniedError).reason).toBe("grant_revoked");
  });

  it("grants nothing through parent linkage when the assignee binding fails", async () => {
    const state: ScriptedDatabase = {
      grants: [grantRow()],
      lockRow: {
        ...taskRow(),
        assigneeParticipantId: "worker_two",
        databaseNow: new Date("2026-09-15T12:00:00.000Z"),
        parentTaskId: "task_parent",
      },
      statements: [],
    };
    const failure = await claimTaskWithEvent(scriptedDatabase(state), {
      claimLeaseTtlMs: 30_000,
      eventSourceId: "src_test",
      participantId: "worker_one",
      sessionId: "sess_one",
      taskId: "task_one",
    }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TaskGrantDeniedError);
    expect((failure as TaskGrantDeniedError).reason).toBe("unauthorized_assignee");
  });

  it("keeps a lost claim race a typeless rejection without consulting grants", async () => {
    const state: ScriptedDatabase = {
      grants: [grantRow({ subject: "worker_two" })],
      lockRow: {
        ...taskRow(),
        claimExpiresAt: new Date("2026-09-15T13:00:00.000Z"),
        claimedAt: new Date("2026-09-15T11:59:00.000Z"),
        claimedBy: "worker_two",
        databaseNow: new Date("2026-09-15T12:00:00.000Z"),
      },
      statements: [],
    };
    await expect(
      claimTaskWithEvent(scriptedDatabase(state), {
        claimLeaseTtlMs: 30_000,
        eventSourceId: "src_test",
        participantId: "worker_one",
        sessionId: "sess_one",
        taskId: "task_one",
      }),
    ).resolves.toBeNull();
    expect(grantStatements(state.statements)).toEqual([]);
  });

  it("denies a create without a grant before the insert", async () => {
    const state: ScriptedDatabase = {
      grants: [grant({ action: "task.create", subject: "worker_two" })],
      readRow: null,
      statements: [],
    };
    const failure = await createTaskWithEventIdempotent(scriptedDatabase(state), {
      actorParticipantId: "worker_one",
      eventSourceId: "src_test",
      kind: "build.widget",
      objective: "Build the widget",
      sessionId: "sess_one",
      taskId: "task_one",
      taskIdSource: "caller",
    }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TaskGrantDeniedError);
    expect((failure as TaskGrantDeniedError).reason).toBe("unauthorized_assignee");
    expect(state.statements.some((statement) => statement.includes("INSERT INTO"))).toBe(false);
  });

  it("preserves idempotent create replay without consulting grants", async () => {
    const state: ScriptedDatabase = {
      grants: [grant({ action: "task.create", subject: "worker_two" })],
      readRow: taskRow(),
      statements: [],
    };
    const result = await createTaskWithEventIdempotent(scriptedDatabase(state), {
      actorParticipantId: "worker_one",
      eventSourceId: "src_test",
      kind: "build.widget",
      objective: "Build the widget",
      sessionId: "sess_one",
      taskId: "task_one",
      taskIdSource: "caller",
    });
    expect(result.status).toBe("replayed");
    expect(grantStatements(state.statements)).toEqual([]);
  });

  it("denies a refresh after revocation without extending the lease", async () => {
    const state: ScriptedDatabase = {
      grants: [grantRow({ revokedAt: new Date("2026-09-15T11:00:00.000Z") })],
      lockRow: {
        ...taskRow(),
        claimExpiresAt: new Date("2026-09-15T13:00:00.000Z"),
        claimId: "claim_abc",
        claimedAt: new Date("2026-09-15T11:59:00.000Z"),
        claimedBy: "worker_one",
        databaseNow: new Date("2026-09-15T12:00:00.000Z"),
      },
      statements: [],
    };
    const failure = await refreshTaskClaim(scriptedDatabase(state), {
      claimId: "claim_abc",
      claimLeaseTtlMs: 30_000,
      participantId: "worker_one",
      sessionId: "sess_one",
      taskId: "task_one",
    }).then(
      () => null,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TaskGrantDeniedError);
    expect((failure as TaskGrantDeniedError).reason).toBe("grant_revoked");
    expect(state.statements.some((statement) => statement.includes("UPDATE tasks"))).toBe(false);
  });

  it("refreshes a live lease when the holder keeps a live grant", async () => {
    const locked = {
      ...taskRow(),
      claimExpiresAt: new Date("2026-09-15T13:00:00.000Z"),
      claimId: "claim_abc",
      claimedAt: new Date("2026-09-15T11:59:00.000Z"),
      claimedBy: "worker_one",
      databaseNow: new Date("2026-09-15T12:00:00.000Z"),
    };
    const state: ScriptedDatabase = {
      grants: [grantRow()],
      lockRow: locked,
      refreshRow: { ...locked, claimExpiresAt: new Date("2026-09-15T13:30:00.000Z") },
      statements: [],
    };
    const task = await refreshTaskClaim(scriptedDatabase(state), {
      claimId: "claim_abc",
      claimLeaseTtlMs: 30_000,
      participantId: "worker_one",
      sessionId: "sess_one",
      taskId: "task_one",
    });
    expect(task?.taskId).toBe("task_one");
  });

  it("keeps a lost refresh race a typeless rejection without extending the lease", async () => {
    const state: ScriptedDatabase = {
      grants: [grantRow()],
      lockRow: {
        ...taskRow(),
        claimExpiresAt: new Date("2026-09-15T11:00:00.000Z"),
        claimId: "claim_abc",
        claimedAt: new Date("2026-09-15T10:00:00.000Z"),
        claimedBy: "worker_one",
        databaseNow: new Date("2026-09-15T12:00:00.000Z"),
      },
      statements: [],
    };
    await expect(
      refreshTaskClaim(scriptedDatabase(state), {
        claimId: "claim_abc",
        claimLeaseTtlMs: 30_000,
        participantId: "worker_one",
        sessionId: "sess_one",
        taskId: "task_one",
      }),
    ).resolves.toBeNull();
    expect(state.statements.some((statement) => statement.includes("UPDATE tasks"))).toBe(false);
  });
});

function effectTask(): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: "2026-09-15T13:00:00.000Z",
    claimId: "claim_abc",
    claimedAt: "2026-09-15T12:00:00.000Z",
    claimedBy: "worker_one",
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
  };
}

function effectEvent(): SessionEvent {
  return {
    createdAt: "2026-09-15T12:00:00.000Z",
    eventId: "evt_test",
    payload: { taskId: "task_one" },
    producerId: "worker_one",
    seq: 1,
    sessionId: "sess_one",
    type: sessionEventType.taskClaimed,
  };
}

interface TaskStoreOverrides {
  readonly claimWithEvent?: SessionPersistenceStores["tasks"]["claimWithEvent"];
  readonly completeWithEvent?: SessionPersistenceStores["tasks"]["completeWithEvent"];
  readonly createWithEvent?: SessionPersistenceStores["tasks"]["createWithEvent"];
  readonly refreshClaim?: SessionPersistenceStores["tasks"]["refreshClaim"];
}

function effectStores(overrides: TaskStoreOverrides): SessionPersistenceStores {
  const unexpected = (name: string) => async (): Promise<never> => {
    throw new Error(`unexpected task store call: ${name}`);
  };
  return {
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
      claimWithEvent: overrides.claimWithEvent ?? unexpected("claimWithEvent"),
      completeWithEvent: overrides.completeWithEvent ?? unexpected("completeWithEvent"),
      createWithEvent: overrides.createWithEvent ?? unexpected("createWithEvent"),
      createOperatorWithEvent: async () => {
        throw new Error("unexpected operator task create");
      },
      ensureScheduledRun: async () => {
        throw new Error("unexpected ensure scheduled run");
      },
      expireClaims: async () => [],
      failWithEvent: async () => null,
      get: async () => null,
      list: async () => [],
      listSnapshots: async () => [],
      recordApproval: async () => null,
      refreshClaim: overrides.refreshClaim ?? unexpected("refreshClaim"),
      releaseWithEvent: async () => null,
      supersedeScheduled: async () => ({ events: [], tasks: [] }),
    },
  };
}

function taskEffects(
  stores: SessionPersistenceStores,
  taskGrantAuthMode?: "disabled" | "required",
) {
  return createSessionTaskEffects({
    approvalValidators: new Map(),
    assertBroadcastEvents: () => undefined,
    eventSourceId: "src_task_grants_test",
    observability: new ModuleObservability({ moduleName: "TaskGrantsEnforcementTest" }),
    stores,
    taskClaimLeaseTtlMs: 30_000,
    ...(taskGrantAuthMode === undefined ? {} : { taskGrantAuthMode }),
  });
}

describe("task-grant denial mapping in task effects", () => {
  it("extracts denials raised directly or wrapped as persistence failures", () => {
    const direct = new TaskGrantDeniedError("grant_revoked");
    expect(extractTaskGrantDenied(direct)).toBe(direct);
    const wrapped = new SessionServicePersistenceError("claimTask", direct);
    expect(extractTaskGrantDenied(wrapped)).toBe(direct);
    expect(extractTaskGrantDenied(new Error("boom"))).toBeNull();
    expect(
      extractTaskGrantDenied(new SessionServicePersistenceError("claimTask", new Error("boom"))),
    ).toBeNull();
  });

  it("maps a denied claim to the typed result shared by every claim path", async () => {
    // claimTask (WebSocket) and claimTaskOverRest (HTTP/REST) both funnel through
    // claimTaskEffect plus mapTaskMutationRejection, so one mapping covers all three.
    const effects = taskEffects(
      effectStores({
        claimWithEvent: async () => {
          throw new TaskGrantDeniedError("unauthorized_kind");
        },
      }),
    );
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
      reason: "unauthorized_kind",
      status: "denied",
      task: null,
    });
  });

  it("keeps race-rejected claims typeless with zero events", async () => {
    const effects = taskEffects(effectStores({ claimWithEvent: async () => null }));
    const result = await Effect.runPromise(
      mapTaskMutationRejection(
        effects.claimTaskEffect({
          participantId: "worker_one",
          sessionId: "sess_one",
          taskId: "task_one",
        }),
      ),
    );
    expect(result).toEqual({ events: [], status: "rejected", task: null });
  });

  it("maps a denied refresh distinctly from a lost lease", async () => {
    const denied = taskEffects(
      effectStores({
        refreshClaim: async () => {
          throw new TaskGrantDeniedError("grant_revoked");
        },
      }),
    );
    await expect(
      Effect.runPromise(
        mapTaskClaimRefreshRejection(
          denied.refreshTaskClaimEffect({
            claimId: "claim_abc",
            participantId: "worker_one",
            sessionId: "sess_one",
            taskId: "task_one",
          }),
        ),
      ),
    ).resolves.toEqual({ events: [], reason: "grant_revoked", status: "denied", task: null });

    const rejected = taskEffects(effectStores({ refreshClaim: async () => null }));
    await expect(
      Effect.runPromise(
        mapTaskClaimRefreshRejection(
          rejected.refreshTaskClaimEffect({
            claimId: "claim_abc",
            participantId: "worker_one",
            sessionId: "sess_one",
            taskId: "task_one",
          }),
        ),
      ),
    ).resolves.toEqual({ events: [], status: "rejected", task: null });
  });

  it("maps a denied create distinctly from idempotent replay", async () => {
    const effects = taskEffects(
      effectStores({
        createWithEvent: async () => {
          throw new TaskGrantDeniedError("unauthorized_assignee");
        },
      }),
    );
    await expect(
      Effect.runPromise(
        effects.createTaskEffect({
          actorParticipantId: "worker_one",
          input: null,
          kind: "build.widget",
          objective: "Build the widget",
          sessionId: "sess_one",
          taskId: "task_one",
        }),
      ),
    ).resolves.toEqual({
      events: [],
      reason: "unauthorized_assignee",
      status: "denied",
      task: null,
    });
  });

  it("keeps terminal writes on the claim fence without a grant gate", async () => {
    const task = effectTask();
    const event = effectEvent();
    const effects = taskEffects(
      effectStores({
        completeWithEvent: async () => ({ event, task }),
      }),
    );
    await expect(
      Effect.runPromise(
        effects.completeTaskEffect({
          claimId: "claim_abc",
          participantId: "worker_one",
          result: {},
          sessionId: "sess_one",
          taskId: "task_one",
        }),
      ),
    ).resolves.toMatchObject({ status: "applied", task });
  });

  it("threads the auth mode into claim, refresh, and create stores", async () => {
    const seen: unknown[] = [];
    const stores = effectStores({
      claimWithEvent: async (input) => {
        seen.push(input.taskGrantAuthMode);
        return null;
      },
      createWithEvent: async (input) => {
        seen.push(input.taskGrantAuthMode);
        return { events: [], status: "replayed", task: effectTask() };
      },
      refreshClaim: async (input) => {
        seen.push(input.taskGrantAuthMode);
        return null;
      },
    });
    const effects = taskEffects(stores, "disabled");
    await Effect.runPromise(
      mapTaskMutationRejection(
        effects.claimTaskEffect({
          participantId: "worker_one",
          sessionId: "sess_one",
          taskId: "task_one",
        }),
      ),
    );
    await Effect.runPromise(
      mapTaskClaimRefreshRejection(
        effects.refreshTaskClaimEffect({
          claimId: "claim_abc",
          participantId: "worker_one",
          sessionId: "sess_one",
          taskId: "task_one",
        }),
      ),
    );
    await Effect.runPromise(
      effects.createTaskEffect({
        actorParticipantId: "worker_one",
        input: null,
        kind: "build.widget",
        objective: "Build the widget",
        sessionId: "sess_one",
        taskId: "task_one",
      }),
    );
    expect(seen).toEqual(["disabled", "disabled", "disabled"]);
  });
});
