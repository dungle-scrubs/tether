import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import {
  cancelTaskWithEvent,
  type DatabasePool,
  ScheduledTaskIdentityMismatchError,
  supersedeScheduledRunsWithEvent,
} from "../src/db.js";
import type { SessionPersistenceStores } from "../src/db-store-contracts.js";
import { ModuleObservability } from "../src/observability.js";
import { computeScheduleWindow, deriveScheduledTaskId } from "../src/protocol.js";
import {
  type ScheduledMaintenanceIdentity,
  SessionServicePersistenceError,
} from "../src/session-service-contracts.js";
import { createSessionTaskEffects } from "../src/session-service-task-effects.js";
import type { TaskRecord } from "../src/types.js";

interface CapturedQuery {
  readonly params: readonly unknown[] | undefined;
  readonly sql: string;
}

/** pg client double that records queries and returns configured rows. */
class RecordingClient {
  readonly queries: CapturedQuery[] = [];
  releaseCount = 0;

  async query<TRow>(sql: string, params?: readonly unknown[]): Promise<{ readonly rows: TRow[] }> {
    this.queries.push({ params, sql });
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

/** Minimal pool wrapper around one recording client. */
function recordingDatabase(client: RecordingClient): DatabasePool {
  return { pool: { connect: async () => client } } as unknown as DatabasePool;
}

const mailboxScope = { accountId: "acct_opaque_1", provider: "fastmail" };
const currentWindow = computeScheduleWindow(1_700_003_600_000, 3_600_000);
const olderWindow = computeScheduleWindow(1_700_000_000_000, 3_600_000);
const identity: ScheduledMaintenanceIdentity = {
  kind: "email_organization",
  mailboxScope,
  scheduleWindow: currentWindow,
  sessionId: "sess_mailbox_1",
};

describe("supersedeScheduledRunsWithEvent atomic predicate", () => {
  it("cancels only older matching unclaimed nonterminal scheduled runs in one transaction", async () => {
    const client = new RecordingClient();
    const result = await supersedeScheduledRunsWithEvent(recordingDatabase(client), {
      eventSourceId: "src_supersede_test",
      kind: identity.kind,
      mailboxAccountId: mailboxScope.accountId,
      mailboxProvider: mailboxScope.provider,
      participantId: "operator_1",
      scheduleAlgorithmVersion: currentWindow.algorithmVersion,
      scheduleIntervalMs: currentWindow.intervalMs,
      scheduleWindowStart: currentWindow.startMs,
      sessionId: identity.sessionId,
    });

    expect(result).toEqual({ events: [], tasks: [] });
    expect(client.queries[0]?.sql).toBe("BEGIN");
    expect(client.queries.at(-1)?.sql).toBe("COMMIT");
    expect(client.releaseCount).toBe(1);

    const update = client.queries.find((query) => query.sql.includes("UPDATE tasks"));
    expect(update).toBeDefined();
    const sql = update?.sql ?? "";
    // The predicate is the atomic fence: a racing claim sets claimed_by, so the
    // row stops matching and can never be cancelled afterward.
    expect(sql).toContain("claimed_by IS NULL");
    expect(sql).toContain("completed_at IS NULL");
    expect(sql).toContain("failed_at IS NULL");
    expect(sql).toContain("cancelled_at IS NULL");
    expect(sql).toContain("schedule_window_start < $7");
    expect(sql).toContain("mailbox_provider = $3");
    expect(sql).toContain("mailbox_account_id = $4");
    expect(sql).toContain("schedule_algorithm_version = $5");
    expect(sql).toContain("schedule_interval_ms = $6");
    expect(update?.params).toEqual([
      identity.sessionId,
      identity.kind,
      mailboxScope.provider,
      mailboxScope.accountId,
      currentWindow.algorithmVersion,
      currentWindow.intervalMs,
      currentWindow.startMs,
    ]);
    // With no reviewed candidate set, apply keeps schedule-identity-only scope
    // and never adds the candidate-bounding predicate.
    expect(sql).not.toContain("task_id = ANY");
  });

  it("bounds apply to exactly the reviewed candidate ids so a stale non-reviewed run is never cancelled", async () => {
    // A stale run (task_stale_after_dryrun) can match the schedule identity yet
    // appear only AFTER the operator's dry-run listing. Supplying the reviewed
    // candidate ids adds an `AND task_id = ANY($8)` fence so apply is bounded to
    // exactly what the operator reviewed and the stale run cannot be cancelled.
    const client = new RecordingClient();
    const reviewedIds = ["task_reviewed_a", "task_reviewed_b"];
    await supersedeScheduledRunsWithEvent(recordingDatabase(client), {
      candidateTaskIds: reviewedIds,
      eventSourceId: "src_supersede_test",
      kind: identity.kind,
      mailboxAccountId: mailboxScope.accountId,
      mailboxProvider: mailboxScope.provider,
      participantId: "operator_1",
      scheduleAlgorithmVersion: currentWindow.algorithmVersion,
      scheduleIntervalMs: currentWindow.intervalMs,
      scheduleWindowStart: currentWindow.startMs,
      sessionId: identity.sessionId,
    });

    const update = client.queries.find((query) => query.sql.includes("UPDATE tasks"));
    const sql = update?.sql ?? "";
    // The candidate fence rides alongside the existing schedule-identity predicates.
    expect(sql).toContain("task_id = ANY($8)");
    expect(sql).toContain("claimed_by IS NULL");
    expect(sql).toContain("schedule_window_start < $7");
    // The stale non-reviewed id is not in the bound array, so ANY($8) excludes it.
    expect(update?.params?.[7]).toEqual(reviewedIds);
    expect(update?.params?.[7]).not.toContain("task_stale_after_dryrun");
  });

  it("preserves schedule-identity-only behavior when the candidate set is empty", async () => {
    const client = new RecordingClient();
    await supersedeScheduledRunsWithEvent(recordingDatabase(client), {
      candidateTaskIds: [],
      eventSourceId: "src_supersede_test",
      kind: identity.kind,
      mailboxAccountId: mailboxScope.accountId,
      mailboxProvider: mailboxScope.provider,
      participantId: "operator_1",
      scheduleAlgorithmVersion: currentWindow.algorithmVersion,
      scheduleIntervalMs: currentWindow.intervalMs,
      scheduleWindowStart: currentWindow.startMs,
      sessionId: identity.sessionId,
    });

    const update = client.queries.find((query) => query.sql.includes("UPDATE tasks"));
    expect(update?.sql ?? "").not.toContain("task_id = ANY");
    expect(update?.params).toHaveLength(7);
  });

  it("proves the race: generic task cancellation lacks the claimed-run guard", async () => {
    const client = new RecordingClient();
    await cancelTaskWithEvent(recordingDatabase(client), {
      eventSourceId: "src_cancel_test",
      participantId: "operator_1",
      sessionId: identity.sessionId,
      taskId: "task_generic",
    });

    const update = client.queries.find((query) => query.sql.includes("UPDATE tasks"));
    // A list-then-generic-cancel sequence would cancel a task that a worker
    // claimed between the list and the cancel, because generic cancel does not
    // exclude claimed runs. Scheduled supersession must not reuse this path.
    expect(update?.sql).not.toContain("claimed_by IS NULL");
  });
});

describe("scheduled supersession service effect", () => {
  const olderPendingTask = createScheduledTask({
    schedule: { mailboxScope, scheduleWindow: olderWindow },
    taskId: "task_older_pending",
  });

  it("supersedes older matching unclaimed runs and returns them with events", async () => {
    const event = createCancelEvent();
    const effects = createEffects({
      supersedeScheduled: async () => ({
        events: [event],
        tasks: [olderPendingTask],
      }),
    });

    const result = await Effect.runPromise(
      effects.supersedeScheduledRunsEffect({
        identity,
        participantId: "operator_1",
      }),
    );

    expect(result.status).toBe("applied");
    expect(result.supersededTasks.map((task) => task.taskId)).toEqual(["task_older_pending"]);
    expect(result.events).toEqual([event]);
    expect(result.refusals).toEqual([]);
  });

  it("threads the reviewed candidate ids to the atomic store op so apply is bounded to them", async () => {
    // Proves the reconciler's reviewed set reaches the DB fence: without this the
    // store applies by schedule identity alone and can cancel a stale run that
    // was never reviewed at dry-run.
    const captured: Array<Record<string, unknown>> = [];
    const effects = createEffects({
      supersedeScheduled: async (supersedeInput) => {
        captured.push(supersedeInput as unknown as Record<string, unknown>);
        return { events: [], tasks: [olderPendingTask] };
      },
    });

    await Effect.runPromise(
      effects.supersedeScheduledRunsEffect({
        candidateTaskIds: ["task_older_pending", "task_reviewed_b"],
        identity,
        participantId: "operator_1",
      }),
    );

    expect(captured).toHaveLength(1);
    expect(captured[0]?.candidateTaskIds).toEqual(["task_older_pending", "task_reviewed_b"]);
  });

  it("omits candidateTaskIds from the store op when the request carries none", async () => {
    const captured: Array<Record<string, unknown>> = [];
    const effects = createEffects({
      supersedeScheduled: async (supersedeInput) => {
        captured.push(supersedeInput as unknown as Record<string, unknown>);
        return { events: [], tasks: [] };
      },
    });

    await Effect.runPromise(
      effects.supersedeScheduledRunsEffect({
        identity,
        participantId: "operator_1",
      }),
    );

    expect(captured[0]).not.toHaveProperty("candidateTaskIds");
  });

  it("refuses a claim that raced supersession so it can never be cancelled afterward", async () => {
    // The atomic predicate excluded the now-claimed task, so it is not in the
    // superseded set; classifying it yields a typed `claimed` refusal.
    const claimedTask = createScheduledTask({
      claimedBy: "part_worker",
      schedule: { mailboxScope, scheduleWindow: olderWindow },
      taskId: "task_raced_claim",
    });
    const effects = createEffects({
      get: async () => claimedTask,
      supersedeScheduled: async () => ({ events: [], tasks: [] }),
    });

    const result = await Effect.runPromise(
      effects.supersedeScheduledRunsEffect({
        candidateTaskIds: ["task_raced_claim"],
        identity,
        participantId: "operator_1",
      }),
    );

    expect(result.supersededTasks).toEqual([]);
    expect(result.refusals).toEqual([{ reason: "claimed", taskId: "task_raced_claim" }]);
  });

  it("returns typed refusals for manual, terminal, and mismatched candidates", async () => {
    const manualTask = createScheduledTask({
      schedule: null,
      taskId: "task_manual",
    });
    const terminalTask = createScheduledTask({
      completedAt: "2026-07-12T00:00:00.000Z",
      schedule: { mailboxScope, scheduleWindow: olderWindow },
      taskId: "task_terminal",
    });
    const mismatchedTask = createScheduledTask({
      schedule: {
        mailboxScope: { accountId: "acct_other", provider: "fastmail" },
        scheduleWindow: olderWindow,
      },
      taskId: "task_mismatch",
    });
    const byId: Record<string, TaskRecord> = {
      task_manual: manualTask,
      task_mismatch: mismatchedTask,
      task_terminal: terminalTask,
    };
    const effects = createEffects({
      get: async ({ taskId }) => byId[taskId] ?? null,
      supersedeScheduled: async () => ({ events: [], tasks: [] }),
    });

    const result = await Effect.runPromise(
      effects.supersedeScheduledRunsEffect({
        candidateTaskIds: ["task_manual", "task_terminal", "task_mismatch"],
        identity,
        participantId: "operator_1",
      }),
    );

    expect(result.refusals).toEqual([
      { reason: "manual", taskId: "task_manual" },
      { reason: "terminal", taskId: "task_terminal" },
      { reason: "schedule_identity_mismatch", taskId: "task_mismatch" },
    ]);
  });

  it("treats a run returned to pending after claim expiry as eligible again", async () => {
    // After the sweeper clears an elapsed claim, claimedBy is null again, so the
    // atomic predicate supersedes it. The candidate is in the superseded set and
    // produces no refusal.
    const expiredThenPending = createScheduledTask({
      claimExpiredBy: "part_worker",
      schedule: { mailboxScope, scheduleWindow: olderWindow },
      taskId: "task_expired_pending",
    });
    const effects = createEffects({
      supersedeScheduled: async () => ({
        events: [],
        tasks: [expiredThenPending],
      }),
    });

    const result = await Effect.runPromise(
      effects.supersedeScheduledRunsEffect({
        candidateTaskIds: ["task_expired_pending"],
        identity,
        participantId: "operator_1",
      }),
    );

    expect(result.supersededTasks.map((task) => task.taskId)).toEqual(["task_expired_pending"]);
    expect(result.refusals).toEqual([]);
  });

  it("rejects a caller-supplied scheduled task id that does not match the derived identity", async () => {
    const effects = createEffects({});
    const error = await Effect.runPromise(
      effects
        .createTaskEffect({
          input: null,
          kind: identity.kind,
          objective: "Organize the mailbox",
          schedule: {
            mailboxAccountId: mailboxScope.accountId,
            mailboxProvider: mailboxScope.provider,
            scheduleAlgorithmVersion: currentWindow.algorithmVersion,
            scheduleIntervalMs: currentWindow.intervalMs,
            scheduleWindowStart: currentWindow.startMs,
          },
          sessionId: identity.sessionId,
          taskId: "task_not_derived",
        })
        .pipe(Effect.flip),
    );
    expect(error).toBeInstanceOf(SessionServicePersistenceError);
    expect((error as SessionServicePersistenceError).cause).toBeInstanceOf(
      ScheduledTaskIdentityMismatchError,
    );
    const mismatch = (error as SessionServicePersistenceError)
      .cause as ScheduledTaskIdentityMismatchError;
    expect(mismatch.derivedTaskId).toBe(deriveScheduledTaskId(identity));
    expect(mismatch.suppliedTaskId).toBe("task_not_derived");
  });

  it("creates a scheduled task through the deterministic ensure path and replays an existing one", async () => {
    const deterministicTaskId = deriveScheduledTaskId(identity);
    const createdTask = createScheduledTask({
      schedule: { mailboxScope, scheduleWindow: currentWindow },
      taskId: deterministicTaskId,
    });
    const createdEvent = createCancelEvent();
    const captured: unknown[] = [];
    let created = true;
    const effects = createEffects({
      createWithEvent: async () => {
        throw new Error("scheduled create must not use the generic create seam");
      },
      ensureScheduledRun: async (ensureInput) => {
        captured.push(ensureInput);
        return created
          ? {
              current: {
                event: createdEvent,
                status: "created",
                task: createdTask,
              },
              supersededEvents: [],
              supersededTasks: [],
              taskId: deterministicTaskId,
            }
          : {
              current: { status: "replayed", task: createdTask },
              supersededEvents: [],
              supersededTasks: [],
              taskId: deterministicTaskId,
            };
      },
    });

    const first = await Effect.runPromise(
      effects.createTaskEffect({
        input: null,
        kind: identity.kind,
        objective: "Organize the mailbox",
        schedule: {
          mailboxAccountId: mailboxScope.accountId,
          mailboxProvider: mailboxScope.provider,
          scheduleAlgorithmVersion: currentWindow.algorithmVersion,
          scheduleIntervalMs: currentWindow.intervalMs,
          scheduleWindowStart: currentWindow.startMs,
        },
        sessionId: identity.sessionId,
        taskId: deterministicTaskId,
      }),
    );
    expect(first.status).toBe("created");

    created = false;
    const replay = await Effect.runPromise(
      effects.createTaskEffect({
        input: null,
        kind: identity.kind,
        objective: "Organize the mailbox",
        schedule: {
          mailboxAccountId: mailboxScope.accountId,
          mailboxProvider: mailboxScope.provider,
          scheduleAlgorithmVersion: currentWindow.algorithmVersion,
          scheduleIntervalMs: currentWindow.intervalMs,
          scheduleWindowStart: currentWindow.startMs,
        },
        sessionId: identity.sessionId,
        taskId: deterministicTaskId,
      }),
    );
    expect(replay.status).toBe("replayed");
    expect(replay.task?.taskId).toBe(deterministicTaskId);
    expect(captured).toHaveLength(2);
    expect(captured[0]).toMatchObject({
      expectedTaskId: deterministicTaskId,
      mailboxAccountId: mailboxScope.accountId,
      mailboxProvider: mailboxScope.provider,
      scheduleWindowStart: currentWindow.startMs,
    });
  });

  it("routes a schedule-carrying create with an omitted taskId through the deterministic superseding path", async () => {
    // The service chokepoint: a generic create that carries a schedule identity but
    // omits a taskId must still resolve to the server-derived deterministic id and
    // atomically supersede older windows, never a random generated id on the
    // unfenced create seam.
    const deterministicTaskId = deriveScheduledTaskId(identity);
    const createdTask = createScheduledTask({
      schedule: { mailboxScope, scheduleWindow: currentWindow },
      taskId: deterministicTaskId,
    });
    const olderCancelEvent = createCancelEvent();
    const createdEvent = {
      ...createCancelEvent(),
      eventId: "evt_created",
      type: "task.created",
    };
    let ensureCalls = 0;
    const captured: Array<Record<string, unknown>> = [];
    const effects = createEffects({
      createWithEvent: async () => {
        throw new Error("scheduled create must not use the generic create seam");
      },
      ensureScheduledRun: async (ensureInput) => {
        ensureCalls += 1;
        captured.push(ensureInput as unknown as Record<string, unknown>);
        return {
          current: {
            event: createdEvent,
            status: "created",
            task: createdTask,
          },
          supersededEvents: [olderCancelEvent],
          supersededTasks: [olderPendingTask],
          taskId: deterministicTaskId,
        };
      },
    });

    const result = await Effect.runPromise(
      effects.createTaskEffect({
        input: null,
        kind: identity.kind,
        objective: "Organize the mailbox",
        schedule: {
          mailboxAccountId: mailboxScope.accountId,
          mailboxProvider: mailboxScope.provider,
          scheduleAlgorithmVersion: currentWindow.algorithmVersion,
          scheduleIntervalMs: currentWindow.intervalMs,
          scheduleWindowStart: currentWindow.startMs,
        },
        sessionId: identity.sessionId,
        taskId: undefined,
      }),
    );

    expect(ensureCalls).toBe(1);
    // No caller id was supplied, so the ensure path derives the id server-side and
    // no expectedTaskId is forwarded.
    expect(captured[0]).not.toHaveProperty("expectedTaskId");
    expect(result.status).toBe("created");
    expect(result.task?.taskId).toBe(deterministicTaskId);
    // The older-window supersession cancellation event rides the broadcast list.
    expect(result.events).toEqual([olderCancelEvent, createdEvent]);
  });
});

function createEffects(overrides: Partial<SessionPersistenceStores["tasks"]>) {
  const stores = createStores(overrides);
  return createSessionTaskEffects({
    approvalValidators: new Map(),
    assertBroadcastEvents: () => undefined,
    eventSourceId: "src_supersession_test",
    observability: new ModuleObservability({
      moduleName: "ScheduledSupersessionTest",
    }),
    stores,
    taskClaimLeaseTtlMs: 1_000,
  });
}

function createStores(
  overrides: Partial<SessionPersistenceStores["tasks"]>,
): SessionPersistenceStores {
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
      create: async () => ({
        created: true,
        session: {
          createdAt: "2026-07-12T00:00:00.000Z",
          sessionId: identity.sessionId,
        },
      }),
      delete: async () => true,
      list: async () => [],
      read: async () => ({
        createdAt: "2026-07-12T00:00:00.000Z",
        sessionId: identity.sessionId,
      }),
      readDebugSummary: async () => {
        throw new Error("unexpected debug summary read");
      },
    },
    tasks: {
      cancelWithEvent: async () => null,
      claimWithEvent: async () => null,
      completeWithEvent: async () => null,
      createWithEvent: async () => {
        throw new Error("unexpected task create");
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
      refreshClaim: async () => null,
      releaseWithEvent: async () => null,
      supersedeScheduled: async () => ({ events: [], tasks: [] }),
      ...overrides,
    },
  };
}

function createScheduledTask(overrides: Partial<TaskRecord>): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-07-12T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: null,
    kind: identity.kind,
    objective: "Organize the mailbox",
    releasedAt: null,
    releasedBy: null,
    result: null,
    schedule: { mailboxScope, scheduleWindow: olderWindow },
    sessionId: identity.sessionId,
    taskId: "task_scheduled",
    ...overrides,
  };
}

function createCancelEvent() {
  return {
    createdAt: "2026-07-12T00:00:00.000Z",
    eventId: "evt_supersede",
    payload: { reason: { reason: "superseded_by_newer_window" } },
    producerId: "system",
    seq: 1,
    sessionId: identity.sessionId,
    type: "control.cancel",
  } as const;
}
