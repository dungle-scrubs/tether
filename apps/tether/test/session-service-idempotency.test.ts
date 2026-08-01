import { Effect } from "effect";
import { describe, expect, it } from "vitest";
import { TaskClaimExpirationDeadlockError } from "../src/db.js";
import type { SessionPersistenceStores } from "../src/db-store-contracts.js";
import { ModuleObservability, type StructuredLogEntry } from "../src/observability.js";
import { sessionEventType } from "../src/protocol.js";
import { SessionServicePersistenceError } from "../src/session-service-contracts.js";
import { createSessionCoreEffects } from "../src/session-service-core-effects.js";
import { createSessionTaskEffects } from "../src/session-service-task-effects.js";
import type { SessionEvent, TaskRecord } from "../src/types.js";

describe("session service idempotency classification", () => {
  it("does not append session.created for duplicate create-session calls", async () => {
    const event = createEvent({ type: sessionEventType.sessionCreated });
    const appendedEvents: SessionEvent[] = [];
    const stores = createStores({
      createSession: async () => ({
        created: false,
        session: {
          createdAt: "2026-01-01T00:00:00.000Z",
          sessionId: "sess_idempotency",
        },
      }),
    });
    const effects = createSessionCoreEffects({
      appendEventEffect: () => {
        appendedEvents.push(event);
        return Effect.succeed(event);
      },
      assertBroadcastEvents: (_operation, _sessionId, events, expectedCount) => {
        expect(events).toHaveLength(expectedCount ?? events.length);
      },
      claimRestControlEffect: () => Effect.succeed({ status: "ok" as const }),
      controlEpochEnforcement: false,
      eventSourceId: "src_idempotency_test",
      stores,
    });

    await expect(
      Effect.runPromise(effects.createSessionEffect({ sessionId: "sess_idempotency" })),
    ).resolves.toMatchObject({
      events: [],
      session: { sessionId: "sess_idempotency" },
    });
    expect(appendedEvents).toHaveLength(0);
  });

  it("emits session.created for client resolution only when the session row is new", async () => {
    const event = createEvent({ type: sessionEventType.sessionCreated });
    const emittedSessionIds: string[] = [];
    let sessionCreated = false;
    const stores = createStores({
      readSession: async () => ({
        createdAt: "2026-01-01T00:00:00.000Z",
        sessionId: "sess_idempotency",
      }),
      upsertBinding: async () => ({
        binding: {
          archivedAt: null,
          createdAt: "2026-01-01T00:00:00.000Z",
          externalId: "chat_1",
          lastSeenAt: "2026-01-01T00:00:00.000Z",
          provider: "external-chat",
          sessionId: "sess_idempotency",
        },
        created: true,
        sessionCreated,
        status: "inserted",
      }),
    });
    const effects = createSessionCoreEffects({
      appendEventEffect: (input) => {
        emittedSessionIds.push(input.sessionId);
        return Effect.succeed({ ...event, sessionId: input.sessionId });
      },
      assertBroadcastEvents: (_operation, _sessionId, events, expectedCount) => {
        expect(events).toHaveLength(expectedCount ?? events.length);
      },
      claimRestControlEffect: () => Effect.succeed({ status: "ok" as const }),
      controlEpochEnforcement: false,
      eventSourceId: "src_idempotency_test",
      stores,
    });

    await expect(
      Effect.runPromise(
        effects.resolveClientSessionEffect({
          externalId: "chat_1",
          provider: "external-chat",
          sessionId: undefined,
        }),
      ),
    ).resolves.toMatchObject({ events: [] });

    sessionCreated = true;
    await expect(
      Effect.runPromise(
        effects.resolveClientSessionEffect({
          externalId: "chat_1",
          provider: "external-chat",
          sessionId: undefined,
        }),
      ),
    ).resolves.toMatchObject({
      events: [{ type: sessionEventType.sessionCreated }],
    });
    expect(emittedSessionIds).toEqual(["sess_idempotency"]);
  });

  it("classifies publish event created, replayed, conflict, and persistence failures", async () => {
    const event = createEvent();
    const broadcastCounts: number[] = [];
    let appendIdempotent: SessionPersistenceStores["events"]["appendIdempotent"] = async () => ({
      event,
      events: [event],
      status: "created",
    });
    const stores = createStores({
      appendIdempotent: (input, options) => appendIdempotent(input, options),
    });
    const effects = createSessionCoreEffects({
      appendEventEffect: () =>
        Effect.fail(new SessionServicePersistenceError("append", "unexpected direct append")),
      assertBroadcastEvents: (_operation, _sessionId, events, expectedCount) => {
        expect(events).toHaveLength(expectedCount ?? events.length);
        broadcastCounts.push(events.length);
      },
      claimRestControlEffect: () => Effect.succeed({ status: "ok" as const }),
      controlEpochEnforcement: false,
      eventSourceId: "src_idempotency_test",
      stores,
    });

    await expect(
      Effect.runPromise(effects.publishEventEffect(createPublishInput())),
    ).resolves.toMatchObject({ event, status: "created" });

    appendIdempotent = async () => ({ event, events: [], status: "replayed" });
    await expect(
      Effect.runPromise(effects.publishEventEffect(createPublishInput())),
    ).resolves.toMatchObject({ event, events: [], status: "replayed" });

    appendIdempotent = async () => ({
      conflictingFields: ["payload"],
      eventId: event.eventId,
      events: [],
      status: "conflict",
    });
    await expect(
      Effect.runPromise(effects.publishEventEffect(createPublishInput())),
    ).resolves.toMatchObject({
      conflictingFields: ["payload"],
      eventId: event.eventId,
      events: [],
      status: "conflict",
    });

    appendIdempotent = async () => {
      throw new Error("database unavailable");
    };
    const failure = await Effect.runPromise(
      Effect.flip(effects.publishEventEffect(createPublishInput())),
    );
    expect(failure).toBeInstanceOf(SessionServicePersistenceError);
    const generatedFailure = await Effect.runPromise(
      Effect.flip(
        effects.publishEventEffect({
          ...createPublishInput(),
          eventId: undefined,
        }),
      ),
    );
    expect(generatedFailure).toBeInstanceOf(SessionServicePersistenceError);
    if (!(generatedFailure instanceof SessionServicePersistenceError)) {
      throw new Error("Expected generated event append to fail through persistence");
    }
    expect(generatedFailure.operation).toBe("append");
    expect(broadcastCounts).toEqual([1, 0, 0]);
  });

  it("classifies task create created, replayed, conflict, and generated-id failures", async () => {
    const task = createTask();
    const event = createEvent({
      payload: { task },
      type: sessionEventType.taskCreated,
    });
    const taskIdSources: string[] = [];
    let createWithEvent: SessionPersistenceStores["tasks"]["createWithEvent"] = async (input) => {
      taskIdSources.push(input.taskIdSource);
      return { event, events: [event], status: "created", task };
    };
    const stores = createStores({
      createWithEvent: (input) => createWithEvent(input),
    });
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: (_observability, _operation, _sessionId, events, expectedCount) => {
        expect(events).toHaveLength(expectedCount ?? events.length);
      },
      eventSourceId: "src_task_idempotency_test",
      observability: new ModuleObservability({
        moduleName: "TaskIdempotencyTest",
      }),
      stores,
      taskClaimLeaseTtlMs: 1_000,
    });

    await expect(
      Effect.runPromise(effects.createTaskEffect(createTaskInput())),
    ).resolves.toMatchObject({ status: "created", task });

    createWithEvent = async () => ({ events: [], status: "replayed", task });
    await expect(
      Effect.runPromise(effects.createTaskEffect(createTaskInput())),
    ).resolves.toMatchObject({ events: [], status: "replayed", task });

    createWithEvent = async () => ({
      conflictingFields: ["objective", "input"],
      events: [],
      status: "conflict",
      task: null,
      taskId: task.taskId,
    });
    await expect(
      Effect.runPromise(effects.createTaskEffect(createTaskInput())),
    ).resolves.toMatchObject({
      conflictingFields: ["objective", "input"],
      events: [],
      status: "conflict",
      task: null,
      taskId: task.taskId,
    });

    createWithEvent = async (input) => {
      taskIdSources.push(input.taskIdSource);
      throw new Error("generated id collided");
    };
    const failure = await Effect.runPromise(
      Effect.flip(effects.createTaskEffect({ ...createTaskInput(), taskId: undefined })),
    );
    expect(failure).toBeInstanceOf(SessionServicePersistenceError);
    expect(taskIdSources).toContain("caller");
    expect(taskIdSources).toContain("generated");
  });

  it("logs task claim expiration retry diagnostics before persistence failures are normalized", async () => {
    const logEntries: StructuredLogEntry[] = [];
    const deadlock = new Error("deadlock detected");
    const retryError = new TaskClaimExpirationDeadlockError({
      diagnostics: {
        maxAttempts: 3,
        operation: "expireTaskClaims",
        requestedBatchSize: 11,
        retryAttempt: 3,
        sqlState: "40P01",
      },
      originalError: deadlock,
    });
    const effects = createSessionTaskEffects({
      approvalValidators: new Map(),
      assertBroadcastEvents: () => undefined,
      eventSourceId: "src_task_expiration_test",
      observability: new ModuleObservability({
        debugEnabled: true,
        logger: { log: (entry) => logEntries.push(entry) },
        moduleName: "TaskExpirationTest",
      }),
      stores: createStores({
        expireClaims: async () => {
          throw retryError;
        },
      }),
      taskClaimLeaseTtlMs: 1_000,
    });

    const failure = await Effect.runPromise(
      Effect.flip(effects.expireTaskClaimsEffect({ batchSize: 11 })),
    );

    expect(failure).toBeInstanceOf(SessionServicePersistenceError);
    expect(logEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          data: expect.objectContaining({
            maxAttempts: 3,
            operation: "expireTaskClaims",
            requestedBatchSize: 11,
            retryAttempt: 3,
            sqlState: "40P01",
          }),
          message: "task_claim_expiration.retry_exhausted",
          operation: "expireTaskClaims",
        }),
      ]),
    );
  });
});

interface StoreOverrides {
  readonly appendIdempotent?: SessionPersistenceStores["events"]["appendIdempotent"];
  readonly createSession?: SessionPersistenceStores["sessions"]["create"];
  readonly createWithEvent?: SessionPersistenceStores["tasks"]["createWithEvent"];
  readonly expireClaims?: SessionPersistenceStores["tasks"]["expireClaims"];
  readonly readSession?: SessionPersistenceStores["sessions"]["read"];
  readonly upsertBinding?: SessionPersistenceStores["clientBindings"]["upsert"];
}

function createStores(overrides: StoreOverrides): SessionPersistenceStores {
  return {
    clientBindings: {
      archive: async () => null,
      find: async () => null,
      list: async () => [],
      upsert:
        overrides.upsertBinding ??
        (async () => {
          throw new Error("unexpected client binding upsert");
        }),
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
      appendIdempotent:
        overrides.appendIdempotent ??
        (async () => {
          throw new Error("unexpected idempotent append");
        }),
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
      create:
        overrides.createSession ??
        (async () => ({
          created: true,
          session: {
            createdAt: "2026-01-01T00:00:00.000Z",
            sessionId: "sess_idempotency",
          },
        })),
      delete: async () => ({ status: "deleted" }) as const,
      list: async () => [],
      read:
        overrides.readSession ??
        (async () => ({
          createdAt: "2026-01-01T00:00:00.000Z",
          sessionId: "sess_idempotency",
        })),
      readDebugSummary: async () => {
        throw new Error("unexpected debug summary read");
      },
    },
    tasks: {
      cancelWithEvent: async () => null,
      claimWithEvent: async () => null,
      completeWithEvent: async () => null,
      createWithEvent:
        overrides.createWithEvent ??
        (async () => {
          throw new Error("unexpected task create");
        }),
      createOperatorWithEvent: async () => {
        throw new Error("unexpected operator task create");
      },
      ensureScheduledRun: async () => {
        throw new Error("unexpected ensure scheduled run");
      },
      expireClaims: overrides.expireClaims ?? (async () => []),
      failWithEvent: async () => null,
      get: async () => null,
      list: async () => [],
      listSnapshots: async () => [],
      recordApproval: async () => null,
      refreshClaim: async () => null,
      releaseWithEvent: async () => null,
      supersedeScheduled: async () => ({ events: [], tasks: [] }),
    },
  };
}

function createPublishInput() {
  return {
    eventId: "evt_idempotency",
    payload: { text: "retry" },
    producerId: "part_idempotency",
    sessionId: "sess_idempotency",
    type: sessionEventType.userMessage,
  };
}

function createTaskInput() {
  return {
    input: { priority: "high" },
    kind: "software_dev",
    objective: "Do the work",
    sessionId: "sess_idempotency",
    taskId: "task_idempotency",
  };
}

function createEvent(input: Partial<SessionEvent> = {}): SessionEvent {
  return {
    createdAt: "2026-01-01T00:00:00.000Z",
    eventId: "evt_idempotency",
    payload: { text: "retry" },
    producerId: "part_idempotency",
    seq: 1,
    sessionId: "sess_idempotency",
    type: sessionEventType.userMessage,
    ...input,
  };
}

function createTask(): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimId: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: { priority: "high" },
    kind: "software_dev",
    objective: "Do the work",
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: "sess_idempotency",
    taskId: "task_idempotency",
  };
}
