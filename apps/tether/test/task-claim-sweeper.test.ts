import { afterEach, describe, expect, it, vi } from "vitest";
import { Effect } from "effect";

import { TaskClaimExpirationDeadlockError } from "../src/db.js";
import {
  TaskClaimSweeper,
  type TaskClaimSweeperSessionService,
} from "../src/task-claim-sweeper.js";
import type { SessionEvent } from "../src/types.js";

const claimExpiredEvent: SessionEvent = {
  createdAt: "2026-06-05T00:00:00.000Z",
  eventId: "evt_claim_expired",
  payload: {},
  producerId: "tether",
  seq: 1,
  sessionId: "sess_sweeper",
  type: "task.claim_expired",
};

describe("TaskClaimSweeper", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("stays disabled when interval is non-positive", () => {
    const service = createSessionServiceFixture();
    const sweeper = new TaskClaimSweeper({
      intervalMs: 0,
      onEvents: () => undefined,
      service: service.service,
    });

    sweeper.start();

    expect(sweeper.debugInfo()).toMatchObject({
      enabled: false,
      running: false,
      scheduled: false,
    });
  });

  it("broadcasts durably committed claim-expiration events", async () => {
    vi.useFakeTimers();
    const broadcasts: (readonly SessionEvent[])[] = [];
    const service = createSessionServiceFixture({
      expireTaskClaims: async () => ({ events: [claimExpiredEvent], expiredCount: 1 }),
    });
    const sweeper = new TaskClaimSweeper({
      batchSize: 7,
      intervalMs: 10,
      onEvents: (events) => broadcasts.push(events),
      service: service.service,
    });

    sweeper.start();
    await vi.advanceTimersByTimeAsync(0);

    expect(service.calls).toEqual([7]);
    expect(broadcasts).toEqual([[claimExpiredEvent]]);
    expect(sweeper.debugInfo().scheduled).toBe(true);

    await sweeper.stop();
  });

  it("logs tick failures and schedules the next pass", async () => {
    vi.useFakeTimers();
    const logged: unknown[] = [];
    vi.spyOn(console, "error").mockImplementation((error) => {
      logged.push(error);
    });
    const service = createSessionServiceFixture({
      expireTaskClaims: async () => {
        throw new Error("sweep failed");
      },
    });
    const sweeper = new TaskClaimSweeper({
      intervalMs: 10,
      onEvents: () => undefined,
      service: service.service,
    });

    sweeper.start();
    await vi.advanceTimersByTimeAsync(0);

    expect(logged).toHaveLength(1);
    expect(logged[0]).toBeInstanceOf(Error);
    expect(sweeper.debugInfo().scheduled).toBe(true);

    await sweeper.stop();
  });

  it("logs exhausted claim-expiration retry failures and schedules the next pass", async () => {
    vi.useFakeTimers();
    const logged: unknown[] = [];
    vi.spyOn(console, "error").mockImplementation((error) => {
      logged.push(error);
    });
    const retryError = new TaskClaimExpirationDeadlockError({
      diagnostics: {
        maxAttempts: 3,
        operation: "expireTaskClaims",
        requestedBatchSize: 50,
        retryAttempt: 3,
        sqlState: "40P01",
      },
      originalError: new Error("deadlock detected"),
    });
    const service = createSessionServiceFixture({
      expireTaskClaims: async () => {
        throw retryError;
      },
    });
    const sweeper = new TaskClaimSweeper({
      intervalMs: 10,
      onEvents: () => undefined,
      service: service.service,
    });

    sweeper.start();
    await vi.advanceTimersByTimeAsync(0);

    expect(logged).toHaveLength(1);
    expect(logged[0]).toBe(retryError);
    expect(sweeper.debugInfo().scheduled).toBe(true);

    await sweeper.stop();
  });

  it("does not create durable events for retry diagnostics", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const retryError = new TaskClaimExpirationDeadlockError({
      diagnostics: {
        maxAttempts: 1,
        operation: "expireTaskClaims",
        requestedBatchSize: 50,
        retryAttempt: 1,
        sqlState: "40P01",
      },
      originalError: new Error("deadlock detected"),
    });
    const broadcasts: (readonly SessionEvent[])[] = [];
    const service = createSessionServiceFixture({
      expireTaskClaims: async () => {
        throw retryError;
      },
    });
    const sweeper = new TaskClaimSweeper({
      intervalMs: 10,
      onEvents: (events) => broadcasts.push(events),
      service: service.service,
    });

    sweeper.start();
    await vi.advanceTimersByTimeAsync(0);

    expect(broadcasts).toEqual([]);

    await sweeper.stop();
  });

  it("waits for an in-flight tick before stop resolves", async () => {
    vi.useFakeTimers();
    const tick = createDeferred<{
      readonly events: readonly SessionEvent[];
      readonly expiredCount: number;
    }>();
    const service = createSessionServiceFixture({
      expireTaskClaims: () => tick.promise,
    });
    const sweeper = new TaskClaimSweeper({
      intervalMs: 10,
      onEvents: () => undefined,
      service: service.service,
    });

    sweeper.start();
    await vi.advanceTimersByTimeAsync(0);
    const stopped = sweeper.stop();
    let resolved = false;
    stopped.then(() => {
      resolved = true;
    });
    await flushPromises();

    expect(resolved).toBe(false);
    tick.resolve({ events: [], expiredCount: 0 });
    await stopped;

    expect(resolved).toBe(true);
    expect(sweeper.debugInfo()).toMatchObject({
      running: false,
      scheduled: false,
    });
  });
});

interface SessionServiceFixture {
  readonly calls: number[];
  readonly service: TaskClaimSweeperSessionService;
}

/** Builds the small service surface used by the task claim sweeper. */
function createSessionServiceFixture(
  options: {
    readonly expireTaskClaims?: (input: { readonly batchSize: number }) => Promise<{
      readonly events: readonly SessionEvent[];
      readonly expiredCount: number;
    }>;
  } = {},
): SessionServiceFixture {
  const calls: number[] = [];
  return {
    calls,
    service: {
      expireTaskClaims: (input) => {
        calls.push(input.batchSize);
        return Effect.tryPromise({
          catch: (error) => error,
          try: () =>
            options.expireTaskClaims?.(input) ?? Promise.resolve({ events: [], expiredCount: 0 }),
        });
      },
    },
  };
}

/** Creates a manually controlled promise for scheduler tests. */
function createDeferred<TValue>(): {
  readonly promise: Promise<TValue>;
  readonly resolve: (value: TValue | PromiseLike<TValue>) => void;
} {
  let resolve: (value: TValue | PromiseLike<TValue>) => void = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

/** Allows queued promise continuations to run. */
async function flushPromises(): Promise<void> {
  await Promise.resolve();
}
