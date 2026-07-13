import { describe, expect, it } from "vitest";

import {
  runTaskClaimExpirationTransactionWithDeadlockRetry,
  sortExpiredTaskClaimRows,
} from "../src/db.js";
import type { TaskClaimExpirationDeadlockError } from "../src/db.js";

describe("task claim expiration persistence helpers", () => {
  it("sorts expired rows by session id and task id", () => {
    const sorted = sortExpiredTaskClaimRows([
      { sessionId: "sess_b", taskId: "task_2" },
      { sessionId: "sess_a", taskId: "task_2" },
      { sessionId: "sess_a", taskId: "task_1" },
    ]);

    expect(sorted).toEqual([
      { sessionId: "sess_a", taskId: "task_1" },
      { sessionId: "sess_a", taskId: "task_2" },
      { sessionId: "sess_b", taskId: "task_2" },
    ]);
  });

  it("retries Postgres deadlocks after rolling back the failed attempt", async () => {
    const firstClient = new FakeTransactionClient();
    const secondClient = new FakeTransactionClient();
    const pool = new FakeTransactionPool([firstClient, secondClient]);
    const deadlock = new Error("deadlock detected") as Error & { readonly code: string };
    Object.defineProperty(deadlock, "code", { value: "40P01" });

    const result = await runTaskClaimExpirationTransactionWithDeadlockRetry({
      action: async (_client, context) => {
        if (context.attempt === 1) {
          context.recordExpiredRowCount(2);
          throw deadlock;
        }
        return "committed";
      },
      maxAttempts: 2,
      pool,
      requestedBatchSize: 7,
    });

    expect(result).toBe("committed");
    expect(firstClient.queries).toEqual(["BEGIN", "ROLLBACK"]);
    expect(secondClient.queries).toEqual(["BEGIN", "COMMIT"]);
    expect(firstClient.releaseCount).toBe(1);
    expect(secondClient.releaseCount).toBe(1);
  });

  it("does not retry non-deadlock failures", async () => {
    const client = new FakeTransactionClient();
    const pool = new FakeTransactionPool([client]);
    const failure = new Error("permission denied");

    await expect(
      runTaskClaimExpirationTransactionWithDeadlockRetry({
        action: async () => {
          throw failure;
        },
        maxAttempts: 3,
        pool,
        requestedBatchSize: 7,
      }),
    ).rejects.toBe(failure);

    expect(client.queries).toEqual(["BEGIN", "ROLLBACK"]);
    expect(client.releaseCount).toBe(1);
  });

  it("stops after max deadlock attempts and preserves retry metadata", async () => {
    const firstClient = new FakeTransactionClient();
    const secondClient = new FakeTransactionClient();
    const pool = new FakeTransactionPool([firstClient, secondClient]);
    const deadlock = new Error("deadlock detected") as Error & { readonly code: string };
    Object.defineProperty(deadlock, "code", { value: "40P01" });

    await expect(
      runTaskClaimExpirationTransactionWithDeadlockRetry({
        action: async (_client, context) => {
          context.recordExpiredRowCount(context.attempt);
          throw deadlock;
        },
        maxAttempts: 2,
        operation: "expireTaskClaims",
        pool,
        requestedBatchSize: 9,
      }),
    ).rejects.toMatchObject({
      diagnostics: {
        expiredRowCount: 2,
        maxAttempts: 2,
        operation: "expireTaskClaims",
        requestedBatchSize: 9,
        retryAttempt: 2,
        sqlState: "40P01",
      },
      originalError: deadlock,
    } satisfies Partial<TaskClaimExpirationDeadlockError>);

    expect(firstClient.queries).toEqual(["BEGIN", "ROLLBACK"]);
    expect(secondClient.queries).toEqual(["BEGIN", "ROLLBACK"]);
    expect(firstClient.releaseCount).toBe(1);
    expect(secondClient.releaseCount).toBe(1);
  });
});

/** Minimal pg client test double for transaction retry behavior. */
class FakeTransactionClient {
  readonly queries: string[] = [];
  releaseCount = 0;

  async query<TRow>(sql: string): Promise<{ readonly rows: TRow[] }> {
    this.queries.push(sql);
    return { rows: [] };
  }

  release(): void {
    this.releaseCount += 1;
  }
}

/** Queues fake clients in the same shape as a pg pool. */
class FakeTransactionPool {
  constructor(private readonly clients: FakeTransactionClient[]) {}

  async connect(): Promise<FakeTransactionClient> {
    const client = this.clients.shift();
    if (!client) {
      throw new Error("No fake transaction client queued");
    }
    return client;
  }
}
