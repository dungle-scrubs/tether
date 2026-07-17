import { describe, expect, it } from "vitest";

import { BoundedExecutionPool, ExecutionPoolFullError } from "../src/execution-pool.js";

describe("BoundedExecutionPool", () => {
  it("bounds active and queued executions and rejects overflow", async () => {
    const pool = new BoundedExecutionPool({ concurrency: 1, queueSize: 1 });
    let releaseFirst = (): void => undefined;
    const first = pool.run(
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve;
        }),
      new AbortController().signal,
    );
    const second = pool.run(async () => undefined, new AbortController().signal);

    await expect(
      pool.run(async () => undefined, new AbortController().signal),
    ).rejects.toBeInstanceOf(ExecutionPoolFullError);
    expect(pool.debugInfo()).toMatchObject({ active: 1, queued: 1, rejected: 1 });

    releaseFirst();
    await Promise.all([first, second]);
    expect(pool.debugInfo()).toMatchObject({ active: 0, completed: 2, queued: 0 });
  });

  it("removes an aborted queued execution without running it", async () => {
    const pool = new BoundedExecutionPool({ concurrency: 1, queueSize: 1 });
    let releaseFirst = (): void => undefined;
    const first = pool.run(
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve;
        }),
      new AbortController().signal,
    );
    const controller = new AbortController();
    let ran = false;
    const queued = pool.run(async () => {
      ran = true;
    }, controller.signal);

    controller.abort();
    await expect(queued).rejects.toBeInstanceOf(DOMException);
    releaseFirst();
    await first;

    expect(ran).toBe(false);
    expect(pool.debugInfo()).toMatchObject({ active: 0, cancelled: 1, queued: 0 });
  });
});
