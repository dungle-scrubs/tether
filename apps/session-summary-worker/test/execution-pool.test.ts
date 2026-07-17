import { describe, expect, it, vi } from "vitest";

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

  it("declines keyed reservations once the combined budget is committed", () => {
    const pool = new BoundedExecutionPool({ concurrency: 1, queueSize: 1 });

    expect(pool.tryReserve("task-1")).toBe(true);
    expect(pool.tryReserve("task-2")).toBe(true);
    expect(pool.tryReserve("task-1")).toBe(true);
    expect(pool.tryReserve("task-3")).toBe(false);
    expect(pool.debugInfo()).toMatchObject({ rejected: 1, reserved: 2 });
  });

  it("consumes reservations on reserved runs and never rejects reserved admission", async () => {
    const pool = new BoundedExecutionPool({ concurrency: 1, queueSize: 1 });
    expect(pool.tryReserve("task-1")).toBe(true);
    expect(pool.tryReserve("task-2")).toBe(true);

    let releaseFirst = (): void => undefined;
    const first = pool.runReserved(
      "task-1",
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve;
        }),
      new AbortController().signal,
    );
    const second = pool.runReserved("task-2", async () => undefined, new AbortController().signal);

    expect(pool.debugInfo()).toMatchObject({ active: 1, queued: 1, reserved: 0 });
    expect(pool.tryReserve("task-3")).toBe(false);

    releaseFirst();
    await Promise.all([first, second]);
    expect(pool.tryReserve("task-3")).toBe(true);
  });

  it("counts live reservations against unkeyed queue admission", async () => {
    const pool = new BoundedExecutionPool({ concurrency: 1, queueSize: 1 });
    expect(pool.tryReserve("task-1")).toBe(true);
    let releaseActive = (): void => undefined;
    const active = pool.run(
      () =>
        new Promise<void>((resolve) => {
          releaseActive = resolve;
        }),
      new AbortController().signal,
    );

    await expect(
      pool.run(async () => undefined, new AbortController().signal),
    ).rejects.toBeInstanceOf(ExecutionPoolFullError);

    releaseActive();
    await active;
  });

  it("does not extend a reservation deadline when the same key is re-reserved", () => {
    vi.useFakeTimers();
    try {
      const pool = new BoundedExecutionPool({
        concurrency: 1,
        queueSize: 1,
        reservationTtlMs: 1_000,
      });
      expect(pool.tryReserve("task-1")).toBe(true);
      // A duplicate claimable event 900ms later re-admits task-1 idempotently
      // but must not push its deadline out to 1_900ms; the phantom reservation
      // still elapses 1_000ms after the original reserve.
      vi.advanceTimersByTime(900);
      expect(pool.tryReserve("task-1")).toBe(true);
      expect(pool.debugInfo()).toMatchObject({ reserved: 1 });

      vi.advanceTimersByTime(101);
      // task-1 elapsed on its original TTL, so the combined budget admits two
      // fresh reservations; an extended deadline would have blocked task-3.
      expect(pool.tryReserve("task-2")).toBe(true);
      expect(pool.tryReserve("task-3")).toBe(true);
      expect(pool.debugInfo()).toMatchObject({ reserved: 2 });
    } finally {
      vi.useRealTimers();
    }
  });

  it("recovers capacity after reserved claims are lost and never execute", () => {
    vi.useFakeTimers();
    try {
      const pool = new BoundedExecutionPool({
        concurrency: 1,
        queueSize: 1,
        reservationTtlMs: 1_000,
      });
      // Two claims are reserved but lost to other workers, so neither reaches
      // runReserved. They fill the combined budget and decline a third task.
      expect(pool.tryReserve("lost-1")).toBe(true);
      expect(pool.tryReserve("lost-2")).toBe(true);
      expect(pool.tryReserve("ready")).toBe(false);

      // Once the acquisition-window TTL elapses the phantom reservations are
      // reclaimed and the idle pool admits executable work again.
      vi.advanceTimersByTime(1_001);
      expect(pool.tryReserve("ready")).toBe(true);
      expect(pool.debugInfo()).toMatchObject({ active: 0, reserved: 1 });
    } finally {
      vi.useRealTimers();
    }
  });

  it("reclaims reservations on the default acquisition-window TTL, not a task-duration TTL", () => {
    vi.useFakeTimers();
    try {
      const pool = new BoundedExecutionPool({ concurrency: 1, queueSize: 1 });
      expect(pool.tryReserve("task-1")).toBe(true);
      expect(pool.tryReserve("task-2")).toBe(true);
      expect(pool.tryReserve("task-3")).toBe(false);

      // The default reservation TTL is matched to claim acquisition, so idle
      // capacity recovers within tens of seconds rather than the former
      // five-minute task-duration window.
      vi.advanceTimersByTime(30_001);
      expect(pool.tryReserve("task-3")).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it("reclaims elapsed reservations on demand", () => {
    vi.useFakeTimers();
    try {
      const pool = new BoundedExecutionPool({
        concurrency: 1,
        queueSize: 1,
        reservationTtlMs: 1_000,
      });
      expect(pool.tryReserve("task-1")).toBe(true);
      expect(pool.tryReserve("task-2")).toBe(true);
      expect(pool.tryReserve("task-3")).toBe(false);

      vi.advanceTimersByTime(1_001);
      expect(pool.tryReserve("task-3")).toBe(true);
      expect(pool.debugInfo()).toMatchObject({ reserved: 1 });
    } finally {
      vi.useRealTimers();
    }
  });
});
