import { describe, expect, it, vi } from "vitest";

import {
  ParticipantCursorWriter,
  ParticipantRuntimeCursorPersistError,
} from "../src/participant-cursor-writer.js";

describe("ParticipantCursorWriter", () => {
  it("serializes writes and coalesces updates to the highest pending sequence", async () => {
    const firstWrite = createDeferred<void>();
    const secondWrite = createDeferred<void>();
    const writes: number[] = [];
    const writer = new ParticipantCursorWriter({
      acknowledgedSeq: 0,
      onError: () => undefined,
      retryAttempts: 1,
      retryBaseDelayMs: 100,
      retryMaxDelayMs: 2_000,
      store: {
        write: async (seq) => {
          writes.push(seq);
          await (writes.length === 1 ? firstWrite.promise : secondWrite.promise);
        },
      },
      writeTimeoutMs: 5_000,
    });

    writer.update(1);
    const firstFlush = writer.flush();
    writer.update(2);
    const joinedFlush = writer.flush();
    await flushMicrotasks();

    expect(firstFlush).toBe(joinedFlush);
    expect(writes).toEqual([1]);
    firstWrite.resolve();
    await flushMicrotasks();
    expect(writes).toEqual([1, 2]);
    secondWrite.resolve();
    await expect(firstFlush).resolves.toMatchObject({ status: "acknowledged", seq: 2 });
    expect(writer.debugInfo()).toMatchObject({
      acknowledgedSeq: 2,
      inFlightSeq: null,
      pendingSeq: null,
    });
  });

  it("keeps a rejected write pending without advancing durable acknowledgement", async () => {
    const errors: Error[] = [];
    const writer = new ParticipantCursorWriter({
      acknowledgedSeq: 3,
      onError: (error) => errors.push(error),
      retryAttempts: 1,
      retryBaseDelayMs: 100,
      retryMaxDelayMs: 2_000,
      store: {
        write: () => Promise.reject(new Error("database unavailable: secret details")),
      },
      writeTimeoutMs: 5_000,
    });

    writer.update(4);

    await expect(writer.flush()).resolves.toMatchObject({ pendingSeq: 4, status: "pending" });
    expect(writer.debugInfo()).toMatchObject({
      acknowledgedSeq: 3,
      failureCount: 1,
      inFlightSeq: null,
      pendingSeq: 4,
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(ParticipantRuntimeCursorPersistError);
    expect(errors[0]).toMatchObject({ attempts: 1, reason: "write_failed", targetSeq: 4 });
    expect(errors[0]?.message).not.toContain("secret details");
  });

  it("times out writes and retries with bounded exponential backoff", async () => {
    vi.useFakeTimers();
    try {
      const writes: number[] = [];
      const errors: Error[] = [];
      const writer = new ParticipantCursorWriter({
        acknowledgedSeq: 0,
        onError: (error) => errors.push(error),
        retryAttempts: 3,
        retryBaseDelayMs: 100,
        retryMaxDelayMs: 150,
        store: {
          write: (seq) => {
            writes.push(seq);
            return new Promise<void>(() => undefined);
          },
        },
        writeTimeoutMs: 5_000,
      });

      writer.update(7);
      const flush = writer.flush();
      expect(writes).toEqual([7]);

      await vi.advanceTimersByTimeAsync(5_099);
      expect(writes).toEqual([7]);
      await vi.advanceTimersByTimeAsync(1);
      expect(writes).toEqual([7, 7]);
      await vi.advanceTimersByTimeAsync(5_149);
      expect(writes).toEqual([7, 7]);
      await vi.advanceTimersByTimeAsync(1);
      expect(writes).toEqual([7, 7, 7]);
      await vi.advanceTimersByTimeAsync(5_000);

      await expect(flush).resolves.toMatchObject({ pendingSeq: 7, status: "pending" });
      expect(errors).toHaveLength(1);
      expect(errors[0]).toMatchObject({
        attempts: 3,
        reason: "write_timeout",
        targetSeq: 7,
        writeTimeoutMs: 5_000,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("targets newer handled progress when it arrives during retry backoff", async () => {
    vi.useFakeTimers();
    try {
      const writes: number[] = [];
      const writer = new ParticipantCursorWriter({
        acknowledgedSeq: 0,
        onError: () => undefined,
        retryAttempts: 2,
        retryBaseDelayMs: 100,
        retryMaxDelayMs: 2_000,
        store: {
          write: (seq) => {
            writes.push(seq);
            return writes.length === 1 ? Promise.reject(new Error("offline")) : undefined;
          },
        },
        writeTimeoutMs: 5_000,
      });

      writer.update(1);
      const flush = writer.flush();
      await flushMicrotasks();
      writer.update(2);
      await vi.advanceTimersByTimeAsync(100);

      await expect(flush).resolves.toEqual({ seq: 2, status: "acknowledged" });
      expect(writes).toEqual([1, 2]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("preserves durable progress when an atomic-max timed-out write finishes late", async () => {
    vi.useFakeTimers();
    try {
      const firstWrite = createDeferred<void>();
      let persistedSeq = 0;
      let writeCount = 0;
      const writer = new ParticipantCursorWriter({
        acknowledgedSeq: 0,
        onError: () => undefined,
        retryAttempts: 2,
        retryBaseDelayMs: 100,
        retryMaxDelayMs: 2_000,
        store: {
          write: async (seq) => {
            writeCount += 1;
            if (writeCount === 1) {
              await firstWrite.promise;
            }
            persistedSeq = Math.max(persistedSeq, seq);
          },
        },
        writeTimeoutMs: 5_000,
      });

      writer.update(1);
      const flush = writer.flush();
      writer.update(2);
      await vi.advanceTimersByTimeAsync(5_100);

      await expect(flush).resolves.toEqual({ seq: 2, status: "acknowledged" });
      expect(persistedSeq).toBe(2);
      firstWrite.resolve();
      await flushMicrotasks();
      expect(persistedSeq).toBe(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("starts a later bounded cycle after exhaustion without losing pending progress", async () => {
    let available = false;
    const writes: number[] = [];
    const errors: Error[] = [];
    const writer = new ParticipantCursorWriter({
      acknowledgedSeq: 10,
      onError: (error) => errors.push(error),
      retryAttempts: 1,
      retryBaseDelayMs: 100,
      retryMaxDelayMs: 2_000,
      store: {
        write: (seq) => {
          writes.push(seq);
          if (!available) {
            throw new Error("offline");
          }
        },
      },
      writeTimeoutMs: 5_000,
    });

    writer.update(11);
    await expect(writer.flush()).resolves.toMatchObject({ status: "pending" });
    available = true;

    await expect(writer.flush()).resolves.toEqual({ seq: 11, status: "acknowledged" });
    expect(writes).toEqual([11, 11]);
    expect(errors).toHaveLength(1);
  });

  it("is a monotonic no-op when no cursor store is configured", async () => {
    const writer = new ParticipantCursorWriter({
      acknowledgedSeq: 4,
      onError: () => undefined,
      retryAttempts: 5,
      retryBaseDelayMs: 100,
      retryMaxDelayMs: 2_000,
      writeTimeoutMs: 5_000,
    });

    writer.update(9);

    await expect(writer.flush()).resolves.toEqual({ status: "no-store" });
    expect(writer.debugInfo()).toEqual({
      acknowledgedSeq: 4,
      failureCount: 0,
      inFlightSeq: null,
      pendingSeq: null,
    });
  });
});

interface Deferred<TValue> {
  readonly promise: Promise<TValue>;
  readonly resolve: (value?: TValue | PromiseLike<TValue>) => void;
}

/** Creates a manually controlled promise. */
function createDeferred<TValue>(): Deferred<TValue> {
  let resolve: Deferred<TValue>["resolve"] = () => undefined;
  const promise = new Promise<TValue>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

/** Lets queued Promise continuations settle without timers. */
async function flushMicrotasks(iterations = 20): Promise<void> {
  for (let index = 0; index < iterations; index += 1) {
    await Promise.resolve();
  }
}
