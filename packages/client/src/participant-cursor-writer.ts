/**
 * Owns serialized, monotonic participant cursor persistence.
 *
 * The Module coalesces handled progress behind one write loop and advances its
 * acknowledged state only after the durable store confirms a write. It does
 * not own event delivery, sockets, reconnects, or shutdown policy.
 */

/** Durable write surface required by the participant cursor writer. */
export interface ParticipantCursorWriterStore {
  /**
   * Atomically retains the maximum stored sequence. Implementations must not
   * let an earlier timed-out call overwrite a later acknowledged sequence.
   */
  readonly write: (seq: number) => void | Promise<void>;
}

/** Construction policy for one participant cursor writer. */
export interface ParticipantCursorWriterOptions {
  readonly acknowledgedSeq: number;
  readonly onError: (error: Error) => void;
  readonly retryAttempts: number;
  readonly retryBaseDelayMs: number;
  readonly retryMaxDelayMs: number;
  readonly store?: ParticipantCursorWriterStore;
  readonly writeTimeoutMs: number;
}

/** Typed terminal error emitted after one bounded cursor write cycle exhausts. */
export class ParticipantRuntimeCursorPersistError extends Error {
  /** Number of write attempts made in the exhausted cycle. */
  readonly attempts: number;
  /** Safe category for the final failed attempt. */
  readonly reason: "write_failed" | "write_timeout";
  /** Highest cursor sequence targeted by the final failed attempt. */
  readonly targetSeq: number;
  /** Per-attempt write deadline. */
  readonly writeTimeoutMs: number;

  constructor(input: {
    readonly attempts: number;
    readonly cause?: unknown;
    readonly reason: "write_failed" | "write_timeout";
    readonly targetSeq: number;
    readonly writeTimeoutMs: number;
  }) {
    super(
      "Participant cursor persistence retry cycle exhausted",
      input.cause === undefined ? undefined : { cause: input.cause },
    );
    this.attempts = input.attempts;
    this.name = "ParticipantRuntimeCursorPersistError";
    this.reason = input.reason;
    this.targetSeq = input.targetSeq;
    this.writeTimeoutMs = input.writeTimeoutMs;
  }
}

/** Readonly acknowledgement and retry state for participant diagnostics. */
export interface ParticipantCursorWriterDebugInfo {
  readonly acknowledgedSeq: number;
  readonly failureCount: number;
  readonly inFlightSeq: number | null;
  readonly pendingSeq: number | null;
}

/** Result of one coalesced cursor flush cycle. */
export type ParticipantCursorFlushOutcome =
  | { readonly seq: number; readonly status: "acknowledged" }
  | { readonly status: "no-store" }
  | { readonly error: Error; readonly pendingSeq: number; readonly status: "pending" };

type CursorWriteAttemptOutcome =
  | { readonly status: "acknowledged" }
  | {
      readonly cause?: unknown;
      readonly reason: "write_failed" | "write_timeout";
      readonly status: "failed";
    };

/** Serializes and coalesces cursor writes for one participant runtime. */
export class ParticipantCursorWriter {
  private acknowledgedSeq: number;
  private cyclePromise: Promise<ParticipantCursorFlushOutcome> | null = null;
  private failureCount = 0;
  private inFlightSeq: number | null = null;
  private pendingSeq: number | null = null;

  constructor(private readonly options: ParticipantCursorWriterOptions) {
    this.acknowledgedSeq = options.acknowledgedSeq;
  }

  /** Returns acknowledged, pending, in-flight, and failure state. */
  debugInfo(): ParticipantCursorWriterDebugInfo {
    return {
      acknowledgedSeq: this.acknowledgedSeq,
      failureCount: this.failureCount,
      inFlightSeq: this.inFlightSeq,
      pendingSeq: this.pendingSeq,
    };
  }

  /** Joins or starts one serialized write cycle for the pending high-water mark. */
  flush(): Promise<ParticipantCursorFlushOutcome> {
    if (!this.options.store) {
      return Promise.resolve({ status: "no-store" });
    }
    if (this.cyclePromise) {
      return this.cyclePromise;
    }
    const cycle = this.runFlushCycle();
    this.cyclePromise = cycle;
    const clearCycle = (): void => {
      if (this.cyclePromise === cycle) {
        this.cyclePromise = null;
      }
    };
    void cycle.then(clearCycle, clearCycle);
    return cycle;
  }

  /** Raises the pending cursor without ever regressing acknowledged state. */
  update(seq: number): void {
    if (!this.options.store || seq <= this.acknowledgedSeq) {
      return;
    }
    this.pendingSeq = Math.max(this.pendingSeq ?? seq, seq);
  }

  /** Drains every pending high-water mark through one store write at a time. */
  private async runFlushCycle(): Promise<ParticipantCursorFlushOutcome> {
    const store = this.options.store;
    if (!store) {
      return { status: "no-store" };
    }
    while (this.pendingSeq !== null && this.pendingSeq > this.acknowledgedSeq) {
      let acknowledged = false;
      for (let attempt = 1; attempt <= this.options.retryAttempts; attempt += 1) {
        const targetSeq = this.pendingSeq;
        this.inFlightSeq = targetSeq;
        const outcome = await this.runWriteAttempt(store, targetSeq);
        this.inFlightSeq = null;
        if (outcome.status === "acknowledged") {
          this.acknowledgedSeq = targetSeq;
          acknowledged = true;
          if (this.pendingSeq <= this.acknowledgedSeq) {
            this.pendingSeq = null;
          }
          break;
        }
        this.failureCount += 1;
        if (attempt === this.options.retryAttempts) {
          const error = new ParticipantRuntimeCursorPersistError({
            attempts: attempt,
            ...(outcome.cause === undefined ? {} : { cause: outcome.cause }),
            reason: outcome.reason,
            targetSeq,
            writeTimeoutMs: this.options.writeTimeoutMs,
          });
          this.options.onError(error);
          return {
            error,
            pendingSeq: this.pendingSeq,
            status: "pending",
          };
        }
        await waitForDelay(
          Math.min(
            this.options.retryMaxDelayMs,
            this.options.retryBaseDelayMs * 2 ** (attempt - 1),
          ),
        );
      }
      if (!acknowledged) {
        break;
      }
    }
    return { seq: this.acknowledgedSeq, status: "acknowledged" };
  }

  /** Applies one write deadline without letting a late result change writer acknowledgement. */
  private async runWriteAttempt(
    store: ParticipantCursorWriterStore,
    targetSeq: number,
  ): Promise<CursorWriteAttemptOutcome> {
    let write: void | Promise<void>;
    try {
      write = store.write(targetSeq);
    } catch (cause) {
      return { cause, reason: "write_failed", status: "failed" };
    }
    let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
    const timeout = new Promise<CursorWriteAttemptOutcome>((resolve) => {
      timeoutHandle = setTimeout(() => {
        resolve({ reason: "write_timeout", status: "failed" });
      }, this.options.writeTimeoutMs);
    });
    const result = Promise.resolve(write).then<
      CursorWriteAttemptOutcome,
      CursorWriteAttemptOutcome
    >(
      () => ({ status: "acknowledged" }),
      (cause: unknown) => ({ cause, reason: "write_failed", status: "failed" }),
    );
    const outcome = await Promise.race([result, timeout]);
    if (timeoutHandle !== null) {
      clearTimeout(timeoutHandle);
    }
    return outcome;
  }
}

/** Waits for one retry delay while retaining the process until durable settlement. */
async function waitForDelay(delayMs: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, delayMs);
  });
}
