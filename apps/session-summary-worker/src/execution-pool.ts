import { SessionSummaryWorkerError } from "./errors.js";

/**
 * Time a keyed reservation survives before it is reclaimed on demand. It is
 * matched to the claim-acquisition window - the claim command plus the initial
 * progress publish that precede {@link BoundedExecutionPool.runReserved} - not
 * to task execution duration. A reservation only needs to outlive claim
 * acquisition or claim loss, so paths that reserve but never execute (lost
 * claim races, duplicate claimable events, discarded replay tasks) elapse on
 * this TTL instead of eroding capacity for the full length of a task.
 */
const defaultReservationTtlMs = 30_000;

/** Safe content-free execution-pool diagnostics. */
export interface ExecutionPoolDebugInfo {
  readonly active: number;
  readonly cancelled: number;
  readonly completed: number;
  readonly concurrency: number;
  readonly failed: number;
  readonly queueSize: number;
  readonly queued: number;
  readonly rejected: number;
  readonly reserved: number;
}

/** Terminal error when both the active and queued worker budgets are full. */
export class ExecutionPoolFullError extends SessionSummaryWorkerError {
  constructor() {
    super("generation_unavailable", "Session Summary worker execution queue is full", {
      retryable: true,
    });
    this.name = "ExecutionPoolFullError";
  }
}

interface QueuedExecution {
  readonly action: () => Promise<unknown>;
  readonly reject: (error: unknown) => void;
  readonly resolve: (value: unknown) => void;
  readonly signal: AbortSignal;
}

/** Fixed-size concurrency pool with a separately bounded FIFO queue. */
export class BoundedExecutionPool {
  readonly #concurrency: number;
  readonly #queueSize: number;
  readonly #queue: QueuedExecution[] = [];
  readonly #reservations = new Map<string, number>();
  readonly #reservationTtlMs: number;
  #active = 0;
  #cancelled = 0;
  #completed = 0;
  #failed = 0;
  #rejected = 0;

  constructor(options: {
    readonly concurrency: number;
    readonly queueSize: number;
    readonly reservationTtlMs?: number;
  }) {
    this.#concurrency = positiveInteger(options.concurrency, "concurrency");
    this.#queueSize = positiveInteger(options.queueSize, "queueSize");
    this.#reservationTtlMs = positiveInteger(
      options.reservationTtlMs ?? defaultReservationTtlMs,
      "reservationTtlMs",
    );
  }

  /**
   * Reserves one unit of the combined budget for a keyed future execution.
   *
   * Admission is decided here, before any task claim is initiated, so a full
   * pool declines the claim instead of winning a claim it can never execute.
   * Reservations whose executions never arrive (lost claim races, discarded
   * replay tasks) elapse and are reclaimed on demand before capacity is judged.
   */
  tryReserve(key: string): boolean {
    this.#reclaimElapsedReservations();
    if (this.#reservations.has(key)) {
      // A duplicate claimable event for an already-reserved task re-admits
      // idempotently but must NOT extend the original deadline: refreshing the
      // expiry would let a phantom reservation (one whose execution never
      // arrives) outlive its acquisition window indefinitely as long as
      // duplicate events keep re-emitting, instead of elapsing on its TTL.
      return true;
    }
    if (this.#committed() >= this.#concurrency + this.#queueSize) {
      this.#rejected += 1;
      return false;
    }
    this.#reservations.set(key, Date.now() + this.#reservationTtlMs);
    return true;
  }

  /** Runs now, queues within budget, or rejects without invoking the action. */
  async run<TResult>(action: () => Promise<TResult>, signal: AbortSignal): Promise<TResult> {
    if (signal.aborted) {
      this.#cancelled += 1;
      throw abortException();
    }
    if (this.#active < this.#concurrency) {
      return this.#runActive(action);
    }
    this.#reclaimElapsedReservations();
    if (this.#committed() >= this.#concurrency + this.#queueSize) {
      this.#rejected += 1;
      throw new ExecutionPoolFullError();
    }
    return this.#enqueue(action, signal);
  }

  /**
   * Runs one execution admitted earlier through {@link tryReserve}.
   *
   * The reservation is consumed here and admission is never re-judged:
   * rejecting a reserved execution after its task claim was won would fail the
   * task terminally on purely local backpressure.
   */
  async runReserved<TResult>(
    key: string,
    action: () => Promise<TResult>,
    signal: AbortSignal,
  ): Promise<TResult> {
    this.#reservations.delete(key);
    if (signal.aborted) {
      this.#cancelled += 1;
      throw abortException();
    }
    if (this.#active < this.#concurrency) {
      return this.#runActive(action);
    }
    return this.#enqueue(action, signal);
  }

  /** Returns a bounded content-free state snapshot. */
  debugInfo(): ExecutionPoolDebugInfo {
    return {
      active: this.#active,
      cancelled: this.#cancelled,
      completed: this.#completed,
      concurrency: this.#concurrency,
      failed: this.#failed,
      queueSize: this.#queueSize,
      queued: this.#queue.length,
      rejected: this.#rejected,
      reserved: this.#reservations.size,
    };
  }

  #committed(): number {
    return this.#active + this.#queue.length + this.#reservations.size;
  }

  #reclaimElapsedReservations(): void {
    const now = Date.now();
    for (const [key, expiresAt] of this.#reservations) {
      if (expiresAt <= now) {
        this.#reservations.delete(key);
      }
    }
  }

  #enqueue<TResult>(action: () => Promise<TResult>, signal: AbortSignal): Promise<TResult> {
    return new Promise<TResult>((resolve, reject) => {
      const queued: QueuedExecution = {
        action,
        reject,
        resolve: (value) => resolve(value as TResult),
        signal,
      };
      const abort = (): void => {
        const index = this.#queue.indexOf(queued);
        if (index < 0) {
          return;
        }
        this.#queue.splice(index, 1);
        this.#cancelled += 1;
        reject(abortException());
      };
      signal.addEventListener("abort", abort, { once: true });
      this.#queue.push(queued);
    });
  }

  async #runActive<TResult>(action: () => Promise<TResult>): Promise<TResult> {
    this.#active += 1;
    try {
      const result = await action();
      this.#completed += 1;
      return result;
    } catch (error) {
      this.#failed += 1;
      throw error;
    } finally {
      this.#active -= 1;
      this.#drain();
    }
  }

  #drain(): void {
    while (this.#active < this.#concurrency) {
      const queued = this.#queue.shift();
      if (!queued) {
        return;
      }
      if (queued.signal.aborted) {
        this.#cancelled += 1;
        queued.reject(abortException());
        continue;
      }
      void this.#runActive(queued.action).then(queued.resolve, queued.reject);
    }
  }
}

function abortException(): DOMException {
  return new DOMException("Execution was cancelled", "AbortError");
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}
