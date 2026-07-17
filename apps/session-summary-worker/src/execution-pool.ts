import { SessionSummaryWorkerError } from "./errors.js";

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
  #active = 0;
  #cancelled = 0;
  #completed = 0;
  #failed = 0;
  #rejected = 0;

  constructor(options: { readonly concurrency: number; readonly queueSize: number }) {
    this.#concurrency = positiveInteger(options.concurrency, "concurrency");
    this.#queueSize = positiveInteger(options.queueSize, "queueSize");
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
    if (this.#queue.length >= this.#queueSize) {
      this.#rejected += 1;
      throw new ExecutionPoolFullError();
    }
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
    };
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
