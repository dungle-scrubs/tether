import { taskIdFromCancelledEvent } from "./protocol.js";
import type { SessionEvent } from "./types.js";

/** Constructor settings for task cancellation memory. */
export interface TaskCancellationRegistryOptions {
  /** Maximum inactive cancelled task ids to remember. */
  readonly maxCancelledTaskIds?: number;
}

/** Inspectable cancellation registry state for runtime diagnostics. */
export interface TaskCancellationRegistryDebugInfo {
  /** Number of active task abort controllers. */
  readonly activeTaskCount: number;
  /** Number of active and inactive cancelled task ids currently remembered. */
  readonly cancelledTaskIdCount: number;
  /** Number of inactive cancelled task ids currently remembered. */
  readonly inactiveCancelledTaskIdCount: number;
  /** Maximum inactive cancelled task ids retained by this registry. */
  readonly maxCancelledTaskIds: number;
}

const defaultMaxCancelledTaskIds = 1024;

/** Tracks cancellation events and exposes abort signals for active task work. */
export class TaskCancellationRegistry {
  private readonly maxCancelledTaskIds: number;
  private readonly cancelledTaskIds = new Set<string>();
  private readonly controllersByTaskId = new Map<string, AbortController>();

  /** Creates a cancellation registry with bounded inactive cancellation memory. */
  constructor(options: TaskCancellationRegistryOptions = {}) {
    this.maxCancelledTaskIds = Math.max(
      0,
      options.maxCancelledTaskIds ?? defaultMaxCancelledTaskIds,
    );
  }

  /** Aborts in-flight work for a task without marking it cancelled. */
  abortActive(taskId: string): void {
    this.controllersByTaskId.get(taskId)?.abort();
  }

  /** Starts an abortable active-work scope for a task. */
  begin(taskId: string): AbortSignal {
    const controller = new AbortController();
    this.controllersByTaskId.set(taskId, controller);
    if (this.cancelledTaskIds.has(taskId)) {
      controller.abort();
    }
    return controller.signal;
  }

  /** Marks a task as cancelled and aborts in-flight work for it. */
  cancel(taskId: string): void {
    const controller = this.controllersByTaskId.get(taskId);
    controller?.abort();
    this.rememberCancellation(taskId);
  }

  /**
   * Ends active-work tracking for a task and drops its cancellation memory.
   *
   * Task ids are unique and terminal tasks never become claimable again, so it
   * is safe to forget a cancelled id once no executor is tracking it. This
   * keeps long-lived participant runtimes from accumulating cancelled ids
   * indefinitely.
   */
  end(taskId: string): void {
    this.controllersByTaskId.delete(taskId);
    this.cancelledTaskIds.delete(taskId);
  }

  /** Checks whether a task has been cancelled in the observed event stream. */
  isCancelled(taskId: string): boolean {
    return this.cancelledTaskIds.has(taskId);
  }

  /** Observes one session event and records task cancellation when present. */
  observe(event: SessionEvent): string | null {
    const taskId = taskIdFromCancelledEvent(event);
    if (taskId) {
      this.cancel(taskId);
    }
    return taskId;
  }

  /** Returns bounded cancellation-memory counters for diagnostics. */
  debugInfo(): TaskCancellationRegistryDebugInfo {
    return {
      activeTaskCount: this.controllersByTaskId.size,
      cancelledTaskIdCount: this.cancelledTaskIds.size,
      inactiveCancelledTaskIdCount: this.inactiveCancelledTaskIdCount(),
      maxCancelledTaskIds: this.maxCancelledTaskIds,
    };
  }

  private rememberCancellation(taskId: string): void {
    if (this.maxCancelledTaskIds === 0) {
      if (this.controllersByTaskId.has(taskId)) {
        this.cancelledTaskIds.add(taskId);
      }
      return;
    }
    this.cancelledTaskIds.delete(taskId);
    this.cancelledTaskIds.add(taskId);
    while (this.inactiveCancelledTaskIdCount() > this.maxCancelledTaskIds) {
      const oldestTaskId = this.cancelledTaskIds.values().next().value;
      if (typeof oldestTaskId !== "string") {
        return;
      }
      if (this.controllersByTaskId.has(oldestTaskId)) {
        this.cancelledTaskIds.delete(oldestTaskId);
        this.cancelledTaskIds.add(oldestTaskId);
        continue;
      }
      this.cancelledTaskIds.delete(oldestTaskId);
    }
  }

  private inactiveCancelledTaskIdCount(): number {
    let count = 0;
    for (const taskId of this.cancelledTaskIds) {
      if (!this.controllersByTaskId.has(taskId)) {
        count += 1;
      }
    }
    return count;
  }
}
