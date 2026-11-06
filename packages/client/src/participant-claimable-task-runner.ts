import { Effect } from "effect";

import {
  isTransientRestReplayFailure,
  ParticipantRestReplayError,
  replayTaskByteCost,
  taskFromReplayStateEvent,
} from "./participant-rest-replay.js";
import { taskFromClaimableEvent } from "./protocol.js";
import { TaskCancellationRegistry } from "./task-cancellation-registry.js";
import type {
  ParticipantRuntimeTaskLoopOptions,
  ParticipantTaskExecutor,
} from "./participant-runtime-client.js";
import type { SessionEvent, TaskRecord } from "./types.js";

/** Cancellation state passed from the claimable-task runner into one task flow. */
export interface TaskCancellationContext {
  /** Aborts the active executor signal for this task. */
  readonly abortActive: () => void;
  /** Returns whether cancellation has been observed for this task. */
  readonly isCancelled: () => boolean;
  /** Executor cancellation signal. */
  readonly signal: AbortSignal;
}

/** Minimal participant client surface needed by the claimable-task runner. */
export interface ParticipantClaimableTaskRunnerClient {
  /** Intentionally closes the participant client. */
  readonly close: () => void;
  /** Returns the transport generation currently admitting event callbacks. */
  readonly connectionGeneration: () => number;
  /** Returns whether the participant client is intentionally stopped. */
  readonly isStopped: () => boolean;
  /** Returns whether terminal delivery policy requires explicit remediation. */
  readonly isPaused: () => boolean;
  /** REST recovery owns finite task state and loads candidates before dispatch. */
  readonly taskRecovery?: {
    readonly load: () => Promise<readonly TaskRecord[]>;
    readonly maxQueueBytes: number;
    readonly maxQueueSize: number;
    readonly fail: (error: ParticipantRestReplayError) => void;
  };
  /** Registers a live/replayed event handler. */
  readonly onEvent: (handler: (event: SessionEvent) => void) => () => void;
  /** Reconnects the participant stream using the client's configured backoff. */
  readonly reconnectAfterClose: () => Promise<void>;
  /** Runs one claimed task flow. */
  readonly runTaskClaimFlow: (input: {
    readonly cancellation: TaskCancellationContext;
    readonly claimRefreshMs: number;
    readonly executor: ParticipantTaskExecutor;
    readonly task: TaskRecord;
  }) => Promise<void>;
  /** Resolves when the current socket closes. */
  readonly waitForClose: () => Promise<void>;
  /** Resolves after server replay is complete. */
  readonly waitForReplayComplete: () => Promise<void>;
}

/** One queued task's reservation, shared by replay, snapshot and live admission. */
interface QueuedRecoveryTask {
  readonly kind: "queued";
  readonly task: TaskRecord;
  readonly generation: number;
  readonly bytes: number;
  readonly touchedGeneration: number | null;
}

/** Exclusive states retained under a single task-id reservation. */
type RecoveryTaskSlot =
  | QueuedRecoveryTask
  | {
      readonly kind: "running";
      readonly task: TaskRecord;
      readonly generation: number;
      readonly activeBytes: number;
      readonly next: QueuedRecoveryTask | null;
      readonly bytes: number;
      readonly touchedGeneration: number | null;
    }
  | {
      readonly kind: "touched";
      readonly generation: number;
      readonly bytes: number;
      readonly touchedGeneration: number;
    };

/** Owns all REST candidate, running and snapshot-touch reservations. */
class RecoveryTaskState {
  private readonly slots = new Map<string, RecoveryTaskSlot>();
  private bytes = 0;
  private snapshotGeneration: number | null = null;

  /** Uses the existing delivery policy without adding a runtime tuning surface. */
  constructor(
    private readonly limits: { readonly maxQueueBytes: number; readonly maxQueueSize: number },
  ) {}

  /** Creates one canonical queued reservation with a precomputed cost. */
  private queued(
    task: TaskRecord,
    generation: number,
    touchedGeneration: number | null,
  ): QueuedRecoveryTask {
    return { kind: "queued", task, generation, bytes: replayTaskByteCost(task), touchedGeneration };
  }

  /** Checks a replacement's net charge before retaining it. */
  private put(taskId: string, slot: RecoveryTaskSlot): void {
    const previous = this.slots.get(taskId);
    const observedQueueSize = this.slots.size + (previous ? 0 : 1);
    if (observedQueueSize > this.limits.maxQueueSize) {
      throw new ParticipantRestReplayError("delivery_queue_overflow", {
        maxQueueSize: this.limits.maxQueueSize,
        observedQueueSize,
      });
    }
    const observedQueueBytes = this.bytes - (previous?.bytes ?? 0) + slot.bytes;
    if (observedQueueBytes > this.limits.maxQueueBytes) {
      throw new ParticipantRestReplayError("delivery_byte_overflow", {
        maxQueueBytes: this.limits.maxQueueBytes,
        observedQueueBytes,
      });
    }
    this.slots.set(taskId, slot);
    this.bytes = observedQueueBytes;
  }

  /** Releases exactly the reservation currently owned by one task id. */
  private remove(taskId: string): void {
    const slot = this.slots.get(taskId);
    if (!slot) return;
    this.bytes -= slot.bytes;
    this.slots.delete(taskId);
  }

  /** Observes canonical lifecycle state while preserving running claim ownership. */
  observe(task: TaskRecord, generation: number, claimable: boolean, selected: boolean): void {
    const previous = this.slots.get(task.taskId);
    const touchedGeneration = this.snapshotGeneration === generation ? generation : null;
    if (previous?.kind === "running") {
      const next =
        selected && (claimable || previous.next) ? this.queued(task, generation, null) : null;
      this.put(task.taskId, {
        ...previous,
        next,
        bytes: previous.activeBytes + (next?.bytes ?? 0),
        touchedGeneration,
      });
    } else if (selected && (claimable || previous?.kind === "queued")) {
      this.put(task.taskId, this.queued(task, generation, touchedGeneration));
    } else if (touchedGeneration !== null) {
      this.put(task.taskId, {
        kind: "touched",
        generation,
        touchedGeneration,
        bytes: Buffer.byteLength(JSON.stringify(task.taskId)),
      });
    } else {
      this.remove(task.taskId);
    }
  }

  /** Starts one generation's snapshot window without a separate unbounded touched-id set. */
  beginSnapshot(generation: number): void {
    this.snapshotGeneration = generation;
  }

  /** Merges only rows not superseded by live state or a running claim. */
  seed(task: TaskRecord, generation: number): void {
    const previous = this.slots.get(task.taskId);
    if (previous?.kind === "running" || previous?.touchedGeneration === generation) return;
    this.put(task.taskId, this.queued(task, generation, null));
  }

  /** Releases snapshot tombstones and flags, without touching a newer snapshot. */
  endSnapshot(generation: number): void {
    if (this.snapshotGeneration === generation) this.snapshotGeneration = null;
    for (const [taskId, slot] of this.slots) {
      if (slot.touchedGeneration !== generation) continue;
      if (slot.kind === "touched") this.remove(taskId);
      else this.slots.set(taskId, { ...slot, touchedGeneration: null });
    }
  }

  /** Takes the oldest eligible queued task; its reservation remains charged while running. */
  take(generation: number): TaskRecord | null {
    for (const [taskId, slot] of this.slots) {
      if (slot.kind !== "queued" || slot.generation !== generation) continue;
      this.slots.set(taskId, { ...slot, kind: "running", activeBytes: slot.bytes, next: null });
      return slot.task;
    }
    return null;
  }

  /** Releases settled running work or retains its later claimable lifecycle update. */
  complete(taskId: string): void {
    const slot = this.slots.get(taskId);
    if (slot?.kind !== "running") return;
    if (slot.next) this.put(taskId, { ...slot.next, touchedGeneration: slot.touchedGeneration });
    else this.remove(taskId);
  }

  /** Returns whether this generation has work waiting behind the active claim. */
  hasPending(generation: number): boolean {
    for (const slot of this.slots.values()) {
      if (slot.kind === "queued" && slot.generation === generation) return true;
      if (slot.kind === "running" && slot.next?.generation === generation) return true;
    }
    return false;
  }

  /** Discards stale queued work while retaining the one running claim's reservation. */
  discard(generation: number): void {
    if (this.snapshotGeneration === generation) this.snapshotGeneration = null;
    for (const [taskId, slot] of this.slots) {
      if (slot.kind !== "running") {
        if (slot.generation === generation) this.remove(taskId);
      } else {
        const next = slot.next?.generation === generation ? null : slot.next;
        this.put(taskId, {
          ...slot,
          next,
          bytes: slot.activeBytes + (next?.bytes ?? 0),
          touchedGeneration: slot.touchedGeneration === generation ? null : slot.touchedGeneration,
        });
      }
    }
  }
}

/** Filters terminal rows before custom selection or task dispatch. */
function isDispatchableClaimableTask(task: TaskRecord): boolean {
  return !task.completedAt && !task.failedAt && !task.cancelledAt;
}

/** Uses the existing claim flow and cancellation registry for either transport mode. */
async function processTask(
  client: Pick<ParticipantClaimableTaskRunnerClient, "runTaskClaimFlow">,
  executor: ParticipantTaskExecutor,
  claimRefreshMs: number,
  cancellations: TaskCancellationRegistry,
  processing: Set<string>,
  task: TaskRecord,
): Promise<void> {
  if (processing.has(task.taskId) || cancellations.isCancelled(task.taskId)) return;
  const signal = cancellations.begin(task.taskId);
  processing.add(task.taskId);
  try {
    await client.runTaskClaimFlow({
      cancellation: {
        abortActive: () => cancellations.abortActive(task.taskId),
        isCancelled: () => cancellations.isCancelled(task.taskId),
        signal,
      },
      claimRefreshMs,
      executor,
      task,
    });
  } finally {
    cancellations.end(task.taskId);
    processing.delete(task.taskId);
  }
}

/** Builds the replay/live claimable-task loop for one participant runtime. */
export function buildParticipantClaimableTaskLoop(
  client: ParticipantClaimableTaskRunnerClient,
  options: ParticipantRuntimeTaskLoopOptions,
): Effect.Effect<void, unknown> {
  const cancellations = new TaskCancellationRegistry();
  const processing = new Set<string>();
  const pendingWork = new Set<Promise<void>>();
  const replayTasks = new Map<string, { readonly generation: number; readonly task: TaskRecord }>();
  const recovery = client.taskRecovery;
  const restTasks = recovery ? new RecoveryTaskState(recovery) : null;
  let activeWork: Promise<void> | null = null;
  let restIdleWake: (() => void) | null = null;
  let replayReadyGeneration: number | null = null;
  let replayEpoch = 0;

  const startTask = (task: TaskRecord): void => {
    if (cancellations.isCancelled(task.taskId)) return;
    const work = processTask(
      client,
      options.executor,
      options.claimRefreshMs,
      cancellations,
      processing,
      task,
    );
    pendingWork.add(work);
    void work
      .catch((error: unknown) => console.error(error))
      .finally(() => pendingWork.delete(work));
  };

  const pumpRestTasks = (): void => {
    if (
      !restTasks ||
      activeWork ||
      replayReadyGeneration === null ||
      client.isStopped() ||
      client.isPaused()
    )
      return;
    const task = restTasks.take(replayReadyGeneration);
    if (!task) {
      restIdleWake?.();
      return;
    }
    const work = processTask(
      client,
      options.executor,
      options.claimRefreshMs,
      cancellations,
      processing,
      task,
    );
    activeWork = work;
    void work
      .catch((error: unknown) => console.error(error))
      .finally(() => {
        restTasks.complete(task.taskId);
        activeWork = null;
        pumpRestTasks();
        restIdleWake?.();
      });
  };

  // One cancellable idle waiter belongs to the current once-mode loop, never to each queued task.
  const waitForRestIdle = () => {
    let resolve = (): void => undefined;
    const promise = new Promise<void>((done) => {
      resolve = done;
    });
    const wake = () => {
      if (!activeWork && !restTasks?.hasPending(client.connectionGeneration())) {
        if (restIdleWake === wake) restIdleWake = null;
        resolve();
      }
    };
    restIdleWake = wake;
    wake();
    return {
      promise,
      cancel: () => {
        if (restIdleWake === wake) restIdleWake = null;
      },
    };
  };

  const subscribe = (): (() => void) =>
    client.onEvent((event) => {
      const cancelledTaskId = cancellations.observe(event);
      if (restTasks && recovery) {
        const task = taskFromReplayStateEvent(event);
        if (task) {
          try {
            restTasks.observe(
              task,
              client.connectionGeneration(),
              taskFromClaimableEvent(event) !== null,
              !cancelledTaskId &&
                isDispatchableClaimableTask(task) &&
                options.shouldClaimTask(task),
            );
          } catch (error) {
            if (error instanceof ParticipantRestReplayError) recovery.fail(error);
            throw error;
          }
        }
        pumpRestTasks();
        return;
      }
      if (cancelledTaskId) {
        replayTasks.delete(cancelledTaskId);
        return;
      }
      const task = taskFromClaimableEvent(event);
      if (!task || !isDispatchableClaimableTask(task) || !options.shouldClaimTask(task)) return;
      const generation = client.connectionGeneration();
      if (replayReadyGeneration !== generation) replayTasks.set(task.taskId, { generation, task });
      else startTask(task);
    });

  /** Discards only queued work owned by one superseded transport generation. */
  function discardReplayTasks(generation: number): void {
    restTasks?.discard(generation);
    for (const [taskId, task] of replayTasks)
      if (task.generation === generation) replayTasks.delete(taskId);
    restIdleWake?.();
  }

  const runLoop = async (): Promise<void> => {
    while (!client.isStopped()) {
      if (client.isPaused()) throw new Error("Participant task loop entered Paused State");
      const currentEpoch = ++replayEpoch;
      const generation = client.connectionGeneration();
      replayReadyGeneration = null;
      const replayReady = client.waitForReplayComplete().then(
        async () => {
          try {
            if (recovery && restTasks) {
              if (
                currentEpoch !== replayEpoch ||
                generation !== client.connectionGeneration() ||
                client.isStopped() ||
                client.isPaused()
              )
                return { kind: "replay-stale", replayEpoch: currentEpoch } as const;
              restTasks.beginSnapshot(generation);
              try {
                const tasks = await recovery.load();
                if (
                  currentEpoch !== replayEpoch ||
                  generation !== client.connectionGeneration() ||
                  client.isStopped() ||
                  client.isPaused()
                )
                  return { kind: "replay-stale", replayEpoch: currentEpoch } as const;
                for (const task of tasks) {
                  if (
                    !cancellations.isCancelled(task.taskId) &&
                    isDispatchableClaimableTask(task) &&
                    options.shouldClaimTask(task)
                  )
                    restTasks.seed(task, generation);
                }
              } catch (error) {
                if (error instanceof ParticipantRestReplayError) recovery.fail(error);
                if (isTransientRestReplayFailure(error))
                  return { kind: "replay-failed", error, replayEpoch: currentEpoch } as const;
                throw error;
              } finally {
                restTasks.endSnapshot(generation);
              }
            }
            await options.replayBarrier?.();
            return { kind: "replay-ready", replayEpoch: currentEpoch } as const;
          } catch (error) {
            return { kind: "barrier-failed", error, replayEpoch: currentEpoch } as const;
          }
        },
        (error: unknown) => ({ kind: "replay-failed", error, replayEpoch: currentEpoch }) as const,
      );
      const closeReady = client
        .waitForClose()
        .then(() => ({ kind: "closed", replayEpoch: currentEpoch }) as const);
      const first = await Promise.race([replayReady, closeReady]);
      if (first.kind === "replay-stale") {
        discardReplayTasks(generation);
        continue;
      }
      if (first.kind === "barrier-failed") throw first.error;
      if (first.kind === "replay-failed") {
        replayEpoch += 1;
        discardReplayTasks(generation);
        if (client.isPaused()) throw first.error;
        await closeReady;
      } else if (first.kind === "replay-ready") {
        if (
          first.replayEpoch !== replayEpoch ||
          generation !== client.connectionGeneration() ||
          client.isStopped() ||
          client.isPaused()
        ) {
          discardReplayTasks(generation);
          continue;
        }
        replayReadyGeneration = generation;
        if (restTasks) pumpRestTasks();
        else {
          for (const task of replayTasks.values())
            if (task.generation === generation) startTask(task.task);
          discardReplayTasks(generation);
        }
        if (options.once) {
          if (restTasks) {
            const idle = waitForRestIdle();
            try {
              const settled = await Promise.race([
                idle.promise.then(() => ({ kind: "idle" }) as const),
                closeReady,
              ]);
              if (settled.kind === "idle") {
                client.close();
                return;
              }
            } finally {
              idle.cancel();
            }
          } else {
            await Promise.allSettled([...pendingWork]);
            client.close();
            return;
          }
        }
        await closeReady;
        replayEpoch += 1;
        replayReadyGeneration = null;
        discardReplayTasks(generation);
      } else {
        replayEpoch += 1;
        replayReadyGeneration = null;
        discardReplayTasks(generation);
      }
      if (!client.isStopped()) {
        if (client.isPaused()) throw new Error("Participant task loop entered Paused State");
        await client.reconnectAfterClose();
      }
    }
  };

  return Effect.acquireUseRelease(
    Effect.sync(subscribe),
    () => Effect.tryPromise({ catch: (error) => error, try: runLoop }),
    (unsubscribe) => Effect.sync(unsubscribe),
  );
}
