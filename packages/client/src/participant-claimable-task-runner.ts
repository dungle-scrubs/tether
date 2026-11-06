import { Effect } from "effect";

import {
  isTransientRestReplayFailure,
  ParticipantRestReplayError,
  taskFromReplayStateEvent,
} from "./participant-rest-replay.js";
import { RecoveryTaskState } from "./participant-recovery-task-state.js";
import { taskFromClaimableEvent } from "./protocol.js";
import { TaskCancellationRegistry } from "./task-cancellation-registry.js";
import type {
  ParticipantRuntimeTaskLoopOptions,
  ParticipantTaskExecutor,
} from "./participant-runtime-client.js";
import type { SessionEvent, TaskRecord } from "./types.js";

/** Shared terminal task-loop message in both transport modes. */
const pausedTaskLoopMessage = "Participant task loop entered Paused State";

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
    /** Loads the canonical active-task snapshot under this generation's read policy. */
    readonly load: () => Promise<readonly TaskRecord[]>;
    /** Maximum retained task bytes, independently equal to the delivery byte budget. */
    readonly maxQueueBytes: number;
    /** Maximum retained task identities, independently equal to the delivery item budget. */
    readonly maxQueueSize: number;
    /** Reports admission failures through the runtime's terminal delivery policy. */
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

  /** Starts the next eligible REST claim only after the active flow settles. */
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

  /** Owns one cancellable idle waiter for the current once-mode loop. */
  const waitForRestIdle = () => {
    /** Captures the idle promise's completion callback without retaining task-specific waiters. */
    let resolve = (): void => undefined;
    const promise = new Promise<void>((done) => {
      resolve = done;
    });
    /** Resolves only when neither an active claim nor queued work remains. */
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
      if (client.isPaused()) throw new Error(pausedTaskLoopMessage);
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
                if (client.isPaused()) throw new Error(pausedTaskLoopMessage);
                client.close();
                return;
              }
            } finally {
              idle.cancel();
            }
          } else {
            await Promise.allSettled([...pendingWork]);
            if (client.isPaused()) throw new Error(pausedTaskLoopMessage);
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
        if (client.isPaused()) throw new Error(pausedTaskLoopMessage);
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
