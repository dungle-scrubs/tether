import { Effect } from "effect";

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

/** Builds the replay/live claimable-task loop for one participant runtime. */
export function buildParticipantClaimableTaskLoop(
  client: ParticipantClaimableTaskRunnerClient,
  options: ParticipantRuntimeTaskLoopOptions,
): Effect.Effect<void, unknown> {
  const cancellations = new TaskCancellationRegistry();
  const processing = new Set<string>();
  const pendingWork = new Set<Promise<void>>();
  const replayTasks = new Map<string, { readonly generation: number; readonly task: TaskRecord }>();
  let replayReadyGeneration: number | null = null;
  let replayEpoch = 0;

  const startTask = (task: TaskRecord): void => {
    if (cancellations.isCancelled(task.taskId)) {
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
    pendingWork.add(work);
    void work
      .catch((error: unknown) => {
        console.error(error);
      })
      .finally(() => pendingWork.delete(work));
  };

  const subscribe = (): (() => void) =>
    client.onEvent((event) => {
      const cancelledTaskId = cancellations.observe(event);
      if (cancelledTaskId) {
        replayTasks.delete(cancelledTaskId);
        return;
      }
      const task = taskFromClaimableEvent(event);
      if (!task || !isDispatchableClaimableTask(task) || !options.shouldClaimTask(task)) {
        return;
      }
      const eventGeneration = client.connectionGeneration();
      if (replayReadyGeneration !== eventGeneration) {
        replayTasks.set(task.taskId, {
          generation: eventGeneration,
          task,
        });
        return;
      }
      startTask(task);
    });

  const runLoop = async (): Promise<void> => {
    while (!client.isStopped()) {
      const currentEpoch = replayEpoch + 1;
      const currentGeneration = client.connectionGeneration();
      replayEpoch = currentEpoch;
      replayReadyGeneration = null;
      const replayReady = client.waitForReplayComplete().then(
        async () => {
          try {
            await options.replayBarrier?.();
          } catch (error) {
            return { error, kind: "barrier-failed", replayEpoch: currentEpoch } as const;
          }
          return { kind: "replay-ready", replayEpoch: currentEpoch } as const;
        },
        (error: unknown) => ({ error, kind: "replay-failed", replayEpoch: currentEpoch }) as const,
      );
      const closeReady = client
        .waitForClose()
        .then(() => ({ kind: "closed", replayEpoch: currentEpoch }) as const);
      const firstResult = await Promise.race([replayReady, closeReady]);

      if (firstResult.kind === "barrier-failed") {
        throw firstResult.error;
      }
      if (firstResult.kind === "replay-failed") {
        replayEpoch += 1;
        discardReplayTasks(currentGeneration);
        if (client.isPaused()) {
          throw firstResult.error;
        }
        await closeReady;
      } else if (firstResult.kind === "replay-ready") {
        if (firstResult.replayEpoch !== replayEpoch) {
          continue;
        }
        replayReadyGeneration = currentGeneration;
        for (const replayTask of replayTasks.values()) {
          if (replayTask.generation === currentGeneration) {
            startTask(replayTask.task);
          }
        }
        discardReplayTasks(currentGeneration);
        if (options.once) {
          await Promise.allSettled([...pendingWork]);
          client.close();
          return;
        }
        await closeReady;
        replayEpoch += 1;
        replayReadyGeneration = null;
        discardReplayTasks(currentGeneration);
      } else {
        replayEpoch += 1;
        replayReadyGeneration = null;
        discardReplayTasks(currentGeneration);
      }
      if (!client.isStopped()) {
        await client.reconnectAfterClose();
      }
    }
  };

  const loop = Effect.tryPromise({
    catch: (error) => error,
    try: runLoop,
  });

  /** Discards only replay work owned by one superseded transport generation. */
  function discardReplayTasks(generation: number): void {
    for (const [taskId, replayTask] of replayTasks) {
      if (replayTask.generation === generation) {
        replayTasks.delete(taskId);
      }
    }
  }

  return Effect.acquireUseRelease(
    Effect.sync(subscribe),
    () => loop,
    (unsubscribe) => Effect.sync(unsubscribe),
  );
}

function isDispatchableClaimableTask(task: TaskRecord): boolean {
  return !task.completedAt && !task.failedAt && !task.cancelledAt;
}

async function processTask(
  client: Pick<ParticipantClaimableTaskRunnerClient, "runTaskClaimFlow">,
  executor: ParticipantTaskExecutor,
  claimRefreshMs: number,
  cancellations: TaskCancellationRegistry,
  processing: Set<string>,
  task: TaskRecord,
): Promise<void> {
  if (processing.has(task.taskId) || cancellations.isCancelled(task.taskId)) {
    return;
  }
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
