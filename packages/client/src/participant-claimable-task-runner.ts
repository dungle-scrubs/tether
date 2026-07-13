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
  /** Returns whether the participant client is intentionally stopped. */
  readonly isStopped: () => boolean;
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
  const replayTasks = new Map<string, TaskRecord>();
  let replayComplete = false;

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
      if (!replayComplete) {
        replayTasks.set(task.taskId, task);
        return;
      }
      startTask(task);
    });

  const loop = Effect.gen(function* () {
    while (!client.isStopped()) {
      replayComplete = false;
      const replayReady = Effect.tryPromise(() => client.waitForReplayComplete()).pipe(
        Effect.map(() => {
          replayComplete = true;
          for (const task of replayTasks.values()) {
            startTask(task);
          }
          replayTasks.clear();
          return "replay_complete" as const;
        }),
      );
      const closeReady = Effect.tryPromise(() => client.waitForClose()).pipe(
        Effect.as("closed" as const),
      );
      const firstResult = yield* Effect.race(replayReady, closeReady);

      if (firstResult === "replay_complete") {
        if (options.once) {
          yield* Effect.promise(() => Promise.allSettled([...pendingWork]));
          client.close();
          return;
        }
        yield* closeReady;
      }
      if (!client.isStopped()) {
        yield* Effect.tryPromise(() => client.reconnectAfterClose());
      }
    }
  });

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
