import { Effect } from "effect";
import { sleepUnrefEffect } from "./effect-timing.js";
import type { TaskCancellationContext } from "./participant-claimable-task-runner.js";
import {
  ParticipantRuntimeCommandError,
  type ParticipantTaskExecutor,
  type ParticipantTaskExecutorContext,
} from "./participant-runtime-client.js";
import { ParticipantTaskExecutionError } from "./participant-task-execution-error.js";
import {
  type AppendSessionEventInput,
  buildTaskOutputEventInput,
  buildTaskProgressEventInput,
} from "./protocol.js";
import type { SessionEvent, TaskRecord } from "./types.js";

/** Boundary log surface needed by the participant task claim flow. */
export interface ParticipantTaskClaimFlowLogger {
  /** Writes a debug event for task claim flow diagnostics. */
  readonly debug: (boundary: string, message: string, details?: Record<string, unknown>) => void;
}

/** Command surface needed to claim, refresh, publish, and resolve one task. */
export interface ParticipantTaskClaimFlowClient {
  /** Publishes one participant-originated event. */
  readonly appendEvent: (input: AppendSessionEventInput) => Promise<void>;
  /** Attempts to claim a task. */
  readonly claimTask: (taskId: string) => Promise<TaskRecord | null>;
  /** Completes a claimed task, fenced by the current Claim ID. */
  readonly completeTask: (
    taskId: string,
    result: Record<string, unknown>,
    claimId: string,
  ) => Promise<void>;
  /** Fails a claimed task, fenced by the current Claim ID. */
  readonly failTask: (
    taskId: string,
    failure: Record<string, unknown>,
    claimId: string,
  ) => Promise<void>;
  /** Refreshes a claimed task lease, fenced by the current Claim ID. */
  readonly refreshTaskClaim: (taskId: string, claimId: string) => Promise<TaskRecord | null>;
  /** Releases a claimed task back to the claimable pool, fenced by the current Claim ID. */
  readonly releaseTask: (taskId: string, claimId: string) => Promise<void>;
}

/**
 * Error raised when the server rejects the task claim command itself, for
 * example with a participant-required or authorization failure. Unlike a
 * transport blip, a command rejection means this runner cannot claim work, so
 * the flow propagates it to the runtime error boundary instead of leaving the
 * runner looking healthy behind a debug-only diagnostic.
 */
export class ParticipantTaskClaimRejectedError extends Error {
  /** Stable participant identity whose claim command was rejected. */
  readonly participantId: string;
  /** Durable session that owns the task. */
  readonly sessionId: string;
  /** Task whose claim command was rejected. */
  readonly taskId: string;

  constructor(input: {
    readonly cause: Error;
    readonly participantId: string;
    readonly sessionId: string;
    readonly taskId: string;
  }) {
    super(`Task claim command rejected: ${input.cause.message}`, { cause: input.cause });
    this.name = "ParticipantTaskClaimRejectedError";
    this.participantId = input.participantId;
    this.sessionId = input.sessionId;
    this.taskId = input.taskId;
  }
}

/** Runtime identity and event context for one task claim flow. */
export interface ParticipantTaskClaimFlowContext {
  /** Control epoch acquired for this participant WebSocket connection. */
  readonly controlEpoch?: number;
  /** Concrete runtime process id that owns the task claim. */
  readonly instanceId: string;
  /** Latest observed event sequence. */
  readonly lastObservedSeq: number;
  /** Stable participant identity that owns the task claim. */
  readonly participantId: string;
  /** Recent session events available to the executor. */
  readonly recentEvents: readonly SessionEvent[];
  /** Durable session that owns the task. */
  readonly sessionId: string;
}

/** Inputs for running one participant task claim flow. */
export interface ParticipantTaskClaimFlowInput {
  /** Cancellation state for the active task. */
  readonly cancellation: TaskCancellationContext;
  /** Interval used to refresh the active claim. */
  readonly claimRefreshMs: number;
  /** Adapter-specific executor for the claimed task. */
  readonly executor: ParticipantTaskExecutor;
  /** Claimable task event payload. */
  readonly task: TaskRecord;
}

type ClaimSetupOutcome =
  | { readonly error: ParticipantTaskClaimRejectedError; readonly type: "claim_rejected" }
  | { readonly type: "claimed"; readonly task: TaskRecord }
  | { readonly type: "cancelled" | "claim_not_won" | "claim_transport_unknown" | "setup_failed" };

type ExecutorOutcome =
  | { readonly error: Error; readonly type: "executor_failed" }
  | {
      readonly execution: Awaited<ReturnType<ParticipantTaskExecutor>>;
      readonly type: "succeeded";
    };

const maxConsecutiveRefreshFailures = 3;

/**
 * Fraction of the remaining claim lease that may elapse before the next
 * refresh attempt. Refreshing at half the lease keeps a full missed attempt of
 * headroom before the sweeper can expire the claim and re-dispatch the task.
 */
const claimRefreshLeaseFraction = 0.5;

/** Floor for lease-derived refresh delays so a nearly elapsed lease cannot busy-loop. */
const minClaimRefreshDelayMs = 50;

/** RFC 3339 timestamp with a mandatory `Z` or numeric UTC offset. */
const rfc3339WithOffsetPattern =
  /^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})$/;

/**
 * Terminal reason surfaced when a freshly claimed task carries a missing or
 * malformed claim deadline. The flow declines the claim before starting refresh
 * or the executor so a claim it cannot safely fence never runs.
 */
export const invalidClaimDeadlineReason = "task.invalid_claim_deadline";

/** Returns whether a claim deadline is a non-null RFC 3339 timestamp with an offset. */
function isValidClaimDeadline(value: string | null): value is string {
  return value !== null && rfc3339WithOffsetPattern.test(value) && !Number.isNaN(Date.parse(value));
}

/** Runs claim, refresh, execution, output, and completion for one task. */
export async function runParticipantTaskClaimFlow(
  client: ParticipantTaskClaimFlowClient,
  context: ParticipantTaskClaimFlowContext,
  logger: ParticipantTaskClaimFlowLogger,
  input: ParticipantTaskClaimFlowInput,
): Promise<void> {
  await Effect.runPromise(buildParticipantTaskClaimFlow(client, context, logger, input));
}

/** Builds the task claim flow as an Effect program for scoped refresh cleanup. */
export function buildParticipantTaskClaimFlow(
  client: ParticipantTaskClaimFlowClient,
  context: ParticipantTaskClaimFlowContext,
  logger: ParticipantTaskClaimFlowLogger,
  input: ParticipantTaskClaimFlowInput,
): Effect.Effect<void, unknown> {
  const shouldStop = (): boolean => isTaskStopped(input.cancellation);
  const debugTaskStop = (message: string): void => {
    logger.debug("runTaskClaimFlow", message, {
      participantId: context.participantId,
      sessionId: context.sessionId,
      taskId: input.task.taskId,
    });
  };
  return Effect.scoped(
    Effect.gen(function* () {
      const claimSetup = yield* claimAndPublishInitialProgress(client, context, logger, input);
      if (claimSetup.type !== "claimed") {
        if (claimSetup.type === "claim_rejected") {
          return yield* Effect.fail(claimSetup.error);
        }
        if (claimSetup.type === "cancelled") {
          debugTaskStop("task.cancelled_before_executor");
        }
        return;
      }

      // Every fenced mutation echoes the Claim ID minted on this claim. A server
      // that predates Claim IDs returns null here and cannot be fenced, so the
      // flow declines the claim rather than issue unfenced mutations.
      const claimId = claimSetup.task.claimId;
      if (claimId === null) {
        debugTaskStop("task.claim_missing_claim_id");
        return;
      }

      // The claim deadline governs refresh cadence and executor liveness. A
      // missing or malformed deadline is a terminal server contract violation, so
      // decline before refresh or the executor rather than run unbounded work.
      const claimExpiresAt = claimSetup.task.claimExpiresAt;
      if (!isValidClaimDeadline(claimExpiresAt)) {
        debugTaskStop(invalidClaimDeadlineReason);
        return;
      }

      yield* buildTaskClaimRefresh(client, logger, {
        cancellation: input.cancellation,
        claimExpiresAt,
        claimId,
        intervalMs: input.claimRefreshMs,
        taskId: input.task.taskId,
      }).pipe(Effect.forkScoped);
      if (shouldStop()) {
        debugTaskStop("task.cancelled_after_claim");
        return;
      }
      const executorContext: ParticipantTaskExecutorContext = {
        ...(context.controlEpoch === undefined ? {} : { controlEpoch: context.controlEpoch }),
        instanceId: context.instanceId,
        participantId: context.participantId,
        recentEvents: context.recentEvents.filter((event) => event.seq < context.lastObservedSeq),
        publishOutput: async (output) => {
          if (shouldStop()) {
            return;
          }
          await client.appendEvent(
            buildTaskOutputEventInput({
              output,
              participantId: context.participantId,
              sessionId: context.sessionId,
              taskId: input.task.taskId,
            }),
          );
        },
        publishProgress: async () => {
          if (shouldStop()) {
            return;
          }
          await client.appendEvent(
            buildTaskProgressEventInput({
              participantId: context.participantId,
              sessionId: context.sessionId,
              taskId: input.task.taskId,
            }),
          );
        },
        sessionId: context.sessionId,
        signal: input.cancellation.signal,
        task: claimSetup.task,
      };
      const execution = yield* runExecutor(input.executor, executorContext);
      if (execution.type === "executor_failed") {
        if (shouldStop()) {
          return;
        }
        // A retryable executor failure is transient, so release the claim back
        // to the claimable pool for another attempt instead of failing the
        // task terminally, which no worker could ever pick up again.
        if (execution.error instanceof ParticipantTaskExecutionError && execution.error.retryable) {
          logger.debug("runTaskClaimFlow", "task.released_for_retry", {
            error: execution.error.message,
            participantId: context.participantId,
            sessionId: context.sessionId,
            taskId: input.task.taskId,
          });
          yield* Effect.tryPromise(() => client.releaseTask(input.task.taskId, claimId));
          return;
        }
        yield* Effect.tryPromise(() =>
          client.failTask(
            input.task.taskId,
            execution.error instanceof ParticipantTaskExecutionError
              ? { ...execution.error.failure }
              : { error: execution.error.message },
            claimId,
          ),
        );
        return;
      }
      if (shouldStop()) {
        debugTaskStop("task.cancelled_after_executor");
        return;
      }
      if (execution.execution.output) {
        const outputPublished = yield* Effect.tryPromise(() =>
          executorContext.publishOutput(execution.execution.output ?? ""),
        ).pipe(
          Effect.match({
            onFailure: (error) => {
              logCompletionTransportFailed(logger, context, input.task.taskId, error);
              return false;
            },
            onSuccess: () => true,
          }),
        );
        if (!outputPublished) {
          return;
        }
        if (shouldStop()) {
          debugTaskStop("task.cancelled_after_output");
          return;
        }
      }
      yield* Effect.tryPromise(() =>
        client.completeTask(input.task.taskId, execution.execution.result, claimId),
      ).pipe(
        Effect.catchAll((error) =>
          Effect.sync(() => {
            logCompletionTransportFailed(logger, context, input.task.taskId, error);
          }),
        ),
      );
    }),
  );
}

function claimAndPublishInitialProgress(
  client: ParticipantTaskClaimFlowClient,
  context: ParticipantTaskClaimFlowContext,
  logger: ParticipantTaskClaimFlowLogger,
  input: ParticipantTaskClaimFlowInput,
): Effect.Effect<ClaimSetupOutcome, never> {
  if (isTaskStopped(input.cancellation)) {
    return Effect.succeed({ type: "cancelled" });
  }
  return Effect.gen(function* () {
    const claimOutcome = yield* Effect.tryPromise({
      catch: (error) => error,
      try: () => client.claimTask(input.task.taskId),
    }).pipe(
      Effect.match({
        onFailure: (error): ClaimSetupOutcome => {
          // A correlated command rejection means the server refused this
          // runner's claim, not that the transport hiccuped. Escalate it so a
          // runner that cannot claim is visible instead of debug-only quiet.
          if (error instanceof ParticipantRuntimeCommandError) {
            return {
              error: new ParticipantTaskClaimRejectedError({
                cause: error,
                participantId: context.participantId,
                sessionId: context.sessionId,
                taskId: input.task.taskId,
              }),
              type: "claim_rejected",
            };
          }
          logger.debug("runTaskClaimFlow", "task.claim_transport_unknown", {
            error: error instanceof Error ? error.message : "Unknown claim error",
            participantId: context.participantId,
            recovery: "lease_expiry_fallback",
            sessionId: context.sessionId,
            taskId: input.task.taskId,
          });
          return { type: "claim_transport_unknown" };
        },
        onSuccess: (task): ClaimSetupOutcome =>
          task ? { task, type: "claimed" } : { type: "claim_not_won" },
      }),
    );
    if (claimOutcome.type === "claim_rejected" || claimOutcome.type === "claim_transport_unknown") {
      return claimOutcome;
    }
    if (claimOutcome.type === "claim_not_won") {
      logger.debug("runTaskClaimFlow", "task.claim_not_won", {
        participantId: context.participantId,
        sessionId: context.sessionId,
        taskId: input.task.taskId,
      });
      return claimOutcome;
    }
    if (isTaskStopped(input.cancellation)) {
      return { type: "cancelled" } as const;
    }
    const progressPublished = yield* Effect.tryPromise(() =>
      client.appendEvent(
        buildTaskProgressEventInput({
          participantId: context.participantId,
          sessionId: context.sessionId,
          taskId: input.task.taskId,
        }),
      ),
    ).pipe(
      Effect.match({
        onFailure: (error) => {
          logger.debug("runTaskClaimFlow", "task.setup_failed_before_executor", {
            error: error instanceof Error ? error.message : "Unknown setup error",
            participantId: context.participantId,
            recovery: "lease_expiry_fallback",
            sessionId: context.sessionId,
            taskId: input.task.taskId,
          });
          return false;
        },
        onSuccess: () => true,
      }),
    );
    return progressPublished ? claimOutcome : { type: "setup_failed" };
  });
}

function runExecutor(
  executor: ParticipantTaskExecutor,
  context: ParticipantTaskExecutorContext,
): Effect.Effect<ExecutorOutcome, never> {
  return Effect.tryPromise({
    catch: normalizeRuntimeError,
    try: () => executor(context),
  }).pipe(
    Effect.match({
      onFailure: (error) => ({ error, type: "executor_failed" }),
      onSuccess: (execution) => ({ execution, type: "succeeded" }),
    }),
  );
}

function logCompletionTransportFailed(
  logger: ParticipantTaskClaimFlowLogger,
  context: ParticipantTaskClaimFlowContext,
  taskId: string,
  error: unknown,
): void {
  logger.debug("runTaskClaimFlow", "task.completion_transport_failed", {
    error: error instanceof Error ? error.message : "Unknown completion transport error",
    participantId: context.participantId,
    recovery: "lease_expiry_fallback",
    sessionId: context.sessionId,
    taskId,
  });
}

/** Periodically refreshes a claimed task lease inside the active task scope. */
function buildTaskClaimRefresh(
  client: ParticipantTaskClaimFlowClient,
  logger: ParticipantTaskClaimFlowLogger,
  input: {
    readonly cancellation: TaskCancellationContext;
    readonly claimExpiresAt: string | null;
    readonly claimId: string;
    readonly intervalMs: number;
    readonly taskId: string;
  },
): Effect.Effect<void, never> {
  if (input.intervalMs <= 0) {
    return Effect.void;
  }
  return Effect.gen(function* () {
    let consecutiveFailures = 0;
    let currentClaimExpiresAt = input.claimExpiresAt;
    let clampReported = false;
    while (!isTaskStopped(input.cancellation)) {
      // The configured interval is clamped against the server lease deadline
      // so a claimRefreshMs at or above the lease TTL cannot silently open a
      // duplicate-execution window between refresh ticks.
      const delayMs = nextClaimRefreshDelayMs(input.intervalMs, currentClaimExpiresAt);
      if (delayMs < input.intervalMs && !clampReported) {
        clampReported = true;
        logger.debug("runTaskClaimFlow", "task.claim_refresh_interval_clamped", {
          claimExpiresAt: currentClaimExpiresAt,
          clampedDelayMs: delayMs,
          configuredIntervalMs: input.intervalMs,
          taskId: input.taskId,
        });
      }
      yield* sleepUnrefEffect(delayMs);
      if (isTaskStopped(input.cancellation)) {
        return;
      }
      const refreshOutcome = yield* Effect.tryPromise(() =>
        client.refreshTaskClaim(input.taskId, input.claimId),
      ).pipe(
        Effect.catchAll((error) =>
          Effect.sync(() => {
            consecutiveFailures += 1;
            logger.debug("runTaskClaimFlow", "task.claim_refresh_failed", {
              consecutiveFailures,
              error: error instanceof Error ? error.message : "Unknown refresh error",
              taskId: input.taskId,
            });
            return "failed" as const;
          }),
        ),
      );
      if (refreshOutcome === "failed") {
        if (
          consecutiveFailures >= maxConsecutiveRefreshFailures ||
          isClaimLeaseExpired(currentClaimExpiresAt)
        ) {
          logger.debug("runTaskClaimFlow", "task.claim_refresh_failure_budget_exhausted", {
            consecutiveFailures,
            maxConsecutiveRefreshFailures,
            taskId: input.taskId,
          });
          input.cancellation.abortActive();
          return;
        }
        continue;
      }
      if (refreshOutcome === null) {
        logger.debug("runTaskClaimFlow", "task.claim_refresh_rejected", {
          taskId: input.taskId,
        });
        input.cancellation.abortActive();
        return;
      }
      consecutiveFailures = 0;
      currentClaimExpiresAt = refreshOutcome.claimExpiresAt;
      if (isClaimLeaseExpired(refreshOutcome.claimExpiresAt)) {
        logger.debug("runTaskClaimFlow", "task.claim_refresh_lease_expired", {
          taskId: input.taskId,
        });
        input.cancellation.abortActive();
        return;
      }
    }
  });
}

/**
 * Derives the next claim refresh delay from the configured interval and the
 * current server lease deadline. The delay never exceeds a safe fraction of
 * the remaining lease, so a configured interval at or above the lease TTL is
 * clamped instead of guaranteeing that every lease elapses between ticks.
 */
export function nextClaimRefreshDelayMs(
  intervalMs: number,
  claimExpiresAt: string | null,
  now: number = Date.now(),
): number {
  if (claimExpiresAt === null) {
    return intervalMs;
  }
  const claimExpiresAtMs = Date.parse(claimExpiresAt);
  if (!Number.isFinite(claimExpiresAtMs)) {
    return intervalMs;
  }
  const safeDelayMs = Math.floor((claimExpiresAtMs - now) * claimRefreshLeaseFraction);
  return Math.min(intervalMs, Math.max(minClaimRefreshDelayMs, safeDelayMs));
}

/** Preserves executor error messages when Effect wraps rejected promises. */
function normalizeRuntimeError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  if (isRecord(error) && typeof error.message === "string" && error.message.trim()) {
    return new Error(error.message);
  }
  return new Error(String(error));
}

/** Narrows unknown values to plain records for error normalization. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

/** Checks whether cancellation has made the current task flow stop publishing visible outputs. */
function isTaskStopped(cancellation: TaskCancellationContext): boolean {
  return cancellation.signal.aborted || cancellation.isCancelled();
}

function isClaimLeaseExpired(claimExpiresAt: string | null): boolean {
  if (!claimExpiresAt) {
    return false;
  }
  const claimExpiresAtMs = Date.parse(claimExpiresAt);
  return Number.isFinite(claimExpiresAtMs) && claimExpiresAtMs <= Date.now();
}
