import { Cause, Effect, Fiber, Option, Runtime } from "effect";

import { sleepUnrefEffect } from "./effect-runtime.js";
import type { SessionEvent } from "./types.js";

export const defaultTaskClaimSweepBatchSize = 50;
export const defaultTaskClaimSweepMs = 1_000;

/**
 * Runtime diagnostics for the task claim sweeper.
 */
export interface TaskClaimSweeperDebugInfo {
  readonly batchSize: number;
  readonly enabled: boolean;
  readonly intervalMs: number;
  readonly running: boolean;
  readonly scheduled: boolean;
}

/**
 * Optional scheduler configuration for expiring elapsed task claim leases.
 */
export interface TaskClaimSweeperConfig {
  readonly batchSize?: number;
  readonly intervalMs?: number;
}

interface TaskClaimSweeperOptions extends TaskClaimSweeperConfig {
  readonly onEvents: (events: readonly SessionEvent[]) => void;
  readonly service: TaskClaimSweeperSessionService;
}

/**
 * Minimal durable service surface required by the task claim sweeper.
 */
export interface TaskClaimSweeperSessionService {
  /** Expires elapsed task claims and returns committed session events. */
  readonly expireTaskClaims: (input: {
    readonly batchSize: number;
  }) => Effect.Effect<
    { readonly events: readonly SessionEvent[]; readonly expiredCount: number },
    unknown
  >;
}

/**
 * Periodically asks the session service to expire elapsed task claim leases.
 * Durable state changes and claim-expired events are committed by the service;
 * the sweeper only schedules the work and broadcasts committed events.
 */
export class TaskClaimSweeper {
  private readonly batchSize: number;
  private readonly intervalMs: number;
  private fiber: Fiber.RuntimeFiber<void, never> | null = null;
  private running: Promise<void> | null = null;
  private stopped = true;
  private scheduled = false;

  /**
   * Stores scheduler dependencies and resolves optional configuration defaults.
   */
  constructor(private readonly options: TaskClaimSweeperOptions) {
    this.batchSize = options.batchSize ?? defaultTaskClaimSweepBatchSize;
    this.intervalMs = options.intervalMs ?? defaultTaskClaimSweepMs;
  }

  /**
   * Returns inspectable scheduler state for tests and operators.
   */
  debugInfo(): TaskClaimSweeperDebugInfo {
    return {
      batchSize: this.batchSize,
      enabled: this.intervalMs > 0,
      intervalMs: this.intervalMs,
      running: this.running !== null,
      scheduled: this.scheduled,
    };
  }

  /**
   * Starts the periodic scheduler. An interval less than or equal to zero keeps
   * the sweeper disabled.
   */
  start(): void {
    if (this.intervalMs <= 0 || !this.stopped) {
      return;
    }
    this.stopped = false;
    this.fiber = Effect.runFork(this.loop());
  }

  /**
   * Stops future ticks and waits for an in-flight tick to finish.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    await this.running;
    if (this.fiber) {
      const fiber = this.fiber;
      this.fiber = null;
      await Effect.runPromise(Fiber.interrupt(fiber));
    }
  }

  /**
   * Runs ticks until the sweeper is stopped or interrupted.
   */
  private loop(): Effect.Effect<void, never> {
    return Effect.gen(this, function* () {
      let delayMs = 0;
      while (!this.stopped) {
        this.scheduled = true;
        yield* sleepUnrefEffect(delayMs);
        this.scheduled = false;
        if (this.stopped) {
          return;
        }
        this.running = this.tick();
        yield* Effect.promise(() => this.running ?? Promise.resolve());
        this.running = null;
        delayMs = this.intervalMs;
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          this.scheduled = false;
          this.running = null;
        }),
      ),
    );
  }

  /**
   * Runs one expiration pass and broadcasts only the events that were durably
   * committed by the service.
   */
  private async tick(): Promise<void> {
    try {
      const result = await Effect.runPromise(
        this.options.service.expireTaskClaims({ batchSize: this.batchSize }),
      );
      if (result.events.length > 0) {
        this.options.onEvents(result.events);
      }
    } catch (error) {
      console.error(unwrapEffectFailure(error));
    }
  }
}

/** Restores the service failure value that Effect.runPromise wraps for rejects. */
function unwrapEffectFailure(error: unknown): unknown {
  if (!Runtime.isFiberFailure(error)) {
    return error;
  }
  return Option.getOrUndefined(Cause.failureOption(error[Runtime.FiberFailureCauseId])) ?? error;
}
