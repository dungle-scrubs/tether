import { randomUUID } from "node:crypto";

import { Cause, Effect, Option, Runtime } from "effect";

import { type ControlEpochStaleError, isControlEpochStaleError } from "./control-epoch.js";
import type { ModuleObservability } from "./observability.js";
import {
  type ControlEpochStaleResult,
  type SessionServiceFailure,
  SessionServicePersistenceError,
} from "./session-service-contracts.js";
import type { SessionEvent } from "./types.js";

/**
 * Returns the fenced Control Epoch failure carried by an effect error, whether
 * it was raised directly or wrapped as a persistence failure cause. The atomic
 * epoch guard runs inside a protected mutation transaction, so a fence surfaces
 * here as a rolled-back persistence error rather than a clean control outcome.
 */
export function extractControlEpochStale(error: unknown): ControlEpochStaleError | null {
  if (isControlEpochStaleError(error)) {
    return error;
  }
  if (error instanceof SessionServicePersistenceError && isControlEpochStaleError(error.cause)) {
    return error.cause;
  }
  return null;
}

/**
 * Maps an atomic Control Epoch fence failure into the typed stale control
 * result. Non-fence failures pass through unchanged so genuine persistence
 * errors are never masked as a stale epoch.
 */
export function catchAtomicEpochStale<TValue>(
  effect: Effect.Effect<TValue, SessionServiceFailure>,
): Effect.Effect<TValue | ControlEpochStaleResult, SessionServiceFailure> {
  return effect.pipe(
    Effect.catchAll((error) => {
      const stale = extractControlEpochStale(error);
      if (stale) {
        const result: ControlEpochStaleResult = {
          currentEpoch: stale.currentEpoch,
          status: "control_epoch_stale",
        };
        return Effect.succeed(result);
      }
      return Effect.fail(error);
    }),
  );
}

/**
 * Builds the traced Effect boundary used by session service operations.
 */
export function createSessionTraceEffect(observability: ModuleObservability) {
  return <TValue, TError>(
    operation: string,
    input: Record<string, unknown>,
    action: Effect.Effect<TValue, TError>,
    summarize?: (value: TValue) => Record<string, unknown>,
  ): Effect.Effect<TValue, TError> =>
    Effect.tryPromise({
      catch: unwrapEffectFailure<TError>,
      try: () =>
        observability.traceBoundary(operation, input, () => Effect.runPromise(action), summarize),
    });
}

/**
 * Converts rejected persistence promises into the typed session service failure
 * channel.
 */
export function trySessionPromise<TValue>(
  run: () => Promise<TValue>,
  operation = "persistence",
): Effect.Effect<TValue, SessionServicePersistenceError> {
  return Effect.tryPromise({
    catch: (cause) => new SessionServicePersistenceError(operation, cause),
    try: run,
  });
}

/**
 * Verifies that events returned from a service operation are safe to broadcast
 * to the requested session.
 */
export function assertBroadcastEventsWithObservability(
  observability: ModuleObservability,
  operation: string,
  sessionId: string,
  events: readonly SessionEvent[],
  expectedCount?: number,
): void {
  observability.assertInvariant(
    expectedCount === undefined || events.length === expectedCount,
    operation,
    "Unexpected number of broadcast events",
    { eventCount: events.length, expectedCount, sessionId },
  );
  for (const event of events) {
    observability.assertInvariant(
      event.sessionId === sessionId,
      operation,
      "Broadcast event session mismatch",
      {
        eventId: event.eventId,
        eventSessionId: event.sessionId,
        expectedSessionId: sessionId,
        type: event.type,
      },
    );
  }
}

/**
 * Creates a process-local source id used to suppress self-originated fanout
 * notifications.
 */
export function newEventSourceId(): string {
  return `src_${randomUUID()}`;
}

/**
 * Restores the original typed Effect failure from the FiberFailure thrown by
 * Effect.runPromise inside the Promise-based trace boundary.
 */
function unwrapEffectFailure<TError>(cause: unknown): TError {
  if (!Runtime.isFiberFailure(cause)) {
    return cause as TError;
  }
  return (Option.getOrUndefined(Cause.failureOption(cause[Runtime.FiberFailureCauseId])) ??
    cause) as TError;
}
