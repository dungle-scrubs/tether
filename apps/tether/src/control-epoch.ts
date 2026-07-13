/**
 * Centralized Control Epoch predicates and the typed stale-epoch failure.
 *
 * A Control Epoch is an immutable, strictly-monotonic, server-issued fencing
 * generation for a participant control owner. Same-instance re-acquisition
 * advances the epoch (fencing the prior owner); renewal and release compare it.
 * These helpers own every epoch validity and staleness decision so the
 * persistence, service, WebSocket, and REST boundaries share one definition.
 */

/** Control channel that a participant control lease governs. */
export type ControlChannel = "rest" | "ws";

/**
 * Returns whether a value is a Control Epoch: a positive safe integer. Epoch 0
 * and negative, fractional, or non-finite values are never valid generations.
 */
export function isPositiveSafeIntegerEpoch(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

/**
 * Parses a caller-supplied Control Epoch, accepting only positive safe integers
 * and the numeric strings/bigints Postgres may return for a bigint column.
 */
export function parseControlEpoch(value: unknown): number | null {
  if (isPositiveSafeIntegerEpoch(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return value > 0n && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : null;
  }
  if (typeof value === "string" && /^[0-9]+$/u.test(value)) {
    const parsed = Number(value);
    return isPositiveSafeIntegerEpoch(parsed) ? parsed : null;
  }
  return null;
}

/**
 * Returns the next strictly-greater Control Epoch. A fresh owner starts at 1;
 * an existing generation advances by one so a returning instance can never
 * reuse a fenced epoch.
 */
export function nextControlEpoch(previousEpoch: number | null): number {
  if (previousEpoch === null) {
    return 1;
  }
  if (!isPositiveSafeIntegerEpoch(previousEpoch)) {
    throw new Error(`Cannot advance from invalid control epoch ${String(previousEpoch)}`);
  }
  const advanced = previousEpoch + 1;
  if (!Number.isSafeInteger(advanced)) {
    throw new Error(
      `Control epoch ${previousEpoch} cannot advance without exceeding safe integers`,
    );
  }
  return advanced;
}

/** Returns whether a candidate epoch matches the current durable generation. */
export function isControlEpochCurrent(currentEpoch: number, candidateEpoch: number): boolean {
  return (
    isPositiveSafeIntegerEpoch(currentEpoch) &&
    isPositiveSafeIntegerEpoch(candidateEpoch) &&
    candidateEpoch === currentEpoch
  );
}

/**
 * Returns whether a candidate epoch is stale against the current generation.
 * A missing, invalid, or non-matching epoch is stale; only the exact current
 * generation is fresh.
 */
export function isControlEpochStale(
  currentEpoch: number,
  candidateEpoch: number | null | undefined,
): boolean {
  if (candidateEpoch === null || candidateEpoch === undefined) {
    return true;
  }
  return !isControlEpochCurrent(currentEpoch, candidateEpoch);
}

/** Discriminant tag for the typed stale-epoch failure. */
export const controlEpochStaleErrorTag = "ControlEpochStale" as const;

/** Public error code surfaced to fenced control callers. */
export const controlEpochStaleErrorCode = "CONTROL_EPOCH_STALE" as const;

/**
 * Typed failure raised when a control-protected command carries a missing,
 * invalid, or fenced Control Epoch. It is a warning: the client reconnects and
 * obtains the current epoch.
 */
export class ControlEpochStaleError extends Error {
  readonly _tag = controlEpochStaleErrorTag;
  readonly code = controlEpochStaleErrorCode;
  readonly controlChannel: ControlChannel;
  readonly currentEpoch: number | null;
  readonly participantId: string;
  readonly providedEpoch: number | null;
  readonly sessionId: string;

  constructor(input: {
    readonly controlChannel: ControlChannel;
    readonly currentEpoch: number | null;
    readonly participantId: string;
    readonly providedEpoch: number | null;
    readonly sessionId: string;
  }) {
    super(
      `Control epoch ${input.providedEpoch === null ? "missing" : String(input.providedEpoch)} is stale for session ${input.sessionId}, participant ${input.participantId}, channel ${input.controlChannel}; current epoch ${input.currentEpoch === null ? "none" : String(input.currentEpoch)}`,
    );
    this.name = "ControlEpochStaleError";
    this.controlChannel = input.controlChannel;
    this.currentEpoch = input.currentEpoch;
    this.participantId = input.participantId;
    this.providedEpoch = input.providedEpoch;
    this.sessionId = input.sessionId;
  }
}

/** Narrows an unknown failure to the typed stale-epoch error. */
export function isControlEpochStaleError(value: unknown): value is ControlEpochStaleError {
  return value instanceof ControlEpochStaleError;
}
