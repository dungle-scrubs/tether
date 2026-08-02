/** Computes one equal-jitter bounded exponential retry delay from a zero-based attempt. */
export function boundedExponentialRetryDelayMs(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number,
  random: () => number = Math.random,
): number {
  assertNonNegativeFiniteNumber("attempt", attempt);
  if (!Number.isInteger(attempt)) {
    throw new Error("Retry attempt must be an integer");
  }
  assertNonNegativeFiniteNumber("baseDelayMs", baseDelayMs);
  assertNonNegativeFiniteNumber("maxDelayMs", maxDelayMs);
  if (baseDelayMs === 0 || maxDelayMs === 0) {
    return 0;
  }
  const boundedDelayMs = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
  const randomFraction = random();
  if (!Number.isFinite(randomFraction) || randomFraction < 0 || randomFraction > 1) {
    throw new Error("Retry random source must return a finite number from zero through one");
  }
  return Math.round(boundedDelayMs / 2 + (boundedDelayMs / 2) * randomFraction);
}

/** Default bounded exponential reconnect limits shared by every Tether transport. */
export const defaultReconnectDelayPolicy = {
  baseDelayMs: 100,
  maxDelayMs: 2_000,
} as const;

/** Optional per-transport overrides of the shared reconnect delay limits. */
export interface ReconnectDelayPolicy {
  /** Initial delay before the first reconnect. */
  readonly baseDelayMs?: number | undefined;
  /** Maximum reconnect delay. */
  readonly maxDelayMs?: number | undefined;
}

/** Resolves one optional reconnect policy into a bounded exponential delay. */
export function boundedReconnectDelayMs(
  attempt: number,
  policy: ReconnectDelayPolicy | undefined,
): number {
  return boundedExponentialRetryDelayMs(
    attempt,
    policy?.baseDelayMs ?? defaultReconnectDelayPolicy.baseDelayMs,
    policy?.maxDelayMs ?? defaultReconnectDelayPolicy.maxDelayMs,
  );
}

/** Rejects retry inputs that cannot produce a deterministic finite delay. */
function assertNonNegativeFiniteNumber(label: string, value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`Retry ${label} must be a non-negative finite number`);
  }
}
