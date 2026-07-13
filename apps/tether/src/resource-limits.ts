/** Process-local resource limit policy and counters for HTTP/WebSocket boundaries. */

export const resourceLimitReason = {
  backpressure: "backpressure",
  bodyTooLarge: "body_too_large",
  rateLimited: "rate_limited",
  replayGapUnrepaired: "replay_gap_unrepaired",
  replayWindowExceeded: "replay_window_exceeded",
  wsPayloadTooLarge: "ws_payload_too_large",
} as const;

export type ResourceLimitReason = (typeof resourceLimitReason)[keyof typeof resourceLimitReason];

/** Configured process-local resource limit values. */
export interface ResourceLimits {
  readonly eventFanoutBatchLimit: number;
  readonly eventListDefaultLimit: number;
  readonly eventListMaxLimit: number;
  readonly httpMaxBodyBytes: number;
  readonly wsBackpressureBufferedBytes: number;
  readonly wsMaxPayloadBytes: number;
  readonly wsMessageRateLimit: number;
  readonly wsMessageRateWindowMs: number;
  readonly wsReplayMaxEvents: number;
}

/** Runtime counters for limit hits that are useful in debug snapshots. */
export interface ResourceLimitCounters {
  readonly bodyTooLargeCount: number;
  readonly replayWindowExceededCount: number;
  readonly wsPayloadTooLargeCount: number;
  readonly wsRateLimitedCount: number;
}

/** Debug snapshot for configured limits and observed limit hits. */
export interface ResourceLimitDebugInfo {
  readonly counters: ResourceLimitCounters;
  readonly limits: ResourceLimits;
}

/** Typed error thrown when a resource limit rejects inbound work. */
export class ResourceLimitExceededError extends Error {
  readonly _tag = "ResourceLimitExceeded";
  readonly max: number | undefined;
  readonly observed: number | undefined;
  readonly reason: ResourceLimitReason;
  readonly routeName: string | undefined;

  constructor(input: {
    readonly max?: number | undefined;
    readonly observed?: number | undefined;
    readonly reason: ResourceLimitReason;
    readonly routeName?: string | undefined;
  }) {
    super(`Resource limit exceeded: ${input.reason}`);
    this.max = input.max;
    this.observed = input.observed;
    this.reason = input.reason;
    this.routeName = input.routeName;
  }
}

/** Default process limits for Tether server resources. */
export const defaultResourceLimits: ResourceLimits = {
  eventFanoutBatchLimit: 500,
  eventListDefaultLimit: 500,
  eventListMaxLimit: 1_000,
  httpMaxBodyBytes: 2 * 1024 * 1024,
  wsBackpressureBufferedBytes: 4 * 1024 * 1024,
  wsMaxPayloadBytes: 2 * 1024 * 1024,
  wsMessageRateLimit: 60,
  wsMessageRateWindowMs: 10_000,
  wsReplayMaxEvents: 2_000,
};

/** Mutable per-process limit counters shared by app-server modules. */
export class ResourceLimitRuntime {
  private bodyTooLargeCount = 0;
  private replayWindowExceededCount = 0;
  private wsPayloadTooLargeCount = 0;
  private wsRateLimitedCount = 0;

  constructor(readonly limits: ResourceLimits) {}

  /** Returns a stable debug snapshot of configured limits and counters. */
  debugInfo(): ResourceLimitDebugInfo {
    return {
      counters: {
        bodyTooLargeCount: this.bodyTooLargeCount,
        replayWindowExceededCount: this.replayWindowExceededCount,
        wsPayloadTooLargeCount: this.wsPayloadTooLargeCount,
        wsRateLimitedCount: this.wsRateLimitedCount,
      },
      limits: this.limits,
    };
  }

  /** Records one rejected HTTP body. */
  recordBodyTooLarge(): void {
    this.bodyTooLargeCount += 1;
  }

  /** Records one rejected initial WebSocket replay. */
  recordReplayWindowExceeded(): void {
    this.replayWindowExceededCount += 1;
  }

  /** Records one oversized WebSocket payload rejection. */
  recordWsPayloadTooLarge(): void {
    this.wsPayloadTooLargeCount += 1;
  }

  /** Records one WebSocket message-rate rejection. */
  recordWsRateLimited(): void {
    this.wsRateLimitedCount += 1;
  }
}

/** Message-rate decision returned before parsing a WebSocket frame. */
export interface WebSocketRateLimitDecision {
  readonly allowed: boolean;
  readonly limit: number;
  readonly observed: number;
  readonly windowMs: number;
}

/** Per-socket fixed-window rate limiter for inbound WebSocket messages. */
export interface WebSocketMessageRateLimiter {
  readonly check: () => WebSocketRateLimitDecision;
}

/** Clock function used to make rate-limit tests deterministic. */
export type ResourceLimitClock = () => number;

/** Creates a per-connection WebSocket message rate limiter. */
export function createWebSocketMessageRateLimiter(input: {
  readonly limit: number;
  readonly now?: ResourceLimitClock | undefined;
  readonly windowMs: number;
}): WebSocketMessageRateLimiter {
  const now = input.now ?? Date.now;
  let count = 0;
  let windowStartedAt = now();
  return {
    check: () => {
      const current = now();
      if (current - windowStartedAt >= input.windowMs) {
        count = 0;
        windowStartedAt = current;
      }
      count += 1;
      return {
        allowed: count <= input.limit,
        limit: input.limit,
        observed: count,
        windowMs: input.windowMs,
      };
    },
  };
}

/** Returns true when an unknown error is a resource-limit rejection. */
export function isResourceLimitExceeded(error: unknown): error is ResourceLimitExceededError {
  return error instanceof ResourceLimitExceededError;
}

/** Builds a typed HTTP body-size limit error. */
export function bodyTooLargeError(input: {
  readonly max: number;
  readonly observed: number;
  readonly routeName?: string | undefined;
}): ResourceLimitExceededError {
  return new ResourceLimitExceededError({
    max: input.max,
    observed: input.observed,
    reason: resourceLimitReason.bodyTooLarge,
    routeName: input.routeName,
  });
}

/** Parses and clamps the REST event-list page size. */
export function parseEventListLimit(
  value: string | null,
  limits: Pick<ResourceLimits, "eventListDefaultLimit" | "eventListMaxLimit">,
): number {
  if (value === null || value.trim() === "") {
    return limits.eventListDefaultLimit;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return limits.eventListDefaultLimit;
  }
  return Math.min(parsed, limits.eventListMaxLimit);
}

/** Parses a positive integer environment value with fallback. */
export function parsePositiveResourceLimit(value: string | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
