/**
 * Owns the reusable REST participant-control lifecycle for external runtimes.
 * It acquires one context per session, renews it from server guidance,
 * invalidates only matching generations, and releases exact generations. It
 * intentionally has no task-route knowledge and never retries protected
 * mutations.
 */

import {
  releaseParticipantControlResponseSchema,
  restControlAcquisitionResponseSchema,
  restControlRenewalResponseSchema,
} from "@dungle-scrubs/tether-protocol";

import { resolveServiceAuthToken } from "./auth-token.js";
import {
  ModuleObservability,
  type ModuleObservabilityOptions,
  readModuleObservabilityOptions,
} from "./observability.js";

/** Fetch-compatible function used by the REST control client. */
export type RestParticipantControlFetch = (input: URL, init: RequestInit) => Promise<Response>;

/** Installed participant control context for one session. */
export interface RestParticipantControlContext {
  readonly acquisitionId: string;
  readonly controlEpoch: number;
  readonly instanceId: string;
  readonly leaseExpiresAt: string;
  readonly participantId: string;
  readonly renewAfterMs: number;
  readonly sessionId: string;
}

/** Timer handle supporting Node's optional event-loop detachment. */
export interface RestParticipantControlTimerHandle {
  readonly unref?: () => void;
}

/** Injected timer seam used for deterministic renewal tests. */
export interface RestParticipantControlTimerScheduler {
  readonly clearTimeout: (handle: RestParticipantControlTimerHandle) => void;
  readonly setTimeout: (callback: () => void, delayMs: number) => RestParticipantControlTimerHandle;
}

/** Static participant identity and service configuration. */
export interface RestParticipantControlClientConfig {
  readonly authToken?: string | null;
  readonly capabilities?: Readonly<Record<string, unknown>>;
  readonly displayName?: string;
  readonly instanceId: string;
  readonly participantId: string;
  readonly runtimeKind: string;
  readonly serviceUrl: string;
}

/** Injectable runtime dependencies and bounded retry configuration. */
export interface RestParticipantControlClientOptions {
  readonly acquisitionIdFactory?: () => string;
  readonly acquisitionTransportRetries?: number;
  readonly fetch?: RestParticipantControlFetch;
  readonly now?: () => number;
  /**
   * Synchronously reports the first renewal error or lease expiry per
   * acquisition/epoch, before retry or invalidation. Successful renewal does
   * not reset delivery. An independent timer checks expiry even if renewal
   * hangs. The fixed payload contains no response or error data.
   * The callback may invalidate the matching generation to suppress retry.
   * A thrown callback invalidates only its exact installed context and also
   * suppresses retry. This notification never grants or restores authority.
   */
  readonly onContextUncertain?: (payload: {
    readonly acquisitionId: string;
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly reason: RestParticipantControlErrorCode | "LEASE_EXPIRED";
    readonly sessionId: string;
  }) => void;
  readonly observability?: ModuleObservabilityOptions;
  readonly shutdownPendingWaitMs?: number;
  readonly timers?: RestParticipantControlTimerScheduler;
}

/** Stable failure classifications surfaced to lifecycle callers. */
export type RestParticipantControlErrorCode =
  | "AUTHENTICATION"
  | "CONTROL_ACQUISITION_ID_REQUIRED"
  | "CONTROL_ACQUISITION_STALE"
  | "CONTROL_CONFLICT"
  | "CONTROL_EPOCH_REQUIRED"
  | "CONTROL_EPOCH_STALE"
  | "INVALID_RESPONSE"
  | "PERSISTENCE"
  | "STOPPED"
  | "TRANSPORT";

/** Bounded renewal-or-expiry reason delivered to `onContextUncertain`. */
type RestParticipantControlUncertaintyReason = RestParticipantControlErrorCode | "LEASE_EXPIRED";

/** Typed, payload-free REST participant-control failure. */
export class RestParticipantControlError extends Error {
  readonly code: RestParticipantControlErrorCode;
  readonly details: {
    readonly operation: "acquire" | "release" | "renew";
    readonly status: number | null;
  };

  constructor(input: {
    readonly code: RestParticipantControlErrorCode;
    readonly operation: "acquire" | "release" | "renew";
    readonly status?: number | null;
  }) {
    super(`REST participant control ${input.operation} failed: ${input.code}`);
    this.code = input.code;
    this.details = {
      operation: input.operation,
      status: input.status ?? null,
    };
    this.name = "RestParticipantControlError";
  }
}

/** Bounded client lifecycle diagnostics. */
export interface RestParticipantControlClientDebugInfo {
  readonly activeContextCount: number;
  readonly acquiringSessionCount: number;
  readonly boundary: ReturnType<ModuleObservability["debugInfo"]>;
  readonly lastFailureCode: RestParticipantControlErrorCode | null;
  readonly renewalTimerCount: number;
  readonly stopped: boolean;
  readonly uncertainReleaseCount: number;
}

type SessionState = {
  acquisitionId: string | null;
  context: RestParticipantControlContext | null;
  /** Renewal timer driving the next heartbeat at `min(renewAfterMs, expiresIn/2)`. */
  timer: RestParticipantControlTimerHandle | null;
  /** Separate expiry timer that notifies `LEASE_EXPIRED` at `leaseExpiresAt`. */
  expiryTimer: RestParticipantControlTimerHandle | null;
  pending: {
    readonly acquisitionId: string;
    readonly controller: AbortController;
    readonly promise: Promise<RestParticipantControlContext>;
  } | null;
  uncertainty: { readonly acquisitionId: string; readonly controlEpoch: number } | null;
};

const defaultTimers: RestParticipantControlTimerScheduler = {
  clearTimeout: (handle) => {
    clearTimeout(handle as ReturnType<typeof setTimeout>);
  },
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
};

/** Reusable REST lifecycle Module for one participant runtime identity. */
export class RestParticipantControlClient {
  readonly #acquisitionIdFactory: () => string;
  readonly #acquisitionTransportRetries: number;
  readonly #authToken: string | null;
  readonly #capabilities: Readonly<Record<string, unknown>>;
  readonly #displayName: string;
  readonly #fetch: RestParticipantControlFetch;
  readonly #instanceId: string;
  #lastFailureCode: RestParticipantControlErrorCode | null = null;
  readonly #now: () => number;
  readonly #observability: ModuleObservability;
  readonly #onContextUncertain: RestParticipantControlClientOptions["onContextUncertain"];
  readonly #participantId: string;
  readonly #runtimeKind: string;
  readonly #serviceUrl: string;
  readonly #shutdownPendingWaitMs: number;
  readonly #states = new Map<string, SessionState>();
  #stopPromise: Promise<void> | null = null;
  #stopped = false;
  readonly #timers: RestParticipantControlTimerScheduler;
  #uncertainReleaseCount = 0;

  constructor(
    config: RestParticipantControlClientConfig,
    options: RestParticipantControlClientOptions = {},
  ) {
    this.#acquisitionIdFactory = options.acquisitionIdFactory ?? defaultAcquisitionIdFactory;
    this.#acquisitionTransportRetries = Math.max(
      0,
      Math.min(3, options.acquisitionTransportRetries ?? 1),
    );
    this.#authToken = resolveServiceAuthToken(config.authToken);
    this.#capabilities = config.capabilities ?? {};
    this.#displayName = config.displayName ?? config.participantId;
    this.#fetch = options.fetch ?? fetch;
    this.#instanceId = config.instanceId;
    this.#now = options.now ?? Date.now;
    this.#observability = new ModuleObservability(
      options.observability ?? readModuleObservabilityOptions("RestParticipantControlClient"),
    );
    this.#onContextUncertain = options.onContextUncertain;
    this.#participantId = config.participantId;
    this.#runtimeKind = config.runtimeKind;
    this.#serviceUrl = config.serviceUrl.replace(/\/$/u, "");
    this.#shutdownPendingWaitMs = Math.max(
      0,
      Math.min(30_000, options.shutdownPendingWaitMs ?? 5_000),
    );
    this.#timers = options.timers ?? defaultTimers;
  }

  /** Acquires or returns the single current context for a session. */
  async context(sessionId: string): Promise<RestParticipantControlContext> {
    return this.#observability.traceBoundary(
      "acquire",
      { operation: "acquire" },
      async () => {
        if (this.#stopped) {
          throw this.#error("STOPPED", "acquire");
        }
        const state = this.#state(sessionId);
        if (state.context && Date.parse(state.context.leaseExpiresAt) <= this.#now()) {
          const expired = state.context;
          this.#notifyUncertainty(expired, "LEASE_EXPIRED");
          if (state.context === expired) {
            this.#invalidateStateContext(state, expired);
          }
        }
        if (state.context) {
          return state.context;
        }
        if (state.pending) {
          return state.pending.promise;
        }
        const acquisitionId = state.acquisitionId ?? this.#acquisitionIdFactory();
        state.acquisitionId = acquisitionId;
        const controller = new AbortController();
        const promise = this.#acquire(sessionId, acquisitionId, controller.signal);
        state.pending = { acquisitionId, controller, promise };
        try {
          return await promise;
        } catch (error) {
          if (
            error instanceof RestParticipantControlError &&
            error.code === "CONTROL_ACQUISITION_STALE"
          ) {
            state.acquisitionId = null;
          }
          throw error;
        } finally {
          if (state.pending?.acquisitionId === acquisitionId) {
            state.pending = null;
          }
        }
      },
      () => ({ outcome: "active" }),
    );
  }

  /** Invalidates the installed generation matching the supplied context snapshot. */
  invalidate(context: RestParticipantControlContext): boolean {
    return this.#observability.traceBoundarySync(
      "invalidate",
      { operation: "invalidate" },
      () => {
        const state = this.#states.get(context.sessionId);
        if (
          !state?.context ||
          state.context.acquisitionId !== context.acquisitionId ||
          state.context.controlEpoch !== context.controlEpoch
        ) {
          return false;
        }
        this.#invalidateStateContext(state, state.context);
        return true;
      },
      (invalidated) => ({ outcome: invalidated ? "invalidated" : "unchanged" }),
    );
  }

  /** Releases one installed session context with its exact epoch. */
  async release(sessionId: string): Promise<boolean> {
    return this.#observability.traceBoundary(
      "release",
      { operation: "release" },
      async () => {
        const state = this.#states.get(sessionId);
        const context = state?.context;
        if (!state || !context) {
          return false;
        }
        this.#clearTimer(state);
        this.#clearExpiryTimer(state);
        state.context = null;
        try {
          const response = await this.#request(
            "release",
            `/sessions/${encodeURIComponent(sessionId)}/participants/${encodeURIComponent(
              this.#participantId,
            )}/control/release`,
            {
              controlEpoch: context.controlEpoch,
              instanceId: this.#instanceId,
            },
          );
          const parsed = releaseParticipantControlResponseSchema.safeParse(response.body);
          if (!parsed.success) {
            throw this.#error("INVALID_RESPONSE", "release", response.status);
          }
          state.acquisitionId = null;
          return parsed.data.released;
        } catch (error) {
          if (isUncertainReleaseFailure(error)) {
            this.#uncertainReleaseCount += 1;
          }
          throw error;
        }
      },
      (released) => ({ outcome: released ? "released" : "inactive" }),
    );
  }

  /** Releases all active contexts and clears every renewal timer once. */
  stop(): Promise<void> {
    if (this.#stopPromise) {
      return this.#stopPromise;
    }
    this.#stopped = true;
    // Clear expiry timers synchronously so a hung release or pending
    // acquisition cannot keep them active past stop.
    for (const state of this.#states.values()) {
      this.#clearExpiryTimer(state);
    }
    this.#stopPromise = (async () => {
      const pending = [...this.#states.values()].flatMap((state) =>
        state.pending ? [{ pending: state.pending, state }] : [],
      );
      const settled = await waitForPendingAcquisitions(
        pending.map(({ pending: acquisition }) => acquisition.promise),
        this.#shutdownPendingWaitMs,
      );
      if (!settled) {
        for (const { pending: acquisition, state } of pending) {
          if (state.pending === acquisition) {
            state.pending = null;
            acquisition.controller.abort();
          }
        }
      }
      const sessionIds = [...this.#states.entries()]
        .filter(([, state]) => state.context !== null)
        .map(([sessionId]) => sessionId);
      const releases = await Promise.allSettled(
        sessionIds.map((sessionId) => this.release(sessionId)),
      );
      for (const state of this.#states.values()) {
        this.#clearTimer(state);
      }
      if (releases.some((result) => result.status === "rejected")) {
        return;
      }
    })();
    return this.#stopPromise;
  }

  /** Returns bounded process-local lifecycle state without participant identity. */
  debugInfo(): RestParticipantControlClientDebugInfo {
    let activeContextCount = 0;
    let acquiringSessionCount = 0;
    let renewalTimerCount = 0;
    for (const state of this.#states.values()) {
      activeContextCount += state.context ? 1 : 0;
      acquiringSessionCount += state.pending ? 1 : 0;
      renewalTimerCount += state.timer ? 1 : 0;
    }
    return {
      activeContextCount,
      acquiringSessionCount,
      boundary: this.#observability.debugInfo(),
      lastFailureCode: this.#lastFailureCode,
      renewalTimerCount,
      stopped: this.#stopped,
      uncertainReleaseCount: this.#uncertainReleaseCount,
    };
  }

  async #acquire(
    sessionId: string,
    acquisitionId: string,
    signal: AbortSignal,
  ): Promise<RestParticipantControlContext> {
    let attempt = 0;
    while (attempt <= this.#acquisitionTransportRetries) {
      try {
        const response = await this.#request(
          "acquire",
          `/sessions/${encodeURIComponent(sessionId)}/participants`,
          {
            acquisitionId,
            capabilities: this.#capabilities,
            controlChannel: "rest",
            displayName: this.#displayName,
            instanceId: this.#instanceId,
            participantId: this.#participantId,
            runtimeKind: this.#runtimeKind,
          },
          signal,
        );
        const parsed = restControlAcquisitionResponseSchema.safeParse(response.body);
        if (!parsed.success) {
          throw this.#error("INVALID_RESPONSE", "acquire", response.status);
        }
        const state = this.#state(sessionId);
        if (parsed.data.acquisitionId !== acquisitionId) {
          throw this.#error("INVALID_RESPONSE", "acquire", response.status);
        }
        if (state.pending?.acquisitionId !== acquisitionId) {
          throw this.#error("STOPPED", "acquire");
        }
        const context: RestParticipantControlContext = {
          acquisitionId: parsed.data.acquisitionId,
          controlEpoch: parsed.data.controlEpoch,
          instanceId: this.#instanceId,
          leaseExpiresAt: parsed.data.leaseExpiresAt,
          participantId: this.#participantId,
          renewAfterMs: parsed.data.renewAfterMs,
          sessionId,
        };
        state.context = context;
        this.#scheduleRenewal(state, context);
        this.#scheduleExpiryTimer(state, context);
        return context;
      } catch (error) {
        if (
          error instanceof RestParticipantControlError &&
          error.code === "TRANSPORT" &&
          attempt < this.#acquisitionTransportRetries
        ) {
          attempt += 1;
          continue;
        }
        throw error;
      }
    }
    throw this.#error("TRANSPORT", "acquire");
  }

  async #renew(context: RestParticipantControlContext): Promise<void> {
    await this.#observability.traceBoundary("renew", { operation: "renew" }, async () => {
      const state = this.#states.get(context.sessionId);
      if (state?.context !== context || this.#stopped) {
        return;
      }
      try {
        const response = await this.#request(
          "renew",
          `/sessions/${encodeURIComponent(context.sessionId)}/participants/${encodeURIComponent(
            this.#participantId,
          )}/heartbeat`,
          {
            controlEpoch: context.controlEpoch,
            instanceId: this.#instanceId,
          },
        );
        const parsed = restControlRenewalResponseSchema.safeParse(response.body);
        if (!parsed.success) {
          throw this.#error("INVALID_RESPONSE", "renew", response.status);
        }
        if (parsed.data.controlEpoch !== context.controlEpoch) {
          throw this.#error("INVALID_RESPONSE", "renew", response.status);
        }
        if (state.context !== context) {
          return;
        }
        const renewed: RestParticipantControlContext = {
          ...context,
          leaseExpiresAt: parsed.data.leaseExpiresAt,
          renewAfterMs: parsed.data.renewAfterMs,
        };
        state.context = renewed;
        this.#scheduleRenewal(state, renewed);
        this.#scheduleExpiryTimer(state, renewed);
      } catch (error) {
        if (state.context !== context) {
          throw error;
        }
        const reason: RestParticipantControlUncertaintyReason =
          error instanceof RestParticipantControlError ? error.code : "INVALID_RESPONSE";
        const isStaleClass =
          error instanceof RestParticipantControlError &&
          (error.code === "CONTROL_EPOCH_STALE" ||
            error.code === "CONTROL_CONFLICT" ||
            error.code === "CONTROL_EPOCH_REQUIRED");
        this.#notifyUncertainty(context, reason);
        if (state.context !== context) {
          throw error;
        }
        if (isStaleClass) {
          this.#invalidateStateContext(state, context);
        } else {
          this.#scheduleRenewalRecovery(state, context);
        }
        throw error;
      }
    });
  }

  async #request(
    operation: "acquire" | "release" | "renew",
    path: string,
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<{ readonly body: unknown; readonly status: number }> {
    let response: Response;
    try {
      response = await this.#fetch(new URL(path, `${this.#serviceUrl}/`), {
        body: JSON.stringify(body),
        headers: {
          ...(this.#authToken ? { authorization: `Bearer ${this.#authToken}` } : {}),
          "content-type": "application/json",
        },
        method: "POST",
        ...(signal ? { signal } : {}),
      });
    } catch {
      throw this.#error("TRANSPORT", operation);
    }
    let responseBody: unknown;
    try {
      responseBody = await readJson(response);
    } catch {
      throw this.#error("TRANSPORT", operation, response.status);
    }
    if (!response.ok) {
      throw this.#error(mapServerError(response.status, responseBody), operation, response.status);
    }
    return { body: responseBody, status: response.status };
  }

  #scheduleRenewal(state: SessionState, context: RestParticipantControlContext): void {
    this.#clearTimer(state);
    const expiresInMs = Math.max(1, Date.parse(context.leaseExpiresAt) - this.#now());
    const delayMs = Math.max(1, Math.min(context.renewAfterMs, Math.floor(expiresInMs / 2)));
    const timer = this.#timers.setTimeout(() => {
      if (state.timer === timer) {
        state.timer = null;
      }
      void this.#renew(context).catch(() => undefined);
    }, delayMs);
    timer.unref?.();
    state.timer = timer;
  }

  #clearTimer(state: SessionState): void {
    if (!state.timer) {
      return;
    }
    this.#timers.clearTimeout(state.timer);
    state.timer = null;
  }

  /**
   * Schedules a one-shot expiry timer at `leaseExpiresAt`. Opt-in: only
   * installed when an uncertainty callback is configured. The callback
   * checks timer and context identity so a late firing after renewal,
   * replacement, invalidate, release, or stop cannot notify or invalidate
   * any other generation.
   */
  #scheduleExpiryTimer(state: SessionState, context: RestParticipantControlContext): void {
    this.#clearExpiryTimer(state);
    if (!this.#onContextUncertain || this.#stopped || state.context !== context) {
      return;
    }
    const expiresInMs = Date.parse(context.leaseExpiresAt) - this.#now();
    // Platform timeout maximum: Node's `setTimeout` accepts at most a
    // signed 32-bit millisecond value. Cap so the timer cannot overflow.
    const platformMaxMs = 2_147_483_647;
    const delayMs = Math.min(platformMaxMs, Math.max(1, expiresInMs));
    const timer = this.#timers.setTimeout(() => {
      if (state.expiryTimer !== timer) {
        return;
      }
      state.expiryTimer = null;
      if (this.#stopped || state.context !== context) {
        return;
      }
      if (Date.parse(context.leaseExpiresAt) > this.#now()) {
        this.#scheduleExpiryTimer(state, context);
        return;
      }
      this.#notifyUncertainty(context, "LEASE_EXPIRED");
      if (state.context === context) {
        this.#invalidateStateContext(state, context);
      }
    }, delayMs);
    timer.unref?.();
    state.expiryTimer = timer;
  }

  #clearExpiryTimer(state: SessionState): void {
    if (!state.expiryTimer) {
      return;
    }
    this.#timers.clearTimeout(state.expiryTimer);
    state.expiryTimer = null;
  }

  #invalidateStateContext(state: SessionState, context: RestParticipantControlContext): void {
    if (state.context !== context) {
      return;
    }
    this.#clearTimer(state);
    this.#clearExpiryTimer(state);
    state.acquisitionId = null;
    state.context = null;
  }

  #scheduleRenewalRecovery(state: SessionState, context: RestParticipantControlContext): void {
    const remainingMs = Date.parse(context.leaseExpiresAt) - this.#now();
    if (remainingMs <= 1) {
      this.#invalidateStateContext(state, context);
      return;
    }
    this.#clearTimer(state);
    const delayMs = Math.max(1, Math.min(1_000, Math.floor(remainingMs / 2)));
    const timer = this.#timers.setTimeout(() => {
      if (state.timer === timer) {
        state.timer = null;
      }
      void this.#renew(context).catch(() => undefined);
    }, delayMs);
    timer.unref?.();
    state.timer = timer;
  }

  #state(sessionId: string): SessionState {
    const existing = this.#states.get(sessionId);
    if (existing) {
      return existing;
    }
    const created: SessionState = {
      acquisitionId: null,
      context: null,
      pending: null,
      timer: null,
      expiryTimer: null,
      uncertainty: null,
    };
    this.#states.set(sessionId, created);
    return created;
  }

  #error(
    code: RestParticipantControlErrorCode,
    operation: "acquire" | "release" | "renew",
    status: number | null = null,
  ): RestParticipantControlError {
    this.#lastFailureCode = code;
    return new RestParticipantControlError({
      code,
      operation,
      status,
    });
  }

  /** Reports uncertainty only for the installed context, once per acquisition/epoch. */
  #notifyUncertainty(
    context: RestParticipantControlContext,
    reason: RestParticipantControlUncertaintyReason,
  ): void {
    const state = this.#states.get(context.sessionId);
    if (!this.#onContextUncertain || state?.context !== context) {
      return;
    }
    if (
      state.uncertainty?.acquisitionId === context.acquisitionId &&
      state.uncertainty.controlEpoch === context.controlEpoch
    ) {
      return;
    }
    state.uncertainty = {
      acquisitionId: context.acquisitionId,
      controlEpoch: context.controlEpoch,
    };
    try {
      this.#onContextUncertain(
        Object.freeze({
          acquisitionId: context.acquisitionId,
          controlEpoch: context.controlEpoch,
          instanceId: context.instanceId,
          reason,
          sessionId: context.sessionId,
        }),
      );
    } catch {
      // A failed consumer gate must not leave this context installed or retry it.
      this.#invalidateStateContext(state, context);
    }
  }
}

/** Creates a browser-and-Node-compatible Acquisition ID. */
function defaultAcquisitionIdFactory(): string {
  return globalThis.crypto.randomUUID();
}

/** Parses JSON without leaking malformed response payloads into typed errors. */
async function readJson(response: Response): Promise<unknown> {
  let text: string;
  try {
    text = await response.text();
  } catch {
    throw new Error("Response body transport failed");
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
}

/** Maps public HTTP status and stable error code to a bounded client failure. */
function mapServerError(status: number, body: unknown): RestParticipantControlErrorCode {
  const code =
    typeof body === "object" && body !== null && "code" in body && typeof body.code === "string"
      ? body.code
      : null;
  if (
    code === "CONTROL_ACQUISITION_ID_REQUIRED" ||
    code === "CONTROL_ACQUISITION_STALE" ||
    code === "CONTROL_EPOCH_REQUIRED" ||
    code === "CONTROL_EPOCH_STALE"
  ) {
    return code;
  }
  if (status === 401 || status === 403) {
    return "AUTHENTICATION";
  }
  if (status === 409) {
    return "CONTROL_CONFLICT";
  }
  return status >= 500 ? "PERSISTENCE" : "INVALID_RESPONSE";
}

/** Identifies release failures where the server may already have committed. */
function isUncertainReleaseFailure(error: unknown): boolean {
  return (
    error instanceof RestParticipantControlError &&
    (error.code === "INVALID_RESPONSE" ||
      error.code === "PERSISTENCE" ||
      error.code === "TRANSPORT")
  );
}

/** Waits for acquisitions without allowing shutdown to hang indefinitely. */
async function waitForPendingAcquisitions(
  acquisitions: readonly Promise<RestParticipantControlContext>[],
  timeoutMs: number,
): Promise<boolean> {
  if (acquisitions.length === 0) {
    return true;
  }
  if (timeoutMs <= 0) {
    return false;
  }
  let timeout: ReturnType<typeof setTimeout> | null = null;
  const timedOut = new Promise<false>((resolve) => {
    timeout = setTimeout(() => resolve(false), timeoutMs);
    timeout.unref?.();
  });
  const settled = Promise.allSettled(acquisitions).then(() => true as const);
  const result = await Promise.race([settled, timedOut]);
  if (timeout) {
    clearTimeout(timeout);
  }
  return result;
}
