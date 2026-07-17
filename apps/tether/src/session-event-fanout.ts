import { createHash } from "node:crypto";
import type pg from "pg";

import { Effect, Fiber } from "effect";

import {
  parseSessionEventNotification,
  sessionEventNotificationChannel,
  type DatabasePool,
  type SessionEventNotification,
} from "./db.js";
import { sleepUnrefEffect } from "./effect-runtime.js";
import type { SubscriptionHub } from "./hub.js";
import { ModuleObservability } from "./observability.js";
import type { ModuleObservabilityOptions } from "./observability.js";
import type { SessionServiceDebugInfo } from "./session-service.js";
import type { SessionEvent } from "./types.js";

const sessionEventFanoutListenerState = {
  connected: "connected",
  connecting: "connecting",
  disabled: "disabled",
  disconnected: "disconnected",
  stopped: "stopped",
} as const;

type SessionEventFanoutListenerState =
  (typeof sessionEventFanoutListenerState)[keyof typeof sessionEventFanoutListenerState];

/** Serializable metadata for the most recent LISTEN client failure. */
export interface SessionEventFanoutListenerErrorInfo {
  readonly message: string;
  readonly name: string;
}

/**
 * Runtime diagnostics for Postgres-backed cross-replica event fanout.
 */
export interface SessionEventFanoutDebugInfo {
  readonly broadcastCount: number;
  readonly catchUpBatchCount: number;
  readonly catchUpEventCount: number;
  readonly catchUpFailureCount: number;
  readonly catchUpRecoveryCount: number;
  readonly catchUpPollCount: number;
  readonly catchUpPollIntervalMs: number;
  readonly coalescedNotificationCount: number;
  readonly connected: boolean;
  readonly droppedNotificationCount: number;
  readonly fanoutCursorSessionCount: number;
  readonly ignoredSelfNotificationCount: number;
  readonly invalidNotificationCount: number;
  readonly lastConnectedAt: string | null;
  readonly lastDisconnectedAt: string | null;
  readonly lastListenerError: SessionEventFanoutListenerErrorInfo | null;
  readonly lastReconnectDelayMs: number | null;
  readonly listenerErrorCount: number;
  readonly listenerState: SessionEventFanoutListenerState;
  readonly lastCatchUpOutcome: SessionEventFanoutCatchUpOutcome;
  readonly notificationCount: number;
  readonly pendingNotificationSessionCount: number;
  readonly reconnectAttemptCount: number;
  readonly reconnectSuccessCount: number;
  readonly sessionCursorCount: number;
  readonly scheduled: boolean;
  readonly sessionLag: readonly SessionEventFanoutSessionLagInfo[];
}

/** Bounded outcome values for catch-up diagnostics and readiness. */
export type SessionEventFanoutCatchUpOutcome = "caught_up" | "events_pending" | "failed" | "idle";

/** Per-session durable catch-up lag without exposing raw session identifiers. */
export interface SessionEventFanoutSessionLagInfo {
  readonly lagAgeMs: number;
  readonly outcome: SessionEventFanoutCatchUpOutcome;
  readonly sessionHash: string;
}

/**
 * Dependencies for a session event fanout listener.
 */
interface SessionEventFanoutOptions {
  readonly eventBatchLimit?: number;
  readonly catchUpPollIntervalMs?: number;
  readonly database: DatabasePool;
  readonly hub: SubscriptionHub;
  readonly listenEnabled?: boolean;
  /** Maximum sessions with one pending cross-replica notification marker. */
  readonly notificationQueueLimit?: number;
  readonly now?: () => number;
  readonly observability?: Omit<ModuleObservabilityOptions, "moduleName">;
  readonly reconnectBaseDelayMs?: number;
  readonly reconnectMaxDelayMs?: number;
  readonly service: SessionEventFanoutSessionService;
}

interface SessionCatchUpState {
  lagStartedAt: number | null;
  outcome: SessionEventFanoutCatchUpOutcome;
  recoveringFromFailure: boolean;
  sessionHash: string;
}

/**
 * Minimal durable service surface required by cross-replica event fanout.
 */
export interface SessionEventFanoutSessionService {
  /** Returns service diagnostics including this replica's event source id. */
  readonly debugInfo: () => Pick<SessionServiceDebugInfo, "eventSourceId">;
  /** Lists committed events after a session cursor. */
  readonly listEvents: (
    sessionId: string,
    afterSeq: number,
    options?: { readonly limit?: number | undefined },
  ) => Effect.Effect<SessionEvent[], unknown>;
}

/**
 * Default interval for polling durable events as a safety net behind
 * LISTEN/NOTIFY.
 */
export const defaultEventFanoutCatchUpPollMs = 1_000;
export const defaultEventFanoutCatchUpStaleMs = 30_000;
export const defaultEventFanoutBatchLimit = 500;
export const defaultEventFanoutNotificationQueueLimit = 1_000;
const defaultListenerReconnectBaseDelayMs = 100;
const defaultListenerReconnectMaxDelayMs = 5_000;

/**
 * Listens for Postgres notifications about committed session events from other
 * Tether replicas and broadcasts them to local WebSocket subscribers.
 */
export class SessionEventFanout {
  private broadcastCount = 0;
  private catchUpBatchCount = 0;
  private catchUpEventCount = 0;
  private catchUpFailureCount = 0;
  private catchUpPollCount = 0;
  private catchUpRecoveryCount = 0;
  private readonly catchUpPollIntervalMs: number;
  private readonly eventBatchLimit: number;
  private client: pg.PoolClient | null = null;
  private listenerState: SessionEventFanoutListenerState = sessionEventFanoutListenerState.stopped;
  private listenerErrorCount = 0;
  private reconnectAttemptCount = 0;
  private reconnectSuccessCount = 0;
  private readonly reconnectBaseDelayMs: number;
  private readonly reconnectMaxDelayMs: number;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnecting: Promise<void> | null = null;
  private lastConnectedAt: string | null = null;
  private lastDisconnectedAt: string | null = null;
  private lastListenerError: SessionEventFanoutListenerErrorInfo | null = null;
  private lastReconnectDelayMs: number | null = null;
  private coalescedNotificationCount = 0;
  private droppedNotificationCount = 0;
  private ignoredSelfNotificationCount = 0;
  private invalidNotificationCount = 0;
  private notificationCount = 0;
  private readonly notificationQueueLimit: number;
  private notificationQueueSaturated = false;
  private readonly now: () => number;
  /** Lowest pending notified seq per session awaiting one coalesced fetch. */
  private readonly pendingNotificationSeqs = new Map<string, number>();
  private readonly observability: ModuleObservability;
  private nextRoundStartIndex = 0;
  private lastCatchUpOutcome: SessionEventFanoutCatchUpOutcome = "idle";
  private readonly sessionCatchUp = new Map<string, SessionCatchUpState>();
  private runningCatchUp: Promise<void> | null = null;
  private processing: Promise<void> = Promise.resolve();
  private catchUpFiber: Fiber.RuntimeFiber<void, never> | null = null;
  private started = false;
  private stopped = true;

  /**
   * Stores the shared database handle, service boundary, and local hub.
   */
  constructor(private readonly options: SessionEventFanoutOptions) {
    this.catchUpPollIntervalMs = options.catchUpPollIntervalMs ?? defaultEventFanoutCatchUpPollMs;
    this.eventBatchLimit = options.eventBatchLimit ?? defaultEventFanoutBatchLimit;
    this.notificationQueueLimit =
      options.notificationQueueLimit ?? defaultEventFanoutNotificationQueueLimit;
    this.now = options.now ?? Date.now;
    this.observability = new ModuleObservability({
      ...options.observability,
      moduleName: "SessionEventFanout",
    });
    this.reconnectBaseDelayMs = options.reconnectBaseDelayMs ?? defaultListenerReconnectBaseDelayMs;
    this.reconnectMaxDelayMs = options.reconnectMaxDelayMs ?? defaultListenerReconnectMaxDelayMs;
  }

  /**
   * Returns inspectable fanout state for tests and operators.
   */
  debugInfo(): SessionEventFanoutDebugInfo {
    return {
      broadcastCount: this.broadcastCount,
      catchUpBatchCount: this.catchUpBatchCount,
      catchUpEventCount: this.catchUpEventCount,
      catchUpFailureCount: this.catchUpFailureCount,
      catchUpPollCount: this.catchUpPollCount,
      catchUpPollIntervalMs: this.catchUpPollIntervalMs,
      catchUpRecoveryCount: this.catchUpRecoveryCount,
      coalescedNotificationCount: this.coalescedNotificationCount,
      connected: this.listenerState === sessionEventFanoutListenerState.connected,
      droppedNotificationCount: this.droppedNotificationCount,
      fanoutCursorSessionCount: 0,
      ignoredSelfNotificationCount: this.ignoredSelfNotificationCount,
      invalidNotificationCount: this.invalidNotificationCount,
      lastConnectedAt: this.lastConnectedAt,
      lastDisconnectedAt: this.lastDisconnectedAt,
      lastListenerError: this.lastListenerError,
      lastReconnectDelayMs: this.lastReconnectDelayMs,
      listenerErrorCount: this.listenerErrorCount,
      listenerState: this.listenerState,
      lastCatchUpOutcome: this.lastCatchUpOutcome,
      notificationCount: this.notificationCount,
      pendingNotificationSessionCount: this.pendingNotificationSeqs.size,
      reconnectAttemptCount: this.reconnectAttemptCount,
      reconnectSuccessCount: this.reconnectSuccessCount,
      sessionCursorCount: this.options.hub.sessionCursors().length,
      scheduled: this.catchUpFiber !== null,
      sessionLag: this.readSessionLag(),
    };
  }

  /**
   * Opens a dedicated Postgres client and begins listening for committed event
   * notifications.
   */
  async start(): Promise<void> {
    if (this.started) {
      return;
    }
    this.started = true;
    this.stopped = false;
    if (this.options.listenEnabled === false) {
      this.listenerState = sessionEventFanoutListenerState.disabled;
      this.startCatchUpLoop();
      return;
    }
    await this.connectListener();
    this.startCatchUpLoop();
  }

  /**
   * Stops listening and releases the dedicated Postgres client.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    this.started = false;
    this.cancelReconnectTimer();
    const runningCatchUp = this.runningCatchUp;
    if (this.catchUpFiber) {
      const fiber = this.catchUpFiber;
      this.catchUpFiber = null;
      await Effect.runPromise(Fiber.interrupt(fiber));
    }
    await runningCatchUp;
    await this.processing;
    await this.reconnecting;
    const client = this.detachActiveClient();
    this.listenerState = sessionEventFanoutListenerState.stopped;
    if (client) {
      try {
        await client.query(`UNLISTEN ${sessionEventNotificationChannel}`);
      } finally {
        this.releaseClient(client);
      }
    }
  }

  /**
   * Parses one raw Postgres notification and queues any remote event for local
   * fanout.
   */
  private readonly handlePgNotification = (message: pg.Notification): void => {
    if (this.stopped || message.channel !== sessionEventNotificationChannel) {
      return;
    }
    this.notificationCount += 1;
    const notification = parseSessionEventNotification(message.payload);
    if (!notification) {
      this.invalidNotificationCount += 1;
      return;
    }
    if (notification.sourceId === this.options.service.debugInfo().eventSourceId) {
      this.ignoredSelfNotificationCount += 1;
      return;
    }
    this.enqueueNotification(notification);
  };

  /** Handles listener connection errors without taking down the app server. */
  private readonly handleClientError = (error: Error): void => {
    this.listenerErrorCount += 1;
    this.lastListenerError = toListenerErrorInfo(error);
    this.handleListenerDisconnect(error);
  };

  /** Handles unexpected listener close events as recoverable disconnects. */
  private readonly handleClientEnd = (): void => {
    this.handleListenerDisconnect();
  };

  /**
   * Serializes notification processing so session event order is preserved per
   * process, while coalescing pending notifications per session: one queued
   * marker fetches every committed event a burst of notifications covered, so
   * queue depth is bounded by subscribed sessions instead of event rate. The
   * durable catch-up poll backstops anything a saturated queue drops.
   */
  private enqueueNotification(notification: SessionEventNotification): void {
    if (!this.hasLocalSocket(notification.sessionId)) {
      return;
    }
    const pendingSeq = this.pendingNotificationSeqs.get(notification.sessionId);
    if (pendingSeq !== undefined) {
      if (notification.seq < pendingSeq) {
        this.pendingNotificationSeqs.set(notification.sessionId, notification.seq);
      }
      this.coalescedNotificationCount += 1;
      return;
    }
    if (this.pendingNotificationSeqs.size >= this.notificationQueueLimit) {
      this.droppedNotificationCount += 1;
      if (!this.notificationQueueSaturated) {
        this.notificationQueueSaturated = true;
        logNotificationQueueSaturated({
          pendingSessionCount: this.pendingNotificationSeqs.size,
          queueLimit: this.notificationQueueLimit,
        });
      }
      return;
    }
    this.pendingNotificationSeqs.set(notification.sessionId, notification.seq);
    this.processing = this.processing
      .then(() => {
        const fromSeq = this.pendingNotificationSeqs.get(notification.sessionId);
        this.pendingNotificationSeqs.delete(notification.sessionId);
        if (
          this.notificationQueueSaturated &&
          this.pendingNotificationSeqs.size <= this.notificationQueueLimit / 2
        ) {
          // Hysteresis: one saturation log per episode, re-armed only after
          // the queue has drained back to half its capacity.
          this.notificationQueueSaturated = false;
        }
        if (fromSeq === undefined) {
          return;
        }
        return this.broadcastNotifiedSession(notification.sessionId, fromSeq);
      })
      .catch((error: unknown) => {
        console.error(error);
      });
  }

  /**
   * Starts the supervised catch-up polling loop when polling is enabled.
   */
  private startCatchUpLoop(): void {
    if (this.stopped || this.catchUpPollIntervalMs <= 0 || this.catchUpFiber) {
      return;
    }
    this.catchUpFiber = Effect.runFork(this.catchUpLoop());
  }

  /**
   * Polls durable events until the fanout is stopped or interrupted.
   */
  private catchUpLoop(): Effect.Effect<void, never> {
    return Effect.gen(this, function* () {
      while (!this.stopped) {
        yield* sleepUnrefEffect(this.catchUpPollIntervalMs);
        if (this.stopped) {
          return;
        }
        this.runningCatchUp = this.catchUp();
        yield* Effect.promise(() => this.runningCatchUp ?? Promise.resolve());
        this.runningCatchUp = null;
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          this.catchUpFiber = null;
          this.runningCatchUp = null;
        }),
      ),
    );
  }

  /**
   * Fetches missed durable events for sessions with local sockets.
   */
  private async catchUp(): Promise<void> {
    this.catchUpPollCount += 1;
    const cursors = this.options.hub.sessionCursors();
    this.synchronizeSessionCatchUpState(cursors.map((cursor) => cursor.sessionId));
    if (cursors.length === 0) {
      this.nextRoundStartIndex = 0;
      this.lastCatchUpOutcome = "idle";
      return;
    }
    const startIndex = this.nextRoundStartIndex % cursors.length;
    this.nextRoundStartIndex = (startIndex + 1) % cursors.length;
    const orderedCursors = [...cursors.slice(startIndex), ...cursors.slice(0, startIndex)];
    for (const cursor of orderedCursors) {
      if (this.hasLocalSocket(cursor.sessionId)) {
        await this.catchUpSession(cursor.sessionId, cursor.lastDeliveredSeq);
      }
    }
  }

  /** Processes at most one bounded batch for one subscribed session. */
  private async catchUpSession(sessionId: string, afterSeq: number): Promise<void> {
    const state = this.sessionCatchUp.get(sessionId);
    if (!state) {
      return;
    }
    if (state.lagStartedAt === null) {
      state.lagStartedAt = this.now();
    }
    state.outcome = "events_pending";
    this.lastCatchUpOutcome = "events_pending";
    try {
      const events = await this.observability.traceBoundary(
        "catchUpBatch",
        {
          afterSeq,
          batchLimit: this.eventBatchLimit,
          replicaId: this.options.service.debugInfo().eventSourceId,
          sessionHash: state.sessionHash,
        },
        () =>
          Effect.runPromise(
            this.options.service.listEvents(sessionId, afterSeq, {
              limit: this.eventBatchLimit,
            }),
          ),
        (batch) => ({
          batchSize: batch.length,
          outcome: batch.length < this.eventBatchLimit ? "caught_up" : "events_pending",
          sequenceEnd: batch.at(-1)?.seq ?? afterSeq,
          sequenceStart: batch[0]?.seq ?? afterSeq,
        }),
        () => ({ outcome: "failed" }),
      );
      if (events.length > 0) {
        this.catchUpBatchCount += 1;
        this.catchUpEventCount += events.length;
        for (const event of events) {
          this.broadcastEvent(event);
        }
      }
      const outcome = events.length < this.eventBatchLimit ? "caught_up" : "events_pending";
      if (state.recoveringFromFailure) {
        this.catchUpRecoveryCount += 1;
        state.recoveringFromFailure = false;
      }
      if (outcome === "caught_up") {
        state.lagStartedAt = null;
      }
      state.outcome = outcome;
      this.lastCatchUpOutcome = outcome;
    } catch (error) {
      this.catchUpFailureCount += 1;
      if (state.lagStartedAt === null) {
        state.lagStartedAt = this.now();
      }
      state.outcome = "failed";
      state.recoveringFromFailure = true;
      this.lastCatchUpOutcome = "failed";
      console.error(error);
    }
  }

  /** Keeps lag state aligned with the current local subscription set. */
  private synchronizeSessionCatchUpState(sessionIds: readonly string[]): void {
    const subscribed = new Set(sessionIds);
    for (const sessionId of this.sessionCatchUp.keys()) {
      if (!subscribed.has(sessionId)) {
        this.sessionCatchUp.delete(sessionId);
      }
    }
    for (const sessionId of sessionIds) {
      if (!this.sessionCatchUp.has(sessionId)) {
        this.sessionCatchUp.set(sessionId, {
          lagStartedAt: null,
          outcome: "idle",
          recoveringFromFailure: false,
          sessionHash: hashSessionId(sessionId),
        });
      }
    }
  }

  /** Projects bounded, payload-free per-session lag diagnostics. */
  private readSessionLag(): readonly SessionEventFanoutSessionLagInfo[] {
    this.synchronizeSessionCatchUpState(
      this.options.hub.sessionCursors().map((cursor) => cursor.sessionId),
    );
    const now = this.now();
    return [...this.sessionCatchUp.values()].map((state) => ({
      lagAgeMs:
        state.lagStartedAt === null
          ? 0
          : Math.min(Math.max(0, now - state.lagStartedAt), Number.MAX_SAFE_INTEGER),
      outcome: state.outcome,
      sessionHash: state.sessionHash,
    }));
  }

  /**
   * Fetches the committed events one coalesced notification marker covers and
   * forwards them to the local hub in one bounded batch. The hub owns
   * per-socket delivery dedupe and cursor completeness; events beyond the
   * batch limit are picked up by the durable catch-up poll.
   */
  private async broadcastNotifiedSession(sessionId: string, fromSeq: number): Promise<void> {
    if (!this.hasLocalSocket(sessionId)) {
      return;
    }
    const events = await Effect.runPromise(
      this.options.service.listEvents(sessionId, fromSeq - 1, { limit: this.eventBatchLimit }),
    );
    for (const event of events) {
      this.broadcastEvent(event);
    }
  }

  /**
   * Broadcasts one durable event to the local hub.
   */
  private broadcastEvent(event: SessionEvent): void {
    this.options.hub.broadcast(event);
    this.broadcastCount += 1;
  }

  /** Returns whether this replica still has sockets subscribed to a session. */
  private hasLocalSocket(sessionId: string): boolean {
    return this.options.hub.sessionCursors().some((cursor) => cursor.sessionId === sessionId);
  }

  /** Opens and subscribes a dedicated LISTEN client. */
  private async connectListener(): Promise<void> {
    this.listenerState = sessionEventFanoutListenerState.connecting;
    const client = await this.options.database.pool.connect();
    this.client = client;
    client.on("notification", this.handlePgNotification);
    client.on("error", this.handleClientError);
    client.on("end", this.handleClientEnd);
    try {
      await client.query(`LISTEN ${sessionEventNotificationChannel}`);
      if (this.stopped) {
        const detachedClient = this.detachActiveClient();
        if (detachedClient) {
          this.releaseClient(detachedClient);
        }
        return;
      }
      this.listenerState = sessionEventFanoutListenerState.connected;
      this.lastConnectedAt = new Date().toISOString();
    } catch (error) {
      const detachedClient = this.detachActiveClient();
      if (detachedClient) {
        this.releaseClient(detachedClient);
      }
      this.listenerState = sessionEventFanoutListenerState.disconnected;
      this.started = false;
      throw error;
    }
  }

  /** Detaches all owned LISTEN handlers and returns the active client once. */
  private detachActiveClient(): pg.PoolClient | null {
    const client = this.client;
    if (!client) {
      return null;
    }
    this.client = null;
    client.off("notification", this.handlePgNotification);
    client.off("error", this.handleClientError);
    client.off("end", this.handleClientEnd);
    return client;
  }

  /** Releases a pg client while keeping async listener handlers non-throwing. */
  private releaseClient(client: pg.PoolClient): void {
    try {
      client.release();
    } catch (error) {
      console.error(error);
    }
  }

  /** Marks the current listener down, releases it once, and schedules recovery. */
  private handleListenerDisconnect(error?: Error): void {
    if (this.stopped) {
      return;
    }
    if (error) {
      console.error(error);
    }
    const client = this.detachActiveClient();
    if (!client) {
      return;
    }
    this.releaseClient(client);
    this.listenerState = sessionEventFanoutListenerState.disconnected;
    this.lastDisconnectedAt = new Date().toISOString();
    this.scheduleReconnect();
  }

  /** Schedules one bounded-delay reconnect attempt while fanout is running. */
  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer || this.reconnecting) {
      return;
    }
    const delayMs = Math.min(
      this.reconnectMaxDelayMs,
      this.reconnectBaseDelayMs * 2 ** this.reconnectAttemptCount,
    );
    this.lastReconnectDelayMs = delayMs;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.reconnecting = this.reconnectListener();
      this.reconnecting.finally(() => {
        this.reconnecting = null;
      });
    }, delayMs);
    this.reconnectTimer.unref?.();
  }

  /** Runs one reconnect attempt and reschedules when the replacement fails. */
  private async reconnectListener(): Promise<void> {
    if (this.stopped) {
      return;
    }
    this.reconnectAttemptCount += 1;
    try {
      await this.connectListener();
      this.reconnectSuccessCount += 1;
    } catch (error) {
      this.lastListenerError = toListenerErrorInfo(error);
      console.error(error);
      if (!this.stopped) {
        this.scheduleReconnect();
      }
    }
  }

  /** Cancels a pending reconnect timer during shutdown. */
  private cancelReconnectTimer(): void {
    if (!this.reconnectTimer) {
      return;
    }
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }
}

/** Hashes a session identifier for correlation without exposing its raw value. */
function hashSessionId(sessionId: string): string {
  return createHash("sha256").update(sessionId).digest("hex").slice(0, 16);
}

/** Emits one structured, payload-free saturation log line per episode. */
function logNotificationQueueSaturated(details: {
  readonly pendingSessionCount: number;
  readonly queueLimit: number;
}): void {
  process.stderr.write(
    `${JSON.stringify({
      details,
      event: "session_event_fanout.notification_queue_saturated",
    })}\n`,
  );
}

/** Converts unknown listener failures into stable diagnostics. */
function toListenerErrorInfo(error: unknown): SessionEventFanoutListenerErrorInfo {
  if (error instanceof Error) {
    return { message: error.message, name: error.name };
  }
  return { message: String(error), name: "Error" };
}
