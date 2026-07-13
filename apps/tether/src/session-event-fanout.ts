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
  readonly catchUpPollCount: number;
  readonly catchUpPollIntervalMs: number;
  readonly connected: boolean;
  readonly fanoutCursorSessionCount: number;
  readonly ignoredSelfNotificationCount: number;
  readonly invalidNotificationCount: number;
  readonly lastConnectedAt: string | null;
  readonly lastDisconnectedAt: string | null;
  readonly lastListenerError: SessionEventFanoutListenerErrorInfo | null;
  readonly lastReconnectDelayMs: number | null;
  readonly listenerErrorCount: number;
  readonly listenerState: SessionEventFanoutListenerState;
  readonly notificationCount: number;
  readonly reconnectAttemptCount: number;
  readonly reconnectSuccessCount: number;
  readonly sessionCursorCount: number;
  readonly scheduled: boolean;
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
  readonly reconnectBaseDelayMs?: number;
  readonly reconnectMaxDelayMs?: number;
  readonly service: SessionEventFanoutSessionService;
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
export const defaultEventFanoutBatchLimit = 500;
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
  private catchUpPollCount = 0;
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
  private ignoredSelfNotificationCount = 0;
  private invalidNotificationCount = 0;
  private notificationCount = 0;
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
      catchUpPollCount: this.catchUpPollCount,
      catchUpPollIntervalMs: this.catchUpPollIntervalMs,
      connected: this.listenerState === sessionEventFanoutListenerState.connected,
      fanoutCursorSessionCount: 0,
      ignoredSelfNotificationCount: this.ignoredSelfNotificationCount,
      invalidNotificationCount: this.invalidNotificationCount,
      lastConnectedAt: this.lastConnectedAt,
      lastDisconnectedAt: this.lastDisconnectedAt,
      lastListenerError: this.lastListenerError,
      lastReconnectDelayMs: this.lastReconnectDelayMs,
      listenerErrorCount: this.listenerErrorCount,
      listenerState: this.listenerState,
      notificationCount: this.notificationCount,
      reconnectAttemptCount: this.reconnectAttemptCount,
      reconnectSuccessCount: this.reconnectSuccessCount,
      sessionCursorCount: this.options.hub.sessionCursors().length,
      scheduled: this.catchUpFiber !== null,
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
   * process.
   */
  private enqueueNotification(notification: SessionEventNotification): void {
    this.processing = this.processing
      .then(() => this.broadcastNotification(notification))
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
    try {
      this.catchUpPollCount += 1;
      for (const cursor of this.options.hub.sessionCursors()) {
        let afterSeq = cursor.lastDeliveredSeq;
        while (this.hasLocalSocket(cursor.sessionId)) {
          const events = await Effect.runPromise(
            this.options.service.listEvents(cursor.sessionId, afterSeq, {
              limit: this.eventBatchLimit,
            }),
          );
          if (events.length === 0) {
            break;
          }
          this.catchUpBatchCount += 1;
          this.catchUpEventCount += events.length;
          for (const event of events) {
            this.broadcastEvent(event);
            afterSeq = event.seq;
          }
          if (events.length < this.eventBatchLimit) {
            break;
          }
        }
      }
    } catch (error) {
      console.error(error);
    }
  }

  /**
   * Fetches the notified committed event from the shared database and forwards
   * it to the local hub. The hub owns per-socket delivery dedupe and cursor
   * completeness.
   */
  private async broadcastNotification(notification: SessionEventNotification): Promise<void> {
    if (!this.hasLocalSocket(notification.sessionId)) {
      return;
    }
    const events = await Effect.runPromise(
      this.options.service.listEvents(notification.sessionId, notification.seq - 1, { limit: 1 }),
    );
    const event = events.find((candidate) => candidate.seq === notification.seq);
    if (event) {
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

/** Converts unknown listener failures into stable diagnostics. */
function toListenerErrorInfo(error: unknown): SessionEventFanoutListenerErrorInfo {
  if (error instanceof Error) {
    return { message: error.message, name: error.name };
  }
  return { message: String(error), name: "Error" };
}
