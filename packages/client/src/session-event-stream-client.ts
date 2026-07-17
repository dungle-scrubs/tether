import { Effect, Either } from "effect";
import WebSocket from "ws";

import { resolveServiceAuthToken } from "./auth-token.js";
import { sleepUnrefEffect } from "./effect-timing.js";
import { ModuleObservability, readModuleObservabilityOptions } from "./observability.js";
import { parseWebSocketServerEnvelope, webSocketOperation } from "./protocol.js";
import { SerialEventDelivery, type SerialEventDeliveryOutcome } from "./serial-event-delivery.js";
import type { SessionEvent } from "./types.js";

const defaultReconnectBaseDelayMs = 100;
const defaultReconnectMaxDelayMs = 2_000;
const observerHandlerTimeoutMs = 30_000;
const observerMaxQueueSize = 2_000;

/** Factory for observer WebSocket connections, injectable by tests and hosts. */
export type SessionEventStreamWebSocketFactory = (url: string) => WebSocket;

/** Connection settings for a passive session event observer stream. */
export interface SessionEventStreamClientConfig {
  /** Last observed event sequence to resume after. */
  readonly afterSeq: number;
  /** Bearer token sent to Tether; falls back to SERVICE_AUTH_TOKEN/TETHER_AUTH_TOKEN when omitted. */
  readonly authToken?: string | null;
  /** Optional reconnect backoff settings for long-running observers. */
  readonly reconnect?: {
    /** Initial delay before reconnecting after an unexpected disconnect. */
    readonly baseDelayMs?: number;
    /** Maximum reconnect delay after repeated failures. */
    readonly maxDelayMs?: number;
  };
  /** Base URL of the Tether HTTP service. */
  readonly serviceUrl: string;
  /** Session whose durable event stream should be observed. */
  readonly sessionId: string;
  /** WebSocket implementation factory, injected by tests and non-Node hosts. */
  readonly webSocketFactory?: SessionEventStreamWebSocketFactory;
}

/** Runtime diagnostics for one passive session event observer. */
export interface SessionEventStreamClientDebugInfo {
  /** Number of WebSocket connections opened. */
  readonly connectCount: number;
  /** Number of durable events delivered by the stream. */
  readonly eventCount: number;
  /** Number of registered event handlers. */
  readonly eventHandlerCount: number;
  /** Highest event sequence whose handlers have all resolved; the durable resume cursor. */
  readonly lastObservedSeq: number;
  /** Number of failed reconnect attempts since startup. */
  readonly reconnectFailureCount: number;
  /** Number of successful reconnects since startup. */
  readonly reconnectSuccessCount: number;
  /** Number of replay completion markers observed. */
  readonly replayCompleteCount: number;
  /** Underlying WebSocket ready state. */
  readonly socketReadyState: number;
  /** Whether the client has been intentionally closed. */
  readonly stopped: boolean;
}

/**
 * Observer-only stream client for replaying and following durable session
 * events. Use this for passive readers such as bridges, approval observers, and
 * dashboards; use `ParticipantRuntimeClient` when the caller must publish
 * participant events, claim tasks, refresh claims, or complete task work.
 */
export class SessionEventStreamClient {
  private connectionGeneration = 0;
  private delivery: SerialEventDelivery<SessionEvent>;
  private readonly deliveryUnsubscribers = new Map<
    (event: SessionEvent) => void | Promise<void>,
    () => void
  >();
  private readonly eventHandlers = new Set<(event: SessionEvent) => void | Promise<void>>();
  private readonly errorBacklog: Error[] = [];
  private readonly errorHandlers = new Set<(error: Error) => void>();
  private readonly observability = new ModuleObservability(
    readModuleObservabilityOptions("SessionEventStreamClient"),
  );
  private closePromise: Promise<void> = Promise.resolve();
  private connectCount = 0;
  private eventCount = 0;
  /** Highest sequence whose handlers have all resolved; the durable resume cursor. */
  private lastObservedSeq: number;
  private reconnectFailureCount = 0;
  private reconnectPromise: Promise<void> | null = null;
  private reconnectSuccessCount = 0;
  private replayComplete: Promise<void> = Promise.resolve();
  private replayCompleteCount = 0;
  private replayCompleteSettled = true;
  private rejectReplayComplete: ((error: Error) => void) | null = null;
  private resolveReplayComplete: (() => void) | null = null;
  private socket: WebSocket | null = null;
  private stopped = false;
  private readonly webSocketFactory: SessionEventStreamWebSocketFactory;

  /** Creates an observer around the supplied stream configuration. */
  private constructor(private readonly config: SessionEventStreamClientConfig) {
    this.delivery = this.createDelivery(config.afterSeq, this.connectionGeneration);
    this.lastObservedSeq = config.afterSeq;
    this.webSocketFactory = config.webSocketFactory ?? ((url) => new WebSocket(url));
  }

  /** Opens a passive observer stream without participant task authority. */
  static async connect(config: SessionEventStreamClientConfig): Promise<SessionEventStreamClient> {
    const client = new SessionEventStreamClient(config);
    await client.open(config.afterSeq);
    return client;
  }

  /** Requests a graceful WebSocket close and stops reconnect attempts. */
  close(): void {
    this.stopped = true;
    this.socket?.close();
  }

  /** Returns stream cursor, connection, handler, and replay diagnostics. */
  debugInfo(): SessionEventStreamClientDebugInfo {
    return {
      connectCount: this.connectCount,
      eventCount: this.eventCount,
      eventHandlerCount: this.eventHandlers.size,
      lastObservedSeq: this.lastObservedSeq,
      reconnectFailureCount: this.reconnectFailureCount,
      reconnectSuccessCount: this.reconnectSuccessCount,
      replayCompleteCount: this.replayCompleteCount,
      socketReadyState: this.socket?.readyState ?? WebSocket.CLOSED,
      stopped: this.stopped,
    };
  }

  /** Registers an error callback and returns an unsubscribe function. */
  onError(handler: (error: Error) => void): () => void {
    this.errorHandlers.add(handler);
    for (const error of this.errorBacklog.splice(0)) {
      handler(error);
    }
    return () => {
      this.errorHandlers.delete(handler);
    };
  }

  /** Registers an event callback and resumes serial delivery of any backlog. */
  onEvent(handler: (event: SessionEvent) => void | Promise<void>): () => void {
    this.eventHandlers.add(handler);
    this.deliveryUnsubscribers.set(handler, this.delivery.onEvent(handler));
    return () => {
      this.eventHandlers.delete(handler);
      this.deliveryUnsubscribers.get(handler)?.();
      this.deliveryUnsubscribers.delete(handler);
    };
  }

  /** Reopens the stream from the highest observed event sequence. */
  async reconnect(): Promise<SessionEventStreamClient> {
    this.socket?.close();
    await this.waitForClose().catch(() => undefined);
    await this.requestReconnect(0);
    return this;
  }

  /** Resolves when the current socket closes. */
  async waitForClose(): Promise<void> {
    await this.closePromise;
  }

  /** Resolves once the current stream replay has completed. */
  async waitForReplayComplete(): Promise<void> {
    await this.replayComplete;
  }

  /** Opens a WebSocket stream from the requested event cursor. */
  private async open(afterSeq: number): Promise<void> {
    await this.observability.traceBoundary(
      "open",
      { afterSeq, sessionId: this.config.sessionId },
      async () => {
        const generation = this.connectionGeneration + 1;
        this.connectionGeneration = generation;
        this.replaceDelivery(afterSeq, generation);
        this.replayCompleteSettled = false;
        const replayComplete = new Promise<void>((resolve, reject) => {
          this.rejectReplayComplete = reject;
          this.resolveReplayComplete = resolve;
        });
        replayComplete.catch(() => undefined);
        this.replayComplete = replayComplete;
        const socket = this.webSocketFactory(
          buildSessionEventStreamUrl({
            ...this.config,
            afterSeq,
          }),
        );
        this.socket = socket;
        this.connectCount += 1;
        this.closePromise = new Promise((resolve) => {
          socket.once("close", () => {
            if (this.connectionGeneration === generation && this.socket === socket) {
              this.socket = null;
              this.settleReplayCompleteError(new Error("WebSocket closed before replay completed"));
            }
            resolve();
          });
        });
        socket.on("error", (error) => {
          if (this.connectionGeneration === generation) {
            this.settleReplayCompleteError(error);
            this.emitError(error);
          }
        });
        socket.on("message", (data) => {
          this.handleMessage(data, generation);
        });
        await new Promise<void>((resolve, reject) => {
          socket.once("open", resolve);
          socket.once("error", reject);
        });
      },
    );
  }

  /** Reopens the stream with bounded retry backoff. */
  private async reconnectWithBackoff(initialDelayMs: number | null = null): Promise<void> {
    await Effect.runPromise(this.buildReconnectWithBackoff(initialDelayMs));
  }

  /** Coalesces caller-triggered and automatic observer reconnect ownership. */
  private requestReconnect(initialDelayMs: number | null = null): Promise<void> {
    if (this.reconnectPromise) {
      return this.reconnectPromise;
    }
    const reconnect = this.reconnectWithBackoff(initialDelayMs);
    this.reconnectPromise = reconnect;
    void reconnect.finally(() => {
      if (this.reconnectPromise === reconnect) {
        this.reconnectPromise = null;
      }
    });
    return reconnect;
  }

  /** Builds the bounded reconnect loop as an Effect program. */
  private buildReconnectWithBackoff(
    initialDelayMs: number | null = null,
  ): Effect.Effect<void, never> {
    return Effect.gen(this, function* () {
      let attempt = 0;
      let delayMs = initialDelayMs ?? observerReconnectDelayMs(attempt, this.config);
      while (!this.stopped) {
        if (delayMs > 0) {
          yield* sleepUnrefEffect(delayMs);
        }
        const opened = yield* Effect.either(
          Effect.tryPromise({
            catch: (error) => error,
            try: () => this.open(this.lastObservedSeq),
          }),
        );
        if (Either.isRight(opened)) {
          this.reconnectSuccessCount += 1;
          return;
        }
        const error = opened.left;
        this.reconnectFailureCount += 1;
        this.emitError(error instanceof Error ? error : new Error("Observer reconnect failed"));
        attempt += 1;
        delayMs = observerReconnectDelayMs(attempt, this.config);
      }
    });
  }

  /** Parses one server envelope and enqueues it for serial, in-order delivery. */
  private handleMessage(data: WebSocket.RawData, generation: number): void {
    if (generation !== this.connectionGeneration) {
      return;
    }
    try {
      const envelope = parseWebSocketServerEnvelope(JSON.parse(String(data)) as unknown);
      if (!envelope) {
        this.delivery.rejectInvalidEnvelope();
        return;
      }
      if (envelope.op === webSocketOperation.event) {
        this.eventCount += 1;
        this.delivery.enqueueEvent(envelope.event);
        return;
      }
      if (envelope.op === webSocketOperation.replayComplete) {
        this.replayCompleteCount += 1;
        this.delivery.enqueueReplayComplete();
        return;
      }
      if (envelope.op === webSocketOperation.error) {
        const error = new Error(envelope.error);
        this.settleReplayCompleteError(error);
        this.emitError(error);
      }
    } catch {
      this.delivery.rejectInvalidEnvelope();
    }
  }

  /** Creates a generation-owned shared delivery state machine. */
  private createDelivery(afterSeq: number, generation: number): SerialEventDelivery<SessionEvent> {
    return new SerialEventDelivery<SessionEvent>({
      handlerTimeoutMs: observerHandlerTimeoutMs,
      initialSeq: afterSeq,
      maxQueueSize: observerMaxQueueSize,
      onOutcome: (outcome) => this.handleDeliveryOutcome(outcome, generation),
    });
  }

  /** Replaces generation-local delivery while preserving public subscriptions. */
  private replaceDelivery(afterSeq: number, generation: number): void {
    for (const unsubscribe of this.deliveryUnsubscribers.values()) {
      unsubscribe();
    }
    this.deliveryUnsubscribers.clear();
    this.delivery = this.createDelivery(afterSeq, generation);
    for (const handler of this.eventHandlers) {
      this.deliveryUnsubscribers.set(handler, this.delivery.onEvent(handler));
    }
  }

  /** Maps shared delivery outcomes to observer cursor, replay, and recovery policy. */
  private handleDeliveryOutcome(
    outcome: SerialEventDeliveryOutcome<SessionEvent>,
    generation: number,
  ): void {
    if (generation !== this.connectionGeneration) {
      return;
    }
    switch (outcome.kind) {
      case "delivery-queue-overflow":
        this.emitError(new Error("Session event delivery queue exceeded its configured limit"));
        this.recoverFromDeliveryFailure(generation);
        return;
      case "duplicate-ignored":
        return;
      case "event-handled":
        this.lastObservedSeq = outcome.event.seq;
        return;
      case "handler-failed":
        this.emitError(
          outcome.cause instanceof Error
            ? outcome.cause
            : new Error("Session event handler failed"),
        );
        this.recoverFromDeliveryFailure(generation);
        return;
      case "handler-timeout":
        this.emitError(new Error("Session event handler timed out"));
        this.recoverFromDeliveryFailure(generation);
        return;
      case "invalid-server-envelope":
        this.emitError(new Error("Unsupported WebSocket envelope"));
        this.recoverFromDeliveryFailure(generation);
        return;
      case "non-contiguous-event":
        this.emitError(new Error("Session event stream contained a sequence gap"));
        this.recoverFromDeliveryFailure(generation);
        return;
      case "replay-complete":
        this.settleReplayComplete();
        return;
    }
  }

  /**
   * Recovers after a handler rejection by discarding un-acknowledged events and
   * reconnecting from the last successfully handled sequence, so the server
   * replays the failed event rather than the stream advancing past it.
   */
  private recoverFromDeliveryFailure(generation: number): void {
    if (this.stopped || generation !== this.connectionGeneration) {
      return;
    }
    const socket = this.socket;
    this.settleReplayCompleteError(new Error("Observer delivery failed before replay completed"));
    this.socket = null;
    socket?.close();
    void this.requestReconnect();
  }

  /** Routes operational failures to registered error handlers. */
  private emitError(error: Error): void {
    if (this.errorHandlers.size === 0) {
      this.errorBacklog.push(error);
      return;
    }
    for (const handler of this.errorHandlers) {
      handler(error);
    }
  }

  /** Resolves the current replay wait once. */
  private settleReplayComplete(): void {
    if (this.replayCompleteSettled) {
      return;
    }
    this.replayCompleteSettled = true;
    this.resolveReplayComplete?.();
    this.resolveReplayComplete = null;
    this.rejectReplayComplete = null;
  }

  /** Rejects the current replay wait once when the stream fails before replay completion. */
  private settleReplayCompleteError(error: Error): void {
    if (this.replayCompleteSettled) {
      return;
    }
    this.replayCompleteSettled = true;
    this.rejectReplayComplete?.(error);
    this.resolveReplayComplete = null;
    this.rejectReplayComplete = null;
  }
}

/**
 * Runtime-kind value that selects the server's passive full-event observer
 * mode: the connection receives the durable event stream (replay + live) but
 * does not register a durable participant or acquire a control lease. Keep this
 * in sync with `classifyHostPresenceStream` in the Tether gateway.
 */
export const sessionEventObserverRuntimeKind = "observer";

/** Builds the observer WebSocket URL without participant or task authority. */
export function buildSessionEventStreamUrl(config: SessionEventStreamClientConfig): string {
  const url = new URL(
    `/sessions/${encodeURIComponent(config.sessionId)}/stream`,
    config.serviceUrl,
  );
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("after", String(config.afterSeq));
  // Opt into the passive full-event observer mode so the server delivers the
  // durable event stream without treating this connection as a control
  // participant. A passive reader never acquires a control lease, so its
  // reconnects cannot collide with the runtime's own control channel.
  url.searchParams.set("runtimeKind", sessionEventObserverRuntimeKind);
  const authToken = resolveServiceAuthToken(config.authToken);
  if (authToken) {
    url.searchParams.set("access_token", authToken);
  }
  return url.toString();
}

/** Computes bounded exponential reconnect backoff from observer configuration. */
function observerReconnectDelayMs(attempt: number, config: SessionEventStreamClientConfig): number {
  const baseDelayMs = config.reconnect?.baseDelayMs ?? defaultReconnectBaseDelayMs;
  const maxDelayMs = config.reconnect?.maxDelayMs ?? defaultReconnectMaxDelayMs;
  return Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
}
