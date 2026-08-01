import {
  boundedExponentialRetryDelayMs,
  classifyWebSocketServerEnvelope,
  SerialEventDelivery,
  type SerialEventDeliveryOutcome,
  type SessionEvent,
  webSocketOperation,
} from "@dungle-scrubs/tether-protocol";

import { BrowserSessionStreamError } from "./errors.js";

const defaultHandlerTimeoutMs = 30_000;
const defaultMaxQueueBytes = 16 * 1024 * 1024;
const defaultMaxQueueSize = 2_000;
const defaultReconnectBaseDelayMs = 100;
const defaultReconnectMaxAttempts = 5;
const defaultReconnectMaxDelayMs = 2_000;

/** Browser session stream lifecycle states. */
export type BrowserSessionStreamState =
  | "closed"
  | "connecting"
  | "live"
  | "paused"
  | "reconnecting"
  | "replaying";

/** Finite retained-delivery limits for browser event streams. */
export interface BrowserSessionDeliveryPolicy {
  /** Deadline for one projection handler invocation. */
  readonly handlerTimeoutMs?: number;
  /** Maximum retained raw WebSocket frame bytes. */
  readonly maxQueueBytes?: number;
  /** Maximum retained events and replay markers. */
  readonly maxQueueSize?: number;
}

/** Reconnect limits for browser event streams. */
export interface BrowserSessionReconnectPolicy {
  /** Initial delay before the first reconnect. */
  readonly baseDelayMs?: number;
  /** Maximum consecutive reconnect attempts before replay succeeds. */
  readonly maxAttempts?: number;
  /** Maximum reconnect delay. */
  readonly maxDelayMs?: number;
}

/** Point-in-time diagnostics for one browser event stream. */
export interface BrowserSessionStreamDebugInfo {
  /** Event sequence currently awaiting its projection handler. */
  readonly activeDeliverySeq: number | null;
  /** Number of retained WebSocket frame bytes. */
  readonly deliveryQueueBytes: number;
  /** Number of retained events and replay markers. */
  readonly deliveryQueueSize: number;
  /** Number of durable events whose handler completed. */
  readonly eventCount: number;
  /** Highest durable sequence whose handler completed. */
  readonly lastHandledSeq: number;
  /** Most recent stable error reason. */
  readonly lastErrorReason: string | null;
  /** Number of successful reconnect socket opens. */
  readonly reconnectCount: number;
  /** Whether the active connection completed replay. */
  readonly replayComplete: boolean;
  /** Current stream lifecycle state. */
  readonly state: BrowserSessionStreamState;
  /** Number of one-time WebSocket tickets requested. */
  readonly ticketCount: number;
}

/** Construction input for one browser session stream. */
export interface BrowserSessionStreamInput {
  /** Durable cursor from the initial snapshot or prior projection. */
  readonly afterSeq: number;
  /** Finite retained-delivery limits. */
  readonly delivery?: BrowserSessionDeliveryPolicy;
  /** Handles one event before the cursor advances and observes timeout cancellation. */
  readonly onEvent: (event: SessionEvent, signal: AbortSignal) => void | Promise<void>;
  /** Receives browser-safe stream failures. */
  readonly onError?: (error: BrowserSessionStreamError) => void;
  /** Receives stream state transitions. */
  readonly onStateChange?: (
    state: BrowserSessionStreamState,
    debug: BrowserSessionStreamDebugInfo,
  ) => void;
  /** Creates one fresh, single-use WebSocket ticket. */
  readonly requestTicket: () => Promise<string>;
  /** Finite reconnect policy. */
  readonly reconnect?: BrowserSessionReconnectPolicy;
  /** Absolute HTTP(S) service URL. */
  readonly serviceUrl: string;
  /** Allowed session id. */
  readonly sessionId: string;
  /** Browser-native WebSocket constructor. */
  readonly webSocketConstructor: typeof WebSocket;
}

/** Browser-only durable event stream with bounded, awaited projection delivery. */
export class BrowserSessionStream {
  private activeGeneration = 0;
  private delivery: SerialEventDelivery<SessionEvent>;
  private eventCount = 0;
  private lastErrorReason: string | null = null;
  private lastHandledSeq: number;
  private reconnectAttempt = 0;
  private reconnectCount = 0;
  private replay = createReplayDeferred();
  private replayComplete = false;
  private socket: WebSocket | null = null;
  private state: BrowserSessionStreamState = "closed";
  private stopped = false;
  private ticketCount = 0;

  private constructor(private readonly input: BrowserSessionStreamInput) {
    validateReconnectPolicy(input.reconnect);
    this.lastHandledSeq = input.afterSeq;
    this.delivery = this.createDelivery(input.afterSeq);
  }

  /** Opens the initial ticket-authenticated stream. */
  static async connect(input: BrowserSessionStreamInput): Promise<BrowserSessionStream> {
    const stream = new BrowserSessionStream(input);
    await stream.open(false);
    return stream;
  }

  /** Intentionally closes the stream and stops reconnect attempts. */
  close(): void {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    this.activeGeneration += 1;
    this.delivery.stop();
    this.setState("closed");
    const socket = this.socket;
    this.socket = null;
    socket?.close();
    this.replay.reject(
      new BrowserSessionStreamError({
        lastHandledSeq: this.lastHandledSeq,
        reason: "stream_closed",
      }),
    );
  }

  /** Returns cursor, queue, replay, ticket, reconnect, and failure diagnostics. */
  debugInfo(): BrowserSessionStreamDebugInfo {
    const delivery = this.delivery.debugInfo();
    return {
      activeDeliverySeq: delivery.activeDeliverySeq,
      deliveryQueueBytes: delivery.queueBytes,
      deliveryQueueSize: delivery.queueSize,
      eventCount: this.eventCount,
      lastHandledSeq: this.lastHandledSeq,
      lastErrorReason: this.lastErrorReason,
      reconnectCount: this.reconnectCount,
      replayComplete: this.replayComplete,
      state: this.state,
      ticketCount: this.ticketCount,
    };
  }

  /** Resolves after replay and every replayed event handler have completed. */
  async waitForReplayComplete(): Promise<void> {
    await this.replay.promise;
  }

  /** Creates one generation-owned bounded delivery machine. */
  private createDelivery(initialSeq: number): SerialEventDelivery<SessionEvent> {
    let delivery: SerialEventDelivery<SessionEvent>;
    delivery = new SerialEventDelivery<SessionEvent>({
      handlerTimeoutMs: this.input.delivery?.handlerTimeoutMs ?? defaultHandlerTimeoutMs,
      initialSeq,
      maxQueueBytes: this.input.delivery?.maxQueueBytes ?? defaultMaxQueueBytes,
      maxQueueSize: this.input.delivery?.maxQueueSize ?? defaultMaxQueueSize,
      onOutcome: (outcome) => this.handleDeliveryOutcome(delivery, outcome),
    });
    delivery.onEvent(async (event, signal) => this.input.onEvent(event, signal));
    return delivery;
  }

  /** Opens one connection using a fresh single-use ticket. */
  private async open(reconnect: boolean): Promise<void> {
    if (this.cannotContinue()) {
      return;
    }
    this.setState(reconnect ? "reconnecting" : "connecting");
    const generation = this.activeGeneration + 1;
    this.activeGeneration = generation;
    let ticket: string;
    try {
      ticket = await this.input.requestTicket();
      this.ticketCount += 1;
    } catch (cause) {
      await this.handleConnectionFailure("ticket_request_failed", cause, reconnect);
      return;
    }
    if (this.cannotContinue() || generation !== this.activeGeneration) {
      return;
    }
    let socket: WebSocket;
    try {
      socket = new this.input.webSocketConstructor(
        buildBrowserSessionStreamUrl({
          afterSeq: this.lastHandledSeq,
          serviceUrl: this.input.serviceUrl,
          sessionId: this.input.sessionId,
          ticket,
        }),
      );
    } catch (cause) {
      await this.handleConnectionFailure("websocket_open_failed", cause, reconnect);
      return;
    }
    this.socket = socket;
    await new Promise<void>((resolve, reject) => {
      let opened = false;
      socket.addEventListener(
        "open",
        () => {
          if (generation !== this.activeGeneration || this.cannotContinue()) {
            socket.close();
            resolve();
            return;
          }
          opened = true;
          if (reconnect) {
            this.reconnectCount += 1;
          }
          this.setState("replaying");
          resolve();
        },
        { once: true },
      );
      socket.addEventListener("message", (event) => {
        if (generation !== this.activeGeneration || this.stopped || this.state === "paused") {
          return;
        }
        this.handleMessage(event.data);
      });
      socket.addEventListener(
        "error",
        () => {
          if (!opened) {
            reject(
              new BrowserSessionStreamError({
                lastHandledSeq: this.lastHandledSeq,
                reason: "websocket_open_failed",
              }),
            );
          }
        },
        { once: true },
      );
      socket.addEventListener(
        "close",
        () => {
          if (generation !== this.activeGeneration || this.stopped || this.state === "paused") {
            return;
          }
          if (!opened) {
            reject(
              new BrowserSessionStreamError({
                lastHandledSeq: this.lastHandledSeq,
                reason: "websocket_closed_before_open",
              }),
            );
            return;
          }
          this.beginReconnect();
        },
        { once: true },
      );
    }).catch(async (cause: unknown) => {
      await this.handleConnectionFailure("websocket_open_failed", cause, reconnect);
    });
  }

  /** Parses and admits one server frame to the active bounded delivery machine. */
  private handleMessage(data: unknown): void {
    if (typeof data !== "string") {
      this.delivery.rejectInvalidEnvelope();
      return;
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(data);
    } catch {
      this.delivery.rejectInvalidEnvelope();
      return;
    }
    const classified = classifyWebSocketServerEnvelope(decoded);
    if (classified.kind === "unknown-op") {
      return;
    }
    if (classified.kind !== "envelope") {
      this.delivery.rejectInvalidEnvelope();
      return;
    }
    const envelope = classified.envelope;
    if (envelope.op === webSocketOperation.event) {
      this.delivery.enqueueEvent(envelope.event, new TextEncoder().encode(data).byteLength);
      return;
    }
    if (envelope.op === webSocketOperation.replayComplete) {
      this.delivery.enqueueReplayComplete();
      return;
    }
    if (envelope.op === webSocketOperation.error) {
      this.pause(envelope.reason ?? "server_stream_error");
    }
  }

  /** Maps bounded delivery outcomes to cursor advancement or finite Paused State. */
  private handleDeliveryOutcome(
    delivery: SerialEventDelivery<SessionEvent>,
    outcome: SerialEventDeliveryOutcome<SessionEvent>,
  ): void {
    if (outcome.kind === "duplicate-ignored") {
      return;
    }
    if (outcome.kind === "event-handled") {
      this.lastHandledSeq = outcome.event.seq;
      this.eventCount += 1;
      return;
    }
    if (outcome.kind === "replay-complete") {
      if (delivery !== this.delivery) {
        return;
      }
      this.reconnectAttempt = 0;
      this.replayComplete = true;
      this.setState("live");
      this.replay.resolve();
      return;
    }
    const reason = deliveryFailureReason(outcome);
    const cause = outcome.kind === "handler-failed" ? outcome.cause : undefined;
    this.pause(reason, cause);
  }

  /** Stops old admission and reconnects only after its active handler settles. */
  private beginReconnect(): void {
    const priorDelivery = this.delivery;
    priorDelivery.stop();
    this.socket = null;
    this.activeGeneration += 1;
    if (this.replayComplete) {
      this.replay = createReplayDeferred();
    }
    this.replayComplete = false;
    void this.reconnect(priorDelivery);
  }

  /** Retries an unexpected close with a fresh ticket and the handled cursor. */
  private async reconnect(priorDelivery: SerialEventDelivery<SessionEvent>): Promise<void> {
    this.reconnectAttempt += 1;
    const maxAttempts = this.input.reconnect?.maxAttempts ?? defaultReconnectMaxAttempts;
    if (this.reconnectAttempt > maxAttempts) {
      this.pause("reconnect_exhausted");
      return;
    }
    this.setState("reconnecting");
    await priorDelivery.waitForSettlement();
    if (this.cannotContinue()) {
      return;
    }
    await delay(reconnectDelayMs(this.reconnectAttempt - 1, this.input.reconnect));
    if (this.cannotContinue()) {
      return;
    }
    this.delivery = this.createDelivery(this.lastHandledSeq);
    await this.open(true);
  }

  /** Returns whether lifecycle policy forbids new transport work. */
  private cannotContinue(): boolean {
    return this.stopped || this.state === "paused";
  }

  /** Classifies failures while opening or issuing a ticket. */
  private async handleConnectionFailure(
    reason: string,
    cause: unknown,
    reconnect: boolean,
  ): Promise<void> {
    if (!reconnect) {
      const error = toStreamError(reason, cause, this.lastHandledSeq);
      this.recordError(error);
      this.replay.reject(error);
      throw error;
    }
    this.recordError(toStreamError(reason, cause, this.lastHandledSeq));
    const failedDelivery = this.delivery;
    failedDelivery.stop();
    await this.reconnect(failedDelivery);
  }

  /** Enters finite Paused State and closes the active socket. */
  private pause(reason: string, cause?: unknown): void {
    if (this.state === "paused" || this.stopped) {
      return;
    }
    const error = toStreamError(reason, cause, this.lastHandledSeq);
    this.recordError(error);
    this.activeGeneration += 1;
    this.delivery.stop();
    this.setState("paused");
    this.replay.reject(error);
    const socket = this.socket;
    this.socket = null;
    socket?.close();
  }

  /** Stores one safe failure and isolates optional diagnostic callback failures. */
  private recordError(error: BrowserSessionStreamError): void {
    this.lastErrorReason = error.reason;
    try {
      this.input.onError?.(error);
    } catch {
      // Diagnostic callbacks cannot change stream lifecycle settlement.
    }
  }

  /** Emits one observable state transition without delegating lifecycle authority. */
  private setState(state: BrowserSessionStreamState): void {
    this.state = state;
    try {
      this.input.onStateChange?.(state, this.debugInfo());
    } catch {
      // Diagnostic callbacks cannot change stream lifecycle settlement.
    }
  }
}

/** Builds a browser observer stream URL containing only a one-time ticket. */
export function buildBrowserSessionStreamUrl(input: {
  readonly afterSeq: number;
  readonly serviceUrl: string;
  readonly sessionId: string;
  readonly ticket: string;
}): string {
  const url = new URL(`/sessions/${encodeURIComponent(input.sessionId)}/stream`, input.serviceUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("after", String(input.afterSeq));
  url.searchParams.set("runtimeKind", "observer");
  url.searchParams.set("ticket", input.ticket);
  return url.toString();
}

/** Computes a validated bounded exponential reconnect delay. */
function reconnectDelayMs(
  attempt: number,
  policy: BrowserSessionReconnectPolicy | undefined,
): number {
  return boundedExponentialRetryDelayMs(
    attempt,
    policy?.baseDelayMs ?? defaultReconnectBaseDelayMs,
    policy?.maxDelayMs ?? defaultReconnectMaxDelayMs,
  );
}

/** Validates all finite reconnect controls at construction time. */
function validateReconnectPolicy(policy: BrowserSessionReconnectPolicy | undefined): void {
  reconnectDelayMs(0, policy);
  const maxAttempts = policy?.maxAttempts ?? defaultReconnectMaxAttempts;
  if (!Number.isFinite(maxAttempts) || !Number.isInteger(maxAttempts) || maxAttempts < 0) {
    throw new Error("Browser session reconnect maxAttempts must be a non-negative finite integer");
  }
}

/** Converts one delivery failure outcome to a stable browser-safe reason. */
function deliveryFailureReason(outcome: SerialEventDeliveryOutcome<SessionEvent>): string {
  switch (outcome.kind) {
    case "delivery-byte-overflow":
      return "delivery_byte_overflow";
    case "delivery-queue-overflow":
      return "delivery_queue_overflow";
    case "handler-failed":
      return "event_handler_failed";
    case "handler-timeout":
      return "event_handler_timeout";
    case "invalid-server-envelope":
      return "invalid_server_envelope";
    case "non-contiguous-event":
      return "non_contiguous_event";
    case "duplicate-ignored":
    case "event-handled":
    case "replay-complete":
      throw new Error(`Successful delivery outcome cannot be mapped to failure: ${outcome.kind}`);
  }
}

/** Browser timer that also handles a zero-delay deterministic test policy. */
async function delay(delayMs: number): Promise<void> {
  await new Promise<void>((resolve) => window.setTimeout(resolve, delayMs));
}

interface ReplayDeferred {
  readonly promise: Promise<void>;
  readonly reject: (error: BrowserSessionStreamError) => void;
  readonly resolve: () => void;
}

/** Creates one replay settlement barrier and suppresses unhandled rejections. */
function createReplayDeferred(): ReplayDeferred {
  let rejectPromise: ((error: BrowserSessionStreamError) => void) | undefined;
  let resolvePromise: (() => void) | undefined;
  const promise = new Promise<void>((resolve, reject) => {
    rejectPromise = reject;
    resolvePromise = resolve;
  });
  promise.catch(() => undefined);
  if (rejectPromise === undefined || resolvePromise === undefined) {
    throw new Error("Replay barrier initialization failed");
  }
  return { promise, reject: rejectPromise, resolve: resolvePromise };
}

/** Retains an existing typed stream error or wraps an unknown failure safely. */
function toStreamError(
  reason: string,
  cause: unknown,
  lastHandledSeq: number,
): BrowserSessionStreamError {
  return cause instanceof BrowserSessionStreamError
    ? cause
    : new BrowserSessionStreamError({ cause, lastHandledSeq, reason });
}
