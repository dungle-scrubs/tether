/**
 * Owns serial delivery of sequenced events and ordered replay markers.
 *
 * The module deliberately excludes sockets, reconnection, command correlation,
 * persistence, and client-specific recent-event policy so both observer and
 * participant transports can share one delivery state machine.
 *
 * Delivery is bounded by both a retained item count and a retained raw-frame
 * byte high-water mark. Queued event frames, the active item, and replay
 * markers all participate in the byte accounting so retained in-memory delivery
 * work stays finite. Handlers receive an `AbortSignal`; a handler timeout aborts
 * the signal and pauses delivery, and the module never begins a replayed
 * invocation of the same event until the timed-out invocation settles.
 */

/** Minimal event shape required by the serial delivery state machine. */
export interface SequencedDeliveryEvent {
  readonly seq: number;
}

/** Fixed byte cost charged for one retained replay-complete marker. */
export const replayMarkerByteCost = 64;

/** Handler invoked for one delivered event with a cancellation signal. */
export type SerialEventDeliveryHandler<TEvent extends SequencedDeliveryEvent> = (
  event: TEvent,
  signal: AbortSignal,
) => void | Promise<void>;

/** Successful outcomes emitted at the delivery Module Interface. */
export type SerialEventDeliveryOutcome<TEvent extends SequencedDeliveryEvent> =
  | {
      readonly kind: "delivery-byte-overflow";
      readonly maxQueueBytes: number;
      readonly observedQueueBytes: number;
    }
  | { readonly event: TEvent; readonly kind: "duplicate-ignored" }
  | {
      readonly kind: "delivery-queue-overflow";
      readonly maxQueueSize: number;
      readonly observedQueueSize: number;
    }
  | {
      readonly event: TEvent;
      readonly handlerCount: number;
      readonly kind: "event-handled";
    }
  | {
      readonly cause: unknown;
      readonly event: TEvent;
      readonly handlerIndex: number;
      readonly kind: "handler-failed";
    }
  | {
      readonly event: TEvent;
      readonly handlerIndex: number;
      readonly kind: "handler-timeout";
      readonly timeoutMs: number;
    }
  | { readonly kind: "invalid-server-envelope" }
  | {
      readonly expectedSeq: number;
      readonly kind: "non-contiguous-event";
      readonly observedSeq: number;
    }
  | { readonly kind: "replay-complete" };

/** Dependencies and limits for one independent serial delivery instance. */
export interface SerialEventDeliveryOptions<TEvent extends SequencedDeliveryEvent> {
  readonly clock?: SerialEventDeliveryClock;
  readonly handlerTimeoutMs: number;
  readonly initialSeq: number;
  /** Positive, finite retained raw-frame byte high-water mark. */
  readonly maxQueueBytes: number;
  /** Positive, finite retained item high-water mark. */
  readonly maxQueueSize: number;
  readonly onOutcome: (outcome: SerialEventDeliveryOutcome<TEvent>) => void;
}

/** Injectable deadline scheduler used to test handler timeouts deterministically. */
export interface SerialEventDeliveryClock {
  readonly clearTimeout: (handle: unknown) => void;
  readonly setTimeout: (callback: () => void, delayMs: number) => unknown;
}

/** Readonly snapshot of the serial delivery state visible to client diagnostics. */
export interface SerialEventDeliveryDebugInfo {
  readonly activeDeliverySeq: number | null;
  readonly draining: boolean;
  readonly halted: boolean;
  readonly handlerCount: number;
  readonly lastHandledSeq: number;
  readonly lastReceivedSeq: number;
  readonly maxQueueBytes: number;
  readonly maxQueueSize: number;
  /** Count of timed-out handler invocations still settling. */
  readonly pendingSettlementCount: number;
  /** Retained raw-frame bytes across queued items and the active item. */
  readonly queueBytes: number;
  readonly queueSize: number;
}

type SerialEventDeliveryQueueItem<TEvent extends SequencedDeliveryEvent> =
  | { readonly byteCost: number; readonly event: TEvent; readonly kind: "event" }
  | { readonly byteCost: number; readonly kind: "replay-complete" };

type HandlerInvocationResult =
  | { readonly kind: "completed" }
  | { readonly cause: unknown; readonly kind: "failed" }
  | { readonly kind: "timeout" };

const systemDeliveryClock: SerialEventDeliveryClock = {
  clearTimeout: (handle) => {
    clearTimeout(handle as ReturnType<typeof setTimeout>);
  },
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
};

/** Rejects a configured bound that is not a positive, finite number. */
function assertPositiveFiniteBound(label: string, value: number): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`Serial event delivery ${label} must be a positive finite number`);
  }
}

/**
 * Serializes event handler snapshots and replay markers behind a small queue
 * Interface. Transport owners map its typed outcomes to their own recovery.
 */
export class SerialEventDelivery<TEvent extends SequencedDeliveryEvent> {
  private activeDeliverySeq: number | null = null;
  private readonly handlers = new Set<SerialEventDeliveryHandler<TEvent>>();
  private readonly queue: SerialEventDeliveryQueueItem<TEvent>[] = [];
  private readonly pendingSettlements = new Set<Promise<void>>();
  private draining = false;
  private halted = false;
  private lastHandledSeq: number;
  private lastReceivedSeq: number;
  private queueBytes = 0;
  private readonly settlementWaiters = new Set<() => void>();
  private stopping = false;

  constructor(private readonly options: SerialEventDeliveryOptions<TEvent>) {
    assertPositiveFiniteBound("handlerTimeoutMs", options.handlerTimeoutMs);
    assertPositiveFiniteBound("maxQueueSize", options.maxQueueSize);
    assertPositiveFiniteBound("maxQueueBytes", options.maxQueueBytes);
    this.lastHandledSeq = options.initialSeq;
    this.lastReceivedSeq = options.initialSeq;
  }

  /** Returns the queue, handler, and handled-cursor state without exposing internals. */
  debugInfo(): SerialEventDeliveryDebugInfo {
    this.assertInternalInvariants();
    return {
      activeDeliverySeq: this.activeDeliverySeq,
      draining: this.draining,
      halted: this.halted || this.stopping,
      handlerCount: this.handlers.size,
      lastHandledSeq: this.lastHandledSeq,
      lastReceivedSeq: this.lastReceivedSeq,
      maxQueueBytes: this.options.maxQueueBytes,
      maxQueueSize: this.options.maxQueueSize,
      pendingSettlementCount: this.pendingSettlements.size,
      queueBytes: this.queueBytes,
      queueSize: this.queue.length,
    };
  }

  /**
   * Enqueues one event for ordered delivery. `frameByteLength` is the raw
   * WebSocket frame size and participates in the retained byte high-water mark.
   */
  enqueueEvent(event: TEvent, frameByteLength: number): void {
    if (this.halted || this.stopping) {
      return;
    }
    if (!Number.isFinite(frameByteLength) || frameByteLength < 0) {
      throw new Error(
        "Serial event delivery frame byte length must be a non-negative finite number",
      );
    }
    if (event.seq <= this.lastReceivedSeq) {
      this.options.onOutcome({ event, kind: "duplicate-ignored" });
      return;
    }
    const expectedSeq = this.lastReceivedSeq + 1;
    if (event.seq !== expectedSeq) {
      this.halted = true;
      this.options.onOutcome({
        expectedSeq,
        kind: "non-contiguous-event",
        observedSeq: event.seq,
      });
      return;
    }
    const observedQueueSize = this.queue.length + 1;
    if (observedQueueSize > this.options.maxQueueSize) {
      this.halted = true;
      this.options.onOutcome({
        kind: "delivery-queue-overflow",
        maxQueueSize: this.options.maxQueueSize,
        observedQueueSize,
      });
      return;
    }
    const observedQueueBytes = this.queueBytes + frameByteLength;
    if (observedQueueBytes > this.options.maxQueueBytes) {
      this.halted = true;
      this.options.onOutcome({
        kind: "delivery-byte-overflow",
        maxQueueBytes: this.options.maxQueueBytes,
        observedQueueBytes,
      });
      return;
    }
    this.lastReceivedSeq = event.seq;
    this.queue.push({ byteCost: frameByteLength, event, kind: "event" });
    this.queueBytes = observedQueueBytes;
    this.assertInternalInvariants();
    void this.drain();
  }

  /** Enqueues replay completion behind every event already received. */
  enqueueReplayComplete(): void {
    if (this.halted || this.stopping) {
      return;
    }
    this.queue.push({ byteCost: replayMarkerByteCost, kind: "replay-complete" });
    this.queueBytes += replayMarkerByteCost;
    void this.drain();
  }

  /** Registers an event handler and returns an unsubscribe callback. */
  onEvent(handler: SerialEventDeliveryHandler<TEvent>): () => void {
    this.handlers.add(handler);
    void this.drain();
    return () => {
      this.handlers.delete(handler);
    };
  }

  /**
   * Resolves after the currently active serial drain has settled, including any
   * timed-out handler invocation that is still running. Transport owners await
   * this before replaying so a timed-out handler cannot overlap its replay.
   */
  waitForSettlement(): Promise<void> {
    if (this.isSettled()) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.settlementWaiters.add(resolve);
    });
  }

  /** Stops new admission and discards work behind the active handler snapshot. */
  stop(): void {
    this.stopping = true;
    if (!this.draining) {
      this.clearQueue();
    }
  }

  /** Halts delivery after the transport rejects an untrusted event envelope. */
  rejectInvalidEnvelope(): void {
    if (this.halted || this.stopping) {
      return;
    }
    this.halted = true;
    this.options.onOutcome({ kind: "invalid-server-envelope" });
  }

  /** Runs at most one queue drain and preserves item order across async handlers. */
  private async drain(): Promise<void> {
    if (this.draining || this.halted || this.stopping) {
      return;
    }
    this.draining = true;
    try {
      while (this.queue.length > 0) {
        const item = this.queue[0];
        if (!item) {
          return;
        }
        if (item.kind === "replay-complete") {
          this.shiftHead();
          this.options.onOutcome({ kind: "replay-complete" });
          continue;
        }
        const handlers = [...this.handlers];
        if (handlers.length === 0) {
          return;
        }
        this.activeDeliverySeq = item.event.seq;
        this.assertInternalInvariants();
        const controller = new AbortController();
        for (const [handlerIndex, handler] of handlers.entries()) {
          const invocation = this.invokeHandler(handler, item.event, controller.signal);
          const result = await invocation.result;
          if (result.kind === "failed") {
            this.halted = true;
            this.activeDeliverySeq = null;
            this.options.onOutcome({
              cause: result.cause,
              event: item.event,
              handlerIndex,
              kind: "handler-failed",
            });
            return;
          }
          if (result.kind === "timeout") {
            controller.abort();
            this.trackPendingSettlement(invocation.settled);
            this.halted = true;
            this.activeDeliverySeq = null;
            this.options.onOutcome({
              event: item.event,
              handlerIndex,
              kind: "handler-timeout",
              timeoutMs: this.options.handlerTimeoutMs,
            });
            return;
          }
        }
        this.shiftHead();
        this.activeDeliverySeq = null;
        this.lastHandledSeq = item.event.seq;
        this.assertInternalInvariants();
        this.options.onOutcome({
          event: item.event,
          handlerCount: handlers.length,
          kind: "event-handled",
        });
        if (this.stopping) {
          return;
        }
      }
    } finally {
      this.activeDeliverySeq = null;
      this.draining = false;
      if (this.stopping) {
        this.clearQueue();
      }
      this.assertInternalInvariants();
      this.maybeNotifySettlement();
    }
  }

  /** Removes the head item and releases its retained byte cost. */
  private shiftHead(): void {
    const item = this.queue.shift();
    if (item) {
      this.queueBytes -= item.byteCost;
    }
  }

  /** Discards all queued work and resets retained byte accounting. */
  private clearQueue(): void {
    this.queue.splice(0);
    this.queueBytes = 0;
  }

  /** Records a still-running timed-out invocation as a settlement barrier. */
  private trackPendingSettlement(settled: Promise<void>): void {
    const tracked = settled.finally(() => {
      this.pendingSettlements.delete(tracked);
      this.maybeNotifySettlement();
    });
    this.pendingSettlements.add(tracked);
  }

  /** Returns whether no drain and no timed-out invocation remain outstanding. */
  private isSettled(): boolean {
    return !this.draining && this.activeDeliverySeq === null && this.pendingSettlements.size === 0;
  }

  /** Resolves settlement waiters once the delivery machine is fully settled. */
  private maybeNotifySettlement(): void {
    if (!this.isSettled()) {
      return;
    }
    for (const resolve of this.settlementWaiters) {
      resolve();
    }
    this.settlementWaiters.clear();
  }

  /** Fails at the mutation site when the delivery state violates its own rules. */
  private assertInternalInvariants(): void {
    if (this.lastHandledSeq > this.lastReceivedSeq) {
      throw new Error("Serial event delivery handled cursor exceeds received cursor");
    }
    if (this.queue.length > this.options.maxQueueSize) {
      throw new Error("Serial event delivery queue exceeds its configured maximum");
    }
    if (this.queueBytes > this.options.maxQueueBytes) {
      throw new Error("Serial event delivery retained bytes exceed the configured maximum");
    }
    if (this.queueBytes < 0) {
      throw new Error("Serial event delivery retained bytes fell below zero");
    }
    if (!this.draining && this.activeDeliverySeq !== null) {
      throw new Error("Serial event delivery has an active sequence outside a drain");
    }
    if (this.activeDeliverySeq !== null) {
      const activeItem = this.queue[0];
      if (activeItem?.kind !== "event" || activeItem.event.seq !== this.activeDeliverySeq) {
        throw new Error("Serial event delivery active sequence is not the queue head");
      }
    }
  }

  /**
   * Invokes one handler under the configured finite delivery deadline. Returns
   * both the raced result and a `settled` promise that resolves only when the
   * underlying invocation actually finishes, so a timed-out handler can be
   * awaited before replay begins.
   */
  private invokeHandler(
    handler: SerialEventDeliveryHandler<TEvent>,
    event: TEvent,
    signal: AbortSignal,
  ): { readonly result: Promise<HandlerInvocationResult>; readonly settled: Promise<void> } {
    const clock = this.options.clock ?? systemDeliveryClock;
    let timeoutHandle: unknown;
    const timeout = new Promise<HandlerInvocationResult>((resolve) => {
      timeoutHandle = clock.setTimeout(
        () => resolve({ kind: "timeout" }),
        this.options.handlerTimeoutMs,
      );
    });
    const invocation: Promise<HandlerInvocationResult> = Promise.resolve()
      .then(() => handler(event, signal))
      .then(
        (): HandlerInvocationResult => ({ kind: "completed" }),
        (cause: unknown): HandlerInvocationResult => ({ cause, kind: "failed" }),
      );
    const result = Promise.race([invocation, timeout]).then((raced) => {
      clock.clearTimeout(timeoutHandle);
      return raced;
    });
    // `invocation` maps its own rejection to a failed result, so it never
    // rejects; one continuation is enough to expose its settlement.
    const settled = invocation.then(() => undefined);
    return { result, settled };
  }
}
