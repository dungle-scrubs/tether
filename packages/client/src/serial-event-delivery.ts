/**
 * Owns serial delivery of sequenced events and ordered replay markers.
 *
 * The module deliberately excludes sockets, reconnection, command correlation,
 * persistence, and client-specific recent-event policy so both observer and
 * participant transports can share one delivery state machine.
 */

/** Minimal event shape required by the serial delivery state machine. */
export interface SequencedDeliveryEvent {
  readonly seq: number;
}

/** Successful outcomes emitted at the delivery Module Interface. */
export type SerialEventDeliveryOutcome<TEvent extends SequencedDeliveryEvent> =
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
  readonly queueSize: number;
}

type SerialEventDeliveryQueueItem<TEvent extends SequencedDeliveryEvent> =
  | { readonly event: TEvent; readonly kind: "event" }
  | { readonly kind: "replay-complete" };

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

/**
 * Serializes event handler snapshots and replay markers behind a small queue
 * Interface. Transport owners map its typed outcomes to their own recovery.
 */
export class SerialEventDelivery<TEvent extends SequencedDeliveryEvent> {
  private activeDeliverySeq: number | null = null;
  private readonly handlers = new Set<(event: TEvent) => void | Promise<void>>();
  private readonly queue: SerialEventDeliveryQueueItem<TEvent>[] = [];
  private draining = false;
  private halted = false;
  private lastHandledSeq: number;
  private lastReceivedSeq: number;
  private readonly settlementWaiters = new Set<() => void>();
  private stopping = false;

  constructor(private readonly options: SerialEventDeliveryOptions<TEvent>) {
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
      queueSize: this.queue.length,
    };
  }

  /** Enqueues one event for ordered delivery. */
  enqueueEvent(event: TEvent): void {
    if (this.halted || this.stopping) {
      return;
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
    this.lastReceivedSeq = event.seq;
    this.queue.push({ event, kind: "event" });
    this.assertInternalInvariants();
    void this.drain();
  }

  /** Enqueues replay completion behind every event already received. */
  enqueueReplayComplete(): void {
    if (this.halted || this.stopping) {
      return;
    }
    this.queue.push({ kind: "replay-complete" });
    void this.drain();
  }

  /** Registers an event handler and returns an unsubscribe callback. */
  onEvent(handler: (event: TEvent) => void | Promise<void>): () => void {
    this.handlers.add(handler);
    void this.drain();
    return () => {
      this.handlers.delete(handler);
    };
  }

  /** Resolves after the currently active serial drain has settled. */
  waitForSettlement(): Promise<void> {
    if (!this.draining && this.activeDeliverySeq === null) {
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
      this.queue.splice(0);
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
          this.queue.shift();
          this.options.onOutcome({ kind: "replay-complete" });
          continue;
        }
        const handlers = [...this.handlers];
        if (handlers.length === 0) {
          return;
        }
        this.activeDeliverySeq = item.event.seq;
        this.assertInternalInvariants();
        for (const [handlerIndex, handler] of handlers.entries()) {
          const result = await this.invokeHandler(handler, item.event);
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
        this.queue.shift();
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
        this.queue.splice(0);
      }
      this.assertInternalInvariants();
      for (const resolve of this.settlementWaiters) {
        resolve();
      }
      this.settlementWaiters.clear();
    }
  }

  /** Fails at the mutation site when the delivery state violates its own rules. */
  private assertInternalInvariants(): void {
    if (this.lastHandledSeq > this.lastReceivedSeq) {
      throw new Error("Serial event delivery handled cursor exceeds received cursor");
    }
    if (this.queue.length > this.options.maxQueueSize) {
      throw new Error("Serial event delivery queue exceeds its configured maximum");
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

  /** Invokes one handler under the configured finite delivery deadline. */
  private async invokeHandler(
    handler: (event: TEvent) => void | Promise<void>,
    event: TEvent,
  ): Promise<HandlerInvocationResult> {
    const clock = this.options.clock ?? systemDeliveryClock;
    let timeoutHandle: unknown;
    const timeout = new Promise<HandlerInvocationResult>((resolve) => {
      timeoutHandle = clock.setTimeout(
        () => resolve({ kind: "timeout" }),
        this.options.handlerTimeoutMs,
      );
    });
    const invocation: Promise<HandlerInvocationResult> = Promise.resolve()
      .then(() => handler(event))
      .then(
        (): HandlerInvocationResult => ({ kind: "completed" }),
        (cause: unknown): HandlerInvocationResult => ({ cause, kind: "failed" }),
      );
    const result = await Promise.race([invocation, timeout]);
    clock.clearTimeout(timeoutHandle);
    return result;
  }
}
