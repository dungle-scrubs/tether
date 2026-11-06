import {
  type EventListResponse,
  taskApprovalRecordedPayloadSchema,
  taskCancelledPayloadSchema,
  taskClaimExpiredPayloadSchema,
  taskCreatedPayloadSchema,
  taskParticipantPayloadSchema,
} from "@dungle-scrubs/tether-protocol";
import type { ZodType } from "zod";

import { sessionEventType } from "./protocol.js";
import {
  createReplayRestTaskClient,
  isRestParticipantReadTimeout,
  isRestParticipantResourceLimit,
  type RestParticipantTaskClient,
  RestParticipantTaskError,
  type RestParticipantTaskClientConfig,
  restResponseEnvelopeAllowance,
} from "./rest-participant-task-client.js";
import { replayMarkerByteCost, type SerialEventDelivery } from "./serial-event-delivery.js";
import type { SessionEvent, TaskRecord } from "./types.js";

/** Original overflow metadata, correlated with its terminal reason. */
export type ParticipantOverflowDetails = {
  /** Count admission failed before retaining the rejected item. */
  readonly delivery_queue_overflow: {
    /** Configured retained item limit. */
    readonly maxQueueSize: number;
    /** Items that admission would retain. */
    readonly observedQueueSize: number;
  };
  /** Byte admission failed before retaining the rejected payload. */
  readonly delivery_byte_overflow: {
    /** Configured retained byte limit. */
    readonly maxQueueBytes: number;
    /** Bytes observed at rejection. */
    readonly observedQueueBytes: number;
    /** Separate HTTP response cap when the response reader rejected the body. */
    readonly maxResponseBytes?: number;
  };
};

/** Constructor arguments preserve the correlation between reason and bounded details. */
type ReplayFailureArgs =
  | [
      reason: "delivery_queue_overflow",
      details: ParticipantOverflowDetails["delivery_queue_overflow"],
    ]
  | [
      reason: "delivery_byte_overflow",
      details: ParticipantOverflowDetails["delivery_byte_overflow"],
    ]
  | [
      reason: "invalid_server_envelope" | "non_contiguous_event",
      details?: Readonly<Record<string, number>>,
    ];

/** Internal replay failures carry only bounded policy and sequence metadata. */
export class ParticipantRestReplayError extends Error {
  readonly reason: ReplayFailureArgs[0];
  readonly safeDetails: Readonly<Record<string, number>>;

  /** Records bounded failure metadata without retaining a response payload. */
  constructor(...[reason, details = {}]: ReplayFailureArgs) {
    super("Participant REST replay failed");
    this.reason = reason;
    this.safeDetails = details;
    this.name = "ParticipantRestReplayError";
  }
}

/** Checks the shared count-first admission policy for history and task reservations. */
export function checkRecoveryBudget(
  limits: { readonly maxQueueSize: number; readonly maxQueueBytes: number },
  observedQueueSize: number,
  observedQueueBytes: number,
): void {
  if (observedQueueSize > limits.maxQueueSize)
    throw new ParticipantRestReplayError("delivery_queue_overflow", {
      maxQueueSize: limits.maxQueueSize,
      observedQueueSize,
    });
  if (observedQueueBytes > limits.maxQueueBytes)
    throw new ParticipantRestReplayError("delivery_byte_overflow", {
      maxQueueBytes: limits.maxQueueBytes,
      observedQueueBytes,
    });
}

type HeldFrame =
  | {
      /** Canonical event retained until ordered delivery. */
      readonly event: SessionEvent;
      /** Retained UTF-8 envelope cost charged to delivery. */
      readonly byteCost: number;
      /** Discriminator for this retained delivery or task state. */
      readonly kind: "event";
    }
  | {
      /** Retained UTF-8 envelope cost charged to delivery. */
      readonly byteCost: number;
      /** Discriminator for this retained delivery or task state. */
      readonly kind: "marker";
    };

/** Validated page with one computed delivery cost per event. */
type ValidatedHistoryPage = EventListResponse & {
  /** Validated event envelopes with their admission costs. */
  readonly frames: readonly {
    /** Canonical event retained until ordered delivery. */
    readonly event: SessionEvent;
    /** Retained UTF-8 envelope cost charged to delivery. */
    readonly byteCost: number;
  }[];
};

const taskPayloadSchemas = new Map<string, ZodType<{ readonly task: TaskRecord }>>([
  [sessionEventType.taskCreated, taskCreatedPayloadSchema],
  [sessionEventType.taskClaimExpired, taskClaimExpiredPayloadSchema],
  [sessionEventType.taskClaimed, taskParticipantPayloadSchema],
  [sessionEventType.taskCompleted, taskParticipantPayloadSchema],
  [sessionEventType.taskFailed, taskParticipantPayloadSchema],
  [sessionEventType.taskReleased, taskParticipantPayloadSchema],
  [sessionEventType.controlCancel, taskCancelledPayloadSchema],
  [sessionEventType.approvalRecorded, taskApprovalRecordedPayloadSchema],
]);

/** Counts the same event envelope retained by serial delivery. */
function eventByteCost(event: SessionEvent): number {
  return Buffer.byteLength(JSON.stringify({ event, op: "event" }));
}

/** Only canonical lifecycle payloads can update REST replay's task candidates. */
export function taskFromReplayStateEvent(event: SessionEvent): TaskRecord | null {
  const schema = taskPayloadSchemas.get(event.type);
  if (!schema) return null;
  const parsed = schema.safeParse(event.payload);
  if (!parsed.success || parsed.data.task.sessionId !== event.sessionId) {
    throw new ParticipantRestReplayError("invalid_server_envelope");
  }
  return parsed.data.task;
}

/** HTTP failures with temporary service, gateway, rate or request-timeout semantics. */
const transientRestStatuses = new Set([408, 429, 500, 502, 503, 504]);

/** Classifies only temporary read failures for the existing transport reconnect owner. */
export function isTransientRestReplayFailure(error: unknown): error is RestParticipantTaskError {
  return (
    error instanceof RestParticipantTaskError &&
    (error.code === "TRANSPORT" || transientRestStatuses.has(error.details.status ?? 0))
  );
}

/** One generation's bounded REST history and ordered WebSocket handoff. */
export class ParticipantRestReplay {
  private readonly controller = new AbortController();
  private readonly rest: RestParticipantTaskClient;
  private readonly held: HeldFrame[] = [];
  private heldBytes = 0;
  private wireReady = false;
  private wireSeq: number;
  private bootstrapping = true;
  private readonly waiters = new Set<() => void>();
  private readonly pageLimit: number;
  /** Highest request size still admissible after a failure in this generation. */
  private historyLimitCeiling: number;

  /** Normalizes bounded HTTP reads while preserving transport timeout provenance. */
  private readonly replayReadFailure = (error: unknown): never => {
    if (isRestParticipantResourceLimit(error)) {
      const resource = error.failure.details.resourceLimit;
      switch (resource.resource) {
        case "active_tasks":
          throw new ParticipantRestReplayError("delivery_queue_overflow", {
            maxQueueSize: resource.limit,
            observedQueueSize: resource.observed,
          });
        case "response_bytes":
          throw new ParticipantRestReplayError("delivery_byte_overflow", {
            maxQueueBytes: this.options.maxQueueBytes,
            observedQueueBytes: resource.observed,
            maxResponseBytes: resource.limit,
          });
        default: {
          const unsupported: never = resource.resource;
          throw new Error(`Unsupported REST resource: ${unsupported}`);
        }
      }
    }
    throw error;
  };

  /** Binds replay to one generation's delivery state and existing resource limits. */
  constructor(
    private readonly options: {
      readonly afterSeq: number;
      readonly config: RestParticipantTaskClientConfig & { readonly sessionId: string };
      readonly delivery: SerialEventDelivery<SessionEvent>;
      readonly lastHandledSeq: () => number;
      readonly maxQueueBytes: number;
      readonly maxQueueSize: number;
      readonly timeoutMs: number;
    },
  ) {
    this.wireSeq = options.afterSeq;
    // Leave room for the wire marker at the normal page boundary.
    this.pageLimit = Math.min(500, Math.max(1, options.maxQueueSize - 1));
    this.historyLimitCeiling = this.pageLimit;
    this.rest = createReplayRestTaskClient(options.config, {
      maxBytes: options.maxQueueBytes + restResponseEnvelopeAllowance,
      maxActiveTasks: options.maxQueueSize,
      signal: this.controller.signal,
      timeoutMs: options.timeoutMs,
    });
  }

  /** Finds a witnessed head with probes bounded by the safe-integer sequence domain. */
  async discoverHead(): Promise<number> {
    const floor = this.options.afterSeq;
    if (!Number.isSafeInteger(floor) || floor < 0) {
      throw new ParticipantRestReplayError("invalid_server_envelope");
    }
    const first = await this.readPage(floor, 1);
    let witnessed = first.events[0]?.seq ?? floor;
    if (first.events.length > 0) {
      const span = Number.MAX_SAFE_INTEGER - floor;
      let offset = 1;
      let upper: number;
      for (;;) {
        const after = floor + offset;
        const page = await this.readPage(after, 1);
        if (page.events.length === 0) {
          upper = after;
          break;
        }
        witnessed = page.events[0]?.seq ?? witnessed;
        offset = offset >= Math.ceil(span / 2) ? span : offset * 2;
      }
      // Once an absent upper boundary is found, concurrent appends cannot expand this interval.
      while (witnessed < upper) {
        const after = witnessed + Math.floor((upper - witnessed) / 2);
        const page = await this.readPage(after, 1);
        const event = page.events[0];
        if (event) witnessed = event.seq;
        else upper = after;
      }
    }
    this.assertActive();
    this.wireSeq = witnessed;
    return witnessed;
  }

  /** Waits for command authority, then delivers history through the ordinary handler path. */
  async replay(head: number): Promise<void> {
    await this.waitForWire();
    let after = this.options.afterSeq;
    let historyLimit = this.pageLimit;
    while (after < head) {
      const availableSlots =
        this.options.maxQueueSize - this.held.length - this.options.delivery.debugInfo().queueSize;
      const page = await this.readHistoryPage(
        after,
        Math.min(historyLimit, head - after, Math.max(1, availableSlots)),
      );
      historyLimit = Math.min(this.historyLimitCeiling, page.pagination.limit * 2);
      const events = page.events;
      if (
        events.length === 0 ||
        (!page.pagination.hasMore && page.pagination.nextAfterSeq < head)
      ) {
        throw new ParticipantRestReplayError("invalid_server_envelope");
      }
      for (const frame of page.frames) {
        this.assertActive();
        this.options.delivery.enqueueEvent(frame.event, frame.byteCost);
      }
      after = events.at(-1)?.seq ?? after;
      // Settlement alone can resolve before a handler is attached. Require the handled cursor.
      await this.waitUntil(() => this.options.lastHandledSeq() >= after);
    }
    this.assertActive();
    while (this.held.length > 0) {
      const frame = this.held.shift();
      if (!frame) break;
      this.heldBytes -= frame.byteCost;
      if (frame.kind === "event") {
        this.options.delivery.enqueueEvent(frame.event, frame.byteCost);
      } else {
        this.options.delivery.enqueueReplayComplete();
      }
      this.assertActive();
    }
    this.bootstrapping = false;
  }

  /** Returns true when the frame was retained for ordered delivery after history. */
  holdEvent(event: SessionEvent, byteCost: number): boolean {
    this.assertActive();
    if (
      event.sessionId !== this.options.config.sessionId ||
      !Number.isSafeInteger(event.seq) ||
      event.seq < 1
    ) {
      throw new ParticipantRestReplayError("invalid_server_envelope");
    }
    taskFromReplayStateEvent(event);
    if (!this.bootstrapping) return false;
    if (event.seq <= this.wireSeq) return true;
    if (event.seq !== this.wireSeq + 1) {
      throw new ParticipantRestReplayError("non_contiguous_event", {
        expectedSeq: this.wireSeq + 1,
        observedSeq: event.seq,
      });
    }
    this.checkBudget(1, byteCost);
    this.wireSeq = event.seq;
    this.held.push({ byteCost, event, kind: "event" });
    this.heldBytes += byteCost;
    return true;
  }

  /** The matching wire epoch must be available before historical handlers can publish. */
  holdMarker(hasControlEpoch: boolean): boolean {
    this.assertActive();
    if (!this.bootstrapping) return false;
    if (!hasControlEpoch) throw new ParticipantRestReplayError("invalid_server_envelope");
    this.checkBudget(1, replayMarkerByteCost);
    this.held.push({ byteCost: replayMarkerByteCost, kind: "marker" });
    this.heldBytes += replayMarkerByteCost;
    this.wireReady = true;
    this.notifyProgress();
    return true;
  }

  /** Seeds the existing runner from canonical active rows, never from skipped malformed rows. */
  async loadActiveTasks(): Promise<readonly TaskRecord[]> {
    const snapshot = await this.rest
      .listActiveTasks(this.options.config.sessionId)
      .catch(this.replayReadFailure);
    this.assertActive();
    if (
      snapshot.invalidRowCount > 0 ||
      snapshot.tasks.some((task) => task.sessionId !== this.options.config.sessionId)
    ) {
      throw new RestParticipantTaskError({ code: "INVALID_RESPONSE", operation: "list-active" });
    }
    return snapshot.tasks;
  }

  /** Wakes genuine delivery waiters after the runtime records successful handling. */
  notifyProgress(): void {
    for (const waiter of [...this.waiters]) waiter();
  }

  /** Whether the transport owner intentionally ended this generation. */
  get aborted(): boolean {
    return this.controller.signal.aborted;
  }

  /** Ends this generation, including fetches and waiters, without touching its replacement. */
  abort(): void {
    this.controller.abort();
    this.held.splice(0);
    this.heldBytes = 0;
  }

  /** Rejects work after its generation has ended. */
  private assertActive(): void {
    if (this.controller.signal.aborted) {
      throw new RestParticipantTaskError({ code: "TRANSPORT", operation: "replay" });
    }
  }

  /** Reduces byte-heavy or timed-out multi-event reads at the same unacknowledged cursor. */
  private async readHistoryPage(
    after: number,
    initialLimit: number,
  ): Promise<ValidatedHistoryPage> {
    let limit = initialLimit;
    for (;;) {
      try {
        return await this.readPage(after, limit);
      } catch (error) {
        this.assertActive();
        const overflow =
          error instanceof ParticipantRestReplayError &&
          (error.reason === "delivery_byte_overflow" || error.reason === "delivery_queue_overflow");
        if ((!overflow && !isRestParticipantReadTimeout(error)) || limit === 1) throw error;
        if (overflow) this.historyLimitCeiling = Math.min(this.historyLimitCeiling, limit - 1);
        limit = Math.max(1, Math.floor(limit / 2));
      }
    }
  }

  /** Validates pagination, contiguous sequences and shared queue budgets at ingress. */
  private async readPage(after: number, limit = this.pageLimit): Promise<ValidatedHistoryPage> {
    this.assertActive();
    const page = await this.rest
      .listEvents(this.options.config.sessionId, {
        after,
        limit,
      })
      .catch(this.replayReadFailure);
    this.assertActive();
    const { events, pagination } = page;
    if (
      pagination.afterSeq !== after ||
      pagination.limit > limit ||
      pagination.returned !== events.length ||
      events.length > pagination.limit ||
      pagination.nextAfterSeq !== (events.at(-1)?.seq ?? after) ||
      (pagination.hasMore && events.length === 0)
    ) {
      throw new ParticipantRestReplayError("invalid_server_envelope");
    }
    let expected = after;
    for (const event of events) {
      if (expected === Number.MAX_SAFE_INTEGER)
        throw new ParticipantRestReplayError("invalid_server_envelope");
      expected += 1;
      if (!Number.isSafeInteger(event.seq) || event.seq !== expected) {
        throw new ParticipantRestReplayError("non_contiguous_event", {
          expectedSeq: expected,
          observedSeq: event.seq,
        });
      }
      if (event.sessionId !== this.options.config.sessionId) {
        throw new ParticipantRestReplayError("invalid_server_envelope");
      }
      taskFromReplayStateEvent(event);
    }
    const frames = events.map((event) => ({ event, byteCost: eventByteCost(event) }));
    this.checkBudget(
      frames.length,
      frames.reduce((bytes, frame) => bytes + frame.byteCost, 0),
    );
    return { ...page, frames };
  }

  /** Includes held wire frames and serial delivery in the same finite budget. */
  private checkBudget(items: number, bytes: number): void {
    const delivery = this.options.delivery.debugInfo();
    const observedQueueSize = delivery.queueSize + this.held.length + items;
    const observedQueueBytes = delivery.queueBytes + this.heldBytes + bytes;
    checkRecoveryBudget(this.options, observedQueueSize, observedQueueBytes);
  }

  /** Waits for delivery progress while remaining cancellable by the generation owner. */
  private waitUntil(predicate: () => boolean): Promise<void> {
    this.assertActive();
    if (predicate()) return Promise.resolve();
    return new Promise((resolve, reject) => {
      /** Releases the delivery-progress and cancellation listeners together. */
      const cleanup = () => {
        this.waiters.delete(check);
        this.controller.signal.removeEventListener("abort", aborted);
      };
      /** Settles the waiter only after its requested progress occurs. */
      const check = () => {
        if (predicate()) {
          cleanup();
          resolve();
        }
      };
      /** Rejects the waiter when its generation loses ownership. */
      const aborted = () => {
        cleanup();
        reject(new RestParticipantTaskError({ code: "TRANSPORT", operation: "replay" }));
      };
      this.waiters.add(check);
      this.controller.signal.addEventListener("abort", aborted, { once: true });
      check();
    });
  }

  /** Bounds the wait for command authority with the existing command timeout. */
  private async waitForWire(): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () =>
          reject(new RestParticipantTaskError({ code: "TRANSPORT", operation: "replay-control" })),
        this.options.timeoutMs,
      );
    });
    try {
      await Promise.race([this.waitUntil(() => this.wireReady), deadline]);
    } finally {
      clearTimeout(timer);
    }
  }
}
