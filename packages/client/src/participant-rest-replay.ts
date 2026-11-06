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
  RestParticipantTaskClient,
  RestParticipantTaskError,
  type RestParticipantTaskClientConfig,
} from "./rest-participant-task-client.js";
import { replayMarkerByteCost, type SerialEventDelivery } from "./serial-event-delivery.js";
import type { SessionEvent, TaskRecord } from "./types.js";

/** Internal replay failures carry only bounded policy and sequence metadata. */
export class ParticipantRestReplayError extends Error {
  /** Records bounded failure metadata without retaining a response payload. */
  constructor(
    readonly reason:
      | "delivery_byte_overflow"
      | "delivery_queue_overflow"
      | "invalid_server_envelope"
      | "non_contiguous_event",
    readonly safeDetails: Readonly<Record<string, number>> = {},
  ) {
    super("Participant REST replay failed");
    this.name = "ParticipantRestReplayError";
  }
}

type HeldFrame =
  | { readonly event: SessionEvent; readonly byteCost: number; readonly kind: "event" }
  | { readonly byteCost: number; readonly kind: "marker" };

/** Validated page with one computed delivery cost per event. */
type ValidatedHistoryPage = EventListResponse & {
  readonly frames: readonly { readonly event: SessionEvent; readonly byteCost: number }[];
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

// Only HTTP envelope metadata gets this allowance; event and task admission budgets stay unchanged.
const responseEnvelopeAllowance = 4_096;

/** Counts a retained canonical task record for the recovery-state owner. */
export function replayTaskByteCost(task: TaskRecord): number {
  return Buffer.byteLength(JSON.stringify(task));
}

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

/** Classifies only temporary read failures for the existing transport reconnect owner. */
export function isTransientRestReplayFailure(error: unknown): error is RestParticipantTaskError {
  return (
    error instanceof RestParticipantTaskError &&
    (error.code === "TRANSPORT" ||
      error.code === "PERSISTENCE" ||
      error.details.status === 429 ||
      error.details.status === 408)
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
    this.rest = new RestParticipantTaskClient(options.config, {
      readPolicy: {
        maxBytes: options.maxQueueBytes + responseEnvelopeAllowance,
        maxActiveTasks: options.maxQueueSize,
        signal: this.controller.signal,
        timeoutMs: options.timeoutMs,
      },
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
    while (after < head) {
      const availableSlots =
        this.options.maxQueueSize - this.held.length - this.options.delivery.debugInfo().queueSize;
      const page = await this.readHistoryPage(
        after,
        Math.min(this.pageLimit, head - after, Math.max(1, availableSlots)),
      );
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
    const snapshot = await this.rest.listActiveTasks(this.options.config.sessionId);
    this.assertActive();
    if (
      snapshot.invalidRowCount > 0 ||
      snapshot.tasks.some((task) => task.sessionId !== this.options.config.sessionId)
    ) {
      throw new RestParticipantTaskError({ code: "INVALID_RESPONSE", operation: "list-active" });
    }
    let bytes = 0;
    for (const task of snapshot.tasks) {
      bytes += replayTaskByteCost(task);
      if (bytes > this.options.maxQueueBytes) {
        throw new ParticipantRestReplayError("delivery_byte_overflow", {
          maxQueueBytes: this.options.maxQueueBytes,
          observedQueueBytes: bytes,
        });
      }
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

  /** Retries only byte-heavy multi-event pages, at the same unacknowledged cursor. */
  private async readHistoryPage(
    after: number,
    initialLimit: number,
  ): Promise<ValidatedHistoryPage> {
    let limit = initialLimit;
    for (;;) {
      try {
        return await this.readPage(after, limit);
      } catch (error) {
        const byteOverflow =
          (error instanceof ParticipantRestReplayError &&
            error.reason === "delivery_byte_overflow") ||
          (error instanceof RestParticipantTaskError &&
            error.code === "RESOURCE_LIMIT" &&
            error.details.resource === "response_bytes");
        if (!byteOverflow || limit === 1) throw error;
        limit = Math.max(1, Math.floor(limit / 2));
      }
    }
  }

  /** Validates pagination, contiguous sequences and shared queue budgets at ingress. */
  private async readPage(after: number, limit = this.pageLimit): Promise<ValidatedHistoryPage> {
    this.assertActive();
    const page = await this.rest.listEvents(this.options.config.sessionId, {
      after,
      limit,
    });
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
    if (observedQueueSize > this.options.maxQueueSize) {
      throw new ParticipantRestReplayError("delivery_queue_overflow", {
        maxQueueSize: this.options.maxQueueSize,
        observedQueueSize,
      });
    }
    const observedQueueBytes = delivery.queueBytes + this.heldBytes + bytes;
    if (observedQueueBytes > this.options.maxQueueBytes) {
      throw new ParticipantRestReplayError("delivery_byte_overflow", {
        maxQueueBytes: this.options.maxQueueBytes,
        observedQueueBytes,
      });
    }
  }

  /** Waits for delivery progress while remaining cancellable by the generation owner. */
  private waitUntil(predicate: () => boolean): Promise<void> {
    this.assertActive();
    if (predicate()) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const cleanup = () => {
        this.waiters.delete(check);
        this.controller.signal.removeEventListener("abort", aborted);
      };
      const check = () => {
        if (predicate()) {
          cleanup();
          resolve();
        }
      };
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
