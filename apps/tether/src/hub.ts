import type { WebSocket } from "ws";

import { serializeErrorEnvelope, serializeEventEnvelope } from "./protocol.js";
import {
  defaultResourceLimits,
  resourceLimitReason,
  type ResourceLimits,
} from "./resource-limits.js";
import type { SessionEvent } from "./types.js";

/**
 * Runtime diagnostics for the WebSocket subscription hub.
 */
export interface SubscriptionHubDebugInfo {
  readonly backpressureCloseCount: number;
  readonly duplicateEventSkipCount: number;
  readonly outOfOrderLiveEventCount: number;
  readonly pendingEventCount: number;
  readonly replayBufferedEventCount: number;
  readonly replayGapRepairCount: number;
  readonly replayGapRepairEventCount: number;
  readonly replayingSocketCount: number;
  readonly sessionCursors: readonly SubscriptionHubSessionCursorDiagnostic[];
  readonly sessionCount: number;
  readonly socketCount: number;
}

/**
 * Lowest contiguous delivered event sequence across local sockets for one
 * subscribed session.
 */
export interface SubscriptionHubSessionCursor {
  readonly lastDeliveredSeq: number;
  readonly sessionId: string;
}

/** Payload-free per-session cursor diagnostics. */
export interface SubscriptionHubSessionCursorDiagnostic extends SubscriptionHubSessionCursor {
  readonly pendingEventCount: number;
  readonly replayingSocketCount: number;
  readonly socketCount: number;
}

/** Snapshot of one socket replay state used by the gateway repair loop. */
export interface SubscriptionHubReplayState {
  readonly contiguousDeliveredSeq: number;
  readonly nextExpectedSeq: number;
  readonly pendingEventCount: number;
  readonly pendingSeqs: readonly number[];
  readonly replaying: boolean;
}

/** Structured diagnostics for an unrepaired replay gap. */
interface ReplayGapUnrepairedLogDetails {
  readonly contiguousDeliveredSeq: number;
  readonly observedSeq: number;
  readonly pendingEventCount: number;
  readonly sessionId: string;
}

interface SocketSubscriptionState {
  readonly afterSeq: number;
  contiguousDeliveredSeq: number;
  readonly pendingBySeq: Map<number, SessionEvent>;
  replaying: boolean;
}

/** Process-local options for the WebSocket subscription hub. */
export interface SubscriptionHubOptions {
  readonly limits: Pick<ResourceLimits, "wsBackpressureBufferedBytes" | "wsReplayMaxEvents">;
}

/** Options for adding a subscribed WebSocket to the hub. */
export interface SubscriptionHubAddOptions {
  readonly afterSeq: number;
  readonly replaying?: boolean | undefined;
}

export class SubscriptionHub {
  private readonly socketsBySession = new Map<string, Map<WebSocket, SocketSubscriptionState>>();
  private backpressureCloseCount = 0;
  private duplicateEventSkipCount = 0;
  private outOfOrderLiveEventCount = 0;
  private replayGapRepairCount = 0;
  private replayGapRepairEventCount = 0;

  constructor(
    private readonly options: SubscriptionHubOptions = {
      limits: {
        wsBackpressureBufferedBytes: defaultResourceLimits.wsBackpressureBufferedBytes,
        wsReplayMaxEvents: defaultResourceLimits.wsReplayMaxEvents,
      },
    },
  ) {}

  /**
   * Returns a snapshot of session and socket fan-out state.
   */
  debugInfo(): SubscriptionHubDebugInfo {
    let socketCount = 0;
    let pendingEventCount = 0;
    let replayBufferedEventCount = 0;
    let replayingSocketCount = 0;
    for (const sockets of this.socketsBySession.values()) {
      socketCount += sockets.size;
      for (const state of sockets.values()) {
        pendingEventCount += state.pendingBySeq.size;
        if (state.replaying) {
          replayingSocketCount += 1;
          replayBufferedEventCount += state.pendingBySeq.size;
        }
      }
    }
    return {
      backpressureCloseCount: this.backpressureCloseCount,
      duplicateEventSkipCount: this.duplicateEventSkipCount,
      outOfOrderLiveEventCount: this.outOfOrderLiveEventCount,
      pendingEventCount,
      replayBufferedEventCount,
      replayGapRepairCount: this.replayGapRepairCount,
      replayGapRepairEventCount: this.replayGapRepairEventCount,
      replayingSocketCount,
      sessionCursors: this.sessionCursorDiagnostics(),
      sessionCount: this.socketsBySession.size,
      socketCount,
    };
  }

  /**
   * Returns one catch-up cursor per session with at least one local socket.
   */
  sessionCursors(): SubscriptionHubSessionCursor[] {
    const cursors: SubscriptionHubSessionCursor[] = [];
    for (const [sessionId, sockets] of this.socketsBySession) {
      let lastDeliveredSeq: number | null = null;
      for (const state of sockets.values()) {
        lastDeliveredSeq =
          lastDeliveredSeq === null
            ? state.contiguousDeliveredSeq
            : Math.min(lastDeliveredSeq, state.contiguousDeliveredSeq);
      }
      if (lastDeliveredSeq !== null) {
        cursors.push({ lastDeliveredSeq, sessionId });
      }
    }
    return cursors;
  }

  /**
   * Adds a socket to one session fan-out set and removes it on close.
   */
  add(sessionId: string, socket: WebSocket, options: number | SubscriptionHubAddOptions): void {
    const normalizedOptions =
      typeof options === "number" ? { afterSeq: options, replaying: false } : options;
    const sockets =
      this.socketsBySession.get(sessionId) ?? new Map<WebSocket, SocketSubscriptionState>();
    sockets.set(socket, {
      afterSeq: normalizedOptions.afterSeq,
      contiguousDeliveredSeq: normalizedOptions.afterSeq,
      pendingBySeq: new Map(),
      replaying: normalizedOptions.replaying ?? false,
    });
    this.socketsBySession.set(sessionId, sockets);
    socket.on("close", () => {
      this.remove(sessionId, socket);
    });
  }

  /**
   * Sends a replayed event only when it extends the contiguous delivered prefix.
   */
  sendReplayEvent(sessionId: string, socket: WebSocket, event: SessionEvent): void {
    const state = this.socketsBySession.get(sessionId)?.get(socket);
    if (!state) {
      return;
    }
    this.deliverOrBufferEvent(sessionId, socket, state, event, "replay");
  }

  /**
   * Sends a committed session event to all currently open sockets subscribed to
   * that event's session.
   */
  broadcast(event: SessionEvent): void {
    const sockets = this.socketsBySession.get(event.sessionId);
    if (!sockets) {
      return;
    }
    for (const [socket, state] of sockets) {
      this.deliverOrBufferEvent(event.sessionId, socket, state, event, "live");
    }
  }

  /**
   * Marks replay complete for a socket after all repair work has drained.
   */
  completeReplay(sessionId: string, socket: WebSocket): void {
    const state = this.socketsBySession.get(sessionId)?.get(socket);
    if (!state) {
      return;
    }
    state.replaying = false;
    this.drainPendingEvents(sessionId, socket, state);
  }

  /**
   * Returns payload-free state for a socket's replay repair loop.
   */
  replayState(sessionId: string, socket: WebSocket): SubscriptionHubReplayState | null {
    const state = this.socketsBySession.get(sessionId)?.get(socket);
    if (!state) {
      return null;
    }
    return {
      contiguousDeliveredSeq: state.contiguousDeliveredSeq,
      nextExpectedSeq: state.contiguousDeliveredSeq + 1,
      pendingEventCount: state.pendingBySeq.size,
      pendingSeqs: [...state.pendingBySeq.keys()].sort((left, right) => left - right),
      replaying: state.replaying,
    };
  }

  /** Records one replay repair query and the number of events it returned. */
  recordReplayGapRepair(eventCount: number): void {
    this.replayGapRepairCount += 1;
    this.replayGapRepairEventCount += eventCount;
  }

  /**
   * Sends or buffers an event while preserving the contiguous delivery cursor.
   */
  private deliverOrBufferEvent(
    sessionId: string,
    socket: WebSocket,
    state: SocketSubscriptionState,
    event: SessionEvent,
    source: "live" | "replay",
  ): void {
    if (event.seq <= state.contiguousDeliveredSeq) {
      this.duplicateEventSkipCount += 1;
      return;
    }
    if (state.pendingBySeq.has(event.seq)) {
      this.duplicateEventSkipCount += 1;
      return;
    }
    if (event.seq > state.contiguousDeliveredSeq + 1) {
      this.bufferFutureEvent(sessionId, socket, state, event, source);
      return;
    }
    this.sendContiguousEvent(sessionId, socket, state, event);
    this.drainPendingEvents(sessionId, socket, state);
  }

  /** Buffers a future event without treating it as delivered. */
  private bufferFutureEvent(
    sessionId: string,
    socket: WebSocket,
    state: SocketSubscriptionState,
    event: SessionEvent,
    source: "live" | "replay",
  ): void {
    if (source === "live") {
      this.outOfOrderLiveEventCount += 1;
    }
    if (state.pendingBySeq.size >= this.options.limits.wsReplayMaxEvents) {
      this.closeUnrepairedGapSocket(sessionId, socket, state, event.seq);
      return;
    }
    state.pendingBySeq.set(event.seq, event);
  }

  /** Sends an event that is exactly the next contiguous sequence. */
  private sendContiguousEvent(
    sessionId: string,
    socket: WebSocket,
    state: SocketSubscriptionState,
    event: SessionEvent,
  ): void {
    if (socket.readyState !== socket.OPEN) {
      return;
    }
    if (socket.bufferedAmount > this.options.limits.wsBackpressureBufferedBytes) {
      this.closeBackpressuredSocket(sessionId, socket);
      return;
    }
    socket.send(serializeEventEnvelope(event));
    state.contiguousDeliveredSeq = event.seq;
  }

  /** Drains any buffered future events that now extend the contiguous prefix. */
  private drainPendingEvents(
    sessionId: string,
    socket: WebSocket,
    state: SocketSubscriptionState,
  ): void {
    while (socket.readyState === socket.OPEN) {
      const nextSeq = state.contiguousDeliveredSeq + 1;
      const nextEvent = state.pendingBySeq.get(nextSeq);
      if (!nextEvent) {
        return;
      }
      state.pendingBySeq.delete(nextSeq);
      this.sendContiguousEvent(sessionId, socket, state, nextEvent);
    }
  }

  /** Closes and removes a slow socket before queuing more serialized events. */
  private closeBackpressuredSocket(sessionId: string, socket: WebSocket): void {
    this.backpressureCloseCount += 1;
    try {
      socket.send(
        serializeErrorEnvelope({
          details: { reason: resourceLimitReason.backpressure },
          error: "WebSocket subscriber is too far behind",
        }),
      );
    } catch {
      // The socket is already unhealthy; close and remove it below.
    }
    socket.close(1013, resourceLimitReason.backpressure);
    this.remove(sessionId, socket);
  }

  /** Closes a socket whose buffered future-event gap exceeded the repair cap. */
  private closeUnrepairedGapSocket(
    sessionId: string,
    socket: WebSocket,
    state: SocketSubscriptionState,
    observedSeq: number,
  ): void {
    logReplayGapUnrepaired({
      contiguousDeliveredSeq: state.contiguousDeliveredSeq,
      observedSeq,
      pendingEventCount: state.pendingBySeq.size,
      sessionId,
    });
    try {
      socket.send(
        serializeErrorEnvelope({
          details: {
            contiguousDeliveredSeq: state.contiguousDeliveredSeq,
            observedSeq,
            pendingEventCount: state.pendingBySeq.size,
            reason: resourceLimitReason.replayGapUnrepaired,
          },
          error: "WebSocket replay gap could not be repaired",
        }),
      );
    } catch {
      // The socket is already unhealthy; close and remove it below.
    }
    socket.close(1013, resourceLimitReason.replayGapUnrepaired);
    this.remove(sessionId, socket);
  }

  /**
   * Removes a socket from a session fan-out set and drops empty session sets.
   */
  private remove(sessionId: string, socket: WebSocket): void {
    const sockets = this.socketsBySession.get(sessionId);
    if (!sockets) {
      return;
    }
    sockets.delete(socket);
    if (sockets.size === 0) {
      this.socketsBySession.delete(sessionId);
    }
  }

  /** Builds payload-free cursor diagnostics for every session with sockets. */
  private sessionCursorDiagnostics(): SubscriptionHubSessionCursorDiagnostic[] {
    const diagnostics: SubscriptionHubSessionCursorDiagnostic[] = [];
    for (const [sessionId, sockets] of this.socketsBySession) {
      let lastDeliveredSeq: number | null = null;
      let pendingEventCount = 0;
      let replayingSocketCount = 0;
      for (const state of sockets.values()) {
        lastDeliveredSeq =
          lastDeliveredSeq === null
            ? state.contiguousDeliveredSeq
            : Math.min(lastDeliveredSeq, state.contiguousDeliveredSeq);
        pendingEventCount += state.pendingBySeq.size;
        if (state.replaying) {
          replayingSocketCount += 1;
        }
      }
      if (lastDeliveredSeq !== null) {
        diagnostics.push({
          lastDeliveredSeq,
          pendingEventCount,
          replayingSocketCount,
          sessionId,
          socketCount: sockets.size,
        });
      }
    }
    return diagnostics;
  }
}

/** Emits one structured replay-gap log line without using console APIs. */
function logReplayGapUnrepaired(details: ReplayGapUnrepairedLogDetails): void {
  process.stderr.write(
    `${JSON.stringify({
      details,
      event: "websocket.replay_gap_unrepaired",
    })}\n`,
  );
}
