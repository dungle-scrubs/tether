import type { WebSocket } from "ws";

import { serializeErrorEnvelope, serializeEventEnvelope } from "./protocol.js";
import {
  defaultResourceLimits,
  resourceLimitReason,
  type ResourceLimits,
  sessionEventByteLength,
} from "./resource-limits.js";
import type { SessionEvent } from "./types.js";

/**
 * Runtime diagnostics for the WebSocket subscription hub.
 */
export interface SubscriptionHubDebugInfo {
  readonly backpressureCloseCount: number;
  readonly duplicateEventSkipCount: number;
  readonly gapBufferDropCount: number;
  readonly outOfOrderLiveEventCount: number;
  readonly pendingEventByteLength: number;
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

/** Structured diagnostics for one dropped pending gap buffer. */
interface GapBufferDroppedLogDetails {
  readonly contiguousDeliveredSeq: number;
  readonly droppedByteLength: number;
  readonly droppedEventCount: number;
  readonly sessionId: string;
}

/** One buffered future event and its measured serialized size. */
interface BufferedSessionEvent {
  readonly byteLength: number;
  readonly event: SessionEvent;
}

interface SocketSubscriptionState {
  readonly afterSeq: number;
  contiguousDeliveredSeq: number;
  /** Wall-clock start of the current gap-repair grace window, if one is open. */
  gapRepairStartedAt: number | null;
  readonly pendingBySeq: Map<number, BufferedSessionEvent>;
  /** Total serialized bytes currently held in pendingBySeq. */
  pendingByteLength: number;
  replaying: boolean;
}

/** Process-local options for the WebSocket subscription hub. */
export interface SubscriptionHubOptions {
  readonly limits: Pick<
    ResourceLimits,
    "wsBackpressureBufferedBytes" | "wsGapRepairGraceMs" | "wsReplayMaxBytes" | "wsReplayMaxEvents"
  >;
  /** Clock override so gap-repair grace windows are testable. */
  readonly now?: (() => number) | undefined;
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
  private gapBufferDropCount = 0;
  private readonly now: () => number;
  private outOfOrderLiveEventCount = 0;
  private replayGapRepairCount = 0;
  private replayGapRepairEventCount = 0;

  constructor(
    private readonly options: SubscriptionHubOptions = {
      limits: {
        wsBackpressureBufferedBytes: defaultResourceLimits.wsBackpressureBufferedBytes,
        wsGapRepairGraceMs: defaultResourceLimits.wsGapRepairGraceMs,
        wsReplayMaxBytes: defaultResourceLimits.wsReplayMaxBytes,
        wsReplayMaxEvents: defaultResourceLimits.wsReplayMaxEvents,
      },
    },
  ) {
    this.now = options.now ?? Date.now;
  }

  /**
   * Returns a snapshot of session and socket fan-out state.
   */
  debugInfo(): SubscriptionHubDebugInfo {
    let socketCount = 0;
    let pendingEventByteLength = 0;
    let pendingEventCount = 0;
    let replayBufferedEventCount = 0;
    let replayingSocketCount = 0;
    for (const sockets of this.socketsBySession.values()) {
      socketCount += sockets.size;
      for (const state of sockets.values()) {
        pendingEventByteLength += state.pendingByteLength;
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
      gapBufferDropCount: this.gapBufferDropCount,
      outOfOrderLiveEventCount: this.outOfOrderLiveEventCount,
      pendingEventByteLength,
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
      gapRepairStartedAt: null,
      pendingBySeq: new Map(),
      pendingByteLength: 0,
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
    const saturated =
      state.pendingBySeq.size >= this.options.limits.wsReplayMaxEvents ||
      state.pendingByteLength >= this.options.limits.wsReplayMaxBytes;
    if (saturated) {
      // During replay the gateway repair loop owns gap recovery, so a saturated
      // buffer is closed with the typed repair failure exactly as before. For a
      // live socket the buffer is only an optimization: the durable catch-up
      // poll re-reads everything after the contiguous cursor, so the buffer is
      // dropped and the poll gets a bounded grace window to repair the gap
      // before the socket is closed as genuinely unrepairable.
      if (state.replaying) {
        this.closeUnrepairedGapSocket(sessionId, socket, state, event.seq);
        return;
      }
      const now = this.now();
      if (
        state.gapRepairStartedAt !== null &&
        now - state.gapRepairStartedAt >= this.options.limits.wsGapRepairGraceMs
      ) {
        this.closeUnrepairedGapSocket(sessionId, socket, state, event.seq);
        return;
      }
      state.gapRepairStartedAt = state.gapRepairStartedAt ?? now;
      this.dropPendingGapBuffer(sessionId, state);
    }
    const byteLength = sessionEventByteLength(event);
    state.pendingBySeq.set(event.seq, { byteLength, event });
    state.pendingByteLength += byteLength;
  }

  /** Drops a saturated pending buffer that the durable catch-up poll re-reads. */
  private dropPendingGapBuffer(sessionId: string, state: SocketSubscriptionState): void {
    this.gapBufferDropCount += 1;
    logGapBufferDropped({
      contiguousDeliveredSeq: state.contiguousDeliveredSeq,
      droppedByteLength: state.pendingByteLength,
      droppedEventCount: state.pendingBySeq.size,
      sessionId,
    });
    state.pendingBySeq.clear();
    state.pendingByteLength = 0;
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
    // Buffered future events are bytes this socket has not consumed yet, so
    // they count toward the slow-consumer budget alongside the ws send buffer.
    if (
      socket.bufferedAmount + state.pendingByteLength >
      this.options.limits.wsBackpressureBufferedBytes
    ) {
      this.closeBackpressuredSocket(sessionId, socket);
      return;
    }
    socket.send(serializeEventEnvelope(event));
    state.contiguousDeliveredSeq = event.seq;
    // The contiguous cursor advanced, so any open gap-repair grace window has
    // made progress and restarts from the next saturation.
    state.gapRepairStartedAt = null;
  }

  /** Drains any buffered future events that now extend the contiguous prefix. */
  private drainPendingEvents(
    sessionId: string,
    socket: WebSocket,
    state: SocketSubscriptionState,
  ): void {
    while (socket.readyState === socket.OPEN) {
      const nextSeq = state.contiguousDeliveredSeq + 1;
      const buffered = state.pendingBySeq.get(nextSeq);
      if (!buffered) {
        return;
      }
      state.pendingBySeq.delete(nextSeq);
      state.pendingByteLength = Math.max(0, state.pendingByteLength - buffered.byteLength);
      this.sendContiguousEvent(sessionId, socket, state, buffered.event);
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

/** Emits one structured gap-buffer drop log line without using console APIs. */
function logGapBufferDropped(details: GapBufferDroppedLogDetails): void {
  process.stderr.write(
    `${JSON.stringify({
      details,
      event: "websocket.replay_gap_buffer_dropped",
    })}\n`,
  );
}
