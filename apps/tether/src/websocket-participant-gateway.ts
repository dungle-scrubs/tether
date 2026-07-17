import type { IncomingMessage, Server } from "node:http";
import type { Duplex } from "node:stream";
import { URL } from "node:url";

import { Effect, Fiber } from "effect";
import type { WebSocket } from "ws";
import { WebSocketServer } from "ws";
import type { z } from "zod";
import {
  authorize,
  authorizeParticipantIdentity,
  effectiveParticipantId,
} from "./auth/authorize.js";
import { authErrorFromUnknown, type AuthRuntime } from "./auth/enforcement.js";
import type { AuthContext } from "./auth/token.js";
import { sleepUnrefEffect } from "./effect-runtime.js";
import { broadcastEvents, controlLeaseConflictError } from "./http-route-runtime.js";
import type { SubscriptionHub } from "./hub.js";
import {
  parseAfterSeq,
  participantRuntimeKindSchema,
  registerParticipantSchema,
  serializeCommandResultEnvelope,
  serializeErrorEnvelope,
  serializeReplayCompleteEnvelope,
  webSocketOperation,
  wsPublishMessageSchema,
  wsTaskCancelMessageSchema,
  wsTaskClaimMessageSchema,
  wsTaskCompleteMessageSchema,
  wsTaskFailMessageSchema,
  wsTaskRefreshMessageSchema,
  wsTaskReleaseMessageSchema,
} from "./protocol.js";
import type { LiveHostPresence, WebSocketPresenceEnvelope } from "./protocol.js";
import {
  createWebSocketMessageRateLimiter,
  resourceLimitReason,
  type ResourceLimitRuntime,
} from "./resource-limits.js";
import { authorizeClientPublishedEvent } from "./session-event-publish-policy.js";
import type { ControlEpochGuard } from "./db.js";
import type {
  ParticipantControlContext,
  SessionServiceEffect,
  TaskClaimRefreshResult,
  TaskMutationResult,
} from "./session-service.js";
import {
  classifyHostPresenceStream,
  type HostPresenceRuntime,
  type HostPresenceStreamKind,
  projectWebSocketPresenceEnvelope,
} from "./host-presence.js";
import { findClientWebSocketCommandSpec } from "./websocket-command-spec.js";

interface ParticipantWebSocketGatewayInput {
  readonly auth: AuthRuntime;
  readonly hostPresence: HostPresenceRuntime;
  readonly hub: SubscriptionHub;
  readonly replicaId: string;
  readonly resourceLimitRuntime: ResourceLimitRuntime;
  readonly server: Server;
  readonly service: SessionServiceEffect;
}

interface WebSocketControlLeaseRefreshInput {
  readonly participantContext: ParticipantControlContext;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
  readonly socket: WebSocket;
}

interface WebSocketTaskMutationCommandInput<
  TBody extends {
    readonly requestId?: string | undefined;
    readonly taskId: string;
  },
> {
  readonly command: string;
  readonly context: ParticipantControlContext;
  readonly hub: SubscriptionHub;
  readonly raw: unknown;
  readonly schema: z.ZodType<TBody>;
  readonly socket: WebSocket;
  readonly mutate: (input: {
    readonly body: TBody;
    readonly participantId: string;
  }) => Effect.Effect<TaskMutationResult, unknown>;
}

interface WebSocketCommandContext {
  readonly op: string;
  readonly requestId?: string;
  readonly taskId?: string;
}

interface HostPresenceWebSocketInput {
  readonly hostPresence: HostPresenceRuntime;
  readonly hub: SubscriptionHub;
  readonly replicaId: string;
  readonly resourceLimitRuntime: ResourceLimitRuntime;
  readonly searchParams: URLSearchParams;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
  readonly socket: WebSocket;
  readonly streamKind: HostPresenceStreamKind;
}

/**
 * Per-process registry of the current control socket for each
 * (sessionId, participantId, instanceId). When a new authenticated acquisition
 * advances the Control Epoch, the prior epoch's socket for that key is closed
 * immediately after the new epoch commits, rather than waiting for its next
 * command or lease refresh to fence it reactively. Cross-replica coordination is
 * out of scope: this only fences sockets in the local process.
 */
export class ControlSocketRegistry {
  private readonly sockets = new Map<string, RegisteredControlSocket>();

  /**
   * Registers the socket that now owns a control key. A late, strictly-older-epoch
   * registration must never replace the current higher-epoch owner: it is rejected
   * and closed without mutating the registry. When the incoming epoch is at least
   * the stored one, any strictly-older-epoch socket still open for the same key is
   * proactively closed and the incoming socket becomes the current owner.
   */
  register(key: string, socket: WebSocket, epoch: number): void {
    const existing = this.sockets.get(key);
    if (existing && existing.socket !== socket) {
      if (epoch < existing.epoch) {
        // A reconnect that acquired a newer epoch already registered ahead of
        // this late, strictly-lower-epoch registration. Reject the stale socket
        // rather than overwriting the current higher-epoch entry with it.
        safeSendWebSocketEnvelope(socket, {
          details: { currentEpoch: existing.epoch, reason: "control_epoch_superseded" },
          error: "WebSocket control epoch was superseded",
        });
        safeCloseWebSocket(socket, 1008, "control epoch superseded");
        return;
      }
      if (existing.epoch < epoch) {
        safeSendWebSocketEnvelope(existing.socket, {
          details: { currentEpoch: epoch, reason: "control_epoch_superseded" },
          error: "WebSocket control epoch was superseded",
        });
        safeCloseWebSocket(existing.socket, 1008, "control epoch superseded");
      }
    }
    this.sockets.set(key, { epoch, socket });
  }

  /** Removes a key only when the closing socket is still its current registrant. */
  remove(key: string, socket: WebSocket): void {
    const existing = this.sockets.get(key);
    if (existing && existing.socket === socket) {
      this.sockets.delete(key);
    }
  }
}

/** One registered current control socket and the epoch it was bound at. */
interface RegisteredControlSocket {
  readonly epoch: number;
  readonly socket: WebSocket;
}

/** Composes the control-socket registry key for one runtime instance identity. */
export function controlSocketKey(
  sessionId: string,
  participantId: string,
  instanceId: string,
): string {
  return `${sessionId}\u0000${participantId}\u0000${instanceId}`;
}

/** Wires participant WebSocket upgrades into an HTTP server. */
export function createParticipantWebSocketGateway(
  input: ParticipantWebSocketGatewayInput,
): WebSocketServer {
  const wsServer = new WebSocketServer({
    maxPayload: input.resourceLimitRuntime.limits.wsMaxPayloadBytes,
    noServer: true,
  });
  const heartbeat = startWebSocketHeartbeat(wsServer);
  const controlSocketRegistry = new ControlSocketRegistry();
  input.server.on("upgrade", (request, socket, head) => {
    handleParticipantWebSocketUpgrade({
      head,
      auth: input.auth,
      controlSocketRegistry,
      hub: input.hub,
      hostPresence: input.hostPresence,
      replicaId: input.replicaId,
      resourceLimitRuntime: input.resourceLimitRuntime,
      request,
      service: input.service,
      socket,
      wsServer,
    });
  });
  const originalClose = wsServer.close.bind(wsServer);
  wsServer.close = ((callback?: (error?: Error) => void): void => {
    heartbeat.stop();
    originalClose(callback);
  }) as typeof wsServer.close;
  return wsServer;
}

interface ParticipantWebSocketUpgradeInput {
  readonly auth: AuthRuntime;
  readonly controlSocketRegistry: ControlSocketRegistry;
  readonly head: Buffer;
  readonly hostPresence: HostPresenceRuntime;
  readonly hub: SubscriptionHub;
  readonly replicaId: string;
  readonly resourceLimitRuntime: ResourceLimitRuntime;
  readonly request: IncomingMessage;
  readonly service: SessionServiceEffect;
  readonly socket: Duplex;
  readonly wsServer: WebSocketServer;
}

/** Handles one participant stream upgrade request. */
function handleParticipantWebSocketUpgrade(input: ParticipantWebSocketUpgradeInput): void {
  const url = input.request.url ? new URL(input.request.url, "http://localhost") : null;
  const match = url?.pathname.match(/^\/sessions\/([^/]+)\/stream$/u);
  const sessionId = match?.[1] ? decodeURIComponent(match[1]) : null;
  if (!url || !sessionId) {
    input.socket.destroy();
    return;
  }
  let authContext: AuthContext | null;
  try {
    authContext = input.auth.authenticateWebSocketUpgrade(input.request, url);
  } catch (error) {
    const reason = authErrorFromUnknown(error);
    input.wsServer.handleUpgrade(input.request, input.socket, input.head, (webSocket) => {
      input.wsServer.emit("connection", webSocket, input.request);
      observeWebSocketPayloadErrors(webSocket, input.resourceLimitRuntime);
      safeSendWebSocketEnvelope(webSocket, {
        details: { reason },
        error: "Unauthorized WebSocket connection",
      });
      safeCloseWebSocket(webSocket, 1008, "unauthorized");
    });
    return;
  }
  input.wsServer.handleUpgrade(input.request, input.socket, input.head, (webSocket) => {
    input.wsServer.emit("connection", webSocket, input.request);
    observeWebSocketPayloadErrors(webSocket, input.resourceLimitRuntime);
    void Effect.runPromise(
      handleWebSocket(
        input.service,
        input.hub,
        input.resourceLimitRuntime,
        input.replicaId,
        authContext,
        sessionId,
        url.searchParams,
        webSocket,
        input.hostPresence,
        input.controlSocketRegistry,
      ),
    ).catch((error: unknown) => {
      console.error(error);
      safeCloseWebSocket(webSocket, 1011, "internal error");
    });
  });
}

/** Registers an upgraded WebSocket, replays historical events, and attaches live command handling. */
function handleWebSocket(
  service: SessionServiceEffect,
  hub: SubscriptionHub,
  resourceLimitRuntime: ResourceLimitRuntime,
  replicaId: string,
  authContext: AuthContext | null,
  sessionId: string,
  searchParams: URLSearchParams,
  socket: WebSocket,
  hostPresence: HostPresenceRuntime,
  controlSocketRegistry: ControlSocketRegistry,
): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    const readDenied = authorize({ action: "read", context: authContext, sessionId });
    if (readDenied) {
      socket.send(
        serializeErrorEnvelope({
          details: { reason: readDenied },
          error: "WebSocket session is not authorized",
        }),
      );
      socket.close(1008, "unauthorized");
      return;
    }
    const hostPresenceStream = classifyHostPresenceStream(searchParams.get("runtimeKind"));
    if (hostPresenceStream) {
      yield* handleHostPresenceWebSocket({
        hub,
        hostPresence,
        replicaId,
        resourceLimitRuntime,
        searchParams,
        service,
        sessionId,
        socket,
        streamKind: hostPresenceStream,
      });
      return;
    }
    const participantContext = yield* registerWebSocketParticipant(
      service,
      authContext,
      hub,
      sessionId,
      searchParams,
      socket,
      controlSocketRegistry,
    );
    if (participantContext) {
      const unregisterNativeSocket = hostPresence.registerNativeSocket();
      const controlKey = controlSocketKey(
        sessionId,
        participantContext.participantId,
        participantContext.instanceId,
      );
      const stopControlLeaseRefresh = startWebSocketControlLeaseRefresh({
        participantContext,
        service,
        sessionId,
        socket,
      });
      socket.on("close", () => {
        unregisterNativeSocket();
        controlSocketRegistry.remove(controlKey, socket);
        stopControlLeaseRefresh();
        void Effect.runPromise(
          service.releaseControlLease({
            controlChannel: "ws",
            controlEpoch: participantContext.controlEpoch,
            instanceId: participantContext.instanceId,
            participantId: participantContext.participantId,
            sessionId,
          }),
        ).catch((error: unknown) => {
          // The socket is already closing; a failed lease release only needs to
          // be logged so it does not surface as an unhandled rejection.
          console.error(error);
        });
      });
    }
    if (socket.readyState !== socket.OPEN) {
      return;
    }
    const afterSeq = parseAfterSeq(searchParams.get("after"));
    hub.add(sessionId, socket, { afterSeq, replaying: true });
    const rateLimiter = createWebSocketMessageRateLimiter({
      limit: resourceLimitRuntime.limits.wsMessageRateLimit,
      windowMs: resourceLimitRuntime.limits.wsMessageRateWindowMs,
    });
    socket.on("message", (data) => {
      const rateDecision = rateLimiter.check();
      if (!rateDecision.allowed) {
        resourceLimitRuntime.recordWsRateLimited();
        safeSendWebSocketEnvelope(socket, {
          details: {
            limit: rateDecision.limit,
            observed: rateDecision.observed,
            reason: resourceLimitReason.rateLimited,
            windowMs: rateDecision.windowMs,
          },
          error: "WebSocket message rate limit exceeded",
        });
        safeCloseWebSocket(socket, 1008, resourceLimitReason.rateLimited);
        return;
      }
      void Effect.runPromise(
        handleWebSocketMessage(
          service,
          hub,
          sessionId,
          participantContext,
          authContext,
          socket,
          data,
        ).pipe(
          Effect.catchAll((error) =>
            Effect.sync(() =>
              safeSendWebSocketEnvelope(socket, {
                error: error instanceof Error ? error.message : "Invalid message",
              }),
            ),
          ),
        ),
      ).catch((error: unknown) => {
        console.error(error);
      });
    });
    // Replay ordering invariant: replay.complete is emitted only after the hub
    // has delivered a contiguous prefix from the requested cursor through the
    // replay and buffered-live boundary, or the socket has been closed with a
    // typed replay_gap_unrepaired error.
    const replayLimit = resourceLimitRuntime.limits.wsReplayMaxEvents;
    const events = yield* service.listEvents(sessionId, afterSeq, { limit: replayLimit + 1 });
    if (events.length > replayLimit) {
      resourceLimitRuntime.recordReplayWindowExceeded();
      safeSendWebSocketEnvelope(socket, {
        details: {
          limit: replayLimit,
          reason: resourceLimitReason.replayWindowExceeded,
        },
        error: "WebSocket replay window exceeded",
      });
      safeCloseWebSocket(socket, 1013, resourceLimitReason.replayWindowExceeded);
      return;
    }
    for (const event of events) {
      hub.sendReplayEvent(sessionId, socket, event);
    }
    yield* repairReplayGaps({
      hub,
      replayLimit,
      service,
      sessionId,
      socket,
    });
    if (socket.readyState !== socket.OPEN) {
      return;
    }
    hub.completeReplay(sessionId, socket);
    socket.send(serializeReplayCompleteEnvelope());
  });
}

/**
 * Handles passive read-only streams without durable participant writes.
 *
 * `host` and `viewer` streams deliver process-local host-presence frames
 * alongside the durable event stream. The `observer` stream is a passive
 * full-event reader that receives the same durable replay and live event
 * delivery (identical hub backpressure and replay-gap repair as the participant
 * path) but sends no presence frames, so a bare `SessionEventStreamClient` that
 * only understands event, replay.complete, and error envelopes is not fed
 * presence frames it cannot parse. None of these streams register a durable
 * participant or acquire a control lease.
 */
function handleHostPresenceWebSocket(
  input: HostPresenceWebSocketInput,
): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    const unregisterPassiveSocket = input.hostPresence.registerPassiveSocket();
    const deliversPresence = input.streamKind !== "observer";
    const host = input.streamKind === "host" ? hostFromSearchParams(input.searchParams) : null;
    const sendPresence = (): void => {
      safeSendPresenceEnvelope(
        input.socket,
        projectWebSocketPresenceEnvelope({
          replicaId: input.replicaId,
          runtime: input.hostPresence,
          sessionId: input.sessionId,
        }),
      );
    };
    const unsubscribePresence = deliversPresence
      ? input.hostPresence.subscribePresence(input.sessionId, sendPresence)
      : null;
    input.socket.on("close", () => {
      unregisterPassiveSocket();
      unsubscribePresence?.();
      if (host) {
        input.hostPresence.removeHost(input.sessionId, host.instanceId);
        input.hostPresence.notifyPresence(input.sessionId);
      }
    });
    if (host) {
      input.hostPresence.upsertHost(input.sessionId, host);
      input.hostPresence.notifyPresence(input.sessionId);
    }
    if (input.socket.readyState !== input.socket.OPEN) {
      return;
    }
    if (deliversPresence) {
      sendPresence();
    }
    const afterSeq = parseAfterSeq(input.searchParams.get("after"));
    input.hub.add(input.sessionId, input.socket, { afterSeq, replaying: true });
    const rateLimiter = createWebSocketMessageRateLimiter({
      limit: input.resourceLimitRuntime.limits.wsMessageRateLimit,
      windowMs: input.resourceLimitRuntime.limits.wsMessageRateWindowMs,
    });
    input.socket.on("message", () => {
      const rateDecision = rateLimiter.check();
      if (!rateDecision.allowed) {
        input.resourceLimitRuntime.recordWsRateLimited();
        safeSendWebSocketEnvelope(input.socket, {
          details: {
            limit: rateDecision.limit,
            observed: rateDecision.observed,
            reason: resourceLimitReason.rateLimited,
            windowMs: rateDecision.windowMs,
          },
          error: "WebSocket message rate limit exceeded",
        });
        safeCloseWebSocket(input.socket, 1008, resourceLimitReason.rateLimited);
        return;
      }
      safeSendWebSocketEnvelope(input.socket, {
        details: { category: "passive_stream" },
        error:
          input.streamKind === "observer"
            ? "Passive observer stream is read-only"
            : "Host-presence stream is read-only",
      });
    });
    const replayLimit = input.resourceLimitRuntime.limits.wsReplayMaxEvents;
    const events = yield* input.service.listEvents(input.sessionId, afterSeq, {
      limit: replayLimit + 1,
    });
    if (events.length > replayLimit) {
      input.resourceLimitRuntime.recordReplayWindowExceeded();
      safeSendWebSocketEnvelope(input.socket, {
        details: {
          limit: replayLimit,
          reason: resourceLimitReason.replayWindowExceeded,
        },
        error: "WebSocket replay window exceeded",
      });
      safeCloseWebSocket(input.socket, 1013, resourceLimitReason.replayWindowExceeded);
      return;
    }
    for (const event of events) {
      input.hub.sendReplayEvent(input.sessionId, input.socket, event);
    }
    yield* repairReplayGaps({
      hub: input.hub,
      replayLimit,
      service: input.service,
      sessionId: input.sessionId,
      socket: input.socket,
    });
    if (input.socket.readyState !== input.socket.OPEN) {
      return;
    }
    input.hub.completeReplay(input.sessionId, input.socket);
    input.socket.send(serializeReplayCompleteEnvelope());
  });
}

interface ReplayGapRepairInput {
  readonly hub: SubscriptionHub;
  readonly replayLimit: number;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
  readonly socket: WebSocket;
}

/** Structured diagnostics for a WebSocket replay gap repair failure. */
interface ReplayGapRepairFailureLogDetails {
  readonly contiguousDeliveredSeq: number;
  readonly pendingEventCount: number;
  readonly pendingSeqs: readonly number[];
  readonly sessionId: string;
}

/** Repairs buffered live gaps before replay.complete is emitted. */
function repairReplayGaps(input: ReplayGapRepairInput): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    let repairedEventCount = 0;
    while (input.socket.readyState === input.socket.OPEN) {
      const state = input.hub.replayState(input.sessionId, input.socket);
      if (!state || state.pendingEventCount === 0) {
        return;
      }
      if (repairedEventCount >= input.replayLimit) {
        closeUnrepairedReplayGap(input, state);
        return;
      }
      const remainingLimit = input.replayLimit - repairedEventCount;
      const repairEvents = yield* input.service.listEvents(
        input.sessionId,
        state.contiguousDeliveredSeq,
        { limit: remainingLimit + 1 },
      );
      if (repairEvents.length === 0 || repairEvents.length > remainingLimit) {
        closeUnrepairedReplayGap(input, state);
        return;
      }
      input.hub.recordReplayGapRepair(repairEvents.length);
      const beforeRepairSeq = state.contiguousDeliveredSeq;
      for (const event of repairEvents) {
        input.hub.sendReplayEvent(input.sessionId, input.socket, event);
      }
      repairedEventCount += repairEvents.length;
      const repairedState = input.hub.replayState(input.sessionId, input.socket);
      if (!repairedState || repairedState.contiguousDeliveredSeq <= beforeRepairSeq) {
        closeUnrepairedReplayGap(input, state);
        return;
      }
    }
  });
}

/** Emits a typed replay gap failure and closes the socket. */
function closeUnrepairedReplayGap(
  input: ReplayGapRepairInput,
  state: {
    readonly contiguousDeliveredSeq: number;
    readonly pendingEventCount: number;
    readonly pendingSeqs: readonly number[];
  },
): void {
  logReplayGapRepairFailure({
    contiguousDeliveredSeq: state.contiguousDeliveredSeq,
    pendingEventCount: state.pendingEventCount,
    pendingSeqs: state.pendingSeqs,
    sessionId: input.sessionId,
  });
  safeSendWebSocketEnvelope(input.socket, {
    details: {
      contiguousDeliveredSeq: state.contiguousDeliveredSeq,
      pendingEventCount: state.pendingEventCount,
      pendingSeqs: state.pendingSeqs,
      reason: resourceLimitReason.replayGapUnrepaired,
    },
    error: "WebSocket replay gap could not be repaired",
  });
  safeCloseWebSocket(input.socket, 1013, resourceLimitReason.replayGapUnrepaired);
}

/** Emits one structured replay repair failure log line without console APIs. */
function logReplayGapRepairFailure(details: ReplayGapRepairFailureLogDetails): void {
  process.stderr.write(
    `${JSON.stringify({
      details,
      event: "websocket.replay_gap_unrepaired",
    })}\n`,
  );
}

/** Counts oversized payload errors emitted by the ws receiver. */
function observeWebSocketPayloadErrors(
  socket: WebSocket,
  resourceLimitRuntime: ResourceLimitRuntime,
): void {
  socket.on("error", (error: Error & { readonly code?: string | undefined }) => {
    if (error.code === "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH") {
      resourceLimitRuntime.recordWsPayloadTooLarge();
    }
  });
}

/** Claims WebSocket control for a participant identity and emits visible registration events. */
function registerWebSocketParticipant(
  service: SessionServiceEffect,
  authContext: AuthContext | null,
  hub: SubscriptionHub,
  sessionId: string,
  searchParams: URLSearchParams,
  socket: WebSocket,
  controlSocketRegistry: ControlSocketRegistry,
): Effect.Effect<ParticipantControlContext | null, unknown> {
  return Effect.gen(function* () {
    if (authContext?.role === "observer") {
      return null;
    }
    const denied = authorize({ action: "task-mutate", context: authContext, sessionId });
    if (denied) {
      socket.send(serializeErrorEnvelope({ error: "WebSocket participant is not authorized" }));
      socket.close(1008, "unauthorized");
      return null;
    }
    const requestedParticipantId = searchParams.get("participantId");
    const identityDenied = authorizeParticipantIdentity(authContext, requestedParticipantId);
    if (identityDenied) {
      socket.send(serializeErrorEnvelope({ error: "WebSocket participant identity mismatch" }));
      socket.close(1008, "identity mismatch");
      return null;
    }
    const participantId = authContext
      ? effectiveParticipantId(authContext, requestedParticipantId)
      : requestedParticipantId;
    if (!participantId) {
      return null;
    }
    const instanceId = searchParams.get("instanceId") ?? participantId;
    const runtimeKind = yield* Effect.try({
      catch: (error) => error,
      try: () => participantRuntimeKindSchema.parse(searchParams.get("runtimeKind") ?? undefined),
    });
    const result = yield* service.registerWebSocketParticipant({
      capabilities: parseCapabilitiesParam(searchParams.get("capabilities")),
      displayName: searchParams.get("displayName") ?? participantId,
      instanceId,
      participantId,
      runtimeKind,
      sessionId,
    });
    if (result.status === "control_conflict") {
      socket.send(
        serializeErrorEnvelope({
          details: controlLeaseConflictError(result.leaseClaim, "ws"),
          error: "Participant already has an active control channel",
        }),
      );
      socket.close(1008, "control channel conflict");
      return null;
    }
    if (result.status === "control_epoch_stale") {
      socket.send(
        serializeErrorEnvelope({
          details: { currentEpoch: result.currentEpoch, reason: "control_epoch_stale" },
          error: "WebSocket control epoch is stale",
        }),
      );
      socket.close(1008, "control epoch stale");
      return null;
    }
    if (result.status !== "ok") {
      socket.send(
        serializeErrorEnvelope({
          details: { reason: result.status },
          error: "WebSocket participant registration failed",
        }),
      );
      socket.close(1011, "participant registration failed");
      return null;
    }
    // Record this socket as the current control owner for its runtime identity
    // and proactively close any prior-epoch socket for the same key. The new
    // epoch is already committed at this point, so the superseded socket is
    // fenced immediately rather than on its next command or refresh.
    controlSocketRegistry.register(
      controlSocketKey(sessionId, result.context.participantId, result.context.instanceId),
      socket,
      result.context.controlEpoch,
    );
    broadcastEvents(hub, result.events);
    return result.context;
  });
}

/** Handles one client-to-server WebSocket command or publish message. */
function handleWebSocketMessage(
  service: SessionServiceEffect,
  hub: SubscriptionHub,
  sessionId: string,
  participantContext: ParticipantControlContext | null,
  authContext: AuthContext | null,
  socket: WebSocket,
  data: WebSocket.RawData,
): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    const raw = yield* Effect.try({
      catch: (error) => error,
      try: () => JSON.parse(String(data)) as unknown,
    });
    const commandContext = readWebSocketCommandContext(raw);
    yield* handleParsedWebSocketMessage(
      service,
      hub,
      sessionId,
      participantContext,
      authContext,
      socket,
      raw,
      commandContext,
    ).pipe(
      Effect.catchAll((error) =>
        Effect.sync(() => {
          if (commandContext?.requestId) {
            sendCommandContextError(
              socket,
              commandContext,
              error instanceof Error ? error.message : "Invalid message",
              { category: "command_processing_failed" },
            );
            return;
          }
          safeSendWebSocketEnvelope(socket, {
            error: error instanceof Error ? error.message : "Invalid message",
          });
        }),
      ),
    );
  });
}

/** Handles an already-parsed WebSocket command with syntactic command context. */
function handleParsedWebSocketMessage(
  service: SessionServiceEffect,
  hub: SubscriptionHub,
  sessionId: string,
  participantContext: ParticipantControlContext | null,
  authContext: AuthContext | null,
  socket: WebSocket,
  raw: unknown,
  commandContext: WebSocketCommandContext | null,
): Effect.Effect<void, unknown> {
  return Effect.gen(function* () {
    const op = commandContext?.op ?? readWebSocketOp(raw);
    const commandSpec = findClientWebSocketCommandSpec(op);
    if (!commandSpec) {
      sendCommandContextError(socket, commandContext, `Unsupported WebSocket op: ${op}`, {
        category: "unsupported_command",
      });
      return;
    }
    if (op === webSocketOperation.publish) {
      const denied = authorize({ action: "publish", context: authContext, sessionId });
      if (denied) {
        sendCommandContextError(socket, commandContext, "WebSocket publish is not authorized", {
          category: "authorization_failed",
        });
        return;
      }
      const parsed = yield* Effect.try({
        catch: (error) => error,
        try: () => wsPublishMessageSchema.parse(raw),
      });
      const publishPolicy = authorizeClientPublishedEvent({
        authContext,
        boundParticipantId: participantContext?.participantId,
        producerId: parsed.producerId,
        type: parsed.type,
      });
      if (publishPolicy.status === "denied") {
        sendCommandError(socket, parsed.requestId, "WebSocket publish is not authorized", {
          reason: publishPolicy.reason,
        });
        return;
      }
      // Fence a participant-owned publish through the socket's server-bound epoch
      // before appending, exactly like the other protected commands. A superseded
      // socket is rejected and closed so it can no longer append participant-owned
      // events. Non-participant authorized producers keep their existing path.
      const publishIsParticipantBound =
        participantContext !== null &&
        publishPolicy.producerId === participantContext.participantId;
      if (publishIsParticipantBound && participantContext) {
        const controlCheck = yield* service.refreshWebSocketControlLease({
          controlEpoch: participantContext.controlEpoch,
          instanceId: participantContext.instanceId,
          participantId: participantContext.participantId,
          sessionId,
        });
        if (controlCheck.status !== "ok") {
          sendCommandError(socket, parsed.requestId, "WebSocket control epoch is stale", {
            reason: "control_epoch_stale",
            ...(controlCheck.status === "control_epoch_stale"
              ? { currentEpoch: controlCheck.currentEpoch }
              : {}),
          });
          safeCloseWebSocket(socket, 1008, "control epoch stale");
          return;
        }
      }
      const publishControlGuard =
        publishIsParticipantBound && participantContext
          ? wsControlGuard(participantContext, sessionId)
          : undefined;
      const result = yield* service.publishEvent({
        ...(publishControlGuard !== undefined ? { controlGuard: publishControlGuard } : {}),
        eventId: parsed.eventId,
        payload: parsed.payload,
        producerId: publishPolicy.producerId,
        sessionId,
        type: parsed.type,
      });
      if (result.status === "conflict") {
        sendCommandError(socket, parsed.requestId, "Event id conflict", {
          reason: "event_id_conflict",
        });
        return;
      }
      broadcastEvents(hub, result.events);
      if (parsed.requestId !== undefined) {
        sendCommandResult(socket, parsed.requestId, webSocketOperation.publish, {
          event: result.event,
          status: result.status,
        });
      }
      return;
    }
    const context = commandSpec.requiresParticipant
      ? requireWsParticipantContext(socket, participantContext, commandContext)
      : participantContext;
    if (!context) {
      return;
    }
    const commandDenied = authorize({ action: "task-mutate", context: authContext, sessionId });
    if (commandDenied) {
      sendCommandContextError(socket, commandContext, "WebSocket command is not authorized", {
        category: "authorization_failed",
      });
      return;
    }
    // Fence protected commands to the epoch bound to this socket at acquisition.
    // A superseded socket's bound epoch is no longer current, so it is rejected
    // with CONTROL_EPOCH_STALE and closed before any mutation runs. The command
    // never trusts a caller-supplied epoch; the server uses the socket's bound
    // epoch.
    const controlCheck = yield* service.refreshWebSocketControlLease({
      controlEpoch: context.controlEpoch,
      instanceId: context.instanceId,
      participantId: context.participantId,
      sessionId,
    });
    if (controlCheck.status !== "ok") {
      sendCommandContextError(socket, commandContext, "WebSocket control epoch is stale", {
        category: "control_epoch_stale",
        ...(controlCheck.status === "control_epoch_stale"
          ? { currentEpoch: controlCheck.currentEpoch }
          : {}),
      });
      safeCloseWebSocket(socket, 1008, "control epoch stale");
      return;
    }
    if (op === webSocketOperation.taskClaim) {
      const parsed = yield* parseWebSocketMessage(raw, wsTaskClaimMessageSchema);
      const result = yield* service.claimTask({
        controlGuard: wsControlGuard(context, sessionId),
        participantId: context.participantId,
        sessionId,
        taskId: parsed.taskId,
      });
      broadcastEvents(hub, result.events);
      sendCommandResult(socket, parsed.requestId, webSocketOperation.taskClaim, {
        task: result.task,
      });
      return;
    }
    if (
      yield* handleWebSocketTaskMutationCommand({
        command: webSocketOperation.taskCancel,
        context,
        hub,
        mutate: ({ body, participantId }) =>
          service.cancelTask({
            controlGuard: wsControlGuard(context, sessionId),
            participantId,
            reason: body.reason,
            sessionId,
            taskId: body.taskId,
          }),
        raw,
        schema: wsTaskCancelMessageSchema,
        socket,
      })
    ) {
      return;
    }
    if (op === webSocketOperation.taskRefresh) {
      const parsed = yield* parseWebSocketMessage(raw, wsTaskRefreshMessageSchema);
      const result = yield* service.refreshTaskClaim({
        controlGuard: wsControlGuard(context, sessionId),
        participantId: context.participantId,
        sessionId,
        taskId: parsed.taskId,
      });
      sendWebSocketTaskClaimRefreshResult(
        socket,
        result,
        parsed.requestId,
        webSocketOperation.taskRefresh,
      );
      return;
    }
    if (
      yield* handleWebSocketTaskMutationCommand({
        command: webSocketOperation.taskComplete,
        context,
        hub,
        mutate: ({ body, participantId }) =>
          service.completeTask({
            controlGuard: wsControlGuard(context, sessionId),
            participantId,
            result: body.result,
            sessionId,
            taskId: body.taskId,
          }),
        raw,
        schema: wsTaskCompleteMessageSchema,
        socket,
      })
    ) {
      return;
    }
    if (
      yield* handleWebSocketTaskMutationCommand({
        command: webSocketOperation.taskFail,
        context,
        hub,
        mutate: ({ body, participantId }) =>
          service.failTask({
            controlGuard: wsControlGuard(context, sessionId),
            failure: body.failure,
            participantId,
            sessionId,
            taskId: body.taskId,
          }),
        raw,
        schema: wsTaskFailMessageSchema,
        socket,
      })
    ) {
      return;
    }
    if (
      yield* handleWebSocketTaskMutationCommand({
        command: webSocketOperation.taskRelease,
        context,
        hub,
        mutate: ({ body, participantId }) =>
          service.releaseTask({
            controlGuard: wsControlGuard(context, sessionId),
            participantId,
            sessionId,
            taskId: body.taskId,
          }),
        raw,
        schema: wsTaskReleaseMessageSchema,
        socket,
      })
    ) {
      return;
    }
  });
}

/** Keeps a WebSocket participant's durable control lease fresh while the socket remains open. */
function startWebSocketControlLeaseRefresh(input: WebSocketControlLeaseRefreshInput): () => void {
  const fiber = Effect.runFork(webSocketControlLeaseRefreshLoop(input));
  return () => {
    void Effect.runPromise(Fiber.interrupt(fiber));
  };
}

/** Keeps a WebSocket participant's durable control lease fresh until the socket closes. */
function webSocketControlLeaseRefreshLoop(
  input: WebSocketControlLeaseRefreshInput,
): Effect.Effect<void, never> {
  const intervalMs = webSocketControlLeaseRefreshIntervalMs(
    input.service.debugInfo().wsControlLeaseTtlMs,
  );
  const closeForRefreshError = (
    error: string,
    details: Record<string, unknown> | undefined,
    closeCode: number,
    closeReason: string,
  ): void => {
    if (input.socket.readyState !== input.socket.OPEN) {
      return;
    }
    input.socket.send(serializeErrorEnvelope({ ...(details ? { details } : {}), error }));
    input.socket.close(closeCode, closeReason);
  };
  return Effect.gen(function* () {
    while (input.socket.readyState === input.socket.OPEN) {
      yield* sleepUnrefEffect(intervalMs);
      if (input.socket.readyState !== input.socket.OPEN) {
        return;
      }
      const shouldContinue = yield* input.service
        .refreshWebSocketControlLease({
          controlEpoch: input.participantContext.controlEpoch,
          instanceId: input.participantContext.instanceId,
          participantId: input.participantContext.participantId,
          sessionId: input.sessionId,
        })
        .pipe(
          Effect.match({
            onFailure: (error) => {
              closeForRefreshError(
                error instanceof Error
                  ? error.message
                  : "Failed to refresh WebSocket control lease",
                undefined,
                1011,
                "control lease refresh failed",
              );
              return false;
            },
            onSuccess: (result) => {
              if (result.status === "control_conflict") {
                closeForRefreshError(
                  "Participant already has an active control channel",
                  controlLeaseConflictError(result.leaseClaim, "ws"),
                  1008,
                  "control channel conflict",
                );
                return false;
              }
              if (result.status === "control_epoch_stale") {
                // The bound epoch was fenced by a newer acquisition. The
                // superseded socket must close so it can no longer renew or
                // apply protected commands.
                closeForRefreshError(
                  "WebSocket control epoch was superseded",
                  { currentEpoch: result.currentEpoch, reason: "control_epoch_stale" },
                  1008,
                  "control epoch stale",
                );
                return false;
              }
              return true;
            },
          }),
        );
      if (!shouldContinue) {
        return;
      }
    }
  });
}

/** Refreshes the lease at half its TTL to tolerate normal timer jitter. */
function webSocketControlLeaseRefreshIntervalMs(leaseTtlMs: number): number {
  return Math.max(1, Math.floor(leaseTtlMs / 2));
}

/** Sends the WebSocket command response for mutating task commands. */
function sendWebSocketTaskMutationResult(
  socket: WebSocket,
  hub: SubscriptionHub,
  result: TaskMutationResult,
  requestId: string | undefined,
  command: string,
  rejectedIsError: boolean,
): void {
  if (result.status === "rejected") {
    if (rejectedIsError) {
      sendCommandError(
        socket,
        requestId,
        "Task is not claimed by this participant or is already terminal",
      );
      return;
    }
    sendCommandResult(socket, requestId, command, { task: result.task });
    return;
  }
  broadcastEvents(hub, result.events);
  sendCommandResult(socket, requestId, command, { task: result.task });
}

/** Matches and executes one WebSocket task mutation command definition. */
function handleWebSocketTaskMutationCommand<
  TBody extends {
    readonly requestId?: string | undefined;
    readonly taskId: string;
  },
>(input: WebSocketTaskMutationCommandInput<TBody>): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    if (readWebSocketOp(input.raw) !== input.command) {
      return false;
    }
    const body = yield* parseWebSocketMessage(input.raw, input.schema);
    const result = yield* input.mutate({
      body,
      participantId: input.context.participantId,
    });
    sendWebSocketTaskMutationResult(
      input.socket,
      input.hub,
      result,
      body.requestId,
      input.command,
      true,
    );
    return true;
  });
}

/** Sends the WebSocket command response for task claim-refresh commands. */
function sendWebSocketTaskClaimRefreshResult(
  socket: WebSocket,
  result: TaskClaimRefreshResult,
  requestId: string | undefined,
  command: string,
): void {
  if (result.status === "rejected") {
    sendCommandError(socket, requestId, "Task claim is missing, expired, or terminal");
    return;
  }
  sendCommandResult(socket, requestId, command, { task: result.task });
}

/** Reads the WebSocket operation from an unknown client message. */
function readWebSocketOp(value: unknown): string {
  if (
    typeof value === "object" &&
    value !== null &&
    "op" in value &&
    typeof value.op === "string"
  ) {
    return value.op;
  }
  throw new Error("WebSocket message is missing op");
}

/** Reads syntactic command metadata before command-specific schema parsing. */
function readWebSocketCommandContext(value: unknown): WebSocketCommandContext | null {
  if (
    typeof value === "object" &&
    value !== null &&
    "op" in value &&
    typeof value.op === "string"
  ) {
    const requestId =
      "requestId" in value && typeof value.requestId === "string" ? value.requestId : undefined;
    const taskId = "taskId" in value && typeof value.taskId === "string" ? value.taskId : undefined;
    return {
      op: value.op,
      ...(requestId !== undefined ? { requestId } : {}),
      ...(taskId !== undefined ? { taskId } : {}),
    };
  }
  return null;
}

/**
 * Builds the atomic Control Epoch fence for a WebSocket-bound participant. The
 * epoch is the one bound to this socket at acquisition; it is validated inside
 * the same transaction as the protected mutation, so a socket fenced between the
 * per-command epoch check and the write can never apply the mutation.
 */
function wsControlGuard(context: ParticipantControlContext, sessionId: string): ControlEpochGuard {
  return {
    controlChannel: "ws",
    controlEpoch: context.controlEpoch,
    instanceId: context.instanceId,
    participantId: context.participantId,
    sessionId,
  };
}

/** Requires a WebSocket participant context before accepting task commands. */
function requireWsParticipantContext(
  socket: WebSocket,
  participantContext: ParticipantControlContext | null,
  commandContext: WebSocketCommandContext | null,
): ParticipantControlContext | null {
  if (participantContext) {
    return participantContext;
  }
  sendCommandContextError(
    socket,
    commandContext,
    "WebSocket command requires participant presence",
    { category: "participant_required" },
  );
  return null;
}

/** Closes a WebSocket only when it is still open, ignoring already-closed sockets. */
function safeCloseWebSocket(socket: WebSocket, code: number, reason: string): void {
  if (socket.readyState === socket.OPEN) {
    socket.close(code, reason);
  }
}

/** Sends an error envelope to a WebSocket, ignoring failures from already-closed sockets. */
function safeSendWebSocketEnvelope(
  socket: WebSocket,
  envelope: { readonly details?: Record<string, unknown>; readonly error: string },
): void {
  if (socket.readyState !== socket.OPEN) {
    return;
  }
  try {
    socket.send(serializeErrorEnvelope(envelope));
  } catch (error) {
    // A concurrent close can make send throw; keep it out of the event loop.
    console.error(error);
  }
}

/** Sends a Host-presence process-local presence frame. */
function safeSendPresenceEnvelope(socket: WebSocket, envelope: WebSocketPresenceEnvelope): void {
  if (socket.readyState !== socket.OPEN) {
    return;
  }
  try {
    socket.send(JSON.stringify(envelope));
  } catch (error) {
    console.error(error);
  }
}

/** Sends a successful WebSocket command result envelope. */
function sendCommandResult(
  socket: WebSocket,
  requestId: string | undefined,
  command: string,
  payload: Record<string, unknown>,
): void {
  socket.send(
    serializeCommandResultEnvelope({
      command,
      payload,
      ...(requestId !== undefined ? { requestId } : {}),
    }),
  );
}

/** Sends a WebSocket command error envelope. */
function sendCommandError(
  socket: WebSocket,
  requestId: string | undefined,
  error: string,
  details?: Record<string, unknown>,
): void {
  socket.send(
    serializeErrorEnvelope({
      ...(details ? { details } : {}),
      error,
      ...(requestId !== undefined ? { requestId } : {}),
    }),
  );
}

/** Sends a command-scoped error with bounded diagnostics from syntactic context. */
function sendCommandContextError(
  socket: WebSocket,
  context: WebSocketCommandContext | null,
  error: string,
  details: Record<string, unknown> = {},
): void {
  sendCommandError(socket, context?.requestId, error, {
    ...details,
    ...(context?.op !== undefined ? { command: context.op } : {}),
    ...(context?.taskId !== undefined ? { taskId: context.taskId } : {}),
  });
}

/** Parses optional URL-encoded participant capabilities from a WebSocket registration request. */
function parseCapabilitiesParam(value: string | null): Record<string, unknown> {
  if (!value) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    // Malformed capabilities JSON is treated as no capabilities so a bad query
    // parameter fails registration cleanly instead of throwing out of the
    // upgrade Effect as an unhandled rejection.
    return {};
  }
  return zodRecord(parsed);
}

/** Reads host-presence host identity fields from a passive host stream query string. */
function hostFromSearchParams(searchParams: URLSearchParams): LiveHostPresence {
  const participantId = searchParams.get("participantId") ?? "host";
  const instanceId = searchParams.get("instanceId") ?? participantId;
  return {
    displayName: searchParams.get("displayName") ?? participantId,
    instanceId,
    participantId,
  };
}

/** Validates an unknown value as a string-keyed JSON record. */
function zodRecord(value: unknown): Record<string, unknown> {
  return registerParticipantSchema.shape.capabilities.parse(value);
}

/** Validates one decoded WebSocket command payload inside the WebSocket Effect command handler. */
function parseWebSocketMessage<TValue>(
  raw: unknown,
  schema: z.ZodType<TValue>,
): Effect.Effect<TValue, unknown> {
  return Effect.try({ catch: (error) => error, try: () => schema.parse(raw) });
}

/**
 * Default interval for server-side WebSocket heartbeat pings. Intermediaries
 * (load balancers, NAT) commonly drop idle connections well before TCP
 * keepalive notices; periodic pings detect and close those zombie sockets so
 * the durable control-lease refresh loop does not keep refreshing a lease held
 * by a dead runtime.
 */
export const defaultWebSocketHeartbeatIntervalMs = 30_000;

/** Tracks liveness for one connected WebSocket so dead peers can be terminated. */
const heartbeatAlive = Symbol("tether/websocket-heartbeat-alive");

interface WebSocketHeartbeat {
  /** Stops the periodic ping loop and clears its timer. */
  readonly stop: () => void;
}

/**
 * Starts a server-wide heartbeat that pings every connected client on an
 * interval and terminates any client that fails to respond before the next
 * tick. Returns a handle whose `stop` clears the timer; `createParticipantWebSocketGateway`
 * wires `stop` into `wsServer.close`.
 */
function startWebSocketHeartbeat(
  wsServer: WebSocketServer,
  intervalMs: number = defaultWebSocketHeartbeatIntervalMs,
): WebSocketHeartbeat {
  const markAlive = (socket: WebSocket): void => {
    (socket as unknown as Record<symbol, boolean>)[heartbeatAlive] = true;
  };
  wsServer.on("connection", (socket) => {
    markAlive(socket);
    socket.on("pong", () => {
      markAlive(socket);
    });
  });
  const timer = setInterval(() => {
    for (const socket of wsServer.clients) {
      if ((socket as unknown as Record<symbol, boolean>)[heartbeatAlive] === false) {
        socket.terminate();
        continue;
      }
      (socket as unknown as Record<symbol, boolean>)[heartbeatAlive] = false;
      socket.ping();
    }
  }, intervalMs);
  timer.unref?.();
  return {
    stop: () => {
      clearInterval(timer);
    },
  };
}
