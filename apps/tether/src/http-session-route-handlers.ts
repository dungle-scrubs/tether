import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { Effect } from "effect";

import {
  authorize,
  authorizeParticipantIdentity,
  effectiveParticipantId,
} from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import {
  broadcastEvents,
  parseJsonBody,
  sendAuthError,
  sendControlEpochStale,
  sendControlLeaseConflict,
  sendJson,
} from "./http-route-runtime.js";
import { defineHttpRoute, type HttpRouteSpec, matchHttpRoute } from "./http-route-spec.js";
import type { SubscriptionHub } from "./hub.js";
import {
  appendEventSchema,
  createSessionSchema,
  heartbeatParticipantSchema,
  parseAfterSeq,
  registerParticipantSchema,
} from "./protocol.js";
import { parseEventListLimit, type ResourceLimits } from "./resource-limits.js";
import { authorizeClientPublishedEvent } from "./session-event-publish-policy.js";
import type { SessionServiceEffect } from "./session-service.js";
import type { SessionEvent } from "./types.js";
import {
  findProjectedSession,
  permanentDeleteEligibility,
  projectSessionInventory,
  type HostPresenceRuntime,
} from "./host-presence.js";

interface SessionHttpRouteHandlerInput {
  readonly authContext: AuthContext | null;
  readonly hub: SubscriptionHub;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly service: SessionServiceEffect;
  readonly hostPresence: HostPresenceRuntime;
  readonly url: URL;
}

const sessionResourceRoutes = {
  context: defineHttpRoute({
    method: "GET",
    name: "session.context",
    pattern: /^\/sessions\/([^/]+)\/context$/u,
  }),
  delete: defineHttpRoute({
    method: "POST",
    name: "session.delete",
    pattern: /^\/sessions\/([^/]+)\/delete$/u,
  }),
  eventsAppend: defineHttpRoute({
    method: "POST",
    name: "session.events.append",
    pattern: /^\/sessions\/([^/]+)\/events$/u,
  }),
  eventsList: defineHttpRoute({
    method: "GET",
    name: "session.events.list",
    pattern: /^\/sessions\/([^/]+)\/events$/u,
  }),
  heartbeat: defineHttpRoute({
    method: "POST",
    name: "session.participant.heartbeat",
    pattern: /^\/sessions\/([^/]+)\/participants\/([^/]+)\/heartbeat$/u,
  }),
  participantList: defineHttpRoute({
    method: "GET",
    name: "session.participant.list",
    pattern: /^\/sessions\/([^/]+)\/participants$/u,
  }),
  participantRegister: defineHttpRoute({
    method: "POST",
    name: "session.participant.register",
    pattern: /^\/sessions\/([^/]+)\/participants$/u,
  }),
  taskContractList: defineHttpRoute({
    method: "GET",
    name: "session.task-contract.list",
    pattern: /^\/sessions\/([^/]+)\/task-contracts$/u,
  }),
  taskContractRead: defineHttpRoute({
    method: "GET",
    name: "session.task-contract.read",
    pattern: /^\/sessions\/([^/]+)\/task-contracts\/([^/]+)$/u,
  }),
} as const;

type SessionResourceRouteMatch =
  | { readonly resource: "context"; readonly sessionId: string }
  | { readonly resource: "delete"; readonly sessionId: string }
  | { readonly resource: "events-append"; readonly sessionId: string }
  | { readonly resource: "events-list"; readonly sessionId: string }
  | {
      readonly participantId: string;
      readonly resource: "heartbeat";
      readonly sessionId: string;
    }
  | { readonly resource: "participant-list"; readonly sessionId: string }
  | { readonly resource: "participant-register"; readonly sessionId: string }
  | { readonly resource: "task-contract-list"; readonly sessionId: string }
  | {
      readonly resource: "task-contract-read";
      readonly sessionId: string;
      readonly taskKind: string;
    };

/** Routes session-scoped REST resources outside task mutation routes. */
export function handleSessionHttpRoute(
  input: SessionHttpRouteHandlerInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const { hub, request, response, service, url } = input;
    if (request.method === "GET" && url.pathname === "/sessions") {
      if (!authorizeRoute(input, "read")) {
        return true;
      }
      const sessions = yield* service.listSessions();
      const eventsBySession = yield* collectSessionEvents(
        service,
        sessions.map((session) => session.sessionId),
      );
      sendJson(response, 200, {
        sessions: projectSessionInventory({
          eventsBySession,
          runtime: input.hostPresence,
          sessions,
        }),
      });
      return true;
    }
    if (request.method === "POST" && url.pathname === "/sessions") {
      if (!authorizeRoute(input, "session-create")) {
        return true;
      }
      const body = yield* parseJsonBody(request, createSessionSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: "session.create",
      });
      const result = yield* service.ensurePublicSession({ sessionId: body.sessionId });
      sendJson(response, 201, { session: result.session });
      return true;
    }
    const route = matchSessionResourceRoute(request.method, url.pathname);
    if (route?.resource === "delete") {
      if (!authorizeRoute(input, "session-create", route.sessionId)) {
        return true;
      }
      const sessions = yield* service.listSessions();
      const session = sessions.find((candidate) => candidate.sessionId === route.sessionId) ?? null;
      const events = session
        ? yield* service.listEvents(route.sessionId, 0, { limit: 10_000 })
        : [];
      const projected = findProjectedSession({
        events,
        liveHosts: input.hostPresence.hosts(route.sessionId),
        session,
      });
      const eligibility = permanentDeleteEligibility(projected);
      if (!eligibility.ok) {
        sendJson(response, eligibility.reason === "not-found" ? 404 : 409, {
          detail: eligibility.detail,
          ok: false,
          reason: eligibility.reason,
        });
        return true;
      }
      const deleted = yield* service.deleteSession({ sessionId: route.sessionId });
      sendJson(
        response,
        deleted ? 200 : 404,
        deleted
          ? { ok: true, sessionId: route.sessionId }
          : { detail: "session not found", ok: false, reason: "not-found" },
      );
      return true;
    }
    if (route?.resource === "events-list") {
      if (!authorizeRoute(input, "read", route.sessionId)) {
        return true;
      }
      const afterSeq = parseAfterSeq(url.searchParams.get("after"));
      const limit = parseEventListLimit(url.searchParams.get("limit"), input.resourceLimits);
      const eventsWithLookahead = yield* service.listEvents(route.sessionId, afterSeq, {
        limit: limit + 1,
      });
      const hasMore = eventsWithLookahead.length > limit;
      const events = hasMore ? eventsWithLookahead.slice(0, limit) : eventsWithLookahead;
      const nextAfterSeq = events.at(-1)?.seq ?? afterSeq;
      sendJson(response, 200, {
        events,
        pagination: {
          afterSeq,
          hasMore,
          limit,
          nextAfterSeq,
          returned: events.length,
        },
      });
      return true;
    }
    if (route?.resource === "events-append") {
      if (!authorizeRoute(input, "publish", route.sessionId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, appendEventSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: sessionResourceRoutes.eventsAppend.name,
      });
      const publishPolicy = authorizeClientPublishedEvent({
        authContext: input.authContext,
        producerId: body.producerId,
        type: body.type,
      });
      if (publishPolicy.status === "denied") {
        sendJson(response, 403, { error: "Forbidden", reason: publishPolicy.reason });
        return true;
      }
      const result = yield* service.publishRestEvent({
        ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
        eventId: body.eventId,
        instanceId: body.instanceId,
        payload: body.payload,
        producerId: publishPolicy.producerId,
        sessionId: route.sessionId,
        type: body.type,
      });
      if (result.status === "control_conflict") {
        sendControlLeaseConflict(response, result.leaseClaim, "rest");
        return true;
      }
      if (result.status === "control_epoch_stale") {
        sendControlEpochStale(response, result.currentEpoch);
        return true;
      }
      if (result.status === "conflict") {
        sendJson(response, 409, {
          conflictingFields: result.conflictingFields,
          error: "Event id conflict",
          eventId: result.eventId,
          reason: "event_id_conflict",
        });
        return true;
      }
      broadcastEvents(hub, result.events);
      const rawPublishStatus: string = result.status;
      const publishStatus = rawPublishStatus === "ok" ? "created" : result.status;
      sendJson(response, publishStatus === "created" ? 201 : 200, {
        event: result.event,
        status: publishStatus,
      });
      return true;
    }
    if (route?.resource === "participant-list") {
      if (!authorizeRoute(input, "read", route.sessionId)) {
        return true;
      }
      const participants = yield* service.listParticipants(route.sessionId);
      sendJson(response, 200, { participants });
      return true;
    }
    if (route?.resource === "participant-register") {
      if (!authorizeRoute(input, "task-mutate", route.sessionId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, registerParticipantSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: sessionResourceRoutes.participantRegister.name,
      });
      if (!authorizeParticipant(input, body.participantId)) {
        return true;
      }
      const participantId = effectiveParticipantId(input.authContext, body.participantId);
      if (body.controlChannel !== "rest") {
        sendJson(response, 400, { error: "REST registration requires controlChannel=rest" });
        return true;
      }
      const result = yield* service.registerRestParticipant({
        capabilities: body.capabilities,
        displayName: body.displayName ?? participantId,
        instanceId: body.instanceId,
        participantId,
        runtimeKind: body.runtimeKind,
        sessionId: route.sessionId,
      });
      if (result.status === "control_conflict") {
        sendControlLeaseConflict(response, result.leaseClaim, "rest");
        return true;
      }
      if (result.status === "control_epoch_stale") {
        sendControlEpochStale(response, result.currentEpoch);
        return true;
      }
      broadcastEvents(hub, result.events);
      sendJson(response, 201, {
        controlEpoch: result.controlEpoch,
        participant: result.participant,
        registrationStatus: result.registrationStatus,
      });
      return true;
    }
    if (route?.resource === "task-contract-list") {
      if (!authorizeRoute(input, "read", route.sessionId)) {
        return true;
      }
      const taskContracts = yield* service.listParticipantTaskContracts(route.sessionId);
      sendJson(response, 200, { taskContracts });
      return true;
    }
    if (route?.resource === "task-contract-read") {
      if (!authorizeRoute(input, "read", route.sessionId)) {
        return true;
      }
      const taskContracts = yield* service.listParticipantTaskContractsByKind({
        sessionId: route.sessionId,
        taskKind: route.taskKind,
      });
      if (taskContracts.length === 0) {
        sendJson(response, 404, { error: "Task contract not found" });
        return true;
      }
      sendJson(response, 200, { taskContract: taskContracts[0], taskContracts });
      return true;
    }
    if (route?.resource === "context") {
      if (!authorizeRoute(input, "read", route.sessionId)) {
        return true;
      }
      const context = yield* service.buildSessionContextView({
        budgetTokens: parseContextBudgetTokens(url.searchParams.get("budgetTokens")),
        forParticipant: parseOptionalContextParticipant(url.searchParams.get("forParticipant")),
        sessionId: route.sessionId,
      });
      sendJson(response, 200, { context });
      return true;
    }
    if (route?.resource === "heartbeat") {
      if (!authorizeRoute(input, "task-mutate", route.sessionId)) {
        return true;
      }
      if (!authorizeParticipant(input, route.participantId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, heartbeatParticipantSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: sessionResourceRoutes.heartbeat.name,
      });
      const result = yield* service.heartbeatRestParticipant({
        capabilities: body.capabilities,
        ...(body.controlEpoch !== undefined ? { controlEpoch: body.controlEpoch } : {}),
        instanceId: body.instanceId ?? route.participantId,
        participantId: effectiveParticipantId(input.authContext, route.participantId),
        sessionId: route.sessionId,
      });
      if (result.status === "control_conflict") {
        sendControlLeaseConflict(response, result.leaseClaim, "rest");
        return true;
      }
      if (result.status === "control_epoch_stale") {
        sendControlEpochStale(response, result.currentEpoch);
        return true;
      }
      if (!result.participant) {
        sendJson(response, 404, { error: "Participant not found" });
        return true;
      }
      broadcastEvents(hub, result.events);
      sendJson(response, 200, { participant: result.participant });
      return true;
    }
    return false;
  });
}

/** Applies route-level authorization for session REST resources. */
function authorizeRoute(
  input: SessionHttpRouteHandlerInput,
  action: Parameters<typeof authorize>[0]["action"],
  sessionId?: string,
): boolean {
  const denied = authorize({ action, context: input.authContext, sessionId });
  if (!denied) {
    return true;
  }
  sendAuthError(input.response, denied);
  return false;
}

/** Applies authenticated participant identity binding for session mutations. */
function authorizeParticipant(
  input: SessionHttpRouteHandlerInput,
  participantId: string | null | undefined,
): boolean {
  const denied = authorizeParticipantIdentity(input.authContext, participantId);
  if (!denied) {
    return true;
  }
  sendAuthError(input.response, denied);
  return false;
}

/** Parses and clamps an approximate context budget token query parameter. */
function parseContextBudgetTokens(value: string | null): number {
  if (value === null || value === "") {
    return 12_000;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return 12_000;
  }
  return Math.min(200_000, Math.max(1_000, parsed));
}

/** Parses an optional participant id for context-view diagnostics. */
function parseOptionalContextParticipant(value: string | null): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function matchSessionResourceRoute(
  method: string | undefined,
  pathname: string,
): SessionResourceRouteMatch | null {
  const eventsList = matchSingleParamRoute(sessionResourceRoutes.eventsList, method, pathname);
  if (eventsList) {
    return { resource: "events-list", sessionId: eventsList };
  }
  const eventsAppend = matchSingleParamRoute(sessionResourceRoutes.eventsAppend, method, pathname);
  if (eventsAppend) {
    return { resource: "events-append", sessionId: eventsAppend };
  }
  const deleteSession = matchSingleParamRoute(sessionResourceRoutes.delete, method, pathname);
  if (deleteSession) {
    return { resource: "delete", sessionId: deleteSession };
  }
  const participantList = matchSingleParamRoute(
    sessionResourceRoutes.participantList,
    method,
    pathname,
  );
  if (participantList) {
    return { resource: "participant-list", sessionId: participantList };
  }
  const participantRegister = matchSingleParamRoute(
    sessionResourceRoutes.participantRegister,
    method,
    pathname,
  );
  if (participantRegister) {
    return { resource: "participant-register", sessionId: participantRegister };
  }
  const taskContractList = matchSingleParamRoute(
    sessionResourceRoutes.taskContractList,
    method,
    pathname,
  );
  if (taskContractList) {
    return { resource: "task-contract-list", sessionId: taskContractList };
  }
  const taskContractRead = matchHttpRoute(sessionResourceRoutes.taskContractRead, method, pathname);
  if (taskContractRead?.[1] && taskContractRead[2]) {
    return {
      resource: "task-contract-read",
      sessionId: decodeURIComponent(taskContractRead[1]),
      taskKind: decodeURIComponent(taskContractRead[2]),
    };
  }
  const context = matchSingleParamRoute(sessionResourceRoutes.context, method, pathname);
  if (context) {
    return { resource: "context", sessionId: context };
  }
  const heartbeat = matchHttpRoute(sessionResourceRoutes.heartbeat, method, pathname);
  if (heartbeat?.[1] && heartbeat[2]) {
    return {
      participantId: decodeURIComponent(heartbeat[2]),
      resource: "heartbeat",
      sessionId: decodeURIComponent(heartbeat[1]),
    };
  }
  return null;
}

/** Materializes bounded event-log inputs for Host-presence inventory projection. */
function collectSessionEvents(
  service: SessionServiceEffect,
  sessionIds: readonly string[],
): Effect.Effect<ReadonlyMap<string, readonly SessionEvent[]>, unknown> {
  return Effect.gen(function* () {
    const eventsBySession = new Map<string, readonly SessionEvent[]>();
    const eventLists = yield* Effect.all(
      sessionIds.map((sessionId) => service.listEvents(sessionId, 0, { limit: 10_000 })),
    );
    sessionIds.forEach((sessionId, index) => {
      eventsBySession.set(sessionId, eventLists[index] ?? []);
    });
    return eventsBySession;
  });
}

function matchSingleParamRoute(
  route: HttpRouteSpec,
  method: string | undefined,
  pathname: string,
): string | null {
  const match = matchHttpRoute(route, method, pathname);
  return match?.[1] ? decodeURIComponent(match[1]) : null;
}
