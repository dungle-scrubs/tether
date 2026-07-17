import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { Effect } from "effect";

import {
  authorize,
  authorizeParticipantIdentity,
  effectiveParticipantId,
} from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import type { RuntimeTopology } from "./config.js";
import {
  findProjectedSession,
  type HostPresenceRuntime,
  permanentDeleteEligibility,
  projectSessionInventory,
} from "./host-presence.js";
import {
  broadcastEvents,
  parseJsonBody,
  sendAuthError,
  sendControlAcquisitionIdRequired,
  sendControlAcquisitionStale,
  sendControlEpochRequired,
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
  releaseParticipantControlEnvelopeSchema,
} from "./protocol.js";
import {
  parseEventListLimit,
  type ResourceLimits,
  sessionEventByteLength,
} from "./resource-limits.js";
import { authorizeClientPublishedEvent } from "./session-event-publish-policy.js";
import type { SessionServiceEffect } from "./session-service.js";
import type { SessionEvent } from "./types.js";

interface SessionHttpRouteHandlerInput {
  readonly authContext: AuthContext | null;
  readonly hostPresence: HostPresenceRuntime;
  readonly hub: SubscriptionHub;
  readonly replicaId: string;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly runtimeTopology: RuntimeTopology;
  readonly service: SessionServiceEffect;
  readonly url: URL;
}

export const sessionResourceRoutes = {
  create: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "session.create",
    pattern: /^\/sessions$/u,
  }),
  context: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "session.context",
    pattern: /^\/sessions\/([^/]+)\/context$/u,
  }),
  delete: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "session.delete",
    pattern: /^\/sessions\/([^/]+)\/delete$/u,
  }),
  eventsAppend: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "session.events.append",
    pattern: /^\/sessions\/([^/]+)\/events$/u,
  }),
  eventsList: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "session.events.list",
    pattern: /^\/sessions\/([^/]+)\/events$/u,
  }),
  heartbeat: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "session.participant.heartbeat",
    pattern: /^\/sessions\/([^/]+)\/participants\/([^/]+)\/heartbeat$/u,
  }),
  controlRelease: defineHttpRoute({
    control: "fenced",
    method: "POST",
    name: "session.participant.control.release",
    pattern: /^\/sessions\/([^/]+)\/participants\/([^/]+)\/control\/release$/u,
  }),
  participantList: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "session.participant.list",
    pattern: /^\/sessions\/([^/]+)\/participants$/u,
  }),
  participantRegister: defineHttpRoute({
    control: "acquisition",
    method: "POST",
    name: "session.participant.register",
    pattern: /^\/sessions\/([^/]+)\/participants$/u,
  }),
  list: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "session.list",
    pattern: /^\/sessions$/u,
  }),
  taskContractList: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "session.task-contract.list",
    pattern: /^\/sessions\/([^/]+)\/task-contracts$/u,
  }),
  taskContractRead: defineHttpRoute({
    control: "not-applicable",
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
      readonly resource: "control-release";
      readonly sessionId: string;
    }
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
    if (matchHttpRoute(sessionResourceRoutes.list, request.method, url.pathname)) {
      if (!authorizeRoute(input, "read")) {
        return true;
      }
      const sessions = yield* service.listSessions();
      sendJson(response, 200, {
        ...projectSessionInventory({
          replicaId: input.replicaId,
          runtime: input.hostPresence,
          sessions,
        }),
      });
      return true;
    }
    if (matchHttpRoute(sessionResourceRoutes.create, request.method, url.pathname)) {
      if (!authorizeRoute(input, "session-create")) {
        return true;
      }
      const body = yield* parseJsonBody(request, createSessionSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: "session.create",
      });
      const result = yield* service.ensurePublicSession({
        sessionId: body.sessionId,
      });
      sendJson(response, 201, { session: result.session });
      return true;
    }
    const route = matchSessionResourceRoute(request.method, url.pathname);
    if (route?.resource === "delete") {
      if (!authorizeRoute(input, "session-create", route.sessionId)) {
        return true;
      }
      if (input.runtimeTopology === "multi") {
        sendJson(response, 409, {
          detail: "permanent delete requires cluster-complete Host Presence",
          ok: false,
          reason: "presence_scope_insufficient",
        });
        return true;
      }
      const sessions = yield* service.listSessions();
      const session = sessions.find((candidate) => candidate.sessionId === route.sessionId) ?? null;
      const projected = findProjectedSession({
        liveHosts: input.hostPresence.hosts(route.sessionId),
        session,
      });
      // Advisory pre-check for a fast, well-shaped refusal. The authoritative
      // check runs again inside the delete transaction under row locks, so a
      // host connect or task claim between this snapshot and the delete cannot
      // slip a protected session through.
      const eligibility = permanentDeleteEligibility(projected);
      if (!eligibility.ok) {
        sendJson(response, eligibility.reason === "not-found" ? 404 : 409, {
          detail: eligibility.detail,
          ok: false,
          reason: eligibility.reason,
        });
        return true;
      }
      const result = yield* service.deleteSession({
        // Re-checked inside the delete transaction after the row locks are
        // held, shrinking the unfenced process-local presence window to the
        // final probe before the delete statement.
        hasLiveHost: () => input.hostPresence.hosts(route.sessionId).length > 0,
        sessionId: route.sessionId,
      });
      if (result.status === "deleted") {
        sendJson(response, 200, { ok: true, sessionId: route.sessionId });
        return true;
      }
      if (result.status === "not_found") {
        sendJson(response, 404, { detail: "session not found", ok: false, reason: "not-found" });
        return true;
      }
      sendJson(response, 409, {
        detail: result.detail,
        ok: false,
        reason: result.reason,
      });
      return true;
    }
    if (route?.resource === "events-list") {
      if (!authorizeRoute(input, "read", route.sessionId)) {
        return true;
      }
      const afterSeq = parseAfterSeq(url.searchParams.get("after"));
      const limit = parseEventListLimit(url.searchParams.get("limit"), input.resourceLimits);
      const page = yield* listEventPageWithinByteBudget({
        afterSeq,
        limit,
        maxBytes: input.resourceLimits.restEventListMaxBytes,
        maxEventBytes: input.resourceLimits.httpMaxBodyBytes,
        service,
        sessionId: route.sessionId,
      });
      const { events } = page;
      const nextAfterSeq = events.at(-1)?.seq ?? afterSeq;
      sendJson(response, 200, {
        events,
        pagination: {
          afterSeq,
          hasMore: page.hasMore,
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
        sendJson(response, 403, {
          error: "Forbidden",
          reason: publishPolicy.reason,
        });
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
      if (result.status === "control_epoch_required") {
        sendControlEpochRequired(response);
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
        sendJson(response, 400, {
          error: "REST registration requires controlChannel=rest",
        });
        return true;
      }
      const result = yield* service.registerRestParticipant({
        acquisitionId: body.acquisitionId,
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
      if (result.status === "control_acquisition_id_required") {
        sendControlAcquisitionIdRequired(response);
        return true;
      }
      if (result.status === "control_acquisition_stale") {
        sendControlAcquisitionStale(response);
        return true;
      }
      if (result.status === "control_epoch_required") {
        sendControlEpochRequired(response);
        return true;
      }
      if (result.status === "control_epoch_stale") {
        sendControlEpochStale(response, result.currentEpoch);
        return true;
      }
      broadcastEvents(hub, result.events);
      sendJson(response, result.acquisitionStatus === "replayed" ? 200 : 201, {
        ...(result.acquisitionId.length > 0 ? { acquisitionId: result.acquisitionId } : {}),
        acquisitionStatus: result.acquisitionStatus,
        controlEpoch: result.controlEpoch,
        leaseExpiresAt: result.leaseExpiresAt,
        participant: result.participant,
        registrationStatus: result.registrationStatus,
        renewAfterMs: result.renewAfterMs,
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
      sendJson(response, 200, {
        taskContract: taskContracts[0],
        taskContracts,
      });
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
      if (result.status === "control_epoch_required") {
        sendControlEpochRequired(response);
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
      sendJson(response, 200, {
        controlEpoch: result.controlEpoch,
        leaseExpiresAt: result.leaseExpiresAt,
        participant: result.participant,
        renewAfterMs: result.renewAfterMs,
      });
      return true;
    }
    if (route?.resource === "control-release") {
      if (!authorizeRoute(input, "task-mutate", route.sessionId)) {
        return true;
      }
      if (!authorizeParticipant(input, route.participantId)) {
        return true;
      }
      const body = yield* parseJsonBody(request, releaseParticipantControlEnvelopeSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: sessionResourceRoutes.controlRelease.name,
      });
      const result = yield* service.releaseRestControlLease({
        ...(body.controlEpoch === undefined ? {} : { controlEpoch: body.controlEpoch }),
        instanceId: body.instanceId,
        participantId: effectiveParticipantId(input.authContext, route.participantId),
        sessionId: route.sessionId,
      });
      if (result.status === "control_conflict") {
        sendControlLeaseConflict(response, result.leaseClaim, "rest");
        return true;
      }
      if (result.status === "control_epoch_required") {
        sendControlEpochRequired(response);
        return true;
      }
      if (result.status === "control_epoch_stale") {
        sendControlEpochStale(response, result.currentEpoch);
        return true;
      }
      sendJson(response, 200, { released: result.released });
      return true;
    }
    return false;
  });
}

/** Bounded REST event-list page and whether more events remain past it. */
interface EventListPage {
  readonly events: SessionEvent[];
  readonly hasMore: boolean;
}

/**
 * Fetches one REST event-list page bounded by both the row limit and a
 * cumulative byte budget. Events are read in bounded pages so a session of
 * near-max events cannot materialize unbounded memory before the response is
 * serialized, mirroring the WebSocket replay-window byte budget. Truncation at
 * the byte boundary is surfaced through the existing pagination contract:
 * fewer events plus hasMore, with the caller's next-cursor advancing past the
 * last returned event. At least one event is always returned when any remain so
 * a single oversized event cannot stall pagination.
 */
function listEventPageWithinByteBudget(input: {
  readonly afterSeq: number;
  readonly limit: number;
  readonly maxBytes: number;
  readonly maxEventBytes: number;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
}): Effect.Effect<EventListPage, unknown> {
  return Effect.gen(function* () {
    // One page never holds more than the byte budget plus a single event, so
    // peak materialization stays bounded regardless of how many events exist.
    const fetchPageSize = Math.max(1, Math.floor(input.maxBytes / input.maxEventBytes));
    const events: SessionEvent[] = [];
    let byteLength = 0;
    let cursor = input.afterSeq;
    for (;;) {
      // Fetch one row beyond the requested page size so a full page still
      // detects that more events remain, matching the prior limit + 1 lookahead.
      const pageLimit = Math.min(fetchPageSize, input.limit + 1 - events.length);
      const page = yield* input.service.listEvents(input.sessionId, cursor, { limit: pageLimit });
      for (const event of page) {
        if (events.length >= input.limit) {
          // Lookahead row: more events exist past the requested page size.
          return { events, hasMore: true };
        }
        const nextByteLength = byteLength + sessionEventByteLength(event);
        if (events.length > 0 && nextByteLength > input.maxBytes) {
          // Stop at the byte boundary; remaining events are reachable
          // through the next-cursor on a follow-up request.
          return { events, hasMore: true };
        }
        byteLength = nextByteLength;
        events.push(event);
      }
      if (page.length < pageLimit) {
        return { events, hasMore: false };
      }
      cursor = events[events.length - 1]?.seq ?? cursor;
    }
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
  const controlRelease = matchHttpRoute(sessionResourceRoutes.controlRelease, method, pathname);
  if (controlRelease?.[1] && controlRelease[2]) {
    return {
      participantId: decodeURIComponent(controlRelease[2]),
      resource: "control-release",
      sessionId: decodeURIComponent(controlRelease[1]),
    };
  }
  return null;
}

function matchSingleParamRoute(
  route: HttpRouteSpec,
  method: string | undefined,
  pathname: string,
): string | null {
  const match = matchHttpRoute(route, method, pathname);
  return match?.[1] ? decodeURIComponent(match[1]) : null;
}
