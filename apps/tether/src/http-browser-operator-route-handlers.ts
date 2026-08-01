import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import {
  type BrowserSessionSnapshot,
  browserOperatorApprovalResponseSchema,
  browserOperatorCommandResponseSchema,
  browserOperatorSessionSchema,
  browserSessionSnapshotSchema,
  browserWebSocketTicketResponseSchema,
  operatorCommandRequestSchema,
  operatorCommandPermission,
  operatorTaskApprovalRequestSchema,
} from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";

import {
  BrowserOperatorAuthorityError,
  type BrowserOperatorRuntime,
} from "./auth/browser-operator-runtime.js";
import type { AuthGrantLifecycle } from "./auth/grant-lifecycle.js";
import { browserSessionCookieName } from "./auth/browser-pairing.js";
import type { AuthTicketLifecycle } from "./auth/ticket-lifecycle.js";
import { OperatorCommandAdmissionError } from "./db.js";
import { broadcastEvents, parseJsonBody, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute } from "./http-route-spec.js";
import { listEventPageWithinByteBudget } from "./http-event-pagination.js";
import type { SubscriptionHub } from "./hub.js";
import type { ResourceLimits } from "./resource-limits.js";
import { snapshotRecordMaxBytes } from "./snapshot-limits.js";
import type { SessionServiceEffect, TaskApprovalResult } from "./session-service.js";
import { SessionServicePersistenceError } from "./session-service-contracts.js";

/** Cookie-authenticated routes that never enter participant or administrator routing. */
export const browserOperatorHttpRoutes = {
  approval: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "operator.task.approval",
    pattern: /^\/operator\/sessions\/([^/]+)\/tasks\/([^/]+)\/approval$/u,
  }),
  command: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "operator.command",
    pattern: /^\/operator\/sessions\/([^/]+)\/commands$/u,
  }),
  self: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "operator.browser-session.read",
    pattern: /^\/operator\/browser-session$/u,
  }),
  selfRevoke: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "operator.browser-session.revoke",
    pattern: /^\/operator\/browser-session\/revoke$/u,
  }),
  snapshot: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "operator.session.snapshot",
    pattern: /^\/operator\/sessions\/([^/]+)\/snapshot$/u,
  }),
  websocketTicket: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "operator.websocket-ticket",
    pattern: /^\/operator\/websocket-ticket$/u,
  }),
} as const;

/** Browser operator route dependencies. */
export interface BrowserOperatorHttpRouteInput {
  readonly grantLifecycle: AuthGrantLifecycle;
  readonly hub: SubscriptionHub;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly runtime: BrowserOperatorRuntime;
  readonly service: SessionServiceEffect;
  readonly ticketLifecycle: AuthTicketLifecycle;
  readonly url: URL;
}

/** Handles scoped operator resources independently from all generic bearer routes. */
export function handleBrowserOperatorHttpRoute(
  input: BrowserOperatorHttpRouteInput,
): Effect.Effect<boolean, unknown> {
  return handleBrowserOperatorHttpRouteEffect(input).pipe(
    Effect.catchIf(
      (error) => error instanceof BrowserOperatorAuthorityError,
      (error) =>
        Effect.sync(() => {
          sendBrowserOperatorError(input.response, error);
          return true;
        }),
    ),
    Effect.catchIf(isOperatorCommandAdmissionFailure, (error) =>
      Effect.sync(() => {
        sendOperatorCommandError(input.response, error.cause);
        return true;
      }),
    ),
  );
}

/** Narrows the service wrapper around one stable transaction-owned command denial. */
function isOperatorCommandAdmissionFailure(
  error: unknown,
): error is SessionServicePersistenceError & { readonly cause: OperatorCommandAdmissionError } {
  return (
    error instanceof SessionServicePersistenceError &&
    error.cause instanceof OperatorCommandAdmissionError
  );
}

/** Runs one matched operator route after its exact resource-action check. */
function handleBrowserOperatorHttpRouteEffect(
  input: BrowserOperatorHttpRouteInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const { request, response, url } = input;
    if (url.pathname.startsWith("/operator/")) {
      response.setHeader("cache-control", "no-store");
    }
    if (matchHttpRoute(browserOperatorHttpRoutes.self, request.method, url.pathname)) {
      const authorized = yield* authorizeBrowserOperator(input.runtime, request, {
        csrfRequired: false,
        resource: { permission: "browser-session.read" },
      });
      sendJson(
        response,
        200,
        browserOperatorSessionSchema.parse({
          expiresAt: authorized.context.expiresAt,
          grantJti: authorized.context.grantJti,
          scope: authorized.scope,
          status: "active",
          subject: authorized.context.participantId,
        }),
      );
      return true;
    }
    if (matchHttpRoute(browserOperatorHttpRoutes.selfRevoke, request.method, url.pathname)) {
      const authorized = yield* authorizeBrowserOperator(input.runtime, request, {
        csrfRequired: true,
        resource: { permission: "browser-session.revoke" },
      });
      const grantJti = requiredGrantJti(authorized.context.grantJti);
      const result = yield* Effect.promise(() =>
        input.grantLifecycle.revoke(grantJti, authorized.context.participantId, "operator-request"),
      );
      response.setHeader("set-cookie", clearBrowserSessionCookie());
      sendJson(response, 200, { ...result });
      return true;
    }
    const snapshotMatch = matchHttpRoute(
      browserOperatorHttpRoutes.snapshot,
      request.method,
      url.pathname,
    );
    if (snapshotMatch?.[1]) {
      const sessionId = routeMatchParam(snapshotMatch, 1);
      yield* authorizeBrowserOperator(input.runtime, request, {
        csrfRequired: false,
        resource: { permission: "session.read", sessionId },
      });
      const eventLimit = Math.min(input.resourceLimits.eventListMaxLimit, 1_000);
      const participantLimit = 1_000;
      const taskLimit = 1_000;
      const eventPage = yield* listEventPageWithinByteBudget({
        afterSeq: 0,
        limit: eventLimit,
        maxBytes: input.resourceLimits.restEventListMaxBytes,
        maxEventBytes: input.resourceLimits.httpMaxBodyBytes,
        service: input.service,
        sessionId,
      });
      const eventSnapshot = fitBrowserSnapshotWithinByteBudget(
        {
          cursor: eventPage.events.at(-1)?.seq ?? 0,
          events: eventPage.events,
          participants: [],
          sessionId,
          tasks: [],
          truncated: { events: eventPage.hasMore, participants: false, tasks: false },
        },
        input.resourceLimits.restEventListMaxBytes,
      );
      const participantBudget = remainingSnapshotBytes(
        eventSnapshot,
        input.resourceLimits.restEventListMaxBytes,
      );
      const participantPage = yield* listParticipantsWithinSnapshotBudget({
        limit: participantLimit,
        maxBytes: participantBudget,
        service: input.service,
        sessionId,
      });
      const participantSnapshot = fitBrowserSnapshotWithinByteBudget(
        {
          ...eventSnapshot,
          participants: participantPage.records,
          truncated: {
            ...eventSnapshot.truncated,
            participants: participantPage.hasMore,
          },
        },
        input.resourceLimits.restEventListMaxBytes,
      );
      const taskBudget = remainingSnapshotBytes(
        participantSnapshot,
        input.resourceLimits.restEventListMaxBytes,
      );
      const taskPage = yield* listTasksWithinSnapshotBudget({
        limit: taskLimit,
        maxBytes: taskBudget,
        service: input.service,
        sessionId,
      });
      const snapshot = fitBrowserSnapshotWithinByteBudget(
        {
          ...participantSnapshot,
          tasks: taskPage.records,
          truncated: {
            ...participantSnapshot.truncated,
            tasks: taskPage.hasMore,
          },
        },
        input.resourceLimits.restEventListMaxBytes,
      );
      sendJson(response, 200, browserSessionSnapshotSchema.parse(snapshot));
      return true;
    }
    const approvalMatch = matchHttpRoute(
      browserOperatorHttpRoutes.approval,
      request.method,
      url.pathname,
    );
    if (approvalMatch?.[1] && approvalMatch[2]) {
      const sessionId = routeMatchParam(approvalMatch, 1);
      const taskId = routeMatchParam(approvalMatch, 2);
      const body = yield* parseJsonBody(request, operatorTaskApprovalRequestSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: browserOperatorHttpRoutes.approval.name,
      });
      const authorized = yield* authorizeBrowserOperator(input.runtime, request, {
        csrfRequired: true,
        resource: {
          action: body.target.action,
          permission: "approval.submit",
          scopeKey: body.target.scopeKey,
          sessionId,
          targetKind: body.target.targetKind,
        },
      });
      const result = yield* input.service.recordTaskApproval({
        decision: body.decision,
        operatorGrantJti: requiredGrantJti(authorized.context.grantJti),
        participantId: authorized.context.participantId,
        reason: body.reason,
        sessionId,
        target: body.target,
        taskId,
      });
      sendOperatorApprovalResult(response, input.hub, result);
      return true;
    }
    const commandMatch = matchHttpRoute(
      browserOperatorHttpRoutes.command,
      request.method,
      url.pathname,
    );
    if (commandMatch?.[1]) {
      const sessionId = routeMatchParam(commandMatch, 1);
      const body = yield* parseJsonBody(request, operatorCommandRequestSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: browserOperatorHttpRoutes.command.name,
      });
      const permission = operatorCommandPermission(body.command);
      const authorized = yield* authorizeBrowserOperator(input.runtime, request, {
        csrfRequired: true,
        resource: {
          command: body.command,
          permission,
          scopeKey: body.scopeKey,
          sessionId,
        },
      });
      const result = yield* input.service.createTask({
        operatorAuthority: {
          grantJti: requiredGrantJti(authorized.context.grantJti),
          request: {
            command: body.command,
            scopeKey: body.scopeKey,
            sessionId,
            ...(body.targetId === undefined ? {} : { targetId: body.targetId }),
          },
        },
        taskId: undefined,
      });
      broadcastEvents(input.hub, result.events);
      sendJson(
        response,
        result.status === "created" ? 201 : 200,
        browserOperatorCommandResponseSchema.parse({ status: result.status, task: result.task }),
      );
      return true;
    }
    if (matchHttpRoute(browserOperatorHttpRoutes.websocketTicket, request.method, url.pathname)) {
      const authorized = yield* authorizeBrowserOperator(input.runtime, request, {
        csrfRequired: true,
        resource: { permission: "websocket.connect" },
      });
      const ticket = yield* Effect.promise(() => input.ticketLifecycle.mint(authorized.context));
      sendJson(response, 201, browserWebSocketTicketResponseSchema.parse(ticket));
      return true;
    }
    return false;
  });
}

/** Lifts expected browser authority rejections into the typed Effect error channel. */
function authorizeBrowserOperator(
  runtime: BrowserOperatorRuntime,
  request: IncomingMessage,
  requirement: Parameters<BrowserOperatorRuntime["authorize"]>[1],
): Effect.Effect<Awaited<ReturnType<BrowserOperatorRuntime["authorize"]>>, unknown> {
  return Effect.tryPromise({
    catch: (error) => error,
    try: () => runtime.authorize(request, requirement),
  });
}

/** Returns a deletion cookie retaining the production security attributes. */
function clearBrowserSessionCookie(): string {
  return `${browserSessionCookieName}=; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Strict`;
}

/** Requires the durable parent id guaranteed by browser operator authentication. */
function requiredGrantJti(grantJti: string | null): string {
  if (grantJti === null) throw new BrowserOperatorAuthorityError("operator_auth_denied");
  return grantJti;
}

/** Reads a required regex capture without accepting an empty resource id. */
function routeMatchParam(match: RegExpMatchArray, index: number): string {
  const value = match[index];
  if (!value) throw new Error("operator_route_parameter_missing");
  return value;
}

interface BoundedSnapshotPage<TRecord> {
  readonly hasMore: boolean;
  readonly records: TRecord[];
}

/** Reads participants in small indexed keyset pages until the response budget is full. */
function listParticipantsWithinSnapshotBudget(input: {
  readonly limit: number;
  readonly maxBytes: number;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
}): Effect.Effect<BoundedSnapshotPage<BrowserSessionSnapshot["participants"][number]>, unknown> {
  return Effect.gen(function* () {
    const records: BrowserSessionSnapshot["participants"] = [];
    let before: BrowserSessionSnapshot["participants"][number] | undefined;
    let byteLength = 0;
    for (;;) {
      const pageLimit = Math.min(
        snapshotReadPageSize(input.maxBytes),
        input.limit + 1 - records.length,
      );
      const page = yield* input.service.listParticipants(input.sessionId, {
        ...(before === undefined
          ? {}
          : {
              before: { lastSeenAt: before.lastSeenAt, participantId: before.participantId },
            }),
        limit: pageLimit,
      });
      for (const participant of page) {
        if (records.length >= input.limit) return { hasMore: true, records };
        const nextByteLength = byteLength + Buffer.byteLength(JSON.stringify(participant)) + 1;
        if (nextByteLength > input.maxBytes) return { hasMore: true, records };
        byteLength = nextByteLength;
        records.push(participant);
      }
      if (page.length < pageLimit) return { hasMore: false, records };
      before = page.at(-1);
    }
  });
}

/** Reads tasks in small indexed keyset pages until the response budget is full. */
function listTasksWithinSnapshotBudget(input: {
  readonly limit: number;
  readonly maxBytes: number;
  readonly service: SessionServiceEffect;
  readonly sessionId: string;
}): Effect.Effect<BoundedSnapshotPage<BrowserSessionSnapshot["tasks"][number]>, unknown> {
  return Effect.gen(function* () {
    const records: BrowserSessionSnapshot["tasks"] = [];
    let before: BrowserSessionSnapshot["tasks"][number] | undefined;
    let byteLength = 0;
    for (;;) {
      const pageLimit = Math.min(
        snapshotReadPageSize(input.maxBytes),
        input.limit + 1 - records.length,
      );
      const page = yield* input.service.listTasks(input.sessionId, "all", {
        ...(before === undefined
          ? {}
          : { before: { createdAt: before.createdAt, taskId: before.taskId } }),
        limit: pageLimit,
      });
      for (const task of page) {
        if (records.length >= input.limit) return { hasMore: true, records };
        const nextByteLength = byteLength + Buffer.byteLength(JSON.stringify(task)) + 1;
        if (nextByteLength > input.maxBytes) return { hasMore: true, records };
        byteLength = nextByteLength;
        records.push(task);
      }
      if (page.length < pageLimit) return { hasMore: false, records };
      before = page.at(-1);
    }
  });
}

/** Caps one keyset read so a full page cannot exceed the persisted row bound. */
function snapshotReadPageSize(maxBytes: number): number {
  return Math.max(1, Math.min(32, Math.floor(maxBytes / snapshotRecordMaxBytes)));
}

/** Trims a row-bounded snapshot until the complete serialized response meets its byte budget. */
export function fitBrowserSnapshotWithinByteBudget(
  input: BrowserSessionSnapshot,
  maxBytes: number,
): BrowserSessionSnapshot {
  const snapshot = {
    ...input,
    events: [...input.events],
    participants: [...input.participants],
    tasks: [...input.tasks],
    truncated: { ...input.truncated },
  };
  if (Buffer.byteLength(JSON.stringify(snapshot)) <= maxBytes) return snapshot;
  if (trimSnapshotArray(snapshot, "tasks", maxBytes)) snapshot.truncated.tasks = true;
  if (Buffer.byteLength(JSON.stringify(snapshot)) <= maxBytes) return snapshot;
  if (trimSnapshotArray(snapshot, "participants", maxBytes)) snapshot.truncated.participants = true;
  if (Buffer.byteLength(JSON.stringify(snapshot)) <= maxBytes) return snapshot;
  if (trimSnapshotArray(snapshot, "events", maxBytes)) {
    snapshot.cursor = snapshot.events.at(-1)?.seq ?? 0;
    snapshot.truncated.events = true;
  }
  if (Buffer.byteLength(JSON.stringify(snapshot)) > maxBytes) {
    throw new Error("browser_snapshot_byte_budget_invalid");
  }
  return snapshot;
}

/** Finds the largest prefix of one snapshot collection that fits using logarithmic probes. */
function trimSnapshotArray(
  snapshot: BrowserSessionSnapshot,
  key: "events" | "participants" | "tasks",
  maxBytes: number,
): boolean {
  const values = snapshot[key];
  if (values.length === 0) return false;
  let lower = 0;
  let upper = values.length;
  while (lower < upper) {
    const midpoint = Math.ceil((lower + upper) / 2);
    const candidate = { ...snapshot, [key]: values.slice(0, midpoint) };
    if (Buffer.byteLength(JSON.stringify(candidate)) <= maxBytes) lower = midpoint;
    else upper = midpoint - 1;
  }
  if (lower === values.length) return false;
  if (key === "tasks") snapshot.tasks = values.slice(0, lower) as typeof snapshot.tasks;
  if (key === "participants") {
    snapshot.participants = values.slice(0, lower) as typeof snapshot.participants;
  }
  if (key === "events") snapshot.events = values.slice(0, lower) as typeof snapshot.events;
  return true;
}

/** Returns the remaining serialized response budget after one bounded snapshot stage. */
function remainingSnapshotBytes(snapshot: BrowserSessionSnapshot, maxBytes: number): number {
  return Math.max(1, maxBytes - Buffer.byteLength(JSON.stringify(snapshot)));
}

/** Renders canonical first-committer-wins approval results. */
function sendOperatorApprovalResult(
  response: ServerResponse,
  hub: SubscriptionHub,
  result: TaskApprovalResult,
): void {
  if (result.status === "rejected") {
    sendJson(
      response,
      result.rejectionReason === "task_not_found" ? 404 : 409,
      browserOperatorApprovalResponseSchema.parse({
        rejectionReason: result.rejectionReason,
        status: result.status,
        task: result.task,
      }),
    );
    return;
  }
  if (result.status === "recorded") broadcastEvents(hub, result.events);
  sendJson(
    response,
    200,
    browserOperatorApprovalResponseSchema.parse({
      approval: result.approval,
      decision: result.decision,
      ...(result.status === "ignored"
        ? {
            existingDecision: result.existingDecision,
            ignoredReason: result.ignoredReason,
          }
        : { event: result.event }),
      status: result.status,
      task: result.task,
    }),
  );
}

/** Maps stable browser authority failures to credential-safe status responses. */
function sendBrowserOperatorError(
  response: ServerResponse,
  error: BrowserOperatorAuthorityError,
): void {
  const unauthorized =
    error.code === "operator_auth_denied" ||
    error.code === "operator_cookie_ambiguous" ||
    error.code === "operator_cookie_missing";
  sendJson(response, unauthorized ? 401 : 403, {
    error: unauthorized ? "Unauthorized" : "Forbidden",
    reason: error.code,
  });
}

/** Maps atomic operator command admission failures without exposing persistence details. */
function sendOperatorCommandError(
  response: ServerResponse,
  error: OperatorCommandAdmissionError,
): void {
  const status =
    error.reason === "operator_command_rate_limited"
      ? 429
      : error.reason === "operator_command_queue_full"
        ? 503
        : 403;
  sendJson(response, status, {
    error: status === 403 ? "Forbidden" : "Operator command unavailable",
    reason: error.reason,
  });
}
