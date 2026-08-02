import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import {
  type BrowserSessionSnapshot,
  browserOperatorApprovalResponseSchema,
  browserOperatorCommandResponseSchema,
  browserOperatorSessionSchema,
  browserSessionSnapshotSchema,
  browserWebSocketTicketResponseSchema,
  operatorCommandPermission,
  operatorCommandRequestSchema,
  operatorTaskApprovalRequestSchema,
} from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";

import {
  BrowserOperatorAuthorityError,
  type BrowserOperatorRuntime,
} from "./auth/browser-operator-runtime.js";
import type { AuthGrantLifecycle } from "./auth/grant-lifecycle.js";
import type { AuthTicketLifecycle } from "./auth/ticket-lifecycle.js";
import {
  fitBrowserSnapshotWithinByteBudget,
  listRecordsWithinSnapshotBudget,
} from "./browser-snapshot-budget.js";
import { OperatorCommandAdmissionError } from "./db.js";
import { clearBrowserSessionCookie } from "./http-browser-pairing-route-handlers.js";
import { listEventPageWithinByteBudget } from "./http-event-pagination.js";
import { broadcastEvents, parseJsonBody, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute, routeMatchParam } from "./http-route-spec.js";
import type { SubscriptionHub } from "./hub.js";
import type { ResourceLimits } from "./resource-limits.js";
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
      const maxBytes = input.resourceLimits.restEventListMaxBytes;
      // The three reads are independent, so they run together and the final fit
      // below is the one authority on the assembled response size.
      const [eventPage, participantPage, taskPage] = yield* Effect.all(
        [
          listEventPageWithinByteBudget({
            afterSeq: 0,
            limit: Math.min(input.resourceLimits.eventListMaxLimit, 1_000),
            maxBytes,
            maxEventBytes: input.resourceLimits.httpMaxBodyBytes,
            service: input.service,
            sessionId,
          }),
          listRecordsWithinSnapshotBudget<BrowserSessionSnapshot["participants"][number]>({
            fetch: (before, limit) =>
              input.service.listParticipants(sessionId, {
                ...(before === undefined ? {} : { before }),
                limit,
              }),
            limit: 1_000,
            maxBytes,
          }),
          listRecordsWithinSnapshotBudget<BrowserSessionSnapshot["tasks"][number]>({
            fetch: (before, limit) =>
              input.service.listTasks(sessionId, "all", {
                ...(before === undefined ? {} : { before }),
                limit,
              }),
            limit: 1_000,
            maxBytes,
          }),
        ],
        { concurrency: 3 },
      );
      const snapshot = fitBrowserSnapshotWithinByteBudget(
        {
          cursor: eventPage.events.at(-1)?.seq ?? 0,
          events: eventPage.events,
          participants: participantPage.records,
          sessionId,
          tasks: taskPage.records,
          truncated: {
            events: eventPage.hasMore,
            participants: participantPage.hasMore,
            tasks: taskPage.hasMore,
          },
        },
        maxBytes,
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

/** Requires the durable parent id guaranteed by browser operator authentication. */
function requiredGrantJti(grantJti: string | null): string {
  if (grantJti === null) throw new BrowserOperatorAuthorityError("operator_auth_denied");
  return grantJti;
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
