import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { Effect } from "effect";

import { authorize } from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import { broadcastEvents, parseJsonBody, sendJson } from "./http-route-runtime.js";
import { sendAuthError } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute } from "./http-route-spec.js";
import type { SubscriptionHub } from "./hub.js";
import { resolveClientSessionSchema } from "./protocol.js";
import type { ResourceLimits } from "./resource-limits.js";
import type { SessionServiceEffect } from "./session-service.js";

interface ClientBindingHttpRouteHandlerInput {
  readonly authContext: AuthContext | null;
  readonly hub: SubscriptionHub;
  readonly request: IncomingMessage;
  readonly resourceLimits: ResourceLimits;
  readonly response: ServerResponse;
  readonly service: SessionServiceEffect;
  readonly url: URL;
}

export const clientBindingRoutes = {
  archive: defineHttpRoute({
    control: "not-applicable",
    method: "DELETE",
    name: "client-binding.archive",
    pattern: /^\/client-bindings\/([^/]+)\/([^/]+)$/u,
  }),
  list: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "client-binding.list",
    pattern: /^\/client-bindings$/u,
  }),
  resolveSession: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "client-binding.resolve-session",
    pattern: /^\/client-bindings\/session$/u,
  }),
} as const;

type ClientBindingRouteMatch =
  | {
      readonly externalId: string;
      readonly provider: string;
      readonly resource: "archive";
    }
  | {
      readonly resource: "list";
    }
  | {
      readonly resource: "resolve-session";
    };

/** Routes client-binding REST requests and owns their response mapping. */
export function handleClientBindingHttpRoute(
  input: ClientBindingHttpRouteHandlerInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const { hub, request, response, service, url } = input;
    const route = matchClientBindingRoute(request.method, url.pathname);
    if (route?.resource === "list") {
      if (!authorizeRoute(input)) {
        return true;
      }
      const provider = url.searchParams.get("provider") ?? undefined;
      const bindings = yield* service.listClientSessionBindings({ provider });
      sendJson(response, 200, { bindings });
      return true;
    }
    if (route?.resource === "resolve-session") {
      if (!authorizeRoute(input)) {
        return true;
      }
      const body = yield* parseJsonBody(request, resolveClientSessionSchema, {
        maxBytes: input.resourceLimits.httpMaxBodyBytes,
        routeName: clientBindingRoutes.resolveSession.name,
      });
      const result = yield* service.resolveClientSession({
        externalId: body.externalId,
        provider: body.provider,
        sessionId: body.sessionId,
      });
      broadcastEvents(hub, result.events);
      sendJson(response, result.created ? 201 : 200, {
        binding: result.binding,
        created: result.created,
        session: result.session,
      });
      return true;
    }
    if (route?.resource === "archive") {
      if (!authorizeRoute(input)) {
        return true;
      }
      const binding = yield* service.archiveClientSessionBinding({
        externalId: route.externalId,
        provider: route.provider,
      });
      if (!binding) {
        sendJson(response, 404, { error: "Client binding not found" });
        return true;
      }
      sendJson(response, 200, { binding });
      return true;
    }
    return false;
  });
}

/** Applies service-scope authorization for external client binding operations. */
function authorizeRoute(input: ClientBindingHttpRouteHandlerInput): boolean {
  const denied = authorize({ action: "client-binding", context: input.authContext });
  if (!denied) {
    return true;
  }
  sendAuthError(input.response, denied);
  return false;
}

function matchClientBindingRoute(
  method: string | undefined,
  pathname: string,
): ClientBindingRouteMatch | null {
  if (matchHttpRoute(clientBindingRoutes.list, method, pathname)) {
    return { resource: "list" };
  }
  if (matchHttpRoute(clientBindingRoutes.resolveSession, method, pathname)) {
    return { resource: "resolve-session" };
  }
  const archiveMatch = matchHttpRoute(clientBindingRoutes.archive, method, pathname);
  if (archiveMatch?.[1] && archiveMatch[2]) {
    return {
      externalId: decodeURIComponent(archiveMatch[2]),
      provider: decodeURIComponent(archiveMatch[1]),
      resource: "archive",
    };
  }
  return null;
}
