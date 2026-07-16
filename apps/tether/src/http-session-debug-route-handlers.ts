import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { Effect } from "effect";

import { authorize } from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import { sendAuthError, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, type HttpRouteSpec, matchHttpRoute } from "./http-route-spec.js";
import type { SessionServiceEffect } from "./session-service.js";

interface SessionDebugHttpRouteHandlerInput {
  readonly authContext: AuthContext | null;
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
  readonly service: SessionServiceEffect;
  readonly url: URL;
}

type SessionDebugResource = "control-leases" | "participants" | "summary" | "tasks";

interface SessionDebugRouteSpec {
  readonly resource: SessionDebugResource;
  readonly route: HttpRouteSpec;
}

export const sessionDebugRoutes = [
  defineSessionDebugRoute("control-leases"),
  defineSessionDebugRoute("participants"),
  defineSessionDebugRoute("summary"),
  defineSessionDebugRoute("tasks"),
] as const;

/** Routes read-only session debug REST resources. */
export function handleSessionDebugHttpRoute(
  input: SessionDebugHttpRouteHandlerInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const { request, response, service, url } = input;
    const route = matchSessionDebugRoute(request.method, url.pathname);
    if (!route) {
      return false;
    }
    const denied = authorize({ action: "admin", context: input.authContext });
    if (denied) {
      sendAuthError(response, denied);
      return true;
    }
    if (route.resource === "control-leases") {
      const controlLeases = yield* service.listControlLeaseSnapshots(route.sessionId);
      sendJson(response, 200, { controlLeases });
      return true;
    }
    if (route.resource === "participants") {
      const participants = yield* service.listParticipantRuntimeSnapshots(route.sessionId);
      sendJson(response, 200, { participants });
      return true;
    }
    if (route.resource === "summary") {
      const summary = yield* service.readSessionDebugSummary(route.sessionId);
      sendJson(response, 200, { summary });
      return true;
    }
    const tasks = yield* service.listTaskSnapshots(route.sessionId);
    sendJson(response, 200, { tasks });
    return true;
  });
}

function matchSessionDebugRoute(
  method: string | undefined,
  pathname: string,
): { readonly resource: SessionDebugResource; readonly sessionId: string } | null {
  for (const spec of sessionDebugRoutes) {
    const match = matchHttpRoute(spec.route, method, pathname);
    const sessionId = match?.[1];
    if (sessionId) {
      return { resource: spec.resource, sessionId };
    }
  }
  return null;
}

function defineSessionDebugRoute(resource: SessionDebugResource): SessionDebugRouteSpec {
  return {
    resource,
    route: defineHttpRoute({
      control: "not-applicable",
      method: "GET",
      name: `session.debug.${resource}`,
      pattern: new RegExp(`^/sessions/([^/]+)/debug/${resource}$`, "u"),
    }),
  };
}
