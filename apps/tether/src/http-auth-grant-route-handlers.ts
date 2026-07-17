import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { Effect } from "effect";
import { z } from "zod";

import type { AuthGrantLifecycle } from "./auth/grant-lifecycle.js";
import { maximumAuthGrantLifetimeSeconds } from "./auth/grant-lifecycle.js";
import { authorize } from "./auth/authorize.js";
import { authRoles, type AuthContext } from "./auth/token.js";
import { parseJsonBody, sendAuthError, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute } from "./http-route-spec.js";

const createSchema = z
  .object({
    reasonCode: z
      .enum(["key-rotation", "operator-request", "security-response"])
      .default("operator-request"),
    role: z.enum(authRoles),
    sessionScope: z.string().min(1).max(255),
    subject: z.string().min(1).max(255),
    ttlSeconds: z.number().int().positive().max(maximumAuthGrantLifetimeSeconds).optional(),
  })
  .strict();
const revokeSchema = z
  .object({
    reasonCode: z
      .enum(["key-rotation", "operator-request", "security-response"])
      .default("operator-request"),
  })
  .strict();
const grantJtiSchema = z
  .string()
  .min(7)
  .max(128)
  .regex(/^grant_[A-Za-z0-9_-]+$/u);

export const authGrantHttpRoutes = {
  create: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "auth.grants.create",
    pattern: /^\/auth\/grants$/u,
  }),
  inspect: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "auth.grants.inspect",
    pattern: /^\/auth\/grants\/([^/]+)$/u,
  }),
  list: defineHttpRoute({
    control: "not-applicable",
    method: "GET",
    name: "auth.grants.list",
    pattern: /^\/auth\/grants$/u,
  }),
  revoke: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "auth.grants.revoke",
    pattern: /^\/auth\/grants\/([^/]+)\/revoke$/u,
  }),
} as const;

interface Input {
  readonly authContext: AuthContext | null;
  readonly lifecycle: AuthGrantLifecycle;
  readonly maxBodyBytes: number;
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
  readonly url: URL;
}

/** Handles admin-only durable authentication grant lifecycle operations. */
export function handleAuthGrantHttpRoute(input: Input): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const route = matchRoute(input.request.method, input.url.pathname);
    if (route === null) return false;
    // Grant lifecycle is a global admin surface: omitting sessionId means
    // authorize accepts only a service-scoped (`*`) admin grant, so a
    // session-scoped admin cannot mint, list, inspect, or revoke grants.
    const denied = authorize({ action: "admin", context: input.authContext });
    if (denied) {
      sendAuthError(input.response, denied);
      return true;
    }
    const actorSubject = input.authContext?.participantId ?? "auth-disabled-admin";
    if (route.kind === "create") {
      if (!input.lifecycle.issuanceEnabled) {
        sendJson(input.response, 503, {
          error: "Authentication grant issuance unavailable",
          reason: "auth_grant_issuance_gated",
        });
        return true;
      }
      const body = yield* parseJsonBody(input.request, createSchema, {
        maxBytes: input.maxBodyBytes,
        routeName: authGrantHttpRoutes.create.name,
      });
      const created = yield* Effect.promise(() =>
        input.lifecycle.create({
          actorSubject,
          reasonCode: body.reasonCode,
          role: body.role,
          sessionScope: body.sessionScope,
          source: "admin",
          subject: body.subject,
          ...(body.ttlSeconds === undefined ? {} : { ttlSeconds: body.ttlSeconds }),
        }),
      );
      sendJson(input.response, 201, { ...created });
      return true;
    }
    if (route.kind === "list") {
      const limit = yield* Effect.try({
        catch: (error) => error,
        try: () =>
          z.coerce
            .number()
            .int()
            .min(1)
            .max(100)
            .parse(input.url.searchParams.get("limit") ?? "50"),
      });
      sendJson(input.response, 200, {
        grants: yield* Effect.promise(() => input.lifecycle.list(limit)),
      });
      return true;
    }
    if (route.kind === "inspect") {
      const jti = yield* parseGrantJti(route.jti);
      const grant = yield* Effect.promise(() => input.lifecycle.inspect(jti));
      return sendGrant(input.response, grant);
    }
    const body = yield* parseJsonBody(input.request, revokeSchema, {
      maxBytes: input.maxBodyBytes,
      routeName: authGrantHttpRoutes.revoke.name,
    });
    const jti = yield* parseGrantJti(route.jti);
    const result = yield* Effect.promise(() =>
      input.lifecycle.revoke(jti, actorSubject, body.reasonCode),
    );
    if (result.grant === null) return sendGrant(input.response, null);
    sendJson(input.response, 200, result);
    return true;
  });
}

function parseGrantJti(value: string): Effect.Effect<string, unknown> {
  return Effect.try({ catch: (error) => error, try: () => grantJtiSchema.parse(value) });
}

type MatchedRoute =
  | { readonly kind: "create" }
  | { readonly kind: "list" }
  | { readonly jti: string; readonly kind: "inspect" }
  | { readonly jti: string; readonly kind: "revoke" };

function matchRoute(method: string | undefined, pathname: string): MatchedRoute | null {
  if (matchHttpRoute(authGrantHttpRoutes.create, method, pathname)) return { kind: "create" };
  if (matchHttpRoute(authGrantHttpRoutes.list, method, pathname)) return { kind: "list" };
  const revoke = matchHttpRoute(authGrantHttpRoutes.revoke, method, pathname);
  if (revoke?.[1]) return { jti: revoke[1], kind: "revoke" };
  const inspect = matchHttpRoute(authGrantHttpRoutes.inspect, method, pathname);
  return inspect?.[1] ? { jti: inspect[1], kind: "inspect" } : null;
}

function sendGrant(
  response: ServerResponse,
  grant: Awaited<ReturnType<AuthGrantLifecycle["inspect"]>>,
): true {
  if (grant === null) {
    sendJson(response, 404, {
      error: "Authentication grant not found",
      reason: "auth_grant_not_found",
    });
  } else {
    sendJson(response, 200, { grant });
  }
  return true;
}
