import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import {
  browserPairingCreateResponseSchema,
  browserPairingExchangeResponseSchema,
  createBrowserPairingRequestSchema,
  exchangeBrowserPairingRequestSchema,
} from "@dungle-scrubs/tether-protocol";
import { Effect } from "effect";

import {
  BrowserPairingError,
  type BrowserPairingLifecycle,
  browserSessionCookieName,
} from "./auth/browser-pairing.js";
import type { CorsOptions } from "./http-route-runtime.js";
import { parseJsonBody, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute } from "./http-route-spec.js";

/** Unauthenticated pairing endpoints protected by exact Origin and one-time credentials. */
export const browserPairingHttpRoutes = {
  create: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "browser.pairing.create",
    pattern: /^\/browser\/pairing-requests$/u,
  }),
  exchange: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "browser.pairing.exchange",
    pattern: /^\/browser\/pairing-requests\/(?<requestId>pair_[A-Za-z0-9_-]{1,120})\/exchange$/u,
  }),
} as const;

/** Browser pairing route dependencies. */
export interface BrowserPairingHttpRouteInput {
  readonly cors: CorsOptions;
  readonly lifecycle: BrowserPairingLifecycle | null;
  readonly maxBodyBytes: number;
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
  readonly url: URL;
}

/** Serializes a browser bearer into the only cookie form accepted by the operator boundary. */
export function serializeBrowserSessionCookie(input: {
  readonly bearer: string;
  readonly expiresAt: string;
}): string {
  const expiresAt = new Date(input.expiresAt);
  if (Number.isNaN(expiresAt.getTime())) {
    throw new Error("browser_session_expiry_invalid");
  }
  return [
    `${browserSessionCookieName}=${input.bearer}`,
    "Path=/",
    `Expires=${expiresAt.toUTCString()}`,
    "Secure",
    "HttpOnly",
    "SameSite=Strict",
  ].join("; ");
}

/** Handles creation and one-time exchange without admitting cookie credentials to generic REST. */
export function handleBrowserPairingHttpRoute(
  input: BrowserPairingHttpRouteInput,
): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    const createMatch = matchHttpRoute(
      browserPairingHttpRoutes.create,
      input.request.method,
      input.url.pathname,
    );
    const exchangeMatch = matchHttpRoute(
      browserPairingHttpRoutes.exchange,
      input.request.method,
      input.url.pathname,
    );
    if (!createMatch && !exchangeMatch) return false;
    if (input.lifecycle === null) {
      sendJson(input.response, 503, {
        error: "Browser pairing unavailable",
        reason: "pairing_unavailable",
      });
      return true;
    }
    const lifecycle = input.lifecycle;
    const origin = exactPairingOrigin(input.request, input.cors);
    if (origin === null) {
      sendJson(input.response, 403, {
        error: "Origin not allowed",
        reason: "pairing_origin_denied",
      });
      return true;
    }
    input.response.setHeader("cache-control", "no-store");
    if (createMatch) {
      const body = yield* parseJsonBody(input.request, createBrowserPairingRequestSchema, {
        maxBytes: input.maxBodyBytes,
        routeName: browserPairingHttpRoutes.create.name,
      });
      const result = yield* Effect.either(
        Effect.tryPromise({
          catch: (error) => error,
          try: () =>
            lifecycle.create({
              ...body,
              origin,
              sourceAddress: sourceAddress(input.request),
            }),
        }),
      );
      if (result._tag === "Left") {
        sendPairingError(input.response, result.left);
        return true;
      }
      sendJson(input.response, 201, browserPairingCreateResponseSchema.parse(result.right));
      return true;
    }
    const requestId = exchangeMatch?.groups?.requestId;
    if (requestId === undefined) {
      sendJson(input.response, 404, { error: "Not found" });
      return true;
    }
    const body = yield* parseJsonBody(input.request, exchangeBrowserPairingRequestSchema, {
      maxBytes: input.maxBodyBytes,
      routeName: browserPairingHttpRoutes.exchange.name,
    });
    const result = yield* Effect.either(
      Effect.tryPromise({
        catch: (error) => error,
        try: () =>
          lifecycle.exchange(requestId, body, {
            origin,
            sourceAddress: sourceAddress(input.request),
          }),
      }),
    );
    if (result._tag === "Left") {
      sendPairingError(input.response, result.left);
      return true;
    }
    input.response.setHeader("set-cookie", serializeBrowserSessionCookie(result.right));
    sendJson(
      input.response,
      200,
      browserPairingExchangeResponseSchema.parse({
        csrfToken: result.right.csrfToken,
        expiresAt: result.right.expiresAt,
        grantJti: result.right.grantJti,
        scope: result.right.scope,
        status: result.right.status,
      }),
    );
    return true;
  });
}

/** Returns an exact allowlisted Origin, rejecting missing and multi-value headers. */
function exactPairingOrigin(request: IncomingMessage, cors: CorsOptions): string | null {
  const origin = request.headers.origin;
  return typeof origin === "string" && cors.allowedOrigins.includes(origin) ? origin : null;
}

/** Hashes the transport source before it crosses the persistence boundary. */
function sourceAddress(request: IncomingMessage): string | null {
  const address = request.socket.remoteAddress;
  return address ?? null;
}

/** Maps bounded pairing lifecycle errors without logging credential values. */
function sendPairingError(response: ServerResponse, error: unknown): void {
  if (!(error instanceof BrowserPairingError)) {
    throw error;
  }
  const statusByCode: Record<BrowserPairingError["code"], number> = {
    pairing_already_exchanged: 410,
    pairing_expired: 410,
    pairing_generation_failed: 503,
    pairing_invalidated: 410,
    pairing_nonce_mismatch: 401,
    pairing_not_confirmed: 409,
    pairing_not_found: 404,
    pairing_origin_mismatch: 401,
    pairing_rate_limited: 429,
    pairing_scope_invalid: 400,
    pairing_secret_invalid: 401,
  };
  sendJson(response, statusByCode[error.code], {
    error: "Browser pairing failed",
    reason: error.code,
  });
}
