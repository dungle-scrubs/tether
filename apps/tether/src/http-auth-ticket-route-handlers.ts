import type { IncomingMessage, ServerResponse } from "node:http";
import type { URL } from "node:url";

import { Effect } from "effect";

import type { AuthTicketLifecycle } from "./auth/ticket-lifecycle.js";
import type { AuthContext } from "./auth/token.js";
import { sendAuthError, sendJson } from "./http-route-runtime.js";
import { defineHttpRoute, matchHttpRoute } from "./http-route-spec.js";

export const authTicketHttpRoutes = {
  mint: defineHttpRoute({
    control: "not-applicable",
    method: "POST",
    name: "auth.tickets.mint",
    pattern: /^\/auth\/tickets$/u,
  }),
} as const;

interface Input {
  readonly authContext: AuthContext | null;
  readonly lifecycle: AuthTicketLifecycle;
  readonly request: IncomingMessage;
  readonly response: ServerResponse;
  readonly url: URL;
}

/** Handles durable-parent ticket minting without exposing raw tickets to other routes. */
export function handleAuthTicketHttpRoute(input: Input): Effect.Effect<boolean, unknown> {
  return Effect.gen(function* () {
    if (!matchHttpRoute(authTicketHttpRoutes.mint, input.request.method, input.url.pathname)) {
      return false;
    }
    const context = input.authContext;
    if (context === null || context.grantJti === null) {
      sendAuthError(input.response, "auth_claim_invalid");
      return true;
    }
    const created = yield* Effect.promise(() => input.lifecycle.mint(context));
    sendJson(input.response, 201, { ...created });
    return true;
  });
}
