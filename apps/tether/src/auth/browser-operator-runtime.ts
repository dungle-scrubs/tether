import type { IncomingMessage } from "node:http";

import { type OperatorGrantScope, operatorGrantScopeSchema } from "@dungle-scrubs/tether-protocol";

import type { AuthRuntime } from "./enforcement.js";
import {
  authorizeOperator,
  type OperatorAuthorityDenialReason,
  type OperatorAuthorityRequest,
} from "./operator-authority.js";
import { browserSessionCookieName, hashBrowserCredential } from "./browser-pairing.js";
import { opaqueCredentialHashesEqual } from "./opaque-credential.js";
import type { BrowserPairingStore, BrowserSessionRecord } from "./browser-pairing-stores.js";
import type { AuthContext } from "./token.js";

/** Stable browser-operator boundary denials safe for HTTP responses and metrics. */
export type BrowserOperatorAuthorityErrorCode =
  | OperatorAuthorityDenialReason
  | "operator_auth_denied"
  | "operator_cookie_ambiguous"
  | "operator_cookie_missing"
  | "operator_csrf_denied"
  | "operator_origin_denied"
  | "operator_role_denied"
  | "operator_scope_invalid"
  | "operator_session_invalid";

/** Typed fail-closed operator authority rejection without credential material. */
export class BrowserOperatorAuthorityError extends Error {
  readonly name = "BrowserOperatorAuthorityError";

  constructor(readonly code: BrowserOperatorAuthorityErrorCode) {
    super(code);
  }
}

/** Required scope tuple and anti-CSRF policy for one operator route. */
export interface BrowserOperatorAuthorizationRequest {
  readonly csrfRequired: boolean;
  readonly resource: OperatorAuthorityRequest;
}

/** Authenticated operator identity and durable browser-session state. */
export interface AuthorizedBrowserOperator {
  readonly context: AuthContext;
  readonly scope: OperatorGrantScope;
  readonly session: BrowserSessionRecord;
}

/** Dedicated browser cookie authority that cannot authenticate generic REST routes. */
export interface BrowserOperatorRuntime {
  readonly authorize: (
    request: IncomingMessage,
    requirement: BrowserOperatorAuthorizationRequest,
  ) => Promise<AuthorizedBrowserOperator>;
}

/** Browser operator runtime dependencies. */
export interface BrowserOperatorRuntimeOptions {
  readonly auth: AuthRuntime;
  readonly logger?: {
    readonly warn: (
      event: "browser.operator.denied",
      details: {
        readonly csrfRequired: boolean;
        readonly permission: OperatorAuthorityRequest["permission"];
        readonly reason: BrowserOperatorAuthorityErrorCode;
      },
    ) => void;
  };
  readonly store: BrowserPairingStore;
}

/** Creates exact-Origin, grant-bound CSRF, and provider-neutral scope enforcement. */
export function createBrowserOperatorRuntime(
  options: BrowserOperatorRuntimeOptions,
): BrowserOperatorRuntime {
  return {
    authorize: async (request, requirement) => {
      try {
        const bearer = extractBrowserSessionCookie(request.headers.cookie);
        const context = await authenticateBrowserToken(options.auth, bearer);
        if (context.grantJti === null || context.role !== "observer") {
          throw new BrowserOperatorAuthorityError("operator_role_denied");
        }
        const authority = await options.store.findBrowserAuthority(context.grantJti);
        if (authority === null || authority.session.grantJti !== context.grantJti) {
          throw new BrowserOperatorAuthorityError("operator_session_invalid");
        }
        const scope = operatorGrantScopeSchema.safeParse(authority.scope);
        if (!scope.success) {
          throw new BrowserOperatorAuthorityError("operator_scope_invalid");
        }
        if (
          request.headers.origin !== undefined &&
          request.headers.origin !== authority.session.origin
        ) {
          throw new BrowserOperatorAuthorityError("operator_origin_denied");
        }
        if (requirement.csrfRequired) {
          if (request.headers.origin !== authority.session.origin) {
            throw new BrowserOperatorAuthorityError("operator_origin_denied");
          }
          assertCsrfToken(request.headers["x-tether-csrf"], authority.session.csrfTokenHash);
        }
        const denial = authorizeOperator(scope.data, requirement.resource);
        if (denial !== null) {
          throw new BrowserOperatorAuthorityError(denial);
        }
        return { context, scope: scope.data, session: authority.session };
      } catch (error) {
        if (error instanceof BrowserOperatorAuthorityError) {
          options.logger?.warn("browser.operator.denied", {
            csrfRequired: requirement.csrfRequired,
            permission: requirement.resource.permission,
            reason: error.code,
          });
        }
        throw error;
      }
    },
  };
}

/** Extracts exactly one dedicated browser session cookie. */
function extractBrowserSessionCookie(cookieHeader: string | undefined): string {
  if (cookieHeader === undefined) {
    throw new BrowserOperatorAuthorityError("operator_cookie_missing");
  }
  const values = cookieHeader
    .split(";")
    .map((part) => part.trim().split("=", 2))
    .filter(([name]) => name === browserSessionCookieName)
    .map(([, value]) => value ?? "");
  if (values.length === 0 || values[0] === "") {
    throw new BrowserOperatorAuthorityError("operator_cookie_missing");
  }
  if (values.length !== 1) {
    throw new BrowserOperatorAuthorityError("operator_cookie_ambiguous");
  }
  return values[0] as string;
}

/** Collapses token verification detail at the dedicated browser boundary. */
async function authenticateBrowserToken(auth: AuthRuntime, bearer: string): Promise<AuthContext> {
  try {
    return await auth.authenticateBrowserSessionToken(bearer);
  } catch {
    throw new BrowserOperatorAuthorityError("operator_auth_denied");
  }
}

/** Verifies a grant-bound anti-CSRF token in constant time. */
function assertCsrfToken(value: string | string[] | undefined, expectedHash: string): void {
  if (typeof value !== "string") {
    throw new BrowserOperatorAuthorityError("operator_csrf_denied");
  }
  if (!opaqueCredentialHashesEqual(hashBrowserCredential(value), expectedHash)) {
    throw new BrowserOperatorAuthorityError("operator_csrf_denied");
  }
}
