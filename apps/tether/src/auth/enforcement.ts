import type { IncomingMessage } from "node:http";
import type { URL } from "node:url";

import type { AuthMode, ServerConfig } from "../config.js";
import {
  AuthGrantAuthorityError,
  createAuthGrantAuthority,
  type AuthGrantAuthority,
} from "./grant-authority.js";
import type { AuthGrantStore } from "./grant-stores.js";
import type { AuthTicketStore } from "./grant-stores.js";
import type { BrowserSessionRecord } from "./browser-pairing-stores.js";
import {
  AuthTicketAuthorityError,
  createAuthTicketAuthority,
  type AuthTicketAuthority,
} from "./ticket-authority.js";
import {
  AuthError,
  createAuthContext,
  type AuthContext,
  type AuthSigningSecrets,
  verifyLegacyAuthToken,
} from "./token.js";

export interface AuthRuntimeDebugInfo {
  /** Accepted signing key ids, never secret values. */
  readonly acceptedKids: readonly string[];
  /** Active signing key id. */
  readonly activeKid: string;
  /** Current enforcement mode. */
  readonly authMode: AuthMode;
  /** Provisional pre-enforcement tgr2 issuance gate, replaced by M7 rollout readiness. */
  readonly grantIssuanceEnabled: boolean;
  /** Whether revocation-blind legacy stateless tokens are accepted in required mode. */
  readonly legacyTokensAllowed: boolean;
  /** Current bounded durable-grant denial cache size. */
  readonly negativeGrantCacheEntries: number;
  /** Hard cap for the durable-grant denial cache. */
  readonly negativeGrantCacheMaximumEntries: number;
}

export interface AuthRuntimeOptions {
  /** Active signing key id used for diagnostics. */
  readonly activeKid: string;
  /**
   * Migration escape hatch that accepts legacy stateless tokens in required
   * mode. Legacy tokens carry no durable grant row and cannot be revoked, so
   * the default rejects them; enable only while rotating a fleet onto tgr2.
   */
  readonly allowLegacyTokens?: boolean;
  /** Browser-session reader used to keep operator grants off generic bearer routes. */
  readonly browserSessionStore?: {
    readonly findBrowserSession: (grantJti: string) => Promise<BrowserSessionRecord | null>;
  };
  /** Durable grant issuer used by lifecycle operations, when configured. */
  readonly issuer?: string | null;
  /** PostgreSQL grant reader required for tgr2 acceptance. */
  readonly grantStore?: AuthGrantStore;
  /** Structured warning sink for auth boundary events. */
  readonly logger?: AuthRuntimeLogger;
  /** Auth enforcement mode. */
  readonly mode: AuthMode;
  /** Injectable authorization clock. */
  readonly now?: () => Date;
  /** Provisional test-only issuance gate until M7 supplies mixed-replica readiness. */
  readonly preEnforcementGrantIssuanceEnabled?: boolean;
  /** Accepted verification secrets keyed by kid. */
  readonly secrets: AuthSigningSecrets;
  /** PostgreSQL ticket store required for single-use browser admission. */
  readonly ticketStore?: AuthTicketStore;
}

export interface AuthRuntimeLogger {
  /** Emits one structured warning event without token contents. */
  readonly warn: (event: string, details: Record<string, unknown>) => void;
}

export interface AuthRuntime {
  /** Authenticates one dedicated browser-session cookie token outside generic REST routing. */
  readonly authenticateBrowserSessionToken: (token: string) => Promise<AuthContext>;
  /** Stops auth-owned background diagnostics. */
  readonly close: () => void;
  /** Returns auth diagnostics without exposing secrets. */
  readonly debugInfo: () => AuthRuntimeDebugInfo;
  /** Authenticates one REST request, or returns null when auth is disabled. */
  readonly authenticateHttpRequest: (
    request: IncomingMessage,
    url: URL,
  ) => Promise<AuthContext | null>;
  /** Authenticates one WebSocket upgrade and retains private command reauthorization. */
  readonly authenticateWebSocketUpgrade: (
    request: IncomingMessage,
    url: URL,
  ) => Promise<AuthenticatedWebSocketAuth>;
}

/** Authenticated socket context plus a bearer-private command authorization closure. */
export interface AuthenticatedWebSocketAuth {
  /** Nonsecret authorization context shared with command handlers. */
  readonly context: AuthContext | null;
  /** Revalidates the original credential at current key, expiry, and PostgreSQL state. */
  readonly authorizeCommand: () => Promise<void>;
}

const disabledWarningIntervalMs = 5 * 60 * 1_000;
const noopAuthRuntimeLogger: AuthRuntimeLogger = {
  warn: () => undefined,
};

/** Creates the process-local auth enforcement runtime for HTTP and WebSocket boundaries. */
export function createAuthRuntime(options: AuthRuntimeOptions): AuthRuntime {
  const logger = options.logger ?? noopAuthRuntimeLogger;
  const disabledWarning = options.mode === "disabled" ? startDisabledModeWarning(logger) : null;
  const authority = createConfiguredGrantAuthority(options);
  const ticketAuthority = createConfiguredTicketAuthority(options, authority);
  const allowLegacyTokens = options.allowLegacyTokens ?? false;
  return {
    authenticateBrowserSessionToken: async (token) => {
      if (options.mode === "disabled") {
        throw new Error(AuthError.ClaimInvalid);
      }
      return authenticateBearerToken({
        allowLegacyTokens: false,
        authority,
        logger,
        method: "COOKIE",
        now: options.now,
        route: "/operator",
        secrets: options.secrets,
        token,
        type: "browser-cookie",
      });
    },
    authenticateHttpRequest: async (request, url) => {
      if (options.mode === "disabled") {
        return null;
      }
      const authentication = {
        allowLegacyTokens,
        authority,
        method: request.method,
        now: options.now,
        route: url.pathname,
        token: extractBearerToken(request.headers.authorization),
        type: "http",
        logger,
        secrets: options.secrets,
      } as const;
      const context = await authenticateBearerToken(authentication);
      rejectBrowserGrantOnGenericBoundary(authentication, context);
      return context;
    },
    authenticateWebSocketUpgrade: async (request, url) => {
      if (options.mode === "disabled") {
        return { authorizeCommand: async () => undefined, context: null };
      }
      const route = url.pathname;
      const authorizationHeader = request.headers.authorization;
      const headerToken = extractBearerToken(authorizationHeader);
      const queryToken = url.searchParams.get("access_token");
      const ticket = url.searchParams.get("ticket");
      const credentialCount = [authorizationHeader, queryToken, ticket].filter(
        (credential) => credential !== undefined && credential !== null,
      ).length;
      if (credentialCount !== 1) {
        const rejected = {
          allowLegacyTokens,
          authority,
          logger,
          method: request.method,
          now: options.now,
          route,
          secrets: options.secrets,
          token: null,
          type: "ws" as const,
        };
        logAuthReject(rejected, AuthError.ClaimInvalid);
        throw new Error(AuthError.ClaimInvalid);
      }
      if (ticket !== null) {
        const authenticated = await authenticateWebSocketTicket({
          logger,
          method: request.method,
          route,
          ticket,
          ticketAuthority,
        });
        await authorizeBrowserTicketOrigin({
          authenticated,
          browserSessionStore: options.browserSessionStore,
          logger,
          method: request.method,
          origin: request.headers.origin,
          route,
        });
        return authenticated;
      }
      const token = headerToken ?? queryToken;
      const context = await authenticateBearerToken({
        allowLegacyTokens,
        authority,
        method: request.method,
        now: options.now,
        route,
        token,
        type: "ws",
        logger,
        secrets: options.secrets,
      });
      rejectBrowserGrantOnGenericBoundary(
        {
          allowLegacyTokens,
          authority,
          logger,
          method: request.method,
          now: options.now,
          route,
          secrets: options.secrets,
          token,
          type: "ws",
        },
        context,
      );
      return {
        authorizeCommand: async () => {
          await authenticateBearerToken({
            allowLegacyTokens,
            authority,
            logger,
            method: "COMMAND",
            now: options.now,
            route,
            secrets: options.secrets,
            token,
            type: "ws-command",
          });
        },
        context,
      };
    },
    close: () => {
      disabledWarning?.stop();
    },
    debugInfo: () => {
      const authorityDebug = authority?.debugInfo();
      return {
        acceptedKids: Object.keys(options.secrets).sort(),
        activeKid: options.activeKid,
        authMode: options.mode,
        grantIssuanceEnabled: options.preEnforcementGrantIssuanceEnabled ?? false,
        legacyTokensAllowed: allowLegacyTokens,
        negativeGrantCacheEntries: authorityDebug?.negativeCacheEntries ?? 0,
        negativeGrantCacheMaximumEntries: authorityDebug?.negativeCacheMaximumEntries ?? 0,
      };
    },
  };
}

/** Builds auth runtime options from live server configuration. */
export function authRuntimeOptionsFromConfig(config: ServerConfig): AuthRuntimeOptions {
  return {
    activeKid: config.authSigningKid,
    allowLegacyTokens: config.authAllowLegacyTokens,
    issuer: config.authIssuer,
    mode: config.authMode,
    secrets: buildSigningSecrets(config),
  };
}

/** Converts an auth error into an HTTP status code. */
export function authErrorStatus(error: AuthError): number {
  if (error === AuthError.StoreUnavailable) return 503;
  return error === AuthError.RoleDenied || error === AuthError.ScopeDenied ? 403 : 401;
}

/** Renders a stable auth error payload without exposing token contents. */
export function authErrorPayload(error: AuthError): {
  readonly error: string;
  readonly reason: AuthError;
} {
  return {
    error:
      authErrorStatus(error) === 503
        ? "Service Unavailable"
        : authErrorStatus(error) === 401
          ? "Unauthorized"
          : "Forbidden",
    reason: error,
  };
}

/** Normalizes unknown thrown values into typed auth reasons for HTTP responses. */
export function authErrorFromUnknown(error: unknown): AuthError {
  return parseAuthError(error);
}

interface AuthenticateBearerTokenInput {
  readonly allowLegacyTokens: boolean;
  readonly authority: AuthGrantAuthority | null;
  readonly logger: AuthRuntimeLogger;
  readonly method: string | undefined;
  readonly now?: (() => Date) | undefined;
  readonly route: string;
  readonly secrets: AuthSigningSecrets;
  readonly token: string | null;
  readonly type: "browser-cookie" | "http" | "ws" | "ws-command";
}

interface DisabledWarning {
  readonly stop: () => void;
}

/** Rejects durable browser grants before they can enter generic REST or bearer WebSocket routes. */
function rejectBrowserGrantOnGenericBoundary(
  authentication: AuthenticateBearerTokenInput,
  context: AuthContext,
): void {
  if (context.grantSource === "browser") {
    logAuthReject(authentication, AuthError.RoleDenied);
    throw new Error(AuthError.RoleDenied);
  }
}

/** Verifies one bearer-style token and logs redacted rejection context. */
async function authenticateBearerToken(input: AuthenticateBearerTokenInput): Promise<AuthContext> {
  if (!input.token) {
    logAuthReject(input, AuthError.Missing);
    throw new Error(AuthError.Missing);
  }
  try {
    if (input.token.startsWith("tgr2.")) {
      if (!input.authority) throw new AuthGrantAuthorityError("auth_claim_invalid");
      return await input.authority.authenticateRestBearer(input.token);
    }
    if (!input.allowLegacyTokens) {
      // Legacy stateless tokens carry no durable grant row, so revocation
      // cannot reach them. Required enforcement rejects them unless the
      // explicit migration escape hatch is enabled.
      throw new Error(AuthError.LegacyTokenRejected);
    }
    return createAuthContext(
      verifyLegacyAuthToken(input.token, {
        ...(input.now === undefined ? {} : { now: input.now() }),
        secrets: input.secrets,
      }),
    );
  } catch (error) {
    const reason = parseAuthError(error);
    logAuthReject(input, reason);
    throw new Error(reason);
  }
}

function createConfiguredGrantAuthority(options: AuthRuntimeOptions): AuthGrantAuthority | null {
  return options.issuer && options.grantStore
    ? createAuthGrantAuthority({
        issuer: options.issuer,
        ...(options.now === undefined ? {} : { now: options.now }),
        secrets: options.secrets,
        store: options.grantStore,
      })
    : null;
}

function createConfiguredTicketAuthority(
  options: AuthRuntimeOptions,
  authority: AuthGrantAuthority | null,
): AuthTicketAuthority | null {
  return authority && options.ticketStore
    ? createAuthTicketAuthority({
        grantAuthority: authority,
        ...(options.now === undefined ? {} : { now: options.now }),
        store: options.ticketStore,
      })
    : null;
}

/** Applies exact WebSocket Origin only when the ticket belongs to a browser grant. */
async function authorizeBrowserTicketOrigin(input: {
  readonly authenticated: AuthenticatedWebSocketAuth;
  readonly browserSessionStore: AuthRuntimeOptions["browserSessionStore"];
  readonly logger: AuthRuntimeLogger;
  readonly method: string | undefined;
  readonly origin: string | undefined;
  readonly route: string;
}): Promise<void> {
  const grantJti = input.authenticated.context?.grantJti;
  if (input.authenticated.context?.grantSource !== "browser") return;
  if (grantJti === null || grantJti === undefined || input.browserSessionStore === undefined) {
    throw new Error(AuthError.StoreUnavailable);
  }
  let session: BrowserSessionRecord | null;
  try {
    session = await input.browserSessionStore.findBrowserSession(grantJti);
  } catch {
    input.logger.warn("auth.reject", {
      method: input.method ?? null,
      reason: AuthError.StoreUnavailable,
      route: input.route,
      transport: "ws",
    });
    throw new Error(AuthError.StoreUnavailable);
  }
  if (session === null) {
    input.logger.warn("auth.reject", {
      method: input.method ?? null,
      reason: AuthError.RoleDenied,
      route: input.route,
      transport: "ws",
    });
    throw new Error(AuthError.RoleDenied);
  }
  if (input.origin !== session.origin) {
    input.logger.warn("auth.reject", {
      method: input.method ?? null,
      reason: AuthError.OriginDenied,
      route: input.route,
      transport: "ws",
    });
    throw new Error(AuthError.OriginDenied);
  }
}

async function authenticateWebSocketTicket(input: {
  readonly logger: AuthRuntimeLogger;
  readonly method: string | undefined;
  readonly route: string;
  readonly ticket: string;
  readonly ticketAuthority: AuthTicketAuthority | null;
}): Promise<AuthenticatedWebSocketAuth> {
  try {
    if (input.ticketAuthority === null) {
      throw new AuthTicketAuthorityError("auth_claim_invalid");
    }
    return await input.ticketAuthority.authenticateTicket(input.ticket);
  } catch (error) {
    const reason = parseAuthError(error);
    input.logger.warn("auth.reject", {
      method: input.method ?? null,
      reason,
      route: input.route,
      transport: "ws",
    });
    throw new Error(reason);
  }
}

/** Extracts a Bearer token from an Authorization header value. */
function extractBearerToken(value: string | undefined): string | null {
  const match = value?.match(/^Bearer\s+(.+)$/iu);
  return match?.[1] ?? null;
}

/** Builds accepted verification secrets from config while keeping active kid current. */
function buildSigningSecrets(config: ServerConfig): AuthSigningSecrets {
  return {
    ...config.authAcceptedSigningSecrets,
    ...(config.authSigningSecret === null
      ? {}
      : { [config.authSigningKid]: config.authSigningSecret }),
  };
}

/** Normalizes thrown auth failures back into the typed reason set. */
function parseAuthError(error: unknown): AuthError {
  if (error instanceof AuthTicketAuthorityError) {
    return error.code as AuthError;
  }
  if (error instanceof AuthGrantAuthorityError) {
    return error.code as AuthError;
  }
  if (error instanceof Error && Object.values(AuthError).includes(error.message as AuthError)) {
    return error.message as AuthError;
  }
  return AuthError.Malformed;
}

/** Logs a redacted auth rejection record at the transport boundary. */
function logAuthReject(input: AuthenticateBearerTokenInput, reason: AuthError): void {
  input.logger.warn("auth.reject", {
    method: input.method ?? null,
    reason,
    route: input.route,
    transport: input.type,
  });
}

/** Emits loud diagnostics while auth is disabled. */
function startDisabledModeWarning(logger: AuthRuntimeLogger): DisabledWarning {
  const warn = (): void => {
    logger.warn("auth.disabled", {
      authMode: "disabled",
      message: "Tether auth enforcement is disabled; use only for local development.",
    });
  };
  warn();
  const timer = setInterval(warn, disabledWarningIntervalMs);
  timer.unref?.();
  return {
    stop: () => {
      clearInterval(timer);
    },
  };
}
