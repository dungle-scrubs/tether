import type { IncomingMessage } from "node:http";
import type { URL } from "node:url";

import type { AuthMode, ServerConfig } from "../config.js";
import {
  AuthError,
  createAuthContext,
  type AuthContext,
  type AuthSigningSecrets,
  verifyAuthToken,
} from "./token.js";

export interface AuthRuntimeDebugInfo {
  /** Accepted signing key ids, never secret values. */
  readonly acceptedKids: readonly string[];
  /** Active signing key id. */
  readonly activeKid: string;
  /** Current enforcement mode. */
  readonly authMode: AuthMode;
}

export interface AuthRuntimeOptions {
  /** Active signing key id used for diagnostics. */
  readonly activeKid: string;
  /** Structured warning sink for auth boundary events. */
  readonly logger?: AuthRuntimeLogger;
  /** Auth enforcement mode. */
  readonly mode: AuthMode;
  /** Accepted verification secrets keyed by kid. */
  readonly secrets: AuthSigningSecrets;
}

export interface AuthRuntimeLogger {
  /** Emits one structured warning event without token contents. */
  readonly warn: (event: string, details: Record<string, unknown>) => void;
}

export interface AuthRuntime {
  /** Stops auth-owned background diagnostics. */
  readonly close: () => void;
  /** Returns auth diagnostics without exposing secrets. */
  readonly debugInfo: () => AuthRuntimeDebugInfo;
  /** Authenticates one REST request, or returns null when auth is disabled. */
  readonly authenticateHttpRequest: (request: IncomingMessage, url: URL) => AuthContext | null;
  /** Authenticates one WebSocket upgrade, or returns null when auth is disabled. */
  readonly authenticateWebSocketUpgrade: (request: IncomingMessage, url: URL) => AuthContext | null;
}

const disabledWarningIntervalMs = 5 * 60 * 1_000;
const noopAuthRuntimeLogger: AuthRuntimeLogger = {
  warn: () => undefined,
};

/** Creates the process-local auth enforcement runtime for HTTP and WebSocket boundaries. */
export function createAuthRuntime(options: AuthRuntimeOptions): AuthRuntime {
  const logger = options.logger ?? noopAuthRuntimeLogger;
  const sortedKids = Object.keys(options.secrets).sort();
  const disabledWarning = options.mode === "disabled" ? startDisabledModeWarning(logger) : null;
  return {
    authenticateHttpRequest: (request, url) => {
      if (options.mode === "disabled") {
        return null;
      }
      return authenticateBearerToken({
        method: request.method,
        route: url.pathname,
        token: extractBearerToken(request.headers.authorization),
        type: "http",
        logger,
        secrets: options.secrets,
      });
    },
    authenticateWebSocketUpgrade: (request, url) => {
      if (options.mode === "disabled") {
        return null;
      }
      return authenticateBearerToken({
        method: request.method,
        route: url.pathname,
        token: url.searchParams.get("access_token"),
        type: "ws",
        logger,
        secrets: options.secrets,
      });
    },
    close: () => {
      disabledWarning?.stop();
    },
    debugInfo: () => ({
      acceptedKids: sortedKids,
      activeKid: options.activeKid,
      authMode: options.mode,
    }),
  };
}

/** Builds auth runtime options from live server configuration. */
export function authRuntimeOptionsFromConfig(config: ServerConfig): AuthRuntimeOptions {
  return {
    activeKid: config.authSigningKid,
    mode: config.authMode,
    secrets: buildSigningSecrets(config),
  };
}

/** Converts an auth error into an HTTP status code. */
export function authErrorStatus(error: AuthError): number {
  return error === AuthError.RoleDenied || error === AuthError.ScopeDenied ? 403 : 401;
}

/** Renders a stable auth error payload without exposing token contents. */
export function authErrorPayload(error: AuthError): {
  readonly error: string;
  readonly reason: AuthError;
} {
  return {
    error: authErrorStatus(error) === 401 ? "Unauthorized" : "Forbidden",
    reason: error,
  };
}

/** Normalizes unknown thrown values into typed auth reasons for HTTP responses. */
export function authErrorFromUnknown(error: unknown): AuthError {
  return parseAuthError(error);
}

interface AuthenticateBearerTokenInput {
  readonly logger: AuthRuntimeLogger;
  readonly method: string | undefined;
  readonly route: string;
  readonly secrets: AuthSigningSecrets;
  readonly token: string | null;
  readonly type: "http" | "ws";
}

interface DisabledWarning {
  readonly stop: () => void;
}

/** Verifies one bearer-style token and logs redacted rejection context. */
function authenticateBearerToken(input: AuthenticateBearerTokenInput): AuthContext {
  if (!input.token) {
    logAuthReject(input, AuthError.Missing);
    throw new Error(AuthError.Missing);
  }
  try {
    return createAuthContext(
      verifyAuthToken(input.token, {
        secrets: input.secrets,
      }),
    );
  } catch (error) {
    const reason = parseAuthError(error);
    logAuthReject(input, reason);
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
