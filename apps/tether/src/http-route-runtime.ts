import type { IncomingMessage, ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";

import { Effect } from "effect";
import { ZodError, type z } from "zod";

import { authErrorPayload, authErrorStatus } from "./auth/enforcement.js";
import type { AuthError } from "./auth/token.js";
import {
  SessionEventSequenceRangeError,
  SessionNotFoundError,
  type ControlLeaseClaim,
} from "./db.js";
import type { SubscriptionHub } from "./hub.js";
import {
  bodyTooLargeError,
  isResourceLimitExceeded,
  resourceLimitReason,
  type ResourceLimitRuntime,
} from "./resource-limits.js";
import { SessionServicePersistenceError } from "./session-service-contracts.js";
import type { ControlChannel, SessionEvent } from "./types.js";

/** Stable public message for unexpected HTTP route failures. */
const internalServerErrorMessage = "Internal server error";
const defaultAllowedCorsHeaders = "authorization, content-type";
const defaultAllowedCorsMethods = "GET, POST, OPTIONS";

/** CORS allowlist configuration for browser-based non-repo clients. */
export interface CorsOptions {
  readonly allowedOrigins: readonly string[];
}

/** Process-level CORS options loaded from BROWSER_ALLOWED_ORIGINS. */
export function readCorsOptionsFromEnv(env: NodeJS.ProcessEnv = process.env): CorsOptions {
  return {
    allowedOrigins: parseAllowedOrigins(env.BROWSER_ALLOWED_ORIGINS),
  };
}

/** Handles OPTIONS preflight through the shared route boundary. */
export function handleCorsPreflight(
  request: IncomingMessage,
  response: ServerResponse,
  options: CorsOptions,
): boolean {
  if (request.method !== "OPTIONS") {
    return false;
  }
  const origin = request.headers.origin;
  if (typeof origin === "string" && options.allowedOrigins.includes(origin)) {
    writeCorsHeaders(response, origin, request);
    response.writeHead(204);
    response.end();
    return true;
  }
  logCorsDenied({ origin: typeof origin === "string" ? origin : null, route: request.url ?? null });
  response.writeHead(403, { "content-type": "application/json" });
  response.end(JSON.stringify({ error: "CORS origin not allowed", reason: "cors_denied" }));
  return true;
}

/** Applies simple-request CORS headers when the request origin is allowlisted. */
export function applyCorsResponseHeaders(
  request: IncomingMessage,
  response: ServerResponse,
  options: CorsOptions,
): void {
  const origin = request.headers.origin;
  if (typeof origin !== "string") {
    return;
  }
  if (!options.allowedOrigins.includes(origin)) {
    logCorsDenied({ origin, route: request.url ?? null });
    return;
  }
  writeCorsHeaders(response, origin, request);
}

/** Structured server-side log details for unexpected HTTP route failures. */
export interface HttpRouteErrorLogDetails {
  /** Internal error details kept on the server side for request correlation. */
  readonly error: {
    readonly message: string;
    readonly name: string;
  };
  /** Public correlation id returned to the caller for this failure. */
  readonly requestId: string;
}

/** Structured logger used by the shared HTTP route error boundary. */
export interface HttpRouteErrorLogger {
  readonly error: (event: "http.route_error", details: HttpRouteErrorLogDetails) => void;
}

/** Optional dependencies for deterministic route error handling and logging. */
export interface HttpRouteErrorOptions {
  readonly logger?: HttpRouteErrorLogger;
  readonly requestIdFactory?: () => string;
  readonly resourceLimitRuntime?: ResourceLimitRuntime;
}

/** Broadcasts committed session events to all live HTTP/WebSocket subscribers. */
export function broadcastEvents(hub: SubscriptionHub, events: readonly SessionEvent[]): void {
  for (const event of events) {
    hub.broadcast(event);
  }
}

/** Converts thrown route errors into JSON HTTP responses. */
export function handleHttpRouteError(
  response: ServerResponse,
  error: unknown,
  options: HttpRouteErrorOptions = {},
): void {
  if (isResourceLimitExceeded(error)) {
    if (error.reason === resourceLimitReason.bodyTooLarge) {
      options.resourceLimitRuntime?.recordBodyTooLarge();
      sendJson(response, 413, {
        error: "Payload Too Large",
        maxBytes: error.max ?? null,
        reason: error.reason,
      });
      return;
    }
  }
  if (error instanceof ZodError) {
    sendJson(response, 400, { error: "Invalid request", issues: error.issues });
    return;
  }
  const sequenceRangeError = eventSequenceRangeErrorFromUnknown(error);
  if (sequenceRangeError) {
    const requestId = options.requestIdFactory?.() ?? createRouteErrorRequestId();
    (options.logger ?? defaultHttpRouteErrorLogger).error("http.route_error", {
      error: {
        message: sequenceRangeError.message,
        name: sequenceRangeError.name,
      },
      requestId,
    });
    sendJson(response, 500, {
      attemptedSeq: sequenceRangeError.attemptedSeq,
      cutoff: sequenceRangeError.cutoff,
      error: "Event sequence exceeds safe integer cutoff",
      requestId,
      sessionId: sequenceRangeError.sessionId,
    });
    return;
  }
  const sessionNotFoundError = sessionNotFoundErrorFromUnknown(error);
  if (sessionNotFoundError) {
    sendJson(response, 404, {
      error: "Session not found",
      operation: sessionNotFoundError.operation,
      reason: "session_not_found",
      sessionId: sessionNotFoundError.sessionId,
    });
    return;
  }
  const requestId = options.requestIdFactory?.() ?? createRouteErrorRequestId();
  const normalizedError = normalizeRouteError(error);
  (options.logger ?? defaultHttpRouteErrorLogger).error("http.route_error", {
    error: normalizedError,
    requestId,
  });
  sendJson(response, 500, {
    error: internalServerErrorMessage,
    requestId,
  });
}

/** Extracts the typed sequence range error from direct or service-wrapped failures. */
function eventSequenceRangeErrorFromUnknown(error: unknown): SessionEventSequenceRangeError | null {
  if (error instanceof SessionEventSequenceRangeError) {
    return error;
  }
  if (
    error instanceof SessionServicePersistenceError &&
    error.cause instanceof SessionEventSequenceRangeError
  ) {
    return error.cause;
  }
  return null;
}

/** Extracts typed missing-session failures from direct or service-wrapped errors. */
function sessionNotFoundErrorFromUnknown(error: unknown, depth = 0): SessionNotFoundError | null {
  if (depth > 5) {
    return null;
  }
  if (error instanceof SessionNotFoundError) {
    return error;
  }
  if (error instanceof SessionServicePersistenceError) {
    return sessionNotFoundErrorFromUnknown(error.cause, depth + 1);
  }
  if (typeof error === "object" && error !== null) {
    const nested = readNestedError(error, "originalError") ?? readNestedError(error, "cause");
    if (nested !== null) {
      return sessionNotFoundErrorFromUnknown(nested, depth + 1);
    }
  }
  return null;
}

/** Reads a nested unknown error field from wrapper error objects. */
function readNestedError(value: object, field: "cause" | "originalError"): unknown | null {
  if (!(field in value)) {
    return null;
  }
  return value[field as keyof typeof value];
}

/** Reads a JSON request body and validates it with the supplied schema. */
export function parseJsonBody<TValue>(
  request: IncomingMessage,
  schema: z.ZodType<TValue>,
  options: {
    readonly maxBytes: number;
    readonly routeName: string;
  },
): Effect.Effect<TValue, unknown> {
  return Effect.gen(function* () {
    const body = yield* Effect.tryPromise({
      catch: (error) => error,
      try: () => readJsonBody(request, options),
    });
    return yield* Effect.try({
      catch: (error) => error,
      try: () => schema.parse(body),
    });
  });
}

/** Serializes a participant control conflict as an HTTP 409 response. */
export function sendControlLeaseConflict(
  response: ServerResponse,
  leaseClaim: Extract<ControlLeaseClaim, { readonly status: "conflict" }>,
  requestedChannel: ControlChannel,
): void {
  sendJson(response, 409, controlLeaseConflictError(leaseClaim, requestedChannel));
}

/** Serializes a fenced Control Epoch rejection as an HTTP 409 response. */
export function sendControlEpochStale(response: ServerResponse, currentEpoch: number | null): void {
  sendJson(response, 409, {
    code: "CONTROL_EPOCH_STALE",
    currentEpoch,
    error: "Control epoch is missing, invalid, or stale",
  });
}

/** Builds the shared participant control-conflict payload. */
export function controlLeaseConflictError(
  leaseClaim: Extract<ControlLeaseClaim, { readonly status: "conflict" }>,
  requestedChannel: ControlChannel,
): Record<string, unknown> {
  return {
    activeControlChannel: leaseClaim.activeLease.controlChannel,
    error: "Participant already has an active control channel",
    instanceId: leaseClaim.activeLease.instanceId,
    leaseExpiresAt: leaseClaim.activeLease.leaseExpiresAt,
    participantId: leaseClaim.activeLease.participantId,
    requestedControlChannel: requestedChannel,
  };
}

/** Writes a JSON HTTP response. */
export function sendJson(
  response: ServerResponse,
  statusCode: number,
  body: Record<string, unknown>,
): void {
  response.writeHead(statusCode, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

/** Writes a redacted auth failure response. */
export function sendAuthError(response: ServerResponse, error: AuthError): void {
  sendJson(response, authErrorStatus(error), authErrorPayload(error));
}

/** Reads and parses a JSON request body, using an empty object for empty bodies. */
async function readJsonBody(
  request: IncomingMessage,
  options: {
    readonly maxBytes: number;
    readonly routeName: string;
  },
): Promise<unknown> {
  const chunks: Buffer[] = [];
  let byteLength = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    byteLength += buffer.byteLength;
    if (byteLength > options.maxBytes) {
      throw bodyTooLargeError({
        max: options.maxBytes,
        observed: byteLength,
        routeName: options.routeName,
      });
    }
    chunks.push(buffer);
  }
  if (chunks.length === 0) {
    return {};
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

/** Creates a non-durable public correlation id for one generic route failure. */
function createRouteErrorRequestId(): string {
  return `req_${randomUUID()}`;
}

/** Normalizes unknown thrown values for server logs without serializing raw data. */
function normalizeRouteError(error: unknown): HttpRouteErrorLogDetails["error"] {
  if (error instanceof Error) {
    return {
      message: error.message,
      name: error.name,
    };
  }
  return {
    message: "Non-Error thrown value",
    name: "UnknownError",
  };
}

/** Parses a comma-delimited exact-origin allowlist. */
function parseAllowedOrigins(value: string | undefined): readonly string[] {
  if (!value) {
    return [];
  }
  return value
    .split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0 && origin !== "*");
}

/** Writes stable CORS headers for an already allowlisted origin. */
function writeCorsHeaders(
  response: ServerResponse,
  origin: string,
  request: IncomingMessage,
): void {
  response.setHeader("access-control-allow-origin", origin);
  response.setHeader("access-control-allow-credentials", "true");
  response.setHeader("access-control-allow-methods", defaultAllowedCorsMethods);
  response.setHeader(
    "access-control-allow-headers",
    request.headers["access-control-request-headers"] ?? defaultAllowedCorsHeaders,
  );
  response.setHeader("vary", "Origin");
}

/** Emits one payload-free CORS denial boundary log. */
function logCorsDenied(details: { readonly origin: string | null; readonly route: string | null }) {
  process.stderr.write(`${JSON.stringify({ details, event: "http.cors_denied" })}\n`);
}

const defaultHttpRouteErrorLogger: HttpRouteErrorLogger = {
  error: (event, details) => {
    process.stderr.write(`${JSON.stringify({ details, event })}\n`);
  },
};
