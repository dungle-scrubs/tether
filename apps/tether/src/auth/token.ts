import { createHmac, timingSafeEqual } from "node:crypto";

import { z } from "zod";

import type { AuthGrantSource } from "./grant-stores.js";

export const authRoles = ["observer", "participant", "admin"] as const;

export type AuthRole = (typeof authRoles)[number];

export const AuthError = {
  ClaimInvalid: "auth_claim_invalid",
  GrantExpired: "auth_grant_expired",
  GrantRevoked: "auth_grant_revoked",
  LegacyTokenRejected: "auth_legacy_token_rejected",
  StoreUnavailable: "auth_store_unavailable",
  TicketConsumed: "auth_ticket_consumed",
  TicketExpired: "auth_ticket_expired",
  BadSignature: "bad_sig",
  Expired: "expired",
  Malformed: "malformed",
  Missing: "missing",
  OriginDenied: "auth_origin_denied",
  RoleDenied: "role",
  ScopeDenied: "scope",
  UnknownKid: "unknown_kid",
} as const;

export type AuthError = (typeof AuthError)[keyof typeof AuthError];

export interface AuthContext {
  /** Durable grant classification used to isolate browser authority, or null for legacy tokens. */
  readonly grantSource: AuthGrantSource | null;
  /** Token expiration time as an ISO string for diagnostics, never authorization source data. */
  readonly expiresAt: string;
  /** Durable grant id, or null for a compatibility legacy token. */
  readonly grantJti: string | null;
  /** Durable grant issuer, or null for a compatibility legacy token. */
  readonly issuer: string | null;
  /** Signing key id that verified this token. */
  readonly kid: string;
  /** Durable participant identity authenticated by the token. */
  readonly participantId: string;
  /** Authenticated role used for authorization checks. */
  readonly role: AuthRole;
  /** Session id this token can access, or `*` for service-wide scope. */
  readonly sessionScope: string;
}

export interface AuthTokenPayload {
  /** Unix timestamp in seconds after which the token is rejected at connect/request time. */
  readonly exp: number;
  /** Signing key id used to select the verification secret. */
  readonly kid: string;
  /** Authenticated durable participant identity. */
  readonly participantId: string;
  /** Authenticated role used by the authorization layer. */
  readonly role: AuthRole;
  /** Scoped durable session id, or `*` for service-wide scope. */
  readonly sessionId: string;
}

export type AuthSigningSecrets = Readonly<Record<string, string>>;

export interface VerifyAuthTokenOptions {
  /** Verification clock, injected by tests. */
  readonly now?: Date;
  /** Accepted signing secrets keyed by `kid`. */
  readonly secrets: AuthSigningSecrets;
}

const authTokenPayloadSchema = z.object({
  exp: z.number().int().positive(),
  kid: z.string().min(1),
  participantId: z.string().min(1),
  role: z.enum(authRoles),
  sessionId: z.string().min(1),
});

/** Creates a signed stateless Tether auth token from a validated payload. */
export function mintAuthToken(payload: AuthTokenPayload, secrets: AuthSigningSecrets): string {
  const parsed = authTokenPayloadSchema.parse(payload);
  const secret = secrets[parsed.kid];
  if (!secret) {
    throw new Error(`No signing secret configured for kid: ${parsed.kid}`);
  }
  const encodedPayload = encodeBase64Url(JSON.stringify(parsed));
  return `${encodedPayload}.${signEncodedPayload(encodedPayload, secret)}`;
}

/** Verifies the legacy two-segment stateless token format. */
export function verifyLegacyAuthToken(
  token: string,
  options: VerifyAuthTokenOptions,
): AuthTokenPayload {
  const [encodedPayload, encodedSignature, extra] = token.split(".");
  if (!encodedPayload || !encodedSignature || extra !== undefined) {
    throw new Error(AuthError.Malformed);
  }
  const payload = decodePayload(encodedPayload);
  const secret = options.secrets[payload.kid];
  if (!secret) {
    throw new Error(AuthError.UnknownKid);
  }
  const expectedSignature = signEncodedPayload(encodedPayload, secret);
  if (!constantTimeEqual(encodedSignature, expectedSignature)) {
    throw new Error(AuthError.BadSignature);
  }
  const nowSeconds = Math.floor((options.now ?? new Date()).getTime() / 1_000);
  if (payload.exp <= nowSeconds) {
    throw new Error(AuthError.Expired);
  }
  return payload;
}

/** Converts a verified token payload into the context trusted by downstream routes. */
export function createAuthContext(payload: AuthTokenPayload): AuthContext {
  return {
    expiresAt: new Date(payload.exp * 1_000).toISOString(),
    grantJti: null,
    grantSource: null,
    issuer: null,
    kid: payload.kid,
    participantId: payload.participantId,
    role: payload.role,
    sessionScope: payload.sessionId,
  };
}

/** Decodes and validates the base64url JSON payload segment. */
function decodePayload(encodedPayload: string): AuthTokenPayload {
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8")) as unknown;
  } catch {
    throw new Error(AuthError.Malformed);
  }
  const parsed = authTokenPayloadSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new Error(AuthError.Malformed);
  }
  return parsed.data;
}

/** Encodes a UTF-8 string as unpadded base64url. */
function encodeBase64Url(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

/** Computes the token signature for an already-encoded payload segment. */
function signEncodedPayload(encodedPayload: string, secret: string): string {
  return createHmac("sha256", secret).update(encodedPayload).digest("base64url");
}

/** Compares two base64url signatures without leaking prefix-match timing. */
function constantTimeEqual(actual: string, expected: string): boolean {
  const actualBuffer = Buffer.from(actual, "base64url");
  const expectedBuffer = Buffer.from(expected, "base64url");
  if (actualBuffer.length !== expectedBuffer.length) {
    return false;
  }
  return timingSafeEqual(actualBuffer, expectedBuffer);
}
