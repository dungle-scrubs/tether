import { createHmac, timingSafeEqual } from "node:crypto";

import { z } from "zod";

import {
  type AuthRole,
  type AuthSigningSecrets,
  authRoles,
  type VerifyAuthTokenOptions,
} from "./token.js";

export const authAudiences = ["tether-rest"] as const;

/** Audience accepted by a versioned authentication grant. */
export type AuthAudience = (typeof authAudiences)[number];

export const AuthGrantTokenError = {
  AudienceMismatch: "grant_token_audience",
  BadSignature: "grant_token_bad_signature",
  Expired: "grant_token_expired",
  InvalidLifetime: "grant_token_lifetime",
  IssuerMismatch: "grant_token_issuer",
  Malformed: "grant_token_malformed",
  UnknownKid: "grant_token_unknown_kid",
} as const;

/** Internal versioned-grant verification classification mapped externally by authorization. */
export type AuthGrantTokenError = (typeof AuthGrantTokenError)[keyof typeof AuthGrantTokenError];

/** Claims carried by the versioned, durable-grant token format. */
export interface AuthGrantTokenPayload {
  /** Intended Tether transport boundary. */
  readonly aud: AuthAudience;
  /** Unix timestamp in seconds after which the grant is invalid. */
  readonly exp: number;
  /** Unix timestamp in seconds when the grant was issued. */
  readonly iat: number;
  /** Configured authority that issued the grant. */
  readonly iss: string;
  /** Durable unique grant id. */
  readonly jti: string;
  /** Signing key id used to select the verification secret. */
  readonly kid: string;
  /** Authenticated role used by the authorization layer. */
  readonly role: AuthRole;
  /** Scoped durable session id, or `*` for service-wide scope. */
  readonly sessionScope: string;
  /** Authenticated durable subject. */
  readonly sub: string;
}

/** Input used to mint one versioned durable-grant token. */
export interface MintAuthGrantTokenInput {
  /** Intended transport boundary. Defaults to the REST bearer boundary. */
  readonly audience?: AuthAudience;
  /** Configured authority issuing the grant. */
  readonly issuer: string;
  /** Durable unique grant id. */
  readonly jti: string;
  /** Active signing key id. */
  readonly kid: string;
  /** Role authorized by the grant. */
  readonly role: AuthRole;
  /** Scoped durable session id, or `*` for service-wide scope. */
  readonly sessionScope: string;
  /** Durable authenticated subject. */
  readonly subject: string;
  /** Grant lifetime in seconds. Defaults to 24 hours and cannot exceed seven days. */
  readonly ttlSeconds?: number;
}

/** Clock overrides for deterministic grant issuance tests. */
export interface MintAuthGrantTokenOptions {
  /** Issuance clock. */
  readonly now?: Date;
}

/** Verification policy for one versioned durable-grant token. */
export interface VerifyAuthGrantTokenOptions extends VerifyAuthTokenOptions {
  /** Audience required at the current boundary. */
  readonly audience: AuthAudience;
  /** Issuer required by this deployment. */
  readonly issuer: string;
}

const authGrantTokenPayloadSchema = z
  .object({
    aud: z.enum(authAudiences),
    exp: z.number().int().positive(),
    iat: z.number().int().nonnegative(),
    iss: z.string().min(1).max(512),
    jti: z.string().min(1).max(128),
    kid: z.string().min(1).max(128),
    role: z.enum(authRoles),
    sessionScope: z.string().min(1).max(255),
    sub: z.string().min(1).max(255),
  })
  .strict();

const authGrantTokenVersion = "tgr2";
const canonicalBase64UrlPattern = /^[A-Za-z0-9_-]+$/u;
const defaultAuthGrantLifetimeSeconds = 24 * 60 * 60;
export const maximumAuthGrantLifetimeSeconds = 7 * 24 * 60 * 60;

/** Mints a versioned durable-grant token without persisting or logging its bearer value. */
export function mintAuthGrantToken(
  input: MintAuthGrantTokenInput,
  secrets: AuthSigningSecrets,
  options: MintAuthGrantTokenOptions = {},
): string {
  const ttlSeconds = input.ttlSeconds ?? defaultAuthGrantLifetimeSeconds;
  if (
    !Number.isSafeInteger(ttlSeconds) ||
    ttlSeconds <= 0 ||
    ttlSeconds > maximumAuthGrantLifetimeSeconds
  ) {
    throw new Error(AuthGrantTokenError.InvalidLifetime);
  }
  const issuedAt = Math.floor((options.now ?? new Date()).getTime() / 1_000);
  const parsed = authGrantTokenPayloadSchema.safeParse({
    aud: input.audience ?? "tether-rest",
    exp: issuedAt + ttlSeconds,
    iat: issuedAt,
    iss: input.issuer,
    jti: input.jti,
    kid: input.kid,
    role: input.role,
    sessionScope: input.sessionScope,
    sub: input.subject,
  });
  if (!parsed.success) {
    throw new Error(AuthGrantTokenError.Malformed);
  }
  const secret = secrets[parsed.data.kid];
  if (!secret) {
    throw new Error(AuthGrantTokenError.UnknownKid);
  }
  const encodedPayload = encodeBase64Url(JSON.stringify(parsed.data));
  const signedValue = `${authGrantTokenVersion}.${encodedPayload}`;
  return `${signedValue}.${signEncodedPayload(signedValue, secret)}`;
}

/** Verifies the signature, claims, issuer, audience, and lifetime of a tgr2 grant. */
export function verifyAuthGrantToken(
  token: string,
  options: VerifyAuthGrantTokenOptions,
): AuthGrantTokenPayload {
  const [version, encodedPayload, encodedSignature, extra] = token.split(".");
  if (
    version !== authGrantTokenVersion ||
    !encodedPayload ||
    !encodedSignature ||
    extra !== undefined ||
    !isCanonicalBase64Url(encodedPayload) ||
    !isCanonicalBase64Url(encodedSignature)
  ) {
    throw new Error(AuthGrantTokenError.Malformed);
  }
  const payload = decodeAuthGrantPayload(encodedPayload);
  const secret = options.secrets[payload.kid];
  if (!secret) {
    throw new Error(AuthGrantTokenError.UnknownKid);
  }
  const expectedSignature = signEncodedPayload(`${version}.${encodedPayload}`, secret);
  if (!constantTimeEqual(encodedSignature, expectedSignature)) {
    throw new Error(AuthGrantTokenError.BadSignature);
  }
  if (payload.iss !== options.issuer) {
    throw new Error(AuthGrantTokenError.IssuerMismatch);
  }
  if (payload.aud !== options.audience) {
    throw new Error(AuthGrantTokenError.AudienceMismatch);
  }
  const nowSeconds = Math.floor((options.now ?? new Date()).getTime() / 1_000);
  if (payload.exp <= nowSeconds) {
    throw new Error(AuthGrantTokenError.Expired);
  }
  if (
    payload.iat > nowSeconds ||
    payload.exp <= payload.iat ||
    payload.exp - payload.iat > maximumAuthGrantLifetimeSeconds
  ) {
    throw new Error(AuthGrantTokenError.InvalidLifetime);
  }
  return payload;
}

/** Decodes tgr2 claims while collapsing parse details into a secret-safe reason code. */
function decodeAuthGrantPayload(encodedPayload: string): AuthGrantTokenPayload {
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8")) as unknown;
  } catch {
    throw new Error(AuthGrantTokenError.Malformed);
  }
  const parsed = authGrantTokenPayloadSchema.safeParse(decoded);
  if (!parsed.success) {
    throw new Error(AuthGrantTokenError.Malformed);
  }
  if (encodeBase64Url(JSON.stringify(parsed.data)) !== encodedPayload) {
    throw new Error(AuthGrantTokenError.Malformed);
  }
  return parsed.data;
}

/** Encodes a UTF-8 string as unpadded base64url. */
function encodeBase64Url(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

/** Computes an HMAC-SHA256 signature for an encoded version and payload. */
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

/** Rejects permissive base64 decodings and requires exact unpadded base64url. */
function isCanonicalBase64Url(value: string): boolean {
  return (
    canonicalBase64UrlPattern.test(value) &&
    Buffer.from(value, "base64url").toString("base64url") === value
  );
}
