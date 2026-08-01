import type { AuthGrantRecord, AuthGrantStore } from "./grant-stores.js";
import { AuthGrantTokenError, verifyAuthGrantToken } from "./grant-token.js";
import type { AuthContext, AuthSigningSecrets } from "./token.js";

export const authGrantAuthorityErrorCodes = [
  "auth_claim_invalid",
  "auth_grant_expired",
  "auth_grant_revoked",
  "auth_store_unavailable",
] as const;

/** Bounded public classification for durable authority denial. */
export type AuthGrantAuthorityErrorCode = (typeof authGrantAuthorityErrorCodes)[number];

/** Secret-safe durable authority failure without database or credential causes. */
export class AuthGrantAuthorityError extends Error {
  /** Stable denial reason safe for transport responses and logs. */
  readonly code: AuthGrantAuthorityErrorCode;

  constructor(code: AuthGrantAuthorityErrorCode) {
    super(code);
    this.name = "AuthGrantAuthorityError";
    this.code = code;
  }
}

/** PostgreSQL-authoritative grant verifier used at REST and command boundaries. */
export interface AuthGrantAuthority {
  /** Reauthorizes a ticket-derived parent grant directly from durable authority. */
  readonly authenticateGrantJti: (jti: string) => Promise<AuthContext>;
  /** Verifies a raw REST bearer and its matching durable row. */
  readonly authenticateRestBearer: (bearer: string) => Promise<AuthContext>;
  /** Returns bounded cache diagnostics without grant identifiers or bearer material. */
  readonly debugInfo: () => AuthGrantAuthorityDebugInfo;
}

/** Secret-safe diagnostics for the bounded negative grant cache. */
export interface AuthGrantAuthorityDebugInfo {
  /** Current denial entries retained for short-circuiting repeated inactive grants. */
  readonly negativeCacheEntries: number;
  /** Hard process-local cap for retained denial entries. */
  readonly negativeCacheMaximumEntries: number;
}

/** Dependencies for the database-authoritative grant verifier. */
export interface AuthGrantAuthorityOptions {
  /** Stable deployment issuer required by tgr2 claims. */
  readonly issuer: string;
  /** Injectable authorization clock. */
  readonly now?: () => Date;
  /** Optional bounded-cache cap override for deterministic tests and constrained runtimes. */
  readonly maximumNegativeCacheEntries?: number;
  /** Currently accepted signing secrets. Removing a key invalidates its grants. */
  readonly secrets: AuthSigningSecrets;
  /** Narrow PostgreSQL grant lookup interface. */
  readonly store: AuthGrantStore;
}

interface NegativeGrantCacheEntry {
  readonly code: "auth_grant_expired" | "auth_grant_revoked";
  readonly expiresAtMilliseconds: number;
}

const defaultMaximumNegativeGrantCacheEntries = 1_024;
const negativeCacheCodes = ["auth_grant_revoked", "auth_grant_expired"] as const;

/** Creates a fail-closed authority with no positive authorization cache. */
export function createAuthGrantAuthority(options: AuthGrantAuthorityOptions): AuthGrantAuthority {
  const now = options.now ?? (() => new Date());
  const maximumNegativeCacheEntries = readMaximumNegativeCacheEntries(options);
  const negativeCache = new Map<string, NegativeGrantCacheEntry>();

  const readGrant = async (jti: string): Promise<AuthGrantRecord | null> => {
    try {
      return await options.store.findByJti(jti);
    } catch {
      throw new AuthGrantAuthorityError("auth_store_unavailable");
    }
  };

  const readNegative = (jti: string, at: Date): NegativeGrantCacheEntry | null => {
    for (const code of negativeCacheCodes) {
      const key = negativeCacheKey(jti, code);
      const cached = negativeCache.get(key);
      if (!cached) continue;
      if (cached.expiresAtMilliseconds <= at.getTime()) {
        negativeCache.delete(key);
        continue;
      }
      return cached;
    }
    return null;
  };

  const denyInactive = (
    record: AuthGrantRecord,
    at: Date,
  ): "auth_grant_expired" | "auth_grant_revoked" | null => {
    const code =
      record.revokedAt !== null
        ? "auth_grant_revoked"
        : record.expiresAt.getTime() <= at.getTime()
          ? "auth_grant_expired"
          : null;
    if (code !== null && record.expiresAt.getTime() > at.getTime()) {
      const key = negativeCacheKey(record.jti, code);
      if (!negativeCache.has(key) && negativeCache.size >= maximumNegativeCacheEntries) {
        const oldestKey = negativeCache.keys().next().value;
        if (oldestKey !== undefined) negativeCache.delete(oldestKey);
      }
      negativeCache.set(key, {
        code,
        expiresAtMilliseconds: record.expiresAt.getTime(),
      });
    }
    return code;
  };

  return {
    authenticateGrantJti: async (jti) => {
      const at = now();
      const cached = readNegative(jti, at);
      if (cached) throw new AuthGrantAuthorityError(cached.code);
      const record = await readGrant(jti);
      if (!record || !recordIsAcceptedParent(record, jti, options)) {
        throw new AuthGrantAuthorityError("auth_claim_invalid");
      }
      const inactive = denyInactive(record, at);
      if (inactive) throw new AuthGrantAuthorityError(inactive);
      return contextFromGrant(record);
    },
    authenticateRestBearer: async (bearer) => {
      const at = now();
      const claims = verifyClaims(bearer, options, at);
      const cached = readNegative(claims.jti, at);
      if (cached) throw new AuthGrantAuthorityError(cached.code);
      const record = await readGrant(claims.jti);
      if (!record || !recordMatchesClaims(record, claims)) {
        throw new AuthGrantAuthorityError("auth_claim_invalid");
      }
      const inactive = denyInactive(record, at);
      if (inactive) throw new AuthGrantAuthorityError(inactive);
      return contextFromGrant(record);
    },
    debugInfo: () => {
      const at = now();
      for (const [key, cached] of negativeCache) {
        if (cached.expiresAtMilliseconds <= at.getTime()) negativeCache.delete(key);
      }
      return {
        negativeCacheEntries: negativeCache.size,
        negativeCacheMaximumEntries: maximumNegativeCacheEntries,
      };
    },
  };
}

function recordIsAcceptedParent(
  record: AuthGrantRecord,
  jti: string,
  options: AuthGrantAuthorityOptions,
): boolean {
  return (
    record.audience === "tether-rest" &&
    record.issuer === options.issuer &&
    record.jti === jti &&
    options.secrets[record.kid] !== undefined
  );
}

function contextFromGrant(record: AuthGrantRecord): AuthContext {
  return {
    expiresAt: record.expiresAt.toISOString(),
    grantJti: record.jti,
    grantSource: record.metadata.source,
    issuer: record.issuer,
    kid: record.kid,
    participantId: record.subject,
    role: record.role,
    sessionScope: record.sessionScope,
  };
}

function negativeCacheKey(jti: string, code: NegativeGrantCacheEntry["code"]): string {
  return JSON.stringify([jti, code]);
}

function readMaximumNegativeCacheEntries(options: AuthGrantAuthorityOptions): number {
  const maximum = options.maximumNegativeCacheEntries ?? defaultMaximumNegativeGrantCacheEntries;
  if (!Number.isSafeInteger(maximum) || maximum <= 0) {
    throw new Error("auth_negative_cache_limit_invalid");
  }
  return maximum;
}

function verifyClaims(
  bearer: string,
  options: AuthGrantAuthorityOptions,
  now: Date,
): ReturnType<typeof verifyAuthGrantToken> {
  try {
    return verifyAuthGrantToken(bearer, {
      audience: "tether-rest",
      issuer: options.issuer,
      now,
      secrets: options.secrets,
    });
  } catch (error) {
    const code =
      error instanceof Error && error.message === AuthGrantTokenError.Expired
        ? "auth_grant_expired"
        : "auth_claim_invalid";
    throw new AuthGrantAuthorityError(code);
  }
}

function recordMatchesClaims(
  record: AuthGrantRecord,
  claims: ReturnType<typeof verifyAuthGrantToken>,
): boolean {
  return (
    record.audience === claims.aud &&
    record.expiresAt.getTime() === claims.exp * 1_000 &&
    record.issuedAt.getTime() === claims.iat * 1_000 &&
    record.issuer === claims.iss &&
    record.jti === claims.jti &&
    record.kid === claims.kid &&
    record.role === claims.role &&
    record.sessionScope === claims.sessionScope &&
    record.subject === claims.sub
  );
}
