import { createHmac, randomBytes, randomUUID } from "node:crypto";

import {
  browserCsrfTokenSchema,
  browserPairingExchangeSecretSchema,
  browserPairingNonceSchema,
  type BrowserPairingCreateResponse,
  type BrowserPairingExchangeResponse,
  type CreateBrowserPairingRequest,
  exchangeBrowserPairingRequestSchema,
  operatorGrantScopeSchema,
  type PublicBrowserPairingRequest,
} from "@dungle-scrubs/tether-protocol";

import type { AuthSigningSecrets } from "./token.js";
import type {
  BrowserPairingRequestRecord,
  BrowserPairingStore,
  ConfirmBrowserPairingStoreResult,
  ExchangeBrowserPairingStoreResult,
} from "./browser-pairing-stores.js";
import { mintAuthGrantToken, verifyAuthGrantToken } from "./grant-token.js";
import {
  hashOpaqueCredential,
  randomOpaqueCredential,
  wholeAuthSecond,
} from "./opaque-credential.js";

/** Host-only cookie name used for browser operator authority. */
export const browserSessionCookieName = "__Host-Http-tether-operator";

/** Default bounded pairing request lifetime. */
export const browserPairingLifetimeMs = 10 * 60 * 1_000;

/** Default per-source creation window. */
export const browserPairingCreationWindowMs = 10 * 60 * 1_000;

/** One-time response to a successful pairing request creation. */
export type CreatedBrowserPairingRequest = BrowserPairingCreateResponse;

/** Successful exchange material consumed only by the HTTP cookie boundary. */
export type ExchangedBrowserPairingRequest = BrowserPairingExchangeResponse & {
  readonly bearer: string;
};

/** Stable public lifecycle errors without raw credentials. */
export class BrowserPairingError extends Error {
  readonly name = "BrowserPairingError";

  constructor(
    readonly code:
      | "pairing_already_exchanged"
      | "pairing_expired"
      | "pairing_generation_failed"
      | "pairing_invalidated"
      | "pairing_nonce_mismatch"
      | "pairing_not_confirmed"
      | "pairing_not_found"
      | "pairing_origin_mismatch"
      | "pairing_rate_limited"
      | "pairing_scope_invalid"
      | "pairing_secret_invalid",
  ) {
    super(code);
  }
}

/** Browser pairing lifecycle dependencies. */
export interface BrowserPairingLifecycleOptions {
  readonly activeKid: string;
  readonly issuer: string;
  readonly logger?: {
    readonly info: (
      event: "browser.pairing.lifecycle",
      details: { readonly requestId: string; readonly status: string },
    ) => void;
    readonly warn: (
      event: "browser.pairing.lifecycle",
      details: { readonly requestId: string; readonly status: string },
    ) => void;
  };
  readonly now?: () => Date;
  readonly randomCsrfToken?: () => string;
  readonly randomExchangeSecret?: () => string;
  readonly randomPhrase?: () => string;
  readonly secrets: AuthSigningSecrets;
  readonly store: BrowserPairingStore;
}

/** Dependencies needed to create a request before grant signing authority is used. */
export interface BrowserPairingRequestCreationOptions {
  readonly logger?: BrowserPairingLifecycleOptions["logger"];
  readonly now?: () => Date;
  readonly randomExchangeSecret?: () => string;
  readonly randomPhrase?: () => string;
  readonly sourceHashSecret?: string | undefined;
  readonly store: BrowserPairingStore;
}

/** Explicit browser pairing lifecycle used by HTTP and loopback CLI surfaces. */
export interface BrowserPairingLifecycle {
  readonly confirm: (
    requestId: string,
    actorSubject: string,
  ) => Promise<ConfirmBrowserPairingStoreResult>;
  readonly create: (
    input: CreateBrowserPairingRequest & {
      readonly origin: string;
      readonly sourceAddress: string | null;
    },
  ) => Promise<CreatedBrowserPairingRequest>;
  readonly exchange: (
    requestId: string,
    input: unknown,
    transport: { readonly origin: string; readonly sourceAddress: string | null },
  ) => Promise<ExchangedBrowserPairingRequest>;
  readonly inspect: (requestId: string) => Promise<PublicBrowserPairingRequest | null>;
}

/** Creates one short-lived pairing request without requiring grant-signing authority. */
export async function createBrowserPairingRequest(
  options: BrowserPairingRequestCreationOptions,
  input: CreateBrowserPairingRequest & {
    readonly origin: string;
    readonly sourceAddress: string | null;
  },
): Promise<CreatedBrowserPairingRequest> {
  const scope = operatorGrantScopeSchema.safeParse(input.requestedScope);
  const nonce = browserPairingNonceSchema.safeParse(input.publicNonce);
  if (!scope.success || !nonce.success || scope.data.sessionIds.length !== 1) {
    throw new BrowserPairingError("pairing_scope_invalid");
  }
  const exchangeSecret = (options.randomExchangeSecret ?? randomOpaqueCredential)();
  if (!browserPairingExchangeSecretSchema.safeParse(exchangeSecret).success) {
    throw new BrowserPairingError("pairing_generation_failed");
  }
  const createdAt = (options.now ?? (() => new Date()))();
  const request: BrowserPairingRequestRecord = {
    confirmedAt: null,
    confirmedBySubject: null,
    createdAt,
    exchangeSecretHash: hashBrowserCredential(exchangeSecret),
    exchangedAt: null,
    expiresAt: new Date(createdAt.getTime() + browserPairingLifetimeMs),
    failedAttempts: 0,
    invalidatedAt: null,
    operatorSubject: input.operatorSubject,
    origin: input.origin,
    publicNonce: nonce.data,
    requestId: `pair_${randomUUID()}`,
    requestedScope: scope.data,
    sourceAddressHash:
      input.sourceAddress === null
        ? null
        : hashBrowserSource(input.sourceAddress, options.sourceHashSecret),
    verificationPhrase: (options.randomPhrase ?? randomVerificationPhrase)(),
  };
  const result = await options.store.create({
    request,
    sourceWindowStartedAt: new Date(createdAt.getTime() - browserPairingCreationWindowMs),
  });
  if (result.status === "rate_limited") {
    options.logger?.warn("browser.pairing.lifecycle", {
      requestId: request.requestId,
      status: result.status,
    });
    throw new BrowserPairingError("pairing_rate_limited");
  }
  options.logger?.info("browser.pairing.lifecycle", {
    requestId: request.requestId,
    status: result.status,
  });
  return { exchangeSecret, request: toPublicPairingRequest(request), status: "created" };
}

/** Creates one fail-closed pairing lifecycle over atomic persistence. */
export function createBrowserPairingLifecycle(
  options: BrowserPairingLifecycleOptions,
): BrowserPairingLifecycle {
  const now = options.now ?? (() => new Date());
  const randomCsrf = options.randomCsrfToken ?? randomOpaqueCredential;
  return {
    confirm: async (requestId, actorSubject) => {
      const result = await options.store.confirm({ actorSubject, confirmedAt: now(), requestId });
      const write = result.status === "confirmed" ? options.logger?.info : options.logger?.warn;
      write?.("browser.pairing.lifecycle", { requestId, status: result.status });
      return result;
    },
    create: (input) =>
      createBrowserPairingRequest(
        {
          ...(options.logger === undefined ? {} : { logger: options.logger }),
          now,
          ...(options.randomExchangeSecret === undefined
            ? {}
            : { randomExchangeSecret: options.randomExchangeSecret }),
          ...(options.randomPhrase === undefined ? {} : { randomPhrase: options.randomPhrase }),
          sourceHashSecret: options.secrets[options.activeKid],
          store: options.store,
        },
        input,
      ),
    exchange: async (requestId, rawInput, transport) => {
      const input = exchangeBrowserPairingRequestSchema.safeParse(rawInput);
      if (!input.success) throw new BrowserPairingError("pairing_secret_invalid");
      const attemptedAt = now();
      const occurredAt = wholeAuthSecond(attemptedAt);
      const grantJti = `grant_${randomUUID()}`;
      const csrfToken = randomCsrf();
      if (!browserCsrfTokenSchema.safeParse(csrfToken).success) {
        throw new BrowserPairingError("pairing_generation_failed");
      }
      const requestAuditId = `req_${randomUUID()}`;
      let exchanged: ExchangedBrowserPairingRequest | null = null;
      const result = await options.store.exchange({
        attemptOrigin: transport.origin,
        attemptSourceAddressHash:
          transport.sourceAddress === null
            ? null
            : hashBrowserSource(transport.sourceAddress, options.secrets[options.activeKid]),
        attemptedAt,
        buildGrant: (request) => {
          const scope = operatorGrantScopeSchema.parse(request.requestedScope);
          const sessionScope = scope.sessionIds.length === 1 ? scope.sessionIds[0] : undefined;
          if (sessionScope === undefined || request.confirmedBySubject === null) {
            throw new BrowserPairingError("pairing_scope_invalid");
          }
          const bearer = mintAuthGrantToken(
            {
              issuer: options.issuer,
              jti: grantJti,
              kid: options.activeKid,
              role: "observer",
              sessionScope,
              subject: request.operatorSubject,
            },
            options.secrets,
            { now: occurredAt },
          );
          const claims = verifyAuthGrantToken(bearer, {
            audience: "tether-rest",
            issuer: options.issuer,
            now: occurredAt,
            secrets: options.secrets,
          });
          exchanged = {
            bearer,
            csrfToken,
            expiresAt: new Date(claims.exp * 1_000).toISOString(),
            grantJti,
            scope,
            status: "exchanged",
          };
          return {
            audit: {
              action: "grant.created",
              actorSubject: request.confirmedBySubject,
              auditId: `audit_${randomUUID()}`,
              metadata: { requestId: requestAuditId },
              occurredAt,
              reasonCode: "operator-request",
            },
            grant: {
              audience: claims.aud,
              expiresAt: new Date(claims.exp * 1_000),
              issuedAt: new Date(claims.iat * 1_000),
              issuer: claims.iss,
              jti: claims.jti,
              kid: claims.kid,
              metadata: { requestId: requestAuditId, source: "browser" },
              revokedAt: null,
              role: claims.role,
              sessionScope: claims.sessionScope,
              subject: claims.sub,
            },
            scope,
            session: {
              createdAt: attemptedAt,
              csrfTokenHash: hashBrowserCredential(csrfToken),
              grantJti,
              origin: request.origin,
            },
          };
        },
        exchangeSecretHash: hashBrowserCredential(input.data.exchangeSecret),
        failureId: `pairfail_${randomUUID()}`,
        publicNonce: input.data.publicNonce,
        requestId,
      });
      const write = result.status === "exchanged" ? options.logger?.info : options.logger?.warn;
      write?.("browser.pairing.lifecycle", { requestId, status: result.status });
      assertExchangeSucceeded(result);
      if (exchanged === null) throw new BrowserPairingError("pairing_generation_failed");
      return exchanged;
    },
    inspect: async (requestId) => {
      const request = await options.store.inspect(requestId);
      return request === null ? null : toPublicPairingRequest(request);
    },
  };
}

/** Hashes credentials and source addresses before durable persistence. */
export function hashBrowserCredential(value: string): string {
  return hashOpaqueCredential(value);
}

/** Produces a non-enumerable source pseudonym for durable abuse accounting. */
function hashBrowserSource(value: string, secret: string | undefined): string {
  if (secret === undefined) throw new BrowserPairingError("pairing_generation_failed");
  return createHmac("sha256", secret)
    .update("tether-browser-pairing-source\0")
    .update(value)
    .digest("hex");
}

/** Converts store exchange outcomes into stable lifecycle errors. */
function assertExchangeSucceeded(result: ExchangeBrowserPairingStoreResult): void {
  if (result.status === "exchanged") return;
  const codeByStatus = {
    already_exchanged: "pairing_already_exchanged",
    expired: "pairing_expired",
    invalidated: "pairing_invalidated",
    nonce_mismatch: "pairing_nonce_mismatch",
    not_confirmed: "pairing_not_confirmed",
    not_found: "pairing_not_found",
    origin_mismatch: "pairing_origin_mismatch",
    secret_invalid: "pairing_secret_invalid",
    source_rate_limited: "pairing_rate_limited",
  } as const;
  throw new BrowserPairingError(codeByStatus[result.status]);
}

/** Removes credential hashes and serializes timestamps for public inspection. */
export function toPublicPairingRequest(
  request: BrowserPairingRequestRecord,
): PublicBrowserPairingRequest {
  const {
    exchangeSecretHash: _exchangeSecretHash,
    sourceAddressHash: _sourceAddressHash,
    ...publicRequest
  } = request;
  return {
    ...publicRequest,
    confirmedAt: request.confirmedAt?.toISOString() ?? null,
    createdAt: request.createdAt.toISOString(),
    exchangedAt: request.exchangedAt?.toISOString() ?? null,
    expiresAt: request.expiresAt.toISOString(),
    invalidatedAt: request.invalidatedAt?.toISOString() ?? null,
  };
}

const verificationWords = ["amber", "cedar", "cobalt", "harbor", "linen", "orbit"] as const;

/** Produces a short nonsecret phrase used only for human request comparison. */
function randomVerificationPhrase(): string {
  const bytes = randomBytes(3);
  return [...bytes].map((value) => verificationWords[value % verificationWords.length]).join(" ");
}
