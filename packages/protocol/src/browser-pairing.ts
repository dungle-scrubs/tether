import { z } from "zod";

import {
  operatorGrantJtiSchema,
  operatorGrantScopeSchema,
  operatorSubjectSchema,
} from "./operator-authority.js";

const base64UrlSecretSchema = z.string().regex(/^[A-Za-z0-9_-]+$/u);

/** Maximum accepted length of one exact HTTP origin string. */
export const exactHttpOriginMaxLength = 512;

/**
 * Returns whether a value is an exact `http:` or `https:` origin: the complete
 * string equals the parsed origin, so it carries no credentials, path, query,
 * or fragment, and opaque origins serialized as `null` are rejected.
 */
export function isExactHttpOrigin(value: string): boolean {
  if (value.length > exactHttpOriginMaxLength) {
    return false;
  }
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    return false;
  }
  return (
    (parsed.protocol === "http:" || parsed.protocol === "https:") &&
    parsed.origin === value &&
    parsed.origin !== "null" &&
    parsed.username === "" &&
    parsed.password === "" &&
    parsed.pathname === "/" &&
    parsed.search === "" &&
    parsed.hash === ""
  );
}

/** Runtime validator for one exact browser origin allowed to reach Tether. */
export const exactHttpOriginSchema = z
  .string()
  .max(exactHttpOriginMaxLength)
  .refine(isExactHttpOrigin, { message: "Expected an exact HTTP origin" });

/** Browser-generated nonce carrying at least 128 bits in canonical base64url form. */
export const browserPairingNonceSchema = base64UrlSecretSchema.min(22).max(86);

/** One-time exchange credential produced from 256 random bits. */
export const browserPairingExchangeSecretSchema = base64UrlSecretSchema.length(43);

/** Public request for an explicitly confirmed browser operator grant. */
export const createBrowserPairingRequestSchema = z
  .object({
    operatorSubject: operatorSubjectSchema,
    publicNonce: browserPairingNonceSchema,
    requestedScope: operatorGrantScopeSchema,
  })
  .strict();

/** Single-use pairing exchange bound to the browser's original public nonce. */
export const exchangeBrowserPairingRequestSchema = z
  .object({
    exchangeSecret: browserPairingExchangeSecretSchema,
    publicNonce: browserPairingNonceSchema,
  })
  .strict();

/** Grant-bound anti-CSRF token returned outside the HttpOnly session cookie. */
export const browserCsrfTokenSchema = base64UrlSecretSchema.length(43);

/** Single-use credential minted for one browser WebSocket upgrade. */
export const browserOneTimeTicketSchema = base64UrlSecretSchema.length(43);

/** Canonical request header carrying the grant-bound browser CSRF token. */
export const browserCsrfHeaderName = "x-tether-csrf";

/** Secret-free pairing request projection shared by browser and loopback CLI surfaces. */
export const publicBrowserPairingRequestSchema = z
  .object({
    confirmedAt: z.string().datetime({ offset: true }).nullable(),
    confirmedBySubject: operatorSubjectSchema.nullable(),
    createdAt: z.string().datetime({ offset: true }),
    exchangedAt: z.string().datetime({ offset: true }).nullable(),
    expiresAt: z.string().datetime({ offset: true }),
    failedAttempts: z.number().int().min(0).max(5),
    invalidatedAt: z.string().datetime({ offset: true }).nullable(),
    operatorSubject: operatorSubjectSchema,
    origin: z.string().min(1).max(512),
    publicNonce: browserPairingNonceSchema,
    requestId: z.string().regex(/^pair_[A-Za-z0-9_-]{1,120}$/u),
    requestedScope: operatorGrantScopeSchema,
    verificationPhrase: z.string().min(1).max(128),
  })
  .strict();

/** One-time response emitted only when a pairing request is created. */
export const browserPairingCreateResponseSchema = z
  .object({
    exchangeSecret: browserPairingExchangeSecretSchema,
    request: publicBrowserPairingRequestSchema,
    status: z.literal("created"),
  })
  .strict();

/** Credential-safe HTTP exchange response after the bearer has entered its cookie. */
export const browserPairingExchangeResponseSchema = z
  .object({
    csrfToken: browserCsrfTokenSchema,
    expiresAt: z.string().datetime({ offset: true }),
    grantJti: operatorGrantJtiSchema,
    scope: operatorGrantScopeSchema,
    status: z.literal("exchanged"),
  })
  .strict();

/** Parsed public request for a browser pairing attempt. */
export type CreateBrowserPairingRequest = z.infer<typeof createBrowserPairingRequestSchema>;

/** Parsed one-time browser pairing exchange. */
export type ExchangeBrowserPairingRequest = z.infer<typeof exchangeBrowserPairingRequestSchema>;

/** Parsed secret-free browser pairing request. */
export type PublicBrowserPairingRequest = z.infer<typeof publicBrowserPairingRequestSchema>;

/** Parsed browser pairing creation response. */
export type BrowserPairingCreateResponse = z.infer<typeof browserPairingCreateResponseSchema>;

/** Parsed browser pairing exchange response. */
export type BrowserPairingExchangeResponse = z.infer<typeof browserPairingExchangeResponseSchema>;
