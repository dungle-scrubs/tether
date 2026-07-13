import type { z } from "zod";

import { resolveServiceAuthToken } from "./auth-token.js";

/** Fetch-compatible function used by client bridges when talking to Tether. */
export type ClientBridgeFetch = (input: URL, init: RequestInit) => Promise<Response>;

/** Error codes returned by client bridge REST helpers. */
export type ClientBridgeRequestErrorCode = "HTTP_ERROR" | "INVALID_RESPONSE" | "NETWORK_ERROR";

/** Typed transport or validation failure from the shared client bridge boundary. */
export class ClientBridgeRequestError extends Error {
  readonly code: ClientBridgeRequestErrorCode;
  readonly details: Record<string, unknown>;

  /** Captures a failed Tether client bridge request with machine-readable context. */
  constructor(input: {
    readonly cause?: unknown;
    readonly code: ClientBridgeRequestErrorCode;
    readonly details?: Record<string, unknown>;
    readonly message: string;
  }) {
    super(input.message, input.cause === undefined ? undefined : { cause: input.cause });
    this.code = input.code;
    this.details = input.details ?? {};
    this.name = "ClientBridgeRequestError";
  }
}

/** Shared JSON request shape used by client bridge REST helpers. */
export interface ClientBridgeJsonRequest<TSchema extends z.ZodType> {
  /** JSON body, or null for requests without a body. */
  readonly body: Record<string, unknown> | null;
  /** HTTP method used by this request. */
  readonly method: "GET" | "POST";
  /** URL path relative to the Tether service URL. */
  readonly path: string;
  /** Zod schema used to validate the JSON response. */
  readonly schema: TSchema;
}

/** Shared JSON request shape after binding a service URL and fetch function. */
export interface BoundClientBridgeJsonRequest<TSchema extends z.ZodType>
  extends ClientBridgeJsonRequest<TSchema> {
  /** Bearer token sent to Tether. */
  readonly authToken?: string | null;
  /** Fetch implementation used for this request. */
  readonly fetch: ClientBridgeFetch;
  /** Tether service URL. */
  readonly serviceUrl: string;
}

/** Runtime diagnostics for a bound client bridge transport. */
export interface ClientBridgeTransportDebugInfo {
  /** Number of HTTP requests issued through this transport. */
  readonly requestCount: number;
}

/** One service-bound client bridge transport instance. */
export interface ClientBridgeTransport {
  /** Returns inspectable transport state for debug surfaces. */
  readonly debugInfo: () => ClientBridgeTransportDebugInfo;
  /** Performs one JSON request against Tether and validates the response body. */
  readonly requestJson: <TSchema extends z.ZodType>(
    input: ClientBridgeJsonRequest<TSchema>,
  ) => Promise<z.infer<TSchema>>;
}

/** Creates a service-bound transport with shared request accounting. */
export function createClientBridgeTransport(input: {
  readonly authToken?: string | null;
  readonly fetch?: ClientBridgeFetch;
  readonly serviceUrl: string;
}): ClientBridgeTransport {
  const fetch = input.fetch ?? defaultClientBridgeFetch;
  let requestCount = 0;
  return {
    debugInfo: () => ({ requestCount }),
    requestJson: async <TSchema extends z.ZodType>(
      request: ClientBridgeJsonRequest<TSchema>,
    ): Promise<z.infer<TSchema>> => {
      requestCount += 1;
      return requestClientBridgeJson({
        ...request,
        ...(input.authToken === undefined ? {} : { authToken: input.authToken }),
        fetch,
        serviceUrl: input.serviceUrl,
      });
    },
  };
}

/** Performs one JSON request against Tether and validates the response body. */
export async function requestClientBridgeJson<TSchema extends z.ZodType>(
  input: BoundClientBridgeJsonRequest<TSchema>,
): Promise<z.infer<TSchema>> {
  const url = new URL(input.path, input.serviceUrl);
  const authToken = resolveServiceAuthToken(input.authToken);
  let response: Response;
  try {
    response = await input.fetch(url, {
      headers: {
        "content-type": "application/json",
        ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
      },
      method: input.method,
      ...(input.body === null ? {} : { body: JSON.stringify(input.body) }),
    });
  } catch (error) {
    throw new ClientBridgeRequestError({
      cause: error,
      code: "NETWORK_ERROR",
      details: { method: input.method, path: input.path },
      message: "Failed to reach Tether service",
    });
  }
  const payload = await readResponseJson(response);
  if (!response.ok) {
    throw new ClientBridgeRequestError({
      code: "HTTP_ERROR",
      details: {
        method: input.method,
        path: input.path,
        payload,
        status: response.status,
      },
      message: `Tether request failed with HTTP ${response.status}`,
    });
  }
  const parsed = input.schema.safeParse(payload);
  if (!parsed.success) {
    throw new ClientBridgeRequestError({
      cause: parsed.error,
      code: "INVALID_RESPONSE",
      details: { method: input.method, path: input.path },
      message: "Tether returned an unexpected response shape",
    });
  }
  return parsed.data;
}

/** Fetch implementation used when callers do not inject one. */
export function defaultClientBridgeFetch(input: URL, init: RequestInit): Promise<Response> {
  return fetch(input, init);
}

/** Reads a JSON response body while preserving non-JSON failure bodies. */
async function readResponseJson(response: Response): Promise<unknown> {
  const contentType = response.headers.get("content-type");
  if (!contentType?.includes("application/json")) {
    return null;
  }
  return response.json() as Promise<unknown>;
}
