import {
  type BrowserOperatorApprovalResponse,
  browserOperatorApprovalResponseSchema,
  type BrowserOperatorCommandResponse,
  browserOperatorCommandResponseSchema,
  type BrowserOperatorSession,
  browserOperatorSessionSchema,
  type BrowserPairingCreateResponse,
  browserPairingCreateResponseSchema,
  type BrowserPairingExchangeResponse,
  browserPairingExchangeResponseSchema,
  type BrowserSessionSnapshot,
  browserSessionSnapshotSchema,
  type BrowserWebSocketTicketResponse,
  browserWebSocketTicketResponseSchema,
  browserCsrfHeaderName,
  type CreateBrowserPairingRequest,
  createBrowserPairingRequestSchema,
  type ExchangeBrowserPairingRequest,
  exchangeBrowserPairingRequestSchema,
  type OperatorCommandRequest,
  operatorCommandRequestSchema,
  type OperatorTaskApprovalRequest,
  operatorTaskApprovalRequestSchema,
} from "@dungle-scrubs/tether-protocol";

import { BrowserOperatorConfigurationError, BrowserOperatorHttpError } from "./errors.js";
import {
  BrowserSessionStream,
  type BrowserSessionDeliveryPolicy,
  type BrowserSessionStreamInput,
  type BrowserSessionReconnectPolicy,
} from "./session-stream.js";

/** Browser fetch subset used by the operator client. */
export type BrowserOperatorFetch = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

/** Browser operator client construction. */
export interface BrowserOperatorClientConfig {
  /** Grant-bound CSRF token returned by the pairing exchange. */
  readonly csrfToken?: string | null;
  /** Finite retained-delivery limits for every connected session. */
  readonly delivery?: BrowserSessionDeliveryPolicy;
  /** Optional browser-fetch override for hosts and tests. */
  readonly fetch?: BrowserOperatorFetch;
  /** Finite stream reconnect policy. */
  readonly reconnect?: BrowserSessionReconnectPolicy;
  /** Absolute Tether HTTP(S) URL, defaulting to the page origin. */
  readonly serviceUrl?: string;
  /** Optional native-WebSocket-compatible constructor for tests. */
  readonly webSocketConstructor?: typeof WebSocket;
}

/** Browser stream connection input. */
export interface ConnectBrowserSessionInput {
  /** Durable cursor returned by the snapshot. */
  readonly afterSeq: number;
  /** Handles each durable event before cursor advance and observes timeout cancellation. */
  readonly onEvent: BrowserSessionStreamInput["onEvent"];
  /** Receives safe stream failures. */
  readonly onError?: BrowserSessionStreamInput["onError"];
  /** Receives observable stream state changes. */
  readonly onStateChange?: BrowserSessionStreamInput["onStateChange"];
  /** Allowed session id. */
  readonly sessionId: string;
}

/** Cookie-authenticated operations counted by the browser operator client. */
export type BrowserOperatorOperation =
  | "approval"
  | "bootstrap"
  | "command"
  | "pairing_create"
  | "pairing_exchange"
  | "snapshot"
  | "websocket_ticket";

/** Point-in-time HTTP client diagnostics. */
export interface BrowserOperatorClientDebugInfo {
  readonly approvalCount: number;
  readonly bootstrapCount: number;
  readonly commandCount: number;
  readonly lastErrorReason: string | null;
  readonly pairingCreateCount: number;
  readonly pairingExchangeCount: number;
  readonly snapshotCount: number;
  readonly ticketCount: number;
}

interface RuntimeParser<T> {
  readonly parse: (value: unknown) => T;
}

/** Browser-only cookie operator client. */
export class BrowserOperatorClient {
  private csrfToken: string | null;
  private readonly delivery: BrowserSessionDeliveryPolicy | undefined;
  private readonly fetch: BrowserOperatorFetch;
  private lastErrorReason: string | null = null;
  private readonly operationCounts: Record<BrowserOperatorOperation, number> = {
    approval: 0,
    bootstrap: 0,
    command: 0,
    pairing_create: 0,
    pairing_exchange: 0,
    snapshot: 0,
    websocket_ticket: 0,
  };
  private readonly reconnect: BrowserSessionReconnectPolicy | undefined;
  private readonly serviceUrl: string;
  private readonly webSocketConstructor: typeof WebSocket;

  constructor(config: BrowserOperatorClientConfig = {}) {
    this.csrfToken = config.csrfToken ?? null;
    this.delivery = config.delivery;
    this.fetch = config.fetch ?? globalThis.fetch.bind(globalThis);
    this.reconnect = config.reconnect;
    this.serviceUrl = normalizeServiceUrl(config.serviceUrl ?? globalThis.location.origin);
    this.webSocketConstructor = config.webSocketConstructor ?? globalThis.WebSocket;
  }

  /** Reads the active cookie session and its allowed resources. */
  async bootstrap(): Promise<BrowserOperatorSession> {
    return this.requestJson("bootstrap", "/operator/browser-session", browserOperatorSessionSchema);
  }

  /** Creates an unconfirmed browser pairing request. */
  async createPairingRequest(
    request: CreateBrowserPairingRequest,
  ): Promise<BrowserPairingCreateResponse> {
    return this.requestJson(
      "pairing_create",
      "/browser/pairing-requests",
      browserPairingCreateResponseSchema,
      {
        body: JSON.stringify(createBrowserPairingRequestSchema.parse(request)),
        headers: jsonHeaders(),
        method: "POST",
      },
    );
  }

  /** Exchanges a confirmed single-use pairing secret and retains only its CSRF token. */
  async exchangePairingRequest(
    requestId: string,
    request: ExchangeBrowserPairingRequest,
  ): Promise<BrowserPairingExchangeResponse> {
    const result = await this.requestJson(
      "pairing_exchange",
      `/browser/pairing-requests/${encodeURIComponent(requestId)}/exchange`,
      browserPairingExchangeResponseSchema,
      {
        body: JSON.stringify(exchangeBrowserPairingRequestSchema.parse(request)),
        headers: jsonHeaders(),
        method: "POST",
      },
    );
    this.csrfToken = result.csrfToken;
    return result;
  }

  /** Loads one bounded provider-neutral snapshot. */
  async loadSnapshot(sessionId: string): Promise<BrowserSessionSnapshot> {
    return this.requestJson(
      "snapshot",
      `/operator/sessions/${encodeURIComponent(sessionId)}/snapshot`,
      browserSessionSnapshotSchema,
    );
  }

  /** Requests one provider-neutral operator command. */
  async requestCommand(
    sessionId: string,
    request: OperatorCommandRequest,
  ): Promise<BrowserOperatorCommandResponse> {
    return this.requestJson(
      "command",
      `/operator/sessions/${encodeURIComponent(sessionId)}/commands`,
      browserOperatorCommandResponseSchema,
      this.mutationRequest(operatorCommandRequestSchema.parse(request)),
    );
  }

  /** Submits one manifest-bound approval and returns the canonical server result. */
  async submitApproval(
    sessionId: string,
    taskId: string,
    request: OperatorTaskApprovalRequest,
  ): Promise<BrowserOperatorApprovalResponse> {
    return this.requestJson(
      "approval",
      `/operator/sessions/${encodeURIComponent(sessionId)}/tasks/${encodeURIComponent(taskId)}/approval`,
      browserOperatorApprovalResponseSchema,
      this.mutationRequest(operatorTaskApprovalRequestSchema.parse(request)),
    );
  }

  /** Mints one single-use ticket for a browser WebSocket upgrade. */
  async issueWebSocketTicket(): Promise<BrowserWebSocketTicketResponse> {
    return this.requestJson(
      "websocket_ticket",
      "/operator/websocket-ticket",
      browserWebSocketTicketResponseSchema,
      this.mutationRequest(),
    );
  }

  /** Opens one awaited browser stream that refreshes its ticket on reconnect. */
  async connectSession(input: ConnectBrowserSessionInput): Promise<BrowserSessionStream> {
    return BrowserSessionStream.connect({
      afterSeq: input.afterSeq,
      ...(this.delivery === undefined ? {} : { delivery: this.delivery }),
      onEvent: input.onEvent,
      ...(input.onError === undefined ? {} : { onError: input.onError }),
      ...(input.onStateChange === undefined ? {} : { onStateChange: input.onStateChange }),
      ...(this.reconnect === undefined ? {} : { reconnect: this.reconnect }),
      requestTicket: async () => (await this.issueWebSocketTicket()).ticket,
      serviceUrl: this.serviceUrl,
      sessionId: input.sessionId,
      webSocketConstructor: this.webSocketConstructor,
    });
  }

  /** Returns safe request counters and the latest stable rejection reason. */
  debugInfo(): BrowserOperatorClientDebugInfo {
    return {
      approvalCount: this.operationCounts.approval,
      bootstrapCount: this.operationCounts.bootstrap,
      commandCount: this.operationCounts.command,
      lastErrorReason: this.lastErrorReason,
      pairingCreateCount: this.operationCounts.pairing_create,
      pairingExchangeCount: this.operationCounts.pairing_exchange,
      snapshotCount: this.operationCounts.snapshot,
      ticketCount: this.operationCounts.websocket_ticket,
    };
  }

  /** Builds a state-changing request using the grant-bound CSRF token. */
  private mutationRequest(body?: unknown): RequestInit {
    if (this.csrfToken === null) {
      const error = new BrowserOperatorConfigurationError({ reason: "csrf_token_missing" });
      this.lastErrorReason = error.reason;
      throw error;
    }
    return {
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      headers: jsonHeaders(this.csrfToken),
      method: "POST",
    };
  }

  /** Performs one cookie-authenticated request and validates its protocol response. */
  private async requestJson<T>(
    operation: BrowserOperatorOperation,
    path: string,
    parser: RuntimeParser<T>,
    init: RequestInit = {},
  ): Promise<T> {
    let response: Response;
    try {
      response = await this.fetch(new URL(path, this.serviceUrl), {
        cache: "no-store",
        credentials: "include",
        ...init,
      });
    } catch (cause) {
      throw this.captureHttpError({ cause, operation, reason: "network_error", status: 0 });
    }
    const body = await readJson(response);
    if (!response.ok) {
      throw this.captureHttpError({
        operation,
        reason: readErrorReason(body),
        status: response.status,
      });
    }
    let parsed: T;
    try {
      parsed = parser.parse(body);
    } catch (cause) {
      throw this.captureHttpError({
        cause,
        operation,
        reason: "invalid_protocol_response",
        status: response.status,
      });
    }
    this.operationCounts[operation] += 1;
    return parsed;
  }

  /** Stores a safe HTTP error reason for diagnostics. */
  private captureHttpError(input: {
    readonly cause?: unknown;
    readonly operation: BrowserOperatorOperation;
    readonly reason: string;
    readonly status: number;
  }): BrowserOperatorHttpError {
    const error = new BrowserOperatorHttpError(input);
    this.lastErrorReason = error.reason;
    return error;
  }
}

/** Validates and normalizes the configured browser service origin. */
function normalizeServiceUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch (cause) {
    throw new BrowserOperatorConfigurationError({ cause, reason: "service_url_invalid" });
  }
  if (
    (url.protocol !== "http:" && url.protocol !== "https:") ||
    url.username !== "" ||
    url.password !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new BrowserOperatorConfigurationError({ reason: "service_url_invalid" });
  }
  return url.toString().replace(/\/$/u, "");
}

/** Builds browser JSON headers with optional CSRF authority. */
function jsonHeaders(csrfToken?: string): Headers {
  const headers = new Headers({
    accept: "application/json",
    "content-type": "application/json",
  });
  if (csrfToken !== undefined) {
    headers.set(browserCsrfHeaderName, csrfToken);
  }
  return headers;
}

/** Reads a JSON body and maps malformed responses to a safe sentinel. */
async function readJson(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

/** Extracts only a stable public reason string from an error body. */
function readErrorReason(body: unknown): string {
  if (typeof body !== "object" || body === null || !("reason" in body)) {
    return "http_error";
  }
  const reason = body.reason;
  return typeof reason === "string" && reason.length > 0 ? reason : "http_error";
}
