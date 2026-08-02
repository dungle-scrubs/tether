/**
 * Runtime-kind value that selects the server's passive full-event observer
 * mode: the connection receives the durable event stream (replay + live) but
 * does not register a durable participant or acquire a control lease. Keep this
 * in sync with `classifyHostPresenceStream` in the Tether gateway.
 */
export const sessionEventObserverRuntimeKind = "observer";

/** Inputs that fully determine one observer WebSocket stream URL. */
export interface SessionStreamUrlInput {
  /** Optional bearer credential carried as the `access_token` parameter. */
  readonly accessToken?: string | undefined;
  /** Durable cursor the server replays after. */
  readonly afterSeq: number;
  /** Absolute HTTP(S) service URL. */
  readonly serviceUrl: string;
  /** Durable session id whose stream is requested. */
  readonly sessionId: string;
  /** Optional single-use browser credential carried as the `ticket` parameter. */
  readonly ticket?: string | undefined;
}

/**
 * Builds the observer stream URL shared by every Tether transport. The passive
 * observer runtime kind never acquires a control lease, so its reconnects
 * cannot collide with a runtime's own control channel.
 */
export function buildSessionStreamUrl(input: SessionStreamUrlInput): string {
  const url = new URL(`/sessions/${encodeURIComponent(input.sessionId)}/stream`, input.serviceUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.searchParams.set("after", String(input.afterSeq));
  url.searchParams.set("runtimeKind", sessionEventObserverRuntimeKind);
  if (input.accessToken !== undefined && input.accessToken !== "") {
    url.searchParams.set("access_token", input.accessToken);
  }
  if (input.ticket !== undefined) {
    url.searchParams.set("ticket", input.ticket);
  }
  return url.toString();
}
