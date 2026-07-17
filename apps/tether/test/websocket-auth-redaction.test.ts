import type { IncomingMessage } from "node:http";

import { describe, expect, it } from "vitest";

import { redactWebSocketCredentials } from "../src/websocket-participant-gateway.js";

describe("WebSocket authentication redaction", () => {
  it("removes the captured bearer from request and downstream URL surfaces", () => {
    const bearer = "tgr2.secret_payload_marker.secret_signature_marker";
    const ticket = "ticket_secret_marker";
    const request = {
      headers: { authorization: `Bearer ${bearer}` },
      url: `/sessions/sess_redaction/stream?after=4&access_token=${bearer}&ticket=${ticket}&participantId=part_redaction`,
    } as IncomingMessage;
    const url = new URL(request.url ?? "", "http://localhost");

    const searchParams = redactWebSocketCredentials(request, url);

    expect(request.url).toBe(
      "/sessions/sess_redaction/stream?after=4&participantId=part_redaction",
    );
    expect(searchParams.get("after")).toBe("4");
    expect(searchParams.get("participantId")).toBe("part_redaction");
    expect(searchParams.has("access_token")).toBe(false);
    expect(searchParams.has("ticket")).toBe(false);
    expect(request.headers.authorization).toBeUndefined();
    const diagnostics = JSON.stringify({
      authorization: request.headers.authorization,
      requestUrl: request.url,
      searchParams: [...searchParams],
    });
    expect(diagnostics).not.toContain(bearer);
    expect(diagnostics).not.toContain("secret_payload_marker");
    expect(diagnostics).not.toContain("secret_signature_marker");
    expect(diagnostics).not.toContain(ticket);
  });
});
