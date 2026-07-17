import type { IncomingMessage } from "node:http";

import { describe, expect, it } from "vitest";

import { redactWebSocketAccessToken } from "../src/websocket-participant-gateway.js";

describe("WebSocket authentication redaction", () => {
  it("removes the captured bearer from request and downstream URL surfaces", () => {
    const bearer = "tgr2.secret_payload_marker.secret_signature_marker";
    const request = {
      url: `/sessions/sess_redaction/stream?after=4&access_token=${bearer}&participantId=part_redaction`,
    } as IncomingMessage;
    const url = new URL(request.url ?? "", "http://localhost");

    const searchParams = redactWebSocketAccessToken(request, url);

    expect(request.url).toBe(
      "/sessions/sess_redaction/stream?after=4&participantId=part_redaction",
    );
    expect(searchParams.get("after")).toBe("4");
    expect(searchParams.get("participantId")).toBe("part_redaction");
    expect(searchParams.has("access_token")).toBe(false);
    const diagnostics = JSON.stringify({
      requestUrl: request.url,
      searchParams: [...searchParams],
    });
    expect(diagnostics).not.toContain(bearer);
    expect(diagnostics).not.toContain("secret_payload_marker");
    expect(diagnostics).not.toContain("secret_signature_marker");
  });
});
