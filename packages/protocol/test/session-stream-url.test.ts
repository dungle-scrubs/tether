import { describe, expect, it } from "vitest";

import {
  buildSessionStreamUrl,
  sessionEventObserverRuntimeKind,
} from "../src/session-stream-url.js";

describe("buildSessionStreamUrl", () => {
  it("upgrades the transport and always requests the passive observer mode", () => {
    expect(
      buildSessionStreamUrl({
        afterSeq: 12,
        serviceUrl: "https://hub.example.test",
        sessionId: "sess email",
      }),
    ).toBe(
      `wss://hub.example.test/sessions/sess%20email/stream?after=12&runtimeKind=${sessionEventObserverRuntimeKind}`,
    );
    expect(
      buildSessionStreamUrl({
        afterSeq: 0,
        serviceUrl: "http://127.0.0.1:17445",
        sessionId: "sess_email",
      }),
    ).toBe(
      `ws://127.0.0.1:17445/sessions/sess_email/stream?after=0&runtimeKind=${sessionEventObserverRuntimeKind}`,
    );
  });

  it("carries only the credential the caller supplied", () => {
    expect(
      buildSessionStreamUrl({
        accessToken: "token_participant",
        afterSeq: 1,
        serviceUrl: "https://hub.example.test",
        sessionId: "sess_email",
      }),
    ).toContain("access_token=token_participant");
    const ticketUrl = buildSessionStreamUrl({
      afterSeq: 1,
      serviceUrl: "https://hub.example.test",
      sessionId: "sess_email",
      ticket: "T".repeat(43),
    });
    expect(ticketUrl).toContain(`ticket=${"T".repeat(43)}`);
    expect(ticketUrl).not.toContain("access_token");
  });
});
