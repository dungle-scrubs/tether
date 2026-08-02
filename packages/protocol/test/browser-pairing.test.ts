import { describe, expect, it } from "vitest";

import {
  browserCsrfHeaderName,
  browserPairingCreateResponseSchema,
  browserPairingExchangeResponseSchema,
  exactHttpOriginSchema,
} from "../src/browser-pairing.js";

const scope = {
  actions: ["archive"],
  commands: ["scan"],
  permissions: ["session.read"],
  scopeKeys: ["account-primary:inbox"],
  sessionIds: ["sess_email"],
  targetKinds: ["message"],
};

describe("browser pairing protocol", () => {
  it("owns the canonical browser mutation CSRF header", () => {
    expect(browserCsrfHeaderName).toBe("x-tether-csrf");
  });

  it("validates credential-bearing creation and credential-safe exchange responses", () => {
    expect(
      browserPairingCreateResponseSchema.safeParse({
        exchangeSecret: "E".repeat(43),
        request: {
          confirmedAt: null,
          confirmedBySubject: null,
          createdAt: "2026-08-01T00:00:00.000Z",
          exchangedAt: null,
          expiresAt: "2026-08-01T00:10:00.000Z",
          failedAttempts: 0,
          invalidatedAt: null,
          operatorSubject: "operator@example.test",
          origin: "https://hub.example.test",
          publicNonce: "N".repeat(22),
          requestId: "pair_protocol",
          requestedScope: scope,
          verificationPhrase: "amber cedar orbit",
        },
        status: "created",
      }).success,
    ).toBe(true);
    expect(
      browserPairingExchangeResponseSchema.safeParse({
        bearer: "must-not-cross-the-response-boundary",
        csrfToken: "C".repeat(43),
        expiresAt: "2026-08-02T00:00:00.000Z",
        grantJti: "grant_browser",
        scope,
        status: "exchanged",
      }).success,
    ).toBe(false);
  });

  it("accepts only exact credential-free HTTP origins", () => {
    for (const origin of ["http://127.0.0.1:17445", "https://hub.example.test"]) {
      expect(exactHttpOriginSchema.parse(origin)).toBe(origin);
    }
    for (const value of [
      "",
      "null",
      "hub.example.test",
      "ws://hub.example.test",
      "https://hub.example.test/",
      "https://hub.example.test/app",
      "https://hub.example.test?query=1",
      "https://hub.example.test#fragment",
      "https://operator:secret@hub.example.test",
      `https://${"h".repeat(600)}.example.test`,
    ]) {
      expect(exactHttpOriginSchema.safeParse(value).success).toBe(false);
    }
  });
});
