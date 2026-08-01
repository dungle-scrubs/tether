import { describe, expect, it } from "vitest";

import {
  browserPairingCreateResponseSchema,
  browserPairingExchangeResponseSchema,
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
});
