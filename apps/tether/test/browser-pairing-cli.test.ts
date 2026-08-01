import { describe, expect, it } from "vitest";

import { parseBrowserPairingCliOptions } from "../src/auth/browser-pairing-cli.js";

describe("browser pairing admin CLI", () => {
  it("parses an explicit request creation with nonce and bounded scopes", () => {
    const scope = {
      actions: ["archive"],
      commands: ["scan"],
      permissions: ["session.read"],
      scopeKeys: ["account-primary:inbox"],
      sessionIds: ["sess_email"],
      targetKinds: ["message"],
    };

    expect(
      parseBrowserPairingCliOptions(
        [
          "create",
          "--operator",
          "operator@example.test",
          "--origin",
          "https://hub.example.test",
          "--nonce",
          "N".repeat(22),
          "--scope",
          JSON.stringify(scope),
        ],
        { DATABASE_URL: "postgres://tether:test@127.0.0.1/tether" },
      ),
    ).toEqual({
      command: "create",
      databaseUrl: "postgres://tether:test@127.0.0.1/tether",
      operatorSubject: "operator@example.test",
      origin: "https://hub.example.test",
      publicNonce: "N".repeat(22),
      requestedScope: scope,
    });
  });

  it("requires an explicit actor and matching verification phrase for confirmation", () => {
    expect(
      parseBrowserPairingCliOptions(
        [
          "confirm",
          "--request",
          "pair_019c1234",
          "--actor",
          "admin@example.test",
          "--phrase",
          "amber cedar orbit",
        ],
        { DATABASE_URL: "postgres://tether:test@127.0.0.1/tether" },
      ),
    ).toEqual({
      actorSubject: "admin@example.test",
      command: "confirm",
      databaseUrl: "postgres://tether:test@127.0.0.1/tether",
      requestId: "pair_019c1234",
      verificationPhrase: "amber cedar orbit",
    });
  });

  it.each([
    "https://hub.example.test/path",
    "https://user@hub.example.test",
    "ftp://hub.example.test",
  ])("rejects a non-origin browser URL: %s", (origin) => {
    expect(() =>
      parseBrowserPairingCliOptions(
        [
          "create",
          "--operator",
          "operator@example.test",
          "--origin",
          origin,
          "--nonce",
          "N".repeat(22),
          "--scope",
          JSON.stringify({
            actions: [],
            commands: [],
            permissions: ["session.read"],
            scopeKeys: ["account-primary:inbox"],
            sessionIds: ["sess_email"],
            targetKinds: [],
          }),
        ],
        { DATABASE_URL: "postgres://tether:test@127.0.0.1/tether" },
      ),
    ).toThrow();
  });
});
