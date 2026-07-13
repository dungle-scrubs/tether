import { describe, expect, it } from "vitest";

import { mintAuthTokenFromCli, parseMintCliOptions } from "../src/auth/mint-cli.js";
import { verifyAuthToken } from "../src/auth/token.js";

describe("tether-mint CLI", () => {
  it("mints a token for participant, session, role, and ttl", () => {
    const token = mintAuthTokenFromCli(
      [
        "--participant",
        "part_cli",
        "--session",
        "sess_cli",
        "--role",
        "participant",
        "--ttl",
        "60",
      ],
      { AUTH_SIGNING_KID: "cli", AUTH_SIGNING_SECRET: "cli-secret" },
      new Date("2026-01-01T00:00:00.000Z"),
    );

    expect(
      verifyAuthToken(token, {
        now: new Date("2026-01-01T00:00:30.000Z"),
        secrets: { cli: "cli-secret" },
      }),
    ).toEqual({
      exp: 1_767_225_660,
      kid: "cli",
      participantId: "part_cli",
      role: "participant",
      sessionId: "sess_cli",
    });
  });

  it("supports service scope and a 30 day default ttl", () => {
    const token = mintAuthTokenFromCli(
      ["--participant", "part_cli", "--session", "*", "--role", "admin"],
      { AUTH_SIGNING_SECRET: "cli-secret" },
      new Date("2026-01-01T00:00:00.000Z"),
    );

    expect(
      verifyAuthToken(token, {
        now: new Date("2026-01-01T00:00:30.000Z"),
        secrets: { default: "cli-secret" },
      }),
    ).toMatchObject({
      exp: 1_769_817_600,
      kid: "default",
      role: "admin",
      sessionId: "*",
    });
  });

  it("requires a server-side signing secret", () => {
    expect(() =>
      parseMintCliOptions(["--participant", "part_cli", "--session", "*", "--role", "admin"], {}),
    ).toThrow("AUTH_SIGNING_SECRET is required");
  });

  it("uses the default signing key id when config is blank", () => {
    expect(
      parseMintCliOptions(["--participant", "part_cli", "--session", "*", "--role", "admin"], {
        AUTH_SIGNING_KID: " ",
        AUTH_SIGNING_SECRET: "cli-secret",
      }).kid,
    ).toBe("default");
  });
});
