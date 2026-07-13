import { describe, expect, it } from "vitest";

import {
  AuthError,
  createAuthContext,
  mintAuthToken,
  verifyAuthToken,
  type AuthTokenPayload,
} from "../src/auth/token.js";

const secrets = {
  current: "current-secret",
  previous: "previous-secret",
} as const;

const payload: AuthTokenPayload = {
  exp: 4_102_444_800,
  kid: "current",
  participantId: "part_auth",
  role: "participant",
  sessionId: "sess_auth",
};

describe("auth token", () => {
  it("round-trips signed payload fields into an auth context", () => {
    const token = mintAuthToken(payload, secrets);

    const verified = verifyAuthToken(token, {
      now: new Date("2026-01-01T00:00:00.000Z"),
      secrets,
    });

    expect(verified).toEqual(payload);
    expect(createAuthContext(verified)).toEqual({
      expiresAt: new Date(payload.exp * 1_000).toISOString(),
      kid: "current",
      participantId: "part_auth",
      role: "participant",
      sessionScope: "sess_auth",
    });
  });

  it("rejects tampered payloads", () => {
    const token = mintAuthToken(payload, secrets);
    const [encodedPayload, signature] = token.split(".");
    if (!encodedPayload || !signature) {
      throw new Error("Expected a two-part token");
    }
    const decodedPayload = JSON.parse(Buffer.from(encodedPayload, "base64url").toString("utf8"));
    const tamperedPayload = Buffer.from(
      JSON.stringify({ ...decodedPayload, participantId: "part_attacker" }),
      "utf8",
    ).toString("base64url");

    expect(() =>
      verifyAuthToken(`${tamperedPayload}.${signature}`, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets,
      }),
    ).toThrow(AuthError.BadSignature);
  });

  it("rejects expired tokens with a typed auth error", () => {
    const token = mintAuthToken({ ...payload, exp: 1_767_225_599 }, secrets);

    expect(() =>
      verifyAuthToken(token, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets,
      }),
    ).toThrow(AuthError.Expired);
  });

  it("selects the verifying secret by kid", () => {
    const previousToken = mintAuthToken({ ...payload, kid: "previous" }, secrets);

    expect(
      verifyAuthToken(previousToken, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets,
      }).kid,
    ).toBe("previous");
    expect(() =>
      verifyAuthToken(previousToken, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets: { current: secrets.current },
      }),
    ).toThrow(AuthError.UnknownKid);
  });
});
