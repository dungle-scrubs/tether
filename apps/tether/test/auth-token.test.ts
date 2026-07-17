import { createHmac } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  AuthError,
  createAuthContext,
  mintAuthToken,
  verifyLegacyAuthToken,
  type AuthTokenPayload,
} from "../src/auth/token.js";
import {
  AuthGrantTokenError,
  mintAuthGrantToken,
  verifyAuthGrantToken,
} from "../src/auth/grant-token.js";
import type { MintAuthGrantTokenInput } from "../src/auth/grant-token.js";

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
  it("mints and verifies a tgr2 REST grant with a 24-hour default lifetime", () => {
    const now = new Date("2026-01-01T00:00:00.000Z");
    const token = mintAuthGrantToken(
      {
        issuer: "https://auth.example.test",
        jti: "grant_auth",
        kid: "current",
        role: "participant",
        sessionScope: "sess_auth",
        subject: "part_auth",
      },
      secrets,
      { now },
    );

    expect(token).toMatch(/^tgr2\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u);
    expect(
      verifyAuthGrantToken(token, {
        audience: "tether-rest",
        issuer: "https://auth.example.test",
        now,
        secrets,
      }),
    ).toEqual({
      aud: "tether-rest",
      exp: 1_767_312_000,
      iat: 1_767_225_600,
      iss: "https://auth.example.test",
      jti: "grant_auth",
      kid: "current",
      role: "participant",
      sessionScope: "sess_auth",
      sub: "part_auth",
    });
  });

  it("rejects tgr2 lifetimes above seven days without exposing claims", () => {
    expect(() =>
      mintAuthGrantToken(
        {
          issuer: "https://auth.example.test",
          jti: "grant_secret_marker",
          kid: "current",
          role: "admin",
          sessionScope: "*",
          subject: "subject_secret_marker",
          ttlSeconds: 7 * 24 * 60 * 60 + 1,
        },
        secrets,
        { now: new Date("2026-01-01T00:00:00.000Z") },
      ),
    ).toThrow(AuthGrantTokenError.InvalidLifetime);

    try {
      mintAuthGrantToken(
        {
          issuer: "https://auth.example.test",
          jti: "grant_secret_marker",
          kid: "current",
          role: "admin",
          sessionScope: "*",
          subject: "subject_secret_marker",
          ttlSeconds: 7 * 24 * 60 * 60 + 1,
        },
        secrets,
      );
    } catch (error) {
      expect(String(error)).not.toContain("secret_marker");
    }
  });

  it("rejects a WebSocket audience at the durable grant mint boundary", () => {
    const input = {
      audience: "tether-websocket",
      issuer: "https://auth.example.test",
      jti: "grant_auth",
      kid: "current",
      role: "participant",
      sessionScope: "sess_auth",
      subject: "part_auth",
    } as unknown as MintAuthGrantTokenInput;

    expect(() => mintAuthGrantToken(input, secrets)).toThrow(AuthGrantTokenError.Malformed);
  });

  it("rejects noncanonical base64url signature encodings", () => {
    const now = new Date("2026-01-01T00:00:00.000Z");
    const token = mintAuthGrantToken(
      {
        issuer: "https://auth.example.test",
        jti: "grant_auth",
        kid: "current",
        role: "participant",
        sessionScope: "sess_auth",
        subject: "part_auth",
      },
      secrets,
      { now },
    );

    for (const suffix of ["!", "=", " "]) {
      expect(() =>
        verifyAuthGrantToken(`${token}${suffix}`, {
          audience: "tether-rest",
          issuer: "https://auth.example.test",
          now,
          secrets,
        }),
      ).toThrow(AuthGrantTokenError.Malformed);
    }
  });

  it("rejects signed payloads with noncanonical JSON or unknown claims", () => {
    const claims = {
      aud: "tether-rest",
      exp: 1_767_312_000,
      iat: 1_767_225_600,
      iss: "https://auth.example.test",
      jti: "grant_auth",
      kid: "current",
      role: "participant",
      sessionScope: "sess_auth",
      sub: "part_auth",
    } as const;
    const noncanonicalPayloads = [
      JSON.stringify({
        sub: claims.sub,
        aud: claims.aud,
        exp: claims.exp,
        iat: claims.iat,
        iss: claims.iss,
        jti: claims.jti,
        kid: claims.kid,
        role: claims.role,
        sessionScope: claims.sessionScope,
      }),
      JSON.stringify({ ...claims, unexpected: "claim" }),
      JSON.stringify(claims, null, 2),
    ];

    for (const payloadJson of noncanonicalPayloads) {
      expect(() =>
        verifyAuthGrantToken(signRawGrantPayload(payloadJson), {
          audience: "tether-rest",
          issuer: claims.iss,
          now: new Date("2026-01-01T00:00:01.000Z"),
          secrets,
        }),
      ).toThrow(AuthGrantTokenError.Malformed);
    }
  });

  it("rejects a correctly signed payload whose lifetime exceeds seven days", () => {
    const issuedAt = 1_767_225_600;
    const token = signRawGrantPayload(
      JSON.stringify({
        aud: "tether-rest",
        exp: issuedAt + 7 * 24 * 60 * 60 + 1,
        iat: issuedAt,
        iss: "https://auth.example.test",
        jti: "grant_auth",
        kid: "current",
        role: "participant",
        sessionScope: "sess_auth",
        sub: "part_auth",
      }),
    );

    expect(() =>
      verifyAuthGrantToken(token, {
        audience: "tether-rest",
        issuer: "https://auth.example.test",
        now: new Date("2026-01-01T00:00:01.000Z"),
        secrets,
      }),
    ).toThrow(AuthGrantTokenError.InvalidLifetime);
  });

  it("validates required grant claims and emits only bounded reason codes", () => {
    expect(() =>
      mintAuthGrantToken(
        {
          issuer: "",
          jti: "grant_auth",
          kid: "current",
          role: "participant",
          sessionScope: "sess_auth",
          subject: "part_auth",
        },
        secrets,
      ),
    ).toThrow(AuthGrantTokenError.Malformed);

    const token = mintAuthGrantToken(
      {
        issuer: "https://issuer.secret-marker.test",
        jti: "grant_secret_marker",
        kid: "current",
        role: "participant",
        sessionScope: "sess_secret_marker",
        subject: "part_secret_marker",
        ttlSeconds: 7 * 24 * 60 * 60,
      },
      secrets,
      { now: new Date("2026-01-01T00:00:00.000Z") },
    );

    expect(() =>
      verifyAuthGrantToken(token, {
        audience: "tether-websocket" as "tether-rest",
        issuer: "https://issuer.secret-marker.test",
        now: new Date("2026-01-01T00:00:01.000Z"),
        secrets,
      }),
    ).toThrowError(new Error(AuthGrantTokenError.AudienceMismatch));
    try {
      verifyAuthGrantToken(token, {
        audience: "tether-rest",
        issuer: "https://wrong.example.test",
        now: new Date("2026-01-01T00:00:01.000Z"),
        secrets,
      });
    } catch (error) {
      expect(String(error)).toBe(`Error: ${AuthGrantTokenError.IssuerMismatch}`);
      expect(String(error)).not.toContain("secret_marker");
      expect(String(error)).not.toContain(token);
    }
  });

  it("keeps the legacy two-segment verifier fail-closed for tgr2", () => {
    const token = mintAuthGrantToken(
      {
        issuer: "https://auth.example.test",
        jti: "grant_auth",
        kid: "current",
        role: "participant",
        sessionScope: "sess_auth",
        subject: "part_auth",
      },
      secrets,
      { now: new Date("2026-01-01T00:00:00.000Z") },
    );

    expect(() => verifyLegacyAuthToken(token, { secrets })).toThrow(AuthError.Malformed);
  });

  it("round-trips signed payload fields into an auth context", () => {
    const token = mintAuthToken(payload, secrets);

    const verified = verifyLegacyAuthToken(token, {
      now: new Date("2026-01-01T00:00:00.000Z"),
      secrets,
    });

    expect(verified).toEqual(payload);
    expect(createAuthContext(verified)).toEqual({
      expiresAt: new Date(payload.exp * 1_000).toISOString(),
      grantJti: null,
      issuer: null,
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
      verifyLegacyAuthToken(`${tamperedPayload}.${signature}`, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets,
      }),
    ).toThrow(AuthError.BadSignature);
  });

  it("rejects expired tokens with a typed auth error", () => {
    const token = mintAuthToken({ ...payload, exp: 1_767_225_599 }, secrets);

    expect(() =>
      verifyLegacyAuthToken(token, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets,
      }),
    ).toThrow(AuthError.Expired);
  });

  it("selects the verifying secret by kid", () => {
    const previousToken = mintAuthToken({ ...payload, kid: "previous" }, secrets);

    expect(
      verifyLegacyAuthToken(previousToken, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets,
      }).kid,
    ).toBe("previous");
    expect(() =>
      verifyLegacyAuthToken(previousToken, {
        now: new Date("2026-01-01T00:00:00.000Z"),
        secrets: { current: secrets.current },
      }),
    ).toThrow(AuthError.UnknownKid);
  });
});

/** Signs exact JSON bytes so verifier tests can exercise fail-closed parsing. */
function signRawGrantPayload(payloadJson: string): string {
  const encodedPayload = Buffer.from(payloadJson, "utf8").toString("base64url");
  const signedValue = `tgr2.${encodedPayload}`;
  const signature = createHmac("sha256", secrets.current).update(signedValue).digest("base64url");
  return `${signedValue}.${signature}`;
}
