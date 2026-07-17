import type { IncomingMessage } from "node:http";

import { describe, expect, it, vi } from "vitest";

import { createAuthRuntime, type AuthRuntimeLogger } from "../src/auth/enforcement.js";
import type { AuthGrantRecord, AuthGrantStore } from "../src/auth/grant-stores.js";
import { mintAuthGrantToken } from "../src/auth/grant-token.js";
import { AuthError, mintAuthToken } from "../src/auth/token.js";

describe("auth enforcement runtime", () => {
  it("authenticates tgr2 through PostgreSQL and retains a private command revalidator", async () => {
    const issuedAt = new Date("2026-07-17T00:00:00.000Z");
    const store: AuthGrantStore = {
      findByJti: vi.fn(
        async (): Promise<AuthGrantRecord> => ({
          audience: "tether-rest",
          expiresAt: new Date("2026-07-18T00:00:00.000Z"),
          issuedAt,
          issuer: "https://auth.runtime.test",
          jti: "grant_runtime",
          kid: "current",
          metadata: { requestId: null, source: "admin" },
          revokedAt: null,
          role: "admin",
          sessionScope: "*",
          subject: "part_runtime",
        }),
      ),
      list: async () => [],
    };
    const runtime = createAuthRuntime({
      activeKid: "current",
      grantStore: store,
      issuer: "https://auth.runtime.test",
      mode: "required",
      now: () => new Date("2026-07-17T01:00:00.000Z"),
      secrets: { current: "runtime-secret" },
    });
    const bearer = mintAuthGrantToken(
      {
        issuer: "https://auth.runtime.test",
        jti: "grant_runtime",
        kid: "current",
        role: "admin",
        sessionScope: "*",
        subject: "part_runtime",
      },
      { current: "runtime-secret" },
      { now: issuedAt },
    );

    const authenticated = await runtime.authenticateWebSocketUpgrade(
      {
        headers: {},
        method: "GET",
        url: `/sessions/sess_1/stream?access_token=${bearer}`,
      } as IncomingMessage,
      new URL(`http://localhost/sessions/sess_1/stream?access_token=${bearer}`),
    );
    expect(authenticated?.context).toMatchObject({ grantJti: "grant_runtime" });
    await expect(authenticated?.authorizeCommand()).resolves.toBeUndefined();
    expect(store.findByJti).toHaveBeenCalledTimes(2);
    expect(JSON.stringify(runtime.debugInfo())).not.toContain(bearer);
  });

  it.each([
    {
      label: "signature changes",
      mutate: (fixture: CommandFixture) => {
        fixture.secrets.current = "rotated-runtime-secret";
      },
      reason: AuthError.ClaimInvalid,
    },
    {
      label: "issuer claim mismatch",
      mutate: (fixture: CommandFixture) => {
        fixture.grant.value = { ...fixture.grant.value, issuer: "https://other.runtime.test" };
      },
      reason: AuthError.ClaimInvalid,
    },
    {
      label: "audience claim mismatch",
      mutate: (fixture: CommandFixture) => {
        fixture.grant.value = {
          ...fixture.grant.value,
          audience: "tether-websocket" as AuthGrantRecord["audience"],
        };
      },
      reason: AuthError.ClaimInvalid,
    },
    {
      label: "issued-at claim mismatch",
      mutate: (fixture: CommandFixture) => {
        fixture.grant.value = {
          ...fixture.grant.value,
          issuedAt: new Date("2026-07-17T00:00:01.000Z"),
        };
      },
      reason: AuthError.ClaimInvalid,
    },
    {
      label: "signing-key removal",
      mutate: (fixture: CommandFixture) => {
        delete fixture.secrets.current;
      },
      reason: AuthError.ClaimInvalid,
    },
    {
      label: "durable revocation",
      mutate: (fixture: CommandFixture) => {
        fixture.grant.value = {
          ...fixture.grant.value,
          revokedAt: new Date("2026-07-17T00:30:00.000Z"),
        };
      },
      reason: AuthError.GrantRevoked,
    },
    {
      label: "expiry",
      mutate: (fixture: CommandFixture) => {
        fixture.currentTime.value = new Date("2026-07-18T00:00:00.000Z");
      },
      reason: AuthError.GrantExpired,
    },
    {
      label: "authority store outage",
      mutate: (fixture: CommandFixture) => {
        fixture.storeUnavailable.value = true;
      },
      reason: AuthError.StoreUnavailable,
    },
  ])("fails closed when command reauthorization detects $label", async ({ mutate, reason }) => {
    const fixture = await createCommandFixture();
    mutate(fixture);

    let rejection: unknown;
    try {
      await fixture.authenticated.authorizeCommand();
    } catch (error) {
      rejection = error;
    }

    expect(rejection).toMatchObject({ message: reason });
    const [, encodedPayload, encodedSignature] = fixture.bearer.split(".");
    const diagnostics = JSON.stringify({
      debug: fixture.runtime.debugInfo(),
      rejection,
      warnings: fixture.warnings,
    });
    expect(diagnostics).not.toContain(fixture.bearer);
    expect(diagnostics).not.toContain(encodedPayload);
    expect(diagnostics).not.toContain(encodedSignature);
    expect(diagnostics).not.toContain(fixture.grant.value.subject);
  });

  it("logs redacted auth rejections through the injected logger", async () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "default",
      logger: collectWarnings(warnings),
      mode: "required",
      secrets: { default: "secret" },
    });

    await expect(
      runtime.authenticateHttpRequest(
        {
          headers: {},
          method: "GET",
        } as IncomingMessage,
        new URL("http://localhost/sessions?access_token=secret"),
      ),
    ).rejects.toThrow(AuthError.Missing);

    expect(warnings).toEqual([
      {
        details: {
          method: "GET",
          reason: AuthError.Missing,
          route: "/sessions",
          transport: "http",
        },
        event: "auth.reject",
      },
    ]);
  });

  it("rejects ambiguous WebSocket credentials without exposing either secret", async () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "default",
      logger: collectWarnings(warnings),
      mode: "required",
      secrets: { default: "secret" },
    });
    const bearer = "tgr2.secret_payload_marker.secret_signature_marker";
    const ticket = "A".repeat(43);
    const url = new URL(`http://localhost/sessions/sess_1/stream?ticket=${ticket}`);

    await expect(
      runtime.authenticateWebSocketUpgrade(
        {
          headers: { authorization: `Bearer ${bearer}` },
          method: "GET",
          url: `${url.pathname}${url.search}`,
        } as IncomingMessage,
        url,
      ),
    ).rejects.toThrow(AuthError.ClaimInvalid);

    const diagnostics = JSON.stringify({ debug: runtime.debugInfo(), warnings });
    expect(diagnostics).not.toContain(bearer);
    expect(diagnostics).not.toContain("secret_payload_marker");
    expect(diagnostics).not.toContain("secret_signature_marker");
    expect(diagnostics).not.toContain(ticket);
    expect(warnings).toEqual([
      {
        details: {
          method: "GET",
          reason: AuthError.ClaimInvalid,
          route: "/sessions/sess_1/stream",
          transport: "ws",
        },
        event: "auth.reject",
      },
    ]);
  });

  it("logs disabled mode warning through the injected logger", () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "disabled",
      logger: collectWarnings(warnings),
      mode: "disabled",
      secrets: {},
    });

    runtime.close();

    expect(warnings).toEqual([
      {
        details: {
          authMode: "disabled",
          message: "Tether auth enforcement is disabled; use only for local development.",
        },
        event: "auth.disabled",
      },
    ]);
  });

  it("rejects legacy stateless tokens in required mode by default", async () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "default",
      logger: collectWarnings(warnings),
      mode: "required",
      secrets: { default: "secret" },
    });
    const bearer = mintLegacyBearer();

    await expect(
      runtime.authenticateHttpRequest(
        {
          headers: { authorization: `Bearer ${bearer}` },
          method: "GET",
        } as IncomingMessage,
        new URL("http://localhost/sessions"),
      ),
    ).rejects.toThrowError(new Error(AuthError.LegacyTokenRejected));

    expect(warnings).toEqual([
      {
        details: {
          method: "GET",
          reason: AuthError.LegacyTokenRejected,
          route: "/sessions",
          transport: "http",
        },
        event: "auth.reject",
      },
    ]);
    expect(JSON.stringify({ debug: runtime.debugInfo(), warnings })).not.toContain(bearer);
  });

  it("rejects legacy stateless tokens on WebSocket upgrades in required mode by default", async () => {
    const runtime = createAuthRuntime({
      activeKid: "default",
      mode: "required",
      secrets: { default: "secret" },
    });
    const bearer = mintLegacyBearer();
    const url = new URL(`http://localhost/sessions/sess_legacy/stream?access_token=${bearer}`);

    await expect(
      runtime.authenticateWebSocketUpgrade(
        {
          headers: {},
          method: "GET",
          url: `${url.pathname}${url.search}`,
        } as IncomingMessage,
        url,
      ),
    ).rejects.toThrowError(new Error(AuthError.LegacyTokenRejected));
  });

  it("accepts legacy stateless tokens only with the explicit migration escape hatch", async () => {
    const runtime = createAuthRuntime({
      activeKid: "default",
      allowLegacyTokens: true,
      mode: "required",
      secrets: { default: "secret" },
    });

    const context = await runtime.authenticateHttpRequest(
      {
        headers: { authorization: `Bearer ${mintLegacyBearer()}` },
        method: "GET",
      } as IncomingMessage,
      new URL("http://localhost/sessions"),
    );

    expect(context).toMatchObject({
      grantJti: null,
      participantId: "part_legacy",
      role: "participant",
      sessionScope: "sess_legacy",
    });
    expect(runtime.debugInfo().legacyTokensAllowed).toBe(true);
  });

  it("reports the legacy escape hatch as disabled by default", () => {
    const runtime = createAuthRuntime({
      activeKid: "default",
      mode: "required",
      secrets: { default: "secret" },
    });

    expect(runtime.debugInfo().legacyTokensAllowed).toBe(false);
  });

  it("rejects tgr2 through the legacy runtime without exposing credential material", async () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "default",
      logger: collectWarnings(warnings),
      mode: "required",
      secrets: { default: "secret" },
    });
    const bearer = "tgr2.secret_payload_marker.secret_signature_marker";

    await expect(
      runtime.authenticateHttpRequest(
        {
          headers: { authorization: `Bearer ${bearer}` },
          method: "GET",
        } as IncomingMessage,
        new URL("http://localhost/sessions"),
      ),
    ).rejects.toThrowError(new Error(AuthError.ClaimInvalid));

    const diagnostics = JSON.stringify({ debug: runtime.debugInfo(), warnings });
    expect(diagnostics).not.toContain(bearer);
    expect(diagnostics).not.toContain("secret_payload_marker");
    expect(diagnostics).not.toContain("secret_signature_marker");
  });
});

interface AuthWarning {
  readonly details: Record<string, unknown>;
  readonly event: string;
}

/** Mints a compatibility legacy stateless bearer for escape-hatch tests. */
function mintLegacyBearer(): string {
  return mintAuthToken(
    {
      exp: 4_102_444_800,
      kid: "default",
      participantId: "part_legacy",
      role: "participant",
      sessionId: "sess_legacy",
    },
    { default: "secret" },
  );
}

function collectWarnings(warnings: AuthWarning[]): AuthRuntimeLogger {
  return {
    warn: (event, details) => {
      warnings.push({ details, event });
    },
  };
}

async function createCommandFixture() {
  const issuedAt = new Date("2026-07-17T00:00:00.000Z");
  const currentTime = { value: new Date("2026-07-17T01:00:00.000Z") };
  const grant: { value: AuthGrantRecord } = {
    value: {
      audience: "tether-rest",
      expiresAt: new Date("2026-07-18T00:00:00.000Z"),
      issuedAt,
      issuer: "https://auth.runtime.test",
      jti: "grant_command_runtime",
      kid: "current",
      metadata: { requestId: null, source: "admin" },
      revokedAt: null,
      role: "participant",
      sessionScope: "sess_command_runtime",
      subject: "part_command_runtime",
    },
  };
  const secrets: Record<string, string> = { current: "runtime-secret" };
  const storeUnavailable = { value: false };
  const store: AuthGrantStore = {
    findByJti: vi.fn(async () => {
      if (storeUnavailable.value) throw new Error("database credential detail");
      return grant.value;
    }),
    list: async () => [],
  };
  const warnings: AuthWarning[] = [];
  const runtime = createAuthRuntime({
    activeKid: "current",
    grantStore: store,
    issuer: "https://auth.runtime.test",
    logger: collectWarnings(warnings),
    mode: "required",
    now: () => currentTime.value,
    secrets,
  });
  const bearer = mintAuthGrantToken(
    {
      issuer: "https://auth.runtime.test",
      jti: grant.value.jti,
      kid: "current",
      role: grant.value.role,
      sessionScope: grant.value.sessionScope,
      subject: grant.value.subject,
    },
    secrets,
    { now: issuedAt },
  );
  const url = new URL(
    `http://localhost/sessions/${grant.value.sessionScope}/stream?access_token=${bearer}`,
  );
  const authenticated = await runtime.authenticateWebSocketUpgrade(
    { headers: {}, method: "GET", url: `${url.pathname}${url.search}` } as IncomingMessage,
    url,
  );
  return {
    authenticated,
    bearer,
    currentTime,
    grant,
    runtime,
    secrets,
    storeUnavailable,
    warnings,
  };
}

type CommandFixture = Awaited<ReturnType<typeof createCommandFixture>>;
