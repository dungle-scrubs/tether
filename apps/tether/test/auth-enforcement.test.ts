import type { IncomingMessage } from "node:http";

import { describe, expect, it } from "vitest";

import { createAuthRuntime, type AuthRuntimeLogger } from "../src/auth/enforcement.js";
import { AuthError } from "../src/auth/token.js";

describe("auth enforcement runtime", () => {
  it("logs redacted auth rejections through the injected logger", () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "default",
      logger: collectWarnings(warnings),
      mode: "required",
      secrets: { default: "secret" },
    });

    expect(() =>
      runtime.authenticateHttpRequest(
        {
          headers: {},
          method: "GET",
        } as IncomingMessage,
        new URL("http://localhost/sessions?access_token=secret"),
      ),
    ).toThrow(AuthError.Missing);

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

  it("rejects tgr2 through the legacy runtime without exposing credential material", () => {
    const warnings: AuthWarning[] = [];
    const runtime = createAuthRuntime({
      activeKid: "default",
      logger: collectWarnings(warnings),
      mode: "required",
      secrets: { default: "secret" },
    });
    const bearer = "tgr2.secret_payload_marker.secret_signature_marker";

    expect(() =>
      runtime.authenticateHttpRequest(
        {
          headers: { authorization: `Bearer ${bearer}` },
          method: "GET",
        } as IncomingMessage,
        new URL("http://localhost/sessions"),
      ),
    ).toThrowError(new Error(AuthError.Malformed));

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

function collectWarnings(warnings: AuthWarning[]): AuthRuntimeLogger {
  return {
    warn: (event, details) => {
      warnings.push({ details, event });
    },
  };
}
