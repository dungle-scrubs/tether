import type { IncomingMessage } from "node:http";

import { describe, expect, it, vi } from "vitest";

import {
  BrowserOperatorAuthorityError,
  type BrowserOperatorAuthorityErrorCode,
  createBrowserOperatorRuntime,
} from "../src/auth/browser-operator-runtime.js";
import { browserSessionCookieName } from "../src/auth/browser-pairing.js";
import type { BrowserPairingStore } from "../src/auth/browser-pairing-stores.js";
import type { AuthRuntime } from "../src/auth/enforcement.js";
import { hashOpaqueCredential } from "../src/auth/opaque-credential.js";
import { createStore as createPairingStore, createScope } from "./fixtures/browser-pairing.js";

const csrfToken = "C".repeat(43);
const bearer = "tgr2.payload.signature";

describe("browser operator runtime", () => {
  it("accepts only the dedicated cookie with exact Origin, grant-bound CSRF, and scope", async () => {
    const auth = createAuthRuntimeFake();
    const runtime = createBrowserOperatorRuntime({ auth, store: createStore() });

    await expect(
      runtime.authorize(
        createRequest({
          cookie: `${browserSessionCookieName}=${bearer}`,
          origin: "https://hub.example.test",
          csrf: csrfToken,
        }),
        {
          csrfRequired: true,
          resource: {
            permission: "approval.submit",
            scopeKey: "account-primary:inbox",
            sessionId: "sess_email",
          },
        },
      ),
    ).resolves.toMatchObject({
      context: { grantJti: "grant_browser", role: "observer" },
      session: { origin: "https://hub.example.test" },
    });
    expect(auth.authenticateBrowserSessionToken).toHaveBeenCalledWith(bearer);
  });

  it("allows a same-origin safe read when Fetch omits the Origin header", async () => {
    const runtime = createBrowserOperatorRuntime({
      auth: createAuthRuntimeFake(),
      store: createStore(),
    });

    await expect(
      runtime.authorize(createRequest({ cookie: `${browserSessionCookieName}=${bearer}` }), {
        csrfRequired: false,
        resource: { permission: "browser-session.read" },
      }),
    ).resolves.toMatchObject({ context: { grantJti: "grant_browser" } });
  });

  const deniedHeaders: readonly [
    Parameters<typeof createRequest>[0],
    BrowserOperatorAuthorityErrorCode,
  ][] = [
    [{}, "operator_cookie_missing"],
    [{ cookie: `${browserSessionCookieName}=${bearer}` }, "operator_origin_denied"],
    [
      { cookie: `${browserSessionCookieName}=${bearer}`, origin: "https://evil.example.test" },
      "operator_origin_denied",
    ],
    [
      { cookie: `${browserSessionCookieName}=${bearer}`, origin: "https://hub.example.test" },
      "operator_csrf_denied",
    ],
  ];

  it.each(deniedHeaders)("fails closed with %s", async (headers, reason) => {
    const warn = vi.fn();
    const runtime = createBrowserOperatorRuntime({
      auth: createAuthRuntimeFake(),
      logger: { warn },
      store: createStore(),
    });

    await expect(
      runtime.authorize(createRequest(headers), {
        csrfRequired: true,
        resource: { permission: "approval.submit" },
      }),
    ).rejects.toEqual(new BrowserOperatorAuthorityError(reason));
    expect(warn).toHaveBeenCalledWith("browser.operator.denied", {
      csrfRequired: true,
      permission: "approval.submit",
      reason,
    });
    expect(JSON.stringify(warn.mock.calls)).not.toContain(bearer);
    expect(JSON.stringify(warn.mock.calls)).not.toContain(csrfToken);
  });
});

function createAuthRuntimeFake(): AuthRuntime {
  return {
    authenticateBrowserSessionToken: vi.fn<AuthRuntime["authenticateBrowserSessionToken"]>(
      async () => ({
        expiresAt: "2026-08-02T00:00:00.000Z",
        grantJti: "grant_browser",
        grantSource: "browser",
        issuer: "https://auth.operator.test",
        kid: "current",
        participantId: "operator@example.test",
        role: "observer",
        sessionScope: "sess_email",
      }),
    ),
    authenticateHttpRequest: vi.fn<AuthRuntime["authenticateHttpRequest"]>(async () => null),
    authenticateWebSocketUpgrade: vi.fn<AuthRuntime["authenticateWebSocketUpgrade"]>(async () => ({
      authorizeCommand: async () => undefined,
      context: null,
    })),
    close: vi.fn(),
    debugInfo: vi.fn<AuthRuntime["debugInfo"]>(() => ({
      acceptedKids: ["current"],
      activeKid: "current",
      authMode: "required",
      grantIssuanceEnabled: true,
      legacyTokensAllowed: false,
      negativeGrantCacheEntries: 0,
      negativeGrantCacheMaximumEntries: 0,
    })),
  };
}

function createRequest(headers: {
  readonly cookie?: string;
  readonly csrf?: string;
  readonly origin?: string;
}): IncomingMessage {
  return {
    headers: {
      ...(headers.cookie === undefined ? {} : { cookie: headers.cookie }),
      ...(headers.csrf === undefined ? {} : { "x-tether-csrf": headers.csrf }),
      ...(headers.origin === undefined ? {} : { origin: headers.origin }),
    },
    method: "POST",
  } as IncomingMessage;
}

function createStore(): BrowserPairingStore {
  return createPairingStore({
    findBrowserAuthority: vi.fn<BrowserPairingStore["findBrowserAuthority"]>(async () => ({
      scope: createScope({ permissions: ["approval.submit", "browser-session.read"] }),
      session: {
        createdAt: new Date("2026-08-01T00:00:00.000Z"),
        csrfTokenHash: hashOpaqueCredential(csrfToken),
        grantJti: "grant_browser",
        origin: "https://hub.example.test",
      },
    })),
  });
}
