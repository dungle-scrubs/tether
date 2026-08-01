import { createHash } from "node:crypto";

import type { OperatorGrantScope } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it, vi } from "vitest";

import { createBrowserPairingLifecycle } from "../src/auth/browser-pairing.js";
import type {
  BrowserPairingRequestRecord,
  BrowserPairingStore,
} from "../src/auth/browser-pairing-stores.js";

const now = new Date("2026-08-01T02:00:00.000Z");
const exchangeSecret = "E".repeat(43);
const csrfToken = "C".repeat(43);

describe("browser pairing lifecycle", () => {
  it("returns a one-time 256-bit secret while persisting only its SHA-256 hash", async () => {
    let stored: BrowserPairingRequestRecord | null = null;
    const create = vi.fn<BrowserPairingStore["create"]>(async ({ request }) => {
      stored = request;
      return { request, status: "created" };
    });
    const store = createStore({
      create,
    });
    const lifecycle = createBrowserPairingLifecycle({
      activeKid: "current",
      issuer: "https://auth.pairing.test",
      now: () => now,
      randomCsrfToken: () => csrfToken,
      randomExchangeSecret: () => exchangeSecret,
      randomPhrase: () => "amber cedar orbit",
      secrets: { current: "pairing-signing-secret" },
      store,
    });

    const created = await lifecycle.create({
      operatorSubject: "operator@example.test",
      origin: "https://hub.example.test",
      publicNonce: "N".repeat(22),
      requestedScope: createScope(),
      sourceAddress: "127.0.0.1",
    });

    expect(created.exchangeSecret).toBe(exchangeSecret);
    expect(created.request).not.toHaveProperty("exchangeSecretHash");
    expect(stored).toMatchObject({
      exchangeSecretHash: createHash("sha256").update(exchangeSecret).digest("hex"),
      failedAttempts: 0,
      origin: "https://hub.example.test",
    });
    expect(JSON.stringify(stored)).not.toContain(exchangeSecret);
  });

  it("builds grant authority from the request locked by the exchange transaction", async () => {
    const lockedRequest: BrowserPairingRequestRecord = {
      confirmedAt: now,
      confirmedBySubject: "confirming-admin@example.test",
      createdAt: now,
      exchangeSecretHash: createHash("sha256").update(exchangeSecret).digest("hex"),
      exchangedAt: null,
      expiresAt: new Date(now.getTime() + 60_000),
      failedAttempts: 0,
      invalidatedAt: null,
      operatorSubject: "locked-operator@example.test",
      origin: "https://hub.example.test",
      publicNonce: "N".repeat(22),
      requestId: "pair_locked",
      requestedScope: createScope(),
      sourceAddressHash: null,
      verificationPhrase: "amber cedar orbit",
    };
    let builtGrant: ReturnType<
      Parameters<BrowserPairingStore["exchange"]>[0]["buildGrant"]
    > | null = null;
    const store = createStore({
      exchange: vi.fn<BrowserPairingStore["exchange"]>(async (input) => {
        builtGrant = input.buildGrant(lockedRequest);
        return { request: lockedRequest, status: "exchanged" };
      }),
      inspect: vi.fn(async () => ({
        ...lockedRequest,
        confirmedBySubject: "stale-inspection@example.test",
        operatorSubject: "stale-operator@example.test",
      })),
    });
    const lifecycle = createBrowserPairingLifecycle({
      activeKid: "current",
      issuer: "https://auth.pairing.test",
      now: () => now,
      randomCsrfToken: () => csrfToken,
      secrets: { current: "pairing-signing-secret" },
      store,
    });

    const exchanged = await lifecycle.exchange(
      lockedRequest.requestId,
      { exchangeSecret, publicNonce: lockedRequest.publicNonce },
      { origin: lockedRequest.origin, sourceAddress: "127.0.0.1" },
    );

    expect(exchanged.scope).toEqual(lockedRequest.requestedScope);
    expect(builtGrant).toMatchObject({
      audit: { actorSubject: "confirming-admin@example.test" },
      grant: { subject: "locked-operator@example.test" },
      session: { origin: "https://hub.example.test" },
    });
    expect(store.inspect).not.toHaveBeenCalled();
  });
});

function createScope(): OperatorGrantScope {
  return {
    actions: ["approve"],
    commands: ["scan"],
    permissions: ["approval.submit", "session.read"],
    scopeKeys: ["account-primary:inbox"],
    sessionIds: ["sess_email"],
    targetKinds: ["message"],
  };
}

function createStore(overrides: Partial<BrowserPairingStore> = {}): BrowserPairingStore {
  return {
    confirm: vi.fn<BrowserPairingStore["confirm"]>(async () => ({ status: "not_found" })),
    create: vi.fn<BrowserPairingStore["create"]>(async ({ request }) => ({
      request,
      status: "created",
    })),
    exchange: vi.fn<BrowserPairingStore["exchange"]>(async () => ({ status: "not_found" })),
    findBrowserAuthority: vi.fn(async () => null),
    findBrowserSession: vi.fn(async () => null),
    inspect: vi.fn(async () => null),
    ...overrides,
  };
}
