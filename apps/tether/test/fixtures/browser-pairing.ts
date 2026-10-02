import type { OperatorGrantScope } from "@dungle-scrubs/tether-protocol";
import { vi } from "vitest";

import type { BrowserPairingStore } from "../../src/auth/browser-pairing-stores.js";

/** Builds one provider-neutral operator grant scope for pairing and operator tests. */
export function createScope(overrides: Partial<OperatorGrantScope> = {}): OperatorGrantScope {
  return {
    actions: ["approve"],
    commands: ["scan"],
    permissions: ["approval.submit", "session.read"],
    scopeKeys: ["account-primary:inbox"],
    sessionIds: ["sess_email"],
    targetKinds: ["message"],
    ...overrides,
  };
}

/** Builds a fully stubbed pairing store whose individual methods can be overridden. */
export function createStore(overrides: Partial<BrowserPairingStore> = {}): BrowserPairingStore {
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
