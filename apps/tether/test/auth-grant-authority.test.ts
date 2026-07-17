import { describe, expect, it, vi } from "vitest";

import { AuthGrantAuthorityError, createAuthGrantAuthority } from "../src/auth/grant-authority.js";
import type { AuthGrantRecord, AuthGrantStore } from "../src/auth/grant-stores.js";
import { mintAuthGrantToken } from "../src/auth/grant-token.js";

const issuer = "https://auth.authority.test";
const kid = "current";
const secret = "authority-test-secret";
const issuedAt = new Date("2026-07-17T00:00:00.000Z");
const expiresAt = new Date("2026-07-18T00:00:00.000Z");

describe("durable grant authority", () => {
  it("validates signed claims against PostgreSQL on every positive authorization", async () => {
    const grant = createGrant();
    const store = createStore(grant);
    const authority = createAuthGrantAuthority({
      issuer,
      now: () => new Date("2026-07-17T01:00:00.000Z"),
      secrets: { [kid]: secret },
      store,
    });

    const first = await authority.authenticateRestBearer(createBearer());
    const second = await authority.authenticateRestBearer(createBearer());

    expect(first).toMatchObject({
      grantJti: grant.jti,
      participantId: grant.subject,
      role: grant.role,
      sessionScope: grant.sessionScope,
    });
    expect(second).toEqual(first);
    expect(store.findByJti).toHaveBeenCalledTimes(2);
  });

  it("reauthorizes a ticket-derived parent grant without bearer claim state", async () => {
    const grant = createGrant();
    const store = createStore(grant);
    const authority = createAuthority(store);

    const first = await authority.authenticateGrantJti(grant.jti);
    const second = await authority.authenticateGrantJti(grant.jti);

    expect(first).toMatchObject({
      grantJti: grant.jti,
      participantId: grant.subject,
      role: grant.role,
      sessionScope: grant.sessionScope,
    });
    expect(second).toEqual(first);
    expect(store.findByJti).toHaveBeenCalledTimes(2);
  });

  it("fails closed with bounded reasons for mismatched, revoked, and unavailable authority", async () => {
    const mismatchedStore = createStore({
      ...createGrant(),
      subject: "different-subject",
    });
    await expect(
      createAuthority(mismatchedStore).authenticateRestBearer(createBearer()),
    ).rejects.toMatchObject({ code: "auth_claim_invalid" });

    const revokedStore = createStore({
      ...createGrant(),
      revokedAt: new Date("2026-07-17T00:30:00.000Z"),
    });
    const revokedAuthority = createAuthority(revokedStore);
    await expect(revokedAuthority.authenticateRestBearer(createBearer())).rejects.toMatchObject({
      code: "auth_grant_revoked",
    });
    await expect(revokedAuthority.authenticateRestBearer(createBearer())).rejects.toMatchObject({
      code: "auth_grant_revoked",
    });
    expect(revokedStore.findByJti).toHaveBeenCalledTimes(1);

    const unavailableStore = createStore(null);
    vi.mocked(unavailableStore.findByJti).mockRejectedValue(new Error("database secret detail"));
    await expect(
      createAuthority(unavailableStore).authenticateRestBearer(createBearer()),
    ).rejects.toEqual(new AuthGrantAuthorityError("auth_store_unavailable"));
  });

  it.each([
    ["audience", { audience: "tether-websocket" as AuthGrantRecord["audience"] }],
    ["expiration", { expiresAt: new Date("2026-07-18T00:00:01.000Z") }],
    ["issued-at", { issuedAt: new Date("2026-07-17T00:00:01.000Z") }],
    ["issuer", { issuer: "https://different-authority.test" }],
    ["jti", { jti: "grant_different" }],
    ["kid", { kid: "different" }],
    ["role", { role: "observer" as const }],
    ["session scope", { sessionScope: "sess_different" }],
    ["subject", { subject: "part_different" }],
  ])("rejects a durable %s claim mismatch", async (_label, override) => {
    const store = createStore({ ...createGrant(), ...override });

    await expect(
      createAuthority(store).authenticateRestBearer(createBearer()),
    ).rejects.toMatchObject({ code: "auth_claim_invalid" });
  });

  it("caps denial entries, evicts oldest first, and expires entries with the grant", async () => {
    let currentTime = new Date("2026-07-17T01:00:00.000Z");
    const grants = new Map(
      ["grant_first", "grant_second", "grant_third"].map((jti) => [
        jti,
        {
          ...createGrant(jti),
          revokedAt: new Date("2026-07-17T00:30:00.000Z"),
        },
      ]),
    );
    const store: AuthGrantStore = {
      findByJti: vi.fn(async (jti) => grants.get(jti) ?? null),
      list: async () => [],
    };
    const authority = createAuthGrantAuthority({
      issuer,
      maximumNegativeCacheEntries: 2,
      now: () => currentTime,
      secrets: { [kid]: secret },
      store,
    });

    for (const jti of grants.keys()) {
      await expect(authority.authenticateRestBearer(createBearer(jti))).rejects.toMatchObject({
        code: "auth_grant_revoked",
      });
    }
    expect(authority.debugInfo()).toEqual({
      negativeCacheEntries: 2,
      negativeCacheMaximumEntries: 2,
    });

    await expect(
      authority.authenticateRestBearer(createBearer("grant_first")),
    ).rejects.toMatchObject({ code: "auth_grant_revoked" });
    expect(store.findByJti).toHaveBeenCalledTimes(4);

    currentTime = expiresAt;
    expect(authority.debugInfo()).toEqual({
      negativeCacheEntries: 0,
      negativeCacheMaximumEntries: 2,
    });
  });

  it("rejects a removed signing key before consulting durable authority", async () => {
    const store = createStore(createGrant());
    const authority = createAuthGrantAuthority({
      issuer,
      now: () => new Date("2026-07-17T01:00:00.000Z"),
      secrets: {},
      store,
    });

    await expect(authority.authenticateRestBearer(createBearer())).rejects.toMatchObject({
      code: "auth_claim_invalid",
    });
    expect(store.findByJti).not.toHaveBeenCalled();
  });
});

function createAuthority(store: AuthGrantStore) {
  return createAuthGrantAuthority({
    issuer,
    now: () => new Date("2026-07-17T01:00:00.000Z"),
    secrets: { [kid]: secret },
    store,
  });
}

function createStore(record: AuthGrantRecord | null): AuthGrantStore {
  return {
    findByJti: vi.fn(async () => record),
    list: async () => [],
  };
}

function createBearer(jti = "grant_authority_test"): string {
  return mintAuthGrantToken(
    {
      issuer,
      jti,
      kid,
      role: "participant",
      sessionScope: "sess_authority",
      subject: "part_authority",
    },
    { [kid]: secret },
    { now: issuedAt },
  );
}

function createGrant(jti = "grant_authority_test"): AuthGrantRecord {
  return {
    audience: "tether-rest",
    expiresAt,
    issuedAt,
    issuer,
    jti,
    kid,
    metadata: { requestId: "req_authority", source: "admin" },
    revokedAt: null,
    role: "participant",
    sessionScope: "sess_authority",
    subject: "part_authority",
  };
}
