import { describe, expect, it, vi } from "vitest";

import { createAuthGrantLifecycle } from "../src/auth/grant-lifecycle.js";
import type { AuthGrantRecord, AuthPersistenceStores } from "../src/auth/grant-stores.js";
import { verifyAuthGrantToken } from "../src/auth/grant-token.js";
import {
  completeBootstrapOneTimeSecret,
  parseBootstrapAdminOptions,
} from "../src/auth/bootstrap-cli.js";

const secret = "test-secret-value-long-enough";

/** Builds a transaction-shaped in-memory persistence seam for lifecycle unit tests. */
function fakeStores(): AuthPersistenceStores & { readonly records: Map<string, AuthGrantRecord> } {
  const records = new Map<string, AuthGrantRecord>();
  return {
    audits: { listForGrant: async () => [] },
    createGrantWithAudit: vi.fn(async ({ grant }) => {
      records.set(grant.jti, grant);
    }),
    grants: {
      findByJti: async (jti) => records.get(jti) ?? null,
      list: async (limit) => [...records.values()].slice(0, limit),
    },
    records,
    revokeGrantWithAudit: vi.fn(async ({ jti, revokedAt }) => {
      const record = records.get(jti);
      if (!record) return { grant: null, status: "not_found" } as const;
      if (record.revokedAt) return { grant: record, status: "already_revoked" } as const;
      const revoked = { ...record, revokedAt };
      records.set(jti, revoked);
      return { grant: revoked, status: "revoked" } as const;
    }),
    tickets: { create: async () => undefined, findByHash: async () => null },
  };
}

describe("auth grant lifecycle", () => {
  it("mints once and persists a matching secret-free 24 hour grant", async () => {
    const stores = fakeStores();
    const now = new Date("2026-07-17T01:02:03.987Z");
    const lifecycle = createAuthGrantLifecycle({
      activeKid: "current",
      issuer: "https://auth.tether.test",
      issuanceEnabled: true,
      now: () => now,
      secrets: { current: secret },
      stores,
    });

    const created = await lifecycle.create({
      actorSubject: "operator",
      reasonCode: "operator-request",
      role: "participant",
      sessionScope: "sess_one",
      source: "admin",
      subject: "part_one",
    });
    const claims = verifyAuthGrantToken(created.bearer, {
      audience: "tether-rest",
      issuer: "https://auth.tether.test",
      now,
      secrets: { current: secret },
    });

    expect(claims.exp - claims.iat).toBe(86_400);
    expect(created.grant.issuedAt).toBe(new Date(claims.iat * 1_000).toISOString());
    expect(created.grant.expiresAt).toBe(new Date(claims.exp * 1_000).toISOString());
    expect(created.grant).not.toHaveProperty("metadata");
    expect(JSON.stringify(stores.records.get(created.grant.jti))).not.toContain(created.bearer);
  });

  it("rejects an over-seven-day grant before persistence", async () => {
    const stores = fakeStores();
    const lifecycle = createAuthGrantLifecycle({
      activeKid: "current",
      issuer: "https://auth.tether.test",
      issuanceEnabled: true,
      secrets: { current: secret },
      stores,
    });

    await expect(
      lifecycle.create({
        actorSubject: "operator",
        reasonCode: "operator-request",
        role: "admin",
        sessionScope: "*",
        source: "admin",
        subject: "admin",
        ttlSeconds: 604_801,
      }),
    ).rejects.toThrow("grant_token_lifetime");
    expect(stores.createGrantWithAudit).not.toHaveBeenCalled();
  });

  it("keeps provisional issuance default-off before touching persistence", async () => {
    const stores = fakeStores();
    const lifecycle = createAuthGrantLifecycle({
      activeKid: "current",
      issuer: "https://auth.tether.test",
      secrets: { current: secret },
      stores,
    });

    expect(lifecycle.issuanceEnabled).toBe(false);
    await expect(
      lifecycle.create({
        actorSubject: "operator",
        reasonCode: "operator-request",
        role: "admin",
        sessionScope: "*",
        source: "admin",
        subject: "admin",
      }),
    ).rejects.toThrow("auth_grant_issuance_unavailable");
    expect(stores.createGrantWithAudit).not.toHaveBeenCalled();
  });

  it("returns the transaction-owned revoked row without a follow-up read", async () => {
    const stores = fakeStores();
    const lifecycle = createAuthGrantLifecycle({
      activeKid: "current",
      issuer: "https://auth.tether.test",
      issuanceEnabled: true,
      secrets: { current: secret },
      stores,
    });
    const created = await lifecycle.create({
      actorSubject: "operator",
      reasonCode: "operator-request",
      role: "admin",
      sessionScope: "*",
      source: "admin",
      subject: "admin",
    });
    const read = vi.spyOn(stores.grants, "findByJti").mockRejectedValue(new Error("read failed"));

    await expect(
      lifecycle.revoke(created.grant.jti, "operator", "operator-request"),
    ).resolves.toMatchObject({ grant: { revokedAt: expect.any(String) }, status: "revoked" });
    expect(read).not.toHaveBeenCalled();
  });
});

describe("bootstrap admin options", () => {
  it("requires database and issuer state and bounds the lifetime", () => {
    const env = {
      AUTH_GRANT_BOOTSTRAP_COMPATIBILITY_CONFIRMED: "true",
      AUTH_ISSUER: "https://auth.tether.test",
      AUTH_SIGNING_SECRET: secret,
      DATABASE_URL: "postgres://localhost/tether",
    };
    expect(parseBootstrapAdminOptions(["--subject", "admin"], env)).toMatchObject({
      issuanceEnabled: true,
      kid: "default",
      subject: "admin",
      ttlSeconds: 86_400,
    });
    expect(() => parseBootstrapAdminOptions(["--subject", "admin", "--ttl", "8d"], env)).toThrow(
      "auth_bootstrap_ttl_invalid",
    );
    expect(() => parseBootstrapAdminOptions(["--subject", "admin"], {})).toThrow(
      "auth_bootstrap_database_url_required",
    );
  });

  it("emits a committed bearer once even when cleanup fails", async () => {
    const output: string[] = [];
    await expect(
      completeBootstrapOneTimeSecret({
        cleanup: async () => {
          throw new Error("cleanup failed");
        },
        issue: async () => '{"bearer":"tgr2.marker.signature"}',
        onCommitted: (value) => output.push(value),
      }),
    ).resolves.toContain("tgr2.marker.signature");
    expect(output).toEqual(['{"bearer":"tgr2.marker.signature"}']);
  });
});
