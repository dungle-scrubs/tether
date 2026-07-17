import { describe, expect, it, vi } from "vitest";

import { createAuthGrantAuthority } from "../src/auth/grant-authority.js";
import type {
  AuthGrantRecord,
  AuthGrantStore,
  AuthTicketRecord,
  AuthTicketStore,
} from "../src/auth/grant-stores.js";
import { createAuthTicketAuthority } from "../src/auth/ticket-authority.js";
import { hashAuthTicket } from "../src/auth/ticket-lifecycle.js";

const now = new Date("2026-07-17T01:00:00.000Z");

describe("authentication ticket authority", () => {
  it("consumes once, reauthorizes the parent per command, and ignores ticket expiry after admission", async () => {
    let currentTime = now;
    const ticketStore = createTicketStore(createTicket());
    const grantStore = createGrantStore(createGrant());
    const authority = createAuthTicketAuthority({
      grantAuthority: createGrantAuthority(grantStore, () => currentTime),
      now: () => currentTime,
      store: ticketStore,
    });

    const authenticated = await authority.authenticateTicket("A".repeat(43));
    currentTime = new Date("2026-07-17T01:00:31.000Z");
    await expect(authenticated.authorizeCommand()).resolves.toBeUndefined();

    expect(authenticated.context).toMatchObject({
      grantJti: "grant_ticket_parent",
      participantId: "part_ticket_parent",
    });
    expect(ticketStore.consume).toHaveBeenCalledOnce();
    expect(grantStore.findByJti).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["consumed", { ...createTicket(), consumedAt: now }, "auth_ticket_consumed"],
    [
      "expired",
      { ...createTicket(), expiresAt: new Date("2026-07-17T01:00:00.000Z") },
      "auth_ticket_expired",
    ],
    [
      "wrong audience",
      {
        ...createTicket(),
        audience: "tether-rest" as AuthTicketRecord["audience"],
      },
      "auth_claim_invalid",
    ],
  ])("classifies a non-consumable %s ticket without another mutation", async (_label, ticket, code) => {
    const ticketStore = createTicketStore(null, ticket);
    const grantStore = createGrantStore(createGrant());
    const authority = createAuthTicketAuthority({
      grantAuthority: createGrantAuthority(grantStore),
      now: () => now,
      store: ticketStore,
    });

    await expect(authority.authenticateTicket("A".repeat(43))).rejects.toMatchObject({ code });
    expect(ticketStore.consume).toHaveBeenCalledOnce();
    expect(ticketStore.findByHash).toHaveBeenCalledOnce();
    expect(grantStore.findByJti).not.toHaveBeenCalled();
  });

  it("burns a consumed ticket when its parent signing key has been removed", async () => {
    let consumed = false;
    const ticket = createTicket();
    const ticketStore: AuthTicketStore = {
      consume: vi.fn(async () => {
        if (consumed) return null;
        consumed = true;
        return { ...ticket, consumedAt: now };
      }),
      create: vi.fn(async () => undefined),
      findByHash: vi.fn(async () => (consumed ? { ...ticket, consumedAt: now } : ticket)),
    };
    const authority = createAuthTicketAuthority({
      grantAuthority: createGrantAuthority(createGrantStore(createGrant()), () => now, {}),
      now: () => now,
      store: ticketStore,
    });

    await expect(authority.authenticateTicket("A".repeat(43))).rejects.toMatchObject({
      code: "auth_claim_invalid",
    });
    await expect(authority.authenticateTicket("A".repeat(43))).rejects.toMatchObject({
      code: "auth_ticket_consumed",
    });
    expect(ticketStore.consume).toHaveBeenCalledTimes(2);
  });

  it("classifies an unconsumable ticket through current parent grant authority", async () => {
    const ticketStore = createTicketStore(null);
    const grantStore = createGrantStore({ ...createGrant(), revokedAt: now });
    const authority = createAuthTicketAuthority({
      grantAuthority: createGrantAuthority(grantStore),
      now: () => now,
      store: ticketStore,
    });

    await expect(authority.authenticateTicket("A".repeat(43))).rejects.toMatchObject({
      code: "auth_grant_revoked",
    });
    expect(ticketStore.consume).toHaveBeenCalledOnce();
    expect(ticketStore.findByHash).toHaveBeenCalledOnce();
    expect(grantStore.findByJti).toHaveBeenCalledOnce();
  });

  it("classifies an expired parent without mutating the ticket after the conditional consume misses", async () => {
    const ticketStore = createTicketStore(null);
    const grantStore = createGrantStore({
      ...createGrant(),
      expiresAt: now,
    });
    const authority = createAuthTicketAuthority({
      grantAuthority: createGrantAuthority(grantStore),
      now: () => now,
      store: ticketStore,
    });

    await expect(authority.authenticateTicket("A".repeat(43))).rejects.toMatchObject({
      code: "auth_grant_expired",
    });
    expect(ticketStore.consume).toHaveBeenCalledOnce();
    expect(ticketStore.findByHash).toHaveBeenCalledOnce();
    expect(grantStore.findByJti).toHaveBeenCalledOnce();
  });

  it("fails closed with a bounded reason when ticket persistence is unavailable", async () => {
    const ticketStore: AuthTicketStore = {
      consume: vi.fn(async () => {
        throw new Error("database details must not escape");
      }),
      create: vi.fn(async () => undefined),
      findByHash: vi.fn(async () => null),
    };
    const authority = createAuthTicketAuthority({
      grantAuthority: createGrantAuthority(createGrantStore(createGrant())),
      now: () => now,
      store: ticketStore,
    });

    await expect(authority.authenticateTicket("A".repeat(43))).rejects.toEqual(
      expect.objectContaining({
        code: "auth_store_unavailable",
        message: "auth_store_unavailable",
      }),
    );
    expect(ticketStore.findByHash).not.toHaveBeenCalled();
  });
});

function createGrantAuthority(
  store: AuthGrantStore,
  clock: () => Date = () => now,
  secrets: Readonly<Record<string, string>> = { current: "secret" },
) {
  return createAuthGrantAuthority({
    issuer: "https://auth.ticket.test",
    now: clock,
    secrets,
    store,
  });
}

function createGrantStore(record: AuthGrantRecord): AuthGrantStore {
  return { findByJti: vi.fn(async () => record), list: vi.fn(async () => []) };
}

function createTicketStore(
  consumed: AuthTicketRecord | null,
  found: AuthTicketRecord = createTicket(),
): AuthTicketStore {
  return {
    consume: vi.fn(async () => consumed),
    create: vi.fn(async () => undefined),
    findByHash: vi.fn(async () => found),
  };
}

function createTicket(): AuthTicketRecord {
  return {
    admissionMetadata: {
      remoteAddressHash: null,
      replicaId: "replica_ticket_test",
      transport: "websocket",
    },
    audience: "tether-websocket",
    consumedAt: null,
    createdAt: now,
    expiresAt: new Date("2026-07-17T01:00:30.000Z"),
    parentGrantJti: "grant_ticket_parent",
    ticketHash: hashAuthTicket("A".repeat(43)),
  };
}

function createGrant(): AuthGrantRecord {
  return {
    audience: "tether-rest",
    expiresAt: new Date("2026-07-18T00:00:00.000Z"),
    issuedAt: new Date("2026-07-17T00:00:00.000Z"),
    issuer: "https://auth.ticket.test",
    jti: "grant_ticket_parent",
    kid: "current",
    metadata: { requestId: null, source: "admin" },
    revokedAt: null,
    role: "participant",
    sessionScope: "sess_ticket",
    subject: "part_ticket_parent",
  };
}
