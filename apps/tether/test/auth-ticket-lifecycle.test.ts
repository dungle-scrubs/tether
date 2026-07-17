import { createHash } from "node:crypto";

import { describe, expect, it, vi } from "vitest";

import { createAuthTicketLifecycle } from "../src/auth/ticket-lifecycle.js";
import type { AuthTicketStore } from "../src/auth/grant-stores.js";
import type { AuthContext } from "../src/auth/token.js";

describe("authentication ticket lifecycle", () => {
  it("returns one opaque 30-second ticket while persisting only its SHA-256 hash", async () => {
    const ticket = "A".repeat(43);
    const store: AuthTicketStore = {
      consume: vi.fn(async () => null),
      create: vi.fn(async () => undefined),
      findByHash: vi.fn(async () => null),
    };
    const lifecycle = createAuthTicketLifecycle({
      now: () => new Date("2026-07-17T01:00:00.000Z"),
      randomTicket: () => ticket,
      replicaId: "replica_ticket_test",
      store,
    });

    const created = await lifecycle.mint(createContext());

    expect(created).toEqual({
      expiresAt: "2026-07-17T01:00:30.000Z",
      ticket,
    });
    expect(store.create).toHaveBeenCalledWith({
      admissionMetadata: {
        remoteAddressHash: null,
        replicaId: "replica_ticket_test",
        transport: "websocket",
      },
      audience: "tether-websocket",
      consumedAt: null,
      createdAt: new Date("2026-07-17T01:00:00.000Z"),
      expiresAt: new Date("2026-07-17T01:00:30.000Z"),
      parentGrantJti: "grant_ticket_parent",
      ticketHash: createHash("sha256").update(ticket).digest("hex"),
    });
    expect(JSON.stringify(vi.mocked(store.create).mock.calls)).not.toContain(ticket);
  });

  it("rejects compatibility credentials without a durable parent grant", async () => {
    const store: AuthTicketStore = {
      consume: vi.fn(async () => null),
      create: vi.fn(async () => undefined),
      findByHash: vi.fn(async () => null),
    };
    const lifecycle = createAuthTicketLifecycle({
      randomTicket: () => "A".repeat(43),
      replicaId: "replica_ticket_test",
      store,
    });

    await expect(lifecycle.mint({ ...createContext(), grantJti: null })).rejects.toThrow(
      "auth_ticket_parent_required",
    );
    expect(store.create).not.toHaveBeenCalled();
  });
});

function createContext(): AuthContext {
  return {
    expiresAt: "2026-07-18T00:00:00.000Z",
    grantJti: "grant_ticket_parent",
    issuer: "https://auth.ticket.test",
    kid: "current",
    participantId: "part_ticket",
    role: "participant",
    sessionScope: "sess_ticket",
  };
}
