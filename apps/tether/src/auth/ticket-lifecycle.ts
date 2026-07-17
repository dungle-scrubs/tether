import { createHash, randomBytes } from "node:crypto";

import {
  maximumAuthTicketAdmissionLifetimeMilliseconds,
  type AuthTicketStore,
} from "./grant-stores.js";
import type { AuthContext } from "./token.js";

/** One-time browser-compatible WebSocket admission credential. */
export interface CreatedAuthTicket {
  /** Admission deadline. Ticket expiry does not govern an established socket. */
  readonly expiresAt: string;
  /** Opaque credential returned exactly once and never persisted. */
  readonly ticket: string;
}

/** Ticket minting boundary used by authenticated REST routes. */
export interface AuthTicketLifecycle {
  /** Mints and persists one hashed ticket for a durable parent grant. */
  readonly mint: (context: AuthContext) => Promise<CreatedAuthTicket>;
}

/** Dependencies for secret generation and hashed ticket persistence. */
export interface AuthTicketLifecycleOptions {
  /** Injectable lifecycle clock. */
  readonly now?: () => Date;
  /** Injectable cryptographically random ticket source. */
  readonly randomTicket?: () => string;
  /** Bounded identity of the replica that minted the ticket. */
  readonly replicaId: string;
  /** Hashed ticket persistence boundary. */
  readonly store: AuthTicketStore;
}

/** Creates the one-time-secret lifecycle without exposing raw tickets to persistence. */
export function createAuthTicketLifecycle(
  options: AuthTicketLifecycleOptions,
): AuthTicketLifecycle {
  const now = options.now ?? (() => new Date());
  const randomTicket = options.randomTicket ?? (() => randomBytes(32).toString("base64url"));
  return {
    mint: async (context) => {
      if (context.grantJti === null) throw new Error("auth_ticket_parent_required");
      const createdAt = now();
      const expiresAt = new Date(
        createdAt.getTime() + maximumAuthTicketAdmissionLifetimeMilliseconds,
      );
      const ticket = randomTicket();
      if (!/^[A-Za-z0-9_-]{43}$/u.test(ticket)) {
        throw new Error("auth_ticket_generation_failed");
      }
      await options.store.create({
        admissionMetadata: {
          remoteAddressHash: null,
          replicaId: options.replicaId,
          transport: "websocket",
        },
        audience: "tether-websocket",
        consumedAt: null,
        createdAt,
        expiresAt,
        parentGrantJti: context.grantJti,
        ticketHash: hashAuthTicket(ticket),
      });
      return { expiresAt: expiresAt.toISOString(), ticket };
    },
  };
}

/** Hashes an opaque ticket before it crosses a durable persistence boundary. */
export function hashAuthTicket(ticket: string): string {
  return createHash("sha256").update(ticket).digest("hex");
}
