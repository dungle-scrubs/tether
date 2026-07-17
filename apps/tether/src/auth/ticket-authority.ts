import type { AuthGrantAuthority } from "./grant-authority.js";
import type { AuthTicketRecord, AuthTicketStore } from "./grant-stores.js";
import { hashAuthTicket } from "./ticket-lifecycle.js";
import type { AuthContext } from "./token.js";

export const authTicketAuthorityErrorCodes = [
  "auth_claim_invalid",
  "auth_ticket_consumed",
  "auth_ticket_expired",
  "auth_store_unavailable",
] as const;

/** Bounded admission failure safe for transport responses and logs. */
export type AuthTicketAuthorityErrorCode = (typeof authTicketAuthorityErrorCodes)[number];

/** Typed ticket admission error without raw credential or persistence causes. */
export class AuthTicketAuthorityError extends Error {
  readonly code: AuthTicketAuthorityErrorCode;

  constructor(code: AuthTicketAuthorityErrorCode) {
    super(code);
    this.name = "AuthTicketAuthorityError";
    this.code = code;
  }
}

/** Ticket-derived socket identity plus parent-only command reauthorization. */
export interface AuthenticatedTicketSession {
  readonly authorizeCommand: () => Promise<void>;
  readonly context: AuthContext;
}

/** Single-use ticket admission boundary. */
export interface AuthTicketAuthority {
  /** Atomically consumes an opaque ticket before returning socket identity. */
  readonly authenticateTicket: (ticket: string) => Promise<AuthenticatedTicketSession>;
}

/** Dependencies for ticket consumption and parent grant authority. */
export interface AuthTicketAuthorityOptions {
  readonly grantAuthority: AuthGrantAuthority;
  readonly now?: () => Date;
  readonly store: AuthTicketStore;
}

/** Creates fail-closed single-use admission without retaining the ticket after consume. */
export function createAuthTicketAuthority(
  options: AuthTicketAuthorityOptions,
): AuthTicketAuthority {
  const now = options.now ?? (() => new Date());
  return {
    authenticateTicket: async (ticket) => {
      if (!/^[A-Za-z0-9_-]{43}$/u.test(ticket)) {
        throw new AuthTicketAuthorityError("auth_claim_invalid");
      }
      const ticketHash = hashAuthTicket(ticket);
      const consumed = await consumeTicket(options.store, ticketHash);
      if (consumed === null) {
        const existing = await findTicket(options.store, ticketHash);
        const eligibleTicket = validateNonConsumableTicket(existing, ticketHash, now());
        await options.grantAuthority.authenticateGrantJti(eligibleTicket.parentGrantJti);
        throw new AuthTicketAuthorityError("auth_claim_invalid");
      }
      if (!isExpectedTicket(consumed, ticketHash)) {
        throw new AuthTicketAuthorityError("auth_claim_invalid");
      }
      const parentGrantJti = consumed.parentGrantJti;
      const context = await options.grantAuthority.authenticateGrantJti(parentGrantJti);
      return createAuthenticatedParentSession(options.grantAuthority, context, parentGrantJti);
    },
  };
}

async function consumeTicket(
  store: AuthTicketStore,
  ticketHash: string,
): Promise<AuthTicketRecord | null> {
  try {
    return await store.consume(ticketHash);
  } catch {
    throw new AuthTicketAuthorityError("auth_store_unavailable");
  }
}

async function findTicket(
  store: AuthTicketStore,
  ticketHash: string,
): Promise<AuthTicketRecord | null> {
  try {
    return await store.findByHash(ticketHash);
  } catch {
    throw new AuthTicketAuthorityError("auth_store_unavailable");
  }
}

function validateNonConsumableTicket(
  ticket: AuthTicketRecord | null,
  ticketHash: string,
  at: Date,
): AuthTicketRecord {
  if (!isExpectedTicket(ticket, ticketHash)) {
    throw new AuthTicketAuthorityError("auth_claim_invalid");
  }
  if (ticket.consumedAt !== null) {
    throw new AuthTicketAuthorityError("auth_ticket_consumed");
  }
  if (ticket.expiresAt.getTime() <= at.getTime()) {
    throw new AuthTicketAuthorityError("auth_ticket_expired");
  }
  return ticket;
}

function isExpectedTicket(
  ticket: AuthTicketRecord | null,
  ticketHash: string,
): ticket is AuthTicketRecord {
  return (
    ticket !== null && ticket.audience === "tether-websocket" && ticket.ticketHash === ticketHash
  );
}

function createAuthenticatedParentSession(
  authority: AuthGrantAuthority,
  context: AuthContext,
  parentGrantJti: string,
): AuthenticatedTicketSession {
  return {
    authorizeCommand: async () => {
      await authority.authenticateGrantJti(parentGrantJti);
    },
    context,
  };
}
