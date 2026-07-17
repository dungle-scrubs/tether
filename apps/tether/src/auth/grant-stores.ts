import type { AuthAudience } from "./grant-token.js";
import type { AuthRole } from "./token.js";

export const authGrantAuditActions = ["grant.created", "grant.revoked"] as const;
export const authGrantAuditReasonCodes = [
  "bootstrap",
  "key-rotation",
  "migration",
  "operator-request",
  "security-response",
] as const;
export const authGrantSources = ["admin", "bootstrap", "migration"] as const;

/** Maximum single-use WebSocket admission window required by the protocol. */
export const maximumAuthTicketAdmissionLifetimeMilliseconds = 30_000;

/** Durable grant audit action names. */
export type AuthGrantAuditAction = (typeof authGrantAuditActions)[number];

/** Bounded audit reason codes that cannot carry arbitrary credential values. */
export type AuthGrantAuditReasonCode = (typeof authGrantAuditReasonCodes)[number];

/** Durable grant creation sources safe to persist. */
export type AuthGrantSource = (typeof authGrantSources)[number];

/** Bounded persistence failure codes that never include query parameters. */
export type AuthPersistenceErrorCode =
  | "auth_audit_list_failed"
  | "auth_audit_limit_invalid"
  | "auth_grant_create_failed"
  | "auth_grant_list_failed"
  | "auth_grant_limit_invalid"
  | "auth_grant_read_failed"
  | "auth_grant_revoke_failed"
  | "auth_metadata_invalid"
  | "auth_ticket_create_failed"
  | "auth_ticket_consume_failed"
  | "auth_ticket_hash_invalid"
  | "auth_ticket_read_failed";

/** Typed persistence error that deliberately omits database causes and parameters. */
export class AuthPersistenceError extends Error {
  readonly code: AuthPersistenceErrorCode;

  constructor(code: AuthPersistenceErrorCode) {
    super(code);
    this.name = "AuthPersistenceError";
    this.code = code;
  }
}

/** Explicit grant metadata. Arbitrary fields and credential-shaped values are rejected. */
export interface AuthGrantMetadata {
  readonly requestId: string | null;
  readonly source: AuthGrantSource;
}

/** Explicit audit metadata. The grant subject and actor already have dedicated columns. */
export interface AuthGrantAuditMetadata {
  readonly requestId: string | null;
}

/** Explicit admission metadata that stores only a hash for the remote address. */
export interface AuthTicketAdmissionMetadata {
  readonly remoteAddressHash: string | null;
  readonly replicaId: string;
  readonly transport: "websocket";
}

/** Durable grant authority as exposed outside the persistence adapter. */
export interface AuthGrantRecord {
  readonly audience: AuthAudience;
  readonly expiresAt: Date;
  readonly issuedAt: Date;
  readonly issuer: string;
  readonly jti: string;
  readonly kid: string;
  readonly metadata: AuthGrantMetadata;
  readonly revokedAt: Date | null;
  readonly role: AuthRole;
  readonly sessionScope: string;
  readonly subject: string;
}

/** Bounded durable audit event for one grant lifecycle change. */
export interface AuthGrantAuditRecord {
  readonly action: AuthGrantAuditAction;
  readonly actorSubject: string;
  readonly auditId: string;
  readonly grantJti: string;
  readonly metadata: AuthGrantAuditMetadata;
  readonly occurredAt: Date;
  readonly reasonCode: AuthGrantAuditReasonCode;
}

/** Audit fields accepted by one transaction-owning lifecycle operation. */
export type AuthGrantLifecycleAuditInput = Omit<AuthGrantAuditRecord, "grantJti">;

/** Hashed WebSocket admission ticket state. */
export interface AuthTicketRecord {
  readonly admissionMetadata: AuthTicketAdmissionMetadata;
  readonly audience: "tether-websocket";
  readonly consumedAt: Date | null;
  readonly createdAt: Date;
  readonly expiresAt: Date;
  readonly parentGrantJti: string;
  readonly ticketHash: string;
}

/** Input for atomically creating a grant and its required audit event. */
export interface CreateAuthGrantWithAuditInput {
  readonly audit: AuthGrantLifecycleAuditInput & {
    readonly action: "grant.created";
  };
  readonly grant: AuthGrantRecord;
}

/** Input for atomically revoking a grant and appending its required audit event. */
export interface RevokeAuthGrantWithAuditInput {
  readonly audit: AuthGrantLifecycleAuditInput & {
    readonly action: "grant.revoked";
  };
  readonly jti: string;
  readonly revokedAt: Date;
}

/** Observable result of an idempotent atomic revocation with its committed row. */
export type RevokeAuthGrantResult =
  | {
      readonly grant: AuthGrantRecord;
      readonly status: "already_revoked" | "revoked";
    }
  | { readonly grant: null; readonly status: "not_found" };

/** Narrow grant reads required by later authorization layers. */
export interface AuthGrantStore {
  /** Reads one durable grant by its public token id. */
  readonly findByJti: (jti: string) => Promise<AuthGrantRecord | null>;
  /** Lists a bounded newest-first page of durable grants. */
  readonly list: (limit: number) => Promise<readonly AuthGrantRecord[]>;
}

/** Cancellation-aware batch reads used only by proactive socket revocation repair. */
export interface AuthGrantRevocationStore {
  /** Reads one bounded grant batch and aborts any in-flight database work when signalled. */
  readonly findManyByJti: (
    grantJtis: readonly string[],
    options: {
      readonly signal: AbortSignal;
      readonly timeoutMs: number;
    },
  ) => Promise<readonly AuthGrantRecord[]>;
}

/** Narrow grant-audit reads. Lifecycle writes are transaction-owning operations. */
export interface AuthGrantAuditStore {
  /** Lists a bounded number of events for one grant. */
  readonly listForGrant: (
    grantJti: string,
    limit: number,
  ) => Promise<readonly AuthGrantAuditRecord[]>;
}

/** Narrow hashed-ticket persistence operations. */
export interface AuthTicketStore {
  /** Atomically consumes one eligible ticket, returning null when no row can transition. */
  readonly consume: (ticketHash: string) => Promise<AuthTicketRecord | null>;
  /** Inserts one validated ticket hash and its bounded admission state. */
  readonly create: (record: AuthTicketRecord) => Promise<void>;
  /** Reads one ticket by SHA-256 hash. */
  readonly findByHash: (ticketHash: string) => Promise<AuthTicketRecord | null>;
}

/** Deep auth persistence Interface with transaction ownership kept inside the module. */
export interface AuthPersistenceStores {
  readonly audits: AuthGrantAuditStore;
  /** Atomically inserts a grant and its required creation audit. */
  readonly createGrantWithAudit: (input: CreateAuthGrantWithAuditInput) => Promise<void>;
  readonly grants: AuthGrantRevocationStore & AuthGrantStore;
  /** Atomically and idempotently revokes a grant and appends its required audit. */
  readonly revokeGrantWithAudit: (
    input: RevokeAuthGrantWithAuditInput,
  ) => Promise<RevokeAuthGrantResult>;
  readonly tickets: AuthTicketStore;
}
