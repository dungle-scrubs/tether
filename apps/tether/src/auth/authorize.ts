import { type AuthContext, AuthError } from "./token.js";

export type AuthAction =
  | "admin"
  | "client-binding"
  | "publish"
  | "read"
  | "scheduled-supersede"
  | "session-create"
  | "task-mutate";

export interface AuthorizeInput {
  /** Authenticated context. Null means auth is explicitly disabled. */
  readonly context: AuthContext | null;
  /** Operation class being authorized. */
  readonly action: AuthAction;
  /** Session id touched by the operation, when known. */
  readonly sessionId?: string | undefined;
}

/** Owns Tether role and session-scope authorization decisions. */
export function authorize(input: AuthorizeInput): AuthError | null {
  const { action, context, sessionId } = input;
  if (!context) {
    return null;
  }
  if (action === "admin") {
    // Admin authority is confined by the grant's session scope. Session debug
    // routes pass their sessionId, so a session-scoped admin grant reads only
    // its own session. Global admin surfaces (grant lifecycle, /debug/server,
    // /ui) pass no sessionId and therefore require a service-scoped (`*`)
    // admin grant; a session-scoped admin receives the typed scope denial.
    if (context.role !== "admin") {
      return AuthError.RoleDenied;
    }
    return authorizeSessionScope(context, sessionId);
  }
  if (action === "scheduled-supersede") {
    // Scheduled-run supersession is operator backlog reconciliation, not a
    // generic task mutation. It requires the admin (operator) role plus session
    // scope, so a scheduler or participant task-mutate credential is rejected.
    if (context.role !== "admin") {
      return AuthError.RoleDenied;
    }
    return authorizeSessionScope(context, sessionId);
  }
  if (action === "session-create" || action === "client-binding") {
    if (context.role === "observer") {
      return AuthError.RoleDenied;
    }
    return isServiceScoped(context) ? null : AuthError.ScopeDenied;
  }
  if (action === "publish" || action === "task-mutate") {
    if (context.role === "observer") {
      return AuthError.RoleDenied;
    }
    return authorizeSessionScope(context, sessionId);
  }
  return authorizeSessionScope(context, sessionId);
}

/** Verifies that a client-supplied participant id cannot override authenticated identity. */
export function authorizeParticipantIdentity(
  context: AuthContext | null,
  participantId: string | null | undefined,
): AuthError | null {
  if (!context || participantId === null || participantId === undefined) {
    return null;
  }
  return participantId === context.participantId ? null : AuthError.ScopeDenied;
}

/** Reads the effective participant id for a mutation after auth identity binding. */
export function effectiveParticipantId(
  context: AuthContext | null,
  participantId: string | null | undefined,
): string {
  if (context) {
    return context.participantId;
  }
  if (!participantId) {
    throw new Error("Participant id is required when auth is disabled");
  }
  return participantId;
}

/** Checks whether a context has service-wide scope. */
export function isServiceScoped(context: AuthContext): boolean {
  return context.sessionScope === "*";
}

/** Verifies session scope for a known session id. */
function authorizeSessionScope(
  context: AuthContext,
  sessionId: string | undefined,
): AuthError | null {
  if (!sessionId) {
    return isServiceScoped(context) ? null : AuthError.ScopeDenied;
  }
  return isServiceScoped(context) || context.sessionScope === sessionId
    ? null
    : AuthError.ScopeDenied;
}
