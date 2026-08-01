import type { OperatorGrantScope, OperatorPermission } from "@dungle-scrubs/tether-protocol";

/** Stable fail-closed reasons for browser operator scope denials. */
export type OperatorAuthorityDenialReason =
  | "operator_action_denied"
  | "operator_command_denied"
  | "operator_permission_denied"
  | "operator_scope_key_denied"
  | "operator_session_denied"
  | "operator_target_kind_denied";

/** One provider-neutral resource-action tuple checked against a durable grant scope. */
export interface OperatorAuthorityRequest {
  readonly action?: string;
  readonly command?: string;
  readonly permission: OperatorPermission;
  readonly scopeKey?: string;
  readonly sessionId?: string;
  readonly targetKind?: string;
}

/** Returns the first stable denial reason, or null when every supplied dimension is allowed. */
export function authorizeOperator(
  scope: OperatorGrantScope,
  request: OperatorAuthorityRequest,
): OperatorAuthorityDenialReason | null {
  if (!scope.permissions.includes(request.permission)) return "operator_permission_denied";
  if (request.sessionId !== undefined && !scope.sessionIds.includes(request.sessionId)) {
    return "operator_session_denied";
  }
  if (request.scopeKey !== undefined && !scope.scopeKeys.includes(request.scopeKey)) {
    return "operator_scope_key_denied";
  }
  if (request.targetKind !== undefined && !scope.targetKinds.includes(request.targetKind)) {
    return "operator_target_kind_denied";
  }
  if (request.action !== undefined && !scope.actions.includes(request.action)) {
    return "operator_action_denied";
  }
  if (request.command !== undefined && !scope.commands.includes(request.command)) {
    return "operator_command_denied";
  }
  return null;
}
