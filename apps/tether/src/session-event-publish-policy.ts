import { authorizeParticipantIdentity, effectiveParticipantId } from "./auth/authorize.js";
import type { AuthContext } from "./auth/token.js";
import { sessionEventType, systemProducerId } from "./protocol.js";

/**
 * Owns authorization for public generic event publish surfaces.
 *
 * This module does not authorize task, approval, participant, or session
 * mutations. Those state-machine operations must use their dedicated command
 * APIs so Tether remains the only producer of lifecycle events.
 */

export const clientPublishDenyReason = {
  IdentityMismatch: "identity_mismatch",
  ReservedProducer: "reserved_producer",
  ServerEventType: "server_event_type",
} as const;

export type ClientPublishDenyReason =
  (typeof clientPublishDenyReason)[keyof typeof clientPublishDenyReason];

export interface ClientPublishedEventPolicyInput {
  /** Authenticated identity, or null when auth is explicitly disabled. */
  readonly authContext: AuthContext | null;
  /** Participant identity already bound by the transport, when available. */
  readonly boundParticipantId?: string | null | undefined;
  /** Producer id requested by the client publish command or REST body. */
  readonly producerId: string;
  /** Event type requested by the client publish command or REST body. */
  readonly type: string;
}

export type ClientPublishedEventPolicyResult =
  | {
      readonly producerId: string;
      readonly status: "allowed";
    }
  | {
      readonly reason: ClientPublishDenyReason;
      readonly status: "denied";
    };

export const serverAuthoritativeSessionEventTypes: ReadonlySet<string> = new Set([
  sessionEventType.approvalRecorded,
  sessionEventType.controlCancel,
  sessionEventType.participantHeartbeat,
  sessionEventType.participantJoined,
  sessionEventType.participantUpdated,
  sessionEventType.sessionCreated,
  sessionEventType.taskClaimExpired,
  sessionEventType.taskClaimed,
  sessionEventType.taskCompleted,
  sessionEventType.taskCreated,
  sessionEventType.taskFailed,
  sessionEventType.taskReleased,
]);

/** Applies public generic event publish policy and returns the effective producer id. */
export function authorizeClientPublishedEvent(
  input: ClientPublishedEventPolicyInput,
): ClientPublishedEventPolicyResult {
  if (
    input.boundParticipantId !== null &&
    input.boundParticipantId !== undefined &&
    input.producerId !== input.boundParticipantId
  ) {
    return deny(clientPublishDenyReason.IdentityMismatch);
  }
  const authIdentityDenied = authorizeParticipantIdentity(input.authContext, input.producerId);
  if (authIdentityDenied) {
    return deny(clientPublishDenyReason.IdentityMismatch);
  }
  const producerId = input.authContext
    ? effectiveParticipantId(input.authContext, input.producerId)
    : (input.boundParticipantId ?? input.producerId);
  if (producerId === systemProducerId) {
    return deny(clientPublishDenyReason.ReservedProducer);
  }
  if (serverAuthoritativeSessionEventTypes.has(input.type)) {
    return deny(clientPublishDenyReason.ServerEventType);
  }
  return { producerId, status: "allowed" };
}

/** Builds a typed policy denial. */
function deny(reason: ClientPublishDenyReason): ClientPublishedEventPolicyResult {
  return { reason, status: "denied" };
}
