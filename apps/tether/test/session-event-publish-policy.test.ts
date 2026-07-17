import { describe, expect, it } from "vitest";

import type { AuthContext } from "../src/auth/token.js";
import {
  authorizeClientPublishedEvent,
  clientPublishDenyReason,
  serverAuthoritativeSessionEventTypes,
} from "../src/session-event-publish-policy.js";
import { sessionEventType, systemProducerId } from "../src/protocol.js";

const participantAuthContext = {
  expiresAt: "2099-01-01T00:00:00.000Z",
  grantJti: null,
  issuer: null,
  kid: "test",
  participantId: "part_authorized",
  role: "participant",
  sessionScope: "sess_policy",
} as const satisfies AuthContext;

describe("client published session event policy", () => {
  it("denies producer ids that do not match the authenticated participant", () => {
    expect(
      authorizeClientPublishedEvent({
        authContext: participantAuthContext,
        producerId: "part_other",
        type: sessionEventType.userMessage,
      }),
    ).toEqual({
      reason: clientPublishDenyReason.IdentityMismatch,
      status: "denied",
    });
  });

  it("denies the reserved system producer after identity binding", () => {
    expect(
      authorizeClientPublishedEvent({
        authContext: {
          ...participantAuthContext,
          participantId: systemProducerId,
        },
        producerId: systemProducerId,
        type: sessionEventType.userMessage,
      }),
    ).toEqual({
      reason: clientPublishDenyReason.ReservedProducer,
      status: "denied",
    });
  });

  it("denies server-authoritative event types", () => {
    const deniedTypes = [
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
    ] as const;

    expect([...serverAuthoritativeSessionEventTypes].sort()).toEqual([...deniedTypes].sort());
    for (const type of deniedTypes) {
      expect(
        authorizeClientPublishedEvent({
          authContext: participantAuthContext,
          producerId: participantAuthContext.participantId,
          type,
        }),
      ).toEqual({
        reason: clientPublishDenyReason.ServerEventType,
        status: "denied",
      });
    }
  });

  it("allows non-reserved protocol and custom event types", () => {
    for (const type of [
      sessionEventType.agentOutput,
      sessionEventType.taskProgress,
      sessionEventType.userMessage,
      "custom.client.event",
    ] as const) {
      expect(
        authorizeClientPublishedEvent({
          authContext: participantAuthContext,
          producerId: participantAuthContext.participantId,
          type,
        }),
      ).toEqual({
        producerId: participantAuthContext.participantId,
        status: "allowed",
      });
    }
  });

  it("keeps reserved producer and event-type denials active when auth is disabled", () => {
    expect(
      authorizeClientPublishedEvent({
        authContext: null,
        producerId: systemProducerId,
        type: sessionEventType.userMessage,
      }),
    ).toEqual({
      reason: clientPublishDenyReason.ReservedProducer,
      status: "denied",
    });
    expect(
      authorizeClientPublishedEvent({
        authContext: null,
        producerId: "part_local",
        type: sessionEventType.taskCompleted,
      }),
    ).toEqual({
      reason: clientPublishDenyReason.ServerEventType,
      status: "denied",
    });
  });

  it("uses a bound WebSocket participant as the transport identity", () => {
    expect(
      authorizeClientPublishedEvent({
        authContext: null,
        boundParticipantId: "part_ws_bound",
        producerId: "part_other",
        type: sessionEventType.userMessage,
      }),
    ).toEqual({
      reason: clientPublishDenyReason.IdentityMismatch,
      status: "denied",
    });
    expect(
      authorizeClientPublishedEvent({
        authContext: participantAuthContext,
        boundParticipantId: "part_ws_bound",
        producerId: participantAuthContext.participantId,
        type: sessionEventType.userMessage,
      }),
    ).toEqual({
      reason: clientPublishDenyReason.IdentityMismatch,
      status: "denied",
    });
  });
});
