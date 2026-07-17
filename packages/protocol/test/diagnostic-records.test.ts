import { describe, expect, it } from "vitest";

import { hostPresenceInventorySchema } from "../src/index.js";
import type {
  ClientSessionBindingRecord,
  ControlLeaseSnapshot,
  ParticipantRuntimeSnapshot,
  SessionDebugSummary,
  SessionRecord,
  TaskSnapshot,
} from "../src/index.js";

describe("diagnostic record ownership", () => {
  it("requires Replica Scope metadata on Host Presence inventories", () => {
    expect(hostPresenceInventorySchema.safeParse({ sessions: [] }).success).toBe(false);
    expect(
      hostPresenceInventorySchema.parse({
        replicaId: "replica_opaque_1",
        scope: "replica",
        sessions: [{ sessionId: "sess_1" }],
      }),
    ).toEqual({
      replicaId: "replica_opaque_1",
      scope: "replica",
      sessions: [{ sessionId: "sess_1" }],
    });
  });

  it("exports shared session and diagnostic records from protocol", () => {
    const session = {
      createdAt: "2026-07-08T00:00:00.000Z",
      sessionId: "sess_1",
    } satisfies SessionRecord;
    const binding = {
      archivedAt: null,
      createdAt: session.createdAt,
      externalId: "123",
      lastSeenAt: session.createdAt,
      provider: "external-chat",
      sessionId: session.sessionId,
    } satisfies ClientSessionBindingRecord;
    const lease = {
      claimedAt: session.createdAt,
      controlChannel: "rest",
      epoch: 1,
      instanceId: "inst_1",
      lastSeenAt: session.createdAt,
      leaseExpiresAt: session.createdAt,
      participantId: "part_1",
      releasedAt: null,
      sessionId: session.sessionId,
      status: "active",
      supersededAt: null,
    } satisfies ControlLeaseSnapshot;
    const task = {
      approvals: [],
      cancelledAt: null,
      claimExpiredAt: null,
      claimExpiredBy: null,
      claimExpiresAt: null,
      claimedAt: null,
      claimedBy: null,
      completedAt: null,
      createdAt: session.createdAt,
      failedAt: null,
      failure: null,
      input: null,
      kind: "coordinate_request",
      objective: "Coordinate this",
      releasedAt: null,
      releasedBy: null,
      result: null,
      sessionId: session.sessionId,
      status: "unclaimed",
      taskId: "task_1",
    } satisfies TaskSnapshot;
    const participant = {
      controlLeaseCount: 1,
      currentControlLease: lease,
      latestControlLease: lease,
      participant: null,
      participantId: lease.participantId,
      registered: false,
      sessionId: session.sessionId,
      status: "lease_without_presence",
    } satisfies ParticipantRuntimeSnapshot;
    const summary = {
      controlLeases: {
        active: 1,
        expired: 0,
        released: 0,
        superseded: 0,
        total: 1,
      },
      participants: {
        activeControl: 0,
        leaseOnly: 1,
        registered: 0,
        total: 1,
        withoutActiveControl: 0,
      },
      sessionId: session.sessionId,
      tasks: {
        activeClaims: 0,
        cancelled: 0,
        claimable: 1,
        claimActive: 0,
        claimCleared: 0,
        claimExpired: 0,
        completed: 0,
        expiredClaims: 0,
        failed: 0,
        terminal: 0,
        total: 1,
        unclaimed: 1,
      },
    } satisfies SessionDebugSummary;

    expect({ binding, participant, session, summary, task }).toMatchObject({
      binding: { provider: "external-chat" },
      participant: { status: "lease_without_presence" },
      session: { sessionId: "sess_1" },
      summary: { sessionId: "sess_1" },
      task: { status: "unclaimed" },
    });
  });
});
