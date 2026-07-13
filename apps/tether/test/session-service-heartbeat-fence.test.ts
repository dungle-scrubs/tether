import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { ControlEpochStaleError } from "../src/control-epoch.js";
import type {
  HeartbeatParticipantWithEventResult,
  ControlLease,
  ControlLeaseClaim,
  ControlLeaseRenewal,
} from "../src/db.js";
import type {
  ControlLeaseStore,
  ParticipantStore,
  SessionPersistenceStores,
} from "../src/db-store-contracts.js";
import { ModuleObservability } from "../src/observability.js";
import { createSessionControlEffects } from "../src/session-service-control-effects.js";
import type { HeartbeatParticipantInput } from "../src/session-service-contracts.js";
import type { ParticipantRecord, SessionEvent } from "../src/types.js";

/**
 * Control-lease double whose renewal always succeeds for the configured epoch, so
 * the outer REST epoch validation passes and the heartbeat reaches the atomic
 * participant fence. It models the pre-mutation view of the lease that a
 * concurrent supersession has not yet reached.
 */
class RenewableControlLeaseStore implements ControlLeaseStore {
  constructor(private readonly renewableEpoch: number) {}

  async claim(): Promise<ControlLeaseClaim> {
    throw new Error("unused");
  }

  async renew(input: { readonly controlEpoch: number }): Promise<ControlLeaseRenewal> {
    if (input.controlEpoch === this.renewableEpoch) {
      return { lease: {} as ControlLease, status: "renewed" };
    }
    return { currentEpoch: this.renewableEpoch, status: "stale" };
  }

  async listSnapshots(): Promise<never[]> {
    return [];
  }

  async release(): Promise<void> {
    return undefined;
  }
}

/**
 * Participant double whose `heartbeatWithEvent` models the atomic epoch fence: a
 * guard epoch that does not equal the authoritative current epoch throws
 * `ControlEpochStaleError` before recording any mutation, exactly as the durable
 * transaction rolls back before the presence UPDATE. It records whether the
 * presence mutation was actually applied.
 */
class FencedParticipantStore implements Partial<ParticipantStore> {
  mutated = false;

  constructor(private readonly authoritativeEpoch: number) {}

  async heartbeatWithEvent(input: {
    readonly capabilities?: Record<string, unknown> | undefined;
    readonly controlGuard?:
      | {
          readonly controlEpoch: number;
          readonly instanceId: string;
          readonly participantId: string;
          readonly sessionId: string;
        }
      | undefined;
    readonly eventSourceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }): Promise<HeartbeatParticipantWithEventResult> {
    if (input.controlGuard && input.controlGuard.controlEpoch !== this.authoritativeEpoch) {
      throw new ControlEpochStaleError({
        controlChannel: "rest",
        currentEpoch: this.authoritativeEpoch,
        participantId: input.participantId,
        providedEpoch: input.controlGuard.controlEpoch,
        sessionId: input.sessionId,
      });
    }
    this.mutated = true;
    const participant: ParticipantRecord = {
      capabilities: input.capabilities ?? {},
      displayName: input.participantId,
      joinedAt: "2026-07-12T00:00:00.000Z",
      lastSeenAt: "2026-07-12T00:00:01.000Z",
      participantId: input.participantId,
      runtimeKind: "worker",
      sessionId: input.sessionId,
    };
    const event: SessionEvent = {
      createdAt: "2026-07-12T00:00:01.000Z",
      eventId: "evt_heartbeat_1",
      payload: { participant },
      producerId: "system",
      seq: 1,
      sessionId: input.sessionId,
      type: "participant.heartbeat",
    };
    return { event, participant };
  }
}

function createHeartbeatEffects(input: {
  readonly controlLeases: ControlLeaseStore;
  readonly participants: Partial<ParticipantStore>;
}) {
  const stores = {
    controlLeases: input.controlLeases,
    participants: input.participants,
  } as unknown as SessionPersistenceStores;
  return createSessionControlEffects({
    assertBroadcastEvents: () => undefined,
    controlEpochEnforcement: false,
    eventSourceId: "src_heartbeat_fence_test",
    observability: new ModuleObservability({ moduleName: "HeartbeatFenceTest" }),
    stores,
    wsControlLeaseTtlMs: 60_000,
  });
}

const heartbeatInput: HeartbeatParticipantInput = {
  capabilities: { tools: ["organize"] },
  instanceId: "inst_a",
  participantId: "part_1",
  sessionId: "sess_1",
};

describe("epoch-fenced REST heartbeat", () => {
  it("does not refresh presence when the atomic epoch fence rejects a stale heartbeat", async () => {
    // The outer lease view still renews epoch 5, but the authoritative control
    // lease has advanced to epoch 6, so the atomic fence rejects the heartbeat.
    const participants = new FencedParticipantStore(6);
    const effects = createHeartbeatEffects({
      controlLeases: new RenewableControlLeaseStore(5),
      participants,
    });

    const result = await Effect.runPromise(
      effects.heartbeatRestParticipantEffect({ ...heartbeatInput, controlEpoch: 5 }),
    );

    expect(result).toEqual({ currentEpoch: 6, status: "control_epoch_stale" });
    // The presence/capabilities mutation must never run for a fenced epoch.
    expect(participants.mutated).toBe(false);
  });

  it("refreshes presence and appends the heartbeat event when the epoch is current", async () => {
    const participants = new FencedParticipantStore(5);
    const effects = createHeartbeatEffects({
      controlLeases: new RenewableControlLeaseStore(5),
      participants,
    });

    const result = await Effect.runPromise(
      effects.heartbeatRestParticipantEffect({ ...heartbeatInput, controlEpoch: 5 }),
    );

    expect(participants.mutated).toBe(true);
    if (result.status !== "ok") {
      throw new Error(`unexpected status ${result.status}`);
    }
    expect(result.events).toHaveLength(1);
    expect(result.participant?.participantId).toBe("part_1");
  });
});
