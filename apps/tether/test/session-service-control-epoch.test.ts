import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { ControlLease, ControlLeaseClaim, ControlLeaseRenewal } from "../src/db.js";
import type { ControlLeaseStore, SessionPersistenceStores } from "../src/db-store-contracts.js";
import { nextControlEpoch } from "../src/control-epoch.js";
import { ModuleObservability } from "../src/observability.js";
import {
  createSessionControlEffects,
  type SessionControlEffects,
} from "../src/session-service-control-effects.js";
import {
  type RestControlledInput,
  SessionServicePersistenceError,
} from "../src/session-service-contracts.js";

/**
 * In-memory single-current-lease store that models the acquire-or-supersede,
 * epoch-comparing renewal, and epoch-aware release contract so the control
 * effects can be exercised without a database.
 */
class FakeControlLeaseStore implements ControlLeaseStore {
  private current: ControlLease | null = null;
  private maxEpoch = 0;
  readonly releaseCalls: Array<{
    readonly controlEpoch: number | undefined;
    readonly epoch: number;
  }> = [];

  async claim(input: {
    readonly controlChannel: "rest" | "ws";
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  }): Promise<ControlLeaseClaim> {
    if (
      this.current &&
      (this.current.instanceId !== input.instanceId ||
        this.current.controlChannel !== input.controlChannel)
    ) {
      return { activeLease: this.current, status: "conflict" };
    }
    const hadCurrent = this.current !== null;
    const epoch = nextControlEpoch(this.maxEpoch === 0 ? null : this.maxEpoch);
    this.maxEpoch = epoch;
    this.current = this.buildLease(input, epoch);
    return { lease: this.current, status: hadCurrent ? "superseded" : "claimed" };
  }

  async renew(input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch: number;
    readonly instanceId: string;
    readonly leaseTtlMs: number;
    readonly participantId: string;
    readonly sessionId: string;
  }): Promise<ControlLeaseRenewal> {
    if (!this.current) {
      return { status: "absent" };
    }
    if (
      this.current.instanceId !== input.instanceId ||
      this.current.controlChannel !== input.controlChannel
    ) {
      return { activeLease: this.current, status: "conflict" };
    }
    if (this.current.epoch !== input.controlEpoch) {
      return { currentEpoch: this.current.epoch, status: "stale" };
    }
    return { lease: this.current, status: "renewed" };
  }

  async listSnapshots(): Promise<never[]> {
    return [];
  }

  async release(input: {
    readonly controlChannel: "rest" | "ws";
    readonly controlEpoch?: number;
    readonly instanceId: string;
    readonly participantId: string;
    readonly sessionId: string;
  }): Promise<void> {
    if (
      this.current &&
      this.current.instanceId === input.instanceId &&
      this.current.controlChannel === input.controlChannel &&
      (input.controlEpoch === undefined || this.current.epoch === input.controlEpoch)
    ) {
      this.releaseCalls.push({ controlEpoch: input.controlEpoch, epoch: this.current.epoch });
      this.current = null;
    }
  }

  currentEpoch(): number | null {
    return this.current?.epoch ?? null;
  }

  private buildLease(
    input: {
      readonly controlChannel: "rest" | "ws";
      readonly instanceId: string;
      readonly participantId: string;
      readonly sessionId: string;
    },
    epoch: number,
  ): ControlLease {
    const now = new Date().toISOString();
    return {
      claimedAt: now,
      controlChannel: input.controlChannel,
      epoch,
      instanceId: input.instanceId,
      lastSeenAt: now,
      leaseExpiresAt: new Date(Date.now() + 60_000).toISOString(),
      participantId: input.participantId,
      releasedAt: null,
      sessionId: input.sessionId,
    };
  }
}

function createEffects(input: {
  readonly controlEpochEnforcement: boolean;
  readonly controlLeases: ControlLeaseStore;
}): SessionControlEffects {
  const stores = {
    controlLeases: input.controlLeases,
  } as unknown as SessionPersistenceStores;
  return createSessionControlEffects({
    assertBroadcastEvents: () => undefined,
    controlEpochEnforcement: input.controlEpochEnforcement,
    eventSourceId: "src_control_epoch_test",
    observability: new ModuleObservability({ moduleName: "ControlEpochTest" }),
    stores,
    wsControlLeaseTtlMs: 60_000,
  });
}

const restInput: RestControlledInput = {
  instanceId: "inst_a",
  participantId: "part_1",
  sessionId: "sess_1",
};

describe("REST control epoch validation", () => {
  it("issues a strictly newer epoch on same-instance reconnect", async () => {
    const store = new FakeControlLeaseStore();
    const first = await store.claim({ ...restInput, controlChannel: "ws", leaseTtlMs: 1000 });
    const second = await store.claim({ ...restInput, controlChannel: "ws", leaseTtlMs: 1000 });
    expect(first.status).toBe("claimed");
    expect(second.status).toBe("superseded");
    if (first.status === "conflict" || second.status === "conflict") {
      throw new Error("unexpected conflict");
    }
    expect(second.lease.epoch).toBeGreaterThan(first.lease.epoch);
  });

  it("accepts a legacy request without an epoch when enforcement is off", async () => {
    const store = new FakeControlLeaseStore();
    const effects = createEffects({ controlEpochEnforcement: false, controlLeases: store });
    const outcome = await Effect.runPromise(effects.claimRestControlEffect(restInput));
    expect(outcome.status).toBe("ok");
    expect(store.currentEpoch()).toBe(1);
  });

  it("rejects a request missing its epoch when enforcement is on", async () => {
    const store = new FakeControlLeaseStore();
    const effects = createEffects({ controlEpochEnforcement: true, controlLeases: store });
    const outcome = await Effect.runPromise(effects.claimRestControlEffect(restInput));
    expect(outcome).toEqual({ currentEpoch: null, status: "control_epoch_stale" });
  });

  it("validates a supplied epoch even when enforcement is off", async () => {
    const store = new FakeControlLeaseStore();
    const claim = await store.claim({ ...restInput, controlChannel: "rest", leaseTtlMs: 1000 });
    if (claim.status === "conflict") {
      throw new Error("unexpected conflict");
    }
    const effects = createEffects({ controlEpochEnforcement: false, controlLeases: store });
    const fresh = await Effect.runPromise(
      effects.claimRestControlEffect({ ...restInput, controlEpoch: claim.lease.epoch }),
    );
    expect(fresh.status).toBe("ok");
    const stale = await Effect.runPromise(
      effects.claimRestControlEffect({ ...restInput, controlEpoch: claim.lease.epoch - 1 || 99 }),
    );
    expect(stale).toEqual({ currentEpoch: claim.lease.epoch, status: "control_epoch_stale" });
  });

  it("reports a different-instance owner as a control conflict without mutating state", async () => {
    const store = new FakeControlLeaseStore();
    await store.claim({
      controlChannel: "ws",
      instanceId: "inst_owner",
      leaseTtlMs: 1000,
      participantId: "part_1",
      sessionId: "sess_1",
    });
    const before = store.currentEpoch();
    const effects = createEffects({ controlEpochEnforcement: true, controlLeases: store });
    const outcome = await Effect.runPromise(
      effects.claimRestControlEffect({ ...restInput, controlEpoch: before ?? 1 }),
    );
    expect(outcome.status).toBe("control_conflict");
    expect(store.currentEpoch()).toBe(before);
  });
});

describe("WebSocket control epoch renewal fencing", () => {
  it("renews the bound epoch and fences a superseded socket", async () => {
    const store = new FakeControlLeaseStore();
    const effects = createEffects({ controlEpochEnforcement: false, controlLeases: store });
    const acquired = await store.claim({
      controlChannel: "ws",
      instanceId: "inst_a",
      leaseTtlMs: 1000,
      participantId: "part_1",
      sessionId: "sess_1",
    });
    if (acquired.status === "conflict") {
      throw new Error("unexpected conflict");
    }
    const boundEpoch = acquired.lease.epoch;

    const renewed = await Effect.runPromise(
      effects.refreshWebSocketControlLeaseEffect({
        controlEpoch: boundEpoch,
        instanceId: "inst_a",
        participantId: "part_1",
        sessionId: "sess_1",
      }),
    );
    expect(renewed.status).toBe("ok");

    // A same-instance reconnect advances the epoch, fencing the old socket.
    const reconnect = await store.claim({
      controlChannel: "ws",
      instanceId: "inst_a",
      leaseTtlMs: 1000,
      participantId: "part_1",
      sessionId: "sess_1",
    });
    if (reconnect.status === "conflict") {
      throw new Error("unexpected conflict");
    }
    expect(reconnect.lease.epoch).toBeGreaterThan(boundEpoch);

    const staleRenewal = await Effect.runPromise(
      effects.refreshWebSocketControlLeaseEffect({
        controlEpoch: boundEpoch,
        instanceId: "inst_a",
        participantId: "part_1",
        sessionId: "sess_1",
      }),
    );
    expect(staleRenewal).toEqual({
      currentEpoch: reconnect.lease.epoch,
      status: "control_epoch_stale",
    });

    // The fenced epoch must not release the replacement lease.
    await store.release({
      controlChannel: "ws",
      controlEpoch: boundEpoch,
      instanceId: "inst_a",
      participantId: "part_1",
      sessionId: "sess_1",
    });
    expect(store.currentEpoch()).toBe(reconnect.lease.epoch);
  });
});

describe("WebSocket participant registration compensation", () => {
  it("releases the just-claimed superseding lease when the participant upsert fails", async () => {
    const store = new FakeControlLeaseStore();
    // A prior socket already owns epoch 1 for this instance.
    const prior = await store.claim({
      controlChannel: "ws",
      instanceId: "inst_a",
      leaseTtlMs: 60_000,
      participantId: "part_1",
      sessionId: "sess_1",
    });
    if (prior.status === "conflict") {
      throw new Error("unexpected conflict");
    }

    const upsertError = new Error("participant upsert failed");
    const stores = {
      controlLeases: store,
      participants: {
        upsertWithEvent: async () => {
          throw upsertError;
        },
      },
    } as unknown as SessionPersistenceStores;
    const effects = createSessionControlEffects({
      assertBroadcastEvents: () => undefined,
      controlEpochEnforcement: false,
      eventSourceId: "src_ws_register_compensation_test",
      observability: new ModuleObservability({ moduleName: "WsRegisterCompensationTest" }),
      stores,
      wsControlLeaseTtlMs: 60_000,
    });

    const failure = await Effect.runPromise(
      effects
        .registerWebSocketParticipantEffect({
          capabilities: {},
          displayName: "Worker",
          instanceId: "inst_a",
          participantId: "part_1",
          runtimeKind: "generic_agent",
          sessionId: "sess_1",
        })
        .pipe(Effect.flip),
    );

    // The registration failure propagates (wrapped as a persistence failure)
    // rather than committing a half-applied superseding epoch.
    expect(failure).toBeInstanceOf(SessionServicePersistenceError);
    expect((failure as SessionServicePersistenceError).cause).toBe(upsertError);
    // The reconnect claim advanced the epoch to 2, superseding epoch 1; the failed
    // upsert then rolled that superseding lease back, so no current lease dangles.
    expect(store.currentEpoch()).toBeNull();
    expect(store.releaseCalls).toEqual([
      { controlEpoch: prior.lease.epoch + 1, epoch: prior.lease.epoch + 1 },
    ]);
  });
});
