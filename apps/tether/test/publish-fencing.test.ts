import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import type { ControlEpochGuard } from "../src/db.js";
import type { SessionPersistenceStores } from "../src/db-store-contracts.js";
import {
  createSessionCoreEffects,
  type SessionCoreEffectsInput,
} from "../src/session-service-core-effects.js";
import type { RestControlOutcome } from "../src/session-service-contracts.js";
import type { SessionEvent } from "../src/types.js";

interface CapturedAppend {
  readonly controlGuard: ControlEpochGuard | undefined;
}

function createCoreEffects(input: {
  readonly controlEpochEnforcement: boolean;
  readonly control?: RestControlOutcome;
  readonly appends: CapturedAppend[];
}) {
  const appendEvent = async (options: {
    readonly controlGuard?: ControlEpochGuard | undefined;
  }): Promise<SessionEvent> => {
    input.appends.push({ controlGuard: options.controlGuard });
    return {
      createdAt: "2026-07-12T00:00:00.000Z",
      eventId: "evt_1",
      payload: {},
      producerId: "part_1",
      seq: 1,
      sessionId: "sess_1",
      type: "custom.event",
    } as unknown as SessionEvent;
  };
  const stores = {
    events: {
      append: (_event: unknown, options: { readonly controlGuard?: ControlEpochGuard }) =>
        appendEvent(options),
      appendIdempotent: async () => {
        throw new Error("unexpected idempotent append");
      },
      list: async () => [],
    },
  } as unknown as SessionPersistenceStores;
  const coreInput: SessionCoreEffectsInput = {
    appendEventEffect: (event) => {
      // The unfenced append path routes through the shared append effect.
      input.appends.push({ controlGuard: undefined });
      return Effect.succeed(event as SessionEvent);
    },
    assertBroadcastEvents: () => undefined,
    claimRestControlEffect: () => Effect.succeed(input.control ?? { status: "ok" }),
    controlEpochEnforcement: input.controlEpochEnforcement,
    eventSourceId: "src_publish_fencing_test",
    stores,
  };
  return createSessionCoreEffects(coreInput);
}

const basePublish = {
  eventId: undefined,
  payload: { hello: "world" },
  producerId: "part_1",
  sessionId: "sess_1",
  type: "custom.event",
};

describe("REST publish epoch fencing", () => {
  it("rejects a participant-owned publish that omits instanceId when enforcement is on", async () => {
    const appends: CapturedAppend[] = [];
    const effects = createCoreEffects({ appends, controlEpochEnforcement: true });

    const result = await Effect.runPromise(
      effects.publishRestEventEffect({ ...basePublish, instanceId: undefined }),
    );

    expect(result).toEqual({ status: "control_epoch_required" });
    // The bypass is closed: no event was appended.
    expect(appends).toEqual([]);
  });

  it("preserves the legacy unfenced publish when enforcement is off and instanceId is absent", async () => {
    const appends: CapturedAppend[] = [];
    const effects = createCoreEffects({ appends, controlEpochEnforcement: false });

    const result = await Effect.runPromise(
      effects.publishRestEventEffect({ ...basePublish, instanceId: undefined }),
    );

    expect(result.status).toBe("created");
    expect(appends).toHaveLength(1);
    expect(appends[0]?.controlGuard).toBeUndefined();
  });

  it("rejects a publish that supplies a controlEpoch but omits instanceId even when enforcement is off", async () => {
    const appends: CapturedAppend[] = [];
    const effects = createCoreEffects({ appends, controlEpochEnforcement: false });

    const result = await Effect.runPromise(
      effects.publishRestEventEffect({ ...basePublish, controlEpoch: 7, instanceId: undefined }),
    );

    // A supplied epoch with an incomplete control context is a stale/invalid
    // control request, not a legacy unfenced publish, so it is rejected rather
    // than silently downgraded to an unfenced append.
    expect(result).toEqual({ currentEpoch: null, status: "control_epoch_stale" });
    expect(appends).toEqual([]);
  });

  it("fences the append with an atomic control guard when instanceId and epoch are supplied", async () => {
    const appends: CapturedAppend[] = [];
    const effects = createCoreEffects({ appends, controlEpochEnforcement: true });

    const result = await Effect.runPromise(
      effects.publishRestEventEffect({ ...basePublish, controlEpoch: 7, instanceId: "inst_a" }),
    );

    expect(result.status).toBe("created");
    expect(appends).toHaveLength(1);
    expect(appends[0]?.controlGuard).toEqual({
      controlChannel: "rest",
      controlEpoch: 7,
      instanceId: "inst_a",
      participantId: "part_1",
      sessionId: "sess_1",
    });
  });

  it("returns the control outcome and does not append when the pre-check rejects", async () => {
    const appends: CapturedAppend[] = [];
    const effects = createCoreEffects({
      appends,
      control: { currentEpoch: 9, status: "control_epoch_stale" },
      controlEpochEnforcement: true,
    });

    const result = await Effect.runPromise(
      effects.publishRestEventEffect({ ...basePublish, controlEpoch: 7, instanceId: "inst_a" }),
    );

    expect(result).toEqual({ currentEpoch: 9, status: "control_epoch_stale" });
    expect(appends).toEqual([]);
  });
});

describe("participant-owned publish guard forwarding", () => {
  it("forwards a supplied control guard into the durable append (WebSocket path)", async () => {
    const appends: CapturedAppend[] = [];
    const effects = createCoreEffects({ appends, controlEpochEnforcement: false });
    const controlGuard: ControlEpochGuard = {
      controlChannel: "ws",
      controlEpoch: 4,
      instanceId: "inst_a",
      participantId: "part_1",
      sessionId: "sess_1",
    };

    const result = await Effect.runPromise(
      effects.publishEventEffect({ ...basePublish, controlGuard }),
    );

    expect(result.status).toBe("created");
    expect(appends[0]?.controlGuard).toEqual(controlGuard);
  });
});
