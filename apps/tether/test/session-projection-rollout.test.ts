import { describe, expect, it } from "vitest";

import {
  canActivateSessionProjectionEventContract,
  canUseCurrentSessionProjectionReducer,
  sessionProjectionReplicaCapabilities,
} from "../src/session-projection-rollout.js";

describe("Session Projection mixed-version rollout", () => {
  it("requires every active replica to report the current reducer version", () => {
    const current = sessionProjectionReplicaCapabilities("replica-current");
    const old = {
      ...current,
      reducerVersion: current.reducerVersion - 1,
      replicaId: "replica-old",
    };

    expect(canUseCurrentSessionProjectionReducer([current, old])).toBe(false);
    expect(canUseCurrentSessionProjectionReducer([current])).toBe(true);
    expect(canUseCurrentSessionProjectionReducer([])).toBe(false);
  });

  it("blocks a projection-affecting event contract until every replica supports its reducer", () => {
    const eventContract = "session.renamedByPolicy";
    const current = sessionProjectionReplicaCapabilities("replica-current");
    const ready = {
      ...current,
      eventContracts: [...current.eventContracts, eventContract],
      reducerVersion: 2,
    };
    const missingContract = { ...ready, eventContracts: current.eventContracts };

    expect(
      canActivateSessionProjectionEventContract([ready, missingContract], {
        eventContract,
        reducerVersion: 2,
      }),
    ).toBe(false);
    expect(
      canActivateSessionProjectionEventContract([ready], { eventContract, reducerVersion: 2 }),
    ).toBe(true);
  });
});
