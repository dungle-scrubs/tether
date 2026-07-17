import { describe, expect, expectTypeOf, it } from "vitest";

import { webSocketErrorEnvelopeSchema } from "../src/index.js";

describe("WebSocket error envelopes", () => {
  it("preserves replay_window_exceeded as a typed safe reason", () => {
    const envelope = webSocketErrorEnvelopeSchema.parse({
      error: "Replay window exceeded",
      op: "error",
      reason: "replay_window_exceeded",
    });

    expectTypeOf(envelope.reason).toEqualTypeOf<string | undefined>();
    expect(envelope.reason).toBe("replay_window_exceeded");
  });

  it("preserves replay_gap_unrepaired as a typed safe reason", () => {
    const envelope = webSocketErrorEnvelopeSchema.parse({
      error: "Replay gap could not be repaired",
      op: "error",
      reason: "replay_gap_unrepaired",
    });

    expectTypeOf(envelope.reason).toEqualTypeOf<string | undefined>();
    expect(envelope.reason).toBe("replay_gap_unrepaired");
  });

  it("preserves unknown server fields and future reason strings", () => {
    const envelope = webSocketErrorEnvelopeSchema.parse({
      error: "Future server error",
      futureDetails: { retryable: false },
      op: "error",
      reason: "future_replay_reason",
    });

    expect(envelope).toMatchObject({
      futureDetails: { retryable: false },
      reason: "future_replay_reason",
    });
  });
});
