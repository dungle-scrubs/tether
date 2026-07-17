import { describe, expect, expectTypeOf, it } from "vitest";

import {
  parseWebSocketServerEnvelope,
  serializePresenceEnvelope,
  serializeReplayCompleteEnvelope,
  webSocketErrorEnvelopeSchema,
  webSocketPresenceEnvelopeSchema,
} from "../src/index.js";

describe("WebSocket error envelopes", () => {
  it("carries optional fenced participant identity on replay completion", () => {
    const envelope = parseWebSocketServerEnvelope(
      JSON.parse(
        serializeReplayCompleteEnvelope({
          controlEpoch: 4,
          instanceId: "instance_summary_worker",
          participantId: "participant_summary_worker",
        }),
      ) as unknown,
    );

    expect(envelope).toEqual({
      controlEpoch: 4,
      instanceId: "instance_summary_worker",
      op: "replay.complete",
      participantId: "participant_summary_worker",
    });
  });

  it("serializes and parses Replica Scope Host Presence envelopes", () => {
    const envelope = parseWebSocketServerEnvelope(
      JSON.parse(
        serializePresenceEnvelope({
          hosts: [
            {
              displayName: "Host One",
              instanceId: "inst_1",
              participantId: "part_1",
            },
          ],
          replicaId: "replica_opaque_1",
        }),
      ) as unknown,
    );

    expect(envelope).toEqual({
      hosts: [
        {
          displayName: "Host One",
          instanceId: "inst_1",
          participantId: "part_1",
        },
      ],
      op: "presence",
      replicaId: "replica_opaque_1",
      scope: "replica",
    });
    expect(webSocketPresenceEnvelopeSchema.safeParse({ hosts: [], op: "presence" }).success).toBe(
      false,
    );
  });

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
