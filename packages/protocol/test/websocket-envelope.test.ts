import { describe, expect, expectTypeOf, it } from "vitest";

import {
  classifyWebSocketServerEnvelope,
  parseWebSocketRecoveryCondition,
  parseWebSocketServerEnvelope,
  serializePresenceEnvelope,
  serializeReplayCompleteEnvelope,
  webSocketErrorEnvelopeSchema,
  webSocketPresenceEnvelopeSchema,
  webSocketRecoveryReason,
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
      limit: 2_000,
      op: "error",
      reason: "replay_window_exceeded",
    });

    expect(parseWebSocketRecoveryCondition(envelope)).toEqual({
      limit: 2_000,
      reason: webSocketRecoveryReason.replayWindowExceeded,
    });
  });

  it("reserves recovery_required without treating unknown server fields as recovery", () => {
    const reserved = webSocketErrorEnvelopeSchema.parse({
      error: "Recovery required",
      op: "error",
      reason: webSocketRecoveryReason.recoveryRequired,
    });
    const future = webSocketErrorEnvelopeSchema.parse({
      error: "Future condition",
      op: "error",
      reason: "future_condition",
    });

    expect(parseWebSocketRecoveryCondition(reserved)).toEqual({
      reason: webSocketRecoveryReason.recoveryRequired,
    });
    expect(parseWebSocketRecoveryCondition(future)).toBeNull();
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

describe("WebSocket server envelope classification", () => {
  it("classifies a valid known-op frame as an envelope", () => {
    const classified = classifyWebSocketServerEnvelope({ op: "replay.complete" });

    expect(classified).toEqual({
      envelope: { op: "replay.complete" },
      kind: "envelope",
    });
  });

  it("classifies a future unknown operation without failing strict parsing", () => {
    const classified = classifyWebSocketServerEnvelope({
      op: "presence.v2",
      payload: { future: true },
    });

    expect(classified).toEqual({ kind: "unknown-op", op: "presence.v2" });
  });

  it("keeps a known-op frame with an invalid body strictly malformed", () => {
    expect(classifyWebSocketServerEnvelope({ op: "event" })).toEqual({ kind: "malformed" });
    expect(
      classifyWebSocketServerEnvelope({ event: { eventId: "evt_1", seq: 1 }, op: "event" }),
    ).toEqual({ kind: "malformed" });
  });

  it("classifies frames without a string op as malformed", () => {
    expect(classifyWebSocketServerEnvelope(null)).toEqual({ kind: "malformed" });
    expect(classifyWebSocketServerEnvelope("frame")).toEqual({ kind: "malformed" });
    expect(classifyWebSocketServerEnvelope({})).toEqual({ kind: "malformed" });
    expect(classifyWebSocketServerEnvelope({ op: 7 })).toEqual({ kind: "malformed" });
  });
});
