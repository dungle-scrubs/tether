import { describe, expect, it } from "vitest";

import {
  registerParticipantSchema,
  releaseParticipantControlSchema,
  restControlAcquisitionResponseSchema,
  restControlRenewalResponseSchema,
} from "../src/rest-schemas.js";

describe("REST participant control contracts", () => {
  it("accepts one bounded Acquisition ID and rejects empty or oversized values", () => {
    expect(
      registerParticipantSchema.safeParse({
        acquisitionId: "acq_1",
        runtimeKind: "generic_agent",
      }).success,
    ).toBe(true);
    expect(
      registerParticipantSchema.safeParse({
        acquisitionId: "",
        runtimeKind: "generic_agent",
      }).success,
    ).toBe(false);
    expect(
      registerParticipantSchema.safeParse({
        acquisitionId: "a".repeat(129),
        runtimeKind: "generic_agent",
      }).success,
    ).toBe(false);
  });

  it("validates acquisition, renewal, and exact release lifecycle shapes", () => {
    expect(
      restControlAcquisitionResponseSchema.safeParse({
        acquisitionId: "acq_1",
        acquisitionStatus: "replayed",
        controlEpoch: 3,
        leaseExpiresAt: "2026-07-16T12:01:00.000Z",
        participant: { participantId: "part_1" },
        registrationStatus: "refreshed",
        renewAfterMs: 30_000,
      }).success,
    ).toBe(true);
    expect(
      restControlRenewalResponseSchema.safeParse({
        controlEpoch: 3,
        leaseExpiresAt: "2026-07-16T12:01:30.000Z",
        participant: { participantId: "part_1" },
        renewAfterMs: 30_000,
      }).success,
    ).toBe(true);
    expect(
      releaseParticipantControlSchema.safeParse({
        controlEpoch: 3,
        instanceId: "inst_1",
      }).success,
    ).toBe(true);
    expect(releaseParticipantControlSchema.safeParse({ instanceId: "inst_1" }).success).toBe(false);
  });
});
