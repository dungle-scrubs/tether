import { describe, expect, it } from "vitest";

import { readinessResponseSchema } from "../src/index.js";

describe("readiness response", () => {
  it("accepts healthy topology metadata and only bounded failure reasons", () => {
    expect(
      readinessResponseSchema.parse({
        ready: true,
        replicaId: "replica_ready",
        runtimeTopology: "multi",
      }),
    ).toEqual({
      ready: true,
      replicaId: "replica_ready",
      runtimeTopology: "multi",
    });
    expect(
      readinessResponseSchema.safeParse({
        detail: "secret database host",
        ready: false,
        reason: "database_connection_refused_at_secret_host",
        replicaId: "replica_ready",
        runtimeTopology: "single",
      }).success,
    ).toBe(false);
  });
});
