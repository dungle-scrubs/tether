import type { ReadinessResponse } from "@dungle-scrubs/tether-protocol";
import { readinessFailureReason } from "@dungle-scrubs/tether-protocol";

import type { RuntimeTopology } from "./config.js";
import type { DatabasePool } from "./db.js";
import type { SessionEventFanout } from "./session-event-fanout.js";

/** HTTP status plus the bounded protocol-owned readiness response. */
export interface ReadinessProjection {
  readonly body: ReadinessResponse;
  readonly status: 200 | 503;
}

/** Dependencies required to evaluate process readiness. */
export interface ReadinessInput {
  readonly database: DatabasePool;
  readonly fanout: Pick<SessionEventFanout, "debugInfo">;
  readonly fanoutStaleAfterMs: number;
  readonly replicaId: string;
  readonly runtimeTopology: RuntimeTopology;
}

/** Evaluates database reachability and local subscriber catch-up health. */
export async function projectReadiness(input: ReadinessInput): Promise<ReadinessProjection> {
  try {
    await input.database.pool.query("SELECT 1");
  } catch {
    return unavailable(input, readinessFailureReason.databaseUnavailable);
  }
  const fanout = input.fanout.debugInfo();
  const repairDisabled =
    fanout.sessionCursorCount > 0 && fanout.catchUpPollIntervalMs <= 0 && !fanout.connected;
  const staleSession = fanout.sessionLag.some(
    (session) => session.lagAgeMs > input.fanoutStaleAfterMs,
  );
  if (repairDisabled || staleSession) {
    return unavailable(input, readinessFailureReason.fanoutCatchUpStale);
  }
  return {
    body: {
      ready: true,
      replicaId: input.replicaId,
      runtimeTopology: input.runtimeTopology,
    },
    status: 200,
  };
}

/** Projects one bounded unavailable response without database or session details. */
function unavailable(
  input: Pick<ReadinessInput, "replicaId" | "runtimeTopology">,
  reason: (typeof readinessFailureReason)[keyof typeof readinessFailureReason],
): ReadinessProjection {
  return {
    body: {
      ready: false,
      reason,
      replicaId: input.replicaId,
      runtimeTopology: input.runtimeTopology,
    },
    status: 503,
  };
}
