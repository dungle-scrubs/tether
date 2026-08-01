import type { ReadinessResponse } from "@dungle-scrubs/tether-protocol";
import { readinessFailureReason } from "@dungle-scrubs/tether-protocol";

import type { RuntimeTopology } from "./config.js";
import type { DatabaseMigrationReadiness } from "./database-migration.js";
import type { SessionEventFanout } from "./session-event-fanout.js";

/** HTTP status plus the bounded protocol-owned readiness response. */
export interface ReadinessProjection {
  readonly body: ReadinessResponse;
  readonly status: 200 | 503;
}

/** Dependencies required to evaluate process readiness. */
export interface ReadinessInput {
  readonly deployment: {
    readonly configurationCompatible: boolean;
    readonly databaseMigrationReadiness: () => Promise<DatabaseMigrationReadiness>;
    readonly signingAuthorityReady: boolean;
  };
  readonly fanout: Pick<SessionEventFanout, "debugInfo">;
  readonly fanoutStaleAfterMs: number;
  readonly replicaId: string;
  readonly runtimeTopology: RuntimeTopology;
}

/** Evaluates database reachability and local subscriber catch-up health. */
export async function projectReadiness(input: ReadinessInput): Promise<ReadinessProjection> {
  let databaseMigrationReadiness: DatabaseMigrationReadiness;
  try {
    databaseMigrationReadiness = await input.deployment.databaseMigrationReadiness();
  } catch {
    return unavailable(input, readinessFailureReason.databaseUnavailable);
  }
  if (databaseMigrationReadiness === "unavailable") {
    return unavailable(input, readinessFailureReason.databaseUnavailable);
  }
  if (databaseMigrationReadiness === "incomplete") {
    return unavailable(input, readinessFailureReason.migrationIncomplete);
  }
  if (!input.deployment.signingAuthorityReady) {
    return unavailable(input, readinessFailureReason.signingAuthorityUnavailable);
  }
  if (!input.deployment.configurationCompatible) {
    return unavailable(input, readinessFailureReason.configurationIncompatible);
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
