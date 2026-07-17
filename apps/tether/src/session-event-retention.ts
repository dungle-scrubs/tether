import type { SessionEventRetentionStatus } from "@dungle-scrubs/tether-protocol";

/**
 * Static current-cutoff retention configuration. It accepts no environment,
 * SDK, summary, or operator input and exposes no mutation capability.
 */
export const sessionEventRetentionConfiguration = Object.freeze({
  boundaryAdvancementEnabled: false,
  deletionEnabled: false,
  reason: "future_safety_gates_unmet",
  status: "disabled",
  unmetGates: Object.freeze([
    "consumer_cursor_coverage",
    "atomic_boundary_advance",
    "replica_convergence",
    "backup_restore_validation",
    "recovery_contract",
  ]),
} as const satisfies SessionEventRetentionStatus);
