import type { EvaluatedSessionSummarySelection } from "./executor.js";

/** Fail-closed M5 outcome. Environment configuration cannot enable generation. */
export const productionSessionSummarySelection = {
  reason: "hard_gates_not_passed",
  status: "disabled",
} as const satisfies EvaluatedSessionSummarySelection;

/** Returns the safe production startup disposition without reading environment input. */
export function productionStartupStatus(): {
  readonly reason: string;
  readonly status: "disabled";
} {
  return {
    reason: productionSessionSummarySelection.reason,
    status: "disabled",
  };
}
