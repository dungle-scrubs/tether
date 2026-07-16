/**
 * Owns bounded, process-local REST participant-control decisions and
 * diagnostics. The module classifies missing Control Epochs and records stable
 * route outcomes. It does not access PostgreSQL or perform domain mutations.
 */

/** Stable REST participant-control enforcement modes. */
export type RestControlMode = "compatibility" | "enforced";

/** Bounded outcomes exposed by REST control diagnostics and telemetry. */
export type RestControlOutcomeName =
  | "acquisition_id_required"
  | "acquisition_replayed"
  | "acquisition_stale"
  | "control_conflict"
  | "epoch_required"
  | "epoch_stale"
  | "fenced_accepted"
  | "persistence_failure"
  | "unfenced_accepted";

/** Policy decision made before persistence or a protected mutation. */
export type RestControlDecision =
  | { readonly status: "accepted_unfenced" }
  | { readonly status: "control_epoch_required" }
  | { readonly controlEpoch: number; readonly status: "validate_fenced" };

/** Last bounded failure classification retained for operator inspection. */
export interface RestControlFailureSnapshot {
  readonly outcome: Extract<
    RestControlOutcomeName,
    | "acquisition_stale"
    | "acquisition_id_required"
    | "control_conflict"
    | "epoch_required"
    | "epoch_stale"
    | "persistence_failure"
  >;
  readonly routeName: string;
}

/** Process-local, identity-free REST control diagnostics. */
export interface RestControlPolicyDebugInfo {
  readonly counts: Readonly<
    Record<string, Readonly<Partial<Record<RestControlOutcomeName, number>>>>
  >;
  readonly lastFailure: RestControlFailureSnapshot | null;
  readonly mode: RestControlMode;
}

/** Input accepted at the REST control policy seam. */
export interface RestControlPolicyInput {
  readonly controlEpoch: number | undefined;
  readonly routeName: string;
}

const failureOutcomes: ReadonlySet<RestControlOutcomeName> = new Set([
  "acquisition_id_required",
  "acquisition_stale",
  "control_conflict",
  "epoch_required",
  "epoch_stale",
  "persistence_failure",
]);
const knownRouteNames: ReadonlySet<string> = new Set([
  "session.events.append",
  "session.participant.control.release",
  "session.participant.heartbeat",
  "session.participant.register",
  "task.approval",
  "task.cancel",
  "task.claim",
  "task.claim.refresh",
  "task.complete",
  "task.fail",
  "task.release",
]);

/** Applies missing-epoch policy and retains bounded per-route diagnostics. */
export class RestControlPolicy {
  readonly #counts = new Map<string, Map<RestControlOutcomeName, number>>();
  #lastFailure: RestControlFailureSnapshot | null = null;
  readonly #mode: RestControlMode;

  constructor(enforcement: boolean) {
    this.#mode = enforcement ? "enforced" : "compatibility";
  }

  /** Returns the stable mode label used by route telemetry. */
  mode(): RestControlMode {
    return this.#mode;
  }

  /** Classifies a request without reading or mutating durable lease state. */
  authorize(input: RestControlPolicyInput): RestControlDecision {
    if (input.controlEpoch !== undefined) {
      return { controlEpoch: input.controlEpoch, status: "validate_fenced" };
    }
    if (this.#mode === "compatibility") {
      return { status: "accepted_unfenced" };
    }
    return { status: "control_epoch_required" };
  }

  /** Returns an immutable snapshot containing only stable labels and counts. */
  debugInfo(): RestControlPolicyDebugInfo {
    return {
      counts: Object.fromEntries(
        [...this.#counts.entries()]
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([routeName, outcomes]) => [
            routeName,
            Object.fromEntries(
              [...outcomes.entries()].sort(([left], [right]) => left.localeCompare(right)),
            ),
          ]),
      ),
      lastFailure: this.#lastFailure,
      mode: this.#mode,
    };
  }

  /** Records one bounded outcome at a stable route name. */
  record(routeName: string, outcome: RestControlOutcomeName): void {
    const stableRouteName = knownRouteNames.has(routeName) ? routeName : "unknown";
    const routeCounts =
      this.#counts.get(stableRouteName) ?? new Map<RestControlOutcomeName, number>();
    routeCounts.set(outcome, (routeCounts.get(outcome) ?? 0) + 1);
    this.#counts.set(stableRouteName, routeCounts);
    if (failureOutcomes.has(outcome)) {
      this.#lastFailure = {
        outcome: outcome as RestControlFailureSnapshot["outcome"],
        routeName: stableRouteName,
      };
    }
  }
}
