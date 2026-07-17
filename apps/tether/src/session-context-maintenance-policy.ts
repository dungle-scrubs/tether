/** Supported summary budget classes for deterministic context maintenance. */
export type SessionContextBudgetClass = "2k" | "8k" | "16k" | "32k";

/** One target plus trigger pair, preserving a 50 percent hysteresis gap. */
export interface SessionContextMaintenancePolicy {
  readonly targetTokens: number;
  readonly triggerTokens: number;
}

/** A-004 policy selected across every supported context budget. */
export const sessionContextMaintenancePolicies: Readonly<
  Record<SessionContextBudgetClass, SessionContextMaintenancePolicy>
> = {
  "2k": { targetTokens: 2_000, triggerTokens: 3_000 },
  "8k": { targetTokens: 8_000, triggerTokens: 12_000 },
  "16k": { targetTokens: 16_000, triggerTokens: 24_000 },
  "32k": { targetTokens: 32_000, triggerTokens: 48_000 },
};

/** Conservative floor proving any truncated 10k-event tail crosses every trigger. */
export const truncatedSessionContextSuffixTokenFloor = 48_000;

/** Deterministic maintenance decision returned by the context builder. */
export type SessionContextMaintenanceDecision =
  | {
      readonly budgetClass: SessionContextBudgetClass;
      readonly status: "below_threshold";
      readonly triggerTokens: number;
      readonly unsummarizedTokens: number;
    }
  | {
      readonly budgetClass: SessionContextBudgetClass;
      readonly status: "enqueue";
      readonly triggerTokens: number;
      readonly unsummarizedTokens: number;
    };

/** Bounded failure signal safe for metrics and structured logs. */
export interface SessionContextMaintenanceScheduleFailure {
  readonly budgetClass: SessionContextBudgetClass;
  readonly code: "schedule_failed";
}

/** Selects maintenance only after the raw suffix crosses its hysteresis trigger. */
export function decideSessionContextMaintenance(input: {
  readonly budgetClass: SessionContextBudgetClass;
  readonly unsummarizedTokens: number;
}): SessionContextMaintenanceDecision {
  const policy = sessionContextMaintenancePolicies[input.budgetClass];
  return {
    budgetClass: input.budgetClass,
    status: input.unsummarizedTokens >= policy.triggerTokens ? "enqueue" : "below_threshold",
    triggerTokens: policy.triggerTokens,
    unsummarizedTokens: input.unsummarizedTokens,
  };
}

/**
 * Starts idempotent maintenance without making a context read wait for task
 * reservation or inference. The scheduler owns bounded failure reporting.
 */
export function enqueueSessionContextMaintenance(
  decision: SessionContextMaintenanceDecision,
  schedule: (input: {
    readonly budgetClass: SessionContextBudgetClass;
    readonly unsummarizedTokens: number;
  }) => Promise<void>,
  reportFailure: (failure: SessionContextMaintenanceScheduleFailure) => void,
): void {
  if (decision.status !== "enqueue") {
    return;
  }
  void Promise.resolve()
    .then(() =>
      schedule({
        budgetClass: decision.budgetClass,
        unsummarizedTokens: decision.unsummarizedTokens,
      }),
    )
    .catch(() => {
      reportFailure({ budgetClass: decision.budgetClass, code: "schedule_failed" });
    });
}
