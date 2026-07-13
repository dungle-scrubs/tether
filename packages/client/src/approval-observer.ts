import type { SessionEvent } from "./types.js";
import { ModuleObservability, readModuleObservabilityOptions } from "./observability.js";

type ApprovalEventPhase = "live" | "processed-output" | "replay";

/** Last approval processing failure captured by the observer diagnostics. */
export interface TaskApprovalObserverFailureInfo {
  /** Approval id whose processing failed. */
  readonly approvalId: string;
  /** Original or wrapped error message. */
  readonly message: string;
  /** Original or wrapped error name. */
  readonly name: string;
  /** Observer operation that failed. */
  readonly operation: string;
}

/** Snapshot of one approval observer's local process state. */
export interface TaskApprovalObserverDebugInfo {
  /** Number of approval ids currently inside processApproval. */
  readonly activeProcessingCount: number;
  /** Current in-flight approval ids. */
  readonly inFlightApprovalIds: readonly string[];
  /** Most recent processing failure, if any. */
  readonly lastError: TaskApprovalObserverFailureInfo | null;
  /** Observer module name used for boundary logs and spans. */
  readonly moduleName: string;
  /** Current processed approval ids known to this process. */
  readonly processedApprovalIds: readonly string[];
  /** Number of replay-buffered approval ids. */
  readonly replayBufferCount: number;
}

/** Callable unsubscribe handle with inspectable local observer state. */
export interface TaskApprovalObserver {
  /** Stops observing approval events. */
  (): void;
  /** Returns a point-in-time debug snapshot of local approval observer state. */
  readonly debugInfo: () => TaskApprovalObserverDebugInfo;
}

/** Error raised when a task approval processing callback rejects. */
export class TaskApprovalProcessingError extends Error {
  /** Approval id that failed processing. */
  readonly approvalId: string;
  /** Observer operation that failed. */
  readonly operation: string;

  /** Captures approval processing context while preserving the original cause. */
  constructor(input: {
    readonly approvalId: string;
    readonly cause: unknown;
    readonly operation: string;
  }) {
    super(`Approval ${input.approvalId} failed during ${input.operation}`, {
      cause: input.cause,
    });
    this.approvalId = input.approvalId;
    this.name = "TaskApprovalProcessingError";
    this.operation = input.operation;
  }
}

/** Minimal event-stream client surface needed by approval observers. */
export interface TaskApprovalObserverClient {
  /** Registers one durable event handler. */
  readonly onEvent: (handler: (event: SessionEvent) => void) => () => void;
  /** Resolves once historical event replay has completed. */
  readonly waitForReplayComplete: () => Promise<void>;
}

/** Options for observing approval events with replay-safe idempotency. */
export interface ObserveTaskApprovalsOptions<TApproval> {
  /** Event stream client that provides replay and live events. */
  readonly client: TaskApprovalObserverClient;
  /** Stable id for one parsed approval, usually the approval event id. */
  readonly approvalId: (approval: TApproval) => string;
  /** Handles observer processing failures. Defaults to console.error. */
  readonly onError?: (error: unknown) => void;
  /** Parses a relevant approval from one durable session event. */
  readonly parseApproval: (event: SessionEvent) => TApproval | null;
  /** Processes one approval after replay and idempotency checks. */
  readonly processApproval: (approval: TApproval) => Promise<void>;
  /** Extracts already-processed approval ids from prior output events. */
  readonly processedApprovalIdFromEvent: (event: SessionEvent) => string | null;
}

/**
 * Observes approval events after replay while suppressing approvals that have
 * already produced follow-up output.
 */
export function observeTaskApprovals<TApproval>(
  options: ObserveTaskApprovalsOptions<TApproval>,
): TaskApprovalObserver {
  const observability = new ModuleObservability(
    readModuleObservabilityOptions("TaskApprovalObserver"),
  );
  const processedApprovalIds = new Set<string>();
  const inFlightApprovalIds = new Set<string>();
  const replayedApprovals = new Map<string, TApproval>();
  const onError = options.onError ?? console.error;
  let activeProcessingCount = 0;
  let lastError: TaskApprovalObserverFailureInfo | null = null;
  let replayComplete = false;
  let stopped = false;

  /** Processes one approval unless it has already been handled. */
  const processApproval = async (
    approval: TApproval,
    eventPhase: Exclude<ApprovalEventPhase, "processed-output">,
  ): Promise<void> => {
    if (stopped) {
      observability.debug("processApproval", "approval.skipped_stopped", {
        eventPhase,
        outcome: "stopped",
      });
      return;
    }
    const approvalId = options.approvalId(approval);
    if (processedApprovalIds.has(approvalId)) {
      observability.debug("processApproval", "approval.skipped_processed", {
        approvalId,
        eventPhase,
        outcome: "skipped_processed",
        ...stateCounts(),
      });
      return;
    }
    if (inFlightApprovalIds.has(approvalId)) {
      observability.debug("processApproval", "approval.skipped_in_flight", {
        approvalId,
        eventPhase,
        outcome: "skipped_in_flight",
        ...stateCounts(),
      });
      return;
    }
    inFlightApprovalIds.add(approvalId);
    activeProcessingCount += 1;
    observability.debug("processApproval", "approval.started", {
      approvalId,
      eventPhase,
      outcome: "started",
      ...stateCounts(),
    });
    try {
      await observability.traceBoundary(
        "processApproval",
        { approvalId, eventPhase },
        async () => {
          try {
            await options.processApproval(approval);
          } catch (cause) {
            const error = new TaskApprovalProcessingError({
              approvalId,
              cause,
              operation: "processApproval",
            });
            lastError = {
              approvalId,
              message: cause instanceof Error ? cause.message : String(cause),
              name: cause instanceof Error ? cause.name : "UnknownError",
              operation: "processApproval",
            };
            throw error;
          }
        },
        () => ({ approvalId, outcome: "processed" }),
      );
      processedApprovalIds.add(approvalId);
      observability.debug("processApproval", "approval.processed", {
        approvalId,
        eventPhase,
        outcome: "processed",
        ...stateCounts(),
      });
    } finally {
      inFlightApprovalIds.delete(approvalId);
      activeProcessingCount -= 1;
    }
  };

  const unsubscribe = options.client.onEvent((event) => {
    const processedApprovalId = options.processedApprovalIdFromEvent(event);
    if (processedApprovalId) {
      processedApprovalIds.add(processedApprovalId);
      replayedApprovals.delete(processedApprovalId);
      observability.debug("onEvent", "approval.processed_output_seen", {
        approvalId: processedApprovalId,
        eventPhase: "processed-output",
        outcome: "processed",
        ...stateCounts(),
      });
      return;
    }
    const approval = options.parseApproval(event);
    if (!approval) {
      return;
    }
    if (!replayComplete) {
      replayedApprovals.set(options.approvalId(approval), approval);
      observability.debug("onEvent", "approval.buffered", {
        approvalId: options.approvalId(approval),
        eventPhase: "replay",
        outcome: "buffered",
        ...stateCounts(),
      });
      return;
    }
    void processApproval(approval, "live").catch(onError);
  });

  void options.client.waitForReplayComplete().then(() => {
    if (stopped) {
      return;
    }
    replayComplete = true;
    for (const approval of replayedApprovals.values()) {
      void processApproval(approval, "replay").catch(onError);
    }
    replayedApprovals.clear();
  });

  const stop: TaskApprovalObserver = Object.assign(
    () => {
      stopped = true;
      unsubscribe();
    },
    {
      debugInfo: (): TaskApprovalObserverDebugInfo => ({
        activeProcessingCount,
        inFlightApprovalIds: [...inFlightApprovalIds],
        lastError,
        moduleName: observability.debugInfo().moduleName,
        processedApprovalIds: [...processedApprovalIds],
        replayBufferCount: replayedApprovals.size,
      }),
    },
  );

  return stop;

  /** Returns local state counts for structured logs. */
  function stateCounts(): Record<string, number> {
    return {
      activeProcessingCount,
      inFlightApprovalCount: inFlightApprovalIds.size,
      processedApprovalCount: processedApprovalIds.size,
      replayBufferCount: replayedApprovals.size,
    };
  }
}
