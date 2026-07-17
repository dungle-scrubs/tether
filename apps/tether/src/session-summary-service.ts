import { randomUUID } from "node:crypto";

import type {
  SessionSummaryGenerationJob,
  SessionSummaryOllamaIdentity,
  SessionSummaryProducerIdentity,
} from "@dungle-scrubs/tether-protocol";

import type {
  SessionSummaryGenerationReservation,
  SessionSummaryStore,
} from "./session-summary-store.js";

/** Durable task kind reserved for external Session Summary generation workers. */
export const sessionSummaryGenerationTaskKind = "session_summary_generation";

/** Inputs selected by Tether before creating one generation reservation. */
export interface RequestSessionSummaryGenerationInput {
  readonly budgetClass: string;
  readonly deadlineAt: Date;
  readonly inputLimitBytes: number;
  readonly maxEventCount: number;
  readonly ollama: SessionSummaryOllamaIdentity;
  readonly outputLimitBytes: number;
  readonly outputSchemaVersion: string;
  readonly producer: SessionSummaryProducerIdentity;
  readonly promptVersion: string;
  readonly sessionId: string;
}

/** Service-owned generation request outcome. */
export type RequestSessionSummaryGenerationResult =
  | SessionSummaryGenerationReservation
  | { readonly job: SessionSummaryGenerationJob; readonly status: "created" };

/** Dependencies for the generation orchestration Module. */
export interface SessionSummaryGenerationServiceDependencies {
  readonly ids?: {
    readonly newSummaryId: () => string;
    readonly newTaskId: () => string;
  };
  readonly store: Pick<SessionSummaryStore, "selectAndReserveGeneration">;
}

/** Service-owned generation orchestration interface. */
export interface SessionSummaryGenerationService {
  readonly requestGeneration: (
    input: RequestSessionSummaryGenerationInput,
  ) => Promise<RequestSessionSummaryGenerationResult>;
}

/**
 * Creates the orchestration Module that reserves policy-selected coverage and
 * exposes it through Tether's existing durable task lifecycle.
 */
export function createSessionSummaryGenerationService(
  dependencies: SessionSummaryGenerationServiceDependencies,
): SessionSummaryGenerationService {
  const ids = dependencies.ids ?? {
    newSummaryId: () => `summary_${randomUUID()}`,
    newTaskId: () => `task_${randomUUID()}`,
  };
  return {
    requestGeneration: async (input) => {
      const reservation = await dependencies.store.selectAndReserveGeneration({
        ...input,
        summaryId: ids.newSummaryId(),
        taskId: ids.newTaskId(),
      });
      if (reservation.status !== "reserved") {
        return reservation;
      }
      return { job: reservation.job, status: "created" };
    },
  };
}
