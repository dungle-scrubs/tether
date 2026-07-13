import { taskContractSummarySchema } from "@dungle-scrubs/tether-protocol";

import type {
  ParticipantRecord,
  ParticipantTaskContractRecord,
  SessionContextView,
  SessionEvent,
  TaskRecord,
} from "./types.js";

/** Input for deterministic context packet packing. */
export interface BoundedSessionContextViewInput {
  /** Active, claimed, or otherwise non-terminal tasks. */
  readonly activeTasks: readonly TaskRecord[];
  /** Approximate token budget for the returned context packet. */
  readonly budgetTokens: number;
  /** Candidate raw events in sequence order. */
  readonly events: readonly SessionEvent[];
  /** Optional participant identity requesting the context. */
  readonly forParticipant: string | null;
  /** Recent terminal tasks, newest first. */
  readonly recentTerminalTasks: readonly TaskRecord[];
  /** Durable session being projected. */
  readonly sessionId: string;
  /** Advertised task contracts visible in this session. */
  readonly taskContracts: readonly ParticipantTaskContractRecord[];
}

/**
 * Projects participant-owned contract summaries into the common Tether
 * discovery shape used by clients and future coordinators.
 */
export function buildParticipantTaskContracts(
  participants: readonly ParticipantRecord[],
): ParticipantTaskContractRecord[] {
  return participants
    .flatMap((participant) => {
      const contracts = participant.capabilities.contracts;
      if (!Array.isArray(contracts)) {
        return [];
      }
      return contracts.flatMap((contract): readonly ParticipantTaskContractRecord[] => {
        const parsed = taskContractSummarySchema.safeParse(contract);
        if (!parsed.success) {
          return [];
        }
        const {
          inputJsonSchema: _inputJsonSchema,
          resultJsonSchema: _resultJsonSchema,
          ...summary
        } = parsed.data;
        return [
          {
            ...summary,
            displayName: participant.displayName,
            ...readInlineJsonSchemas(contract),
            participantId: participant.participantId,
            runtimeKind: participant.runtimeKind,
            sessionId: participant.sessionId,
          },
        ];
      });
    })
    .sort(compareParticipantTaskContracts);
}

/** Packs durable state into a context packet without model summarization. */
export function buildBoundedSessionContextView(
  input: BoundedSessionContextViewInput,
): SessionContextView {
  const fixedContext = {
    activeTasks: input.activeTasks,
    forParticipant: input.forParticipant,
    kind: "session_context" as const,
    latestSummary: null,
    recentTerminalTasks: input.recentTerminalTasks,
    sessionId: input.sessionId,
    taskContracts: input.taskContracts,
  };
  const fixedTokens = estimateJsonTokens(fixedContext);
  const availableEventTokens = Math.max(0, input.budgetTokens - fixedTokens);
  const recentEvents = selectRecentEventsForBudget(input.events, availableEventTokens);
  const contextWithoutBudget = {
    ...fixedContext,
    recentEventRange: {
      endSeq: recentEvents.at(-1)?.seq ?? null,
      startSeq: recentEvents[0]?.seq ?? null,
    },
    recentEvents,
  };
  return {
    ...contextWithoutBudget,
    budget: {
      estimatedTokens: estimateJsonTokens(contextWithoutBudget),
      omittedEventCount: input.events.length - recentEvents.length,
      requestedTokens: input.budgetTokens,
    },
  };
}

/** Selects the newest sequence-ordered events that fit an approximate budget. */
function selectRecentEventsForBudget(
  events: readonly SessionEvent[],
  budgetTokens: number,
): readonly SessionEvent[] {
  if (budgetTokens <= 0) {
    return [];
  }
  const selected: SessionEvent[] = [];
  let usedTokens = 0;
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (!event) {
      continue;
    }
    const eventTokens = estimateJsonTokens(event);
    if (selected.length > 0 && usedTokens + eventTokens > budgetTokens) {
      break;
    }
    if (selected.length === 0 && eventTokens > budgetTokens) {
      break;
    }
    selected.unshift(event);
    usedTokens += eventTokens;
  }
  return selected;
}

/** Estimates token usage from JSON size for deterministic budget packing. */
function estimateJsonTokens(value: unknown): number {
  return Math.ceil(JSON.stringify(value).length / 4);
}

/**
 * Reads optional inline schema data from an advertised contract. Participants
 * may publish schemas beside stable refs so coordinators and clients can
 * inspect task interfaces without importing domain packages.
 */
function readInlineJsonSchemas(contract: unknown): {
  readonly inputJsonSchema?: Record<string, unknown>;
  readonly resultJsonSchema?: Record<string, unknown>;
} {
  if (typeof contract !== "object" || contract === null) {
    return {};
  }
  const inputJsonSchema = (contract as Readonly<Record<string, unknown>>).inputJsonSchema;
  const resultJsonSchema = (contract as Readonly<Record<string, unknown>>).resultJsonSchema;
  return {
    ...(isJsonSchemaRecord(inputJsonSchema)
      ? { inputJsonSchema: inputJsonSchema as Record<string, unknown> }
      : {}),
    ...(isJsonSchemaRecord(resultJsonSchema)
      ? { resultJsonSchema: resultJsonSchema as Record<string, unknown> }
      : {}),
  };
}

/** Returns true when a value is a lightweight JSON Schema object. */
function isJsonSchemaRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Orders contract discovery results deterministically for client rendering. */
function compareParticipantTaskContracts(
  left: ParticipantTaskContractRecord,
  right: ParticipantTaskContractRecord,
): number {
  return (
    left.participantId.localeCompare(right.participantId) ||
    left.taskKind.localeCompare(right.taskKind) ||
    left.version.localeCompare(right.version)
  );
}
