import { taskContractSummarySchema } from "@dungle-scrubs/tether-protocol";
import type { SessionSummaryRecord } from "@dungle-scrubs/tether-protocol";

import type { SessionContextBudgetClass } from "./session-context-maintenance-policy.js";
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
  /** Total raw events eligible after summary coverage, including unmaterialized rows. */
  readonly eligibleEventCount?: number;
  /** Optional participant identity requesting the context. */
  readonly forParticipant: string | null;
  /** Latest active summary for the selected budget class, when one exists. */
  readonly latestSummary?: SessionSummaryRecord | null;
  /** Recent terminal tasks, newest first. */
  readonly recentTerminalTasks: readonly TaskRecord[];
  /** Durable session being projected. */
  readonly sessionId: string;
  /** Advertised task contracts visible in this session. */
  readonly taskContracts: readonly ParticipantTaskContractRecord[];
}

/** Supported context budget classes used for summary selection and evaluation. */
export const sessionContextBudgetClasses = ["2k", "8k", "16k", "32k"] as const;

/** Maps a clamped request budget to one stable supported summary budget class. */
export function classifySessionContextBudget(budgetTokens: number): SessionContextBudgetClass {
  if (budgetTokens < 8_000) {
    return "2k";
  }
  if (budgetTokens < 16_000) {
    return "8k";
  }
  if (budgetTokens < 32_000) {
    return "16k";
  }
  return "32k";
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
  const eligibleEventCount = input.eligibleEventCount ?? input.events.length;
  if (!Number.isSafeInteger(eligibleEventCount) || eligibleEventCount < input.events.length) {
    throw new Error("Session Context eligible event count is invalid");
  }
  const selectedSummary = canUseSessionSummaryForContext({
    budgetTokens: input.budgetTokens,
    forParticipant: input.forParticipant,
    sessionId: input.sessionId,
    summary: input.latestSummary ?? null,
  })
    ? projectContextSummary(input.latestSummary ?? null)
    : null;
  const baseContext = {
    forParticipant: input.forParticipant,
    kind: "session_context" as const,
    latestSummary: selectedSummary,
    mode: selectedSummary === null ? ("raw_only" as const) : ("summary_with_raw_tail" as const),
    sessionId: input.sessionId,
  };
  const emptyContext = {
    ...baseContext,
    activeTasks: [] as readonly TaskRecord[],
    recentEventRange: { endSeq: null, startSeq: null },
    recentEvents: [] as readonly SessionEvent[],
    recentTerminalTasks: [] as readonly TaskRecord[],
    taskContracts: [] as readonly ParticipantTaskContractRecord[],
  };
  const activeTasks = selectPrefixForBudget(
    input.activeTasks,
    input.budgetTokens,
    (items) => ({
      ...emptyContext,
      activeTasks: items,
    }),
    eligibleEventCount,
  );
  const activeContext = { ...emptyContext, activeTasks };
  const recentTerminalTasks = selectPrefixForBudget(
    input.recentTerminalTasks,
    input.budgetTokens,
    (items) => ({ ...activeContext, recentTerminalTasks: items }),
    eligibleEventCount,
  );
  const taskContext = { ...activeContext, recentTerminalTasks };
  const taskContracts = selectPrefixForBudget(
    input.taskContracts,
    input.budgetTokens,
    (items) => ({ ...taskContext, taskContracts: items }),
    eligibleEventCount,
  );
  const recentEvents = selectRecentEventsForBudget({
    budgetTokens: input.budgetTokens,
    eligibleEventCount,
    events: input.events,
    fixedContext: {
      ...baseContext,
      activeTasks,
      recentTerminalTasks,
      taskContracts,
    },
  });
  const omittedEventCount = eligibleEventCount - recentEvents.length;
  const contextWithoutBudget = {
    ...baseContext,
    activeTasks,
    recentEventRange: {
      endSeq: recentEvents.at(-1)?.seq ?? null,
      startSeq: recentEvents[0]?.seq ?? null,
    },
    recentEvents,
    recentTerminalTasks,
    taskContracts,
  };
  return {
    ...contextWithoutBudget,
    budget: buildContextBudget(contextWithoutBudget, omittedEventCount, input.budgetTokens),
  };
}

/** Decides whether one summary and the minimal response envelope fit the budget. */
export function canUseSessionSummaryForContext(input: {
  readonly budgetTokens: number;
  readonly forParticipant: string | null;
  readonly sessionId: string;
  readonly summary: SessionSummaryRecord | null;
}): boolean {
  const summary = projectContextSummary(input.summary);
  if (
    summary === null ||
    input.summary?.sessionId !== input.sessionId ||
    summary.budgetClass !== classifySessionContextBudget(input.budgetTokens)
  ) {
    return false;
  }
  const contextWithoutBudget: Omit<SessionContextView, "budget"> = {
    activeTasks: [],
    forParticipant: input.forParticipant,
    kind: "session_context",
    latestSummary: summary,
    mode: "summary_with_raw_tail",
    recentEventRange: { endSeq: null, startSeq: null },
    recentEvents: [],
    recentTerminalTasks: [],
    sessionId: input.sessionId,
    taskContracts: [],
  };
  return (
    buildContextBudget(contextWithoutBudget, 0, input.budgetTokens).estimatedTokens <=
    input.budgetTokens
  );
}

/** Projects only validated context-facing fields from one published record. */
function projectContextSummary(
  summary: SessionSummaryRecord | null,
): SessionContextView["latestSummary"] {
  if (summary === null || summary.content === null || summary.integrity === null) {
    return null;
  }
  return {
    budgetClass: summary.budgetClass,
    content: summary.content,
    coversSeqFrom: summary.coversSeqFrom,
    coversSeqTo: summary.coversSeqTo,
    integrity: summary.integrity,
    ollama: summary.ollama,
    outputSchemaVersion: summary.outputSchemaVersion,
    producer: summary.producer,
    promptVersion: summary.promptVersion,
    source: summary.source,
    summaryId: summary.summaryId,
  };
}

/** Selects the newest sequence-ordered events that fit an approximate budget. */
function selectRecentEventsForBudget(input: {
  readonly budgetTokens: number;
  readonly eligibleEventCount: number;
  readonly events: readonly SessionEvent[];
  readonly fixedContext: Omit<SessionContextView, "budget" | "recentEventRange" | "recentEvents">;
}): readonly SessionEvent[] {
  if (input.budgetTokens <= 0) {
    return [];
  }
  const selected: SessionEvent[] = [];
  for (let index = input.events.length - 1; index >= 0; index -= 1) {
    const event = input.events[index];
    if (!event) {
      continue;
    }
    const proposed = [event, ...selected];
    const contextWithoutBudget = {
      ...input.fixedContext,
      recentEventRange: {
        endSeq: proposed.at(-1)?.seq ?? null,
        startSeq: proposed[0]?.seq ?? null,
      },
      recentEvents: proposed,
    };
    const budget = buildContextBudget(
      contextWithoutBudget,
      input.eligibleEventCount - proposed.length,
      input.budgetTokens,
    );
    if (budget.estimatedTokens > input.budgetTokens) {
      break;
    }
    selected.unshift(event);
  }
  return selected;
}

/** Selects one deterministic prefix after higher-priority context has been packed. */
function selectPrefixForBudget<TItem>(
  items: readonly TItem[],
  budgetTokens: number,
  buildContext: (selected: readonly TItem[]) => Omit<SessionContextView, "budget">,
  omittedEventCount: number,
): readonly TItem[] {
  const selected: TItem[] = [];
  for (const item of items) {
    const proposed = [...selected, item];
    const budget = buildContextBudget(buildContext(proposed), omittedEventCount, budgetTokens);
    if (budget.estimatedTokens > budgetTokens) {
      break;
    }
    selected.push(item);
  }
  return selected;
}

/** Computes stable accounting including the budget record itself. */
function buildContextBudget(
  contextWithoutBudget: Omit<SessionContextView, "budget">,
  omittedEventCount: number,
  requestedTokens: number,
): SessionContextView["budget"] {
  let estimatedTokens = 0;
  for (;;) {
    const nextEstimate = estimateSessionContextTokens({
      ...contextWithoutBudget,
      budget: { estimatedTokens, omittedEventCount, requestedTokens },
    });
    if (nextEstimate === estimatedTokens) {
      return { estimatedTokens, omittedEventCount, requestedTokens };
    }
    estimatedTokens = nextEstimate;
  }
}

/** Estimates token usage from JSON size for deterministic budget packing. */
export function estimateSessionContextTokens(value: unknown): number {
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
