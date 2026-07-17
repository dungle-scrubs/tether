import type { SessionSummaryRecord } from "@dungle-scrubs/tether-protocol";

import { sessionContextMaintenancePolicies } from "../../src/session-context-maintenance-policy.js";
import {
  buildBoundedSessionContextView,
  classifySessionContextBudget,
} from "../../src/session-service-context.js";
import type { ParticipantTaskContractRecord, SessionEvent, TaskRecord } from "../../src/types.js";

const supportedBudgets = [2_000, 8_000, 16_000, 32_000] as const;

/** Builds the executable A-004 baseline from representative bounded inputs. */
export function buildSessionContextA004Report() {
  const events = Array.from({ length: 10_000 }, (_, index): SessionEvent => {
    const seq = index + 2;
    return {
      createdAt: "2026-07-17T00:00:00.000Z",
      eventId: `evt_${seq}`,
      payload: { text: seq === 10_001 ? "latest override" : `tail ${seq}` },
      producerId: "participant",
      seq,
      sessionId: "sess_a004",
      type: "user.message",
    };
  });
  const cases = supportedBudgets.map((budgetTokens) => {
    const input = {
      activeTasks,
      budgetTokens,
      eligibleEventCount: 10_001,
      events,
      forParticipant: "part_coordinator",
      latestSummary: {
        ...publishedSummary,
        budgetClass: classifySessionContextBudget(budgetTokens),
      },
      recentTerminalTasks: terminalTasks,
      sessionId: "sess_a004",
      taskContracts,
    } as const;
    const first = buildBoundedSessionContextView(input);
    const second = buildBoundedSessionContextView(input);
    return {
      activeTaskCount: first.activeTasks.length,
      budgetTokens,
      contractCount: first.taskContracts.length,
      deterministic: JSON.stringify(first) === JSON.stringify(second),
      estimatedTokens: first.budget.estimatedTokens,
      latestRawSeq: first.recentEvents.at(-1)?.seq ?? null,
      omittedEventCount: first.budget.omittedEventCount,
      summaryRetained: first.latestSummary?.summaryId === publishedSummary.summaryId,
      terminalTaskCount: first.recentTerminalTasks.length,
      withinBudget: first.budget.estimatedTokens <= budgetTokens,
    };
  });
  return {
    cases,
    modelCallsDuringRead: 0,
    reportVersion: "session-context-a004.v1" as const,
    selectedPolicy: sessionContextMaintenancePolicies,
    status: cases.every(
      (entry) =>
        entry.activeTaskCount > 0 &&
        entry.contractCount > 0 &&
        entry.deterministic &&
        entry.latestRawSeq === 10_001 &&
        entry.summaryRetained &&
        entry.terminalTaskCount > 0 &&
        entry.withinBudget,
    )
      ? ("passed" as const)
      : ("failed" as const),
    supportedBudgets,
  };
}

const publishedSummary: SessionSummaryRecord = {
  budgetClass: "2k",
  content: {
    facts: [
      {
        category: "decision",
        sourceEventIds: ["evt_1"],
        statement: "Use the durable task contracts.",
        subjectIds: ["task_lookup"],
      },
    ],
    headline: "Earlier durable decisions",
    narrative: "The coordinator selected contract-backed execution.",
    openQuestions: ["Confirm the latest raw override."],
  },
  coversSeqFrom: 1,
  coversSeqTo: 1,
  createdAt: "2026-07-17T00:00:00.000Z",
  failure: null,
  generationTaskId: "task_summary_a004",
  integrity: { algorithm: "sha256", hash: "a".repeat(64) },
  ollama: {
    contextSize: 32_768,
    model: "evaluated-local-model",
    quantization: "q4_k_m",
    revision: "revision-1",
    thinkingMode: "low",
  },
  outputSchemaVersion: "summary.v1",
  producer: { id: "summary-worker", version: "1.0.0" },
  promptVersion: "prompt.v1",
  publishedAt: "2026-07-17T00:01:00.000Z",
  quarantinedAt: null,
  sessionId: "sess_a004",
  source: {
    eventCount: 1,
    firstEventId: "evt_1",
    lastEventId: "evt_1",
    rangeHash: "b".repeat(64),
  },
  summaryId: "summary_a004",
  supersededAt: null,
  validatedAt: "2026-07-17T00:00:59.000Z",
};

const activeTasks = [task("task_lookup", "lookup"), task("task_verify", "verify")] as const;
const terminalTasks = [
  { ...task("task_prior", "prior"), completedAt: "2026-07-17T00:00:00.000Z" },
] as const;

const taskContracts = [
  contract("part_lookup", "lookup"),
  contract("part_verify", "verify"),
] as const;

/** Builds one compact representative task. */
function task(taskId: string, kind: string): TaskRecord {
  return {
    cancelledAt: null,
    claimExpiredAt: null,
    claimExpiredBy: null,
    claimExpiresAt: null,
    claimedAt: null,
    claimedBy: null,
    completedAt: null,
    createdAt: "2026-07-17T00:00:00.000Z",
    failedAt: null,
    failure: null,
    input: null,
    kind,
    objective: `Execute ${kind}`,
    releasedAt: null,
    releasedBy: null,
    result: null,
    sessionId: "sess_a004",
    taskId,
  };
}

/** Builds one representative participant contract. */
function contract(participantId: string, taskKind: string): ParticipantTaskContractRecord {
  return {
    approval: "none",
    description: `Execute ${taskKind}`,
    displayName: participantId,
    inputSchemaRef: `${taskKind}.input.v1`,
    participantId,
    participantRuntimeKind: "generic_agent",
    readOnlyByDefault: true,
    resultSchemaRef: `${taskKind}.result.v1`,
    runtimeKind: "generic_agent",
    sessionId: "sess_a004",
    taskKind,
    title: taskKind,
    version: "1",
  };
}
