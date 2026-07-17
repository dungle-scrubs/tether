import type { SessionSummaryRecord } from "@dungle-scrubs/tether-protocol";
import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { buildSessionContextA004Report } from "../evals/session-context/report.js";
import {
  buildBoundedSessionContextView,
  canUseSessionSummaryForContext,
  classifySessionContextBudget,
} from "../src/session-service-context.js";
import type { SessionEvent } from "../src/types.js";

const publishedSummary: SessionSummaryRecord = {
  budgetClass: "8k",
  content: {
    facts: [],
    headline: "Earlier session state",
    narrative: "The durable state before the exact raw suffix.",
    openQuestions: [],
  },
  coversSeqFrom: 1,
  coversSeqTo: 2,
  createdAt: "2026-07-17T00:00:00.000Z",
  failure: null,
  generationTaskId: "task_summary_1",
  integrity: {
    algorithm: "sha256",
    hash: "a".repeat(64),
  },
  ollama: {
    contextSize: 32_768,
    model: "local-model",
    quantization: "q4_k_m",
    revision: "revision-1",
    thinkingMode: "low",
  },
  outputSchemaVersion: "summary.v1",
  producer: { id: "summary-worker", version: "1.0.0" },
  promptVersion: "prompt.v1",
  publishedAt: "2026-07-17T00:01:00.000Z",
  quarantinedAt: null,
  sessionId: "sess_context",
  source: {
    eventCount: 2,
    firstEventId: "evt_1",
    lastEventId: "evt_2",
    rangeHash: "b".repeat(64),
  },
  summaryId: "summary_1",
  supersededAt: null,
  validatedAt: "2026-07-17T00:00:59.000Z",
};

const suffixEvents = [
  {
    createdAt: "2026-07-17T00:02:00.000Z",
    eventId: "evt_3",
    payload: { text: "later" },
    seq: 3,
    sessionId: "sess_context",
    producerId: "participant_1",
    type: "user.message",
  },
  {
    createdAt: "2026-07-17T00:03:00.000Z",
    eventId: "evt_4",
    payload: { text: "latest" },
    seq: 4,
    sessionId: "sess_context",
    producerId: "participant_1",
    type: "user.message",
  },
] as const satisfies readonly SessionEvent[];

describe("bounded Session Context views", () => {
  it.each([
    [1_000, "2k"],
    [7_999, "2k"],
    [8_000, "8k"],
    [12_000, "8k"],
    [16_000, "16k"],
    [32_000, "32k"],
    [200_000, "32k"],
  ] as const)("selects the largest supported class within %i tokens", (tokens, budgetClass) => {
    expect(classifySessionContextBudget(tokens)).toBe(budgetClass);
  });

  it("packs a published summary before its exact raw suffix", () => {
    const context = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens: 8_000,
      events: suffixEvents,
      forParticipant: null,
      latestSummary: publishedSummary,
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(context.latestSummary).toMatchObject({
      budgetClass: "8k",
      content: publishedSummary.content,
      coversSeqFrom: 1,
      coversSeqTo: 2,
      integrity: publishedSummary.integrity,
      ollama: publishedSummary.ollama,
      producer: publishedSummary.producer,
      summaryId: "summary_1",
    });
    expect(context.recentEvents.map((event) => event.seq)).toEqual([3, 4]);
    expect(context.recentEventRange).toEqual({ startSeq: 3, endSeq: 4 });
    expect(context.budget.omittedEventCount).toBe(0);
    expect(context.mode).toBe("summary_with_raw_tail");
  });

  it("falls back to deterministic raw-only packing when no summary exists", () => {
    const context = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens: 8_000,
      events: suffixEvents,
      forParticipant: null,
      latestSummary: null,
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(context.mode).toBe("raw_only");
    expect(context.latestSummary).toBeNull();
    expect(context.recentEvents.map((event) => event.seq)).toEqual([3, 4]);
  });

  it("keeps summary metadata and omits the eligible suffix when the summary dominates", () => {
    const dominantSummary: SessionSummaryRecord = {
      ...publishedSummary,
      budgetClass: "2k",
      content: {
        facts: [],
        headline: "Dominant earlier state",
        narrative: "x".repeat(6_000),
        openQuestions: [],
      },
    };
    const summaryBudget = Array.from({ length: 7_999 }, (_, index) => index + 1).find(
      (budgetTokens) =>
        canUseSessionSummaryForContext({
          budgetTokens,
          forParticipant: null,
          sessionId: "sess_context",
          summary: dominantSummary,
        }),
    );
    if (summaryBudget === undefined) {
      throw new Error("Expected the bounded summary to fit one supported budget");
    }
    const context = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens: summaryBudget,
      events: suffixEvents,
      forParticipant: null,
      latestSummary: dominantSummary,
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(context.latestSummary?.summaryId).toBe("summary_1");
    expect(context.recentEvents).toEqual([]);
    expect(context.recentEventRange).toEqual({ startSeq: null, endSeq: null });
    expect(context.budget.omittedEventCount).toBe(2);
    expect(context.budget.estimatedTokens).toBeLessThanOrEqual(summaryBudget);
  });

  it("uses raw-only fallback when a published summary cannot fit a tiny budget", () => {
    const context = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens: 200,
      events: suffixEvents,
      forParticipant: null,
      latestSummary: publishedSummary,
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(context.latestSummary).toBeNull();
    expect(context.mode).toBe("raw_only");
    expect(context.budget.estimatedTokens).toBeLessThanOrEqual(200);
  });

  it.each([
    ["wrong budget class", { ...publishedSummary, budgetClass: "16k" }],
    ["wrong session", { ...publishedSummary, sessionId: "sess_other" }],
  ] as const)("rejects a published summary for the %s", (_label, latestSummary) => {
    const context = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens: 8_000,
      events: suffixEvents,
      forParticipant: null,
      latestSummary,
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(context.latestSummary).toBeNull();
    expect(context.mode).toBe("raw_only");
  });

  it("selects the newest contiguous suffix without changing sequence order", () => {
    const boundaryEvents = [
      { ...suffixEvents[0], payload: { text: "x".repeat(2_000) } },
      suffixEvents[1],
    ] as const;
    const context = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens: 500,
      events: boundaryEvents,
      forParticipant: null,
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(context.recentEvents.map((event) => event.seq)).toEqual([4]);
    expect(context.recentEventRange).toEqual({ startSeq: 4, endSeq: 4 });
    expect(context.budget.omittedEventCount).toBe(1);
    expect(context.budget.estimatedTokens).toBeLessThanOrEqual(context.budget.requestedTokens);
  });

  it.each([
    2_000, 8_000, 16_000, 32_000,
  ] as const)("passes the A-004 context invariants at a %i-token budget", (budgetTokens) => {
    const first = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens,
      events: suffixEvents,
      forParticipant: null,
      latestSummary: {
        ...publishedSummary,
        budgetClass: classifySessionContextBudget(budgetTokens),
      },
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });
    const second = buildBoundedSessionContextView({
      activeTasks: [],
      budgetTokens,
      events: suffixEvents,
      forParticipant: null,
      latestSummary: {
        ...publishedSummary,
        budgetClass: classifySessionContextBudget(budgetTokens),
      },
      recentTerminalTasks: [],
      sessionId: "sess_context",
      taskContracts: [],
    });

    expect(second).toEqual(first);
    expect(first.latestSummary?.content.headline).toBe("Earlier session state");
    expect(first.recentEvents.at(-1)?.payload).toEqual({ text: "latest" });
    expect(first.budget.estimatedTokens).toBeLessThanOrEqual(budgetTokens);
  });

  it("binds A-004 to the committed supported budgets and hysteresis policy", () => {
    const report: unknown = JSON.parse(
      readFileSync(
        new URL("../evals/session-context/reports/a-004-baseline.json", import.meta.url),
        "utf8",
      ),
    );

    expect(report).toEqual(buildSessionContextA004Report());
  });

  it("keeps context construction isolated from handled-event cursor ownership", () => {
    const contextSources = ["session-service-context.ts", "session-service-read-effects.ts"]
      .map((fileName) => readFileSync(new URL(`../src/${fileName}`, import.meta.url), "utf8"))
      .join("\n");

    expect(contextSources).not.toMatch(
      /ParticipantCursorWriter|cursorStore|lastHandledSeq|pendingCursorSeq/u,
    );
  });
});
