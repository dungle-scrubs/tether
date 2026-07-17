import { describe, expect, it } from "vitest";

import {
  sessionSummaryEvalCorpus,
  validateSessionSummaryEvalCorpus,
} from "../evals/session-summary/corpus.js";

describe("Session Summary evaluation corpus", () => {
  it("contains valid contiguous ranges covering every required behavior family", () => {
    const validation = validateSessionSummaryEvalCorpus(sessionSummaryEvalCorpus);
    const eventTypes = new Set(
      sessionSummaryEvalCorpus.flatMap((evalCase) =>
        [...evalCase.previousEvents, ...evalCase.rangeEvents, ...evalCase.laterTail].map(
          (event) => event.type,
        ),
      ),
    );

    expect(validation).toEqual({ ok: true });
    expect([...eventTypes]).toEqual(
      expect.arrayContaining([
        "approval.decided",
        "assistant.started",
        "participant.registered",
        "scheduled.superseded",
        "task.completed",
        "user.command",
        "user.decision",
        "user.message",
      ]),
    );
    expect(sessionSummaryEvalCorpus.some((evalCase) => evalCase.laterTail.length > 0)).toBe(true);
    expect(sessionSummaryEvalCorpus.some((evalCase) => evalCase.sensitiveValues.length > 0)).toBe(
      true,
    );
  });
});
