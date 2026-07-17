import type { SessionSummaryGenerationJob } from "@dungle-scrubs/tether-protocol";
import { describe, expect, it } from "vitest";

import { createSessionSummaryGenerationService } from "../src/session-summary-service.js";
import type { SelectAndReserveSessionSummaryGenerationInput } from "../src/session-summary-store.js";

describe("Session Summary generation service", () => {
  it("creates a service-owned task with the protocol generation job as input", async () => {
    const job = generationJob();
    const reservations: SelectAndReserveSessionSummaryGenerationInput[] = [];
    const store = {
      selectAndReserveGeneration: async (input: SelectAndReserveSessionSummaryGenerationInput) => {
        reservations.push(input);
        return { job, status: "reserved" as const };
      },
    };
    const service = createSessionSummaryGenerationService({
      ids: {
        newSummaryId: () => job.summaryId,
        newTaskId: () => job.taskId,
      },
      store,
    });

    const result = await service.requestGeneration({
      budgetClass: "standard",
      deadlineAt: new Date(job.deadlineAt),
      inputLimitBytes: job.inputLimitBytes,
      maxEventCount: 20,
      ollama: job.ollama,
      outputLimitBytes: job.outputLimitBytes,
      outputSchemaVersion: job.outputSchemaVersion,
      producer: job.producer,
      promptVersion: job.promptVersion,
      sessionId: job.sessionId,
    });

    expect(result).toEqual({ job, status: "created" });
    expect(reservations).toHaveLength(1);
  });
});

function generationJob(): SessionSummaryGenerationJob {
  return {
    budgetClass: "standard",
    deadlineAt: "2026-07-17T00:05:00.000Z",
    expectedPrevious: { coversSeqTo: null, summaryId: null },
    inputLimitBytes: 262_144,
    kind: "session_summary.generate.v1",
    ollama: {
      contextSize: 32_768,
      model: "qwen3:8b",
      quantization: "Q4_K_M",
      revision: "sha256:model-revision",
      thinkingMode: "enabled",
    },
    outputLimitBytes: 32_768,
    outputSchemaVersion: "session-summary.v1",
    previousSummary: null,
    producer: { id: "session-summary-worker", version: "1.0.0" },
    promptVersion: "session-summary-prompt.v1",
    range: { from: 1, to: 20 },
    sessionId: "sess_1",
    source: {
      eventCount: 20,
      firstEventId: "evt_1",
      lastEventId: "evt_20",
      rangeHash: "b".repeat(64),
    },
    summaryId: "summary_1",
    taskId: "task_summary_1",
  };
}
