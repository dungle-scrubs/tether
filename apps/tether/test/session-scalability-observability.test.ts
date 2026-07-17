import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

describe("session scalability observability contract", () => {
  it("uses the protocol span vocabulary and bounded correlation at every summary boundary", async () => {
    const [projection, context, summaryStore, worker, ollama] = await Promise.all([
      source("../src/db-session-projections.ts"),
      source("../src/session-service-read-effects.ts"),
      source("../src/session-summary-store.ts"),
      source("../../session-summary-worker/src/runtime.ts"),
      source("../../session-summary-worker/src/ollama.ts"),
    ]);

    expect(projection).toContain("sessionScalabilitySpanNames.projectionApply");
    expect(projection).toContain("sessionScalabilitySpanNames.projectionBackfill");
    expect(projection).toContain("sessionScalabilitySpanNames.projectionVerify");
    expect(context).toContain("sessionScalabilitySpanNames.contextBuild");
    expect(summaryStore).toContain("sessionScalabilitySpanNames.summaryRangeSelect");
    expect(summaryStore).toContain("sessionScalabilitySpanNames.summaryJobCreate");
    expect(summaryStore).toContain("sessionScalabilitySpanNames.summaryCandidateSubmit");
    expect(summaryStore).toContain("sessionScalabilitySpanNames.summaryPublish");
    expect(worker).toContain("sessionScalabilitySpanNames.summaryWorkerHandle");
    expect(ollama).toContain("sessionScalabilitySpanNames.ollamaGenerate");
    expect(`${summaryStore}\n${worker}\n${ollama}`).toContain("deriveSessionSummaryCorrelationId");

    const scalabilitySources = [projection, context, summaryStore, worker, ollama].join("\n");
    expect(scalabilitySources).not.toMatch(
      /"(?:event\.payload|session\.id|summary\.content|summary\.id|task\.id)"/u,
    );
  });
});

async function source(relativePath: string): Promise<string> {
  return readFile(new URL(relativePath, import.meta.url), "utf8");
}
