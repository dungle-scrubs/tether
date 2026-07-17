import { mkdir, writeFile } from "node:fs/promises";
import { cpus, platform, totalmem } from "node:os";
import { dirname } from "node:path";

import { z } from "zod";

import { selectSessionSummaryPublication } from "../../src/session-summary-publication-config.js";
import { sessionSummaryEvalConfigurations, sessionSummaryEvalThresholds } from "./config.js";
import { sessionSummaryEvalCorpus, validateSessionSummaryEvalCorpus } from "./corpus.js";
import { runSessionSummaryEvalCase, unloadSessionSummaryEvalModel } from "./ollama-runner.js";
import {
  buildSessionSummaryEvalCandidate,
  type SessionSummaryEvalCandidateReport,
} from "./report.js";

const ollamaVersionSchema = z.object({ version: z.string().min(1) });
const ollamaTagsSchema = z.object({
  models: z.array(
    z.object({
      details: z.object({
        context_length: z.number().int().positive(),
        quantization_level: z.string().min(1),
      }),
      digest: z.string().regex(/^[0-9a-f]{64}$/u),
      name: z.string().min(1),
    }),
  ),
});

const options = {
  baseUrl: process.env.OLLAMA_BASE_URL ?? "http://127.0.0.1:11434",
  requestTimeoutMs: 60_000,
  sampleIntervalMs: 50,
} as const;

/** Complete machine-measured A-005 report. */
export interface SessionSummaryEvalBaselineReport {
  readonly candidates: readonly SessionSummaryEvalCandidateReport[];
  readonly corpusVersion: "session-summary-corpus.v1";
  readonly generatedAt: string;
  readonly ollamaVersion: string;
  readonly reportVersion: "session-summary-a005.v1";
  readonly selection: ReturnType<typeof selectSessionSummaryPublication>;
  readonly targetMachine: {
    readonly cpuModel: string;
    readonly logicalCpuCount: number;
    readonly platform: string;
    readonly totalMemoryBytes: number;
  };
  readonly thresholds: typeof sessionSummaryEvalThresholds;
}

/** Runs every available candidate against every sanitized hard-gate case. */
export async function runSessionSummaryEvaluation(): Promise<SessionSummaryEvalBaselineReport> {
  const corpusValidation = validateSessionSummaryEvalCorpus(sessionSummaryEvalCorpus);
  if (!corpusValidation.ok) {
    throw new Error(`Invalid Session Summary eval corpus: ${corpusValidation.issues.join(", ")}`);
  }
  const [versionResponse, tagsResponse] = await Promise.all([
    fetchJson(`${options.baseUrl}/api/version`),
    fetchJson(`${options.baseUrl}/api/tags`),
  ]);
  const version = ollamaVersionSchema.parse(versionResponse).version;
  const tags = ollamaTagsSchema.parse(tagsResponse).models;
  const candidates: SessionSummaryEvalCandidateReport[] = [];

  for (const configuration of sessionSummaryEvalConfigurations) {
    assertAvailableCandidate(configuration, tags);
    const runs = [];
    for (const evalCase of sessionSummaryEvalCorpus) {
      await unloadSessionSummaryEvalModel(configuration.identity.model, options);
      for (let repeat = 1; repeat <= sessionSummaryEvalThresholds.minimumRepeats; repeat += 1) {
        process.stderr.write(
          `session-summary eval ${configuration.identity.thinkingMode} ${evalCase.id} ${repeat}/${sessionSummaryEvalThresholds.minimumRepeats}\n`,
        );
        runs.push(
          await runSessionSummaryEvalCase(configuration, evalCase, repeat, repeat === 1, options),
        );
      }
    }
    candidates.push(buildSessionSummaryEvalCandidate({ configuration, runs }));
  }

  const cpu = cpus()[0];
  const reportWithoutSelection = {
    candidates,
    corpusVersion: "session-summary-corpus.v1" as const,
    generatedAt: new Date().toISOString(),
    ollamaVersion: version,
    reportVersion: "session-summary-a005.v1" as const,
    targetMachine: {
      cpuModel: cpu?.model ?? "unknown",
      logicalCpuCount: cpus().length,
      platform: platform(),
      totalMemoryBytes: totalmem(),
    },
    thresholds: sessionSummaryEvalThresholds,
  };
  return {
    ...reportWithoutSelection,
    selection: selectSessionSummaryPublication(reportWithoutSelection),
  };
}

function assertAvailableCandidate(
  configuration: (typeof sessionSummaryEvalConfigurations)[number],
  tags: z.infer<typeof ollamaTagsSchema>["models"],
): void {
  const model = tags.find((entry) => entry.name === configuration.identity.model);
  if (
    model === undefined ||
    `sha256:${model.digest}` !== configuration.identity.revision ||
    model.details.quantization_level !== configuration.identity.quantization ||
    model.details.context_length < configuration.identity.contextSize
  ) {
    throw new Error(`Pinned candidate unavailable: ${configuration.identity.model}`);
  }
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url, { signal: AbortSignal.timeout(options.requestTimeoutMs) });
  if (!response.ok) {
    throw new Error(`Ollama returned HTTP ${response.status}`);
  }
  return response.json();
}

if (import.meta.url === new URL(process.argv[1] ?? "", "file:").href) {
  const report = await runSessionSummaryEvaluation();
  const serialized = `${JSON.stringify(report, null, 2)}\n`;
  const outputFlag = process.argv.indexOf("--output");
  const outputPath = outputFlag === -1 ? undefined : process.argv[outputFlag + 1];
  if (outputPath === undefined) {
    process.stdout.write(serialized);
  } else {
    await mkdir(dirname(outputPath), { recursive: true });
    await writeFile(outputPath, serialized, "utf8");
  }
}
