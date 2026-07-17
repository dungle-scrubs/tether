import { sessionSummaryContentSchema } from "@dungle-scrubs/tether-protocol";
import { z } from "zod";

import type { SessionSummaryEvalCorpusCase } from "./corpus.js";
import {
  buildSessionSummaryEvalPrompt,
  sessionSummaryEvalSystemPrompt,
  sessionSummaryStructuredOutputSchema,
} from "./prompt.js";
import type { SessionSummaryEvalConfiguration, SessionSummaryEvalRun } from "./report.js";
import { scoreSessionSummaryCase } from "./scorer.js";

const ollamaChatResponseSchema = z.object({
  load_duration: z.number().nonnegative(),
  message: z.object({ content: z.string() }),
  total_duration: z.number().nonnegative(),
});

const ollamaProcessResponseSchema = z.object({
  models: z.array(
    z.object({
      name: z.string(),
      size: z.number().int().nonnegative(),
      size_vram: z.number().int().nonnegative(),
    }),
  ),
});

/** Direct-Ollama connection settings for one evaluation run. */
export interface SessionSummaryOllamaEvalOptions {
  readonly baseUrl: string;
  readonly requestTimeoutMs: number;
  readonly sampleIntervalMs: number;
}

/** Runs and scores one case repetition without retaining generated content. */
export async function runSessionSummaryEvalCase(
  configuration: SessionSummaryEvalConfiguration,
  evalCase: SessionSummaryEvalCorpusCase,
  repeat: number,
  cold: boolean,
  options: SessionSummaryOllamaEvalOptions,
): Promise<SessionSummaryEvalRun> {
  const memory = { peakMemoryBytes: 0, peakVramBytes: 0 };
  let sampling = true;
  const sampler = sampleOllamaMemory(configuration.identity.model, options, memory, () => sampling);
  const startedAt = performance.now();
  try {
    const response = await postJson(
      `${options.baseUrl}/api/chat`,
      {
        format: sessionSummaryStructuredOutputSchema,
        keep_alive: "5m",
        messages: [
          { content: sessionSummaryEvalSystemPrompt, role: "system" },
          { content: buildSessionSummaryEvalPrompt(evalCase), role: "user" },
        ],
        model: configuration.identity.model,
        options: {
          num_ctx: configuration.requestOptions.numContext,
          num_predict: configuration.requestOptions.numPredict,
          seed: configuration.requestOptions.seed,
          temperature: configuration.requestOptions.temperature,
        },
        stream: false,
        think: configuration.identity.thinkingMode === "enabled",
      },
      options.requestTimeoutMs,
    );
    const parsedResponse = ollamaChatResponseSchema.parse(response);
    const parsedContent = parseJson(parsedResponse.message.content);
    const score = scoreSessionSummaryCase(evalCase, parsedContent);
    await sampleOllamaMemoryOnce(configuration.identity.model, options, memory);
    return {
      caseId: evalCase.id,
      cold,
      failures: gateFailures(score.gates),
      ollamaDurationMs: nanosecondsToMilliseconds(parsedResponse.total_duration),
      ollamaLoadDurationMs: nanosecondsToMilliseconds(parsedResponse.load_duration),
      passed: score.passed,
      peakMemoryBytes: memory.peakMemoryBytes,
      peakVramBytes: memory.peakVramBytes,
      repeat,
      wallDurationMs: Math.round(performance.now() - startedAt),
    };
  } catch (error: unknown) {
    if (process.env.SESSION_SUMMARY_EVAL_DEBUG === "1") {
      process.stderr.write(`${error instanceof Error ? error.message : "unknown failure"}\n`);
    }
    return {
      caseId: evalCase.id,
      cold,
      failures: [boundedFailure(error)],
      ollamaDurationMs: 0,
      ollamaLoadDurationMs: 0,
      passed: false,
      peakMemoryBytes: memory.peakMemoryBytes,
      peakVramBytes: memory.peakVramBytes,
      repeat,
      wallDurationMs: Math.round(performance.now() - startedAt),
    };
  } finally {
    sampling = false;
    await sampler;
  }
}

/** Unloads a candidate so its first measured request includes cold-load cost. */
export async function unloadSessionSummaryEvalModel(
  model: string,
  options: SessionSummaryOllamaEvalOptions,
): Promise<void> {
  await postJson(
    `${options.baseUrl}/api/generate`,
    { keep_alive: 0, model },
    options.requestTimeoutMs,
  );
}

function gateFailures(
  gates: ReturnType<typeof scoreSessionSummaryCase>["gates"],
): readonly string[] {
  return Object.entries(gates).flatMap(([gate, result]) =>
    result.failures.map((failure) => `${gate}: ${failure}`),
  );
}

async function sampleOllamaMemory(
  model: string,
  options: SessionSummaryOllamaEvalOptions,
  memory: { peakMemoryBytes: number; peakVramBytes: number },
  shouldContinue: () => boolean,
): Promise<void> {
  while (shouldContinue()) {
    await sampleOllamaMemoryOnce(model, options, memory);
    await delay(options.sampleIntervalMs);
  }
}

async function sampleOllamaMemoryOnce(
  model: string,
  options: SessionSummaryOllamaEvalOptions,
  memory: { peakMemoryBytes: number; peakVramBytes: number },
): Promise<void> {
  try {
    const response = await fetch(`${options.baseUrl}/api/ps`, {
      signal: AbortSignal.timeout(Math.min(options.requestTimeoutMs, 1_000)),
    });
    const processes = ollamaProcessResponseSchema.parse(await response.json());
    const process = processes.models.find((entry) => entry.name === model);
    if (process) {
      memory.peakMemoryBytes = Math.max(memory.peakMemoryBytes, process.size);
      memory.peakVramBytes = Math.max(memory.peakVramBytes, process.size_vram);
    }
  } catch {
    // A missed sample is represented by zero and fails the resource gate.
  }
}

async function postJson(url: string, body: unknown, timeoutMs: number): Promise<unknown> {
  const response = await fetch(url, {
    body: JSON.stringify(body),
    headers: { "content-type": "application/json" },
    method: "POST",
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!response.ok) {
    const responseBody = await response.text();
    throw new Error(`Ollama returned HTTP ${response.status}: ${responseBody.slice(0, 512)}`);
  }
  return response.json();
}

function parseJson(value: string): unknown {
  const parsed: unknown = JSON.parse(value);
  return sessionSummaryContentSchema.parse(parsed);
}

function nanosecondsToMilliseconds(value: number): number {
  return Math.round(value / 1_000_000);
}

function boundedFailure(error: unknown): string {
  if (error instanceof Error) {
    return `runner: ${error.name}`.slice(0, 128);
  }
  return "runner: unknown_failure";
}

async function delay(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
}
