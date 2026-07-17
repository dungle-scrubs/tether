import type { SessionSummaryEvalConfiguration } from "./report.js";

/** Qualification thresholds selected before the A-005 candidate run. */
export const sessionSummaryEvalThresholds = {
  maximumP95LatencyMs: 30_000,
  maximumPeakMemoryBytes: 2_147_483_648,
  minimumRepeats: 3,
  minimumStability: 1,
} as const;

const baseConfiguration = {
  outputSchemaVersion: "session-summary.v1",
  promptVersion: "session-summary.v1",
  requestOptions: {
    numContext: 32_768,
    numPredict: 2_048,
    seed: 33,
    temperature: 0,
  },
} as const;

/** Available fully pinned direct-Ollama configurations evaluated by A-005. */
export const sessionSummaryEvalConfigurations = [
  {
    ...baseConfiguration,
    identity: {
      contextSize: 32_768,
      model: "qwen3:0.6b",
      quantization: "Q4_K_M",
      revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
      thinkingMode: "disabled",
    },
  },
  {
    ...baseConfiguration,
    identity: {
      contextSize: 32_768,
      model: "qwen3:0.6b",
      quantization: "Q4_K_M",
      revision: "sha256:7df6b6e09427a769808717c0a93cadc4ae99ed4eb8bf5ca557c90846becea435",
      thinkingMode: "enabled",
    },
  },
] as const satisfies readonly SessionSummaryEvalConfiguration[];
