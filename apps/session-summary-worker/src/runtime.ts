import {
  type ParticipantTaskExecutor,
  type RunParticipantRuntimeInput,
  runParticipantRuntime,
} from "@dungle-scrubs/tether-client";
import {
  deriveSessionSummaryCorrelationId,
  deriveSessionSummaryCandidateConfigurationId,
  sessionScalabilitySpanNames,
  sessionSummaryGenerationJobSchema,
} from "@dungle-scrubs/tether-protocol";
import { SpanStatusCode, trace } from "@opentelemetry/api";

import type { EvaluatedSessionSummarySelection } from "./executor.js";
import { BoundedExecutionPool, type ExecutionPoolDebugInfo } from "./execution-pool.js";
import { SessionSummaryWorkerError } from "./errors.js";

/** Narrow runner seam used by production and orchestration tests. */
export type ParticipantRuntimeRunner = (input: RunParticipantRuntimeInput) => Promise<void>;

/** Validated standalone worker runtime configuration. */
export interface SessionSummaryWorkerRuntimeConfig {
  readonly afterSeq: number;
  readonly authToken: string;
  readonly claimRefreshMs: number;
  readonly concurrency: number;
  readonly displayName: string;
  readonly instanceId: string;
  readonly participantId: string;
  readonly queueSize: number;
  readonly serviceUrl: string;
  readonly sessionId: string;
}

/** Content-free worker runtime diagnostics. */
export interface SessionSummaryWorkerDebugInfo extends ExecutionPoolDebugInfo {
  readonly failed: number;
  readonly lastDurationMs: number | null;
  readonly lastFailureCode: string | null;
  readonly started: number;
  readonly succeeded: number;
}

/**
 * Standalone Session Summary participant runtime.
 *
 * This Module owns worker orchestration, admission, and safe observability. The
 * shared client owns replay, claims, refresh, cancellation, and completion.
 */
export class SessionSummaryWorkerRuntime {
  readonly #config: SessionSummaryWorkerRuntimeConfig;
  readonly #executor: ParticipantTaskExecutor;
  readonly #pool: BoundedExecutionPool;
  readonly #runner: ParticipantRuntimeRunner;
  readonly #selection: EvaluatedSessionSummarySelection;
  #failed = 0;
  #lastDurationMs: number | null = null;
  #lastFailureCode: string | null = null;
  #started = 0;
  #succeeded = 0;

  constructor(input: {
    readonly config: SessionSummaryWorkerRuntimeConfig;
    readonly executor: ParticipantTaskExecutor;
    readonly runner?: ParticipantRuntimeRunner;
    readonly selection: EvaluatedSessionSummarySelection;
  }) {
    this.#config = validateConfig(input.config);
    this.#executor = input.executor;
    this.#pool = new BoundedExecutionPool({
      concurrency: input.config.concurrency,
      queueSize: input.config.queueSize,
    });
    this.#runner = input.runner ?? runParticipantRuntime;
    this.#selection = input.selection;
  }

  /** Runs until the shared participant runtime exits. */
  async run(): Promise<{ readonly reason?: string; readonly status: "disabled" | "stopped" }> {
    if (this.#selection.status === "disabled") {
      return { reason: this.#selection.reason, status: "disabled" };
    }
    await this.#runner({
      afterSeq: this.#config.afterSeq,
      authToken: this.#config.authToken,
      capabilities: { workKinds: ["session_summary_generation"] },
      claimRefreshMs: this.#config.claimRefreshMs,
      displayName: this.#config.displayName,
      executor: (context) => this.#execute(context),
      instanceId: this.#config.instanceId,
      participantId: this.#config.participantId,
      runtimeKind: "generic_agent",
      serviceUrl: this.#config.serviceUrl,
      sessionId: this.#config.sessionId,
      shouldClaimTask: (task) => sessionSummaryGenerationJobSchema.safeParse(task.input).success,
      workKinds: ["session_summary_generation"],
    });
    return { status: "stopped" };
  }

  /** Returns only bounded counts, duration, and failure codes. */
  debugInfo(): SessionSummaryWorkerDebugInfo {
    return {
      ...this.#pool.debugInfo(),
      failed: this.#failed,
      lastDurationMs: this.#lastDurationMs,
      lastFailureCode: this.#lastFailureCode,
      started: this.#started,
      succeeded: this.#succeeded,
    };
  }

  async #execute(
    context: Parameters<ParticipantTaskExecutor>[0],
  ): Promise<Awaited<ReturnType<ParticipantTaskExecutor>>> {
    const parsedJob = sessionSummaryGenerationJobSchema.safeParse(context.task.input);
    return trace.getTracer("session-summary-worker").startActiveSpan(
      sessionScalabilitySpanNames.summaryWorkerHandle,
      {
        attributes: {
          ...(parsedJob.success
            ? {
                "summary.correlation_id": deriveSessionSummaryCorrelationId(
                  parsedJob.data.summaryId,
                ),
                "summary.candidate_configuration_id": deriveSessionSummaryCandidateConfigurationId(
                  parsedJob.data.ollama,
                ),
                "summary.range_size": parsedJob.data.range.to - parsedJob.data.range.from + 1,
              }
            : {}),
          "worker.queue.bounded": true,
        },
      },
      async (span) => {
        const startedAt = performance.now();
        this.#started += 1;
        try {
          const result = await this.#pool.run(() => this.#executor(context), context.signal);
          this.#succeeded += 1;
          span.setStatus({ code: SpanStatusCode.OK });
          return result;
        } catch (error) {
          this.#failed += 1;
          this.#lastFailureCode =
            error instanceof SessionSummaryWorkerError ? error.code : "unknown_failure";
          span.setAttribute("worker.failure.code", this.#lastFailureCode);
          span.setStatus({ code: SpanStatusCode.ERROR });
          throw error;
        } finally {
          this.#lastDurationMs = Math.round(performance.now() - startedAt);
          span.end();
        }
      },
    );
  }
}

function validateConfig(
  config: SessionSummaryWorkerRuntimeConfig,
): SessionSummaryWorkerRuntimeConfig {
  boundedPositiveInteger(config.claimRefreshMs, "claimRefreshMs", 300_000);
  boundedPositiveInteger(config.concurrency, "concurrency", 64);
  boundedPositiveInteger(config.queueSize, "queueSize", 1_024);
  if (!Number.isSafeInteger(config.afterSeq) || config.afterSeq < 0) {
    throw new TypeError("afterSeq must be a non-negative safe integer");
  }
  return { ...config };
}

function boundedPositiveInteger(value: number, name: string, maximum: number): number {
  if (!Number.isSafeInteger(value) || value <= 0 || value > maximum) {
    throw new TypeError(`${name} must be between 1 and ${maximum}`);
  }
  return value;
}
