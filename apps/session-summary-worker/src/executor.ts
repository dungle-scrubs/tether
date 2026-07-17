import { createHash } from "node:crypto";

import type { ParticipantTaskExecutor } from "@dungle-scrubs/tether-client";
import {
  canonicalizeSessionSummaryContent,
  type SessionEvent,
  type SessionSummaryGenerationJob,
  sessionSummaryCandidateSubmissionSchema,
  sessionSummaryGenerationJobSchema,
} from "@dungle-scrubs/tether-protocol";

import { abortError, SessionSummaryWorkerError } from "./errors.js";
import type { OllamaClient } from "./ollama.js";
import type { TetherApiClient } from "./tether-api.js";

/** Evaluated candidate selection consumed by the worker executor. */
export type EvaluatedSessionSummarySelection =
  | {
      readonly candidate: {
        readonly identity: SessionSummaryGenerationJob["ollama"];
        readonly outputSchemaVersion: string;
        readonly promptVersion: string;
      };
      readonly status: "enabled";
    }
  | { readonly reason: string; readonly status: "disabled" };

/** Dependencies and poison-range policy for one worker executor. */
export interface SessionSummaryExecutorOptions {
  readonly evaluatedSelection: EvaluatedSessionSummarySelection;
  readonly maxExecutionMs: number;
  readonly ollama: OllamaClient;
  readonly tether: TetherApiClient;
}

/** Builds the task executor supplied to the shared participant runtime. */
export function createSessionSummaryExecutor(
  options: SessionSummaryExecutorOptions,
): ParticipantTaskExecutor {
  const maxExecutionMs = boundedPositiveInteger(
    options.maxExecutionMs,
    "maxExecutionMs",
    3_600_000,
  );
  return async (context) => {
    if (options.evaluatedSelection.status !== "enabled") {
      throw new SessionSummaryWorkerError(
        "generation_unavailable",
        "Session Summary generation is disabled by evaluated policy",
      );
    }
    const jobResult = sessionSummaryGenerationJobSchema.safeParse(context.task.input);
    if (!jobResult.success) {
      throw new SessionSummaryWorkerError("stale_job", "Task input is not a Session Summary job");
    }
    const job = jobResult.data;
    const deadlineAt = Date.parse(job.deadlineAt);
    if (Date.now() >= deadlineAt) {
      throw abortError(deadlineAt);
    }
    if (
      context.controlEpoch === undefined ||
      context.sessionId !== job.sessionId ||
      context.task.taskId !== job.taskId ||
      !sameIdentity(job, options.evaluatedSelection.candidate)
    ) {
      throw new SessionSummaryWorkerError(
        "identity_mismatch",
        "Claim, task, or evaluated candidate identity did not match the job",
      );
    }
    const deadlineSignal = AbortSignal.timeout(
      Math.min(maxExecutionMs, Math.max(1, deadlineAt - Date.now())),
    );
    const signal = AbortSignal.any([context.signal, deadlineSignal]);
    try {
      const events = await options.tether.fetchExactRange({
        from: job.range.from,
        sessionId: job.sessionId,
        signal,
        to: job.range.to,
      });
      assertSource(job, events);
      const promptInput = JSON.stringify({ events, previousSummary: job.previousSummary });
      if (Buffer.byteLength(promptInput, "utf8") > job.inputLimitBytes) {
        throw new SessionSummaryWorkerError(
          "input_too_large",
          "Summary input exceeded its byte limit",
        );
      }
      const content = await options.ollama.generate({
        events,
        job,
        outputLimitBytes: job.outputLimitBytes,
        signal,
      });
      const submission = sessionSummaryCandidateSubmissionSchema.parse({
        claimantId: context.participantId,
        content,
        controlEpoch: context.controlEpoch,
        instanceId: context.instanceId,
        integrity: {
          algorithm: "sha256",
          hash: createHash("sha256")
            .update(canonicalizeSessionSummaryContent(content))
            .digest("hex"),
        },
        kind: "session_summary.candidate.v1",
        ollama: job.ollama,
        range: job.range,
        sessionId: job.sessionId,
        source: job.source,
        summaryId: job.summaryId,
        taskId: job.taskId,
      });
      await options.tether.submitCandidate(submission, signal);
      return { result: { status: "candidate_submitted" } };
    } catch (error) {
      if (signal.aborted) {
        if (deadlineSignal.aborted) {
          throw new SessionSummaryWorkerError(
            "deadline_exceeded",
            "Session Summary execution deadline elapsed",
          );
        }
        throw abortError(deadlineAt);
      }
      if (
        error instanceof SessionSummaryWorkerError &&
        (error.code === "input_too_large" || error.code === "invalid_output")
      ) {
        throw new SessionSummaryWorkerError(
          "poison_range",
          "Session Summary range produced invalid or oversized work",
          { attempt: 1 },
        ).withSummaryCorrelation(job.summaryId);
      }
      throw (
        error instanceof SessionSummaryWorkerError
          ? error
          : new SessionSummaryWorkerError(
              "generation_unavailable",
              "Session Summary worker boundary failed",
              { cause: error },
            )
      ).withSummaryCorrelation(job.summaryId);
    }
  };
}

function assertSource(job: SessionSummaryGenerationJob, events: readonly SessionEvent[]): void {
  const first = events[0];
  const last = events.at(-1);
  const rangeHash = createHash("sha256")
    .update(events.map((event) => `${event.seq}:${event.eventId}\n`).join(""))
    .digest("hex");
  if (
    events.length !== job.source.eventCount ||
    first?.eventId !== job.source.firstEventId ||
    last?.eventId !== job.source.lastEventId ||
    rangeHash !== job.source.rangeHash
  ) {
    throw new SessionSummaryWorkerError(
      "unsafe_sequence",
      "Fetched events did not match the reserved source identity",
    );
  }
}

function sameIdentity(
  job: SessionSummaryGenerationJob,
  candidate: Extract<EvaluatedSessionSummarySelection, { readonly status: "enabled" }>["candidate"],
): boolean {
  return (
    job.outputSchemaVersion === candidate.outputSchemaVersion &&
    job.promptVersion === candidate.promptVersion &&
    job.ollama.contextSize === candidate.identity.contextSize &&
    job.ollama.model === candidate.identity.model &&
    job.ollama.quantization === candidate.identity.quantization &&
    job.ollama.revision === candidate.identity.revision &&
    job.ollama.thinkingMode === candidate.identity.thinkingMode
  );
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function boundedPositiveInteger(value: number, name: string, maximum: number): number {
  const integer = positiveInteger(value, name);
  if (integer > maximum) {
    throw new TypeError(`${name} must not exceed ${maximum}`);
  }
  return integer;
}
