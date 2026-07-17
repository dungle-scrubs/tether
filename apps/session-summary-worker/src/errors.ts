import { ParticipantTaskExecutionError } from "@dungle-scrubs/tether-client";
import type { SessionSummaryFailure } from "@dungle-scrubs/tether-protocol";

/** Stable failure codes emitted by the worker task executor. */
export type SessionSummaryWorkerErrorCode = SessionSummaryFailure["code"];

/** Bounded typed failure crossing the worker and client-runtime seam. */
export class SessionSummaryWorkerError extends ParticipantTaskExecutionError {
  readonly code: SessionSummaryWorkerErrorCode;
  readonly retryable: boolean;

  constructor(
    code: SessionSummaryWorkerErrorCode,
    message: string,
    options: {
      readonly attempt?: number;
      readonly cause?: unknown;
      readonly retryable?: boolean;
    } = {},
  ) {
    const boundedMessage = message.slice(0, 512);
    super(
      boundedMessage,
      {
        attempt: Math.max(1, Math.min(10, Math.trunc(options.attempt ?? 1))),
        code,
        message: boundedMessage,
        retryable: options.retryable ?? false,
      },
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.name = "SessionSummaryWorkerError";
    this.code = code;
    this.retryable = options.retryable ?? false;
  }

  /** Returns protocol-bounded task failure metadata. */
  toFailure(attempt: number): SessionSummaryFailure {
    return {
      attempt: Math.max(1, Math.min(10, Math.trunc(attempt))),
      code: this.code,
      message: this.message,
      retryable: this.retryable,
    };
  }
}

/** Maps transport aborts to a stable cancellation or deadline failure. */
export function abortError(deadlineAt: number): SessionSummaryWorkerError {
  return Date.now() >= deadlineAt
    ? new SessionSummaryWorkerError("deadline_exceeded", "Session Summary deadline elapsed")
    : new SessionSummaryWorkerError("cancelled", "Session Summary generation was cancelled");
}
