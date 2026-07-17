import { ParticipantTaskExecutionError } from "@dungle-scrubs/tether-client";
import {
  deriveSessionSummaryCorrelationId,
  type SessionSummaryFailure,
} from "@dungle-scrubs/tether-protocol";

/** Stable failure codes emitted by the worker task executor. */
export type SessionSummaryWorkerErrorCode = SessionSummaryFailure["code"];

/** Bounded typed failure crossing the worker and client-runtime seam. */
export class SessionSummaryWorkerError extends ParticipantTaskExecutionError {
  readonly code: SessionSummaryWorkerErrorCode;
  readonly correlationId: string | null;
  readonly retryable: boolean;

  constructor(
    code: SessionSummaryWorkerErrorCode,
    message: string,
    options: {
      readonly attempt?: number;
      readonly cause?: unknown;
      readonly correlationId?: string;
      readonly retryable?: boolean;
    } = {},
  ) {
    const boundedMessage = message.slice(0, 512);
    super(
      boundedMessage,
      {
        attempt: Math.max(1, Math.min(10, Math.trunc(options.attempt ?? 1))),
        code,
        ...(options.correlationId === undefined ? {} : { correlationId: options.correlationId }),
        message: boundedMessage,
        retryable: options.retryable ?? false,
      },
      options.cause === undefined ? undefined : { cause: options.cause },
    );
    this.name = "SessionSummaryWorkerError";
    this.code = code;
    this.correlationId = options.correlationId ?? null;
    this.retryable = options.retryable ?? false;
  }

  /** Returns this failure with the bounded diagnostic correlation attached. */
  withSummaryCorrelation(summaryId: string): SessionSummaryWorkerError {
    const correlationId = deriveSessionSummaryCorrelationId(summaryId);
    return this.correlationId === correlationId
      ? this
      : new SessionSummaryWorkerError(this.code, this.message, {
          cause: this.cause,
          correlationId,
          retryable: this.retryable,
        });
  }

  /** Returns protocol-bounded task failure metadata. */
  toFailure(attempt: number): SessionSummaryFailure {
    return {
      attempt: Math.max(1, Math.min(10, Math.trunc(attempt))),
      code: this.code,
      ...(this.correlationId === null ? {} : { correlationId: this.correlationId }),
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
