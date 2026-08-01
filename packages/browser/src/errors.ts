/** Stable failure raised for a rejected or malformed browser operator response. */
export class BrowserOperatorHttpError extends Error {
  /** Browser-safe operation name. */
  readonly operation: string;
  /** Stable server or client rejection reason. */
  readonly reason: string;
  /** HTTP status, or zero when no valid response was available. */
  readonly status: number;

  constructor(input: {
    readonly cause?: unknown;
    readonly operation: string;
    readonly reason: string;
    readonly status: number;
  }) {
    super(`Browser operator request failed: ${input.reason}`, {
      ...(input.cause === undefined ? {} : { cause: input.cause }),
    });
    this.name = "BrowserOperatorHttpError";
    this.operation = input.operation;
    this.reason = input.reason;
    this.status = input.status;
  }
}

/** Stable event-stream failure with no credential or private payload fields. */
export class BrowserSessionStreamError extends Error {
  /** Last fully handled durable sequence. */
  readonly lastHandledSeq: number;
  /** Stable transport or protocol reason. */
  readonly reason: string;

  constructor(input: {
    readonly cause?: unknown;
    readonly lastHandledSeq: number;
    readonly reason: string;
  }) {
    super(`Browser session stream failed: ${input.reason}`, {
      ...(input.cause === undefined ? {} : { cause: input.cause }),
    });
    this.name = "BrowserSessionStreamError";
    this.lastHandledSeq = input.lastHandledSeq;
    this.reason = input.reason;
  }
}
