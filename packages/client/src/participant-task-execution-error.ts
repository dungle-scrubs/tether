/**
 * Executor failure with bounded structured metadata that the shared task flow
 * persists verbatim instead of collapsing to a message-only failure.
 */
export class ParticipantTaskExecutionError extends Error {
  readonly failure: Readonly<Record<string, unknown>>;
  /**
   * Whether the failure is transient, so the shared task flow releases the
   * claim back to the claimable pool instead of failing the task terminally.
   * Derived from the `retryable` flag inside the structured failure payload.
   */
  readonly retryable: boolean;

  constructor(message: string, failure: Readonly<Record<string, unknown>>, options?: ErrorOptions) {
    super(message, options);
    this.name = "ParticipantTaskExecutionError";
    this.failure = { ...failure };
    this.retryable = failure.retryable === true;
  }
}
