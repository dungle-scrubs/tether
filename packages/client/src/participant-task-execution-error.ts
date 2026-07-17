/**
 * Executor failure with bounded structured metadata that the shared task flow
 * persists verbatim instead of collapsing to a message-only failure.
 */
export class ParticipantTaskExecutionError extends Error {
  readonly failure: Readonly<Record<string, unknown>>;

  constructor(message: string, failure: Readonly<Record<string, unknown>>, options?: ErrorOptions) {
    super(message, options);
    this.name = "ParticipantTaskExecutionError";
    this.failure = { ...failure };
  }
}
