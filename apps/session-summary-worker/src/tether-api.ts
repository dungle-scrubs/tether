import {
  type SessionEvent,
  type SessionSummaryCandidateSubmission,
  sessionEventSchema,
  sessionSummaryCandidateSubmissionSchema,
} from "@dungle-scrubs/tether-protocol";
import { z } from "zod";

import { SessionSummaryWorkerError } from "./errors.js";

const eventPageSchema = z.strictObject({
  events: z.array(sessionEventSchema),
  pagination: z.strictObject({
    afterSeq: z.number().int().nonnegative().safe(),
    hasMore: z.boolean(),
    limit: z.number().int().positive().safe(),
    nextAfterSeq: z.number().int().nonnegative().safe(),
    returned: z.number().int().nonnegative().safe(),
  }),
});

/** Bounded authenticated HTTP configuration for Tether worker calls. */
export interface TetherApiClientOptions {
  readonly authToken: string;
  readonly baseUrl: string;
  readonly fetch?: typeof globalThis.fetch;
  readonly pageSize: number;
  readonly requestTimeoutMs: number;
}

/** Authenticated exact-range and candidate-submission adapter for Tether. */
export class TetherApiClient {
  readonly #authToken: string;
  readonly #baseUrl: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #pageSize: number;
  readonly #requestTimeoutMs: number;

  constructor(options: TetherApiClientOptions) {
    this.#authToken = requireNonempty(options.authToken, "authToken");
    this.#baseUrl = requireNonempty(options.baseUrl, "baseUrl").replace(/\/$/u, "");
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#pageSize = boundedPositiveInteger(options.pageSize, "pageSize", 10_000);
    this.#requestTimeoutMs = boundedPositiveInteger(
      options.requestTimeoutMs,
      "requestTimeoutMs",
      1_800_000,
    );
  }

  /** Reads only the inclusive selected range and rejects gaps or foreign sessions. */
  async fetchExactRange(input: {
    readonly from: number;
    readonly sessionId: string;
    readonly signal: AbortSignal;
    readonly to: number;
  }): Promise<readonly SessionEvent[]> {
    const events: SessionEvent[] = [];
    let after = input.from - 1;
    while (after < input.to) {
      const remaining = input.to - after;
      const limit = Math.min(this.#pageSize, remaining);
      const url = new URL(
        `${this.#baseUrl}/sessions/${encodeURIComponent(input.sessionId)}/events`,
      );
      url.searchParams.set("after", String(after));
      url.searchParams.set("limit", String(limit));
      const response = await this.#request(url.toString(), { method: "GET", signal: input.signal });
      if (!response.ok) {
        throw new SessionSummaryWorkerError(
          "generation_unavailable",
          `Tether event range request failed with HTTP ${response.status}`,
          { retryable: response.status === 429 || response.status >= 500 },
        );
      }
      const page = eventPageSchema.safeParse(await response.json());
      if (!page.success || page.data.events.length === 0) {
        throw new SessionSummaryWorkerError(
          "unsafe_sequence",
          "Tether returned an invalid or incomplete event range",
        );
      }
      for (const event of page.data.events) {
        const expectedSeq = input.from + events.length;
        if (
          event.seq !== expectedSeq ||
          event.seq > input.to ||
          event.sessionId !== input.sessionId
        ) {
          throw new SessionSummaryWorkerError(
            "unsafe_sequence",
            "Tether returned a non-contiguous or foreign event range",
          );
        }
        events.push(event);
      }
      after = events.at(-1)?.seq ?? after;
    }
    return events;
  }

  /** Submits one protocol-validated candidate before task completion. */
  async submitCandidate(
    submissionInput: SessionSummaryCandidateSubmission,
    signal: AbortSignal,
  ): Promise<void> {
    const submission = sessionSummaryCandidateSubmissionSchema.parse(submissionInput);
    const url = `${this.#baseUrl}/sessions/${encodeURIComponent(submission.sessionId)}/tasks/${encodeURIComponent(submission.taskId)}/summary-candidate`;
    const response = await this.#request(url, {
      body: JSON.stringify(submission),
      headers: { "content-type": "application/json" },
      method: "POST",
      signal,
    });
    if (!response.ok) {
      // Tether reports publication conflicts as 4xx; throttling and server
      // unavailability are transient and stay retryable so the claim flow can
      // treat them as such instead of recording a durable false conflict.
      if (response.status === 429 || response.status >= 500) {
        throw new SessionSummaryWorkerError(
          "generation_unavailable",
          `Session Summary candidate submission failed with HTTP ${response.status}`,
          { retryable: true },
        );
      }
      throw new SessionSummaryWorkerError(
        "publication_conflict",
        `Session Summary candidate submission failed with HTTP ${response.status}`,
      );
    }
  }

  async #request(input: string | URL, init: RequestInit): Promise<Response> {
    const timeout = AbortSignal.timeout(this.#requestTimeoutMs);
    try {
      return await this.#fetch(input, {
        ...init,
        headers: { authorization: `Bearer ${this.#authToken}`, ...init.headers },
        signal: AbortSignal.any([init.signal ?? new AbortController().signal, timeout]),
      });
    } catch (error) {
      if (init.signal?.aborted) {
        throw new SessionSummaryWorkerError("cancelled", "Tether request was cancelled", {
          cause: error,
        });
      }
      throw new SessionSummaryWorkerError(
        "generation_unavailable",
        timeout.aborted ? "Tether request timed out" : "Tether transport failed",
        { cause: error, retryable: true },
      );
    }
  }
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function requireNonempty(value: string, name: string): string {
  if (value.length === 0) {
    throw new TypeError(`${name} must not be empty`);
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
